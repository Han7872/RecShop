# -*- coding: utf-8 -*-
"""m9_adapter.py — per-service 宽表 adapter (M9 Phase 0.5, B3)。

一个 case 的 raw/metrics/metrics_v2.jsonl(长表) → BARO/RCD 能直接吃的宽表:
    time | <svc>__<metric> | <svc>__<metric> | ...
(`time` = epoch 秒 float;每列 = 一个 (service, metric) 通道)

★通道选择(防泄漏,M9 黑板 §1.2 MAJOR 裁定):
  ① probe-panel 通道  service == "probe-panel"(source=http_probe) —— 统一探针面板,
     按 labels.target_service re-key 到真服务名。全 case / 全 fault 恒同 11 目标 → 无 fault-correlated 泄漏。
  ② prom 通道        source ∈ {cadvisor, kube_state, otel}(container_* / http_server_* / kube-state)。
  ③ off-graph 伪节点  service == "mysql"(items_lock_granted_count)、service == "host"(vm_cpu_saturation_ratio)
     —— 让 db_lock / host_cpu 的根在特征空间【有列】,而非全员 MISS。

★必须排除(硬红线):
  - service == "traffic-probe"(carrier 载体通道)。载体是 per-fault 选的,"探谁 = 答案" →
    喂进方法会污染成绩。**精确匹配排除,不用 startswith**(startswith("traffic-probe") 会连 panel 一起误删;
    panel 已按裁定改名 probe-panel,双保险)。
  - service == "stressor"(注入车辆,非业务)。
  - source == "nginx_config"(catalog-gw 配置状态量:proxy_read_timeout_ms 等)。它几乎是 cfg 类故障的
    oracle 观测,默认不进 X;需要时 --include-nginx-config 打开(仅供对照实验,不做默认口径)。

★GT-blind:本模块【绝不】读 groundtruth.json。GT 只在 m9_score.py 事后打分时读。

对齐 / 稠密化:
  - 只取 stage ∈ {pre_fault, during_fault};inject_time = during_fault 记录的最小 ts。
  - ts 落 --bucket(默认 2s,= runner poll 节拍)网格 → pivot(mean) → reindex 到完整网格 → ffill+bfill。
    (各通道原生节拍不同:panel/http_probe 2s 真新鲜、cadvisor ~5s scrape、OTel export 15s、
     窗级聚合 1 点/窗 —— 网格 + ffill 让它们能同表;独立值分层如实见 EVAL_NOTES。)
  - 全空列丢弃;ffill/bfill 后无 NaN(BARO 的 RobustScaler / RCD 的离散化都吃不了 NaN)。

★已知方法侧限制(非本 adapter 可修,记账用):BARO/RCD 的 preprocess 都会 drop_constant(normal_df) ——
  在 pre 窗恒定的列(如 mysql items_lock 恒 0、host 窗级 1 点/窗)会被方法自己丢掉。故 off-graph 伪节点
  即便有列也可能排不进 rank。为此本 adapter 默认额外派生一条【稠密】host 列(见 --no-derived-host):
      host__container_cpu_sum_cores      = Σ_svc container_cpu_usage_cores
      host__container_throttled_sum      = Σ_svc container_cpu_throttled_seconds_rate
  两列对【所有 case / 所有 fault 一视同仁】地派生(与 GT 无关)→ 不构成泄漏。

用法:
    python scripts/chaos/ctk/m9_adapter.py <case_dir> [--csv out.csv] [--bucket 2]
    # 或作模块: from m9_adapter import build_wide;  df, inject, info = build_wide(case_dir)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

# ---------------- 通道白/黑名单 ----------------
PANEL_SERVICE = "probe-panel"          # 统一探针面板(M9 新增;不是 carrier)
CARRIER_SERVICE = "traffic-probe"      # ★carrier 载体 —— 精确匹配排除
EXCLUDE_SERVICES = {CARRIER_SERVICE, "stressor"}
# prom 通道 source。native runner 发 cadvisor/kube_state/otel;★package_for_delivery 打包后
# 把 source 统一改写成 "prometheus"(实测交付区 single_20260709)→ 两种口径都吃,否则打包件上全空表。
PROM_SOURCES = {"cadvisor", "kube_state", "otel", "prometheus"}

# nginx_config 状态量(service=catalog-gw)。打包后 source 被抹成 prometheus → 只能按 metric 名认。
NGINX_METRICS = {"proxy_read_timeout_ms", "proxy_retry_enabled",
                 "proxy_retry_tries", "proxy_connect_timeout_ms"}
OFFGRAPH = {                            # service -> 允许的 metric(off-graph 伪节点)
    "mysql": {"items_lock_granted_count"},
    "host": {"vm_cpu_saturation_ratio"},
}
STAGES = ("pre_fault", "during_fault")

# panel 指标 → 列后缀(request_success 翻成 error 更符合"越大越异常"的方向)
PANEL_METRICS = {
    "request_duration_ms": "panel_latency_ms",
    "request_success": "panel_error",
    "http_status_code": None,           # 类别量,不进 X
}

CPU_COL = "container_cpu_usage_cores"
THROTTLE_COL = "container_cpu_throttled_seconds_rate"


def _epoch(ts):
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    s = str(ts).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def _iter_records(case_dir):
    path = os.path.join(case_dir, "raw", "metrics", "metrics_v2.jsonl")
    with open(path, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _column_for(rec, include_nginx=False):
    """一条 record → (svc, column_name, value) 或 None(该通道不进 X)。"""
    svc = rec.get("service")
    metric = rec.get("metric")
    source = rec.get("source")
    val = rec.get("value")
    if not svc or not metric or val is None:
        return None
    if isinstance(val, bool):
        val = 1.0 if val else 0.0
    try:
        val = float(val)
    except (TypeError, ValueError):
        return None
    if isinstance(val, float) and np.isnan(val):
        return None

    # --- 硬排除:carrier / stressor(精确匹配) ---
    if svc in EXCLUDE_SERVICES:
        return None

    # --- ① panel ---
    if svc == PANEL_SERVICE:
        target = (rec.get("labels") or {}).get("target_service")
        if not target:
            return None
        suffix = PANEL_METRICS.get(metric, None)
        if suffix is None:
            return None
        if metric == "request_success":
            val = 0.0 if val >= 1.0 else 1.0        # success 1/0 → error 0/1
        return str(target), f"{target}__{suffix}", val

    # --- ③ off-graph 伪节点 ---
    if svc in OFFGRAPH:
        if metric not in OFFGRAPH[svc]:
            return None
        return svc, f"{svc}__{metric}", val

    # --- nginx_config(默认关;按 source 或 metric 名认,兼容打包后 source 被抹平) ---
    if source == "nginx_config" or metric in NGINX_METRICS:
        if not include_nginx:
            return None
        return svc, f"{svc}__{metric}", val

    # --- ② prom 通道 ---
    if source in PROM_SOURCES:
        return svc, f"{svc}__{metric}", val

    # http_probe 但既非 panel 又非 carrier(理论上不存在)→ 保守丢弃
    return None


def _stage_windows(case_dir):
    """raw/metrics/manifest.json 的 stage_windows → {stage: (start_epoch, end_epoch)}。缺失 → {}。"""
    path = os.path.join(case_dir, "raw", "metrics", "manifest.json")
    try:
        m = json.load(open(path, encoding="utf-8-sig"))
    except Exception:
        return {}
    out = {}
    for st, w in (m.get("stage_windows") or {}).items():
        if st not in STAGES or not isinstance(w, dict):
            continue
        s, e = _epoch(w.get("start")), _epoch(w.get("end"))
        if s is not None and e is not None:
            out[st] = (s, e)
    return out


def build_wide(case_dir, bucket=2.0, include_nginx=False, derived_host=True,
               gap_aware=True):
    """长表 → (wide_df, inject_time, info dict)。GT-blind。

    wide_df: DataFrame,首列 'time'(epoch float),其余 <svc>__<metric>,末列 'stage';无 NaN。
    inject_time: during_fault 最小 ts(epoch float);无 during 数据 → None。
    info: {'n_rows','n_cols','services','pre_points','during_points','dropped_all_nan', ...}

    ★gap_aware(默认 True):**按 stage 分段建网格**。
      runner 的 pre 窗采满即停 → 注入原语(rollout 类要 50s)在两次 capture_stage 之间执行 →
      during 窗才重开采集。故 pre 末条与 during 首条之间存在一段【零采样盲区】
      (实测中位 14.6s、最长 113.3s @dual11;区间内 28/28 case 零记录)。
      旧行为把 2s 网格【铺过】盲区再 ffill/bfill 填满 → 伪造 normal 行(实测 390/1722 = 22.7%,
      最坏 62.8%),把 130 个列次的 IQR 从 >0 压成 0(RobustScaler 在 IQR==0 时 scale_=1.0 → 退化)。
      gap_aware=True:pre / during 各自建网格、各自段内 ffill+bfill,**盲区的桶根本不生成**,
      且不再有跨 stage 的 bfill(旧行为里 during-only 的列会被 bfill 回灌进 normal 窗 = 反向泄漏)。
      gap_aware=False:与修复前完全一致(留一个可复现旧结果的开关)。
    """
    recs = []
    during_ts = []
    for r in _iter_records(case_dir):
        stage = r.get("stage")
        if stage not in STAGES:
            continue
        t = _epoch(r.get("timestamp") or r.get("ts"))
        if t is None:
            continue
        parsed = _column_for(r, include_nginx=include_nginx)
        if parsed is None:
            continue
        svc, col, val = parsed
        recs.append((t, col, val, svc, stage))
        if stage == "during_fault":
            during_ts.append(t)

    if not recs or not during_ts:
        return None, None, {"n_rows": 0, "n_cols": 0, "services": [],
                            "pre_points": 0, "during_points": 0, "dropped_all_nan": []}

    df = pd.DataFrame(recs, columns=["time", "col", "val", "svc", "stage"])
    # 落 bucket 网格(2s = runner poll 节拍),同格多值取均值
    b = float(bucket) if bucket and bucket > 0 else 1.0
    df["tb"] = np.floor(df["time"] / b) * b
    # ★inject 必须落同一网格:否则首个 during 桶(floor 后 < 原始 inject)会被误算进 normal 段
    #   (窗级 1 点/窗 的通道上这会让 during_points 直接变 0 —— 实测踩过)。
    inject = float(np.floor(min(during_ts) / b) * b)

    manifest_windows = _stage_windows(case_dir)
    gap_info = {}

    if not gap_aware:
        # ---------- 旧行为(单一网格铺过盲区 + 全局 ffill/bfill) ----------
        wide = df.pivot_table(index="tb", columns="col", values="val", aggfunc="mean").sort_index()
        grid = np.arange(wide.index.min(), wide.index.max() + b / 2.0, b)
        wide = wide.reindex(index=pd.Index(grid, name="tb"))
        dropped = [c for c in wide.columns if wide[c].isna().all()]
        if dropped:
            wide = wide.drop(columns=dropped)
        wide = wide.ffill().bfill()
        wide = wide.dropna(axis=1, how="any")
        stage_col = pd.Series(np.where(wide.index.values < inject, "pre_fault", "during_fault"),
                              index=wide.index)
    else:
        # ---------- gap-aware:按 stage 分段建网格,禁止跨盲区 ffill ----------
        segs = []
        stages_present = []
        for st in STAGES:
            sub = df[df["stage"] == st]
            if sub.empty:
                continue
            w = sub.pivot_table(index="tb", columns="col", values="val", aggfunc="mean").sort_index()
            # 段内网格:边界取【该 stage 真实记录】的 min/max(manifest 窗仅作审计,
            # 不用它撑边界——manifest 窗可能宽于实际采样,撑出来的桶还是 ffill 出来的假行)
            lo, hi = float(w.index.min()), float(w.index.max())
            g = np.arange(lo, hi + b / 2.0, b)
            w = w.reindex(index=pd.Index(g, name="tb"))
            w = w.ffill().bfill()          # ★只在段内填,盲区两侧互不串味
            segs.append((st, w))
            stages_present.append(st)
        wide = pd.concat([w for _, w in segs], axis=0).sort_index()
        stage_col = pd.concat([pd.Series(st, index=w.index) for st, w in segs]).sort_index()
        dropped = [c for c in wide.columns if wide[c].isna().all()]
        if dropped:
            wide = wide.drop(columns=dropped)
        # 段内 ffill/bfill 后仍有 NaN 的列 = 某一 stage 整段无数据的列。旧行为靠跨 stage bfill 把它们
        # 补活(= 把 during 的值灌进 normal 窗),gap-aware 下如实丢弃。
        cross_stage_only = [c for c in wide.columns if wide[c].isna().any()]
        wide = wide.dropna(axis=1, how="any")
        gap_info["dropped_cross_stage_only"] = cross_stage_only
        # 盲区大小(审计用)
        pre_max = df.loc[df["stage"] == "pre_fault", "time"].max() if (df["stage"] == "pre_fault").any() else None
        dur_min = min(during_ts)
        if pre_max is not None:
            gap_info["blind_gap_sec"] = round(float(dur_min - pre_max), 2)
        if manifest_windows:
            gap_info["manifest_stage_windows"] = {
                st: [round(s, 2), round(e, 2)] for st, (s, e) in manifest_windows.items()}

    # 派生稠密 host 列(GT-blind:对每个 case 一视同仁地算)
    if derived_host:
        cpu_cols = [c for c in wide.columns
                    if c.endswith("__" + CPU_COL) and not c.startswith("host__")]
        thr_cols = [c for c in wide.columns
                    if c.endswith("__" + THROTTLE_COL) and not c.startswith("host__")]
        if cpu_cols:
            wide["host__container_cpu_sum_cores"] = wide[cpu_cols].sum(axis=1)
        if thr_cols:
            wide["host__container_throttled_sum"] = wide[thr_cols].sum(axis=1)

    wide["stage"] = stage_col.reindex(wide.index).values
    wide = wide.reset_index().rename(columns={"tb": "time"})
    wide = wide.sort_values("time").reset_index(drop=True)
    # stage 挪到末列(方法侧只吃数值列;下游 m9_score 会显式 drop 掉它)
    wide = wide[[c for c in wide.columns if c != "stage"] + ["stage"]]

    feat_cols = [c for c in wide.columns if c not in ("time", "stage")]
    services = sorted({c.split("__", 1)[0] for c in feat_cols})
    info = {
        "n_rows": int(wide.shape[0]),
        "n_cols": len(feat_cols),
        "services": services,
        "pre_points": int((wide["time"] < inject).sum()),
        "during_points": int((wide["time"] >= inject).sum()),
        "dropped_all_nan": dropped,
        "gap_aware": bool(gap_aware),
    }
    info.update(gap_info)
    return wide, inject, info


def col_to_service(col):
    """列名 → 服务名(去掉 __<metric> 后缀)。"""
    return col.split("__", 1)[0]


def main():
    ap = argparse.ArgumentParser(description="metrics_v2 长表 → per-service 宽表(GT-blind)")
    ap.add_argument("case_dir")
    ap.add_argument("--csv", help="宽表写到该 CSV")
    ap.add_argument("--bucket", type=float, default=2.0, help="时间网格秒(默认 2 = poll 节拍)")
    ap.add_argument("--include-nginx-config", action="store_true",
                    help="把 catalog-gw nginx_config 状态量也当特征(默认关,近 oracle)")
    ap.add_argument("--no-derived-host", action="store_true", help="不派生稠密 host 列")
    ap.add_argument("--no-gap-aware", action="store_true",
                    help="旧行为:单一网格铺过 pre→during 的零采样盲区并全局 ffill/bfill(仅供复现旧结果)")
    a = ap.parse_args()

    df, inject, info = build_wide(a.case_dir, bucket=a.bucket,
                                  include_nginx=a.include_nginx_config,
                                  derived_host=not a.no_derived_host,
                                  gap_aware=not a.no_gap_aware)
    if df is None:
        print("EMPTY: 无可用记录(panel/prom/off-graph 通道均无 pre+during 数据)")
        sys.exit(2)
    print(f"rows={info['n_rows']} cols={info['n_cols']} "
          f"pre={info['pre_points']} during={info['during_points']} inject={inject:.1f} "
          f"gap_aware={info['gap_aware']} blind_gap={info.get('blind_gap_sec')}")
    print("services: " + ", ".join(info["services"]))
    if a.csv:
        df.to_csv(a.csv, index=False)
        print(f"→ {a.csv}")


if __name__ == "__main__":
    main()
