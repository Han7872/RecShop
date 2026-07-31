# -*- coding: utf-8 -*-
"""m9_score.py — 单 case 的 BARO + RCD 打分器(M9 Phase 0.5, B4)。

流程:  case_dir --(m9_adapter, GT-blind)--> 宽表 --> BARO / RCD --> 排名(列名→服务, 去重保序)
       --> 与 groundtruth.json 的 root_cause_services 对比 --> ★MRCBench 四族指标(上游论文 4.3)

MRCBench 四族(K=1,3,5;单 case 即 per-sample,多 case 由上层 macro 平均):
  设真根集 G,方法排序服务列表 L,Top-K 集 T^K,R=|G|
    Hit@K      = 1(T^K ∩ G ≠ ∅)
    Recall@R   = |T^R ∩ G| / |G|          (预算 = 真实根因数)
    FullHit@K  = 1(G ⊆ T^K)
    NDCG@K     : rel_j = 1 若 L[j] ∈ G;DCG@K = Σ_{j=1..K} rel_j / log2(j+1)
                 IDCG@K = Σ_{j=1..min(K,|G|)} 1/log2(j+1);NDCG = DCG/IDCG
  单根 case 自动退化(Hit@K == FullHit@K;Recall@R == Hit@1)。

方法环境(照抄 third_party/METHODS_SETUP.md,已跑通):
  sys.path 前置 third_party/_cl_patched(patched causallearn 0.1.2.3 + RCAEval vendored 4 文件补丁)
  + third_party/RCAEval;importlib 单文件加载 e2e/baro.py 与 e2e/rcd.py(绕开 e2e/__init__ 的 sknetwork 依赖)。

退出码(驱动器 m9_drive.sh 靠它判"密度到底修没修好"):
  0 = 两法均给出非空排名
  3 = ★任一方法空排名(BARO 或 RCD)
  4 = adapter 无数据 / 方法整体异常

用法:
  python scripts/chaos/ctk/m9_score.py <case_dir> [--type net_delay_single] [--out m9_verdict.jsonl]
  (--out 追加一行 JSON;stdout 恒打印同一 JSON)
"""
from __future__ import annotations

import argparse
import importlib.util
import io
import json
import math
import os
import sys
from datetime import datetime, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))

# patched causallearn 必须排在 site-packages 之前;RCAEval 包次之(METHODS_SETUP.md 铁律)
sys.path.insert(0, os.path.join(REPO_ROOT, "third_party", "_cl_patched"))
sys.path.insert(0, os.path.join(REPO_ROOT, "third_party", "RCAEval"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # m9_adapter

import numpy as np  # noqa: E402  (退化尺度列剔除要用 np.percentile)

from m9_adapter import build_wide, col_to_service  # noqa: E402

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

K_LIST = (1, 3, 5)

# GT root 名 → adapter 里的节点名(off-graph 伪节点)
GT_ALIAS = {
    "mysql_items_lock": "mysql",
    "mysql:items": "mysql",
    "node": "host",
}


def _load_method(rel_path, mod_name):
    path = os.path.join(REPO_ROOT, rel_path)
    spec = importlib.util.spec_from_file_location(mod_name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def gt_roots(case_dir):
    """★仅事后打分用(绝不进 adapter)。"""
    g = json.load(open(os.path.join(case_dir, "groundtruth.json"), encoding="utf-8-sig"))
    out = []
    for x in (g.get("root_cause_services") or []):
        s = str(x).strip()
        s = GT_ALIAS.get(s, s)
        for suf in ("_service", "_api"):
            if s.endswith(suf):
                s = s[: -len(suf)]
        if s and s not in out:
            out.append(s)
    return out


def fault_of(case_dir):
    try:
        m = json.load(open(os.path.join(case_dir, "metadata.json"), encoding="utf-8-sig"))
        return (m.get("config") or {}).get("fault", "?")
    except Exception:
        return "?"


def ranks_to_services(rank_cols):
    """排名列名 → 服务名,去重保序(同服务多 metric 只算最靠前那次)。"""
    seen = set()
    out = []
    for c in rank_cols or []:
        s = col_to_service(str(c))
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def mrcbench(rank_svcs, roots):
    """MRCBench 四族指标(上游论文 4.3)。rank_svcs = 去重保序服务排名;roots = 真根集。"""
    G = set(roots)
    R = max(1, len(G))
    res = {}
    for K in K_LIST:
        topk = rank_svcs[:K]
        res[f"hit@{K}"] = 1.0 if (set(topk) & G) else 0.0
        res[f"fullhit@{K}"] = 1.0 if G.issubset(set(topk)) else 0.0
        dcg = sum((1.0 / math.log2(j + 2)) for j, s in enumerate(topk) if s in G)
        idcg = sum((1.0 / math.log2(j + 2)) for j in range(min(K, len(G))))
        res[f"ndcg@{K}"] = (dcg / idcg) if idcg > 0 else 0.0
    topr = rank_svcs[:R]
    res["recall@R"] = len(set(topr) & G) / float(len(G)) if G else 0.0
    # -------------------------------------------------------------------------
    # ★ RCAEval 口径新增(2026-07-14 M11 A1)—— 四族公式一个字节不改,只【新增】key。
    # avg@k = RCAEval 的 per-case AC@k(accuracy@k),定义见
    #   third_party/RCAEval/RCAEval/benchmark/evaluation.py::Evaluator.accuracy:
    #     per-case AC@k = |ranks[:k] ∩ answers| / |answers|
    #   = 命中根因在 top-k 的平均占比。macro over cases → RCAEval 语料级 AC@k。
    #   (RCAEval 的 Avg@k = (1/k)Σ_{j≤k}AC@j 是语料级二次聚合,不在 per-case 层算。)
    #   空排名 / 根不在候选 → 交集为空 → 0(与四族对空排名一致)。G 空 → 0。
    for K in K_LIST:
        topk = rank_svcs[:K]
        res[f"avg@{K}"] = (len(set(topk) & G) / float(len(G))) if G else 0.0
    # mrr = 每个根因的 reciprocal rank(1/rank;不在排名则 0)取均值。
    #   |G|=1 退化为该根 1/rank(即标准 MRR);|G|>1 = 全根 reciprocal 均值(本项目口径,注明)。
    if G and rank_svcs:
        pos = {s: i + 1 for i, s in enumerate(rank_svcs)}
        res["mrr"] = sum((1.0 / pos[g]) if g in pos else 0.0 for g in G) / float(len(G))
    else:
        res["mrr"] = 0.0
    return res


def main():
    ap = argparse.ArgumentParser(description="BARO + RCD + MRCBench 四族指标(单 case)")
    ap.add_argument("case_dir")
    ap.add_argument("--type", default=None, help="类型标签(驱动器传入,如 net_delay_single / dual01)")
    ap.add_argument("--out", default=None, help="追加一行 JSON 到该文件(m9_verdict.jsonl)")
    ap.add_argument("--bucket", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--include-nginx-config", action="store_true")
    ap.add_argument("--no-gap-aware", action="store_true",
                    help="旧行为:网格铺过 pre→during 零采样盲区 + 全局 ffill/bfill(仅供复现旧结果)")
    a = ap.parse_args()

    case_dir = os.path.abspath(a.case_dir)
    case_id = os.path.basename(case_dir.rstrip("/\\"))
    verdict = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "case_id": case_id,
        "case_dir": case_dir.replace("\\", "/"),
        "type": a.type,
        "fault": fault_of(case_dir),
    }

    try:
        roots = gt_roots(case_dir)
    except Exception as e:
        roots = []
        verdict["gt_error"] = f"{type(e).__name__}: {e}"
    verdict["gt_roots"] = roots

    df, inject, info = build_wide(case_dir, bucket=a.bucket,
                                  include_nginx=a.include_nginx_config,
                                  gap_aware=not a.no_gap_aware)
    if df is None or inject is None or df.shape[0] < 4:
        verdict.update({"status": "NO_DATA", "adapter": info,
                        "empty_ranking": {"baro": True, "rcd": True}})
        _emit(verdict, a.out)
        sys.exit(4)

    # adapter 现在返回一列 'stage'(审计用,非数值特征)→ 喂方法前必须丢掉
    if "stage" in df.columns:
        df = df.drop(columns=["stage"])

    # =========================================================================
    # ★★ 单位归一(2026-07-11 主循环实测发现的真 bug,必须在喂方法【之前】做)
    # -------------------------------------------------------------------------
    # 机制: BARO 的 RobustScorer 用 RobustScaler(median/IQR) 归一化后【跨列】比 max|z| 排名。
    #   但 sklearn 的 RobustScaler 在 IQR==0 时把 scale_ 置为 1.0(_handle_zeros_in_scale)
    #   → 该列的 "z" 退化成【原始单位的差值】, 不再无量纲。
    #   我方 container_memory_*/network_*_bytes 单位是【字节】且 pre 窗近乎平坦(实测 30s 窗仅 2 个
    #   独立值 → IQR=0)→ z = 内存涨的【字节数】(实测 24576/8192/4096, 全是 2 的幂×1024 = 字节量化签名)
    #   → 拿 "24576 字节" 跟 catalog 真延迟信号的 "11259 个 IQR" 比大小 = 拿字节比毫秒
    #   → 垃圾列凭"字节数字大"稳赢, 真根因被挤到第二。实测: 归一后 service_cpu 的 catalog 立刻回第一。
    #
    # ★为什么是"字节→MB"而不是"剔掉 IQR=0 的列":
    #   IQR=0 有【两副面孔】—— 同样 IQR=0:
    #     · 内存(字节): z = 24576 → 垃圾, 靠单位大取胜
    #     · 错误率(pre 全 0 → during 全 1, 如 catalog__panel_error): z = 1.0 → ★这是最好的信号★
    #   一刀切剔 IQR=0 会把【错误率这类最具判别力的列一起删掉】(实测 pod_failure 的 catalog__panel_error
    #   正是 0→1)。改按单位归一: 字节→MB 后内存 z 降到 ~0.02, 错误率 z=1.0 得以幸存并竞争。
    #
    # 这正是 RCAEval 自带 convert_mem_mb 的本意 —— 它只匹配列名以 "_mem" 结尾的列(他们的命名约定),
    #   我方列名是 <svc>__container_memory_rss_bytes → 不匹配 → 从未生效。此处按语义补上。
    #   (drop_constant 只删【严格恒定】列; 内存有阶跃变化 → 漏网。drop_near_constant 仅在
    #    dk_select_useful=True 时启用且同样按列名过滤。)
    #
    # 本归一是【GT-blind】(只看列名单位, 不看标签)、方法无关 —— 修正数值退化, 不是调参凑答案。
    # =========================================================================
    _byte_cols = [c for c in df.columns if c != "time" and "bytes" in c]
    if _byte_cols:
        df = df.copy()
        for _c in _byte_cols:
            df[_c] = df[_c] / 1e6          # bytes → MB(与 RCAEval convert_mem_mb 同语义)

    verdict["adapter"] = {
        "rows": info["n_rows"], "cols": info["n_cols"],
        "pre_points": info["pre_points"], "during_points": info["during_points"],
        "n_services": len(info["services"]),
        "root_covered": all(r in info["services"] for r in roots) if roots else None,
        "byte_cols_normalized_to_mb": len(_byte_cols),   # 单位归一列数(可审计)
        "gap_aware": info.get("gap_aware"),
        "blind_gap_sec": info.get("blind_gap_sec"),
    }

    baro = _load_method("third_party/RCAEval/RCAEval/e2e/baro.py", "m9_baro").baro
    rcd = _load_method("third_party/RCAEval/RCAEval/e2e/rcd.py", "m9_rcd").rcd

    results = {}
    errors = {}
    try:
        results["baro"] = ranks_to_services(baro(df, inject_time=inject, dataset="recshop").get("ranks", []))
    except Exception as e:
        results["baro"] = []
        errors["baro"] = f"{type(e).__name__}: {e}"
    try:
        results["rcd"] = ranks_to_services(
            rcd(df, inject_time=inject, dataset="recshop", seed=a.seed,
                gamma=5, localized=True, bins=5).get("ranks", []))
    except Exception as e:
        results["rcd"] = []
        errors["rcd"] = f"{type(e).__name__}: {e}"

    empty = {m: (len(r) == 0) for m, r in results.items()}
    verdict["empty_ranking"] = empty
    verdict["baro_rank_top5"] = results["baro"][:5]
    verdict["rcd_rank_top5"] = results["rcd"][:5]
    verdict["metrics"] = {m: (mrcbench(r, roots) if (r and roots) else None)
                          for m, r in results.items()}
    if errors:
        verdict["errors"] = errors

    # ★退出码必须把【方法基础设施异常】与【真·空排名】分开(审查 R2 MAJOR):
    #   exit 3 = 真空排名(方法跑通了但排不出根)→ 驱动器判"该类型密度没修好"→ 跳过剩余 reps。
    #   exit 4 = 方法异常/无数据(_cl_patched 环境炸 / RCD 崩 / pandas dtype 等)→ 与采集质量无关,
    #            驱动器必须【继续采完该类型剩余 reps】——绝不能让打分器的 bug 吃掉今晚采不回来的数据。
    #   若两者同时(一个方法崩、另一个真空排名),按 4 处理(infra 疑云优先,不轻易砍 reps)。
    if errors:
        verdict["status"] = "METHOD_ERROR"
        _emit(verdict, a.out)
        sys.exit(4)
    verdict["status"] = "EMPTY_RANKING" if any(empty.values()) else "OK"
    _emit(verdict, a.out)
    sys.exit(3 if any(empty.values()) else 0)


def _emit(v, out_path):
    line = json.dumps(v, ensure_ascii=False)
    print(line)
    if out_path:
        d = os.path.dirname(os.path.abspath(out_path))
        if d and not os.path.isdir(d):
            os.makedirs(d, exist_ok=True)
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


if __name__ == "__main__":
    main()
