#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dprobe_annotate_manifestation.py —— 采后给 D 档 case 的 GT 补『注入机制 ≠ 观测表现』注记。

## 为什么需要这个(2026-07-27 中途判定发现)

`sasrec_slow_hard` 注入的是 **NetworkChaos delay 35s**,GT 里 `fault_type` 记的是
`dependency_latency`(=注入了什么)。但**指标层看到的不是"慢",是"不可用"**:

  sasrec 的 readinessProbe = httpGet /health, timeoutSeconds=5, periodSeconds=15,
  failureThreshold=3  ⇒  35s 延迟必使探针超时 → 3×15 = **45s 后 pod 转 NotReady**。
  实测 `kube_pod_container_status_ready` 在注入后 **+43s** 掉 0(与理论吻合),
  8/8 个 slow_hard case 全部命中;livenessProbe = 5×30 = 150s 才重启,故障窗 140s 通常不到,
  但窗更长的 case 会踩到 → **2/8 出现 restart_delta=1**。

⇒ 一个只读 metric 的 RCA 方法会把它判成"sasrec 不可用",而不是"sasrec 延迟"。
   **若只记 `dependency_latency` 就是在误导评测者。** 这与 runner 里既有的诚实警示同源:
   「高 loss 会静默退化成【可用性故障】却被标 network_loss → 强度必须先标定」。

## 本脚本做什么(只增字段,不改既有字段)

给每个 case 的 `groundtruth.json` 增补:

    "_manifestation": {
        "injected_mechanism": "network_delay_35s_on_sasrec_egress",   # 注入了什么
        "metric_layer_manifestation": "unavailability",               # 指标层看起来是什么
        "why": "...readinessProbe 超时 → NotReady 的推导与实测...",
        "ready_drop_observed": true, "ready_drop_after_inject_s": 43.0,
        "restart_delta_in_window": 0,
        "eval_note": "按指标层评测时本 case 与 service_unavailable 不可分, 报分须单列或合并声明"
    }

**`fault_type` 原样不动**(它如实记录了"注入了什么"),新增字段记录"观测到什么"。
两者都保留 = 评测者可以自己选口径,而不是被单一标签误导。

用法(采集完成后跑一次;幂等):
    python dprobe_annotate_manifestation.py
    python dprobe_annotate_manifestation.py --root <dir> --dry-run
"""
from __future__ import annotations
import argparse
import datetime
import glob
import json
import os

DEFAULT_ROOT = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")),
    "datasets", "k8s_pilot", "dprobe_crosslayer")

# 注入机制的字面描述(与 dprobe_collect.SURFACES 对应)
MECHANISM = {
    "sasrec_down": "podchaos_pod_failure_on_sasrec",
    "sasrec_slow_hard": "networkchaos_delay_35s_on_sasrec_egress",
    "sasrec_slow_soft": "networkchaos_delay_2s_on_sasrec_egress",
}
PROBE_FACT = ("sasrec readinessProbe = httpGet /health, timeoutSeconds=5, periodSeconds=15, "
              "failureThreshold=3 ⇒ 延迟 >5s 必使探针超时, 3×15=45s 后转 NotReady; "
              "livenessProbe = timeout 5s × period 30s × failureThreshold 5 = 150s 才重启。")


def ts(s):
    return datetime.datetime.fromisoformat(s).timestamp()


def read_series(case_dir, metric, pod_prefix="sasrec"):
    p = os.path.join(case_dir, "raw", "metrics", metric + ".json")
    if not os.path.exists(p):
        return None
    d = json.load(open(p, encoding="utf-8"))
    for s in ((d.get("result") or {}).get("data") or {}).get("result") or []:
        if (s.get("metric", {}).get("pod") or "").startswith(pod_prefix):
            return [(float(t), float(v)) for t, v in (s.get("values") or [])]
    return None


def analyse(case_dir, summ):
    inj, rec = ts(summ["injected_at"]), ts(summ["recovered_at"])
    ready = read_series(case_dir, "kube_pod_container_status_ready")
    restarts = read_series(case_dir, "kube_pod_container_status_restarts_total")

    drop_at = None
    drop = False
    if ready:
        prev = None
        for t, v in ready:
            if prev is not None and prev >= 1.0 and v <= 0.0 and inj <= t <= rec + 5:
                drop, drop_at = True, round(t - inj, 1)
                break
            prev = v
    delta = None
    if restarts:
        pre = [v for t, v in restarts if t < inj]
        dur = [v for t, v in restarts if inj <= t <= rec]
        if pre and dur:
            delta = round(max(dur) - max(pre), 3)

    surface = summ["surface"]
    if surface == "sasrec_down":
        manif, note = "unavailability", ("注入即可用性故障, 指标层表现与之一致(ready→0), "
                                         "标签与表现无歧义。")
    elif surface == "sasrec_slow_hard":
        if drop:
            manif = "unavailability"
            note = ("★注入的是延迟, 但指标层表现为【不可用】—— 按指标层评测时本 case 与 "
                    "service_unavailable **不可分**, 报分须单列或合并声明; "
                    "把它当『延迟型故障』的定位样本会高估方法对延迟的敏感性。")
        else:
            manif, note = "latency", "延迟未持续到触发探针失败, 指标层仍表现为延迟。"
    else:                                    # slow_soft(阴性对照)
        if drop:
            manif = "unavailability"
            note = ("★阴性对照本不该掉 ready —— 出现即说明 2s 延迟也顶穿了探针, "
                    "该 case 不能再当『纯延迟、服务仍可用』的对照, 必须剔除或重标。")
        else:
            manif, note = "latency", "指标层表现为延迟, 服务保持 Ready —— 符合阴性对照设计。"

    return {
        "injected_mechanism": MECHANISM.get(surface, surface),
        "metric_layer_manifestation": manif,
        "why": PROBE_FACT,
        "ready_drop_observed": bool(drop),
        "ready_drop_after_inject_s": drop_at,
        "restart_delta_in_window": delta,
        "fault_window_seconds": round(rec - inj, 1),
        "eval_note": note,
        "_annotated_by": "dprobe_annotate_manifestation.py",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=DEFAULT_ROOT)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    n = 0
    flagged = []
    for d in sorted(glob.glob(os.path.join(a.root, "*", "*"))):
        sp, gp = os.path.join(d, "summary.json"), os.path.join(d, "groundtruth.json")
        if not (os.path.exists(sp) and os.path.exists(gp)):
            continue
        summ = json.load(open(sp, encoding="utf-8"))
        gt = json.load(open(gp, encoding="utf-8"))
        m = analyse(d, summ)
        gt["_manifestation"] = m
        if not a.dry_run:
            with open(gp, "w", encoding="utf-8") as f:
                json.dump(gt, f, ensure_ascii=False, indent=1)
        n += 1
        mark = ""
        if summ["surface"] == "sasrec_slow_hard" and m["ready_drop_observed"]:
            mark = "  ← 延迟退化成不可用"
            flagged.append(summ["case_id"])
        if summ["surface"] == "sasrec_slow_soft" and m["ready_drop_observed"]:
            mark = "  ←★阴性对照被污染, 需剔除"
            flagged.append(summ["case_id"])
        print(f"  {summ['case_id']:38s} manif={m['metric_layer_manifestation']:15s} "
              f"drop@{m['ready_drop_after_inject_s']} restartΔ={m['restart_delta_in_window']}{mark}")

    print(f"\n{'(dry-run) ' if a.dry_run else ''}已注记 {n} 个 case")
    print(f"其中需在报分时单列/剔除的:{len(flagged)} 个")
    if flagged:
        for c in flagged[:12]:
            print(f"    {c}")
        if len(flagged) > 12:
            print(f"    ... 另 {len(flagged)-12} 个")


if __name__ == "__main__":
    main()
