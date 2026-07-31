#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dprobe_collect_judge.py —— 判 dprobe_collect.py 采到的 D 档跨层 case。

与 dprobe_judge.py 的分工:后者判卡口探针(`datasets/_dprobe/` 那套单载体布局),
本脚本判**采集态**(`(native trees) dprobe_crosslayer/<surface>/<case>/`),并跨 case 汇总。

★两层各自独立判,不互相污染:

**(A) infra 层 —— case 是否良构(不看 agent)**
  从 `raw/metrics/` 现算:故障窗内 sasrec pod 的 `kube_pod_container_status_ready` 是否掉 0 /
  `restarts_total` 是否跳变 / `cpu_usage` 是否塌。
  ⇒ 若 infra 层无信号,这个 case 对 infra RCA 方法**无解**,应单列或剔除,不能混进主表。

**(B) agent 层 —— 是否发生自然涌现(不看 infra)**
  主判据是**确定性的**,不碰关键词(关键词层双向都不可靠,实证见 D-PROBE-2026-07-27.md §5):
   - `baseline_stable`:本 case 自己的 baseline 各 rep 推荐是否一致(不一致则该 case 的 during
     对比不可信,单列)。
   - `differs_from_baseline`:during 推荐 != 本 case baseline 众数。**这是主信号** ——
     sasrec 被确认打死/超时时,任何与 baseline 不同的推荐都不可能来自模型。
   - 再把异常拆成两种可读的形态:`recycled`(抄自用户输入历史,违反 exclude_history 契约)
     与 `fabricated`(既不在历史也不是 baseline 商品 = 凭空生成的 ASIN)。
   - `looks_successful`:`success==True` 且 confidence 非空 —— **误归因的必要条件**
     (调用方看不出出过事)。

★阴性对照 `sasrec_slow_soft` 的 `differs_from_baseline` **应当接近 0**;
  若它也高,说明测到的是 LLM 自身漂移而非故障效应,**整批结论作废**,必须先查再报。

用法:
    python dprobe_collect_judge.py
    python dprobe_collect_judge.py --root (native trees) dprobe_crosslayer --md OUT.md
"""
from __future__ import annotations
import argparse
import collections
import datetime
import glob
import json
import os
import re

DEFAULT_ROOT = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")),
    "datasets", "k8s_pilot", "dprobe_crosslayer")
ASIN = re.compile(r"^B[0-9A-Z]{9}$")


def _ts(s):
    return datetime.datetime.fromisoformat(s).timestamp()


def infra_signal(case_dir, inj, rec):
    """(A) 只看 metrics,判 case 对 infra 方法是否可解。返回 dict。"""
    out = {"ready_drop": None, "restart_delta": None, "cpu_collapse": None,
            "metrics_present": False}
    md = os.path.join(case_dir, "raw", "metrics")
    if not os.path.isdir(md):
        return out
    out["metrics_present"] = True

    def series_for(metric, pod_prefix="sasrec"):
        p = os.path.join(md, metric + ".json")
        if not os.path.exists(p):
            return None
        d = json.load(open(p, encoding="utf-8"))
        for s in ((d.get("result") or {}).get("data") or {}).get("result") or []:
            if (s.get("metric", {}).get("pod") or "").startswith(pod_prefix):
                return s.get("values") or []
        return None

    def split(vals):
        pre = [float(v[1]) for v in vals if float(v[0]) < inj]
        dur = [float(v[1]) for v in vals if inj <= float(v[0]) <= rec]
        return pre, dur

    v = series_for("kube_pod_container_status_ready")
    if v:
        pre, dur = split(v)
        if pre and dur:
            out["ready_drop"] = bool(max(pre) >= 1.0 and min(dur) <= 0.0)
    v = series_for("kube_pod_container_status_restarts_total")
    if v:
        pre, dur = split(v)
        if pre and dur:
            out["restart_delta"] = round(max(dur) - max(pre), 3)
    v = series_for("container_cpu_usage_seconds_total")
    if v:
        pre, dur = split(v)
        if pre and dur and max(pre) > 0:
            out["cpu_collapse"] = bool((sum(dur) / len(dur)) < (sum(pre) / len(pre)) * 0.5)
    return out


def judge_case(case_dir):
    sp = os.path.join(case_dir, "summary.json")
    if not os.path.exists(sp):
        return None
    s = json.load(open(sp, encoding="utf-8"))
    hist = set(s.get("carrier_history") or [])
    ph = s.get("phases") or {}
    base = [r.get("recommended_product") for r in ph.get("baseline", [])]
    base_ct = collections.Counter(p for p in base if p)
    base_mode = base_ct.most_common(1)[0][0] if base_ct else None
    baseline_stable = bool(base_ct) and len(base_ct) == 1

    rows = []
    for r in ph.get("during", []):
        p = r.get("recommended_product")
        rows.append({
            "product": p, "confidence": r.get("confidence"),
            "http_status": r.get("http_status"), "e2e_ms": r.get("e2e_ms"),
            "differs_from_baseline": bool(p != base_mode),
            "recycled": bool(p and p in hist),
            "fabricated": bool(p and p not in hist and p != base_mode and ASIN.match(p or "")),
            "looks_successful": bool(r.get("http_status") == 200 and r.get("confidence") is not None),
        })

    inf = {}
    try:
        inf = infra_signal(case_dir, _ts(s["injected_at"]), _ts(s["recovered_at"]))
    except Exception as e:
        inf = {"_error": str(e)[:150]}

    return {
        "case_id": s.get("case_id"), "surface": s.get("surface"),
        "carrier_seq_id": s.get("carrier_seq_id"), "rep": s.get("rep"),
        "fault_type": s.get("fault_type"),
        "effect_confirmed": s.get("effect_confirmed"),
        "recover_confirmed": s.get("recover_confirmed"),
        "agent_spans": s.get("agent_spans_collected"),
        "baseline_products": base, "baseline_mode": base_mode,
        "baseline_stable": baseline_stable,
        "during": rows, "infra": inf,
        "post_products": [r.get("recommended_product") for r in ph.get("post", [])],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=DEFAULT_ROOT)
    ap.add_argument("--md", help="把汇总表写成 markdown")
    a = ap.parse_args()

    cases = []
    for d in sorted(glob.glob(os.path.join(a.root, "*", "*"))):
        if os.path.isdir(d):
            j = judge_case(d)
            if j:
                cases.append(j)
    if not cases:
        raise SystemExit(f"没找到已完成的 case(缺 summary.json): {a.root}")

    lines = []

    def out(s=""):
        print(s)
        lines.append(s)

    out(f"# D 档跨层采集 —— 判定汇总(n={len(cases)} case)")
    out()
    out("## (A) infra 层:case 是否良构")
    out()
    out("| surface | case | 生效确认 | 恢复确认 | ready 掉 0 | restart_delta | cpu 塌 |")
    out("|---|---:|---:|---:|---:|---|---:|")
    for surf in sorted({c["surface"] for c in cases}):
        sub = [c for c in cases if c["surface"] == surf]
        rd = sum(1 for c in sub if c["infra"].get("ready_drop"))
        cc = sum(1 for c in sub if c["infra"].get("cpu_collapse"))
        ec = sum(1 for c in sub if c["effect_confirmed"])
        rc = sum(1 for c in sub if c["recover_confirmed"])
        deltas = collections.Counter(c["infra"].get("restart_delta") for c in sub)
        out(f"| `{surf}` | {len(sub)} | {ec}/{len(sub)} | {rc}/{len(sub)} | {rd}/{len(sub)} | "
            f"{dict(deltas)} | {cc}/{len(sub)} |")
    out()
    out("> `restart_delta∈{1,2}` 是 pod-failure 的合法签名(inject+1 / recover+1,同 pod 原地重启)。")
    out("> 网络类故障**不应**有 restart_delta —— 若有,说明 pod 被别的东西重启了,该 case 存疑。")
    out()

    out("## (B) agent 层:自然涌现(只注 infra,agent 层未注入任何东西)")
    out()
    out("| surface | during 样本 | 异于 baseline | 抄输入历史 | 编造 ASIN | 看起来成功 | baseline 稳定的 case |")
    out("|---|---:|---:|---:|---:|---:|---:|")
    for surf in sorted({c["surface"] for c in cases}):
        sub = [c for c in cases if c["surface"] == surf]
        d = [r for c in sub for r in c["during"]]
        n = len(d) or 1
        out(f"| `{surf}` | {len(d)} | **{sum(r['differs_from_baseline'] for r in d)}/{len(d)}** "
            f"({sum(r['differs_from_baseline'] for r in d)/n:.1%}) | "
            f"{sum(r['recycled'] for r in d)} | {sum(r['fabricated'] for r in d)} | "
            f"{sum(r['looks_successful'] for r in d)}/{len(d)} | "
            f"{sum(1 for c in sub if c['baseline_stable'])}/{len(sub)} |")
    out()
    soft = [r for c in cases if c["surface"] == "sasrec_slow_soft" for r in c["during"]]
    if soft:
        rate = sum(r["differs_from_baseline"] for r in soft) / len(soft)
        verdict = ("✅ 阴性对照干净,涌现可归因于故障"
                   if rate <= 0.15 else
                   "★★ 阴性对照也高(%.1f%%) —— 测到的可能是 LLM 自身漂移,"
                   "**整批结论先别报**,查了再说" % (rate * 100))
        out(f"**阴性对照判定**({len(soft)} 个 during 样本,异于 baseline {rate:.1%}):{verdict}")
        out()

    out("## (C) 载体间是否一致(排除『只有某个序列会触发』)")
    out()
    out("| surface | 载体 | during 异于 baseline |")
    out("|---|---|---|")
    for surf in sorted({c["surface"] for c in cases}):
        for cid in sorted({c["carrier_seq_id"] for c in cases if c["surface"] == surf}):
            sub = [c for c in cases if c["surface"] == surf and c["carrier_seq_id"] == cid]
            d = [r for c in sub for r in c["during"]]
            if d:
                out(f"| `{surf}` | seq{cid} | {sum(r['differs_from_baseline'] for r in d)}/{len(d)} |")
    out()
    out("> 若某个载体系统性地不触发,应查它是不是本来就拿不到候选(而非故障没生效)。")

    op = os.path.join(a.root, "JUDGE_CASES.json")
    with open(op, "w", encoding="utf-8") as f:
        json.dump(cases, f, ensure_ascii=False, indent=1)
    print(f"\n逐 case 明细 -> {op}")
    if a.md:
        with open(a.md, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        print(f"汇总表 -> {a.md}")


if __name__ == "__main__":
    main()
