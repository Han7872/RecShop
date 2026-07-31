#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dprobe_emergence.py —— D 档【自然涌现率】卡口探针

回答一个问题(CROSSLAYER-DESIGN-2026-07-21.md L15 定的前置):
    **只注 infra 故障、不碰 agent 内容层时, agent 的输出到底变不变?**

  - 若 agent 老实报错(recommended_product=unknown / 文本明说工具失败) → **两层解耦**,
    infra 故障不会伪装成 agent 语义故障 → 跨层叠加(C/D 档)的论点不成立, 应放弃叠加。
  - 若 agent **看起来正常但内容错了**(编造 ASIN / 高 confidence 配空数据 / 静默换推荐)
    → **误归因区间成立**, 跨层数据集有真空白可填。

★注入面只有三处(设计稿 §4 因果链表已被实证推翻: rec-agent **不调 catalog**)。
  实测 `services/recommendation_agent/agents/tools.py` 全文件唯一 HTTP 下游 = `SASREC_API_URL`
  (`/recommend` + `/health`), 另加 DeepSeek(langchain_openai)。故本探针打 **sasrec**。

★★读数前必读的偏置(必须随结果一起报)
  当前 K8S 镜像 `recweb-rec-agent:latest` 的 tools.py **没有 `_filter_real_title`**(grep 0 命中),
  且 pod 内**没有** `electronics.inter` → 候选未过滤占位符、标题全是"未知商品"。
  而 REDESIGN_v2 实证过"base agent 面对 unknown 更易自发编造" ⇒
  **本探针测到的涌现率是【偏高的上界】**。上界≈0 可直接否掉跨层论点(这是本探针最有价值的用法);
  上界>0 则**不能**直接当结论, 需在修好镜像(重建 + 挂 electronics.inter)后复测。

用法(前置: kubectl 在 PATH, port-forward rec-agent 5001 + sasrec 8200 已起):
    python dprobe_emergence.py --surface sasrec_down      --reps 5
    python dprobe_emergence.py --surface sasrec_slow_hard --reps 5
    python dprobe_emergence.py --surface sasrec_slow_soft --reps 5   # 阴性对照
    python dprobe_emergence.py --surface all --reps 5

产物: datasets/_dprobe/<surface>/{baseline,during,post}_r<i>.json + summary.json
判定**不在本脚本内做**(收原始、离线判), 见同目录 dprobe_judge.py。
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
NS = "recweb-chaos"
KUBECTL = os.environ.get(
    "KUBECTL", r"kubectl")
CARRIER_POOL = os.path.join(REPO, "scripts", "chaos", "agentfault",
                            "assets", "carrier_pool.json")
OUT_ROOT = os.path.join(REPO, "datasets", "_dprobe")

RECAGENT_URL = "http://127.0.0.1:5001/recommend"
SASREC_HEALTH = "http://127.0.0.1:8200/health"
PROBE_TIMEOUT_S = 240      # agent 端到端本身 ~40s, 故障期 +30s 工具超时 → 给足
TOP_K = 5

# ---- 三个注入面(全部只打 sasrec, 逐字沿用 chaos_k8s_runner 的 CRD 体例) ----
#   slow_soft 是【阴性对照】: 延迟低于工具 30s 超时 → 工具正常返回, 只有时延变化。
#   若 slow_soft 也测出"涌现", 说明测到的是 LLM 本身的不确定性而非故障效应。
SURFACES = {
    "sasrec_down":      dict(kind="PodChaos",     crd="dprobe-podfail-sasrec",
                             note="pod-failure → requests.ConnectionError → 工具返回『错误: 无法连接到推荐服务』"),
    "sasrec_slow_hard": dict(kind="NetworkChaos", crd="dprobe-delay-hard-sasrec", latency_ms=35000,
                             note="延迟 35s > 工具 timeout=30s → requests.Timeout → 工具返回『错误: 推荐服务响应超时』"),
    # ★2000ms 不是 8000ms: netem 按【每个包】加延迟, 一次 HTTP 要多个 RTT(握手+请求+响应),
    #   实际 e2e 增量 = latency × RTT 数(runner 原话"强度按 RTT 累积")。8s × 数个 RTT 会顶穿
    #   工具的 timeout=30s, 就不再是"工具成功"的阴性对照了。
    "sasrec_slow_soft": dict(kind="NetworkChaos", crd="dprobe-delay-soft-sasrec", latency_ms=2000,
                             note="★阴性对照: 延迟 2s(累积后仍 < 工具 30s timeout) → 工具正常返回真实候选, 只有时延变化"),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _kubectl(args, stdin=None, timeout=60):
    p = subprocess.run([KUBECTL] + args, input=stdin, capture_output=True,
                       text=True, encoding="utf-8", errors="ignore", timeout=timeout)
    return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()


def crd_yaml(surface: str) -> str:
    s = SURFACES[surface]
    if s["kind"] == "PodChaos":
        return (f"apiVersion: chaos-mesh.org/v1alpha1\n"
                f"kind: PodChaos\n"
                f"metadata:\n  name: {s['crd']}\n  namespace: {NS}\n"
                f"spec:\n  action: pod-failure\n  mode: all\n"
                f"  selector:\n    namespaces: [{NS}]\n"
                f"    labelSelectors:\n      app: sasrec\n"
                f"  duration: \"900s\"\n")
    return (f"apiVersion: chaos-mesh.org/v1alpha1\n"
            f"kind: NetworkChaos\n"
            f"metadata:\n  name: {s['crd']}\n  namespace: {NS}\n"
            f"spec:\n  action: delay\n  mode: all\n"
            f"  selector:\n    namespaces: [{NS}]\n"
            f"    labelSelectors:\n      app: sasrec\n"
            f"  direction: to\n"
            f"  delay:\n    latency: \"{s['latency_ms']}ms\"\n"
            f"    jitter: \"0ms\"\n    correlation: \"0\"\n"
            f"  duration: \"900s\"\n")


_RECAGENT_POD = None


def recagent_pod():
    global _RECAGENT_POD
    if _RECAGENT_POD is None:
        rc, so, se = _kubectl(["get", "pods", "-n", NS, "-l", "app=recommendation_agent",
                               "-o", "jsonpath={.items[0].metadata.name}"])
        _RECAGENT_POD = so.strip()
    return _RECAGENT_POD


def sasrec_health(timeout=6):
    """返回 (ok: bool, ms: float) —— ★必须【从 rec-agent pod 内部】探。

    踩坑实证(2026-07-27 冒烟): 用宿主 `kubectl port-forward svc/sasrec 8200` 探,
    8s netem 注进去了却完全测不到(恒 5-29ms)。原因: port-forward 走 API server → kubelet →
    直接进 pod netns 的 **loopback**, 而 netem 挂在 **eth0** 上 ⇒ **port-forward 流量绕过 netem**。
    而真正要测的 rec-agent → sasrec 是 pod 间走 eth0 的, 注入对它有效。
    ⇒ 确认探针必须走与被测路径【同一条链路】, 否则会把生效的注入误判成没生效。
    """
    pod = recagent_pod()
    if not pod:
        return False, 0.0
    try:
        rc, so, se = _kubectl(
            ["exec", "-n", NS, pod, "--", "curl", "-s", "-o", "/dev/null",
             "-w", "%{http_code} %{time_total}", "--max-time", str(timeout),
             "http://sasrec:8200/health"],
            timeout=timeout + 25)
    except Exception:
        return False, timeout * 1000.0
    parts = (so or "").split()
    if len(parts) != 2:
        return False, timeout * 1000.0
    try:
        code, secs = int(parts[0]), float(parts[1])
    except Exception:
        return False, timeout * 1000.0
    return (code == 200), secs * 1000.0


def probe_once(seq, top_k=TOP_K):
    """打一发 rec-agent /recommend。返回 dict(原样收, 不做判定)。"""
    body = json.dumps({"item_sequence": seq, "top_k": top_k}).encode()
    req = urllib.request.Request(RECAGENT_URL, data=body,
                                 headers={"Content-Type": "application/json"},
                                 method="POST")
    t0 = time.time()
    rec = {"t_start": _now(), "http_status": None, "e2e_ms": None,
           "resp": None, "raw_len": None, "driver_error": None}
    try:
        with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT_S) as r:
            raw = r.read().decode("utf-8", "ignore")
            rec["http_status"] = r.status
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "ignore")
        rec["http_status"] = e.code
    except Exception as e:
        raw, rec["driver_error"] = "", f"{type(e).__name__}: {e}"
    rec["e2e_ms"] = round((time.time() - t0) * 1000.0, 1)
    rec["raw_len"] = len(raw)
    rec["t_end"] = _now()
    if raw:
        try:
            rec["resp"] = json.loads(raw)
        except Exception:
            rec["resp"] = None
            rec["raw_unparsed"] = raw[:4000]
    return rec


def _brief(rec):
    """一行速览(仅供 stdout, 不是判定)。"""
    r = (rec.get("resp") or {}).get("recommendation") or {}
    return (f"http={rec['http_status']} e2e={rec['e2e_ms']}ms "
            f"prod={r.get('recommended_product')!r} conf={r.get('confidence')!r} "
            f"bytes={rec['raw_len']}")


def wait_effect(surface, want_broken: bool, max_wait=180):
    """等注入真正生效(或真正恢复)。用 sasrec /health 直接观测, 不猜。
    ★PodChaos 的 pause-swap 会晚 10-20s 落地(黑板 G2ext Phase A 实证), 必须等确认再采。"""
    s = SURFACES[surface]
    soft = (surface == "sasrec_slow_soft")
    t0 = time.time()
    hits = 0
    while time.time() - t0 < max_wait:
        ok, ms = sasrec_health(timeout=12)
        # 阈值按【pod 内实测 baseline ~5-30ms】定(不是 port-forward 的口径)
        if surface == "sasrec_down":
            broken = (not ok)
        elif soft:
            broken = ok and ms >= 1500        # 2s 延迟 → 仍成功但明显变慢
        else:
            broken = (not ok) or ms >= 12000  # 35s 延迟 → health 12s 超时即失败
        hit = (broken == want_broken)
        hits = hits + 1 if hit else 0
        print(f"    [wait_effect] want_broken={want_broken} ok={ok} ms={ms:.0f} "
              f"streak={hits} t={time.time()-t0:.0f}s", flush=True)
        if hits >= 2:
            return True
        time.sleep(5)
    return False


def run_surface(surface, reps, carrier):
    s = SURFACES[surface]
    out_dir = os.path.join(OUT_ROOT, surface)
    os.makedirs(out_dir, exist_ok=True)
    summary = {"surface": surface, "note": s["note"], "reps": reps,
               "carrier_seq_id": carrier["seq_id"], "carrier_history": carrier["history"],
               "image_bias_warning": (
                   "K8S rec-agent 镜像缺 _filter_real_title 且 pod 内无 electronics.inter → "
                   "候选含占位符、标题全『未知商品』; REDESIGN_v2 实证 base agent 面对 unknown 更易编造 "
                   "⇒ 本次涌现率是【偏高的上界】"),
               "phases": {}}

    def do_phase(name, n):
        print(f"  --- phase {name} ({n} reps) ---", flush=True)
        rows = []
        for i in range(1, n + 1):
            rec = probe_once(carrier["history"])
            with open(os.path.join(out_dir, f"{name}_r{i}.json"), "w",
                      encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=1)
            print(f"    r{i}: {_brief(rec)}", flush=True)
            rows.append({"rep": i, "http_status": rec["http_status"],
                         "e2e_ms": rec["e2e_ms"], "raw_len": rec["raw_len"],
                         "driver_error": rec["driver_error"]})
        summary["phases"][name] = rows

    print(f"\n=== surface={surface} :: {s['note']} ===", flush=True)

    # 0) 清残留 + 确认起点干净
    _kubectl(["delete", "podchaos,networkchaos", "--all", "-n", NS, "--ignore-not-found"])
    time.sleep(5)
    ok, ms = sasrec_health()
    summary["pre_sasrec_health"] = {"ok": ok, "ms": round(ms, 1)}
    if not ok:
        print(f"  [FATAL] 起点 sasrec 就不健康(ok={ok}) — 中止", flush=True)
        summary["aborted"] = "sasrec_unhealthy_at_start"
        return summary

    # 1) baseline
    do_phase("baseline", reps)

    # 2) 注入
    rc, so, se = _kubectl(["apply", "-f", "-"], stdin=crd_yaml(surface))
    summary["inject"] = {"rc": rc, "out": so or se, "at": _now()}
    print(f"  [inject] rc={rc} {so or se}", flush=True)
    if rc != 0:
        summary["aborted"] = "crd_apply_failed"
        return summary
    summary["effect_confirmed"] = wait_effect(surface, want_broken=True)
    print(f"  [inject] effect_confirmed={summary['effect_confirmed']}", flush=True)

    # 3) during
    do_phase("during", reps)

    # 4) 恢复
    rc, so, se = _kubectl(["delete", "podchaos,networkchaos", "--all",
                           "-n", NS, "--ignore-not-found"])
    summary["recover"] = {"rc": rc, "at": _now()}
    # ★sasrec 恢复要重载 9.2GB pickle, 给足 10 分钟(pod-failure 面尤其)
    summary["recover_confirmed"] = wait_effect(surface, want_broken=False, max_wait=600)
    print(f"  [recover] confirmed={summary['recover_confirmed']}", flush=True)

    # 5) post(确认不是永久损坏)
    do_phase("post", reps)

    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1)
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--surface", default="all",
                    choices=list(SURFACES) + ["all"])
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--carrier", type=int, default=0, help="carrier_pool sequences 索引")
    a = ap.parse_args()

    seqs = json.load(open(CARRIER_POOL, encoding="utf-8"))["sequences"]
    carrier = seqs[a.carrier]
    print(f"carrier seq_id={carrier['seq_id']} hist={carrier['history']}", flush=True)

    # 前置只读核查(不代起, 缺则退出)
    ok, ms = sasrec_health()
    if not ok:
        sys.exit("FATAL: sasrec :8200 不通 → kubectl port-forward svc/sasrec 8200:8200 -n recweb-chaos")
    r = probe_once(carrier["history"][:1] or ["B000PGJ7SA"], top_k=1)
    if r["http_status"] != 200:
        sys.exit(f"FATAL: rec-agent :5001 不通(http={r['http_status']}) → port-forward svc/rec-agent 5001:5001")
    print(f"[preflight] sasrec OK({ms:.0f}ms) · rec-agent OK", flush=True)

    targets = list(SURFACES) if a.surface == "all" else [a.surface]
    allsum = {}
    for t in targets:
        allsum[t] = run_surface(t, a.reps, carrier)
    os.makedirs(OUT_ROOT, exist_ok=True)
    with open(os.path.join(OUT_ROOT, "ALL_SUMMARY.json"), "w", encoding="utf-8") as f:
        json.dump(allsum, f, ensure_ascii=False, indent=1)
    print("\n=== 全部完成 → datasets/_dprobe/ ===", flush=True)


if __name__ == "__main__":
    main()
