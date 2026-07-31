#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dprobe_collect.py —— D 档跨层采集器(只注 infra 故障, 同时收 infra + agent 两层遥测)

与 dprobe_emergence.py 的分工:
  - dprobe_emergence.py = **卡口探针**(单载体 n=5, 只回答"信号存不存在", 结论见
    `(project docs)/dprobe/D-PROBE-2026-07-27.md`: 工具失败面 10/10 涌现、工具成功面 0/5)。
  - 本脚本         = **数据集采集器**(多载体多 rep, 每 case 产出三层产物 + GT), 无人值守。

每个 case = 一个注入窗, 目录结构对齐 k8s_pilot 的体例:
    <case>/groundtruth.json          root_cause_services / fault_type / 三段窗口
    <case>/metadata.json             载体、surface、镜像、口径、生效确认
    <case>/raw/agent_responses/      每次 /recommend 的完整响应(含 4 agent 全文 + trace_id)
    <case>/raw/agent_spans/spans.jsonl   本 case 期间新增的 agent span(observe-only 内容层)
    <case>/raw/metrics/*.json        Prometheus query_range(整 case 窗, 逐服务资源指标)

★三条环境铁律(全部是实证踩出来的, 改动前先读)
  1. **走 kubectl proxy 8001, 不用 port-forward。** port-forward 绑定具体 pod, pod 一换就
     "failed to find sandbox" 整个死掉(2026-07-27 实证: observe 补丁 rollout 后立刻断)。
     无人值守必须用 API server 代理。
  2. **注入生效/恢复确认必须从 pod 内部探。** 宿主 port-forward 走 pod netns 的 loopback,
     **绕过挂在 eth0 的 netem** —— 2026-07-27 实证: 8s 延迟注进去了却恒测 5-29ms, 险些误判没生效。
  3. **回收 span 的 kubectl exec 必须带 `MSYS2_ARG_CONV_EXCL='*'`**, 否则 Git Bash 的 MSYS 会把
     `/agentfault-data/...` 改写成 Windows 路径, cat 必失败(G1 曾因此静默丢 10 个 case 的轨迹)。
     本脚本用 subprocess 直调 kubectl(不过 shell), 天然规避; 但人工复跑命令时务必带。

前置(不代起, 缺则 FATAL):
  - 集群 + Chaos Mesh + Docker OTel 栈 + `kubectl proxy --port=8001`
    (★Prometheus 的 cadvisor / kube-state-metrics 两个 target 就是经 host.docker.internal:8001 抓的,
      proxy 不起 = 两个 target down = 采到的 metrics 全空)
  - rec-agent 已切 observe-only 变体:
      powershell -File scripts/chaos/agentfault/k8s/patch_recagent_observe.ps1
    采完还原: restore_recagent_stock.ps1

用法:
    python dprobe_collect.py --carriers 8 --reps 2            # 3 surface × 8 载体 × 2 rep = 48 case
    python dprobe_collect.py --carriers 1 --reps 1 --surfaces sasrec_down   # smoke
已存在且 summary.json 完整的 case 目录会被跳过(resume 安全)。
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
NS = "recweb-chaos"
KUBECTL = os.environ.get(
    "KUBECTL", r"kubectl")
PROXY = "http://127.0.0.1:8001"
RECAGENT_URL = f"{PROXY}/api/v1/namespaces/{NS}/services/rec-agent:5001/proxy/recommend"
PROM = os.environ.get("PROM_URL", "http://localhost:9090")
CARRIER_POOL = os.path.join(REPO, "scripts", "chaos", "agentfault",
                            "assets", "carrier_pool.json")
OUT_ROOT = os.path.join(REPO, "datasets", "k8s_pilot", "dprobe_crosslayer")
SPAN_PATH = "/agentfault-data/spans.jsonl"
PROBE_TIMEOUT_S = 300
TOP_K = 5

# surface 定义与卡口探针逐字一致(见 dprobe_emergence.SURFACES), 便于两边结果对读。
SURFACES = {
    "sasrec_down": dict(
        kind="PodChaos", crd="dprobe-podfail-sasrec",
        fault_type="service_unavailable",
        note="PodChaos pod-failure on sasrec -> 工具 requests.ConnectionError"),
    "sasrec_slow_hard": dict(
        kind="NetworkChaos", crd="dprobe-delay-hard-sasrec", latency_ms=35000,
        fault_type="dependency_latency",
        note="NetworkChaos delay 35s(> 工具 timeout=30s) -> 工具 requests.Timeout"),
    "sasrec_slow_soft": dict(
        kind="NetworkChaos", crd="dprobe-delay-soft-sasrec", latency_ms=2000,
        fault_type="network_delay",
        note="★阴性对照: delay 2s, 工具仍成功返回真实候选 -> 预期 agent 层【无】故障"),
}
PHASES = [("baseline", 2), ("during", 3), ("post", 1)]


def now():
    return datetime.now(timezone.utc).isoformat()


def kc(args, timeout=90, stdin=None):
    p = subprocess.run([KUBECTL] + args, input=stdin, capture_output=True,
                       text=True, encoding="utf-8", errors="ignore", timeout=timeout)
    return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()


_POD = {"name": None}


def recagent_pod(force=False):
    """★每次注入前后都重新解析 —— pod 会因 rollout / pause-swap 换名, 缓存会指向死 pod。"""
    if force or not _POD["name"]:
        rc, so, _ = kc(["get", "pods", "-n", NS, "-l", "app=recommendation_agent",
                        "--field-selector=status.phase=Running",
                        "-o", "jsonpath={.items[0].metadata.name}"])
        _POD["name"] = so.strip()
    return _POD["name"]


def sasrec_health_from_pod(timeout=6):
    """从 rec-agent pod 内探 sasrec(与被测路径同一条链路, 见铁律 2)。返回 (ok, ms)。"""
    pod = recagent_pod()
    if not pod:
        return False, 0.0
    try:
        rc, so, _ = kc(["exec", "-n", NS, pod, "--", "curl", "-s", "-o", "/dev/null",
                        "-w", "%{http_code} %{time_total}", "--max-time", str(timeout),
                        "http://sasrec:8200/health"], timeout=timeout + 30)
    except Exception:
        return False, timeout * 1000.0
    parts = so.split()
    if len(parts) != 2:
        return False, timeout * 1000.0
    try:
        return int(parts[0]) == 200, float(parts[1]) * 1000.0
    except Exception:
        return False, timeout * 1000.0


def crd_yaml(surface):
    s = SURFACES[surface]
    if s["kind"] == "PodChaos":
        return (f"apiVersion: chaos-mesh.org/v1alpha1\nkind: PodChaos\n"
                f"metadata:\n  name: {s['crd']}\n  namespace: {NS}\n"
                f"spec:\n  action: pod-failure\n  mode: all\n"
                f"  selector:\n    namespaces: [{NS}]\n    labelSelectors:\n      app: sasrec\n"
                f"  duration: \"900s\"\n")
    return (f"apiVersion: chaos-mesh.org/v1alpha1\nkind: NetworkChaos\n"
            f"metadata:\n  name: {s['crd']}\n  namespace: {NS}\n"
            f"spec:\n  action: delay\n  mode: all\n"
            f"  selector:\n    namespaces: [{NS}]\n    labelSelectors:\n      app: sasrec\n"
            f"  direction: to\n  delay:\n    latency: \"{s['latency_ms']}ms\"\n"
            f"    jitter: \"0ms\"\n    correlation: \"0\"\n  duration: \"900s\"\n")


def wait_effect(surface, want_broken, max_wait=300):
    soft = surface == "sasrec_slow_soft"
    t0, hits = time.time(), 0
    while time.time() - t0 < max_wait:
        ok, ms = sasrec_health_from_pod(timeout=12)
        if surface == "sasrec_down":
            broken = not ok
        elif soft:
            broken = ok and ms >= 1500
        else:
            broken = (not ok) or ms >= 12000
        hits = hits + 1 if (broken == want_broken) else 0
        if hits >= 2:
            return True
        time.sleep(5)
    return False


def probe_once(seq, retries=1):
    body = json.dumps({"item_sequence": seq, "top_k": TOP_K}).encode()
    for attempt in range(retries + 1):
        rec = {"t_start": now(), "attempt": attempt}
        t0 = time.time()
        try:
            req = urllib.request.Request(
                RECAGENT_URL, data=body,
                headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT_S) as r:
                raw, rec["http_status"] = r.read().decode("utf-8", "ignore"), r.status
        except urllib.error.HTTPError as e:
            raw, rec["http_status"] = e.read().decode("utf-8", "ignore"), e.code
        except Exception as e:
            raw, rec["http_status"] = "", -1
            rec["driver_error"] = f"{type(e).__name__}: {e}"
        rec["e2e_ms"] = round((time.time() - t0) * 1000, 1)
        rec["t_end"], rec["raw_len"] = now(), len(raw)
        if raw:
            try:
                rec["resp"] = json.loads(raw)
            except Exception:
                rec["raw_unparsed"] = raw[:4000]
        if rec["http_status"] == 200:
            return rec
        # 驱动层失败(代理抖动等)才重试;业务层非 200 原样留档不重试
        if rec["http_status"] != -1 or attempt == retries:
            return rec
        time.sleep(8)
    return rec


def span_line_count():
    pod = recagent_pod(force=True)
    if not pod:
        return 0
    rc, so, _ = kc(["exec", "-n", NS, pod, "--", "sh", "-c",
                    f"wc -l < {SPAN_PATH}"], timeout=60)
    try:
        return int(so.strip())
    except Exception:
        return 0


def dump_spans_since(start_line, dest):
    """把本 case 期间新增的 span 落到 dest。★不用 shell -> 天然绕开 MSYS 路径改写(铁律 3)。"""
    pod = recagent_pod(force=True)
    if not pod:
        return 0
    rc, so, se = kc(["exec", "-n", NS, pod, "--", "sh", "-c",
                     f"tail -n +{start_line + 1} {SPAN_PATH}"], timeout=180)
    if rc != 0:
        return 0
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with open(dest, "w", encoding="utf-8") as f:
        f.write(so + ("\n" if so and not so.endswith("\n") else ""))
    return sum(1 for ln in so.splitlines() if ln.strip())


PROM_EXPRS = {
    "container_cpu_usage_seconds_total":
        'sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="%s"}[1m]))' % NS,
    "container_memory_working_set_bytes":
        'sum by (pod) (container_memory_working_set_bytes{namespace="%s"})' % NS,
    "container_cpu_cfs_throttled_seconds_total":
        'sum by (pod) (rate(container_cpu_cfs_throttled_seconds_total{namespace="%s"}[1m]))' % NS,
    "kube_pod_container_status_restarts_total":
        'sum by (pod) (kube_pod_container_status_restarts_total{namespace="%s"})' % NS,
    "kube_pod_container_status_ready":
        'sum by (pod) (kube_pod_container_status_ready{namespace="%s"})' % NS,
}


def prom_range(expr, t_start, t_end, step="5s"):
    url = (f"{PROM}/api/v1/query_range?query={urllib.parse.quote(expr)}"
           f"&start={t_start}&end={t_end}&step={step}")
    try:
        with urllib.request.urlopen(url, timeout=45) as r:
            return json.loads(r.read().decode("utf-8", "ignore"))
    except Exception as e:
        return {"status": "error", "_client_error": f"{type(e).__name__}: {e}"}


def collect_metrics(case_dir, t0_unix, t1_unix):
    d = os.path.join(case_dir, "raw", "metrics")
    os.makedirs(d, exist_ok=True)
    got = {}
    for name, expr in PROM_EXPRS.items():
        res = prom_range(expr, t0_unix, t1_unix)
        with open(os.path.join(d, f"{name}.json"), "w", encoding="utf-8") as f:
            json.dump({"expr": expr, "start": t0_unix, "end": t1_unix,
                       "step": "5s", "result": res}, f, ensure_ascii=False)
        got[name] = len((res.get("data") or {}).get("result") or [])
    return got


def run_case(surface, carrier, rep, out_root):
    s = SURFACES[surface]
    cid = f"{surface}__c{carrier['seq_id']}__r{rep}"
    cdir = os.path.join(out_root, surface, cid)
    done = os.path.join(cdir, "summary.json")
    if os.path.exists(done):
        print(f"[skip] {cid}(已完成)", flush=True)
        return "skipped"
    os.makedirs(os.path.join(cdir, "raw", "agent_responses"), exist_ok=True)

    print(f"\n=== {cid} :: {s['note']} ===", flush=True)
    kc(["delete", "podchaos,networkchaos", "--all", "-n", NS, "--ignore-not-found"])
    time.sleep(5)
    ok, _ = sasrec_health_from_pod()
    if not ok:
        raise RuntimeError("起点 sasrec 不健康, 跳过本 case")

    t0_unix = int(time.time()) - 30          # 前留 30s 让 baseline 指标进窗
    span0 = span_line_count()
    summ = {"case_id": cid, "surface": surface, "note": s["note"],
            "fault_type": s["fault_type"], "carrier_seq_id": carrier["seq_id"],
            "carrier_history": carrier["history"], "rep": rep,
            "span_start_line": span0, "t_start": now(), "phases": {}, "windows": {}}

    def phase(name, n):
        rows = []
        summ["windows"][name] = {"start": now()}
        for i in range(1, n + 1):
            rec = probe_once(carrier["history"])
            with open(os.path.join(cdir, "raw", "agent_responses", f"{name}_r{i}.json"),
                      "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=1)
            r = ((rec.get("resp") or {}).get("recommendation") or {})
            print(f"    [{name}] r{i} http={rec['http_status']} e2e={rec['e2e_ms']}ms "
                  f"prod={r.get('recommended_product')!r} conf={r.get('confidence')!r}", flush=True)
            rows.append({"rep": i, "http_status": rec["http_status"],
                         "e2e_ms": rec["e2e_ms"],
                         "trace_id": (rec.get("resp") or {}).get("trace_id"),
                         "recommended_product": r.get("recommended_product"),
                         "confidence": r.get("confidence")})
        summ["windows"][name]["end"] = now()
        summ["phases"][name] = rows

    phase("baseline", dict(PHASES)["baseline"])

    rc, so, se = kc(["apply", "-f", "-"], stdin=crd_yaml(surface))
    summ["injected_at"] = now()
    summ["inject_rc"] = rc
    if rc != 0:
        raise RuntimeError(f"CRD apply 失败: {so or se}")
    summ["effect_confirmed"] = wait_effect(surface, True)
    print(f"    [inject] effect_confirmed={summ['effect_confirmed']}", flush=True)

    phase("during", dict(PHASES)["during"])

    kc(["delete", "podchaos,networkchaos", "--all", "-n", NS, "--ignore-not-found"])
    summ["recovered_at"] = now()
    summ["recover_confirmed"] = wait_effect(surface, False, max_wait=600)
    print(f"    [recover] confirmed={summ['recover_confirmed']}", flush=True)

    phase("post", dict(PHASES)["post"])

    t1_unix = int(time.time()) + 15
    summ["t_end"] = now()
    summ["agent_spans_collected"] = dump_spans_since(
        span0, os.path.join(cdir, "raw", "agent_spans", "spans.jsonl"))
    summ["metrics_series"] = collect_metrics(cdir, t0_unix, t1_unix)
    print(f"    [collect] agent_spans={summ['agent_spans_collected']} "
          f"metrics={summ['metrics_series']}", flush=True)

    gt = {
        "case_id": cid,
        "root_cause_services": ["sasrec"],
        "n_distinct_root_services": 1,
        "fault_types": [s["fault_type"]],
        "component_ground_truth": [{
            "service": "sasrec", "fault_type": s["fault_type"], "role": "root",
            "injected_at": summ["injected_at"], "recovered_at": summ["recovered_at"],
            "crd_kind": s["kind"], "crd_name": s["crd"],
        }],
        "_agent_layer_note": (
            "★本 case 【只注入了 infra 故障】, agent 内容层【未注入任何东西】"
            "(rec-agent 跑 AGENTFAULT_OBSERVE=1 observe-only)。因此 agent 侧若出现语义错答, "
            "那是 infra 故障【自然涌现】的结果, 根因仍然只有 sasrec 一个 —— "
            "这正是本档(D 档)要测的『误归因区间』。任何把 agent 当第二根因的标注都是错的。"),
    }
    with open(os.path.join(cdir, "groundtruth.json"), "w", encoding="utf-8") as f:
        json.dump(gt, f, ensure_ascii=False, indent=1)
    with open(os.path.join(cdir, "metadata.json"), "w", encoding="utf-8") as f:
        json.dump({
            "case_id": cid, "collector": "dprobe_collect.py", "arity": "single",
            "layer_design": "D 档: infra-only 注入 + 双层观测",
            "recagent_image": "recweb-rec-agent:agentfault (AGENTFAULT_OBSERVE=1, 不注入)",
            "recagent_code_caveat": (
                "★该镜像的 tools.py 无 _filter_real_title 且 pod 内无 electronics.inter -> "
                "候选未过滤占位符、标题恒为『未知商品』; 与 (archived) agentfault_v2 的本地采集"
                "【不同镜像口径】, 跨集比较须声明。"),
            "probe_path": "kubectl proxy :8001 -> svc/rec-agent:5001 (不用 port-forward)",
            "prometheus": PROM, "namespace": NS,
        }, f, ensure_ascii=False, indent=1)
    with open(done, "w", encoding="utf-8") as f:
        json.dump(summ, f, ensure_ascii=False, indent=1)
    return "ok"


def preflight():
    bad = []
    rc, _, _ = kc(["get", "nodes"], timeout=30)
    if rc != 0:
        bad.append("K8S API 不可达")
    try:
        urllib.request.urlopen(f"{PROXY}/api/v1/namespaces/{NS}/pods?limit=1", timeout=8).read()
    except Exception as e:
        bad.append(f"kubectl proxy :8001 不通({e}) -> nohup kubectl proxy --port=8001 "
                   "--address=0.0.0.0 --accept-hosts='.*' &  ★Prometheus 的 cadvisor/"
                   "kube-state-metrics 也经它抓, 不起则 metrics 全空")
    r = prom_range('up{job="cadvisor"}', int(time.time()) - 120, int(time.time()))
    n = len((r.get("data") or {}).get("result") or [])
    if n == 0:
        bad.append("Prometheus 没有 cadvisor 序列(target down?)")
    pod = recagent_pod(force=True)
    if not pod:
        bad.append("找不到 Running 的 rec-agent pod")
    else:
        rc, so, _ = kc(["exec", "-n", NS, pod, "--", "printenv"], timeout=60)
        if "AGENTFAULT_OBSERVE=1" not in so:
            bad.append("rec-agent 不是 observe-only 变体 -> 先跑 patch_recagent_observe.ps1")
        if "AGENTFAULT_INJECT" in so:
            bad.append("★rec-agent 上挂着 AGENTFAULT_INJECT —— D 档必须【只注 infra】, 立即停")
    ok, _ = sasrec_health_from_pod()
    if not ok:
        bad.append("pod 内探 sasrec 不健康")
    for b in bad:
        print(f"  [FATAL] {b}", flush=True)
    return not bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--carriers", type=int, default=8)
    ap.add_argument("--reps", type=int, default=2)
    ap.add_argument("--surfaces", default="all")
    ap.add_argument("--out", default=OUT_ROOT)
    a = ap.parse_args()

    print(f"=== preflight @ {now()} ===", flush=True)
    if not preflight():
        sys.exit("preflight FAILED —— 按上方 FATAL 起前置后重跑")
    print("=== preflight OK ===", flush=True)

    seqs = json.load(open(CARRIER_POOL, encoding="utf-8"))["sequences"][:a.carriers]
    targets = list(SURFACES) if a.surfaces == "all" else a.surfaces.split(",")
    plan = [(s, c, r) for c in seqs for r in range(1, a.reps + 1) for s in targets]
    total = len(plan)
    print(f"计划 {total} case = {len(targets)} surface × {len(seqs)} 载体 × {a.reps} rep", flush=True)

    prog = os.path.join(a.out, "PROGRESS.json")
    os.makedirs(a.out, exist_ok=True)
    stat = {"total": total, "ok": 0, "skipped": 0, "failed": 0,
            "started": now(), "failures": []}

    for i, (s, c, r) in enumerate(plan, 1):
        stat["current"] = f"{i}/{total} {s}__c{c['seq_id']}__r{r}"
        stat["updated"] = now()
        with open(prog, "w", encoding="utf-8") as f:
            json.dump(stat, f, ensure_ascii=False, indent=1)
        try:
            res = run_case(s, c, r, a.out)
            stat["ok" if res == "ok" else "skipped"] += 1
        except Exception as e:
            stat["failed"] += 1
            stat["failures"].append({"case": stat["current"], "err": str(e)[:300]})
            print(f"  [FAIL] {stat['current']}: {e}", flush=True)
            traceback.print_exc()
            kc(["delete", "podchaos,networkchaos", "--all", "-n", NS, "--ignore-not-found"])
            time.sleep(15)
        print(f"--- 进度 {i}/{total} ok={stat['ok']} skip={stat['skipped']} "
              f"fail={stat['failed']} ---", flush=True)

    stat["current"] = "DONE"
    stat["finished"] = now()
    with open(prog, "w", encoding="utf-8") as f:
        json.dump(stat, f, ensure_ascii=False, indent=1)
    kc(["delete", "podchaos,networkchaos", "--all", "-n", NS, "--ignore-not-found"])
    print(f"\n=== 采集完成 ok={stat['ok']} skip={stat['skipped']} fail={stat['failed']} "
          f"-> {a.out} ===", flush=True)


if __name__ == "__main__":
    main()
