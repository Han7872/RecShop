# -*- coding: utf-8 -*-
"""clean-baseline 采集器 —— 消 base LLM 自发幻觉的对照臂(inject vs baseline 增量法)。

动机(judge 原型发现):DeepSeek base agent **会自发幻觉**,故纯 MAST 二元 judge 把"该 agent
是否幻觉"当"是否被注入"会误报。**解法(贴传统故障注入标准做法:注入 vs 基线对照)**:
对同一输入跑 K 次**不注入**的 clean run,judge 每条,统计每 agent 的**自发 yes 率**
baseline_yes_rate[agent]。faulted case 里某 agent 判 yes **且** 其 baseline 自发率低 → 归因为注入;
自发率高的 agent(base 本就爱瞎编)即便判 yes 也不归因 → 抵消自发幻觉。

本采集器起临时 rec_agent 实例(**AGENTFAULT_INJECT 关**,只开 openinference 内容捕获),
同 PROBE_SEQ 跑 K 次,每次 judge 该 clean trace,产出:
  {"n_runs": K, "yes_rate": {agent: r}, "per_run": [...], "probe_seq": [...]}
落 `(v1)_smoke/judge/clean_baseline_rates.json`(judge --baseline 读它)。

复用 injector_smoke 的实例起法(临时端口/dead-OTLP/绕 Clash/venv python),但不 arm 注入器。
主循环亲驱(不 run_in_background)。用法:
  python collect_clean_baseline.py --runs 5
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))                     # .../judge
AGENTFAULT_DIR = os.path.dirname(HERE)                                # .../agentfault
REPO = os.path.dirname(os.path.dirname(os.path.dirname(AGENTFAULT_DIR)))
SVC_DIR = os.path.join(REPO, "services", "recommendation_agent")
OUT_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "_smoke", "judge")
VENV_PY = os.path.join(REPO, "scratchpad", "phase1_venv", "Scripts", "python.exe")
INJ_LOADER_DIR = os.path.join(AGENTFAULT_DIR, "injector", "loader")   # 复用 loader(仅开 INSTRUMENT)
os.makedirs(OUT_DIR, exist_ok=True)

sys.path.insert(0, HERE)
import hallucinate_judge as HJ   # judge_trace / AGENT_ORDER / _load_dotenv

DEAD_OTLP = "http://127.0.0.1:14318"
PROBE_SEQ = ["015600206X", "6300215695", "0446673145"]
PROBE_TOPK = 5
BASE_PORT = 5121
HEALTH_TIMEOUT_S = 180
PROBE_TIMEOUT_S = 180

os.environ["NO_PROXY"] = "127.0.0.1,localhost"
os.environ["no_proxy"] = "127.0.0.1,localhost"
for _pk in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_pk, None)
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _req(url, method="GET", body=None, timeout=90):
    data = body.encode("utf-8") if isinstance(body, str) else body
    r = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        r.add_header("Content-Type", "application/json")
    return _OPENER.open(r, timeout=timeout)


def wait_health(port, timeout_s=HEALTH_TIMEOUT_S):
    url = f"http://127.0.0.1:{port}/recommend/health"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with _req(url, timeout=8) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(2.0)
    return False


def probe(port):
    url = f"http://127.0.0.1:{port}/recommend"
    body = json.dumps({"item_sequence": PROBE_SEQ, "top_k": PROBE_TOPK})
    try:
        with _req(url, method="POST", body=body, timeout=PROBE_TIMEOUT_S) as r:
            j = json.loads(r.read().decode("utf-8", "ignore"))
            return r.status, j
    except Exception as e:
        return -1, {"_err": str(e)}


def build_env(port, span_file):
    env = dict(os.environ)
    env["RECOMMENDATION_PORT"] = str(port)
    env["RECOMMENDATION_HOST"] = "127.0.0.1"
    env["NACOS_ENABLED"] = "false"
    env["OTEL_ENABLED"] = "true"
    env["OTEL_SERVICE_NAME"] = f"recweb_agentfault_baseline_{port}"
    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = DEAD_OTLP
    env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = DEAD_OTLP
    env["SPAN_FILE"] = span_file
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    for pk in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        env.pop(pk, None)
    env.pop("SASREC_API_URL", None)
    # clear ALL agent-fault knobs; **do NOT** arm injector (baseline = clean)
    for k in list(env.keys()):
        if k.startswith("AGENTFAULT_KIND_") or k.startswith("AGENT_FAULT_"):
            env.pop(k, None)
    env["AGENTFAULT_INSTRUMENT"] = "1"   # openinference content capture only
    env.pop("AGENTFAULT_INJECT", None)   # <-- key difference: NO injection
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = INJ_LOADER_DIR + (os.pathsep + pp if pp else "")
    return env


def one_clean_run(port, idx):
    span_file = os.path.join(OUT_DIR, f"baseline_run{idx}_spans.jsonl")
    log_path = os.path.join(OUT_DIR, f"baseline_run{idx}_server.log")
    for p in (span_file, log_path):
        try:
            os.remove(p)
        except OSError:
            pass
    env = build_env(port, span_file)
    logf = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen([VENV_PY, "app.py"], cwd=SVC_DIR, env=env,
                            stdout=logf, stderr=subprocess.STDOUT)
    conv = None
    try:
        if not wait_health(port):
            print(f"  run{idx}: HEALTH TIMEOUT (env-gap)")
            return None
        status, j = probe(port)
        if status == 200 and isinstance(j, dict) and j.get("success"):
            conv = j.get("conversation") or {}
        else:
            print(f"  run{idx}: probe http={status} (skip)")
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        try:
            logf.close()
        except Exception:
            pass
    return conv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5, help="clean runs K (baseline sample size)")
    args = ap.parse_args()
    HJ._load_dotenv()

    if not os.path.exists(VENV_PY):
        print(f"FATAL venv missing: {VENV_PY}")
        return 2

    per_run = []
    yes_counts = {a: 0 for a in HJ.AGENT_ORDER}
    judged = 0
    for i in range(args.runs):
        print(f"[baseline] clean run {i+1}/{args.runs} (no injection) ...")
        conv = one_clean_run(BASE_PORT + i, i)
        if not conv:
            per_run.append({"run": i, "judged": False})
            continue
        jt = HJ.judge_trace(conv)
        yn = jt["yes_no"]
        judged += 1
        for a in HJ.AGENT_ORDER:
            if yn.get(a) == 1:
                yes_counts[a] += 1
        per_run.append({"run": i, "judged": True, "yes_no": yn,
                        "tokens": jt["tokens"].get("total"), "latency_s": jt["latency_s"]})
        print(f"  run{i}: spontaneous yes = {[a for a in HJ.AGENT_ORDER if yn.get(a)==1]}")

    yes_rate = {a: round(yes_counts[a] / judged, 3) for a in HJ.AGENT_ORDER} if judged else {}
    out = {
        "n_runs": judged,
        "n_attempted": args.runs,
        "probe_seq": PROBE_SEQ,
        "yes_rate": yes_rate,     # 各 agent 自发 yes 率(judge --baseline 减它)
        "yes_counts": yes_counts,
        "per_run": per_run,
        "note": ("clean baseline(不注入)下各 agent 自发幻觉 yes 率。faulted case 里某 agent "
                 "判 yes 且此率低 → 归因注入;此率高 → base 本就爱瞎编,不归因(消自发)。"),
    }
    outp = os.path.join(OUT_DIR, "clean_baseline_rates.json")
    with open(outp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n[baseline] {judged}/{args.runs} judged -> {outp}")
    print(f"[baseline] spontaneous yes_rate = {yes_rate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
