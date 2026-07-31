# -*- coding: utf-8 -*-
"""Phase1 承重闸 launcher — starts ONE ephemeral rec_agent instance per fault
mode, probes /recommend once, and records the raw (trace_id, http_status, e2e_ms)
plus a CHECKSUM pollution guard. The offline verifier (phase1_smoke_verify.py)
then reads the SPAN_FILE by trace_id and judges content-span mounting.

This is the LIVE driver (the main loop runs it). It edits NOTHING under services/.
It only starts the existing app.py as a subprocess with an env dict that mirrors
agentchaos_runner.start_temp_instance (read-only reference: ctk/agentchaos_runner.py
L269-315), PLUS two openinference knobs:
    - venv python as the interpreter (inherits conda langchain, has openinference slice)
    - PYTHONPATH prepend the phase1_loader dir so sitecustomize auto-imports it
    - PHASE1_INSTRUMENT=1 to arm the loader

Env per instance:
    RECOMMENDATION_PORT          = 5101..5104 (distinct per mode; avoids TIME_WAIT coupling)
    NACOS_ENABLED=false          (else service discovery bypasses fixed ports)
    OTEL_ENABLED=true
    OTEL_SERVICE_NAME            = recweb_agentfault_phase1_<mode> (distinct from persistent stack)
    OTEL_EXPORTER_OTLP_ENDPOINT  = dead port 127.0.0.1:14318 (BSP silently fails -> no Jaeger pollution)
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = same dead port
    SPAN_FILE                    = (v1)_smoke/phase1_<mode>_spans.jsonl
    PHASE1_INSTRUMENT=1 + PYTHONPATH=.../phase1_loader
    NO_PROXY=127.0.0.1,localhost + stripped HTTP(S)_PROXY (loopback bypasses Clash;
                                  external DeepSeek reachability is an env-prereq, NOT Phase1)
    AGENT_FAULT_* cleared, then set per mode

CHECKSUM guard (best-effort pollution detector): items/inventory must be unchanged
across the probe (this service does no DB write). A change = baseline pollution;
we WARN + record it but do not mask a content-layer verdict.

Usage:
    python phase1_launcher.py <mode>           # one mode
    python phase1_launcher.py normal
Exit codes: 0=probe returned a response (verifier will judge); 1=usage; 2=venv
missing; 3=health timeout (env-gap); 4=probe errored.
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

# ---------------- paths ----------------
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
SVC_DIR = os.path.join(REPO, "services", "recommendation_agent")
SMOKE_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "_smoke")
VENV_PY = os.path.join(REPO, "scratchpad", "phase1_venv", "Scripts", "python.exe")
LOADER_DIR = os.path.join(HERE, "phase1_loader")
os.makedirs(SMOKE_DIR, exist_ok=True)

# ---------------- constants (mirror agentchaos_runner) ----------------
DEAD_OTLP = "http://127.0.0.1:14318"
DELAY_MS = 15000
CHECKSUM_BASELINE = {"items": 1088112223, "inventory": 944901079}
PROBE_SEQ = ["015600206X", "6300215695", "0446673145"]   # in-SASRec-vocab (TASK-W verified)
PROBE_TOPK = 5
HEALTH_TIMEOUT_S = 180      # first /recommend builds the graph lazily; allow ample warmup
PROBE_TIMEOUT_S = 120

# mode -> (port, faults dict)
MODES = {
    "normal":  (5101, {}),
    "delay":   (5102, {"Product_Analyzer": "delay"}),         # delay still invokes -> content present
    "error":   (5103, {"User_Behavior_Analyzer": "error"}),   # raises pre-invoke -> NO content (expected)
    "garbage": (5104, {"Product_Analyzer": "garbage"}),       # early-return pre-invoke -> NO content (expected)
}

# ---------------- Clash bypass for THIS driver's own HTTP (probe + health) ----------------
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


def probe_recommend(port, timeout_s=PROBE_TIMEOUT_S):
    url = f"http://127.0.0.1:{port}/recommend"
    body = json.dumps({"item_sequence": PROBE_SEQ, "top_k": PROBE_TOPK})
    t0 = time.time()
    try:
        with _req(url, method="POST", body=body, timeout=timeout_s) as r:
            raw = r.read().decode("utf-8", "ignore")
            e2e = (time.time() - t0) * 1000.0
            try:
                j = json.loads(raw)
            except Exception:
                j = None
            return r.status, e2e, j
    except urllib.error.HTTPError as e:
        e2e = (time.time() - t0) * 1000.0
        try:
            j = json.loads(e.read().decode("utf-8", "ignore"))
        except Exception:
            j = None
        return e.code, e2e, j
    except Exception as e:
        e2e = (time.time() - t0) * 1000.0
        return -1, e2e, {"_runner_error": str(e)}


# ---------------- best-effort CHECKSUM pollution guard ----------------
def _load_env_cfg():
    cfg = {}
    envp = os.path.join(REPO, ".env")
    try:
        with open(envp, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#") or "=" not in s:
                    continue
                k, _, v = s.partition("=")
                cfg[k.strip()] = v.strip().strip('"').strip("'")
    except Exception:
        pass
    return cfg


def checksum_tables():
    """Read-only CHECKSUM TABLE items, inventory. Returns {table:int} or {_error:..}."""
    try:
        import mysql.connector  # inherited from conda via venv --system-site-packages
        cfg = _load_env_cfg()
        host = cfg.get("DB_HOST", "127.0.0.1")
        if host in ("localhost", "::1", ""):
            host = "127.0.0.1"
        cn = mysql.connector.connect(
            host=host, port=int(cfg.get("DB_PORT", "3306")),
            user=cfg.get("DB_USER", "root"), password=cfg.get("DB_PASSWORD", ""),
            database=cfg.get("DB_NAME", "shopify2"), connection_timeout=10,
        )
        try:
            cur = cn.cursor()
            out = {}
            for t in ("items", "inventory"):
                cur.execute(f"CHECKSUM TABLE {t}")  # whitelist, not user input
                row = cur.fetchone()
                out[t] = int(row[1]) if row and row[1] is not None else None
            cur.close()
            return out
        finally:
            cn.close()
    except Exception as e:
        return {"_error": str(e)}


# ---------------- instance lifecycle ----------------
def build_env(mode):
    port, faults = MODES[mode]
    env = dict(os.environ)
    env["RECOMMENDATION_PORT"] = str(port)
    env["RECOMMENDATION_HOST"] = "127.0.0.1"
    env["NACOS_ENABLED"] = "false"
    env["OTEL_ENABLED"] = "true"
    env["OTEL_SERVICE_NAME"] = f"recweb_agentfault_phase1_{mode}"
    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = DEAD_OTLP
    env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = DEAD_OTLP
    env["SPAN_FILE"] = os.path.join(SMOKE_DIR, f"phase1_{mode}_spans.jsonl")
    # loopback bypasses Clash; external DeepSeek reachability is an env-prereq.
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    for pk in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        env.pop(pk, None)
    # SASREC_API_URL unset -> tools.py fallback to real 8200 (this service's real downstream)
    env.pop("SASREC_API_URL", None)
    # clear ALL AGENT_FAULT_* inherited from the shell, then set this mode's
    for k in list(env.keys()):
        if k.startswith("AGENT_FAULT_"):
            env.pop(k, None)
    for name, kind in faults.items():
        env[f"AGENT_FAULT_{name}"] = kind
    env["AGENT_FAULT_DELAY_MS"] = str(DELAY_MS)
    # openinference loader arming
    env["PHASE1_INSTRUMENT"] = "1"
    env["PHASE1_INSTRUMENT_MODE"] = env.get("PHASE1_INSTRUMENT_MODE", "minimal")
    # PYTHONPATH: loader dir FIRST so sitecustomize.py wins any other sitecustomize
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = LOADER_DIR + (os.pathsep + pp if pp else "")
    return env, port, faults


def run_mode(mode):
    if mode not in MODES:
        print(f"[phase1-launcher] unknown mode {mode!r}; choose one of {sorted(MODES)}")
        return 1
    if not os.path.exists(VENV_PY):
        print(f"[phase1-launcher] FATAL: venv python missing: {VENV_PY}")
        print(f"                   run phase1_bootstrap.sh first.")
        return 2

    env, port, faults = build_env(mode)
    span_file = env["SPAN_FILE"]
    log_path = os.path.join(SMOKE_DIR, f"phase1_{mode}_server.log")
    probe_path = os.path.join(SMOKE_DIR, f"phase1_{mode}_probe.json")
    # start fresh artifacts for this mode (avoid stale spans confusing the verifier)
    for p in (span_file, log_path, probe_path):
        try:
            os.remove(p)
        except OSError:
            pass

    print(f"[phase1-launcher] mode={mode} port={port} faults={faults}")
    print(f"                  SPAN_FILE = {span_file}")
    print(f"                  PYTHONPATH loader = {LOADER_DIR}")
    print(f"                  instrument mode = {env['PHASE1_INSTRUMENT_MODE']}")

    cs_before = checksum_tables()
    logf = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        [VENV_PY, "app.py"], cwd=SVC_DIR, env=env,
        stdout=logf, stderr=subprocess.STDOUT,
    )
    probe_result = None
    rc = 0
    try:
        print(f"[phase1-launcher] waiting for /recommend/health (up to {HEALTH_TIMEOUT_S}s) ...")
        if not wait_health(port):
            # health failed: dump loader/otel lines for diagnosis before giving up
            logf.flush()
            try:
                logf.seek(0)
                tail = logf.read().splitlines()[-40:]
            except Exception:
                tail = []
            print("[phase1-launcher] HEALTH TIMEOUT — server log tail:")
            for ln in tail:
                print("    " + ln)
            print("[phase1-launcher] classify as ENV-GAP (sasrec:8200/deepseek/venv) — not a Phase1 content verdict.")
            probe_result = {"mode": mode, "health_ok": False, "env_gap": True}
            rc = 3
        else:
            print(f"[phase1-launcher] health OK; probing POST /recommend (timeout {PROBE_TIMEOUT_S}s) ...")
            status, e2e, j = probe_recommend(port)
            probe_result = {
                "mode": mode, "health_ok": True, "http_status": status,
                "e2e_ms": round(e2e, 1), "resp": j,
                "trace_id": (j or {}).get("trace_id", "") if isinstance(j, dict) else "",
            }
            print(f"[phase1-launcher] http_status={status} e2e_ms={probe_result['e2e_ms']} trace_id={probe_result['trace_id']}")
            if status != 200 or not isinstance(j, dict) or not j.get("success"):
                rc = 4
    finally:
        try:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()
        except Exception:
            pass
        try:
            logf.close()
        except Exception:
            pass
        cs_after = checksum_tables()
        if probe_result is not None:
            probe_result["checksum_before"] = cs_before
            probe_result["checksum_after"] = cs_after
            probe_result["checksum_baseline"] = CHECKSUM_BASELINE
            probe_result["span_file"] = span_file
            probe_result["server_log"] = log_path
            # pollution = after != before, or either != known baseline
            polluted = False
            for t in ("items", "inventory"):
                b = cs_before.get(t); a = cs_after.get(t)
                if isinstance(b, int) and isinstance(a, int) and b != a:
                    polluted = True
            probe_result["baseline_pollution"] = polluted
            if polluted:
                print(f"[phase1-launcher] WARNING: CHECKSUM changed across probe (items/inventory) = BASELINE POLLUTION")
            with open(probe_path, "w", encoding="utf-8") as f:
                json.dump(probe_result, f, ensure_ascii=False, indent=2)
            print(f"[phase1-launcher] wrote {probe_path}")

    return rc


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python phase1_launcher.py <normal|delay|error|garbage>")
        sys.exit(1)
    sys.exit(run_mode(sys.argv[1]))
