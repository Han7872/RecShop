# -*- coding: utf-8 -*-
"""agentfault 注入器 LIVE 冒烟 —— 主循环亲驱(不 run_in_background 撒手)。

对每个 (agent, kind) 组合:起一个临时 rec_agent 实例(注入器 armed)、探一次
/recommend、按三路独立信号判定注入是否**真生效且内容层可见**:
  1. 台账(ledger.jsonl):注入器落了该 agent/该 kind 的一条 GT 溯源记录;
  2. 响应载荷:wrong_item_pick -> resp.recommendation.recommended_product == 哨兵 ASIN;
             hallucinate  -> resp 成功 200(链不炸)+ conversation 里该 agent 段落 = 改写后文本;
  3. 内容层 span(SPAN_FILE):hallucinate -> 该 agent 的 ChatOpenAI output content
             == 改写后文本(证"内容层看得见语义故障",= 双轨内容轨的价值);
             wrong_item_pick -> Synthesizer tool_call 的 recommended_product == 哨兵 ASIN。

CHECKSUM(items/inventory)前后不变 = 不污染持久栈(rec_agent 无 DB 写,本应恒等)。

env 复用 phase1_launcher 配方:临时端口 / NACOS_ENABLED=false / OTLP 指死端口 14318 /
distinct OTEL_SERVICE_NAME / 绕 Clash / venv python(继承 conda langchain + openinference 切片)。

用法:
  python injector_smoke.py hallucinate:Product_Analyzer
  python injector_smoke.py wrong_item_pick:Recommendation_Synthesizer
  python injector_smoke.py all        # 跑内置组合集
Exit:0=该组合 PASS;1=usage;2=venv missing;3=health/env-gap;4=probe 非 200;5=注入判定 FAIL。
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))                     # .../injector
AGENTFAULT_DIR = os.path.dirname(HERE)                                # .../agentfault
REPO = os.path.dirname(os.path.dirname(os.path.dirname(AGENTFAULT_DIR)))  # repo root
sys.path.insert(0, HERE)  # 让 contract_validator 可导入(与本文件同目录)
from contract_validator import validate_synthesizer_contract, first_failed_check
SVC_DIR = os.path.join(REPO, "services", "recommendation_agent")
SMOKE_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "_smoke", "injector")
VENV_PY = os.path.join(REPO, "scratchpad", "phase1_venv", "Scripts", "python.exe")
LOADER_DIR = os.path.join(HERE, "loader")
os.makedirs(SMOKE_DIR, exist_ok=True)

DEAD_OTLP = "http://127.0.0.1:14318"
CHECKSUM_TABLES = ("items", "inventory")
PROBE_SEQ = ["015600206X", "6300215695", "0446673145"]   # in-SASRec-vocab (Phase1-verified)
PROBE_TOPK = 5
# wrong_item_pick 哨兵:v2 改用真标题、品类中性商品(LaView 12V 电源适配器),消 v1 "B00000FAULT"
# 字面含 FAULT + 无标题两条 judge 识别捷径(上界脚注 [b])。judge 只能靠"不匹配用户画像/不在
# 候选集"定位 = 真正考验 wrong-pick 定位力。注入判据(picked==哨兵)与契约 item_in_candidates 恒成立。
WRONG_ASIN = "B00EKWZK5E"
HEALTH_TIMEOUT_S = 180
PROBE_TIMEOUT_S = 180

# 内置组合集:每项 (label, kind, agent, port)
COMBOS = [
    ("hallu_product",   "hallucinate",     "Product_Analyzer",           5111),
    ("hallu_userbeh",   "hallucinate",     "User_Behavior_Analyzer",     5112),
    ("wrongpick_synth", "wrong_item_pick", "Recommendation_Synthesizer", 5113),
    ("format_synth",    "format_violation", "Recommendation_Synthesizer", 5114),
]

# format_violation 子类型 → 该 case 期望被契约校验器的哪一项 check 抓到
FORMAT_SUBTYPE = None   # None=注入器默认(missing_field/confidence);可 env 覆盖
FORMAT_EXPECT_CHECK = {
    "missing_field":  "required_fields",
    "type_violation": "field_types",
    "empty_required": "field_types",
    "malformed_json": "json_parsable",
}

# ---- Clash bypass for this driver's own HTTP ----
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


def probe(port, seq=None, top_k=None):
    """探一次 /recommend。seq/top_k 给了就用(载体轮换,P0-2),没给回退默认 PROBE_SEQ/PROBE_TOPK
    (后向兼容:老调用 probe(port) 行为不变)。"""
    url = f"http://127.0.0.1:{port}/recommend"
    item_sequence = seq if seq else PROBE_SEQ
    tk = top_k if top_k is not None else PROBE_TOPK
    body = json.dumps({"item_sequence": item_sequence, "top_k": tk})
    t0 = time.time()
    try:
        with _req(url, method="POST", body=body, timeout=PROBE_TIMEOUT_S) as r:
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
        return -1, e2e, {"_driver_error": str(e)}


def _load_env_cfg():
    cfg = {}
    try:
        with open(os.path.join(REPO, ".env"), "r", encoding="utf-8") as f:
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
    try:
        import mysql.connector
        cfg = _load_env_cfg()
        host = cfg.get("DB_HOST", "127.0.0.1")
        if host in ("localhost", "::1", ""):
            host = "127.0.0.1"
        cn = mysql.connector.connect(
            host=host, port=int(cfg.get("DB_PORT", "3306")),
            user=cfg.get("DB_USER", "root"), password=cfg.get("DB_PASSWORD", ""),
            database=cfg.get("DB_NAME", "shopify2"), connection_timeout=10)
        try:
            cur = cn.cursor()
            out = {}
            for t in CHECKSUM_TABLES:
                cur.execute(f"CHECKSUM TABLE {t}")
                row = cur.fetchone()
                out[t] = int(row[1]) if row and row[1] is not None else None
            cur.close()
            return out
        finally:
            cn.close()
    except Exception as e:
        return {"_error": str(e)}


def build_env(kind, agent, port, span_file, ledger_file):
    env = dict(os.environ)
    env["RECOMMENDATION_PORT"] = str(port)
    env["RECOMMENDATION_HOST"] = "127.0.0.1"
    env["NACOS_ENABLED"] = "false"
    env["OTEL_ENABLED"] = "true"
    env["OTEL_SERVICE_NAME"] = f"recweb_agentfault_inject_{agent}_{kind}"
    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = DEAD_OTLP
    env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = DEAD_OTLP
    env["SPAN_FILE"] = span_file
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    for pk in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        env.pop(pk, None)
    env.pop("SASREC_API_URL", None)
    # clear ALL agent-fault knobs inherited from shell, then set THIS combo's
    for k in list(env.keys()):
        if k.startswith("AGENTFAULT_KIND_") or k.startswith("AGENT_FAULT_"):
            env.pop(k, None)
    env["AGENTFAULT_INSTRUMENT"] = "1"        # arm openinference content capture
    env["AGENTFAULT_INJECT"] = "1"            # arm injector
    env["AGENTFAULT_KIND_" + agent] = kind    # inject THIS kind on THIS agent
    env["AGENTFAULT_WRONG_ASIN"] = WRONG_ASIN
    env["AGENTFAULT_LEDGER"] = ledger_file
    # format_violation 子类型/字段:优先本 driver 进程 env(上面已被清),回退默认由注入器定
    _fsub = os.environ.get("AGENTFAULT_FORMAT_SUBTYPE")
    _ffld = os.environ.get("AGENTFAULT_FORMAT_FIELD")
    if _fsub:
        env["AGENTFAULT_FORMAT_SUBTYPE"] = _fsub
    if _ffld:
        env["AGENTFAULT_FORMAT_FIELD"] = _ffld
    # loader dir FIRST so its sitecustomize wins
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = LOADER_DIR + (os.pathsep + pp if pp else "")
    return env


# ---------------- span helpers (mirror phase1_smoke_verify content matchers) ----------------
def read_spans(span_file, trace_id):
    spans, last, stable = [], [], 0
    for _ in range(8):
        spans = []
        try:
            with open(span_file, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        rec = json.loads(s)
                    except Exception:
                        continue
                    if rec.get("trace_id") == trace_id:
                        spans.append(rec)
        except FileNotFoundError:
            spans = []
        if len(spans) == len(last) and len(spans) > 0:
            stable += 1
            if stable >= 2:
                return spans
        else:
            stable = 0
        last = spans
        time.sleep(0.5)
    return last


def _agent_of_span(span, by_id, agent_names):
    """该 ChatOpenAI span 归属哪个 agent —— 靠 LangChain Run 树祖先里的 LangGraph 节点名
    (= agent 裸名),非 OTel parent chain(两套并行 OTel 树,见 phase1 判据)。返回 agent 名或 ""。"""
    cur = span
    seen = set()
    while cur and cur.get("span_id") not in seen:
        seen.add(cur.get("span_id"))
        nm = cur.get("name")
        if nm in agent_names:
            return nm
        pid = cur.get("parent_span_id")
        cur = by_id.get(pid) if pid else None
    return ""


def chatopenai_output_contents(spans, by_id=None, only_agent=None, agent_names=None):
    """ChatOpenAI span 的 output content 文本。

    only_agent 给定时**只**收该 agent 子树下的 ChatOpenAI span(修 Reviewer 高危发现:
    原版无 agent 过滤 → boilerplate 空证 / 跨 agent 误配)。"""
    out = []
    names = set(agent_names or [])
    for s in spans:
        if s.get("name") != "ChatOpenAI":
            continue
        if only_agent:
            if by_id is None:
                continue
            if _agent_of_span(s, by_id, names) != only_agent:
                continue
        attrs = s.get("attributes") or {}
        for k, v in attrs.items():
            if k.startswith("llm.output_messages.") and k.endswith(".message.content") and isinstance(v, str) and v:
                out.append(v)
    return out


def chatopenai_toolcall_asins(spans):
    """ChatOpenAI span 里 Synthesize_Recommendation tool_call 的 recommended_product。"""
    asins = []
    for s in spans:
        if s.get("name") != "ChatOpenAI":
            continue
        attrs = s.get("attributes") or {}
        # openinference 把 tool_call arguments 拍平成 ...tool_calls.<M>.tool_call.function.arguments (JSON 串)
        for k, v in attrs.items():
            if k.endswith(".tool_call.function.arguments") and isinstance(v, str):
                try:
                    a = json.loads(v)
                    if "recommended_product" in a:
                        asins.append(a["recommended_product"])
                except Exception:
                    pass
    return asins


def synthesizer_toolcall_arg_strings(spans):
    """Synthesizer tool_call 的 **raw arguments 串**(不预解析 —— malformed_json 破坏的正是原串,
    契约校验器要拿原串走"JSON 可解析"查)。返回 list[str]。取 span name==ChatOpenAI 且
    tool_call function.name==Synthesize_Recommendation 的那些 arguments。"""
    out = []
    for s in spans:
        if s.get("name") != "ChatOpenAI":
            continue
        attrs = s.get("attributes") or {}
        # 找出 Synthesize_Recommendation 的 tool_call 下标,再取同下标 arguments
        for k, v in attrs.items():
            if k.endswith(".tool_call.function.name") and v == "Synthesize_Recommendation":
                arg_key = k[: -len(".name")] + ".arguments"
                av = attrs.get(arg_key)
                if isinstance(av, str):
                    out.append(av)
    return out


AGENT_NAMES = ["Sequence_Recommender", "User_Behavior_Analyzer",
               "Product_Analyzer", "Recommendation_Synthesizer"]


def read_all_ledger(ledger_file):
    rows = []
    try:
        with open(ledger_file, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    rows.append(json.loads(s))
                except Exception:
                    continue
    except FileNotFoundError:
        pass
    return rows


def read_ledger_entries(ledger_file, trace_id, agent, kind, strict_trace=True):
    """只返回真正**注入成功**的记录(status=='injected' 或旧格式无 status)。
    inject_failed 记录不算注入 —— 拒答/失败绝不当 hallucinate case。

    ★采前审 FIX-A(critical,COLLECTION_DESIGN §6b 硬化):strict_trace=True(默认)时,给定
    trace_id 则要求台账记录 trace_id **非空且精确相等**;**空 trace_id 的注入记录不再通配匹配每个 rep**
    (旧逻辑 `trace_id and e.get('trace_id') and ...` 中间项为空即跳过不等判断 → K reps 共享台账时一条空
    trace 记录会污染每个 rep,是 false-faulted 隐患)。K-rep 采集 runner 必须用 strict_trace。
    strict_trace=False 保留旧宽松语义(单 probe/实例的场景,无跨 rep 污染面)。"""
    out = []
    for e in read_all_ledger(ledger_file):
        if e.get("agent") != agent or e.get("kind") != kind:
            continue
        if e.get("status") == "inject_failed":
            continue
        if trace_id:
            et = e.get("trace_id")
            if strict_trace:
                if not et or et != trace_id:   # 空 trace 或不等 → 不匹配(防跨 rep 污染)
                    continue
            else:
                if et and et != trace_id:       # 旧宽松:空 trace 通配(仅单 probe 场景安全)
                    continue
        out.append(e)
    return out


def non_target_injected_agents(ledger_file, trace_id, target_agent, strict_trace=True):
    """隔离负检:除 target 外,还有哪些 agent 落了 injected 台账(应为空)。
    ★FIX-A:strict_trace=True 时空 trace 记录不通配(防跨 rep 污染,同 read_ledger_entries)。"""
    bad = set()
    for e in read_all_ledger(ledger_file):
        if e.get("status") == "inject_failed":
            continue
        a = e.get("agent")
        if not a or a == target_agent:
            continue
        et = e.get("trace_id")
        if trace_id:
            if strict_trace:
                if not et or et != trace_id:
                    continue
            else:
                if et and et != trace_id:
                    continue
        bad.add(a)
    return sorted(bad)


# ---------------- one combo ----------------
def run_combo(label, kind, agent, port):
    span_file = os.path.join(SMOKE_DIR, f"{label}_spans.jsonl")
    ledger_file = os.path.join(SMOKE_DIR, f"{label}_ledger.jsonl")
    log_path = os.path.join(SMOKE_DIR, f"{label}_server.log")
    verdict_path = os.path.join(SMOKE_DIR, f"{label}_verdict.json")
    for p in (span_file, ledger_file, log_path, verdict_path):
        try:
            os.remove(p)
        except OSError:
            pass

    if not os.path.exists(VENV_PY):
        print(f"[inject-smoke] FATAL venv missing: {VENV_PY} (run phase1_bootstrap.sh)")
        return 2

    env = build_env(kind, agent, port, span_file, ledger_file)
    print(f"[inject-smoke] {label}: kind={kind} agent={agent} port={port}")
    print(f"               SPAN_FILE={span_file}")
    print(f"               LEDGER   ={ledger_file}")

    cs_before = checksum_tables()
    logf = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen([VENV_PY, "app.py"], cwd=SVC_DIR, env=env,
                            stdout=logf, stderr=subprocess.STDOUT)
    verdict = {"label": label, "kind": kind, "agent": agent, "port": port}
    rc = 0
    try:
        print(f"[inject-smoke] waiting /recommend/health (<= {HEALTH_TIMEOUT_S}s) ...")
        if not wait_health(port):
            logf.flush()
            try:
                logf.seek(0)
                tail = logf.read().splitlines()[-40:]
            except Exception:
                tail = []
            print("[inject-smoke] HEALTH TIMEOUT — server log tail:")
            for ln in tail:
                print("    " + ln)
            verdict.update({"health_ok": False, "env_gap": True, "verdict": "INCONCLUSIVE(env-gap)"})
            rc = 3
        else:
            status, e2e, j = probe(port)
            trace_id = (j or {}).get("trace_id", "") if isinstance(j, dict) else ""
            print(f"[inject-smoke] http={status} e2e_ms={e2e:.0f} trace_id={trace_id}")
            verdict.update({"health_ok": True, "http_status": status,
                            "e2e_ms": round(e2e, 1), "trace_id": trace_id,
                            "resp": j})
            if status != 200 or not isinstance(j, dict) or not j.get("success"):
                verdict["verdict"] = "FAIL(probe-non-200)"
                rc = 4
            else:
                # ---- 信号判定(Reviewer 加固后:agent-scoped span + 分叉 needle + 隔离负检)----
                spans = read_spans(span_file, trace_id) if trace_id else []
                by_id = {s["span_id"]: s for s in spans if s.get("span_id")}
                ledger = read_ledger_entries(ledger_file, trace_id, agent, kind)
                rec = (j.get("recommendation") or {})

                sig = {}
                sig["ledger_entry"] = (len(ledger) >= 1)
                # 隔离负检:除 target 外无其它 agent 被注入(修 Reviewer missing-isolation 发现)
                non_target = non_target_injected_agents(ledger_file, trace_id, agent)
                sig["isolation_ok"] = (len(non_target) == 0)

                if kind == "wrong_item_pick":
                    picked = rec.get("recommended_product")
                    sig["response_asin_swapped"] = (picked == WRONG_ASIN)
                    span_asins = chatopenai_toolcall_asins(spans)
                    sig["span_toolcall_asin_swapped"] = (WRONG_ASIN in span_asins)
                    verdict["evidence"] = {"response_pick": picked,
                                           "span_toolcall_asins": span_asins,
                                           "ledger": ledger[:2],
                                           "non_target_injected": non_target,
                                           "span_count": len(spans)}
                    passed = (sig["ledger_entry"] and sig["response_asin_swapped"]
                              and sig["span_toolcall_asin_swapped"] and sig["isolation_ok"])
                elif kind == "format_violation":
                    # on-thesis:注入的结构破坏必须**被契约校验器(消费方)抓到** = "被对应方法消费"的实证。
                    # 从 Synthesizer tool_call span 取 raw args → 过 validate_synthesizer_contract。
                    subtype = (ledger[0].get("violation", {}).get("subtype")
                               if ledger else None)
                    expect_check = FORMAT_EXPECT_CHECK.get(subtype)
                    arg_strs = synthesizer_toolcall_arg_strings(spans)
                    consumed_by_validator = False
                    failed_check = None
                    checks_dump = None
                    for a in arg_strs:
                        ok_c, checks = validate_synthesizer_contract(a, candidates=None)
                        fc = first_failed_check(checks)
                        if not ok_c:
                            failed_check = fc
                            checks_dump = checks
                            # 被消费 = 校验器判违约,且失败项 == 该子类型期望项
                            if expect_check is None or fc == expect_check:
                                consumed_by_validator = True
                                break
                    sig["span_toolcall_present"] = (len(arg_strs) >= 1)
                    sig["contract_violation_caught"] = consumed_by_validator
                    verdict["evidence"] = {"ledger": ledger[:2],
                                           "subtype": subtype,
                                           "expected_failed_check": expect_check,
                                           "actual_failed_check": failed_check,
                                           "checks": checks_dump,
                                           "toolcall_args_count": len(arg_strs),
                                           "non_target_injected": non_target,
                                           "span_count": len(spans)}
                    # 链存活也记(format 故障 rec_agent 可能 .get 默认值愈合响应 → 黑盒未必炸,契约层才见)
                    sig["chain_alive_200"] = (status == 200 and bool(j.get("success")))
                    passed = (sig["ledger_entry"] and sig["span_toolcall_present"]
                              and sig["contract_violation_caught"] and sig["isolation_ok"])
                else:  # hallucinate
                    # 分叉 needle(注入指纹,非共有 boilerplate)—— 修 Reviewer vacuous-signal 发现
                    needle = (ledger[0].get("divergent_needle") if ledger else "") or ""
                    if not needle:
                        # 兼容旧格式:退回 injected_excerpt 但取尾段(仍避开开头 boilerplate)
                        ie = (ledger[0].get("injected_excerpt", "") if ledger else "").rstrip("…")
                        needle = ie[-40:] if len(ie) > 40 else ie
                    # agent-scoped:只在 **target agent 子树** 的 ChatOpenAI span 里找 needle
                    tgt_contents = chatopenai_output_contents(
                        spans, by_id=by_id, only_agent=agent, agent_names=AGENT_NAMES)
                    span_sees = bool(needle) and any(needle in c for c in tgt_contents)
                    sig["span_sees_rewritten_on_target"] = span_sees
                    sig["chain_alive_200"] = (status == 200 and bool(j.get("success")))
                    verdict["evidence"] = {"ledger": ledger[:2],
                                           "target_span_content_count": len(tgt_contents),
                                           "needle": needle,
                                           "non_target_injected": non_target,
                                           "span_count": len(spans)}
                    passed = (sig["ledger_entry"] and sig["chain_alive_200"]
                              and sig["span_sees_rewritten_on_target"] and sig["isolation_ok"])

                verdict["signals"] = sig
                verdict["verdict"] = "PASS" if passed else "FAIL(injection-not-verified)"
                if not passed:
                    rc = 5
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
        verdict["checksum_before"] = cs_before
        verdict["checksum_after"] = cs_after
        polluted = any(isinstance(cs_before.get(t), int) and isinstance(cs_after.get(t), int)
                       and cs_before[t] != cs_after[t] for t in CHECKSUM_TABLES)
        verdict["baseline_pollution"] = polluted
        if polluted:
            print("[inject-smoke] WARNING: CHECKSUM changed = BASELINE POLLUTION")
            verdict["verdict"] = "INCONCLUSIVE(pollution)"
            rc = max(rc, 5)
        verdict["span_file"] = span_file
        verdict["ledger_file"] = ledger_file
        verdict["server_log"] = log_path
        with open(verdict_path, "w", encoding="utf-8") as f:
            json.dump(verdict, f, ensure_ascii=False, indent=2)
        print(f"[inject-smoke] {label} verdict = {verdict.get('verdict')} -> {verdict_path}")
    return rc


def main():
    if len(sys.argv) < 2:
        print("usage: python injector_smoke.py <kind:agent | all>")
        print("  kinds: hallucinate | wrong_item_pick")
        return 1
    arg = sys.argv[1].strip()
    combos = []
    if arg == "all":
        combos = COMBOS
    else:
        if ":" not in arg:
            print("bad arg; use kind:agent e.g. hallucinate:Product_Analyzer")
            return 1
        kind, agent = arg.split(":", 1)
        port = {"Sequence_Recommender": 5111, "User_Behavior_Analyzer": 5112,
                "Product_Analyzer": 5111, "Recommendation_Synthesizer": 5113}.get(agent, 5119)
        combos = [(f"{kind}_{agent}", kind, agent, port)]

    results = {}
    worst = 0
    for (label, kind, agent, port) in combos:
        rc = run_combo(label, kind, agent, port)
        results[label] = rc
        worst = max(worst, rc)
        print("-" * 60)
    print("[inject-smoke] summary:")
    for label, rc in results.items():
        print(f"   {label:20s} rc={rc} ({'PASS' if rc == 0 else 'FAIL/INC'})")
    return worst


if __name__ == "__main__":
    sys.exit(main())
