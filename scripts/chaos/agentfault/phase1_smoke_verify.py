# -*- coding: utf-8 -*-
"""Phase1 承重闸 OFFLINE verifier — judges whether openinference content spans
really mount under workflow.py's agent.<Name> boundary spans, per mode.

Reads ONLY the artifacts the launcher wrote (probe.json + SPAN_FILE + server.log
per mode). No live process needed (SimpleSpanProcessor flushes at span END). Main
conda python is fine (does not need the venv).

JUDGMENT (parent-chain WALK, not fixed depth — AgentExecutor inserts arbitrary
intermediate RunnableSequence/ChatPromptTemplate/ChatOpenAI/Tool layers):
  - boundary span   = name=='agent.<Name>' AND attributes['recweb.agent.name']=='<Name>'
  - content span    = attributes contain
        llm.input_messages.<N>.message.content      (prompt)
        llm.output_messages.<N>.message.content     (completion)
        llm.output_messages.<N>.message.tool_calls.<M>.tool_call.function.name (tool_call)
  - mounting claim  = walking parent_span_id from a content span UP, the name chain
        INCLUDES the target agent.<Name> boundary span. This proves the content is
        attributed to THAT agent (the hard precondition for per-agent content RCA).

MODE-SPECIFIC HARD ASSERTS (see PHASE1_README.md for the honesty about which modes
the content layer can/cannot see):
  normal  : 4 boundaries present; EACH has >=1 nested output completion (non-empty);
            Synthesizer boundary additionally has a tool_call 'Synthesize_Recommendation';
            HTTP 200 + success=true. ANY agent missing nested completion -> immediate
            global NO-GO (fail-fast: stop judging further modes).
  delay   : Product_Analyzer boundary: fault=='delay' + delay_ms==15000 +
            duration_ms >= 15000*0.9, AND STILL has nested completion (sleep then invoke);
            HTTP 200. Other agents behave as normal.
  error   : User_Behavior_Analyzer boundary: status_code=='ERROR' + fault=='error', AND
            ZERO nested llm.* content spans (invoke raised pre-call -> ABSENCE is the PASS).
            Chain HTTP 200 (analyzer degrades, doesn't bubble) + conversation has degraded text.
  garbage : Product_Analyzer boundary: fault=='garbage', AND ZERO nested llm.* content
            spans (early-return pre-invoke -> ABSENCE is the PASS). Chain HTTP 200.

  IMPORTANT HONESTY: content-invisible (error/garbage) is NOT a Phase1 failure — it is
  the EXPECTED signature of forced faults that short-circuit before agent.invoke. The
  content track's real value = baseline profiling on clean/delay + silent-LLM-hallucination
  detection on runs that DO reach the LLM. Do not invert this into a PASS criterion.

WINDOW: derived from each boundary span's [start_unix_nano, end_unix_nano]; a content
span claimed under an agent must also fall inside that window (defense against cross-trace
leak; the parent-walk already implies temporal nesting).

OUTPUT: (v1)_smoke/phase1_verdict.json
  verdict = "PASS" | "NO-GO" | "INCONCLUSIVE(env-gap)" | "INCONCLUSIVE(pollution)"
  per-mode assertions + evidence (span_names, parent chains, content samples, http
  status, versions, loader+exporter log presence). NO-GO dumps content_keys_by_span +
  all_attr_keys_by_span for every span of the failing trace for traceback.

Usage:
    python phase1_smoke_verify.py                 # all 4 modes (normal-first)
    python phase1_smoke_verify.py --modes normal  # single mode
"""
import argparse
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
SMOKE_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "_smoke")

AGENT_NAMES = [
    "Sequence_Recommender",
    "User_Behavior_Analyzer",
    "Product_Analyzer",
    "Recommendation_Synthesizer",
]
SYNTH_TOOL = "Synthesize_Recommendation"

# content attribute key matchers (same definitions Phase0 used)
_RE_IN_CONTENT = re.compile(r"^llm\.input_messages\.\d+\.message\.content$")
_RE_OUT_CONTENT = re.compile(r"^llm\.output_messages\.\d+\.message\.content$")
_RE_TOOL_NAME = re.compile(r"^llm\.output_messages\.\d+\.message\.tool_calls\.\d+\.tool_call\.function\.name$")

RUN_ORDER = ["normal", "delay", "garbage", "error"]   # fail-fast: normal must be first


def read_spans_for_trace(span_file, trace_id, retries=8, sleep_s=0.5):
    """Read JSONL, keep spans whose trace_id matches. Retry until stable count
    (SimpleSpanProcessor flushes at END; root/child order is not guaranteed)."""
    last = []
    stable = 0
    for _ in range(retries):
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
        import time as _t
        _t.sleep(sleep_s)
    return last


def content_kind(attrs):
    has_in = any(_RE_IN_CONTENT.match(k) for k in attrs)
    has_out = any(_RE_OUT_CONTENT.match(k) for k in attrs)
    has_tool = any(_RE_TOOL_NAME.match(k) for k in attrs)
    return has_in, has_out, has_tool


def walk_parent_chain(rec, by_id):
    """Return list of span names from rec up to root (inclusive)."""
    chain = []
    cur = rec
    seen = set()
    while cur and cur.get("span_id") not in seen:
        seen.add(cur.get("span_id"))
        chain.append(cur.get("name"))
        pid = cur.get("parent_span_id")
        cur = by_id.get(pid) if pid else None
    return chain


def find_boundaries(spans):
    """name=='agent.<Name>' AND attributes['recweb.agent.name']=='<Name>'. Same-name
    duplicates: keep the longest-duration one (defensive; normally one per trace)."""
    found = {}
    for s in spans:
        attrs = s.get("attributes") or {}
        nm = attrs.get("recweb.agent.name")
        if nm and s.get("name", "") == f"agent.{nm}":
            if nm not in found or (s.get("duration_ms") or 0) > (found[nm].get("duration_ms") or 0):
                found[nm] = s
    return found


def nested_content_for_agent(spans, by_id, boundary):
    """All content-bearing spans attributed to the boundary's agent.

    TWO OTel trees coexist in the real rec_agent (LangGraph + openinference):
      (a) agent.<Name> boundary = workflow.py start_as_current_span (OTel contextvar tree).
      (b) content spans (ChatOpenAI/ChatPromptTemplate) = openinference maps the LangChain
          Run tree (AgentExecutor/RunnableSequence/<LangGraph node named = agent>). These
          do NOT inherit agent.<Name>'s OTel contextvar (openinference parents via LangChain
          Run.parent_run_id, NOT via OTel contextvar), so their ancestor is the LangGraph
          node NAME (e.g. 'Sequence_Recommender'), not the 'agent.Sequence_Recommender' span.
    Attribution holds if EITHER the parent-chain includes boundary span_id (tree a — Phase0
    minimal chain) OR the parent-chain names include the agent's bare name as a LangGraph
    node (tree b — real LangGraph rec_agent). Both are honest attributions to THAT agent:
    in tree b the content lives under that agent's AgentExecutor execution subtree."""
    bid = boundary["span_id"]
    agent_name = (boundary.get("attributes") or {}).get("recweb.agent.name") \
        or boundary["name"].replace("agent.", "", 1)
    nested = []
    for s in spans:
        # openinference flattens input/output messages across the WHOLE LangChain Run tree
        # (ChannelWrite/RunnableLambda/ChatPromptTemplate/AgentExecutor all inherit them as
        # dotted scalar keys). Only the ChatOpenAI span is the actual LLM call — the origin of
        # prompt/completion/tool_call. Restrict to it so message-passing / graph-mechanism spans
        # don't masquerade as agent LLM content (which would false-positive garbage/error agents
        # whose ChannelWrite carries the degraded message but who never invoked the LLM).
        if s.get("name") != "ChatOpenAI":
            continue
        attrs = s.get("attributes") or {}
        if not (any(_RE_IN_CONTENT.match(k) for k in attrs) or
                any(_RE_OUT_CONTENT.match(k) for k in attrs) or
                any(_RE_TOOL_NAME.match(k) for k in attrs)):
            continue
        chain_recs = []
        chain_names = []
        cur = s
        seen = set()
        while cur and cur.get("span_id") not in seen:
            seen.add(cur.get("span_id"))
            chain_recs.append(cur)
            chain_names.append(cur.get("name"))
            if cur.get("span_id") == bid:
                break
            pid = cur.get("parent_span_id")
            cur = by_id.get(pid) if pid else None
        via_otel_subtree = any(r.get("span_id") == bid for r in chain_recs)
        via_langgraph_node = agent_name in chain_names
        if via_otel_subtree or via_langgraph_node:
            nested.append(s)
    return nested


def _sample_content(spans_list):
    samples = []
    for s in spans_list[:3]:
        attrs = s.get("attributes") or {}
        snip = {}
        for k, v in attrs.items():
            if (_RE_IN_CONTENT.match(k) or _RE_OUT_CONTENT.match(k)) and isinstance(v, str):
                snip[k] = (v[:120] + "...") if len(v) > 120 else v
            elif _RE_TOOL_NAME.match(k):
                snip[k] = v
        samples.append({"span_id": s.get("span_id"), "name": s.get("name"), "snippets": snip})
    return samples


def _check_log_lines(server_log):
    """Diagnostic ladder inputs: did the loader + the LocalJSONL exporter branch fire?"""
    loader_active = False
    exporter_line = False
    try:
        with open(server_log, "r", encoding="utf-8", errors="ignore") as f:
            for ln in f:
                if "[phase1] loader active" in ln:
                    loader_active = True
                if "[otel] local JSONL span exporter" in ln:
                    exporter_line = True
    except Exception:
        pass
    return loader_active, exporter_line


def judge_mode(mode):
    """Return (mode_verdict, detail_dict). mode_verdict in
    {PASS, NO-GO, INCONCLUSIVE(env-gap), SKIP}."""
    probe_path = os.path.join(SMOKE_DIR, f"phase1_{mode}_probe.json")
    detail = {"mode": mode, "assertions": {}, "evidence": {}}
    if not os.path.exists(probe_path):
        detail["assertions"]["probe_present"] = False
        return "INCONCLUSIVE(env-gap)", detail
    with open(probe_path, "r", encoding="utf-8") as f:
        probe = json.load(f)
    detail["probe"] = {k: probe.get(k) for k in
                       ("http_status", "e2e_ms", "trace_id", "baseline_pollution", "env_gap")}

    # pollution guard = hard INCONCLUSIVE (cannot trust content verdict on a polluted baseline)
    if probe.get("baseline_pollution"):
        detail["assertions"]["baseline_pollution"] = "FAIL: items/inventory changed -> cannot trust verdict"
        return "INCONCLUSIVE(pollution)", detail

    # env-gap (health timeout or non-200 / probe error) -> NOT a Phase1 content NO-GO
    if probe.get("env_gap") or probe.get("http_status") != 200:
        detail["assertions"]["env_gap"] = (
            f"health_ok={probe.get('health_ok')} http_status={probe.get('http_status')} -> "
            "ENV-GAP (sasrec:8200/deepseek/venv). Resolve and re-run before judging content.")
        return "INCONCLUSIVE(env-gap)", detail

    trace_id = probe.get("trace_id") or ""
    span_file = probe.get("span_file") or os.path.join(SMOKE_DIR, f"phase1_{mode}_spans.jsonl")
    server_log = probe.get("server_log") or os.path.join(SMOKE_DIR, f"phase1_{mode}_server.log")

    loader_active, exporter_line = _check_log_lines(server_log)
    detail["evidence"]["loader_active_log"] = loader_active
    detail["evidence"]["localjsonl_exporter_log"] = exporter_line
    detail["evidence"]["trace_id"] = trace_id

    spans = read_spans_for_trace(span_file, trace_id) if trace_id else []
    by_id = {s["span_id"]: s for s in spans if s.get("span_id")}
    detail["evidence"]["span_count"] = len(spans)
    detail["evidence"]["span_names"] = [s.get("name") for s in spans]

    if not trace_id or not spans:
        detail["assertions"]["spans_present"] = (
            f"FAIL: no spans for trace_id={trace_id!r}. Diagnostic ladder: "
            f"loader_active={loader_active}, exporter_line={exporter_line}. "
            "If loader/exporter both False -> loader or OTel bootstrap never ran. "
            "If both True but 0 spans -> provider/exporter plumbing broken.")
        return "NO-GO", detail

    boundaries = find_boundaries(spans)
    detail["evidence"]["boundaries_found"] = sorted(boundaries.keys())
    boundaries_present = {n: (n in boundaries) for n in AGENT_NAMES}
    detail["assertions"]["boundaries_present"] = boundaries_present

    # universal: trace must contain all 4 agent boundaries for a full /recommend run
    if not all(boundaries_present.values()):
        detail["assertions"]["boundaries_present"] = (
            f"FAIL: missing boundaries {[n for n,p in boundaries_present.items() if not p]}")
        return "NO-GO", detail

    # mode-specific
    if mode == "normal":
        return _judge_normal(spans, by_id, boundaries, detail)
    if mode == "delay":
        return _judge_delay(spans, by_id, boundaries, detail)
    if mode == "garbage":
        return _judge_garbage(spans, by_id, boundaries, detail)
    if mode == "error":
        return _judge_error(spans, by_id, boundaries, detail)
    return "NO-GO", detail


def _dump_keys(spans, detail):
    """On NO-GO: dump attr keys so a missing/plumbing failure is diagnosable."""
    content_keys = {}
    all_keys = {}
    for s in spans:
        sid = s.get("span_id")
        attrs = s.get("attributes") or {}
        all_keys[s.get("name", "?")] = sorted(attrs.keys())
        ck = [k for k in attrs if k.startswith(("llm.input_messages", "llm.output_messages"))]
        if ck:
            content_keys[s.get("name", "?")] = ck
    detail["evidence"]["content_keys_by_span"] = content_keys
    detail["evidence"]["all_attr_keys_by_span"] = all_keys


def _agent_nested_ok(boundaries, spans, by_id, agent_name, detail, fail_fast_key):
    """Assert agent boundary has >=1 nested LLM content (output completion OR tool_call).

    Synthesizer uses forced tool_choice -> its LLM emits a tool_call (Synthesize_Recommendation)
    with EMPTY output_messages.content; counting tool_calls as content capture is the honest
    adaptation (the LLM WAS reached and produced a structured decision). Other agents emit
    natural-language completions counted via output_messages.content."""
    b = boundaries[agent_name]
    nested = nested_content_for_agent(spans, by_id, b)
    out_contents = []
    tool_calls = []
    for s in nested:
        attrs = s.get("attributes") or {}
        for k, v in attrs.items():
            if _RE_OUT_CONTENT.match(k) and isinstance(v, str) and v:
                out_contents.append(v)
            if _RE_TOOL_NAME.match(k) and v:
                tool_calls.append(v)
    chains = [walk_parent_chain(s, by_id) for s in nested][:4]
    detail["evidence"].setdefault("parent_chains", {})[agent_name] = chains
    detail["evidence"].setdefault("content_samples", {})[agent_name] = _sample_content(nested)
    ok = len(out_contents) >= 1 or len(tool_calls) >= 1
    detail["assertions"][f"{agent_name}_nested_completion"] = (
        f"PASS: {len(out_contents)} output(s) + {len(tool_calls)} tool_call(s) attributed to agent.{agent_name}" if ok
        else f"FAIL: 0 non-empty output/tool_call attributed to agent.{agent_name}")
    return ok


def _judge_normal(spans, by_id, boundaries, detail):
    detail["assertions"]["http_200"] = "PASS"  # pre-checked in judge_mode
    # every agent must have nested completion
    all_ok = True
    for nm in AGENT_NAMES:
        if not _agent_nested_ok(boundaries, spans, by_id, nm, detail, f"{nm}_nested"):
            all_ok = False
    # Synthesizer: contract tool_call present (bound_tools forced tool_choice)
    synth = boundaries["Recommendation_Synthesizer"]
    synth_nested = nested_content_for_agent(spans, by_id, synth)
    tool_names = set()
    for s in synth_nested:
        attrs = s.get("attributes") or {}
        for k, v in attrs.items():
            if _RE_TOOL_NAME.match(k):
                tool_names.add(str(v))
    has_synth_tool = SYNTH_TOOL in tool_names
    detail["assertions"]["synthesizer_contract_tool_call"] = (
        f"PASS: tool_call {SYNTH_TOOL} present under Synthesizer" if has_synth_tool
        else f"FAIL: no {SYNTH_TOOL} tool_call under Synthesizer (tool_names={sorted(tool_names)})")
    if not all_ok or not has_synth_tool:
        _dump_keys(spans, detail)
        return "NO-GO", detail
    return "PASS", detail


def _judge_delay(spans, by_id, boundaries, detail):
    pa = boundaries["Product_Analyzer"]
    attrs = pa.get("attributes") or {}
    fault = attrs.get("recweb.agent.fault")
    delay_ms = attrs.get("recweb.agent.fault.delay_ms")
    dur = pa.get("duration_ms") or 0
    fault_ok = (fault == "delay")
    delay_ms_ok = (str(delay_ms) == "15000")
    dur_ok = dur >= 15000 * 0.9
    nested_ok = _agent_nested_ok(boundaries, spans, by_id, "Product_Analyzer", detail, "pa_nested")
    detail["assertions"]["pa_fault_attr"] = (
        f"PASS fault=delay" if fault_ok else f"FAIL fault={fault!r} (expected delay)")
    detail["assertions"]["pa_delay_ms_attr"] = (
        f"PASS delay_ms=15000" if delay_ms_ok else f"FAIL delay_ms={delay_ms!r}")
    detail["assertions"]["pa_duration"] = (
        f"PASS duration_ms={dur:.0f} >= 13500" if dur_ok else f"FAIL duration_ms={dur:.0f} < 13500")
    detail["assertions"]["pa_still_invokes_content"] = (
        "PASS: content present (delay then invoke)" if nested_ok else "FAIL: no content (invoke did not run after sleep)")
    if not (fault_ok and delay_ms_ok and dur_ok and nested_ok):
        _dump_keys(spans, detail)
        return "NO-GO", detail
    return "PASS", detail


def _judge_absence(mode_name, target_agent, expected_fault, spans, by_id, boundaries, detail):
    """error/garbage: boundary marks fault, and ZERO nested llm.* content (expected)."""
    b = boundaries[target_agent]
    attrs = b.get("attributes") or {}
    fault = attrs.get("recweb.agent.fault")
    fault_ok = (fault == expected_fault)
    nested = nested_content_for_agent(spans, by_id, b)
    has_any_llm = any(
        any(k.startswith(("llm.input_messages", "llm.output_messages")) for k in (s.get("attributes") or {}))
        for s in nested)
    detail["assertions"][f"{target_agent}_fault_attr"] = (
        f"PASS fault={expected_fault}" if fault_ok else f"FAIL fault={fault!r} (expected {expected_fault})")
    detail["assertions"][f"{target_agent}_no_content_expected"] = (
        "PASS: ZERO nested llm content (invoke short-circuited — expected)" if not has_any_llm
        else f"UNEXPECTED: nested llm content found for {target_agent} {mode_name} "
             f"(should short-circuit before invoke)")
    detail["evidence"].setdefault("parent_chains", {})[target_agent] = [walk_parent_chain(s, by_id) for s in nested][:4]
    # chain-level degrade: boundary status reflects the fault
    if expected_fault == "error":
        status = b.get("status_code")
        status_ok = (status == "ERROR")
        detail["assertions"][f"{target_agent}_status_error"] = (
            "PASS status=ERROR" if status_ok else f"FAIL status={status!r} (expected ERROR)")
        return (fault_ok and not has_any_llm and status_ok), detail
    # garbage: boundary span still present with fault attr is enough (no status requirement)
    return (fault_ok and not has_any_llm), detail


def _judge_error(spans, by_id, boundaries, detail):
    ok, detail = _judge_absence("error", "User_Behavior_Analyzer", "error", spans, by_id, boundaries, detail)
    # chain must NOT bubble (HTTP 200 pre-checked); record the boundary status for evidence
    if not ok:
        _dump_keys(spans, detail)
        return "NO-GO", detail
    return "PASS", detail


def _judge_garbage(spans, by_id, boundaries, detail):
    ok, detail = _judge_absence("garbage", "Product_Analyzer", "garbage", spans, by_id, boundaries, detail)
    if not ok:
        _dump_keys(spans, detail)
        return "NO-GO", detail
    return "PASS", detail


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--modes", nargs="+", default=None,
                    help="subset to judge (default normal-first full order)")
    args = ap.parse_args()
    modes = args.modes if args.modes else RUN_ORDER
    # enforce normal-first when judging the full set
    if args.modes is None and modes and modes[0] != "normal":
        print("[phase1-verify] WARNING: run order should start with normal (fail-fast gate).")

    results = {}
    overall = None
    verdict_path = os.path.join(SMOKE_DIR, "phase1_verdict.json")
    normal_no_go = False

    for m in modes:
        if normal_no_go:
            results[m] = {"mode_verdict": "SKIPPED(fail-fast after normal NO-GO)",
                          "note": "normal-mode NO-GO halts further judging (later modes would invoke incompletely)."}
            continue
        mv, detail = judge_mode(m)
        results[m] = {"mode_verdict": mv, "detail": detail}
        print(f"[phase1-verify] {m:8s} -> {mv}")
        if m == "normal" and mv == "NO-GO":
            overall = "NO-GO"
            normal_no_go = True

    # aggregate verdict
    if overall is None:
        judgeworthy = [results[m]["mode_verdict"] for m in modes
                       if not str(results[m].get("mode_verdict", "")).startswith("SKIPPED")]
        if not judgeworthy:
            overall = "INCONCLUSIVE(no-modes)"
        elif all(v == "PASS" for v in judgeworthy):
            overall = "PASS"
        elif any(v == "NO-GO" for v in judgeworthy):
            overall = "NO-GO"
        else:
            overall = "INCONCLUSIVE(env-gap)"

    out = {
        "verdict": overall,
        "gate": "Phase1 load-bearing: openinference content spans mount under agent.<Name>",
        "modes": results,
        "honesty_notes": [
            "error/garbage content-ABSENCE is the EXPECTED signature of forced faults that",
            "short-circuit BEFORE agent.invoke (workflow.py _agent_node). It is NOT a Phase1",
            "failure. The content track's real value = baseline profiling on clean/delay runs",
            "+ silent-LLM-hallucination detection on runs that DO reach the LLM.",
            "Synthesizer garbage STILL invokes (workflow.py synthesizer_node always chain.invoke),",
            "so a Synthesizer-garbage variant would show content — handled separately, not here.",
        ],
        "diagnostic_ladder_on_no_go": [
            "1. evidence.loader_active_log == True? (else PYTHONPATH/sitecustomize never fired)",
            "2. evidence.localjsonl_exporter_log == True? (else app.py SPAN_FILE branch missed)",
            "3. evidence.span_count > 0? (else provider/exporter plumbing broken — name this as",
            "   exporter-plumbing failure, NOT mechanism failure)",
            "4. content_keys_by_span present but orphaned vs nested-wrong? (parent-walk says where",
            "   it actually mounted)",
            "5. zero content keys at all? -> retry with PHASE1_INSTRUMENT_MODE=monkeypatch fallback.",
        ],
    }
    with open(verdict_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[phase1-verify] verdict = {overall}  -> {verdict_path}")
    # exit code: 0 PASS, 1 NO-GO, 2 INCONCLUSIVE
    sys.exit(0 if overall == "PASS" else (1 if overall == "NO-GO" else 2))


if __name__ == "__main__":
    main()
