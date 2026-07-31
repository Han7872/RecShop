#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Phase0 承重闸 (load-bearing gate) — openinference content-span capture smoke.

WHAT THIS PROVES (and NOTHING more):
  On the EXACT prod stack (langchain==0.3.24 / langchain-core==0.3.56 /
  langchain-openai==0.3.14 / opentelemetry-{api,sdk}==1.42.1), a one-line
  `LangChainInstrumentor().instrument()` from openinference-instrumentation-langchain
  captures CONTENT-layer spans for a MINIMAL LangChain chain:
    - full prompt        -> attr keys `llm.input_messages.*.message.content`
    - completion         -> attr keys `llm.output_messages.*.message.content`
    - tool_calls         -> `llm.output_messages.*.message.tool_calls.*.tool_call.function.{name,arguments}`
  and that those content spans NEST under a manual `agent.<Name>` boundary span exactly
  like services/recommendation_agent/workflow.py L144/L169 create theirs.

WHAT THIS IS NOT:
  - NOT an injector, NOT a detector (Phase0 excludes both by contract).
  - NOT proof of capture on the real 4-agent rec_agent (that is Phase1, via a
    sitecustomize loader on a temp process — this smoke only proves the mechanism on a
    minimal chain).
  - Runs OFFLINE with a fake chat model: 0 tokens, 0 network, 0 DeepSeek, touches no
    running service, no OTLP/Jaeger, no DB. `--live` is an OPTIONAL secondary confirmation
    only (default off); the authoritative verdict is the offline fake-model path.

MUST run under the throwaway venv's python (scratchpad/phase0_venv/Scripts/python.exe).
The bootstrap `phase0_setup_and_run.sh` builds that venv + installs deps + runs this.

Verdict JSON  -> (v1)_smoke/phase0_verdict.json
Local JSONL   -> (v1)_smoke/phase0_spans.jsonl   (assertion G survival probe)
"""

import argparse
import json
import os
import re
import sys
import threading
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Inlined copy of services/recommendation_agent/local_span_exporter.py
# (Review suggestion ④: copy the ~60 self-contained lines INLINE rather than adding
#  services/recommendation_agent/ to sys.path — keeps this smoke fully standalone and
#  free of any accidental coupling to the real service tree. Assertion G then proves the
#  content spans survive this exact scalar-only JSONL serializer that Phase1 will reuse.)
# ---------------------------------------------------------------------------
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

_WRITE_LOCK = threading.Lock()


def _fmt_trace_id(tid):
    try:
        return format(tid, "032x")
    except Exception:
        return ""


def _fmt_span_id(sid):
    try:
        if sid in (None, 0):
            return ""
        return format(sid, "016x")
    except Exception:
        return ""


def _serialize_attributes(attrs):
    """Keep only JSON-serializable scalars/strings (mirrors the real exporter EXACTLY:
    non-scalars are dropped to str()). openinference emits FLATTENED scalar keys, so the
    content survives this — that is precisely what assertion G verifies empirically."""
    out = {}
    if not attrs:
        return out
    for k, v in attrs.items():
        try:
            if isinstance(v, (str, int, float, bool)):
                out[k] = v
            elif isinstance(v, (list, tuple)):
                out[k] = [x for x in v if isinstance(x, (str, int, float, bool))]
            else:
                out[k] = str(v)
        except Exception:
            out[k] = "<unserializable>"
    return out


class InlineLocalJSONLSpanExporter(SpanExporter):
    """Verbatim behavior of the prod LocalJSONLSpanExporter, inlined for standalone smoke."""

    def __init__(self, file_path):
        self._file_path = file_path
        d = os.path.dirname(file_path)
        if d:
            os.makedirs(d, exist_ok=True)
        # truncate at start so a rerun's assertion G reads only THIS run's spans
        with open(file_path, "w", encoding="utf-8"):
            pass

    def export(self, spans):
        lines = []
        for sp in spans:
            try:
                ctx = sp.get_span_context()
                parent = sp.parent
                status = sp.status
                rec = {
                    "trace_id": _fmt_trace_id(ctx.trace_id),
                    "span_id": _fmt_span_id(ctx.span_id),
                    "parent_span_id": _fmt_span_id(parent.span_id) if parent else "",
                    "name": sp.name,
                    "start_unix_nano": sp.start_time,
                    "end_unix_nano": sp.end_time,
                    "duration_ms": ((sp.end_time - sp.start_time) / 1e6)
                    if (sp.end_time and sp.start_time) else None,
                    "status_code": status.status_code.name if status else "UNSET",
                    "kind": sp.kind.name if sp.kind is not None else None,
                    "attributes": _serialize_attributes(sp.attributes),
                }
                lines.append(json.dumps(rec, ensure_ascii=False))
            except Exception:
                continue
        if not lines:
            return SpanExportResult.SUCCESS
        try:
            with _WRITE_LOCK:
                with open(self._file_path, "a", encoding="utf-8") as f:
                    f.write("\n".join(lines) + "\n")
            return SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE

    def shutdown(self):
        return None

    def force_flush(self, timeout_millis=30000):
        return True


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SENTINEL = "PHASE0_SENTINEL_9f3a2b"   # unique token planted in the SYSTEM prompt (assertion C)
CONTRACT_TOOL = "Synthesize_Recommendation"   # the Synthesizer contract tool name (assertion E)
CONTRACT_FIELD = "recommended_product"        # a required arg field of that contract (assertion E)
BOUNDARY_SPAN = "agent.Test"                  # mirrors workflow.py `agent.<Name>` (assertion F)

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, os.pardir, os.pardir, os.pardir))
_OUT_DIR = os.path.join(_REPO_ROOT, "datasets", "_archive", "agentfault", "agentfault", "_smoke")
VERDICT_PATH = os.path.join(_OUT_DIR, "phase0_verdict.json")
JSONL_PATH = os.path.join(_OUT_DIR, "phase0_spans.jsonl")


def _pkg_version(dist_name):
    try:
        from importlib.metadata import version
        return version(dist_name)
    except Exception as e:
        return f"<unknown:{e}>"


# ---------------------------------------------------------------------------
# TracerProvider: InMemory (authoritative) + inlined JSONL + optional Console.
# set_tracer_provider MUST run BEFORE instrument().
# ---------------------------------------------------------------------------
def build_provider(console=False):
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    resource = Resource.create({"service.name": "phase0_smoke_agentfault"})
    provider = TracerProvider(resource=resource)
    mem = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(mem))                      # authoritative
    provider.add_span_processor(SimpleSpanProcessor(InlineLocalJSONLSpanExporter(JSONL_PATH)))
    if console:
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)   # BEFORE instrument()
    return provider, mem


def build_fake_model():
    """Offline fake chat model returning a completion WITH content AND a Synthesizer
    tool_call. Both FakeMessagesListChatModel and GenericFakeChatModel are BaseChatModel
    subclasses, so they traverse the SAME CallbackManager.on_chat_model_start/on_llm_end
    path openinference hooks — a sound proxy for a real emission."""
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
    from langchain_core.messages import AIMessage

    ai = AIMessage(
        content="Recommended product B01ABC based on the user's recent electronics history.",
        tool_calls=[{
            "name": CONTRACT_TOOL,
            "args": {
                CONTRACT_FIELD: "B01ABC",
                "reason": "matches the buyer's recent taste",
                "confidence": 0.91,
            },
            "id": "call_c1",
            "type": "tool_call",
        }],
    )
    return FakeMessagesListChatModel(responses=[ai])


def build_live_model():
    """OPTIONAL secondary confirmation only (default off). Points langchain_openai at
    DeepSeek (OpenAI-compatible). Requires DEEPSEEK_API_KEY + explicit --live. Never the
    authoritative path. Caller is responsible for Clash bypass (NO_PROXY) in the env."""
    from langchain_openai import ChatOpenAI
    key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not key:
        raise RuntimeError("--live requested but DEEPSEEK_API_KEY not set")
    return ChatOpenAI(
        model=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"),
        api_key=key,
        base_url=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
        temperature=0,
        timeout=30,
    )


def run_chain(provider, live=False):
    """Instrument, build minimal chain, invoke INSIDE an `agent.Test` boundary span."""
    from openinference.instrumentation.langchain import LangChainInstrumentor
    LangChainInstrumentor().instrument(tracer_provider=provider)

    from opentelemetry import trace
    from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
    from langchain_core.messages import HumanMessage

    system_prompt = (
        "You are the Recommendation Synthesizer. " + SENTINEL +
        " Given the user's history, choose the single best product and call "
        + CONTRACT_TOOL + "."
    )
    prompt = ChatPromptTemplate.from_messages(
        [("system", system_prompt), MessagesPlaceholder("messages")]
    )
    model = build_live_model() if live else build_fake_model()
    chain = prompt | model

    tracer = trace.get_tracer("phase0.smoke")
    ran_ok = True
    err = None
    # boundary span mirrors workflow.py: start_as_current_span("agent.<Name>") + recweb.agent.name
    with tracer.start_as_current_span(BOUNDARY_SPAN) as span:
        span.set_attribute("recweb.agent.name", "Test")
        try:
            chain.invoke({"messages": [HumanMessage(
                content="Recommend one product for a buyer who likes wireless earbuds.")]})
        except Exception as e:  # noqa: BLE001
            ran_ok = False
            err = repr(e)
    provider.force_flush()
    return ran_ok, err


# ---------------------------------------------------------------------------
# Assertion engine — prefix/regex matching (NOT exact indices), so it is robust across
# openinference key-naming across versions. On any B-F fail we dump every content key.
# ---------------------------------------------------------------------------
_RE_IN_CONTENT = re.compile(r"^llm\.input_messages\.\d+\.message\.content$")
_RE_OUT_CONTENT = re.compile(r"^llm\.output_messages\.\d+\.message\.content$")
_RE_TOOL_NAME = re.compile(r"^llm\.output_messages\.\d+\.message\.tool_calls\.\d+\.tool_call\.function\.name$")


def _content_keys(attrs):
    return sorted(k for k in attrs
                  if k.startswith(("llm.input_messages", "llm.output_messages"))
                  or k in ("input.value", "output.value"))


def evaluate(spans):
    """spans: list[ReadableSpan] from InMemorySpanExporter. Returns (assertions, evidence)."""
    by_id = {}
    for sp in spans:
        by_id[sp.context.span_id] = sp

    # gather across all spans
    all_input_content = []
    all_output_content = []
    tool_name_hits = []      # (span, key, value)
    boundary_present = any(sp.name == BOUNDARY_SPAN for sp in spans)

    for sp in spans:
        attrs = dict(sp.attributes or {})
        for k, v in attrs.items():
            if _RE_IN_CONTENT.match(k) and isinstance(v, str) and v:
                all_input_content.append(v)
            if _RE_OUT_CONTENT.match(k) and isinstance(v, str) and v:
                all_output_content.append(v)
            if _RE_TOOL_NAME.match(k) and v == CONTRACT_TOOL:
                tool_name_hits.append((sp, k, v))

    # --- flexible tool-name fallback (openinference key naming can vary by version) ---
    if not tool_name_hits:
        for sp in spans:
            for k, v in dict(sp.attributes or {}).items():
                if ("tool_call" in k) and k.endswith("function.name") and v == CONTRACT_TOOL:
                    tool_name_hits.append((sp, k, v))

    # E: for each tool-name hit, the sibling .arguments must contain the contract field
    tool_args_ok = False
    tool_arg_evidence = None
    tool_carrier_span = None
    for sp, name_key, _ in tool_name_hits:
        prefix = name_key[: name_key.rfind(".name")]  # ...tool_call.function
        args_key = prefix + ".arguments"
        av = (sp.attributes or {}).get(args_key)
        if isinstance(av, str) and CONTRACT_FIELD in av:
            tool_args_ok = True
            tool_arg_evidence = {"name_key": name_key, "arguments_key": args_key, "arguments": av}
            tool_carrier_span = sp
            break
    # broad fallback: any key containing 'arguments' whose value carries the field, on a
    # span that also carries the contract tool name
    if not tool_args_ok and tool_name_hits:
        sp = tool_name_hits[0][0]
        for k, v in dict(sp.attributes or {}).items():
            if "arguments" in k and isinstance(v, str) and CONTRACT_FIELD in v:
                tool_args_ok = True
                tool_arg_evidence = {"name_key": tool_name_hits[0][1], "arguments_key": k, "arguments": v}
                tool_carrier_span = sp
                break

    # F: the span carrying output content (completion) must chain up to agent.Test.
    # Walk the FULL parent chain by span_id (openinference may insert an intermediate
    # chain/LLM span between agent.Test and the message-bearing span).
    nesting_ok = False
    nesting_path = None
    output_carrier = None
    for sp in spans:
        if any(_RE_OUT_CONTENT.match(k) for k in (sp.attributes or {})):
            output_carrier = sp
            break
    if output_carrier is not None:
        path = []
        cur = output_carrier
        seen = set()
        while cur is not None:
            path.append(cur.name)
            if cur.name == BOUNDARY_SPAN:
                nesting_ok = True
                break
            pctx = cur.parent
            if pctx is None:
                break
            pid = pctx.span_id
            if pid in seen:
                break
            seen.add(pid)
            cur = by_id.get(pid)
        nesting_path = path

    a = {
        "A_ran_ok": None,  # filled by caller (needs err + span count)
        "B_prompt_captured": bool(all_input_content),
        "C_prompt_is_real": any(SENTINEL in c for c in all_input_content),
        "D_completion_captured": bool(all_output_content),
        "E_toolcalls_captured": bool(tool_name_hits) and tool_args_ok,
        "F_nesting_ok": nesting_ok,
    }
    evidence = {
        "span_count": len(spans),
        "span_names": [sp.name for sp in spans],
        "boundary_span_present": boundary_present,
        "input_content_sample": (all_input_content[0][:240] if all_input_content else None),
        "output_content_sample": (all_output_content[0][:240] if all_output_content else None),
        "tool_name_hits": [k for _, k, _ in tool_name_hits],
        "tool_arg_evidence": tool_arg_evidence,
        "nesting_path": nesting_path,
        "tool_carrier_span": (tool_carrier_span.name if tool_carrier_span else None),
        "output_carrier_span": (output_carrier.name if output_carrier is not None else None),
    }
    return a, evidence


def check_jsonl_survives():
    """G (SECONDARY): the content keys must survive the scalar-only JSONL serializer."""
    try:
        with open(JSONL_PATH, "r", encoding="utf-8") as f:
            recs = [json.loads(ln) for ln in f if ln.strip()]
    except Exception as e:  # noqa: BLE001
        return False, {"error": repr(e)}
    has_in = has_out = has_tool = False
    for r in recs:
        for k, v in (r.get("attributes") or {}).items():
            if _RE_IN_CONTENT.match(k) and v:
                has_in = True
            if _RE_OUT_CONTENT.match(k) and v:
                has_out = True
            if ("tool_call" in k) and k.endswith("function.name") and v == CONTRACT_TOOL:
                has_tool = True
    ok = has_in and has_out and has_tool
    return ok, {"jsonl_records": len(recs), "has_input_content": has_in,
                "has_output_content": has_out, "has_tool_name": has_tool}


def dump_all_content_keys(spans):
    out = {}
    for sp in spans:
        ck = _content_keys(dict(sp.attributes or {}))
        if ck:
            out[sp.name] = ck
    return out


def main():
    ap = argparse.ArgumentParser(description="Phase0 openinference content-span smoke")
    ap.add_argument("--console", action="store_true", help="also print spans to console")
    ap.add_argument("--live", action="store_true",
                    help="OPTIONAL secondary confirmation via real DeepSeek (not authoritative)")
    args = ap.parse_args()

    os.makedirs(_OUT_DIR, exist_ok=True)

    # H (SECONDARY): pins intact — the 0.3.24-compat proof. Recorded from the running venv.
    lc_ver = _pkg_version("langchain")
    lccore_ver = _pkg_version("langchain-core")
    oi_ver = _pkg_version("openinference-instrumentation-langchain")
    pins_intact = (lc_ver == "0.3.24" and lccore_ver == "0.3.56")

    provider, mem = build_provider(console=args.console)
    ran_ok, err = run_chain(provider, live=args.live)
    spans = list(mem.get_finished_spans())

    assertions, evidence = evaluate(spans)
    assertions["A_ran_ok"] = bool(ran_ok and len(spans) >= 1 and err is None)
    jsonl_ok, jsonl_ev = check_jsonl_survives()
    assertions["G_jsonl_survives"] = jsonl_ok
    assertions["H_pins_intact"] = pins_intact

    core_keys = ["A_ran_ok", "B_prompt_captured", "C_prompt_is_real",
                 "D_completion_captured", "E_toolcalls_captured", "F_nesting_ok"]
    core_pass = all(assertions[k] for k in core_keys)
    verdict = "PASS" if core_pass else "NO-GO"

    result = {
        "verdict": verdict,
        "core_pass": core_pass,
        "note": ("Phase0 承重闸: openinference content-span capture on minimal chain, "
                 "prod stack pinned. Offline fake-model path is authoritative; "
                 "--live is only a secondary confirmation. Proves MECHANISM on a minimal "
                 "chain, NOT real 4-agent rec_agent capture (=Phase1)."),
        "mode": "live-deepseek" if args.live else "offline-fake-model",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version.split()[0],
        "versions": {
            "langchain": lc_ver,
            "langchain_core": lccore_ver,
            "langchain_openai": _pkg_version("langchain-openai"),
            "opentelemetry_sdk": _pkg_version("opentelemetry-sdk"),
            "openinference_instrumentation_langchain": oi_ver,
        },
        "assertions": assertions,
        "assertions_legend": {
            "A_ran_ok": ">=1 span, no exception",
            "B_prompt_captured": "llm.input_messages.*.message.content non-empty",
            "C_prompt_is_real": "input content contains the planted SYSTEM-prompt SENTINEL",
            "D_completion_captured": "llm.output_messages.*.message.content non-empty",
            "E_toolcalls_captured": ("tool_call.function.name=='%s' AND arguments contains '%s'"
                                     % (CONTRACT_TOOL, CONTRACT_FIELD)),
            "F_nesting_ok": "completion span chains up to the manual 'agent.Test' boundary",
            "G_jsonl_survives": "content keys survive the scalar-only local JSONL exporter (SECONDARY)",
            "H_pins_intact": "langchain==0.3.24 & langchain-core==0.3.56 unchanged (SECONDARY, 0.3.24-compat proof)",
        },
        "evidence": evidence,
        "jsonl_evidence": jsonl_ev,
        "ran_error": err,
    }
    if not core_pass:
        result["content_keys_by_span"] = dump_all_content_keys(spans)
        result["all_attr_keys_by_span"] = {
            sp.name: sorted((sp.attributes or {}).keys()) for sp in spans
        }

    with open(VERDICT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(json.dumps({"verdict": verdict, "assertions": assertions,
                      "openinference": oi_ver, "spans": len(spans),
                       "span_names": evidence["span_names"]},
                      ensure_ascii=False, indent=2))
    print("verdict written -> %s" % VERDICT_PATH)
    return 0 if core_pass else 1


if __name__ == "__main__":
    sys.exit(main())
