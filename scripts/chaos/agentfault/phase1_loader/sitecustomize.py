"""Phase1 openinference content-layer loader (auto-imported by the interpreter).

This file MUST be named exactly `sitecustomize.py` and live in a directory placed
at the FRONT of PYTHONPATH by the launcher. CPython imports `sitecustomize` once
during site initialization, BEFORE app.py / workflow.py import langchain. That is
the required ordering: openinference must hook langchain's callback stack before
the first ChatOpenAI / AgentExecutor is built (which happens lazily on the first
/recommend, well after startup — so ordering is safe either way).

The PROVEN-PRIMARY strategy (offline-verified for this exact combo of
opentelemetry 1.42.1 + openinference-langchain 0.1.67 + langchain 0.3.24):
    LangChainInstrumentor().instrument()   # NO tracer_provider kwarg
openinference's _instrument EAGERLY captures trace_api.get_tracer_provider() into
an OITracer wrapping a ProxyTracer. At this moment app.py has NOT yet called
set_tracer_provider, so get_tracer_provider() returns the global
ProxyTracerProvider. When app.py later calls set_tracer_provider(_otel_provider),
the proxy late-binds to the real provider, and content spans route to its
SimpleSpanProcessor(LocalJSONL) -> SPAN_FILE. This was proven end-to-end offline
(content span captured by the REAL LocalJSONLSpanExporter, parent-chain
terminating at agent.<Name>, after a set_tracer_provider-then-instrument re-order).

CRITICAL: this loader NEVER calls set_tracer_provider itself. OTel's
set_tracer_provider is guarded by a Once: a second call is a silent no-op (with a
warning). If we called it here, app.py's real bootstrap would be neutered and
SPAN_FILE would never receive any span. Only wrap/observe — never set.

FALLBACK (env PHASE1_INSTRUMENT_MODE=monkeypatch): if the live smoke shows ZERO
content spans under the minimal approach (would indicate some proxy-late-bind
regression in a future langchain/otel rev), this loader instead monkeypatches
trace_api.set_tracer_provider: when app.py calls it with the real provider, we
let that call through (installs _otel_provider + LocalJSONL), then call
LangChainInstrumentor().instrument(tracer_provider=provider) with the SAME
provider. This bypasses the proxy-late-bind entirely. Off by default; the minimal
approach is preferred because it carries no Once-guard risk.

SAFETY: the whole body is guarded. Any failure prints to stderr and is swallowed
so app.py startup is never broken by an observability loader.
"""
import os
import sys
import traceback


def _err(msg):
    try:
        sys.stderr.write("[phase1] " + msg + "\n")
        sys.stderr.flush()
    except Exception:
        pass


def _do_minimal():
    """PRIMARY: instrument with no tracer_provider kwarg; rely on proxy late-bind.

    NOTE: is_instrumented_by_opentelemetry is a BOOLEAN PROPERTY in this
    openinference-instrumentation rev (not a method) — do not call it.
    """
    from openinference.instrumentation.langchain import LangChainInstrumentor
    li = LangChainInstrumentor()
    if li.is_instrumented_by_opentelemetry:
        _err("langchain already instrumented (skip).")
        return
    li.instrument()
    _err("instrumented openinference-langchain (minimal; proxy late-bind).")


def _do_monkeypatch():
    """FALLBACK: wrap set_tracer_provider so instrument() binds the REAL provider."""
    from opentelemetry import trace as trace_api
    from openinference.instrumentation.langchain import LangChainInstrumentor

    original = trace_api.set_tracer_provider
    state = {"done": False}

    def wrapper(provider, *args, **kwargs):
        result = original(provider, *args, **kwargs)
        # instrument exactly once, with the provider app.py just installed.
        if not state["done"]:
            state["done"] = True
            try:
                LangChainInstrumentor().instrument(tracer_provider=provider)
                _err("instrumented openinference-langchain via monkeypatch wrapper.")
            except Exception as e:  # never break the real set_tracer_provider
                _err(f"monkeypatch instrument failed (ignored): {e!r}")
        return result

    # Keep the wrapper installed (idempotent guard = state["done"]); do not restore,
    # because app.py's set is the only legitimate set we expect.
    trace_api.set_tracer_provider = wrapper
    _err("wrapped trace_api.set_tracer_provider (monkeypatch mode armed).")


def _main():
    # env gate: only activate when explicitly requested. Keeps the venv python safe
    # to use elsewhere as a plain interpreter.
    if os.environ.get("PHASE1_INSTRUMENT", "").strip() != "1":
        return
    _err("loader active, will hook langchain instrumentation.")

    mode = os.environ.get("PHASE1_INSTRUMENT_MODE", "minimal").strip().lower() or "minimal"
    if mode == "monkeypatch":
        _do_monkeypatch()
    elif mode == "minimal":
        _do_minimal()
    elif mode == "off":
        _err("PHASE1_INSTRUMENT_MODE=off -> no instrumentation (loader is a no-op).")
    else:
        _err(f"unknown PHASE1_INSTRUMENT_MODE={mode!r}; falling back to minimal.")
        _do_minimal()


try:
    _main()
except Exception:
    # Observability must NEVER break the host process.
    _err("loader crashed (ignored):\n" + traceback.format_exc())
