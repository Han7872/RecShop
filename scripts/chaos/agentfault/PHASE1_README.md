# Phase1 — openinference content-layer mounting gate (GO / NO-GO)

Phase1 is the **load-bearing gate** for the whole agent-fault content track. It
proves that openinference content spans (prompt / completion / tool_calls) really
mount under `workflow.py`'s `agent.<Name>` boundary spans on the **real**
rec_agent (LangGraph 4-agent chain, `create_openai_tools_agent` + `AgentExecutor`
+ `ChatOpenAI(DeepSeek)` + `@tool` SASRec), across the normal / delay / error /
garbage fault modes, and survive the **real** `local_span_exporter.py` to the
`SPAN_FILE` JSONL. If Phase1 collapses, the downstream content track + judge +
dual-track plans switch capture strategy.

This doc is the design + diagnosis reference. It is intentionally explicit about
the three candidate failure modes so a NO-GO is diagnosed by evidence, not guess.

## What is already plumbed (Phase1 does NOT re-lay)

- `services/recommendation_agent/app.py` OTel bootstrap: `set_tracer_provider` +
  `BatchSpanProcessor(OTLP)` + `SPAN_FILE`-gated `SimpleSpanProcessor(LocalJSONL)`.
- `local_span_exporter.py` (`_serialize_attributes` is **scalar-only**: lists flatten
  to scalars, list-of-dicts -> `[]`, non-scalars -> `str(v)`).
- `workflow.py` `agent.<Name>` boundary span wrapping `agent.invoke` (L144/L152),
  `recweb.agent.name` attribute, `_apply_agent_fault` (delay/error/garbage), and
  `trace_id` write-back into the `/recommend` response for trace-precise windowing.

## Surface (red lines honored)

New files live ONLY under `scripts/chaos/agentfault/` (+ smoke outputs under
`(v1)_smoke/`). Zero edits under `services/**`, `chaos_k8s_runner.py`,
`agentchaos_runner.py` (read-only reference for the `start_temp_instance` pattern).
Nothing is installed into the conda env; langchain is never upgraded.

## The three candidate failure modes (name them on NO-GO)

1. **Provider-timing (biggest historical assumption).** openinference's `_instrument`
   EAGERLY captures `get_tracer_provider()` into an `OITracer`. If it binds the wrong
   provider, content spans route to a no-op provider and `SPAN_FILE` gets zero content
   spans (no error raised). **Mitigation**: sitecustomize instruments with NO
   `tracer_provider` kwarg, relying on `ProxyTracerProvider` late-bind to the real
   provider once `app.py` calls `set_tracer_provider`. This exact sequence was proven
   offline (content span captured by the real `LocalJSONLSpanExporter`, parent-chain
   terminating at `agent.<Name>`). **Fallback** (`PHASE1_INSTRUMENT_MODE=monkeypatch`):
   wrap `set_tracer_provider` so `instrument(tracer_provider=provider)` runs with the
   real provider right after app.py installs it. Off by default (Once-guard risk).
   See `phase1_loader/sitecustomize.py`.

2. **Shadowing drift.** With `--system-site-packages` + `pip install`, pip could drag
   `opentelemetry-*` / `wrapt` / `openinference-instrumentation` INTO the venv
   site-packages, shadowing conda's copies and splitting global provider state from
   app.py's bootstrap. Version-equality alone does NOT catch this. **Mitigation**:
   `pip install --no-deps` of only the 3 openinference wheels + a post-install
   `module.__file__`-resolves-to-conda assertion for `langchain` / `langchain_core` /
   `wrapt` (necessary AND sufficient). See `phase1_bootstrap.sh`.

3. **Exporter-plumbing (distinct from mechanism failure).** `_serialize_attributes`
   is scalar-only. openinference 0.1.67 emits content as dotted scalar string keys
   (str values), so it survives the `str(v)` branch — **proven offline against the
   real exporter**. But if a future openinference rev emits list-of-dicts, the JSONL
   would silently drop them (`[]`). The smoke catches this (absent content keys ->
   NO-GO); diagnose it as exporter-plumbing, NOT as mechanism failure. The verifier
   dumps `content_keys_by_span` + `all_attr_keys_by_span` on NO-GO for exactly this.

## Honesty about which modes the content layer can see

Forced `error` and `garbage` faults **short-circuit BEFORE `agent.invoke`**
(`workflow.py` `_agent_node`: error raises at `_apply_agent_fault`, garbage
early-returns). Both therefore produce **ZERO content spans** — their signature
lives only in the `agent.<Name>` boundary span attrs (`recweb.agent.fault`) +
absent children + the downstream degraded message. The smoke ASSERTS this absence
as the PASS criterion for those modes; it is not a Phase1 failure.

The content track's real value is therefore:
- (a) baseline content profiling on clean / delay runs (delay still invokes after
  the sleep, so content IS produced), and
- (b) detecting **silent** LLM misbehavior — hallucination / contract violation —
  on runs that DO reach the LLM (the `task成败=契约有效性` target).

**Important asymmetry**: the Synthesizer node (`workflow.py synthesizer_node`) is a
bare chain, NOT an `_agent_node`; its `garbage` fault only sets an attribute and it
**ALWAYS** calls `synthesizer_chain.invoke`. So a *Synthesizer-garbage* variant would
still produce content. The four modes verified here instrument `_agent_node` faults
(analyzer-class), where content-invisibility is correct. Do not generalize
"garbage => no content" to the Synthesizer.

## Order + fail-fast

Run order is **normal, delay, garbage, error**. normal is first because it proves
"can mount at all". If normal yields zero nested content spans, the run halts —
later modes invoke incompletely and have no further diagnostic value. The verifier
also stops on a normal NO-GO.

## Files

| file | role |
|---|---|
| `phase1_bootstrap.sh` | throwaway venv (inherits conda langchain) + `--no-deps` openinference slice + `__file__`-origin drift/shadow assert |
| `phase1_loader/sitecustomize.py` | auto-imported loader; PRIMARY minimal `instrument()`, FALLBACK env-gated `monkeypatch`; env-gated by `PHASE1_INSTRUMENT=1`; never raises |
| `phase1_launcher.py` | per-mode ephemeral rec_agent instance (venv python) + one `/recommend` probe + best-effort CHECKSUM pollution guard; writes `phase1_<mode>_probe.json` |
| `phase1_smoke_verify.py` | offline judge: parent-chain walk mounting asserts per mode; fail-fast; writes `phase1_verdict.json` |
| `phase1_run.sh` | orchestrator: bootstrap -> normal-first launch (fail-fast on env-gap) -> remaining modes -> verify |

## Env prerequisites (NOT Phase1 NO-GOs — resolve before judging)

- `sasrec_api` up and model-loaded on :8200 (`GET /health` 200). Sequence_Recommender
  calls it; 8200 down -> normal mode 500 -> classified ENV-GAP, not a content failure.
- DeepSeek reachable from the temp instance (`.env` has `DEEPSEEK_API_KEY` /
  `DEEPSEEK_API_BASE` / `DEEPSEEK_MODEL`, auto-loaded by app.py). Clash is bypassed
  for loopback only; external reachability is your responsibility.
- Persistent OTel stack NOT required: temp instance OTLP points at a dead port
  (`127.0.0.1:14318`) so the BSP silently fails and content spans go only to the
  local JSONL (no Jaeger pollution).
- `items` / `inventory` checksum baseline: `1088112223` / `944901079`. This service
  does no DB write; a change = baseline pollution -> verdict INCONCLUSIVE(pollution).

## Verdict semantics

`phase1_verdict.json` `verdict`:
- `PASS` — all judged modes' mode-specific asserts pass; content track is viable.
- `NO-GO` — content spans missing or mis-mounted; switch capture strategy.
- `INCONCLUSIVE(env-gap)` — DeepSeek / sasrec / health not reachable; resolve + re-run.
- `INCONCLUSIVE(pollution)` — baseline checksum changed; re-establish baseline first.
