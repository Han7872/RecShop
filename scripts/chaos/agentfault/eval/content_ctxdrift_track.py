# -*- coding: utf-8 -*-
"""content_ctxdrift_track.py — the context_drift STRUCTURAL track (step 3, NEW work).

Scope (does NOT rebuild Tier-A): Tier-A (`eval_agentfault_tierA.py` ->
`(upstream batch)BASELINE_RESULTS.md`) already established the deterministic CONTENT
locators for the STRUCTURED / hallucinate families and showed that context_drift is NOT localized
by any of the span-duration / output-content methods it tried — every such method landed at or
below the random floor. (No digits are pinned here on purpose: the dataset is re-collected from
time to time, so the exact Tier-A numbers are PARSED AT RUNTIME from BASELINE_RESULTS.md — see
`parse_tiera_hit1` / `--tiera-md` — and interpolated into every figure this script emits.) This
script adds ONLY the piece Tier-A deliberately left out: the context_drift input-structure locator
+ outcome stratification, plus the honest X-vs-Y determination of whether that locator is an
observable detector or a ledger-sourced oracle.

WHAT context_drift IS: a pre-call deletion (injector `_maybe_context_drop`) that strips the
UPSTREAM agent's HumanMessage (name==dropped_agent) from the DOWNSTREAM target agent's input
messages, before the target's LLM call. GT root (`root_cause_set`) = the DOWNSTREAM TARGET agent
whose input was stripped (NOT the upstream agent that was removed).

=== PIVOTAL X-vs-Y FINDING (evidence-based; re-derived at runtime from the spans) ===
The captured per-agent LLM input prompts (openinference `llm.input_messages.*` on the ChatOpenAI
child spans) DO exist — but they are captured by the LangChain `on_chat_model_start` callback,
which fires in `BaseChatModel.generate()` BEFORE `_generate` runs, whereas the injector deletes
the upstream message INSIDE the patched `_generate` (`messages[:] = kept`). Consequence: the span
records the PRE-deletion message set. Empirically (counts re-derived at runtime by
`probe_span_observability`, and reported verbatim in the emitted MD/JSON), the dropped upstream
message is STILL PRESENT in the target agent's captured input in every drift trace checked.
=> The deletion is NOT observable from the captured trajectory. This is case **Y (ledger-only)**.
The locator is therefore a **structural canary ORACLE** (deterministic upper bound; injection-
point-visible signal sourced from the injection ledger). A real trajectory monitor could
reconstruct it only from per-agent input-message structure that reflects what the model ACTUALLY
received (post-pruning) — the captured content-layer reflects the pre-injection state instead, a
known instrumentation gap. We label it exactly so and never dress it up as a learned/observable
method (mirrors Tier-A's contract-oracle framing).

Outputs (under (upstream batch) only):
  * content_ctxdrift_results.json
  * RESULTS_CONTENT_CTXDRIFT.md

Offline, deterministic, no network / no services / no LLM. Run from repo root:
  PYTHONIOENCODING=utf-8 python scripts/chaos/agentfault/eval/content_ctxdrift_track.py \
      --dataset-dir (archived) agentfault_v2

DEPENDENCY (run first, do NOT edit): compute_context_drift_outcome.py must have produced
<dataset-dir>/context_drift_outcomes.json. This script reads it read-only.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
from collections import Counter, defaultdict

import numpy as np
import pandas as pd

# ---- repo paths + reuse import shim (COPIED from eval_agentfault_tierA.py) ---------------------
HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
CTK = os.path.join(REPO_ROOT, "scripts", "chaos", "ctk")
sys.path.insert(0, CTK)

from m9_score import mrcbench, K_LIST            # noqa: E402  (MRCBench 四族 scorer, entity-agnostic)

# m9_score re-wraps sys.stdout at import, which GC-closes the shared stdout buffer. Reopen a
# fresh, independent stream on a dup of fd 1 so our prints survive (same shim as Tier-A).
try:
    sys.stdout = io.TextIOWrapper(io.FileIO(os.dup(1), "w"), encoding="utf-8", errors="replace")
except Exception:
    pass

AGENTS = [
    "Sequence_Recommender", "User_Behavior_Analyzer",
    "Product_Analyzer", "Recommendation_Synthesizer",
]
SEEDS = [0, 1, 2, 3, 4]

# fixed forward pipeline order -> the "expected upstream set" for each agent (all strictly-earlier
# agents). Used by the (would-be) structural detector reconstruction and to document the canary.
PIPELINE = list(AGENTS)
UPSTREAM_OF = {a: PIPELINE[:i] for i, a in enumerate(PIPELINE)}

RANDOM_ANALYTIC_HIT1 = 1.0 / len(AGENTS)   # uniform pick among the candidate agents

# n below which a stratum is an anecdote (count only), never a rate/statistic.
SMALL_N_FOR_STATS = 10
# max run_ids we enumerate inline in prose / JSON stratum before truncating.
MAX_ENUMERATE_RUN_IDS = 10

# ===================================================================================
#  Tier-A quoted numbers — PARSED AT RUNTIME from BASELINE_RESULTS.md (the authoritative,
#  regenerated source). NEVER hardcoded: Tier-A is re-run whenever the dataset is re-collected,
#  and stale constants baked into this script's prose silently contradict its own computed fields.
#  On parse failure we FAIL LOUDLY and emit a clearly-marked "unavailable" string — never a
#  fallback constant.
# ===================================================================================
TIERA_UNAVAILABLE = "UNAVAILABLE (Tier-A parse FAILED — see stderr)"

# short key -> substring that identifies the baseline row in the Tier-A family table.
TIERA_QUOTE_PATTERNS = {
    "trivial_corrected": "Trivial span-anomaly [CORRECTED dur]",
    "rf_infra": "Supervised ref: RF-infra ceiling",
    "rf_content": "Supervised ref: RF-content ceiling",
    "contract_oracle": "Contract oracle",
}


class TierAParseError(RuntimeError):
    """Raised when BASELINE_RESULTS.md cannot be parsed for a family's Hit@1 column."""


def _cell_float(cell):
    """'0.139±0.000' / '0.139' / 'N/A' -> float | None."""
    s = str(cell).strip().strip("*`")
    if not s or s.upper().startswith("N/A"):
        return None
    s = s.split("±")[0].strip()
    try:
        return float(s)
    except Exception:
        return None


def parse_tiera_hit1(md_path, family):
    """Parse the Hit@1 (headline) column of the Tier-A table for one fault family.

    Returns an ordered dict {baseline_label_exactly_as_in_md: float|None}.
    Raises TierAParseError if the file / heading / table / Hit@1 column is not found.
    """
    if not os.path.exists(md_path):
        raise TierAParseError(f"Tier-A file not found: {md_path}")
    with open(md_path, "r", encoding="utf-8") as fh:
        lines = fh.read().splitlines()

    head = f"### Fault family: {family}"
    start = next((i for i, ln in enumerate(lines) if ln.strip().startswith(head)), None)
    if start is None:
        raise TierAParseError(f"heading '{head}' not found in {md_path}")

    rows, hdr_seen, hit1_col = {}, False, None
    for ln in lines[start + 1:]:
        st = ln.strip()
        if st.startswith("#"):                       # next section -> family table is over
            break
        if not st.startswith("|"):
            continue
        cells = [c.strip() for c in st.strip("|").split("|")]
        if not hdr_seen:
            if any(c.lower().startswith("hit@1") for c in cells):
                hit1_col = next(i for i, c in enumerate(cells) if c.lower().startswith("hit@1"))
                hdr_seen = True
            continue
        if set("".join(cells)) <= set("-: "):        # markdown separator row
            continue
        if hit1_col is None or len(cells) <= hit1_col:
            continue
        rows[cells[0].strip("*")] = _cell_float(cells[hit1_col])

    if not hdr_seen:
        raise TierAParseError(f"no table header with a 'Hit@1' column under '{head}' in {md_path}")
    if not rows:
        raise TierAParseError(f"table under '{head}' in {md_path} has no data rows")
    return rows


def tiera_quotes(md_path, family, wanted=("trivial_corrected", "rf_infra", "rf_content")):
    """-> (by_label {label: float|str}, by_key {short_key: float|str}, ok, err_msg).

    On ANY parse problem (missing file/heading/table, or a wanted row absent) we print a loud
    error to stderr AND stdout and return the clearly-marked unavailable string for every wanted
    quote. We never substitute a remembered constant.
    """
    try:
        rows = parse_tiera_hit1(md_path, family)
        by_label, by_key, missing = {}, {}, []
        for key in wanted:
            pat = TIERA_QUOTE_PATTERNS[key]
            hit = next(((lab, v) for lab, v in rows.items() if pat in lab), None)
            if hit is None or hit[1] is None:
                missing.append(f"{key} ({pat!r})")
                continue
            by_label[hit[0]] = hit[1]
            by_key[key] = hit[1]
        if missing:
            raise TierAParseError(f"row(s) not found / non-numeric in family '{family}': "
                                  + "; ".join(missing))
        return by_label, by_key, True, ""
    except TierAParseError as e:
        msg = (f"[FATAL][tiera-parse] could not read Tier-A Hit@1 quotes for family '{family}' "
               f"from {md_path}: {e}. Emitting '{TIERA_UNAVAILABLE}' — refusing to fall back to "
               f"stale hardcoded constants. Re-run eval_agentfault_tierA.py or pass --tiera-md.")
        print(msg, file=sys.stderr)
        try:
            print(msg)
        except Exception:
            pass
        by_label = {TIERA_QUOTE_PATTERNS[k]: TIERA_UNAVAILABLE for k in wanted}
        by_key = {k: TIERA_UNAVAILABLE for k in wanted}
        return by_label, by_key, False, str(e)


# ------------------------------------------------------------------ helpers ---
def roots_of(cell):
    s = str(cell).strip()
    if s == "" or s.lower() == "nan":
        return []
    return [x.strip() for x in s.split(";") if x.strip()]


def _fnum(x):
    try:
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return np.nan
        return float(s)
    except Exception:
        return np.nan


def rank_to_metrics(ranked, roots):
    return mrcbench(ranked, roots)


def hit1(metrics_list):
    """mean Hit@1 over a list of per-case metric dicts (None-safe)."""
    vals = [m["hit@1"] for m in metrics_list if m is not None]
    return float(np.mean(vals)) if vals else None


def full_macro(metrics_list, keys=("hit@1", "hit@3", "ndcg@3", "hit@5", "recall@R", "mrr")):
    scored = [m for m in metrics_list if m is not None]
    if not scored:
        return {k: None for k in keys}
    return {k: float(np.mean([m[k] for m in scored])) for k in keys}


# ===================================================================================
#  X-vs-Y span probe — re-derive the verdict from the captured spans at runtime.
#  Parses the span jsonl DIRECTLY (does NOT import injector_smoke, which has import-time
#  env side effects). If spans are unavailable (gitignored; may be absent on a fresh clone),
#  we degrade gracefully and record that the verdict falls back to the documented prior finding.
# ===================================================================================
SCENARIO_TARGET_DROP = {
    "ctxdrift_ub_from_seq":     ("User_Behavior_Analyzer",      "Sequence_Recommender"),
    "ctxdrift_prod_from_ub":    ("Product_Analyzer",            "User_Behavior_Analyzer"),
    "ctxdrift_synth_from_prod": ("Recommendation_Synthesizer",  "Product_Analyzer"),
}


def _agent_ancestor(span, by_id):
    """Walk parent chain to the bare-agent-name span (Sequence_Recommender / ...)."""
    cur, seen = span, 0
    while cur is not None and seen < 40:
        nm = cur.get("name", "")
        if nm in AGENTS:
            return nm
        cur = by_id.get(cur.get("parent_span_id"))
        seen += 1
    return None


def _input_msg_names(span):
    """List of (role, name, content_len) for a ChatOpenAI span's llm.input_messages.*."""
    a = span.get("attributes", {})
    out, i = [], 0
    while f"llm.input_messages.{i}.message.role" in a:
        out.append((
            a.get(f"llm.input_messages.{i}.message.role"),
            a.get(f"llm.input_messages.{i}.message.name"),
            len(str(a.get(f"llm.input_messages.{i}.message.content", ""))),
        ))
        i += 1
    return out


def _resolved_agent(span):
    """The EXECUTING agent name the injector now stamps on the `agentfault.resolved_input` span
    (attr `agentfault.resolved_input.agent`). This is a LEGITIMATE observable — a real trajectory
    monitor knows which agent's LLM call each span belongs to — and REPLACES the fragile
    parent-chain walk (`_agent_ancestor`) that broke on the captured JSONL (parent chain severed).
    It is NOT injection GT. Returns the agent name, or None if the attribute is ABSENT (old data)."""
    a = span.get("attributes", {})
    v = a.get("agentfault.resolved_input.agent")
    return v if v else None


def _resolved_msg_names(span):
    """POST-pruning received message .name list, emitted by the injector (agentfault_injector.py
    `_maybe_context_drop`) on the `agentfault.resolved_input` span. This is the OBSERVABLE signal
    (what the model ACTUALLY received after `messages[:]=kept`), unlike openinference
    `llm.input_messages.*` which is the on_chat_model_start PRE-pruning snapshot.

    Reads the scalar JSON convenience attr `agentfault.resolved_input.msg_names` first, then falls
    back to the per-message `agentfault.resolved_input_messages.*.message.name` schema. Returns a
    list of names (may contain None), or None if the attribute is ABSENT (old / un-recollected data).
    """
    a = span.get("attributes", {})
    raw = a.get("agentfault.resolved_input.msg_names")
    if raw is not None:
        try:
            v = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(v, list):
                return v
        except Exception:
            pass
    if ("agentfault.resolved_input_messages.0.message.name" in a or
            "agentfault.resolved_input_messages.0.message.role" in a):
        out, i = [], 0
        while (f"agentfault.resolved_input_messages.{i}.message.role" in a or
               f"agentfault.resolved_input_messages.{i}.message.name" in a):
            out.append(a.get(f"agentfault.resolved_input_messages.{i}.message.name"))
            i += 1
        return out
    return None


def observed_rank_from_spans(trace_spans):
    """OBSERVABLE structural DETECTOR (X): read the POST-pruning received-message names the injector
    now emits into the trajectory, then apply the chaosgraph AllRequiredNodesVisited SET-DIFF shape
    (invariants.py L37-51): for each agent, missing = expected_upstream - received. This reads what
    the model ACTUALLY received from the captured spans (NOT the injection ledger, NOT a parent
    walk) -> a genuine detector.

    Attribution is by the NEW `agentfault.resolved_input.agent` attribute (`_resolved_agent`), which
    the injector stamps on every resolved_input span — replacing the fragile `_agent_ancestor`
    parent-chain walk that broke on the captured JSONL.

    Predicted root = the SHALLOWEST agent (earliest in PIPELINE order) whose missing set is
    non-empty. Rationale: if the pruning propagates downstream (later agents also lose the upstream),
    several agents show a missing upstream; the ROOT / injection point is the earliest one
    (introduce-vs-propagate, FALAT-style). If exactly one agent is anomalous, this reduces to
    flagging it.

    Returns (ranked_or_None, observed_present):
      * observed_present=False -> no `agentfault.resolved_input.agent` attribute in this trace's
        spans (old / un-recollected data) -> caller must fall back to the ledger oracle.
      * observed_present=True, ranked=[predicted_root, ...] -> a real detection from the trajectory.
      * observed_present=True, ranked=None -> attr present but no upstream missing (clean; no
        detection, no false positive).
    """
    if not trace_spans:
        return None, False
    received_by_agent = {}
    observed = False
    for s in trace_spans:
        agent = _resolved_agent(s)          # observable attribution (NOT parent walk / NOT ledger)
        if agent is None:
            continue
        observed = True
        names = _resolved_msg_names(s)
        if names is None:
            names = []
        received_by_agent.setdefault(agent, set()).update(n for n in names if n)
    if not observed:
        return None, False
    # chaosgraph set-diff: an agent is anomalous when an expected upstream message is missing from
    # the set it actually received (mirrors AllRequiredNodesVisited: missing = required - visited).
    # SHALLOWEST-flag: the earliest such agent in PIPELINE order = the introduce/injection point;
    # later anomalous agents are downstream propagation of the same drop.
    missing_agents = [a for a in PIPELINE
                      if a in received_by_agent and (set(UPSTREAM_OF[a]) - received_by_agent[a])]
    if not missing_agents:
        return None, True
    predicted_root = missing_agents[0]      # shallowest = earliest in the fixed pipeline
    rest = [a for a in AGENTS if a != predicted_root]
    return [predicted_root] + rest, True


def load_scenario_spans(dataset_dir):
    """{scenario: {trace_id: [span, ...]}} for the context_drift scenarios. Parses the span jsonl
    once (same defensive parse as probe_span_observability). Empty dict if spans/ absent."""
    spans_dir = os.path.join(dataset_dir, "spans")
    out = {}
    if not os.path.isdir(spans_dir):
        return out
    for scen in SCENARIO_TARGET_DROP:
        fp = os.path.join(spans_dir, f"{scen}.jsonl")
        if not os.path.exists(fp):
            continue
        by_trace = defaultdict(list)
        with open(fp, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    s = json.loads(line)
                except Exception:
                    continue
                by_trace[s.get("trace_id")].append(s)
        out[scen] = dict(by_trace)
    return out


def probe_span_observability(dataset_dir):
    """For each context_drift scenario, count traces where the DROPPED upstream message is still
    PRESENT in the TARGET agent's captured LLM input. present==all -> deletion invisible -> Y.

    Returns (verdict, evidence_dict). verdict in {'Y', 'X', 'UNKNOWN'}.
    """
    spans_dir = os.path.join(dataset_dir, "spans")
    ev = {"per_scenario": {}, "spans_dir_present": os.path.isdir(spans_dir),
          "content_layer_input_captured": None, "note": ""}
    if not os.path.isdir(spans_dir):
        ev["note"] = ("spans/ not present locally (gitignored) -> could not re-derive the verdict "
                      "from data; falling back to the documented prior investigation (Y).")
        return "UNKNOWN", ev

    total_present = total_traces = 0
    any_content_input = False
    for scen, (target, dropped) in SCENARIO_TARGET_DROP.items():
        fp = os.path.join(spans_dir, f"{scen}.jsonl")
        if not os.path.exists(fp):
            continue
        by_trace = defaultdict(list)
        with open(fp, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    s = json.loads(line)
                except Exception:
                    continue
                by_trace[s.get("trace_id")].append(s)
        present = absent = 0
        for tid, ss in by_trace.items():
            by_id = {s["span_id"]: s for s in ss}
            cos = [s for s in ss if s.get("name") == "ChatOpenAI"
                   and _agent_ancestor(s, by_id) == target]
            if not cos:
                continue
            found_dropped = False
            for s in cos:
                msgs = _input_msg_names(s)
                if msgs:
                    any_content_input = True
                if any(nm == dropped for (_r, nm, _l) in msgs):
                    found_dropped = True
            if found_dropped:
                present += 1
            else:
                absent += 1
        ev["per_scenario"][scen] = {
            "target": target, "dropped_upstream": dropped,
            "traces_with_target_llm_span": present + absent,
            "dropped_msg_PRESENT_in_captured_input": present,
            "dropped_msg_ABSENT_in_captured_input": absent,
        }
        total_present += present
        total_traces += present + absent

    ev["content_layer_input_captured"] = any_content_input
    ev["totals"] = {"traces": total_traces, "dropped_present": total_present,
                    "dropped_absent": total_traces - total_present}
    if total_traces == 0:
        ev["note"] = "no target-agent ChatOpenAI spans found -> cannot determine; fall back to Y."
        return "UNKNOWN", ev
    if total_present == total_traces:
        # dropped msg present in EVERY trace's captured input -> deletion invisible in spans -> Y
        ev["note"] = (
            f"In {total_present}/{total_traces} (100%) of drift traces the dropped upstream message "
            "is STILL PRESENT in the target "
            "agent's captured llm.input_messages (name match). The content-layer input IS captured, "
            "but by the on_chat_model_start callback (fires before _generate, hence before the "
            "injector's messages[:]=kept prune) -> the span reflects the PRE-deletion state. The "
            "deletion is therefore NOT observable from the trajectory -> Y (ledger-only oracle).")
        return "Y", ev
    if total_present == 0:
        ev["note"] = ("dropped msg ABSENT from every target input -> deletion IS visible in spans "
                      "-> X (observable structural detector).")
        return "X", ev
    ev["note"] = (f"mixed: dropped msg present in {total_present}/{total_traces} traces -> "
                  "partial observability; treated conservatively as oracle (Y) unless fully absent.")
    return "Y", ev


# ===================================================================================
#  Locators
# ===================================================================================
def oracle_rank_from_ledger(journal_agent):
    """Structural canary ORACLE (Y): the injection ledger records the injection-point agent
    (= the downstream TARGET whose input lost the upstream message). Rank it first, rest in
    canonical order. This is an injection-point-visible signal sourced from the ledger, NOT an
    observable detector — deterministic upper bound only."""
    if not journal_agent:
        return None
    rest = [a for a in AGENTS if a != journal_agent]
    return [journal_agent] + rest


def convlen_anomaly_rank(row, clean_row, rng):
    """OUTPUT-FEATURE-BLIND locator: rank agents by |Δ conv_<A>_text_len| between the drift case
    and its same-carrier clean (normal) case. Predicts the agent whose OUTPUT text length deviates
    most from clean. This is an output-content feature (per-agent produced text), NOT input
    structure -> expected ~random on context_drift (a pure input deletion the system recovers from).
    Seeded random tie-break (deltas can tie, esp. at 0)."""
    deltas = []
    for a in AGENTS:
        d = row.get(a, np.nan)
        c = clean_row.get(a, np.nan)
        delta = abs(d - c) if (not np.isnan(d) and not np.isnan(c)) else -1.0
        deltas.append(delta)
    deltas = np.array(deltas, dtype=float)
    tie = rng.random(len(AGENTS))
    order = np.lexsort((tie, -deltas))
    return [AGENTS[j] for j in order]


# ===================================================================================
#  Main
# ===================================================================================
def _parse_args(argv=None):
    ap = argparse.ArgumentParser(description="context_drift structural track (oracle/detector) + "
                                             "output-feature-blind contrast + outcome stratification.")
    ap.add_argument("--dataset-dir", default=os.path.join(REPO_ROOT, "datasets", "_archive", "agentfault", "agentfault_v2"),
                    help="dataset tree root (has dataset_agentfault.csv + journal/ + spans/ + "
                         "context_drift_outcomes.json). Default (archived) agentfault_v2.")
    ap.add_argument("--tiera-md", default=None,
                    help="path to the Tier-A BASELINE_RESULTS.md whose Hit@1 columns are quoted "
                         "(default: <dataset-dir>/BASELINE_RESULTS.md). Parsed at runtime; on "
                         "parse failure the quotes are emitted as an explicit 'UNAVAILABLE' "
                         "marker — never as stale constants.")
    return ap.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    base = args.dataset_dir if os.path.isabs(args.dataset_dir) else \
        os.path.join(REPO_ROOT, args.dataset_dir)
    csv_path = os.path.join(base, "dataset_agentfault.csv")
    outcomes_path = os.path.join(base, "context_drift_outcomes.json")
    out_json = os.path.join(base, "content_ctxdrift_results.json")
    out_md = os.path.join(base, "RESULTS_CONTENT_CTXDRIFT.md")
    tiera_md = args.tiera_md or os.path.join(base, "BASELINE_RESULTS.md")
    if tiera_md and not os.path.isabs(tiera_md):
        tiera_md = os.path.join(REPO_ROOT, tiera_md)

    if not os.path.exists(csv_path):
        print(f"[ERR] {csv_path} not found")
        return 2
    if not os.path.exists(outcomes_path):
        print(f"[ERR] {outcomes_path} not found — run compute_context_drift_outcome.py first")
        return 2

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    outcomes = json.load(open(outcomes_path, "r", encoding="utf-8"))

    # ---- Tier-A quotes: PARSED (never hardcoded). Loud failure -> "UNAVAILABLE" marker. ----
    tiera_by_label, tiera, tiera_ok, tiera_err = tiera_quotes(tiera_md, "context_drift")
    # contract-oracle quotes for the consolidation section (structured families)
    oracle_q = {}
    for fam in ("wrong_item_pick", "format_violation"):
        _bl, _bk, _ok, _ = tiera_quotes(tiera_md, fam, wanted=("contract_oracle",))
        oracle_q[fam] = _bk["contract_oracle"]
        tiera_ok = tiera_ok and _ok

    # ---- build clean-carrier (normal) conv-len lookup: carrier_seq_id -> {agent: len} ----
    clean_by_carrier = {}
    for _, r in df[df["kind"] == "normal"].iterrows():
        clean_by_carrier[r["carrier_seq_id"]] = {a: _fnum(r.get(f"conv_{a}_text_len")) for a in AGENTS}

    # ---- context_drift rows ----
    cd = df[df["kind"] == "context_drift"].copy().reset_index(drop=True)
    cd["_roots"] = cd["root_cause_set"].map(roots_of)

    # journal 'agent' (target = injection-point) per case — LEDGER-SOURCED signal for the oracle.
    def journal_agent(run_id):
        jp = os.path.join(base, "journal", f"{run_id}.json")
        jj = json.load(open(jp, "r", encoding="utf-8")) if os.path.exists(jp) else {}
        return jj.get("agent", ""), jj.get("drop", ""), jj.get("trace_id", "")

    # spans grouped {scenario: {trace_id: [span]}} for the OBSERVABLE detector (post-pruning attr).
    scenario_spans = load_scenario_spans(base)

    # ---- X-vs-Y probe (evidence-based) ----
    verdict, evidence = probe_span_observability(base)
    if verdict == "X":
        locator_label = "structural detector (observable from spans)"
    else:
        locator_label = ("structural canary ORACLE (deterministic upper bound; injection-point-"
                         "visible signal sourced from the injection ledger; a real detector would "
                         "reconstruct it from per-agent input-message structure, which requires "
                         "capturing per-agent input prompts that reflect the post-pruning message "
                         "set actually sent to the model — a known instrumentation gap)")

    # ---- per-case scoring ----
    per_case = []   # dicts: run_id, scenario, roots, outcome, oracle_metrics, convlen_metrics(seed-avg), ...
    rngs = {s: np.random.default_rng(1000 + s) for s in SEEDS}
    n_target_eq_root = 0
    for _, r in cd.iterrows():
        run_id = r["run_id"]
        scenario = r["scenario_id"]
        roots = r["_roots"]
        outcome = outcomes.get(run_id, {}).get("outcome", "unknown")
        jagent, jdrop, jtrace = journal_agent(run_id)
        if jagent in roots:
            n_target_eq_root += 1

        # oracle (ledger-sourced): predict the injection-point/target agent
        oracle_ranked = oracle_rank_from_ledger(jagent)
        oracle_m = rank_to_metrics(oracle_ranked, roots) if oracle_ranked else None

        # OBSERVABLE detector: read the post-pruning received-message names from THIS case's spans
        # (trace-keyed) and apply the chaosgraph set-diff. Present -> genuine detector; absent (old
        # data) -> fall back to the ledger oracle and label the case so.
        trace_spans = scenario_spans.get(scenario, {}).get(jtrace)
        observed_ranked, observed_present = observed_rank_from_spans(trace_spans)
        if observed_present and observed_ranked:
            source = "observed_detector"
            observed_m = rank_to_metrics(observed_ranked, roots)
            case_m = observed_m          # the reported locator result for this case
        else:
            source = "ledger_oracle"     # attr absent (or no detection) -> upper-bound fallback
            observed_m = None
            case_m = oracle_m

        # output-feature-blind conv-len anomaly (seed-averaged over tie-breaks)
        clean = clean_by_carrier.get(r["carrier_seq_id"])
        drift_lens = {a: _fnum(r.get(f"conv_{a}_text_len")) for a in AGENTS}
        conv_ms = []
        if clean is not None:
            for s in SEEDS:
                ranked = convlen_anomaly_rank(drift_lens, clean, rngs[s])
                conv_ms.append(rank_to_metrics(ranked, roots))
        conv_m = full_macro(conv_ms) if conv_ms else None

        per_case.append({
            "run_id": run_id, "scenario_id": scenario, "roots": roots,
            "outcome": outcome, "ledger_target_agent": jagent, "ledger_dropped_upstream": jdrop,
            "oracle_metrics": oracle_m, "convlen_metrics": conv_m,
            "source": source, "observed_metrics": observed_m, "case_metrics": case_m,
        })

    n_cd = len(per_case)

    # ---- aggregate: oracle + conv-len, overall + per scenario ----
    def agg_hit1(sel, key):
        return hit1([c[key] for c in per_case if sel(c)])

    def agg_full(sel, key):
        return full_macro([c[key] for c in per_case if sel(c) and c[key] is not None])

    all_sel = lambda c: True
    oracle_overall = agg_full(all_sel, "oracle_metrics")
    conv_overall = agg_full(all_sel, "convlen_metrics")

    scenarios = sorted(set(c["scenario_id"] for c in per_case))
    oracle_by_scen = {sc: agg_hit1(lambda c, sc=sc: c["scenario_id"] == sc, "oracle_metrics")
                      for sc in scenarios}
    conv_by_scen = {sc: agg_hit1(lambda c, sc=sc: c["scenario_id"] == sc, "convlen_metrics")
                    for sc in scenarios}

    # ---- OBSERVABLE-vs-ORACLE source split (do NOT merge) ----
    # observable-detector: genuine method (reads post-pruning attr from spans). Reported ONLY over
    # cases that carry the attribute. oracle-fallback: upper bound, over the remaining (attr-absent)
    # cases. On un-recollected data the attr is absent everywhere -> all cases fall back -> the
    # oracle-fallback number == the step-3 oracle result (proves backward-compat).
    source_counts = Counter(c["source"] for c in per_case)
    is_observed = lambda c: c["source"] == "observed_detector"
    is_oracle_fb = lambda c: c["source"] == "ledger_oracle"
    observable_hit1 = agg_hit1(is_observed, "observed_metrics")
    observable_full = agg_full(is_observed, "observed_metrics")
    oracle_fb_hit1 = agg_hit1(is_oracle_fb, "oracle_metrics")
    oracle_fb_full = agg_full(is_oracle_fb, "oracle_metrics")
    n_observed = sum(1 for c in per_case if is_observed(c))
    n_oracle_fb = sum(1 for c in per_case if is_oracle_fb(c))

    # constant-Synthesizer reference: always predict Recommendation_Synthesizer first
    def const_synth_rank():
        rest = [a for a in AGENTS if a != "Recommendation_Synthesizer"]
        return ["Recommendation_Synthesizer"] + rest
    const_ms = [rank_to_metrics(const_synth_rank(), c["roots"]) for c in per_case]
    const_synth_overall = full_macro(const_ms)
    const_synth_by_scen = {sc: hit1([rank_to_metrics(const_synth_rank(), c["roots"])
                                     for c in per_case if c["scenario_id"] == sc])
                           for sc in scenarios}

    # ---- outcome stratification ----
    outcome_counts = Counter(c["outcome"] for c in per_case)
    strata = {}
    for oc in ("recovered", "silent_wrong", "unknown"):
        sel = lambda c, oc=oc: c["outcome"] == oc
        n = sum(1 for c in per_case if sel(c))
        if n == 0:
            continue
        strata[oc] = {
            "n": n,
            "oracle_hit1": agg_hit1(sel, "oracle_metrics"),
            "convlen_hit1": agg_hit1(sel, "convlen_metrics"),
            "run_ids": ([c["run_id"] for c in per_case if sel(c)]
                        if n <= MAX_ENUMERATE_RUN_IDS else None),
        }

    # derived facts used by the narrative (never hardcoded in prose)
    silent_ids = [c["run_id"] for c in per_case if c["outcome"] == "silent_wrong"]
    n_silent = len(silent_ids)
    n_recovered = outcome_counts.get("recovered", 0)
    # Constant-Synthesizer support: how many cases are actually rooted at the Synthesizer, and in
    # which scenarios (was hardcoded as "12/36 / synth_from_prod only").
    synth_cases = [c for c in per_case if "Recommendation_Synthesizer" in c["roots"]]
    n_synth_root = len(synth_cases)
    synth_scens = sorted(set(c["scenario_id"] for c in synth_cases))
    # conv-len scenario spread (drives the "unstable / carried by one scenario" wording)
    conv_pairs = sorted(((sc, v) for sc, v in conv_by_scen.items() if v is not None),
                        key=lambda kv: kv[1])
    conv_lo = conv_pairs[0] if conv_pairs else (None, None)
    conv_hi = conv_pairs[-1] if conv_pairs else (None, None)
    conv_spread = (conv_hi[1] - conv_lo[1]) if conv_pairs else None
    conv_h1 = conv_overall.get("hit@1")
    conv_scen_str = ", ".join(f"{_f3(v)} `{sc}`" for sc, v in reversed(conv_pairs)) or "N/A"
    conv_vs_floor = ("N/A" if conv_h1 is None else
                     "above" if conv_h1 > RANDOM_ANALYTIC_HIT1 + 1e-9 else
                     "below" if conv_h1 < RANDOM_ANALYTIC_HIT1 - 1e-9 else "at")
    conv_stability = ("wildly scenario-unstable" if (conv_spread or 0) >= 0.25 else
                      "scenario-varying" if (conv_spread or 0) >= 0.10 else
                      "scenario-stable")
    silent_caveat = _silent_wrong_caveat(n_silent, silent_ids, n_cd)

    # ---- assemble results json ----
    results = {
        "dataset_dir": base,
        "n_context_drift_cases": n_cd,
        "ledger_target_equals_gt_root": f"{n_target_eq_root}/{n_cd}",
        "x_vs_y": {
            "verdict": verdict,
            "locator_label": locator_label,
            "evidence": evidence,
        },
        "outcome_counts": dict(outcome_counts),
        "scorer": (f"m9_score.mrcbench (上游 4.3 四族); {len(AGENTS)}-agent single-root space"),
        "reference_rows": {
            "Random (analytic)": {
                "hit@1": RANDOM_ANALYTIC_HIT1,
                "note": (f"1/{len(AGENTS)} agents; Hit@3={min(3, len(AGENTS)) / len(AGENTS):.3f}, "
                         f"Hit@5={min(5, len(AGENTS)) / len(AGENTS):.3f} analytic"),
            },
            "Constant-Synthesizer": {
                "hit@1": const_synth_overall["hit@1"],
                "by_scenario": const_synth_by_scen,
                "note": (f"always blame Recommendation_Synthesizer; = fraction of ctxdrift cases "
                         f"rooted at Synthesizer ({n_synth_root}/{n_cd}"
                         + (f", scenario(s): {', '.join(synth_scens)}" if synth_scens else "")
                         + ")"),
            },
        },
        "structural_locator": {
            "kind": ("oracle" if verdict != "X" else "detector"),
            "label": locator_label,
            "overall": oracle_overall,
            "hit1_by_scenario": oracle_by_scen,
            "source": "injection ledger / journal['agent'] (injection-point = downstream target)",
        },
        "observable_vs_oracle": {
            "note": ("Per-case source split. observed_detector = a GENUINE method: reads the "
                     "post-pruning received-message names the injector now emits into the "
                     "trajectory (span `agentfault.resolved_input`, attr "
                     "`agentfault.resolved_input.msg_names`) and applies the chaosgraph "
                     "AllRequiredNodesVisited set-diff (missing = expected_upstream - received). "
                     "ledger_oracle = the upper-bound fallback used when that attribute is ABSENT "
                     "(old / un-recollected data). The two numbers are NOT merged. On THIS dataset "
                     f"the attribute is present in {n_observed}/{n_cd} case(s) and absent in "
                     f"{n_oracle_fb}/{n_cd}"
                     + (" — so every case falls back to the ledger oracle and the oracle-fallback "
                        "Hit@1 reproduces the step-3 result exactly (backward-compat)."
                        if n_observed == 0 else
                        (" — so the observable detector carries every case and the oracle fallback "
                         "is empty." if n_oracle_fb == 0 else
                         " — the two subsets are reported side by side, never pooled."))),
            "source_counts": dict(source_counts),
            "observable_detector": {
                "n": n_observed,
                "hit@1": observable_hit1,
                "overall": observable_full,
                "label": "GENUINE observable detector (post-pruning received-msg set-diff; "
                         "chaosgraph AllRequiredNodesVisited shape)",
            },
            "oracle_fallback": {
                "n": n_oracle_fb,
                "hit@1": oracle_fb_hit1,
                "overall": oracle_fb_full,
                "label": "ledger-sourced ORACLE (upper bound; attribute absent -> fallback)",
            },
        },
        "output_feature_blind": {
            "convlen_anomaly_vs_clean_carrier": {
                "overall": conv_overall,
                "hit1_by_scenario": conv_by_scen,
                "seeds": SEEDS,
                "note": (f"per-agent |Δ conv_<A>_text_len| vs same-carrier normal case; seed-avg "
                         f"tie-break; OUTPUT-content feature. NOT a dependable locator: "
                         f"{conv_stability} (by scenario: {conv_scen_str}; spread "
                         f"{_f3(conv_spread)}) — it tracks which agent emits the most variable "
                         f"free text, not the drop (the Synthesizer's templated tool-call output "
                         f"barely changes, hence {_f3(conv_by_scen.get('ctxdrift_synth_from_prod'))}"
                         f" on `ctxdrift_synth_from_prod`). The overall {_f3(conv_h1)} is carried "
                         f"by `{conv_hi[0]}` ({_f3(conv_hi[1])}); under group-aware CV the same "
                         f"feature set collapses (Tier-A RF-content {_q(tiera['rf_content'])})."),
            },
            "tierA_quoted": tiera_by_label,
            "tierA_quoted_source": {
                "path": tiera_md, "family": "context_drift", "parsed_ok": tiera_ok,
                "parse_error": tiera_err or None,
                "note": ("Hit@1 column parsed at runtime from the Tier-A BASELINE_RESULTS.md "
                         "table; NOT hardcoded. If parsing fails these read "
                         f"'{TIERA_UNAVAILABLE}' rather than a stale constant."),
            },
        },
        "outcome_stratification": strata,
        "silent_wrong_run_ids": silent_ids,
        "silent_wrong_caveat": silent_caveat,
        "content_track_consolidation": {
            "structured_families": (f"Tier-A contract oracle Hit@1="
                                    f"{_q(oracle_q['wrong_item_pick'])} on wrong_item_pick & "
                                    f"{_q(oracle_q['format_violation'])} on format_violation "
                                    f"(deterministic content locatable)."),
            "hallucinate": "content signal present (divergent_needle) but names FAMILY not agent; "
                           "genuinely hard at agent granularity (Tier-A).",
            "context_drift": (f"NOT content-locatable from OUTPUT/infra features (Tier-A "
                              f"RF-content {_q(tiera['rf_content'])}, RF-infra "
                              f"{_q(tiera['rf_infra'])}, Trivial-corrected "
                              f"{_q(tiera['trivial_corrected'])} vs random floor "
                              f"{RANDOM_ANALYTIC_HIT1:.3f}); only the input-structure canary "
                              f"(this script) catches it."),
        },
    }

    with open(out_json, "w", encoding="utf-8") as fh:
        json.dump(results, fh, ensure_ascii=False, indent=2)

    # ---- console summary ----
    print(f"[ctxdrift-track] dataset_dir={base}")
    print(f"[ctxdrift-track] context_drift cases = {n_cd} | ledger target==GT root = "
          f"{n_target_eq_root}/{n_cd}")
    print(f"[ctxdrift-track] X-vs-Y verdict = {verdict} | content-layer input captured = "
          f"{evidence.get('content_layer_input_captured')}")
    print(f"[ctxdrift-track]   {evidence.get('note')}")
    print(f"[ctxdrift-track] locator ({results['structural_locator']['kind']}) Hit@1 = "
          f"{_f3(oracle_overall['hit@1'])} overall | by scenario: "
          + ", ".join(f"{k}={_f3(v)}" for k, v in oracle_by_scen.items()))
    print(f"[ctxdrift-track] output-feature-blind conv-len Hit@1 = {_f3(conv_overall['hit@1'])} "
          f"(Tier-A RF-content {_q(tiera['rf_content'])}, parsed from {tiera_md})")
    print(f"[ctxdrift-track] source split = {dict(source_counts)} "
          f"(observed_detector={n_observed}, ledger_oracle_fallback={n_oracle_fb})")
    print(f"[ctxdrift-track]   OBSERVABLE detector Hit@1 = {_f3(observable_hit1)} (n={n_observed}, "
          f"genuine; post-pruning received-msg set-diff) | ORACLE-fallback Hit@1 = "
          f"{_f3(oracle_fb_hit1)} (n={n_oracle_fb}, upper bound) [NOT merged]")
    # NEW-attribute observability line: the X-vs-Y probe above reads openinference llm.input_messages
    # (still Y — pre-prune snapshot). The NEW `agentfault.resolved_input.agent` attr, when present,
    # makes the drift OBSERVABLE from the trajectory independent of that Y verdict.
    if n_observed > 0:
        print(f"[ctxdrift-track] NEW `agentfault.resolved_input.agent` attr PRESENT -> drift is "
              f"OBSERVABLE from the trajectory in {n_observed}/{n_cd} case(s) via post-pruning "
              f"received-msg set-diff (X locally), independent of the llm.input_messages Y verdict.")
    else:
        print("[ctxdrift-track] NEW `agentfault.resolved_input.agent` attr ABSENT on this dataset "
              "-> drift not yet observable; recollect with the emission-enabled injector to light "
              "up the observable detector (X). Falls back to the ledger oracle (Y upper bound).")
    print(f"[ctxdrift-track] outcome counts = {dict(outcome_counts)}")

    # ---- MD writer ----
    # everything the MD narrative needs, all COMPUTED above (no literals in prose)
    narrative = {
        "tiera": tiera, "tiera_by_label": tiera_by_label, "tiera_ok": tiera_ok,
        "tiera_md": tiera_md, "oracle_q": oracle_q,
        "n_silent": n_silent, "silent_ids": silent_ids, "silent_caveat": silent_caveat,
        "n_recovered": n_recovered, "n_synth_root": n_synth_root, "synth_scens": synth_scens,
        "conv_lo": conv_lo, "conv_hi": conv_hi, "conv_spread": conv_spread,
        "conv_scen_str": conv_scen_str, "conv_vs_floor": conv_vs_floor,
        "conv_stability": conv_stability,
        "n_observed": n_observed, "n_oracle_fb": n_oracle_fb,
    }
    md = _build_md(results, oracle_overall, conv_overall, oracle_by_scen, conv_by_scen,
                   const_synth_overall, const_synth_by_scen, strata, scenarios, evidence, verdict,
                   locator_label, n_cd, n_target_eq_root, narrative)
    with open(out_md, "w", encoding="utf-8") as fh:
        fh.write(md)

    print(f"[ctxdrift-track] -> {out_json}")
    print(f"[ctxdrift-track] -> {out_md}")
    return 0


def _f3(x):
    return "N/A" if x is None else f"{x:.3f}"


def _q(x):
    """Format a QUOTED Tier-A value: 3dp if numeric, else the verbatim marker string."""
    if x is None:
        return "N/A"
    if isinstance(x, str):
        return x
    return f"{x:.3f}"


def _id_list(run_ids, cap=MAX_ENUMERATE_RUN_IDS):
    shown = ", ".join(f"`{r}`" for r in run_ids[:cap])
    extra = len(run_ids) - cap
    return shown + (f" (+{extra} more)" if extra > 0 else "")


def _silent_wrong_caveat(n, run_ids, n_total):
    """Honesty caveat whose WARNING SCALES with n (derived, never hardcoded).

    n==0 -> nothing to caveat; n==1 -> single-case anecdote; 1<n<SMALL_N_FOR_STATS -> a count,
    not a rate; n>=SMALL_N_FOR_STATS -> reportable proportion, still flagged as a small sample.
    """
    if n == 0:
        return (f"no `silent_wrong` cases among the {n_total} context_drift cases -> the injection "
                f"never changed the final recommendation in this dataset; nothing to stratify.")
    frac = f"{n}/{n_total}"
    if n == 1:
        return (f"silent_wrong n={n} (run_id {_id_list(run_ids)}) -> single-case anecdote only; "
                f"NO statistics can be drawn from n={n}. Reported for completeness, not as a "
                f"stratum estimate.")
    if n < SMALL_N_FOR_STATS:
        return (f"silent_wrong n={frac} context_drift cases (run_ids: {_id_list(run_ids)}) "
                f"-> n={n} is too small for statistics; read it as a COUNT, not a rate (a binomial "
                f"interval at n={n} spans most of [0,1]). The per-stratum Hit@1 is descriptive "
                f"only.")
    return (f"silent_wrong n={frac} = {n / n_total:.3f} of context_drift cases (run_ids: "
            f"{_id_list(run_ids)}) -> large enough to quote as a proportion, but still a small "
            f"sample: treat the interval as wide and do not compare strata without a test.")


def _build_md(results, oracle_overall, conv_overall, oracle_by_scen, conv_by_scen,
              const_synth_overall, const_synth_by_scen, strata, scenarios, evidence, verdict,
              locator_label, n_cd, n_target_eq_root, nar):
    L = []
    A = L.append
    A("# agentfault v2 — context_drift STRUCTURAL track (offline)\n")
    A(f"> Generated by `scripts/chaos/agentfault/eval/content_ctxdrift_track.py` (deterministic; "
      f"no network/services/LLM). Scorer = `m9_score.mrcbench` (上游 4.3 四族). Entity space = "
      f"{len(AGENTS)} in-process agents (single-root). This is the piece Tier-A left out; it does "
      f"NOT rebuild the Tier-A content locators. All Tier-A figures quoted below are parsed at "
      f"runtime from the Tier-A results file, never hardcoded.\n")
    A(f"- context_drift cases: **{n_cd}** | injection-ledger target agent == GT root "
      f"(`root_cause_set`): **{n_target_eq_root}/{n_cd}** (GT root = the DOWNSTREAM target whose "
      f"input lost the upstream message).")
    A(f"- Outcome labels (from `context_drift_outcomes.json`): "
      + ", ".join(f"**{k}**={v}" for k, v in results["outcome_counts"].items()) + ".\n")

    # X vs Y
    _ovo_obs = (results.get("observable_vs_oracle") or {}).get("observable_detector") or {}
    A("## Pivotal finding: X (observable) vs Y (ledger-only) — **openinference "
      "`llm.input_messages` channel only**\n")
    A(f"**VERDICT: {verdict} — {'observable structural detector' if verdict=='X' else 'ledger-only structural canary ORACLE (upper bound)'} "
      f"— FOR THE openinference `llm.input_messages` CHANNEL.**\n")
    if _ovo_obs.get("n"):
        A(f"> ⚠️ Scope: this verdict is about that ONE channel. A SECOND channel — the injector's "
          f"post-pruning `agentfault.resolved_input` span attribute — was added afterwards and "
          f"DOES make the deletion observable on this dataset "
          f"(n={_ovo_obs['n']}, Hit@1 = {_f3(_ovo_obs['hit@1'])}; see the source-split section "
          f"below). Do not quote the Y verdict as \"context_drift is unobservable\".\n")
    A("Evidence (re-derived at runtime by parsing the captured spans directly — no `injector_smoke` "
      "import, which has import-time env side effects):\n")
    clc = evidence.get("content_layer_input_captured")
    A(f"- Per-agent LLM **input** prompts ARE captured in spans (openinference "
      f"`llm.input_messages.*` on the `ChatOpenAI` child spans): **{clc}**.")
    if evidence.get("per_scenario"):
        A("- Yet in the drift cases the DROPPED upstream message is still PRESENT in the TARGET "
          "agent's captured input (name match):")
        A("")
        A("| scenario | target agent | dropped upstream | traces | dropped-msg PRESENT | ABSENT |")
        A("|---|---|---|---|---|---|")
        for sc, d in evidence["per_scenario"].items():
            A(f"| `{sc}` | {d['target']} | {d['dropped_upstream']} | "
              f"{d['traces_with_target_llm_span']} | {d['dropped_msg_PRESENT_in_captured_input']} | "
              f"{d['dropped_msg_ABSENT_in_captured_input']} |")
        t = evidence.get("totals", {})
        A(f"\n  Totals: dropped-msg present in **{t.get('dropped_present')}/{t.get('traces')}** "
          f"target-agent traces.")
    A("")
    A("**Mechanism (why the deletion is invisible in captured data):** the injector "
      "(`_maybe_context_drop`) deletes the upstream `HumanMessage` INSIDE the patched "
      "`ChatOpenAI._generate` (`messages[:] = kept`), i.e. AFTER LangChain's `on_chat_model_start` "
      "callback — which is what openinference uses to record `llm.input_messages` — has already "
      "fired in `BaseChatModel.generate()`. So the span records the **pre-deletion** message set. "
      "The content-layer input is captured, but at the wrong point: it reflects what the pipeline "
      "assembled, not what the model actually received after pruning.\n")
    A(f"**Consequence (this channel only):** the deletion cannot be observed **through "
      f"`llm.input_messages`**. A locator restricted to that channel is therefore labeled exactly "
      f"as:\n\n> {locator_label}\n")
    A("Restricted to this channel it mirrors Tier-A's contract-oracle framing: a deterministic, "
      "injection-visible upper bound — NOT a learned or observable method. A real trajectory "
      "monitor could reconstruct the canary only if the per-agent input were captured post-pruning "
      "(the message set actually sent to the model), comparing each agent's received "
      "upstream-message names against the fixed pipeline's expected upstream set and flagging the "
      "agent missing an expected upstream message."
      + (" **That capture gap has since been CLOSED** by the injector's `agentfault.resolved_input` "
         "emission — see the next section for the genuine observable detector."
         if _ovo_obs.get("n") else " That capture is the instrumentation gap.") + "\n")

    # observable-vs-oracle source split (NEW: after post-pruning emission was added to the injector)
    ovo = results.get("observable_vs_oracle", {})
    if ovo:
        obs = ovo["observable_detector"]
        orc = ovo["oracle_fallback"]
        A("## Observable detector vs oracle fallback (source split — NOT merged)\n")
        A("The injector now emits the POST-pruning received-message names into the trajectory "
          "(span `agentfault.resolved_input`, attr `agentfault.resolved_input.msg_names`). The "
          "detector reads that and applies the chaosgraph `AllRequiredNodesVisited` set-diff "
          "(`missing = expected_upstream − received`) — a GENUINE observable method. When the "
          "attribute is ABSENT (old / un-recollected data) the case falls back to the ledger-sourced "
          "ORACLE (upper bound). The two are reported separately and never merged.\n")
        A(f"- source split: `{ovo['source_counts']}`")
        A(f"- **observable detector** (genuine): Hit@1 = **{_f3(obs['hit@1'])}** over "
          f"**n={obs['n']}** cases carrying the attribute.")
        A(f"- **oracle fallback** (upper bound): Hit@1 = **{_f3(orc['hit@1'])}** over "
          f"**n={orc['n']}** attribute-absent cases.")
        if obs["n"] == 0:
            A("\n> On this (un-recollected) dataset the post-pruning attribute is absent everywhere, "
              "so all cases fall back to the ledger oracle and the oracle-fallback number reproduces "
              "the step-3 result exactly (backward-compat). The observable detector lights up only "
              "after re-collection with the emission-enabled injector.")
        A("")

    # structural track headline
    A("## context_drift structural track — Hit@1\n")
    A("| locator | Hit@1 (overall) | " + " | ".join(f"Hit@1 `{s}`" for s in scenarios) + " |")
    A("|---|---|" + "|".join(["---"] * len(scenarios)) + "|")
    kind = results["structural_locator"]["kind"].upper()
    A(f"| **structural canary {kind}** ({'ledger-sourced' if verdict!='X' else 'observable'}) | "
      f"**{_f3(oracle_overall['hit@1'])}** | "
      + " | ".join(_f3(oracle_by_scen[s]) for s in scenarios) + " |")
    A(f"| Constant-Synthesizer (reference) | {_f3(const_synth_overall['hit@1'])} | "
      + " | ".join(_f3(const_synth_by_scen[s]) for s in scenarios) + " |")
    A(f"| Random (analytic) | {RANDOM_ANALYTIC_HIT1:.3f} | "
      + " | ".join(f"{RANDOM_ANALYTIC_HIT1:.3f}" for _ in scenarios) + " |")
    A("")
    scen_clause = (f"the `{nar['synth_scens'][0]}` scenario only"
                   if len(nar["synth_scens"]) == 1
                   else "scenario(s) " + ", ".join(f"`{s}`" for s in nar["synth_scens"]))
    A(f"> The canary scores Hit@1 = {_f3(oracle_overall['hit@1'])} because the injection ledger "
      f"names the target agent, which IS the GT root ({n_target_eq_root}/{n_cd}). It is an upper "
      f"bound, not a deployable detector (see verdict above). Constant-Synthesizer = "
      f"{_f3(const_synth_overall['hit@1'])} (Synthesizer is root in {scen_clause}, "
      f"{nar['n_synth_root']}/{n_cd}).")
    A("")
    n_ag = len(AGENTS)
    rnd_h3 = min(3, n_ag) / n_ag
    A(f"> **@3/@5 footnote (canonical padding):** with a {n_ag}-agent space and a single root always "
      f"among them, Hit@5 = FullHit@5 = {min(5, n_ag) / n_ag:.3f} trivially (top-5 ⊇ all {n_ag}) and "
      f"any locator that names the target first has Hit@3 = 1.000; a random ranker gets Hit@3 = "
      f"{rnd_h3:.3f}. K=1 is the only discriminative headline (same convention as the Tier-A / M9 "
      f"scorers). Single-root degenerate identities also hold: Recall@R ≡ Hit@1, NDCG@1 ≡ Hit@1.")
    A("")

    # output-feature-blind
    A("## Output-feature-blind contrast (context_drift is invisible to output/infra features)\n")
    A("The structural canary is the ONLY thing that catches context_drift. Output-content and infra "
      "features do NOT locate it:\n")
    A("| method | Hit@1 | source |")
    A("|---|---|---|")
    A(f"| conv_<A>_text_len anomaly vs clean carrier | {_f3(conv_overall['hit@1'])} | "
      f"computed here (per-agent |Δ output-text-len| vs same-carrier normal; seed-avg) |")
    tq_src = os.path.relpath(nar["tiera_md"], REPO_ROOT).replace("\\", "/")
    for name, v in results["output_feature_blind"]["tierA_quoted"].items():
        A(f"| {name} | {_q(v)} | parsed at runtime from Tier-A `{tq_src}` (context_drift family) |")
    A(f"| Random floor | {RANDOM_ANALYTIC_HIT1:.3f} | analytic ({RANDOM_ANALYTIC_HIT1:.3f} = "
      f"1/{len(AGENTS)} agents) |")
    A("")
    if not nar["tiera_ok"]:
        A(f"> ⚠️ **Tier-A quotes UNAVAILABLE:** `{tq_src}` could not be parsed for the Hit@1 "
          f"column, so the quoted Tier-A rows above are emitted as an explicit unavailable marker "
          f"rather than stale constants. Re-run `eval_agentfault_tierA.py` (or pass `--tiera-md`) "
          f"and regenerate this document.\n")
    A("conv-len anomaly by scenario: " + ", ".join(f"`{s}`={_f3(conv_by_scen[s])}"
                                                    for s in scenarios) + ".")
    A("")
    lo_sc, lo_v = nar["conv_lo"]
    hi_sc, hi_v = nar["conv_hi"]
    synth_v = conv_by_scen.get("ctxdrift_synth_from_prod")
    A("**Honest reading of the conv-len number (do not overclaim it as ~random).** Overall Hit@1 "
      f"= {_f3(conv_overall['hit@1'])}, {nar['conv_vs_floor']} the "
      f"{RANDOM_ANALYTIC_HIT1:.3f} floor — BUT it is {nar['conv_stability']} "
      f"({' / '.join(_f3(conv_by_scen[s]) for s in scenarios)} across the {len(scenarios)} "
      f"scenarios; spread {_f3(nar['conv_spread'])}) and therefore NOT a dependable locator. "
      f"It scores its worst, {_f3(lo_v)}, on `{lo_sc}`"
      + (": the Synthesizer's output is a templated tool-call whose length barely moves, so the "
         "anomaly ranker never picks it" if lo_sc == "ctxdrift_synth_from_prod" else "")
      + f"; and its best, {_f3(hi_v)}, on `{hi_sc}`"
      + (" because the analyzer's free-text length happens to swing most there"
         if hi_sc == "ctxdrift_ub_from_seq" else "")
      + ". The signal is tracking **which agent emits the most variable free text, not the drop** — "
      f"its overall is carried by one scenario, not localization skill. Under group-aware CV the "
      f"very same conv-len feature set collapses to Tier-A RF-content "
      f"{_q(nar['tiera']['rf_content'])}.\n")
    _obs = (results.get("observable_vs_oracle") or {}).get("observable_detector") or {}
    _orc = (results.get("observable_vs_oracle") or {}).get("oracle_fallback") or {}
    _canary_status = (
        f"and on this dataset that canary IS observable: the post-pruning "
        f"`agentfault.resolved_input.msg_names` attribute carries it for n={_obs.get('n')} of the "
        f"{n_cd} cases (Hit@1 = {_f3(_obs.get('hit@1'))}), with the ledger-sourced ORACLE fallback "
        f"used on n={_orc.get('n')} cases"
        if _obs.get("n") else
        "and on this dataset that canary is a ledger-sourced ORACLE, not yet an observable detector"
    )
    A("**Thesis:** context_drift leaves NO latency signature (the system recovers) and NO "
      "needle/asin/contract signal, and per-agent OUTPUT text-length is at best an unstable, "
      f"scenario-dependent proxy ({_f3(synth_v)} where the target is the Synthesizer) — no "
      "output-content or infra method is a reliable localizer. Only the INPUT-structure canary "
      "(which requires the pre-call injection point / post-pruning input capture) localizes it "
      f"dependably — {_canary_status}.\n")

    # outcome stratification
    A("## Outcome stratification\n")
    A("| outcome | n | canary Hit@1 | conv-len anomaly Hit@1 |")
    A("|---|---|---|---|")
    for oc in ("recovered", "silent_wrong", "unknown"):
        if oc not in strata:
            continue
        s = strata[oc]
        A(f"| {oc} | {s['n']} | {_f3(s['oracle_hit1'])} | {_f3(s['convlen_hit1'])} |")
    A("")
    n_rec, n_sw = nar["n_recovered"], nar["n_silent"]
    if n_rec:
        A(f"- **`recovered` (n={n_rec}/{n_cd}):** the injection did not change the final "
          f"recommendation ASIN vs the clean carrier (system tolerated the drop). The canary still "
          f"names the target (upper bound).")
    if n_sw:
        A(f"- ⚠️ **`silent_wrong` (n={n_sw}/{n_cd}):** {nar['silent_caveat']} "
          f"{'This is the one case' if n_sw == 1 else f'These are the {n_sw} cases'} where the drop "
          f"propagated to a different final recommendation (the black-box outcome differs; only the "
          f"content/structure track could have flagged "
          f"{'it' if n_sw == 1 else 'them'} pre-hoc).")
    else:
        A(f"- **`silent_wrong` (n=0):** {nar['silent_caveat']}")
    A("")

    # consolidation
    A("## Content-track consolidation (no rebuild)\n")
    c = results["content_track_consolidation"]
    A(f"- **Structured families (wrong_item_pick, format_violation):** {c['structured_families']}")
    A(f"- **hallucinate:** {c['hallucinate']}")
    A(f"- **context_drift:** {c['context_drift']}")
    A("")
    oq = nar["oracle_q"]
    A("So the content track resolves cleanly by family: structured = deterministic-content "
      f"locatable (Tier-A contract oracle Hit@1 = {_q(oq['wrong_item_pick'])} wrong_item_pick / "
      f"{_q(oq['format_violation'])} format_violation), hallucinate = content-signal present but "
      "family-not-agent, "
      "context_drift = NOT output-content-locatable — caught only by the input-structure canary, "
      + (f"which on this dataset is OBSERVABLE from the trajectory via the post-pruning "
         f"`agentfault.resolved_input` attribute (n={_obs.get('n')}, Hit@1 = "
         f"{_f3(_obs.get('hit@1'))}; oracle fallback n={_orc.get('n')}). The (Y) verdict above "
         f"applies only to the openinference `llm.input_messages` channel, which captures "
         f"pre-pruning.\n"
         if _obs.get("n") else
         "which on this dataset is a ledger-sourced ORACLE / upper bound (Y), pending post-pruning "
         "per-agent input capture.\n"))
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
