# -*- coding: utf-8 -*-
"""run_infra_negatives.py — BARO / RCD / (Eadro N/A) honest negatives on agentfault.

Implements SPEC_INFRA.md (scripts/chaos/agentfault/eval/infra_negatives/SPEC_INFRA.md):
mature metric-RCA methods (BARO / RCD / Eadro) applied to the agentfault dataset
(agent *semantic* faults) in the RCAEval honest-negative convention — "we tried,
here is exactly how far each method runs and how it degrades, and why".

Method code = vendored RCAEval originals, single-file loaded (never copied, never
edited): third_party/RCAEval/RCAEval/e2e/baro.py::baro and rcd.py::rcd, loaded via
importlib.util.spec_from_file_location to bypass e2e/__init__ (sknetwork dep) —
identical to third_party/baro_pilot.py / rcd_pilot.py.

Feeding modes (see SPEC §2, adaptation labels are mandatory in every table):
  * BARO  -> "BARO-stage2 (adapted: case-level pseudo-series, change-point given)"
     agentfault has NO within-case time series (each agent runs once per case ->
     1 scalar per agent per case), so BOCPD stage-1 is structurally impossible.
     NOTE the vendored RCAEval baro() is itself already the change-point-given
     variant (it splits on inject_time; no BOCPD inside) -> we call the ORIGINAL
     function unmodified on a case-level pseudo-series: rows = <all normal> cases
     (before segment, time 0..7) + 1 target faulted case (after segment, time 8),
     inject_time = 8, columns = <agent>_latency.
     dataset=None is REQUIRED: with dataset="recshop" the preprocess path calls
     drop_constant on the 1-row anomalous segment, which is degenerate on a single
     row ((df != df.iloc[0]).any() is all-False) and drops EVERY column -> empty
     ranking. This failure shape is probed once and recorded in the results.
     With dataset=None the 'time' column stays in the frame and gets ranked too;
     it is filtered out of the ranking post-hoc (recorded in the adaptation note).
  * RCD   -> "RCD (adapted: family-pooled; per-case runs but degenerate — see probes)"
     RCD needs multi-row normal/anomalous segments for chi^2 CI tests; a case has
     1 anomalous row. Two per-case failure shapes, both probed and recorded:
     dataset="recshop" -> drop_constant on the 1-row anomalous segment drops every
     column -> empty ranking; dataset=None -> mechanically RUNS (single-case probe
     + full faulted-case appendix run recorded), but the chi^2 CI then has exactly ONE
     anomalous sample (statistically void) and the 'time' column stays in as a
     candidate variable (rcd() only drops time when dataset!=None) -> the output is
     near-constant (top-1 histogram dominated by one agent) and scores at/below the
     constant-Synthesizer prior (not below the 0.25 random floor). So per-case is DEGENERATE, not "cannot run" — the SPEC §2b
     draft label said "per-case structurally N/A"; downgraded to match the probed
     reality (2026-07-17 review fix). Adaptation: family pooling — normal_df = <all normal> rows,
     anomalous_df = all rows of one fault family (family sizes derived from the
     dataset at runtime; v1 = hallucinate 24 / wrong_item_pick 8 / format_violation
     32; v2 adds context_drift). One run per family -> ONE family-level ranking,
     assigned to every case of the family for per-case scoring (RCAEval per-case
     convention; honestly labelled "族级单排名摊派"). seeds 0-4, mean±range +
     rank-stability reported (rcd_pilot's concern). RCD may return a PARTIAL list
     (it outputs F-node neighbours, not a full ordering): for MRCBench the list is
     padded with the remaining agents in canonical order (same convention as
     score_whowhen.ranked_of; @3/@5 are then padding artifacts — footnoted).
  * Eadro -> NOT RUN, structural N/A (needs within-case time series + a service
     dependency graph mined from traces + supervised training; agentfault has no
     within-case series, a fixed 4-agent sequential pipeline with no hidden
     topology, and the dataset (dozens of cases) is far below its training
     regime). Documented only.

Channels (SPEC §1): main = span_<A>_duration_corrected_ms (injection overhead
subtracted, EVAL_NOTES §4a — the eval iron rule); contamination contrast = raw
span_<A>_duration_ms (narrative only: raw scores borrow the injection artifact).

Scoring = m9_score.mrcbench (上游论文 4.3 四族: Hit@K/FullHit@K/Recall@R/NDCG@K,
K=1,3,5, + avg@K + mrr), macro per fault family x overall; Hit@1 is the headline,
@3/@5 are ceiling artifacts with 4 candidates (footnoted, same wording convention
as score_whowhen.py). Contrast rows: Random (empirical seeds 0-4; analytic floor
0.25) + always-Synthesizer constant baseline (same scoring path).

Offline, deterministic, idempotent. No network, no LLM, no service dependency.

Usage (from repo root, conda env recweb2):
  PYTHONIOENCODING=utf-8 python3 \
      scripts/chaos/agentfault/eval/infra_negatives/run_infra_negatives.py \
      --method all --channel both

Outputs (default): (v1)infra_negatives/
  infra_negatives_results.json + RESULTS_INFRA_NEGATIVES.md
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import io
import json
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", "..", ".."))
CTK = os.path.join(REPO, "scripts", "chaos", "ctk")
RCAEVAL_DIR = os.path.join(REPO, "third_party", "RCAEval")
CL_PATCHED = os.path.join(REPO, "third_party", "_cl_patched")

# path order (front to back): _cl_patched (MUST precede any causallearn import,
# SPEC §0 / rcd_pilot.py L14-20 pattern) > RCAEval (for `from RCAEval.io...` inside
# the single-file-loaded modules) > ctk (m9_score).
sys.path.insert(0, CTK)
sys.path.insert(0, RCAEVAL_DIR)
sys.path.insert(0, CL_PATCHED)

# m9_score re-wraps sys.stdout at import time (GC-closes the shared buffer), so a
# fresh independent stream is reopened on a dup of fd 1 right after — exactly the
# eval_agentfault_tierA.py L55-80 / score_whowhen.py workaround.
from m9_score import mrcbench, K_LIST  # noqa: E402

try:
    sys.stdout = io.TextIOWrapper(io.FileIO(os.dup(1), "w"), encoding="utf-8", errors="replace")
except Exception:
    pass

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

# ---- RCD prerequisites: patched causallearn + pinned version (SPEC §0) --------
import causallearn  # noqa: E402

_CL_FILE = (causallearn.__file__ or "").replace("\\", "/")
assert "third_party" in _CL_FILE, f"causallearn did not resolve to the patched copy: {_CL_FILE}"
import importlib.metadata as _md  # noqa: E402

_CL_VERSION = _md.version("causal-learn")
assert _CL_VERSION == "0.1.2.3", f"causal-learn must be 0.1.2.3 (got {_CL_VERSION})"
import matplotlib  # noqa: E402  (rcd.py imports it at module top; presence verified)


def _load_single(rel_path, mod_name):
    """Single-file load of a vendored RCAEval e2e module (bypasses e2e/__init__'s
    sknetwork import) — the baro_pilot.py / rcd_pilot.py pattern. Read-only."""
    path = os.path.join(REPO, rel_path)
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_baro_mod = _load_single("third_party/RCAEval/RCAEval/e2e/baro.py", "baro_only")
baro = _baro_mod.baro
_rcd_mod = _load_single("third_party/RCAEval/RCAEval/e2e/rcd.py", "rcd_only")
rcd = _rcd_mod.rcd

# ---- constants -----------------------------------------------------------------
CSV_PATH = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "dataset_agentfault.csv")
DEFAULT_OUT = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "infra_negatives")

# candidate entities: the 4 in-process LangGraph agents, in pipeline execution
# order — same order as injector_smoke.AGENT_NAMES (hardcoded on purpose: SPEC §1
# says do NOT import injector_smoke, its import has env side effects).
AGENTS = [
    "Sequence_Recommender",
    "User_Behavior_Analyzer",
    "Product_Analyzer",
    "Recommendation_Synthesizer",
]
# fault families (derived from data at runtime in main(); this is the canonical
# display order — any family present in the faulted rows but not listed here is
# appended alphabetically). v1 has (hallucinate, wrong_item_pick, format_violation);
# v2 adds context_drift (a CONTENT-deletion fault with no latency/needle signature —
# EXPECTED to sit ~random on these infra methods; that is the honest, desired finding,
# so it is included as its own per-family row, never dropped).
FAMILY_ORDER = ["hallucinate", "context_drift", "wrong_item_pick", "format_violation"]
FAMILIES = ("hallucinate", "wrong_item_pick", "format_violation")  # overwritten in main() from data
SEEDS = [0, 1, 2, 3, 4]
CHANNELS = {
    "corrected": "span_{a}_duration_corrected_ms",
    "raw": "span_{a}_duration_ms",
}
LAT_SUFFIX = "_latency"

BARO_LABEL = "BARO-stage2 (adapted: case-level pseudo-series, change-point given)"
RCD_LABEL = "RCD (adapted: family-pooled; per-case runs but degenerate — see probes)"

REPORT_KEYS = ["hit@1", "hit@3", "ndcg@3", "hit@5", "recall@R", "mrr"]

FOOTNOTES = [
    "[1] corrected channel = span duration with the injection overhead subtracted "
    "(span_<A>_duration_corrected_ms; the dedicated sub-LLM span "
    "agentfault.subllm_rewrite is measured and removed, EVAL_NOTES §4a). This is "
    "the eval iron rule: raw durations carry the injection's own latency artifact.",
    "[2] BARO and RCD rows are ADAPTED runs, not the original end-to-end pipelines. "
    "BARO: agentfault has no within-case time series (1 scalar per agent per case), "
    "so BOCPD change-point detection (stage-1) is structurally impossible; the run "
    "feeds the ORIGINAL vendored baro() (which is itself the change-point-given "
    "RobustScorer variant — it has no BOCPD inside) a case-level pseudo-series of "
    "{n_normal} normal cases + 1 target case with inject_time given; dataset=None is required "
    "(dataset!=None drops every column of the 1-row anomalous segment via "
    "drop_constant — probed and recorded); the 'time' column is filtered from the "
    "ranking post-hoc. RCD: per-case runs but is DEGENERATE, not structurally "
    "impossible — dataset='recshop' preprocess drops every column of the 1-row "
    "anomalous segment (empty ranking); dataset=None mechanically runs, but the "
    "chi-square CI then has exactly ONE anomalous sample (statistically void) and "
    "the 'time' column remains a candidate variable (rcd() only drops it when "
    "dataset!=None); the full {n_faulted}-case per-case run (probe appendix) is "
    "near-constant (one agent dominates the top-1 histogram) and scores at/below the "
    "constant-Synthesizer prior {synth_prior}, NOT below the 0.25 random floor. "
    "Family pooling is therefore "
    "the adaptation: samples pooled per fault family ({fam_sizes}) and the single "
    "family-level ranking "
    "assigned to every case of the family (族级单排名摊派); RCD returns F-node "
    "neighbours (a possibly-partial set), padded with remaining agents in canonical "
    "order for MRCBench.",
    "[3] agent semantic faults are BY DESIGN invisible in infra scalars (the "
    "injection rewrites message content; timing/status/span counts stay nominal "
    "except the injection overhead, which the corrected channel removes). A "
    "near-floor score here is therefore the expected honest NEGATIVE RESULT — a "
    "conclusion about method-family applicability, not a failed experiment.",
    "[4] raw-channel scores are a CONTAMINATION CONTRAST, not a usable result: any "
    "lift over the corrected channel comes from the injection's own latency "
    "artifact (extra sub-LLM call, time ∝ tokens), i.e. the method is detecting "
    "the measurement of the fault injection, not the fault. Mirrors the tierA "
    "trivial-heuristic finding on this same dataset (the RAW channel scores far above "
    "the CORRECTED one on hallucinate — see BASELINE_RESULTS.md for the current pair).",
    "[5] With 4 candidates and full (or padded-full) rankings, hit@5 == 1.0 always "
    "and hit@3 == 0.75 for random — @3/@5 are ceiling artifacts. recall@R is NOT a "
    "ceiling artifact: under single-root GT, R = max(1,|G|) = 1, so recall@R "
    "degenerates to (exactly equals) hit@1. Only Hit@1 and the rank-sensitive "
    "ndcg@3 / mrr are discriminative. Same footnote convention as the Who&When "
    "scorer.",
    "[6] Random row = empirical seeded permutations (seeds 0-4, mean); the analytic "
    "floor is 0.25. always_synthesizer row = constant prediction scored through the "
    "identical path; the GT prior is skewed ({synth_prior} Synthesizer), so family "
    "columns where the prior is 100% must be read against it.",
]

EADRO_TEXT = (
    "Eadro is NOT run — structural N/A (documented per SPEC §2c, not a skipped TODO):\n\n"
    "1. **No within-case time series.** Eadro consumes per-service metric/log/trace "
    "time series inside each case window; agentfault has exactly one scalar per agent "
    "per case (each agent executes once per pipeline run).\n"
    "2. **No hidden topology to exploit.** Eadro mines a service dependency graph from "
    "traces for its GNN; the 4 LangGraph agents form a FIXED sequential pipeline "
    "(Sequence_Recommender -> User_Behavior_Analyzer -> Product_Analyzer -> "
    "Recommendation_Synthesizer) — the graph is a known chain, there is nothing to learn.\n"
    "3. **Supervised training regime does not fit.** Eadro trains on labelled anomaly "
    "windows; {n_rows} cases ({n_faulted} faulted) is far below its training-data regime, and any "
    "result would be dominated by split variance (see also the K8S-line Eadro leakage "
    "lesson, (project docs)/eadro/ — the 0.842 number in the old EVAL_NOTES §5 was a "
    "case-split leakage artifact; the honest number was 0.326).\n"
)


# ---------------------------------------------------------------- data loading --
def derive_families(df):
    """Fault families present in the FAULTED rows, in canonical display order
    (FAMILY_ORDER first, any extras appended alphabetically). Fully data-driven —
    no hardcoded family list. v1 -> (hallucinate, wrong_item_pick, format_violation);
    v2 -> (hallucinate, context_drift, wrong_item_pick, format_violation)."""
    present = set(df.loc[df["injected"] == "1", "kind"].astype(str))
    ordered = [f for f in FAMILY_ORDER if f in present]
    ordered += sorted(f for f in present if f not in FAMILY_ORDER)
    return tuple(ordered)


def load_df():
    df = pd.read_csv(CSV_PATH, dtype=str).fillna("")
    if len(df) <= 0:
        raise SystemExit(f"no rows in {CSV_PATH}")
    n_f = int((df["injected"] == "1").sum())
    n_n = int((df["injected"] == "0").sum())
    fam_break = df.loc[df["injected"] == "1", "kind"].value_counts().to_dict()
    print("[load_df] %s: %d rows (%d faulted + %d normal); faulted per family: %s"
          % (os.path.relpath(CSV_PATH, REPO).replace("\\", "/"), len(df), n_f, n_n,
             json.dumps(fam_break, ensure_ascii=False)))
    return df


def roots_of(cell):
    s = str(cell).strip()
    if not s or s.lower() == "nan":
        return []
    return [x.strip() for x in s.split(";") if x.strip()]


def agent_matrix(df, channel):
    """N x 4 numeric matrix of the chosen duration channel; columns renamed to
    <agent>_latency (the RCAEval-habitual column shape, SPEC §2a)."""
    cols = [CHANNELS[channel].format(a=a) for a in AGENTS]
    M = df[cols].apply(pd.to_numeric, errors="coerce")
    M.columns = [a + LAT_SUFFIX for a in AGENTS]
    if M.isna().any().any():
        na = int(M.isna().sum().sum())
        raise SystemExit(f"channel {channel}: {na} NaN durations — refusing to impute silently")
    return M


def cols_to_agents(rank_cols):
    """ranked column names -> dedup ordered agent names; non-agent columns (e.g.
    'time' in the dataset=None BARO path) are dropped."""
    seen, out = set(), []
    for c in rank_cols or []:
        c = str(c)
        if not c.endswith(LAT_SUFFIX):
            continue
        a = c[: -len(LAT_SUFFIX)]
        if a in AGENTS and a not in seen:
            seen.add(a)
            out.append(a)
    return out


def pad_canonical(ranked):
    """partial ranking -> full 4-agent ranking, remaining agents appended in
    canonical order (same convention as score_whowhen.ranked_of; footnote [2]/[5])."""
    out = list(ranked)
    for a in AGENTS:
        if a not in out:
            out.append(a)
    return out


def macro(dicts):
    if not dicts:
        return {}
    keys = dicts[0].keys()
    return {k: sum(d[k] for d in dicts) / float(len(dicts)) for k in keys}


# ------------------------------------------------------------------- probes -----
def probe_baro_recshop(normal_block, target_row):
    """SPEC honesty probe: what happens if the vendored baro() is fed with the
    dataset!=None preprocess path (drop_constant on a 1-row anomalous segment)."""
    frame = pd.concat([normal_block, target_row], ignore_index=True)
    frame.insert(0, "time", range(len(frame)))
    try:
        res = baro(frame, inject_time=len(normal_block), dataset="recshop")
        return {
            "outcome": "ran",
            "ranks": list(map(str, res.get("ranks", []))),
            "note": "dataset!=None preprocess drops every column of the 1-row anomalous "
                    "segment (drop_constant on a single row is all-constant) -> empty "
                    "intersection -> empty ranking",
        }
    except Exception as e:  # pragma: no cover - recorded, not raised
        return {"outcome": "exception", "error": f"{type(e).__name__}: {e}"}


def probe_rcd_per_case(normal_block, target_row, dataset):
    """SPEC §2b honesty probe: per-case RCD (8 normal rows vs 1 anomalous row)."""
    frame = pd.concat([normal_block, target_row], ignore_index=True)
    frame.insert(0, "time", range(len(frame)))
    try:
        res = rcd(frame, inject_time=len(normal_block), dataset=dataset,
                  seed=0, gamma=5, localized=True, bins=5)
        return {"outcome": "ran", "dataset": str(dataset),
                "ranks": list(map(str, res.get("ranks", [])))}
    except Exception as e:
        return {"outcome": "exception", "dataset": str(dataset),
                "error": f"{type(e).__name__}: {str(e)[:300]}"}


def probe_rcd_per_case_full(df, M, normal_block):
    """Full per-case RCD appendix run over ALL faulted cases (dataset=None, seed 0,
    corrected channel). Reconciles the single-case probe with the family-pooling
    justification: the per-case path mechanically RUNS but is degenerate — the
    chi-square CI sees exactly one anomalous sample (statistically void), the
    'time' column stays in as a candidate variable (rcd() only drops time when
    dataset!=None), and the output is near-constant, scoring below the 0.25
    random floor. Deterministic, ~seconds."""
    inject_t = len(normal_block)
    top1, hits, n_nonempty, n_time, n_err = {}, 0, 0, 0, 0
    for i in df.index[df["injected"] == "1"]:
        frame = pd.concat([normal_block, M.loc[[i]]], ignore_index=True)
        frame.insert(0, "time", range(len(frame)))
        try:
            res = rcd(frame, inject_time=inject_t, dataset=None,
                      seed=0, gamma=5, localized=True, bins=5)
            ranks_raw = list(map(str, res.get("ranks", [])))
        except Exception:
            ranks_raw = []
            n_err += 1
        if "time" in ranks_raw:
            n_time += 1
        raw = cols_to_agents(ranks_raw)
        if raw:
            n_nonempty += 1
        top = raw[0] if raw else "(empty)"
        top1[top] = top1.get(top, 0) + 1
        ranked = pad_canonical(raw)
        roots = roots_of(df.at[i, "root_cause_set"])
        if ranked and roots and ranked[0] == roots[0]:
            hits += 1
    n = int((df["injected"] == "1").sum())
    return {
        "outcome": "ran",
        "dataset": "None", "seed": 0, "channel": "corrected",
        "n_cases": n,
        "n_nonempty_rankings": n_nonempty,
        "n_exceptions": n_err,
        "top1_histogram": top1,
        "hit@1": (hits / float(n)) if n else 0.0,
        "n_cases_with_time_in_raw_ranks": n_time,
        "note": "per-case RCD mechanically RUNS but is degenerate: the chi-square CI "
                "has exactly 1 anomalous sample (statistically void), 'time' stays "
                "in as a candidate variable in the dataset=None path, and the output "
                "is near-constant and lands at/below the constant-Synthesizer prior "
                "(NOT below the 0.25 random floor) — this degeneracy "
                "(not 'cannot run') is the load-bearing reason for family pooling.",
    }


# ------------------------------------------------------------------ BARO --------
def run_baro_channel(df, channel):
    """BARO-stage2 adapted run, one channel. Deterministic (no RNG).
    Per faulted case: pseudo-series = 8 normal rows (time 0..7) + target case
    (time 8), inject_time=8, ORIGINAL vendored baro(), dataset=None."""
    M = agent_matrix(df, channel)
    normal_block = M.loc[df["injected"] == "0"].reset_index(drop=True)
    per_case, mrc_rows = [], []
    by_family = {f: [] for f in FAMILIES}
    inject_t = len(normal_block)
    for i in df.index[df["injected"] == "1"]:
        frame = pd.concat([normal_block, M.loc[[i]]], ignore_index=True)
        frame.insert(0, "time", range(len(frame)))
        res = baro(frame, inject_time=inject_t, dataset=None)
        ranked = cols_to_agents(res["ranks"])  # 'time' filtered here (footnote [2])
        roots = roots_of(df.at[i, "root_cause_set"])
        mrc = mrcbench(ranked, roots)
        fam = df.at[i, "kind"]
        mrc_rows.append(mrc)
        if fam in by_family:
            by_family[fam].append(mrc)
        per_case.append({
            "run_id": df.at[i, "run_id"], "family": fam, "root": roots[0] if roots else None,
            "ranked": ranked, "hit@1": mrc["hit@1"],
        })
    return {
        "label": BARO_LABEL,
        "channel": channel,
        "n_cases": len(per_case),
        "overall": macro(mrc_rows),
        "by_family": {f: macro(v) for f, v in by_family.items() if v},
        "per_case": per_case,
    }


# ------------------------------------------------------------------- RCD --------
def run_rcd_channel(df, channel):
    """RCD adapted run, one channel. Family-pooled: normal_df = 8 normal rows,
    anomalous_df = all rows of the family; seeds 0-4; single family ranking
    assigned to every case of the family (族级单排名摊派)."""
    M = agent_matrix(df, channel)
    normal_block = M.loc[df["injected"] == "0"].reset_index(drop=True)
    inject_t = len(normal_block)

    fam_runs = {}   # family -> {seed: {"ranks_raw": [...], "ranked_padded": [...] , "error": ...}}
    for fam in FAMILIES:
        idx = df.index[(df["injected"] == "1") & (df["kind"] == fam)]
        anomal_block = M.loc[idx].reset_index(drop=True)
        frame_base = pd.concat([normal_block, anomal_block], ignore_index=True)
        frame_base.insert(0, "time", range(len(frame_base)))
        fam_runs[fam] = {"n_anomalous_rows": int(len(anomal_block)), "seeds": {}}
        for sd in SEEDS:
            frame = frame_base.copy()
            try:
                res = rcd(frame, inject_time=inject_t, dataset="recshop",
                          seed=sd, gamma=5, localized=True, bins=5)
                raw = cols_to_agents(res.get("ranks", []))
                fam_runs[fam]["seeds"][sd] = {
                    "ranks_raw": raw,
                    "ranked_padded": pad_canonical(raw),
                    "n_returned": len(raw),
                }
            except Exception as e:
                fam_runs[fam]["seeds"][sd] = {
                    "error": f"{type(e).__name__}: {str(e)[:300]}",
                    "ranks_raw": None, "ranked_padded": None, "n_returned": 0,
                }

    # per-seed scoring: family ranking assigned to every case of the family
    per_seed = {}
    for sd in SEEDS:
        mrc_rows = []
        by_family = {f: [] for f in FAMILIES}
        for i in df.index[df["injected"] == "1"]:
            fam = df.at[i, "kind"]
            rec = fam_runs[fam]["seeds"][sd]
            ranked = rec["ranked_padded"] or []
            mrc = mrcbench(ranked, roots_of(df.at[i, "root_cause_set"]))
            mrc_rows.append(mrc)
            if fam in by_family:
                by_family[fam].append(mrc)
        per_seed[sd] = {
            "overall": macro(mrc_rows),
            "by_family": {f: macro(v) for f, v in by_family.items() if v},
        }

    # seed aggregation: mean / min / max of each metric over seeds
    def agg(getter):
        vals = {k: [getter(sd).get(k, 0.0) for sd in SEEDS] for k in getter(SEEDS[0])}
        return {k: {"mean": float(np.mean(v)), "min": float(np.min(v)), "max": float(np.max(v))}
                for k, v in vals.items()}

    overall_agg = agg(lambda sd: per_seed[sd]["overall"])
    by_family_agg = {
        f: agg(lambda sd, f=f: per_seed[sd]["by_family"].get(f, {}))
        for f in FAMILIES if all(f in per_seed[sd]["by_family"] for sd in SEEDS)
    }

    # stability: distinct raw rankings / distinct top-1 per family across seeds
    stability = {}
    for fam in FAMILIES:
        seq = [tuple(fam_runs[fam]["seeds"][sd].get("ranks_raw") or ()) for sd in SEEDS]
        tops = [(s[0] if s else "(empty)") for s in seq]
        stability[fam] = {
            "rankings_by_seed": [list(s) for s in seq],
            "top1_by_seed": tops,
            "n_distinct_rankings": len(set(seq)),
            "n_distinct_top1": len(set(tops)),
            "stable": len(set(seq)) == 1,
        }

    # per-case detail for seed 0 (representative; all seeds kept in fam_runs)
    per_case = []
    for i in df.index[df["injected"] == "1"]:
        fam = df.at[i, "kind"]
        rec = fam_runs[fam]["seeds"][0]
        ranked = rec["ranked_padded"] or []
        roots = roots_of(df.at[i, "root_cause_set"])
        mrc = mrcbench(ranked, roots)
        per_case.append({
            "run_id": df.at[i, "run_id"], "family": fam, "root": roots[0] if roots else None,
            "ranked_padded_seed0": ranked, "hit@1_seed0": mrc["hit@1"],
        })

    return {
        "label": RCD_LABEL,
        "channel": channel,
        "params": {"dataset": "recshop", "gamma": 5, "localized": True, "bins": 5,
                   "seeds": SEEDS},
        "family_runs": fam_runs,
        "per_seed": per_seed,
        "overall_seedagg": overall_agg,
        "by_family_seedagg": by_family_agg,
        "seed_stability": stability,
        "per_case_seed0": per_case,
    }


# --------------------------------------------------------------- baselines ------
def run_random_baseline(df):
    """Empirical random permutation per case, seeds 0-4, mean (tierA convention).
    Analytic floor = 0.25 for Hit@1 with 4 candidates."""
    per_seed = []
    by_family_seeds = {f: [] for f in FAMILIES}
    for sd in SEEDS:
        rng = np.random.RandomState(sd)
        mrc_rows = []
        by_family = {f: [] for f in FAMILIES}
        for i in df.index[df["injected"] == "1"]:
            order = AGENTS[:]
            rng.shuffle(order)
            mrc = mrcbench(order, roots_of(df.at[i, "root_cause_set"]))
            mrc_rows.append(mrc)
            fam = df.at[i, "kind"]
            if fam in by_family:
                by_family[fam].append(mrc)
        per_seed.append(macro(mrc_rows))
        for f in FAMILIES:
            if by_family[f]:
                by_family_seeds[f].append(macro(by_family[f]))
    return {
        "label": "random (empirical, seeds 0-4 mean; analytic floor hit@1=0.25)",
        "overall": macro(per_seed),
        "by_family": {f: macro(v) for f, v in by_family_seeds.items() if v},
    }


def run_always_synthesizer(df):
    """Constant always-Synthesizer baseline through the identical scoring path
    (score_whowhen convention; GT prior = Synthesizer share of faulted rows,
    computed per dataset — v1 40/64=62.5%, v2 36/96=37.5%; footnote [6])."""
    ranked = pad_canonical(["Recommendation_Synthesizer"])
    mrc_rows = []
    by_family = {f: [] for f in FAMILIES}
    for i in df.index[df["injected"] == "1"]:
        mrc = mrcbench(ranked, roots_of(df.at[i, "root_cause_set"]))
        mrc_rows.append(mrc)
        fam = df.at[i, "kind"]
        if fam in by_family:
            by_family[fam].append(mrc)
    return {
        "label": "always_synthesizer (const baseline)",
        "ranked": ranked,
        "overall": macro(mrc_rows),
        "by_family": {f: macro(v) for f, v in by_family.items() if v},
    }


# -------------------------------------------------------------- self checks -----
def run_self_checks(df, results, channels):
    """SPEC §4: 4 mandatory self-checks. Returns dict of named booleans + detail."""
    checks = {}

    # (1) normal-baseline stats consistent with the CSV (8 rows): independent
    # re-read with the csv module, compare every normal-row duration value used.
    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    normals_csv = [r for r in rows if r["injected"] == "0"]
    n_normal = int((df["injected"] == "0").sum())
    ok1 = len(normals_csv) == n_normal and n_normal > 0
    detail1 = {"n_normal_rows_csv": len(normals_csv), "n_normal_rows_df": n_normal}
    for ch in channels:
        M = agent_matrix(df, ch)
        used = M.loc[df["injected"] == "0"].reset_index(drop=True)
        ref = []
        for r in normals_csv:
            ref.append([float(r[CHANNELS[ch].format(a=a)]) for a in AGENTS])
        ref = np.array(ref)
        same = used.shape == (n_normal, len(AGENTS)) and np.allclose(used.to_numpy(), ref, rtol=0, atol=1e-9)
        detail1[f"channel_{ch}_matrix_matches_csv"] = bool(same)
        ok1 = ok1 and same
    checks["1_normal_baseline_matches_csv"] = {"pass": bool(ok1), **detail1}

    # (2) every scored ranking contains exactly the 4 agents, no dups — including
    # EVERY seed's padded ranking in the RCD family runs (not just seed 0): an
    # rcd() exception on any seed >= 1 sets ranked_padded=None and would otherwise
    # be silently scored as an all-zero empty ranking; it must fail here visibly.
    ok2, n_checked, n_seed_rankings = True, 0, 0
    for m in results.get("methods", {}).values():
        for ch_res in m.get("channels", {}).values():
            for pc in ch_res.get("per_case", []) or ch_res.get("per_case_seed0", []):
                ranked = pc.get("ranked") or pc.get("ranked_padded_seed0") or []
                n_checked += 1
                if not (len(ranked) == 4 and set(ranked) == set(AGENTS)):
                    ok2 = False
            for fam_rec in (ch_res.get("family_runs") or {}).values():
                for sd_rec in fam_rec["seeds"].values():
                    ranked = sd_rec.get("ranked_padded") or []
                    n_seed_rankings += 1
                    if sd_rec.get("error") or not (len(ranked) == 4 and set(ranked) == set(AGENTS)):
                        ok2 = False
    checks["2_rankings_exactly_4_agents_no_dup"] = {
        "pass": bool(ok2), "n_rankings_checked": n_checked,
        "n_rcd_family_seed_rankings_checked_all_seeds": n_seed_rankings,
    }

    # (3) RCD prerequisites: patched causallearn path + pinned version
    checks["3_causallearn_patched_and_pinned"] = {
        "pass": ("third_party" in _CL_FILE) and (_CL_VERSION == "0.1.2.3"),
        "causallearn_file": _CL_FILE,
        "causal_learn_version": _CL_VERSION,
    }

    # (4) GT coverage: every faulted row has exactly one root, in AGENTS (count derived)
    faulted = df[df["injected"] == "1"]
    n_faulted = int(len(faulted))
    ok4 = n_faulted > 0
    bad = []
    for i in faulted.index:
        roots = roots_of(df.at[i, "root_cause_set"])
        if len(roots) != 1 or roots[0] not in AGENTS:
            ok4 = False
            bad.append(df.at[i, "run_id"])
    checks["4_gt_coverage_all_faulted_single_root_in_agents"] = {
        "pass": bool(ok4), "n_faulted": n_faulted, "n_faulted_covered": n_faulted - len(bad),
        "bad_rows": bad,
    }
    return checks


# ------------------------------------------------------------------ report ------
def fmt(x):
    return "%.3f" % x


def _fam_sizes_str(results):
    nbf = results["n_by_family"]
    return " / ".join("%s %d" % (f, nbf[f]) for f in FAMILIES)


def markdown_report(results):
    r = results
    fam_sizes = _fam_sizes_str(r)
    lines = []
    lines.append("# BARO / RCD / Eadro on agentfault — honest infra negatives (SPEC_INFRA)")
    lines.append("")
    lines.append("Dataset: `%s` (%d rows = %d faulted + %d normal; families: %s; single-root GT). "
                 "Candidates = the 4 in-process LangGraph agents. Generated by "
                 "`scripts/chaos/agentfault/eval/infra_negatives/run_infra_negatives.py` "
                 "(offline, deterministic)." % (
                     r["dataset"], r["n_rows"], r["n_faulted"], r["n_normal"], fam_sizes))
    lines.append("")
    lines.append("**Headline: on the honest channel (corrected durations) both BARO and RCD sit at/near "
                 "the random floor — the expected structural negative (footnote [3]): agent semantic "
                 "faults do not perturb infra scalars once the injection overhead is removed.**")
    lines.append("")

    # -------- how far each method runs
    lines.append("## 各方法\"能跑到哪一步\" (feeding modes & structural probes)")
    lines.append("")
    lines.append("| method | original pipeline | what structurally fails here | what was actually run |")
    lines.append("|---|---|---|---|")
    lines.append("| BARO | BOCPD change-point (stage-1) + RobustScorer ranking (stage-2) on within-case "
                 "multivariate time series | no within-case series (1 scalar per agent per case) -> "
                 "stage-1 impossible; note the vendored RCAEval `baro()` is already the "
                 "change-point-given variant (no BOCPD inside) | ORIGINAL vendored `baro()` on a "
                 "case-level pseudo-series (%d normal cases = before, 1 target case = after, "
                 "inject_time given), `dataset=None`, 'time' filtered from ranks post-hoc [2] |"
                 % r["n_normal"])
    lines.append("| RCD | chi-square CI causal discovery (Psi-PC over F-node) on multi-row "
                 "normal/anomalous segments | per-case = 1 anomalous row -> chi-square CI "
                 "statistically void: `dataset=\"recshop\"` gives an empty ranking, `dataset=None` "
                 "runs but is degenerate (probes below, incl. full %d-case appendix run); "
                 "ranking is F-node neighbours (possibly partial) | ORIGINAL vendored `rcd()` "
                 "family-pooled (%d normal rows vs %s family rows), seeds 0-4, family ranking "
                 "assigned to each case (族级单排名摊派), canonical padding for MRCBench [2] |" % (
                     r["n_faulted"], r["n_normal"], fam_sizes))
    lines.append("| Eadro | trace-mined dependency graph + multimodal time series + supervised GNN | "
                 "all three inputs missing/degenerate — see the Eadro section | NOT RUN (structural N/A) |")
    lines.append("")
    pb = r["feeding_mode"]["probes"]
    lines.append("Structural probes (recorded, per SPEC honesty rule):")
    lines.append("")
    lines.append("- **BARO with `dataset=\"recshop\"` (preprocess path):** outcome=`%s`, ranks=`%s` — %s" % (
        pb["baro_dataset_recshop"]["outcome"], pb["baro_dataset_recshop"].get("ranks"),
        pb["baro_dataset_recshop"].get("note", pb["baro_dataset_recshop"].get("error", ""))))
    for key, label in (("rcd_per_case_recshop", "RCD per-case (`dataset=\"recshop\"`)"),
                       ("rcd_per_case_none", "RCD per-case (`dataset=None`)")):
        p = pb[key]
        got = ("ranks=`%s`" % p.get("ranks")) if p["outcome"] == "ran" else ("error=`%s`" % p.get("error"))
        note = p.get("note")
        lines.append("- **%s:** outcome=`%s`, %s%s" % (label, p["outcome"], got,
                                                       (" — " + note) if note else ""))
    fp = pb.get("rcd_per_case_full_corrected")
    if fp:
        lines.append("- **RCD per-case FULL appendix run (all %d faulted cases, `dataset=None`, seed 0, "
                     "corrected):** %d/%d non-empty rankings, %d exception(s), top-1 histogram `%s`, "
                     "hit@1=%.3f (at/below the constant-Synthesizer prior; NOT below the 0.25 random "
                     "floor), 'time' in raw ranks in %d case(s). "
                     "Reconciliation of the probe above with the family-pooling adaptation: per-case "
                     "RCD RUNS but is DEGENERATE — a chi-square CI with exactly one anomalous sample "
                     "is statistically void and the output is near-constant; that degeneracy (not "
                     "'cannot run') is why the headline RCD rows are family-pooled." % (
                         fp["n_cases"], fp["n_nonempty_rankings"], fp["n_cases"], fp["n_exceptions"],
                         json.dumps(fp["top1_histogram"], ensure_ascii=False), fp["hit@1"],
                         fp["n_cases_with_time_in_raw_ranks"]))
    lines.append("")

    # -------- main table
    lines.append("## 主表 — Hit@1 headline (MRCBench, macro over cases) [1][2][4][5]")
    lines.append("")
    nbf = r["n_by_family"]
    fam_hdr = "".join(" %s (n=%d) |" % (f, nbf[f]) for f in FAMILIES)
    hdr = ("| method (adaptation label) | channel | overall hit@1 |" + fam_hdr
           + " hit@3 [5] | hit@5 [5] | ndcg@3 | mrr |")
    lines.append(hdr)
    lines.append("|" + "---|" * (7 + len(FAMILIES)))

    def row_from(label, channel, overall, byfam, suffix=""):
        cells = [label, channel, fmt(overall.get("hit@1", 0.0)) + suffix]
        for f in FAMILIES:
            d = byfam.get(f)
            cells.append(fmt(d.get("hit@1", 0.0)) if d else "-")
        cells += [fmt(overall.get("hit@3", 0.0)), fmt(overall.get("hit@5", 0.0)),
                  fmt(overall.get("ndcg@3", 0.0)), fmt(overall.get("mrr", 0.0))]
        return "| " + " | ".join(cells) + " |"

    def ch_cell(ch):
        # raw rows must carry the contamination-contrast flag on the cell itself
        # (footnote [4]) — the guard text must be wired to the misquotable cell.
        return "raw [4]" if ch == "raw" else ch

    meth = r["methods"]
    if "baro_stage2" in meth:
        for ch, res in meth["baro_stage2"]["channels"].items():
            lines.append(row_from(BARO_LABEL, ch_cell(ch), res["overall"], res["by_family"]))
    if "rcd" in meth:
        for ch, res in meth["rcd"]["channels"].items():
            ov = {k: v["mean"] for k, v in res["overall_seedagg"].items()}
            bf = {f: {k: v["mean"] for k, v in agg.items()}
                  for f, agg in res["by_family_seedagg"].items()}
            rng = res["overall_seedagg"]["hit@1"]
            suffix = " (seeds 0-4 mean; range %s–%s)" % (fmt(rng["min"]), fmt(rng["max"]))
            lines.append(row_from(RCD_LABEL, ch_cell(ch), ov, bf, suffix=suffix))
    bl = r["baselines"]
    lines.append(row_from(bl["random"]["label"], "-", bl["random"]["overall"], bl["random"]["by_family"]))
    lines.append(row_from(bl["always_synthesizer"]["label"], "-",
                          bl["always_synthesizer"]["overall"], bl["always_synthesizer"]["by_family"]))
    lines.append("")
    lines.append("raw rows are a CONTAMINATION CONTRAST, not usable results — footnote [4] "
                 "(the lift, e.g. BARO raw hallucinate, is the injection's own latency artifact).")
    lines.append("")

    # -------- RCD seed stability
    if "rcd" in meth:
        lines.append("## RCD 种子稳定性 (seeds 0-4)")
        lines.append("")
        lines.append("| channel | family | rankings by seed (raw, unpadded) | distinct rankings | distinct top-1 | stable? |")
        lines.append("|" + "---|" * 6)
        for ch, res in meth["rcd"]["channels"].items():
            for f in FAMILIES:
                st = res["seed_stability"][f]
                rk = "; ".join("[" + ",".join(s) + "]" if s else "[]" for s in st["rankings_by_seed"])
                lines.append("| %s | %s | %s | %d | %d | %s |" % (
                    ch, f, rk, st["n_distinct_rankings"], st["n_distinct_top1"],
                    "yes" if st["stable"] else "no"))
        lines.append("")

    # -------- Eadro
    lines.append("## Eadro — 结构性 N/A(不跑,写清为什么)")
    lines.append("")
    lines.append(EADRO_TEXT.format(n_rows=r["n_rows"], n_faulted=r["n_faulted"]))

    # -------- self checks
    lines.append("## 自检 (SPEC §4)")
    lines.append("")
    for name, c in r["self_checks"].items():
        lines.append("- [%s] %s — %s" % ("PASS" if c["pass"] else "FAIL", name,
                                         json.dumps({k: v for k, v in c.items() if k != "pass"},
                                                    ensure_ascii=False)))
    lines.append("")

    # -------- footnotes (numbers derived from the dataset, not hardcoded)
    _fmt = {"n_faulted": r["n_faulted"], "n_rows": r["n_rows"], "n_normal": r["n_normal"],
            "fam_sizes": fam_sizes, "synth_prior": r["synth_prior_str"]}
    footnotes = [fn.format(**_fmt) if "{" in fn else fn for fn in FOOTNOTES]
    if "rcd" in meth:
        # [5] rank-sensitivity caveat for the RCD rows, driven by the observed
        # n_returned (ranks beyond the returned F-node-neighbour prefix are
        # pad_canonical()'s fixed order, i.e. artifacts, not method signal).
        nret = sorted({sd_rec.get("n_returned", 0)
                       for res in meth["rcd"]["channels"].values()
                       for fam_rec in res["family_runs"].values()
                       for sd_rec in fam_rec["seeds"].values()})
        if nret == [1]:
            footnotes[4] += (" For the RCD rows specifically, every family/seed run returned a "
                             "single F-node neighbour (n_returned=1 — see family_runs in the JSON), "
                             "so ranks 2-4 are pad_canonical()'s fixed canonical order and "
                             "hit@3/ndcg@3/mrr are canonical-padding artifacts there, not method "
                             "signal; rank-sensitivity applies to the BARO rows only.")
        else:
            footnotes[4] += (" For the RCD rows, ranks beyond the returned F-node-neighbour prefix "
                             "(n_returned values %s across family/seed runs — see family_runs in "
                             "the JSON) are canonical padding; hit@3/ndcg@3/mrr are padding-inflated "
                             "wherever n_returned < 3." % nret)
    lines.append("## Footnotes (honesty items, SPEC §3)")
    lines.append("")
    for fn in footnotes:
        lines.append("- " + fn)
    lines.append("")
    lines.append("Environment: causallearn=`%s` (patched copy, causal-learn %s), pandas %s, numpy %s, "
                 "matplotlib %s. K_LIST=%s." % (
                     r["env"]["causallearn_file"], r["env"]["causal_learn_version"],
                     r["env"]["pandas"], r["env"]["numpy"], r["env"]["matplotlib"],
                     list(K_LIST)))
    return "\n".join(lines)


# -------------------------------------------------------------------- main ------
def main():
    global CSV_PATH, FAMILIES
    ap = argparse.ArgumentParser(description="BARO/RCD honest negatives on agentfault (SPEC_INFRA)")
    ap.add_argument("--method", choices=["baro", "rcd", "all"], default="all")
    ap.add_argument("--channel", choices=["corrected", "raw", "both"], default="both")
    # parametrization (defaults reproduce v1 EXACTLY; v2 run passes --dataset-dir).
    ap.add_argument("--dataset-dir", default=None,
                    help="convenience: sets csv=<dir>/dataset_agentfault.csv and "
                         "out=<dir>/infra_negatives unless --csv/--out-dir override. "
                         "Omit for the v1 default.")
    ap.add_argument("--csv", default=None, help="dataset CSV (default = v1)")
    ap.add_argument("--out-dir", default=None, help="output dir (default = v1)")
    args = ap.parse_args()

    # resolve paths — every default falls back to the v1 constant so that running
    # with NO flags reproduces v1 byte-for-byte (project iron rule: v2 must NOT
    # overwrite v1).
    csv_path, out_dir = args.csv, args.out_dir
    if args.dataset_dir:
        if csv_path is None:
            csv_path = os.path.join(args.dataset_dir, "dataset_agentfault.csv")
        if out_dir is None:
            out_dir = os.path.join(args.dataset_dir, "infra_negatives")
    if csv_path is None:
        csv_path = CSV_PATH
    if out_dir is None:
        out_dir = DEFAULT_OUT
    CSV_PATH = os.path.abspath(csv_path)

    np.random.seed(0)
    import random as _random
    _random.seed(0)

    channels = ["corrected", "raw"] if args.channel == "both" else [args.channel]
    df = load_df()
    FAMILIES = derive_families(df)  # data-driven; incl. context_drift on v2
    faulted = df[df["injected"] == "1"]
    n_by_family = {f: int((faulted["kind"] == f).sum()) for f in FAMILIES}
    n_faulted = int(len(faulted))
    n_normal = int((df["injected"] == "0").sum())
    n_synth = int((faulted["root_cause_set"] == "Recommendation_Synthesizer").sum())
    synth_prior_str = "%d/%d = %.1f%%" % (n_synth, n_faulted,
                                          (100.0 * n_synth / n_faulted) if n_faulted else 0.0)
    print("[main] families=%s  synth_prior=%s" % (list(FAMILIES), synth_prior_str))

    # structural probes (always on corrected channel; recorded honesty evidence)
    M0 = agent_matrix(df, "corrected")
    normal_block = M0.loc[df["injected"] == "0"].reset_index(drop=True)
    first_faulted = df.index[df["injected"] == "1"][0]
    target_row = M0.loc[[first_faulted]]
    probes = {
        "probe_case_run_id": df.at[first_faulted, "run_id"],
        "baro_dataset_recshop": probe_baro_recshop(normal_block, target_row),
        "rcd_per_case_recshop": probe_rcd_per_case(normal_block, target_row, "recshop"),
        "rcd_per_case_none": probe_rcd_per_case(normal_block, target_row, None),
        "rcd_per_case_full_corrected": probe_rcd_per_case_full(df, M0, normal_block),
    }
    probes["rcd_per_case_recshop"].setdefault(
        "note", "dataset!=None preprocess drop_constant kills the 1-row anomalous "
                "segment -> empty ranking")
    probes["rcd_per_case_none"].setdefault(
        "note", "mechanically runs, but the chi-square CI has exactly 1 anomalous "
                "sample (statistically void) and 'time' stays as a candidate "
                "variable -> degenerate; full %d-case appendix run = "
                "rcd_per_case_full_corrected" % n_faulted)

    fam_sizes_str = " / ".join("%s %d" % (f, n_by_family[f]) for f in FAMILIES)

    methods = {}
    if args.method in ("baro", "all"):
        methods["baro_stage2"] = {
            "label": BARO_LABEL,
            "channels": {ch: run_baro_channel(df, ch) for ch in channels},
        }
    if args.method in ("rcd", "all"):
        methods["rcd"] = {
            "label": RCD_LABEL,
            "channels": {ch: run_rcd_channel(df, ch) for ch in channels},
        }

    baselines = {
        "random": run_random_baseline(df),
        "always_synthesizer": run_always_synthesizer(df),
    }

    results = {
        "spec": "scripts/chaos/agentfault/eval/infra_negatives/SPEC_INFRA.md",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "dataset": os.path.relpath(CSV_PATH, REPO).replace("\\", "/"),
        "n_rows": int(len(df)),
        "n_faulted": int((df["injected"] == "1").sum()),
        "n_normal": int((df["injected"] == "0").sum()),
        "n_by_family": n_by_family,
        "synth_prior_str": synth_prior_str,
        "families": list(FAMILIES),
        "agents": AGENTS,
        "channels_run": channels,
        "env": {
            "python": sys.version.split()[0],
            "pandas": pd.__version__,
            "numpy": np.__version__,
            "matplotlib": matplotlib.__version__,
            "causallearn_file": _CL_FILE,
            "causal_learn_version": _CL_VERSION,
        },
        "feeding_mode": {
            "baro": {
                "label": BARO_LABEL,
                "function": "third_party/RCAEval/RCAEval/e2e/baro.py::baro (ORIGINAL, single-file loaded)",
                "how": "per faulted case: rows = %d normal cases (time 0..%d) + target case (time %d); "
                       "columns = <agent>_latency; inject_time=%d explicit (change-point given); "
                       "dataset=None (dataset!=None is degenerate on 1-row segment — see probe); "
                       "'time' column filtered from ranking post-hoc." % (
                           n_normal, n_normal - 1, n_normal, n_normal),
                "is_analog": False,
            },
            "rcd": {
                "label": RCD_LABEL,
                "function": "third_party/RCAEval/RCAEval/e2e/rcd.py::rcd (ORIGINAL, single-file loaded, "
                            "patched causallearn 0.1.2.3)",
                "how": "per fault family: normal_df = %d normal rows, anomalous_df = family rows "
                       "(%s); "
                       "dataset='recshop', gamma=5, localized=True, bins=5, seeds 0-4; " % (
                           n_normal, fam_sizes_str) +
                       "single family ranking assigned to every case of the family; "
                       "partial F-node-neighbour output padded with remaining agents in canonical order. "
                       "Per-case path probed, NOT structurally impossible: dataset='recshop' -> empty "
                       "ranking (drop_constant on 1-row segment); dataset=None -> runs but degenerate "
                       "(1 anomalous sample chi-square is void; 'time' stays a candidate) — see probes "
                       "incl. rcd_per_case_full_corrected.",
                "is_analog": False,
            },
            "eadro": {"label": "Eadro — NOT RUN, structural N/A", "reason_doc": "see markdown report"},
            "probes": probes,
        },
        "methods": methods,
        "baselines": baselines,
    }
    results["self_checks"] = run_self_checks(df, results, channels)

    os.makedirs(out_dir, exist_ok=True)
    out_json = os.path.join(out_dir, "infra_negatives_results.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    out_md = os.path.join(out_dir, "RESULTS_INFRA_NEGATIVES.md")
    report = markdown_report(results)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(report + "\n")

    print(report)
    print("\n[written] %s" % out_json)
    print("[written] %s" % out_md)
    all_pass = all(c["pass"] for c in results["self_checks"].values())
    print("[self-checks] %s" % ("ALL PASS" if all_pass else "FAILURES PRESENT"))
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
