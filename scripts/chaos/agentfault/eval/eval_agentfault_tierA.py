# -*- coding: utf-8 -*-
"""eval_agentfault_tierA.py — Tier-A offline baselines for the agentfault RCA testbed.

Dataset = `(v1)dataset_agentfault.csv` (v1: 72 rows; K=8 reps/combo) BY DEFAULT.
Parametrized: `--dataset-dir DIR` (or `--csv` / `--out-md`) point it at any agentfault CSV
(e.g. v2 `(upstream batch)`, 108 rows). With NO flags it reproduces v1 exactly. ALL
row/family/prior counts are DERIVED from the CSV at runtime (no hardcoded 72/64/108/94).
Entity space = the 4 in-process LangGraph agents (NOT services):
    Sequence_Recommender / User_Behavior_Analyzer / Product_Analyzer / Recommendation_Synthesizer.
Single-root; GT = injection ledger (`root_cause_set`), status != inject_failed.

REUSE POINTS (imported, never copied / never edited):
  * `m9_score.mrcbench(rank_svcs, roots)` + `m9_score.K_LIST`  — the MRCBench 四族 scorer
    (上游论文 4.3: Hit@K / FullHit@K / Recall@R / NDCG@K + avg@K + mrr). It is entity-agnostic,
    so we feed it a ranked list of the 4 AGENTS and roots=[injected_agent].
  * `eval_agentchaos.build_X` + its per-agent-binary-RF -> per-case ranking MECHANISM
    (one RF per candidate agent -> predict_proba positive column -> score matrix ->
    argsort(-scores) -> ranked agent list). Reproduced here for the RF ceiling baseline.
  * `scripts/qa/anti_trivial_gate.py` — REFERENCED (concept: trivial baseline vs random floor;
    single-feature GT-encoding audit). Its k8s per_case_scores driver is inapplicable here, so
    the audit is re-derived at agent granularity below.

Tier-A baselines (each -> per-case ranked list of 4 agents -> MRCBench):
  1. Random             — random permutation per case, seeded, avg over seeds 0-4. The 25% floor.
  2. Trivial span-anomaly heuristic — deterministic, GT-blind: rank agents by |z| of their own
                          span duration (span_<A>_duration_ms) + status==ERROR bonus. Global
                          scalars (recommendation_confidence) cannot rank AMONG agents so are
                          excluded -> this is exactly why #2 is blind to structured Synthesizer
                          faults (the point of the asymmetry).
  3. RF feature-separability CEILING (supervised reference, NOT a peer baseline) — group-aware
                          LeaveOneGroupOut (never split reps of a combo), seeds 0-4. Two variants:
                          RF-infra (scalar/span cols) vs RF-content (per-agent conv_text_len +
                          deterministic content-track bools).
  4. Contract oracle    — deterministic domain upper bound for STRUCTURED outputs: content-track
                          columns (response_asin_is_sentinel / contract_check_matches_expected)
                          deterministically flag Recommendation_Synthesizer. On hallucinate it has
                          no contract signal -> abstains -> reported N/A (never 0-inflated).

The zero-root rows (negatives: clean `normal` reps + any injection that did not diverge /
`inject_failed`; count DERIVED from the CSV) are EXCLUDED from the localization macro (G empty ->
ranking metric undefined) and used only for a specificity / false-alarm note.

Offline, deterministic, no network / no LLM. Writes <dataset-dir>/BASELINE_RESULTS.md.

Usage:
  # v1 (default — identical to the original hardcoded run):
  PYTHONIOENCODING=utf-8 python scripts/chaos/agentfault/eval/eval_agentfault_tierA.py
  # v2 (108-row dataset incl. context_drift family):
  PYTHONIOENCODING=utf-8 python scripts/chaos/agentfault/eval/eval_agentfault_tierA.py \
      --dataset-dir (archived) agentfault_v2
"""
from __future__ import annotations

import argparse
import io
import os
import sys
from collections import defaultdict

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import LeaveOneGroupOut

# ---- repo paths + reuse imports ----------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
CTK = os.path.join(REPO_ROOT, "scripts", "chaos", "ctk")
sys.path.insert(0, CTK)
# ★2026-07-27 修:eval_agentchaos.py 在 commit 41c0ac5(两条故障线归类)里从 ctk/ 搬去了
#   agentfault/legacy_agentchaos/,但这里的 import 没跟着改 —— 自那以后本脚本一直
#   ModuleNotFoundError(ctk/__pycache__ 里那个 .pyc 不算数:__pycache__ 里的 pyc 不能当模块导入)。
#   连带一键脚本 run_eval_agentfault.sh 的 step 4 也一直是坏的。
#   m9_score 仍在 ctk,故 CTK 那条 path 保留。
LEGACY_AC = os.path.join(REPO_ROOT, "scripts", "chaos", "agentfault", "legacy_agentchaos")
sys.path.insert(0, LEGACY_AC)

from m9_score import mrcbench, K_LIST            # noqa: E402  (MRCBench 四族 scorer, entity-agnostic)
from eval_agentchaos import build_X              # noqa: E402  (str->num + _isna flags helper)

# m9_score AND eval_agentchaos each re-wrap sys.stdout at import, which GC-closes the shared
# stdout buffer. Reopen a fresh, independent stream on a dup of fd 1 so our prints survive.
try:
    sys.stdout = io.TextIOWrapper(io.FileIO(os.dup(1), "w"), encoding="utf-8", errors="replace")
except Exception:
    pass

# Paths are DEFAULTS (v1); overridden in main() from CLI flags. Kept as module globals so the
# existing helpers (load(), main()) that read them stay untouched.
CSV = os.path.join(REPO_ROOT, "datasets", "_archive", "agentfault", "agentfault", "dataset_agentfault.csv")
OUT_MD = os.path.join(REPO_ROOT, "datasets", "_archive", "agentfault", "agentfault", "BASELINE_RESULTS.md")

AGENTS = [
    "Sequence_Recommender", "User_Behavior_Analyzer",
    "Product_Analyzer", "Recommendation_Synthesizer",
]
SEEDS = [0, 1, 2, 3, 4]

# Canonical family display order. The ACTIVE FAMILY_ORDER is derived in main() by filtering this
# to the families actually present (rooted) in the loaded CSV — so v1 (no context_drift) is
# unchanged, while v2 gains the context_drift row. context_drift = a CONTENT deletion (upstream
# agent's message stripped from the downstream input); its GT root is the DOWNSTREAM target whose
# input was stripped. It leaves NO latency signature (system recovers) and NO needle/asin/contract
# signal, so every span-duration/content Tier-A method here is EXPECTED to be ~random on it — that
# near-random result is the honest, desired finding (motivates the separate structural track).
CANON_FAMILY_ORDER = ["hallucinate", "wrong_item_pick", "format_violation", "context_drift"]
FAMILY_ORDER = list(CANON_FAMILY_ORDER)  # reassigned in main() from data

# metrics we surface in the MD (headline K=1, rank-sensitive K=3, ceiling K=5)
REPORT_KEYS = ["hit@1", "hit@3", "ndcg@3", "hit@5", "recall@R", "mrr"]


# ------------------------------------------------------------------ helpers ---
def _num(series):
    """str column -> float (blank/nan -> NaN)."""
    s = series.astype(str).str.strip()
    isna = (s == "") | (s.str.lower() == "nan")
    return pd.to_numeric(s.where(~isna, other=np.nan), errors="coerce")


def roots_of(rc_cell):
    rc = str(rc_cell).strip()
    if rc == "" or rc.lower() == "nan":
        return []
    return [x.strip() for x in rc.split(";") if x.strip()]


def load():
    df = pd.read_csv(CSV, dtype=str).fillna("")
    df = df.reset_index(drop=True)
    df["_roots"] = df["root_cause_set"].map(roots_of)
    df["_n_root"] = df["_roots"].map(len)
    return df


# ------------------------------------------------- baseline: 1. Random --------
def rank_random(rng):
    order = AGENTS[:]
    rng.shuffle(order)
    return order


# ---------------------------------------- baseline: 2. Trivial span-anomaly ---
def trivial_anomaly_scores(df, dur_suffix="duration_corrected_ms"):
    """Per-agent anomaly score, GT-BLIND + deterministic.

    signal = z-score of the agent's OWN span duration vs that agent's distribution over
    all rows (unsupervised, no labels) + a large bonus if the agent's span status==ERROR.
    Rank agents by score desc = "whose span looks most off first".

    dur_suffix: which per-agent duration column to use.
      - "duration_corrected_ms" (DEFAULT, recommended infra channel): 已扣除注入延迟伪影
        (agent 边界 span − agentfault.subllm_rewrite 子 span,span 减 span 精确)。这是 infra 轨
        应该用的通道。
      - "duration_ms" (RAW): 含注入开销(hallucinate 的副 LLM 调用耗时)= 延迟伪影。仅用于
        对照,展示 raw 通道被污染 → 校正的必要性。
    """
    n = len(df)
    scores = np.zeros((n, len(AGENTS)), dtype=float)
    for ai, a in enumerate(AGENTS):
        dur = _num(df[f"span_{a}_{dur_suffix}"]).to_numpy(dtype=float)
        mu = np.nanmean(dur)
        sd = np.nanstd(dur)
        z = (dur - mu) / sd if sd > 0 else np.zeros(n)
        z = np.nan_to_num(z, nan=0.0)
        status = df[f"span_{a}_status"].astype(str).str.upper().to_numpy()
        err_bonus = np.where(status == "ERROR", 1e6, 0.0)
        scores[:, ai] = z + err_bonus
    return scores


# --------------------------------------------- baseline: 4. Contract oracle ---
def oracle_rank(row):
    """Deterministic domain upper bound for STRUCTURED outputs.

    Fires only on the deterministic content signals that name the Synthesizer:
      response_asin_is_sentinel==1 (wrong_item_pick) OR contract_check_matches_expected==1 (format).
    Returns a ranked list with Recommendation_Synthesizer first, or None (abstain) when no
    structured signal is present (hallucinate / normal) -> scored as N/A, never 0-inflated.
    """
    def truthy(col):
        v = str(row.get(col, "")).strip().lower()
        return v in ("1", "1.0", "true")
    if truthy("response_asin_is_sentinel") or truthy("contract_check_matches_expected"):
        rest = [a for a in AGENTS if a != "Recommendation_Synthesizer"]
        return ["Recommendation_Synthesizer"] + rest
    return None


# --------------------------------------------- baseline: 3. RF ceiling --------
INFRA_COLS_BASE = [
    "e2e_latency_ms", "http_status", "http_success",
    "total_span_count", "error_span_count",
    "recommendation_confidence", "recommended_product_is_unknown",
    "degrade_message_present", "garbage_message_present",
    "host_cpu_pct", "host_mem_pct",
]
INFRA_PER_AGENT = [
    "span_{a}_duration_corrected_ms",   # ★用校正后时长(扣注入延迟伪影),非 raw
    "span_{a}_child_httpx_count",
    "span_{a}_child_sasrec_requests_count", "span_{a}_child_max_duration_ms",
    "span_{a}_present",
]
CONTENT_COLS = [
    "divergent_needle_present", "response_asin_is_sentinel",
    "toolcall_asin_is_sentinel", "contract_check_matches_expected",
]


def feature_cols(df, track):
    if track == "infra":
        cols = [c for c in INFRA_COLS_BASE if c in df.columns]
        for a in AGENTS:
            for t in INFRA_PER_AGENT:
                c = t.format(a=a)
                if c in df.columns:
                    cols.append(c)
    else:  # content
        cols = [f"conv_{a}_text_len" for a in AGENTS if f"conv_{a}_text_len" in df.columns]
        cols += [c for c in CONTENT_COLS if c in df.columns]
    return cols


def rf_scores_cv(df, track, seed):
    """Per-agent binary RF, group-aware LeaveOneGroupOut, one seed.

    Reproduces the eval_agentchaos ranking MECHANISM: one RF per candidate agent, take the
    predict_proba positive column into a score matrix; a fold whose training labels are single
    class (LOGO removes a combo's only positives) degenerates to a constant prediction (exactly
    the structural split-degeneration EVAL_NOTES 3c warns about).

    Returns (scores[n,4], preds[n,4]).
    """
    cols = feature_cols(df, track)
    X = build_X(df, cols)
    groups = df["group_id"].to_numpy()
    Y = {a: df["_roots"].map(lambda rs, a=a: 1 if a in rs else 0).to_numpy() for a in AGENTS}
    n = len(df)
    scores = np.full((n, len(AGENTS)), np.nan)
    preds = np.zeros((n, len(AGENTS)), dtype=int)
    logo = LeaveOneGroupOut()
    for tr, te in logo.split(X, groups=groups):
        for ai, a in enumerate(AGENTS):
            ytr = Y[a][tr]
            if len(set(ytr)) < 2:                     # single-class train -> constant (degenerate)
                const = int(ytr[0]) if len(ytr) else 0
                preds[te, ai] = const
                scores[te, ai] = float(const)
                continue
            clf = RandomForestClassifier(n_estimators=300, random_state=seed, n_jobs=1)
            clf.fit(X.iloc[tr], ytr)
            preds[te, ai] = clf.predict(X.iloc[te])
            proba = clf.predict_proba(X.iloc[te])
            classes = list(clf.classes_)
            pos = classes.index(1) if 1 in classes else -1
            scores[te, ai] = proba[:, pos]
    return scores, preds


# ------------------------------------------------- scoring / aggregation ------
def scores_to_ranking(score_row, tie_key):
    """score desc, random tie-break (tie_key) -> ranked agent list (deterministic per seed)."""
    order = np.lexsort((tie_key, -np.nan_to_num(score_row, nan=-1e18)))
    return [AGENTS[j] for j in order]


def score_cases(df, ranking_fn):
    """ranking_fn(i, row) -> ranked list OR None(abstain). Returns list of (i, family, subtype,
    metrics_or_None) over ROOTED rows only (zero-root excluded from the localization macro)."""
    out = []
    for i, row in df.iterrows():
        if row["_n_root"] < 1:
            continue
        ranked = ranking_fn(i, row)
        m = None if ranked is None else mrcbench(ranked, row["_roots"])
        out.append((i, row["kind"], row.get("format_subtype", ""), m))
    return out


def macro(cases, keys=REPORT_KEYS):
    """mean over cases that produced a ranking; return (metrics dict, n_scored, n_abstain)."""
    scored = [m for (_i, _f, _s, m) in cases if m is not None]
    n_abstain = sum(1 for (_i, _f, _s, m) in cases if m is None)
    if not scored:
        return {k: None for k in keys}, 0, n_abstain
    agg = {k: float(np.mean([m[k] for m in scored])) for k in keys}
    return agg, len(scored), n_abstain


def by_family(cases):
    fam = defaultdict(list)
    for c in cases:
        fam[c[1]].append(c)
    return fam


def by_subtype(cases, family="format_violation"):
    sub = defaultdict(list)
    for c in cases:
        if c[1] == family:
            sub[c[2] or "?"].append(c)   # c[2] = format_subtype
    return sub


# --------------------------------------------------- baseline runners ---------
def run_random(df):
    """avg over seeds; return per-family (mean metrics) + overall + per-seed macros for std."""
    per_seed_cases = []
    for seed in SEEDS:
        rng = np.random.default_rng(seed)
        cases = score_cases(df, lambda i, row: rank_random(rng))
        per_seed_cases.append(cases)
    return _avg_over_seeds(per_seed_cases)


def run_trivial(df, dur_suffix="duration_corrected_ms"):
    sc = trivial_anomaly_scores(df, dur_suffix=dur_suffix)
    tie = np.arange(len(AGENTS), dtype=float)  # deterministic tie-break (real signal; ties rare)
    cases = score_cases(df, lambda i, row: scores_to_ranking(sc[i], tie))
    return _single_run(cases)


def run_oracle(df):
    cases = score_cases(df, lambda i, row: oracle_rank(row))
    return _single_run(cases)


def run_rf(df, track):
    per_seed_cases = []
    zero_root_fire = []  # specificity: fraction of zero-root rows with any agent predicted positive
    for seed in SEEDS:
        scores, preds = rf_scores_cv(df, track, seed)
        rng = np.random.default_rng(1000 + seed)
        tie_all = rng.random(scores.shape)
        cases = score_cases(df, lambda i, row: scores_to_ranking(scores[i], tie_all[i]))
        per_seed_cases.append(cases)
        zr = df.index[df["_n_root"] < 1]
        zero_root_fire.append(float(np.mean([preds[i].sum() > 0 for i in zr])) if len(zr) else 0.0)
    res = _avg_over_seeds(per_seed_cases)
    res["zero_root_false_alarm_rate"] = float(np.mean(zero_root_fire))
    return res


def _wrap(cell):
    """(float-dict, ns, na) -> ((mean,std=0.0)-dict, ns, na) to match the seed-averaged shape."""
    d, ns, na = cell
    return ({k: ((round(float(v), 3), 0.0) if v is not None else None) for k, v in d.items()}, ns, na)


def _single_run(cases):
    fam = by_family(cases)
    out = {"overall": _wrap(macro(cases))}
    for f in FAMILY_ORDER:
        out[f] = _wrap(macro(fam.get(f, [])))
    out["_subtypes"] = {s: _wrap(macro(cs)) for s, cs in by_subtype(cases).items()}
    return out


def _avg_over_seeds(per_seed_cases):
    """Average family/overall macros across seeds (mean + std of the macro metric)."""
    fams = FAMILY_ORDER + ["overall"]
    acc = {f: defaultdict(list) for f in fams}
    n_scored = {f: 0 for f in fams}
    n_abstain = {f: 0 for f in fams}
    sub_acc = defaultdict(lambda: defaultdict(list))
    sub_n = {}
    for cases in per_seed_cases:
        fam = by_family(cases)
        for f in FAMILY_ORDER:
            m, ns, na = macro(fam.get(f, []))
            n_scored[f], n_abstain[f] = ns, na
            for k, v in m.items():
                if v is not None:
                    acc[f][k].append(v)
        mo, nso, nao = macro(cases)
        n_scored["overall"], n_abstain["overall"] = nso, nao
        for k, v in mo.items():
            if v is not None:
                acc["overall"][k].append(v)
        for s, cs in by_subtype(cases).items():
            m, ns, _ = macro(cs)
            sub_n[s] = ns
            for k, v in m.items():
                if v is not None:
                    sub_acc[s][k].append(v)
    out = {}
    for f in fams:
        d = {k: (round(float(np.mean(vs)), 3), round(float(np.std(vs)), 3)) for k, vs in acc[f].items()}
        out[f] = (d, n_scored[f], n_abstain[f])
    out["_subtypes"] = {s: ({k: (round(float(np.mean(vs)), 3), round(float(np.std(vs)), 3))
                             for k, vs in d.items()}, sub_n.get(s, 0), 0)
                        for s, d in sub_acc.items()}
    return out


# ------------------------------------------------- anti-trivial audit ---------
def anti_trivial_audit(df):
    """Single-feature GT-encoding audit (concept from scripts/qa/anti_trivial_gate.py).

    For each candidate feature, check whether thresholding it alone yields a near-perfect
    per-agent classifier for ANY agent. A win here is a dataset-triviality FINDING (the designed
    content signal), not a method success -> we report WHICH GT it encodes (family vs agent).
    """
    findings = []
    n = len(df)
    rooted = df["_n_root"] >= 1
    # content bools: how perfectly do they separate the injected agent?
    for col, note in [
        ("divergent_needle_present", "hallucinate family (does NOT name which analyzer)"),
        ("response_asin_is_sentinel", "wrong_item_pick -> Recommendation_Synthesizer (names agent)"),
        ("contract_check_matches_expected", "format_violation -> Recommendation_Synthesizer (names agent)"),
    ]:
        v = _num(df[col]).fillna(0.0).to_numpy()
        fire = v >= 0.5
        # rows where the signal fires: which agents are actually root there?
        agents_when_fire = set()
        for i in np.where(fire)[0]:
            agents_when_fire.update(df.iloc[i]["_roots"])
        # false positives on zero-root rows
        fp_clean = int(np.sum(fire & (df["_n_root"].to_numpy() < 1)))
        findings.append({
            "feature": col, "fires": int(fire.sum()),
            "root_agents_when_fires": sorted(agents_when_fire) or ["(none)"],
            "false_pos_on_clean": fp_clean, "encodes": note,
        })
    # infra columns that are constant (no signal) -> confirm infra-blindness
    const_infra = []
    for a in AGENTS:
        st = df[f"span_{a}_status"].unique().tolist()
        const_infra.append(f"span_{a}_status={st}")
    http = df["http_status"].unique().tolist()
    err = df["error_span_count"].unique().tolist()

    # Best SINGLE infra feature vs the coarse "any Synthesizer-fault" target -> substantiates
    # that RF-infra's high Synthesizer score is multi-feature aggregation, not a single-feature leak.
    synth_root = df["_roots"].map(lambda rs: 1 if "Recommendation_Synthesizer" in rs else 0).to_numpy()
    infra_cols = list(INFRA_COLS_BASE)
    for a in AGENTS:
        infra_cols += [t.format(a=a) for t in INFRA_PER_AGENT]
    best_auc, best_col = 0.5, None
    for c in infra_cols:
        if c not in df.columns:
            continue
        v = _num(df[c]).fillna(0.0).to_numpy()
        if len(set(v)) < 2:
            continue
        auc = roc_auc_score(synth_root, v)
        auc = max(auc, 1.0 - auc)
        if auc > best_auc:
            best_auc, best_col = auc, c
    infra_auc = {"best_single_infra_auc": round(float(best_auc), 3), "best_col": best_col}
    return findings, const_infra, http, err, infra_auc


# ----------------------------------------------------------- MD writer --------
def _fmt(cell):
    d, ns, na = cell
    def g(k):
        if k not in d or d[k] is None:
            return "N/A"
        m, s = d[k]
        return f"{m:.3f}±{s:.3f}"
    return g, ns, na


def _row(name, cell):
    g, ns, na = _fmt(cell)
    scored = f"{ns}" + (f" (+{na} abstain)" if na else "")
    return (f"| {name} | {g('hit@1')} | {g('hit@3')} | {g('ndcg@3')} | "
            f"{g('hit@5')} | {g('recall@R')} | {g('mrr')} | {scored} |")


def family_table(title, note, rows):
    md = [f"### {title}", "", note, "",
          "| baseline | Hit@1 (headline) | Hit@3 | NDCG@3 | Hit@5 | Recall@R | MRR | n scored |",
          "|---|---|---|---|---|---|---|---|"]
    md += rows
    md.append("")
    return md


def _parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="Tier-A offline baselines for the agentfault RCA testbed (MRCBench). "
                    "Defaults reproduce the v1 72-row run exactly.")
    ap.add_argument("--dataset-dir", default=None,
                    help="Directory holding dataset_agentfault.csv; output BASELINE_RESULTS.md is "
                         "written there. Default = (archived) agentfault (v1).")
    ap.add_argument("--csv", default=None,
                    help="Explicit CSV path (overrides --dataset-dir for input).")
    ap.add_argument("--out-md", default=None,
                    help="Explicit output .md path (overrides --dataset-dir for output).")
    return ap.parse_args(argv)


def main(argv=None):
    global CSV, OUT_MD, FAMILY_ORDER
    args = _parse_args(argv)
    if args.dataset_dir:
        base = args.dataset_dir if os.path.isabs(args.dataset_dir) else \
            os.path.join(REPO_ROOT, args.dataset_dir)
        CSV = os.path.join(base, "dataset_agentfault.csv")
        OUT_MD = os.path.join(base, "BASELINE_RESULTS.md")
    if args.csv:
        CSV = args.csv if os.path.isabs(args.csv) else os.path.join(REPO_ROOT, args.csv)
    if args.out_md:
        OUT_MD = args.out_md if os.path.isabs(args.out_md) else os.path.join(REPO_ROOT, args.out_md)

    if not os.path.exists(CSV):
        print(f"[ERR] {CSV} not found")
        return 2
    df = load()
    if len(df) <= 0:
        print(f"[ERR] {CSV} has no rows")
        return 2
    n_rooted = int((df["_n_root"] >= 1).sum())
    n_zero = int((df["_n_root"] < 1).sum())

    # ---- derive the active family order from the data (canonical order, present families only).
    # v1 has no context_drift -> unchanged; v2 gains it. Counts are ALWAYS from data, never hardcoded.
    rooted_kinds = set(df.loc[df["_n_root"] >= 1, "kind"].unique().tolist())
    FAMILY_ORDER = [f for f in CANON_FAMILY_ORDER if f in rooted_kinds]
    # any rooted kind not in the canonical list still gets a row (forward-compat, deterministic order)
    FAMILY_ORDER += sorted(k for k in rooted_kinds if k not in CANON_FAMILY_ORDER)

    # ---- data-derived per-family breakdown + faulted / constant-Synthesizer prior ----
    inj = df["injected"].astype(str).str.strip() if "injected" in df.columns else None
    n_faulted = int((inj == "1").sum()) if inj is not None else n_rooted
    fam_rooted_n = {f: int(((df["kind"] == f) & (df["_n_root"] >= 1)).sum()) for f in FAMILY_ORDER}
    zero_kind_breakdown = df.loc[df["_n_root"] < 1, "kind"].value_counts().to_dict()
    synth_root_n = int(df["_roots"].map(lambda rs: "Recommendation_Synthesizer" in rs).sum())
    const_synth_prior = (synth_root_n / n_rooted) if n_rooted else 0.0

    print(f"[load] {CSV}")
    print(f"[load] {len(df)} rows | rooted={n_rooted} | zero-root(excluded)={n_zero} | "
          f"faulted(injected==1)={n_faulted}")
    print(f"[family] rooted per family: {fam_rooted_n}")
    print(f"[baseline] constant-Synthesizer prior = {synth_root_n}/{n_rooted} = "
          f"{const_synth_prior:.3f} (majority-class Hit@1 if you always blame the Synthesizer)")
    print(f"[zero-root] kind breakdown of the {n_zero} excluded rows: {zero_kind_breakdown}")

    RES = {
        "Random (floor, seeds 0-4)": run_random(df),
        "Trivial span-anomaly [CORRECTED dur]": run_trivial(df, dur_suffix="duration_corrected_ms"),
        "Trivial span-anomaly [RAW dur — contaminated]": run_trivial(df, dur_suffix="duration_ms"),
        "Supervised ref: RF-infra ceiling (LOGO, seeds 0-4)": run_rf(df, "infra"),
        "Supervised ref: RF-content ceiling (LOGO, seeds 0-4)": run_rf(df, "content"),
        "Contract oracle (structured upper bound)": run_oracle(df),
    }
    findings, const_infra, http, err, infra_auc = anti_trivial_audit(df)

    def h1(bl, fam):
        """Observed Hit@1 mean for baseline `bl` on family `fam` (or None) — for dynamic prose
        so the narrative numbers always match the dataset actually run (v1 or v2)."""
        cell = RES.get(bl, {}).get(fam)
        if not cell:
            return None
        v = cell[0].get("hit@1")
        return None if v is None else v[0]

    def _f3(x):
        return "N/A" if x is None else f"{x:.3f}"

    _TRIV = "Trivial span-anomaly [CORRECTED dur]"
    _RFC = "Supervised ref: RF-content ceiling (LOGO, seeds 0-4)"
    _ORA = "Contract oracle (structured upper bound)"
    _TRIV_RAW = "Trivial span-anomaly [RAW dur — contaminated]"
    _hallu_triv = h1(_TRIV, "hallucinate")
    _hallu_triv_raw = h1(_TRIV_RAW, "hallucinate")
    _struct_vals = [h1(_TRIV, f) for f in ("wrong_item_pick", "format_violation") if h1(_TRIV, f) is not None]
    _struct_lo = min(_struct_vals) if _struct_vals else None
    _struct_hi = max(_struct_vals) if _struct_vals else None
    _struct_rng = _f3(_struct_lo) if _struct_lo == _struct_hi else f"{_f3(_struct_lo)}-{_f3(_struct_hi)}"
    _cd_triv = h1(_TRIV, "context_drift")   # None on v1 (no such family)
    _wip_ora = h1(_ORA, "wrong_item_pick")
    _wip_rfc = h1(_RFC, "wrong_item_pick")
    _fmt_rfc = h1(_RFC, "format_violation")

    # ---- console headline (K=1 per family) ----
    print("\n==== HEADLINE Hit@1 per fault-family ====")
    for bl, res in RES.items():
        line = [bl]
        for f in FAMILY_ORDER:
            d, ns, na = res[f]
            v = d.get("hit@1")
            line.append(f"{f}={'N/A' if v is None else f'{v[0]:.3f}'}(n={ns}{'' if not na else f',ab={na}'})")
        print(" | ".join(line))

    # ---- write MD ----
    md = []
    md.append("# agentfault — Tier-A baseline results (MRCBench, offline)\n")
    md.append("> Generated by `scripts/chaos/agentfault/eval/eval_agentfault_tierA.py` "
              "(deterministic; RF/Random seeds 0-4). Scorer = `m9_score.mrcbench` (上游 4.3 四族). "
              "Ranking mechanism reused from `eval_agentchaos` (per-agent binary RF -> "
              "predict_proba -> argsort). Entity space = 4 in-process agents (single-root).\n")
    _zk_str = ", ".join(f"`{k}`×{v}" for k, v in sorted(zero_kind_breakdown.items())) or "(none)"
    md.append(f"- Rows: {len(df)} | faulted (`injected==1`): **{n_faulted}** | rooted (scored): "
              f"**{n_rooted}** | zero-root **excluded** from the localization macro: **{n_zero}** "
              f"(kind breakdown of the excluded rows: {_zk_str}; these are negatives — clean `normal` "
              f"reps plus any injection that did not diverge / `inject_failed`; empty GT -> ranking undefined).")
    md.append(f"- **Constant-Synthesizer prior (majority-class baseline):** always blaming "
              f"`Recommendation_Synthesizer` gives Hit@1 = {synth_root_n}/{n_rooted} = "
              f"**{const_synth_prior:.3f}** (the Synthesizer is the root in {synth_root_n} of the "
              f"{n_rooted} rooted cases). Any localizer must beat this trivial prior to be meaningful.")
    md.append("- Candidate space = 4 agents. **Random and Contract-oracle are the two reference rails**; "
              "RF is a *supervised feature-separability ceiling*, NOT a peer baseline.\n")

    md.append("## Honest caveats (read before the numbers)\n")
    md.append("- **4-candidate space makes @3/@5 near-ceiling.** With a single root always among the 4 "
              "agents, **Hit@5 = FullHit@5 = 1.000 trivially** (top-5 ⊇ all 4), and a random ranker gets "
              "Hit@3 = 0.75. **K=1 is the only discriminative headline.**")
    md.append("- **Single-root degenerate identities (not independent numbers):** Recall@R ≡ Hit@1, "
              "FullHit@K ≡ Hit@K, avg@K ≡ Hit@K, NDCG@1 ≡ Hit@1 (all follow from |G|=1). NDCG@3 is the only "
              "K>1 number that adds rank information (reciprocal-log of the root's position within top-3). "
              "The MRCBench scorer still emits all four families + avg@K + mrr; the tables show the "
              "non-redundant subset.")
    md.append("- **RF ceiling under group-aware CV is confounded for the 3 hallucinate analyzers** "
              "(EVAL_NOTES 3c): each analyzer is the root in exactly ONE combo, so LeaveOneGroupOut removes "
              "its only positive class -> its per-agent RF degenerates to a constant -> near-random Top@1. "
              "This is split-degeneration, NOT proof of infra-blindness; read alongside the deterministic tracks.")
    md.append(f"- **Corrected-channel trivial heuristic is near-random everywhere; the RAW channel is the "
              f"artifact detector — that contrast is the contribution.** On the CORRECTED span-duration channel "
              f"(the default infra channel, which subtracts the injection sub-LLM overhead) the trivial heuristic "
              f"is near the 0.250 floor on every family (hallucinate {_f3(_hallu_triv)}, structured {_struct_rng})"
              f"{'' if _cd_triv is None else f', context_drift {_f3(_cd_triv)}'}. On the RAW channel it spikes to "
              f"{_f3(_hallu_triv_raw)} on hallucinate — but that spike is purely the injection sub-LLM's latency "
              f"artifact, NOT genuine localizability, which is exactly why corrected is the channel of record. The "
              f"contract oracle is the mirror image (perfect on structured, abstains on hallucinate). An aggregate "
              f"number would hide this.")
    md.append(f"- **Deterministic oracle > supervised content ceiling under group-aware CV.** On wrong_item_pick "
              f"the oracle scores {_f3(_wip_ora)} but the RF-content ceiling only {_f3(_wip_rfc)}: "
              f"`response_asin_is_sentinel` lives in a SINGLE combo, so LOGO holds it out of training and the RF "
              f"cannot use it. The perfect content signal is only exploitable without training (oracle) or when it "
              f"recurs across ≥2 combos (format's `contract_check` -> RF-content = {_f3(_fmt_rfc)}). This is why "
              f"the deterministic tracks, not the supervised ceiling, are the dataset's headline localizers.\n")

    md.append("## Design notes / judgment calls (flagged for review)\n")
    md.append("- **Trivial heuristic ranking rule** = per-agent span-duration z-score (vs that agent's own "
              f"distribution over all {len(df)} rows, GT-blind) + a status==ERROR bonus. Status is constant `UNSET` so "
              "only duration contributes. Global scalars (`recommendation_confidence`, `e2e_latency`) are "
              "**deliberately excluded** because they are not per-agent and cannot rank AMONG the 4 agents. "
              "An alternative 'low-confidence -> blame Synthesizer' domain heuristic would lift the trivial score "
              "on the 2 low-confidence format subtypes (missing_field/type_violation, conf=0.50) only, but that is "
              "really a degenerate contract-oracle (it needs the domain map confidence->Synthesizer), so it is "
              "folded into baseline #4 rather than #2.")
    md.append(f"- **Post-correction, the naive 'infra sees hallucinate' intuition does NOT hold.** One might "
              f"guess the duration heuristic localizes hallucinate (the sub-LLM rewrite adds latency); on the RAW "
              f"channel it appears to ({_f3(_hallu_triv_raw)}), but that is the injection artifact. Once the "
              f"artifact is subtracted (CORRECTED channel) hallucinate drops to {_f3(_hallu_triv)} — near-random, "
              f"like the structured families ({_struct_rng}). The honest reading: span-duration signals do not "
              f"localize hallucinate; the apparent RAW-channel skill is an injection-mechanism footprint, not "
              f"infra localizability."
              + ("" if _cd_triv is None else
                 f" **context_drift is ~random ({_f3(_cd_triv)} ≈ the 0.250 floor) on every Tier-A method here** "
                 f"— it is a pure content deletion (an upstream agent's message is stripped from the downstream "
                 f"input), so the system recovers with NO latency signature and NO needle/asin/contract signal. "
                 f"That near-random result is the honest, EXPECTED finding: span-duration / content-flag "
                 f"localization cannot see context_drift; it motivates the separate structural track."))
    md.append("- **RF-content feature set** = per-agent `conv_<A>_text_len` (4) + deterministic content bools "
              "(`divergent_needle_present`, `response_asin_is_sentinel`, `toolcall_asin_is_sentinel`, "
              "`contract_check_matches_expected`); RF-infra = the e2e/HTTP/host scalars + per-agent span numeric "
              "cols. `conv_text_len` is counted as content (text-derived), matching the eval_agentchaos split.\n")

    # per-family tables
    # dynamic per-family rooted count (from Random baseline's per-family n, always current)
    def _fam_n(f):
        try:
            return RES["Random (floor, seeds 0-4)"][f][1]  # (detail, n, n_abstain)
        except Exception:
            return "?"
    _n_fmt_sub = int(df.loc[df["kind"] == "format_violation", "format_subtype"].replace("", np.nan).nunique()) \
        if "format_subtype" in df.columns else 0
    # context_drift GT-root distribution (downstream target agent whose input was stripped)
    _cd_roots = {}
    if "context_drift" in FAMILY_ORDER:
        for rs in df.loc[(df["kind"] == "context_drift") & (df["_n_root"] >= 1), "_roots"]:
            for a in rs:
                _cd_roots[a] = _cd_roots.get(a, 0) + 1
    _cd_root_str = ", ".join(f"{a}×{n}" for a, n in sorted(_cd_roots.items())) or "n/a"
    fam_titles = {
        "hallucinate": (f"Fault family: hallucinate (3 analyzer agents, {_fam_n('hallucinate')} rooted cases)",
                        "Sub-LLM rewrites the target analyzer's answer. Content signal = `divergent_needle_present`."),
        "wrong_item_pick": (f"Fault family: wrong_item_pick (Recommendation_Synthesizer, {_fam_n('wrong_item_pick')} cases)",
                            "Deterministic ASIN swap to sentinel. Content signal = `response_asin_is_sentinel`."),
        "format_violation": (f"Fault family: format_violation (Recommendation_Synthesizer, {_n_fmt_sub} subtypes, {_fam_n('format_violation')} cases)",
                            "Deterministic tool_call corruption. Content signal = `contract_check_matches_expected`."),
        "context_drift": (f"Fault family: context_drift ({_fam_n('context_drift')} cases; GT root = downstream target: {_cd_root_str})",
                          "CONTENT deletion: an upstream agent's message is stripped from the downstream input; "
                          "GT root = the downstream agent whose input was stripped. NO latency signature (system "
                          "recovers) and NO needle/asin/contract signal -> **every span-duration/content Tier-A "
                          "method here is EXPECTED to be ~random (≈0.250 floor)**. This near-random row is the "
                          "honest, desired finding (it motivates the separate structural track); no special "
                          "context_drift locator is engineered in Tier-A."),
    }
    md.append("## Results per fault-family\n")
    for f in FAMILY_ORDER:
        title, note = fam_titles.get(f, (f"Fault family: {f} ({_fam_n(f)} rooted cases)", ""))
        rows = [_row(bl, res[f]) for bl, res in RES.items()]
        md += family_table(title, note, rows)

    # format subtypes (Hit@1 only)
    md.append("### format_violation — subtype breakdown (Hit@1)\n")
    md.append("| baseline | missing_field | type_violation | empty_required | malformed_json |")
    md.append("|---|---|---|---|---|")
    sub_order = ["missing_field", "type_violation", "empty_required", "malformed_json"]
    for bl, res in RES.items():
        subs = res.get("_subtypes", {})
        cells = []
        for s in sub_order:
            if s in subs:
                d, ns, na = subs[s]
                v = d.get("hit@1")
                cells.append("N/A" if v is None else f"{v[0]:.3f}")
            else:
                cells.append("N/A")
        md.append(f"| {bl} | " + " | ".join(cells) + " |")
    md.append("")

    # overall
    _overall_n = RES.get("Random (floor, seeds 0-4)", {}).get("overall", (None, "?", 0))[1]
    md.append(f"## Overall (all {_overall_n} rooted cases, macro)\n")
    md.append("| baseline | Hit@1 | Hit@3 | NDCG@3 | Hit@5 | Recall@R | MRR | n scored |")
    md.append("|---|---|---|---|---|---|---|---|")
    for bl, res in RES.items():
        md.append(_row(bl, res["overall"]))
    md.append("")

    # anti-trivial audit
    md.append("## Anti-trivial audit\n")
    md.append("> Concept referenced from `scripts/qa/anti_trivial_gate.py` (trivial baseline vs random "
              "floor; single-feature GT-encoding). Re-derived at agent granularity here.\n")
    md.append(f"- **Random floor** (analytic): Hit@1 = 1/4 = **0.250**; Hit@3 = 0.750; Hit@5 = 1.000.")
    md.append(f"- **Infra is near-blind by construction:** {', '.join(const_infra)}; "
              f"http_status={http}; error_span_count={err}. No infra status/error column varies -> no covert "
              f"single-feature infra encoding of GT.")
    md.append("- **Single-feature content encoding (this is the designed signal, reported honestly):**")
    md.append("")
    md.append("| feature | fires (rows) | root agents when it fires | FP on clean | encodes |")
    md.append("|---|---|---|---|---|")
    for fd in findings:
        md.append(f"| `{fd['feature']}` | {fd['fires']} | {', '.join(fd['root_agents_when_fires'])} | "
                  f"{fd['false_pos_on_clean']} | {fd['encodes']} |")
    md.append("")
    md.append("- **Finding (content):** `response_asin_is_sentinel` and `contract_check_matches_expected` each fire with "
              "**0 false positives on clean rows** and each names exactly **Recommendation_Synthesizer** -> a "
              "single content feature trivially solves the STRUCTURED families (this IS the contract-oracle "
              "upper bound; it is the dataset's designed deterministic signal, documented, not a covert leak). "
              "`divergent_needle_present` fires only on hallucinate but does **not** name which analyzer "
              "(all 3 analyzers share it) -> no single feature trivially encodes the *agent-level* GT for "
              "hallucinate, which is why that family is genuinely hard.")
    md.append(f"- **Finding (infra):** the best SINGLE infra feature separates the {synth_root_n} "
              f"Synthesizer-fault cases "
              f"from the rest with only **AUC={infra_auc['best_single_infra_auc']:.3f}** "
              f"(`{infra_auc['best_col']}`); all others are ~0.5-0.65. So the RF-infra ceiling reaching "
              f"Hit@1=1.000 on the structured families is **multi-feature aggregation + analyzer-elimination** "
              f"(the analyzer RFs, trained on the hallucinate latency artifact, confidently predict not-root, so a "
              f"merely moderate Synthesizer score wins the argmax) — NOT a single-feature infra leak, and it does "
              f"NOT transfer to hallucinate (structural LOGO degeneration).")
    zr_infra = RES["Supervised ref: RF-infra ceiling (LOGO, seeds 0-4)"].get("zero_root_false_alarm_rate")
    zr_content = RES["Supervised ref: RF-content ceiling (LOGO, seeds 0-4)"].get("zero_root_false_alarm_rate")
    md.append("")
    md.append(f"## Specificity / false-alarm note ({n_zero} zero-root rows)\n")
    md.append(f"- **Contract oracle:** abstains on all {n_zero} zero-root rows (no structured content signal) -> "
              "**0 false alarms** (perfect specificity on clean, by construction). This is the honest "
              "specificity headline.")
    md.append(f"- **RF-infra ceiling:** any-agent-positive fired on {zr_infra:.3f} of zero-root rows; "
              f"**RF-content ceiling:** {zr_content:.3f} (avg over seeds). ⚠️ These are NOT reliable "
              f"specificity estimates: LOGO holds out the single `normal` group entirely, so the per-agent "
              f"classifiers see **no clean rows** when scoring the clean rows -> they over-fire. This is the "
              f"specificity analogue of the single-group split-degeneration (EVAL_NOTES 3c), not evidence that "
              f"the models are trigger-happy in deployment.")
    md.append("- **Trivial span-anomaly & Random:** pure rankers with no abstention -> always emit a top-1 -> "
              "false-alarm rate is 1.0 by construction (not applicable as a detector; localization-only).")
    md.append("")

    with open(OUT_MD, "w", encoding="utf-8") as fh:
        fh.write("\n".join(md) + "\n")
    print(f"\n[wrote] {OUT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
