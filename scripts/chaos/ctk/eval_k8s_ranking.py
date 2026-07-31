# -*- coding: utf-8 -*-
"""eval_k8s_ranking -- K8S pilot UNSUPERVISED per-service anomaly ranking baseline.

Port of eval_dualroot_v2_rca.py (RCAEval-style per-service robust-z ranking) adapted
to the WF-1 K8S feature view ((native trees) features_k8s[_all].csv).

==================================================================================
WHAT THIS BASELINE DOES (headline U):
  For each scored candidate service, score the row's anomaly by taking each of that
  service's own `svc_<svc>__<metric>_p95` columns, computing a CROSS-ROW robust z
  = (x - median) / (1.4826 * MAD) over the whole dataset (no dedicated normal window;
  median/MAD is the dataset-level normal-ish baseline -> biased conservative), clipping
  to the upper tail (clip>=0), and taking the per-row MAX across that service's metrics.
  Rank all candidates by score (ties broken by candidate name ascending, stable).

  >>> A service ranks high ONLY if ITS OWN telemetry shows a cross-row anomaly. There
  >>> is NO carrier-port prior, NO topology propagation, NO supervised label leakage.
  >>> root_cause_services / root_cause_primary / affected_services are used ONLY to
  >>> score the ranking AFTER it is produced -- NEVER enter the scoring (GROUND-TRUTH-
  >>> NOT-IN-X).

==================================================================================
HEADLINE FINDING (M4b Phase 4 update -- host/mysql now OBSERVABLE):
  Phase 3 added REAL scraped host/mysql series so the previously-off-graph shared-
  resource roots are now ON-graph and locatable by this same per-service robust-z ranker:
    - host_cpu roots   : svc_host__vm_cpu_saturation_ratio_p95 (4 strong 0.44-0.52 +
      2 short-stage weak ~0.02 vs ~0.008 corpus baseline -> robust-z upper-tail high)
    - db_lock roots    : svc_mysql__items_lock_granted_count_p95 (value=2 lock signal in
      the 6 db_lock rows vs observed 0 elsewhere -> robust-z upper-tail high)
  Prior to Phase 3 these were structural MISS (no series); now they SCORE. The finding
  EVOLVED: a pure per-service metric ranker CAN localize shared-resource roots ONCE they
  have a real observation series -- the previous MISS was an instrumentation gap, not a
  method ceiling. The remaining unobservable tier is the 20 uninstrumented BASE_SERVICES
  (no telemetry cols at all).

  Full on-graph definition space = 25 BASE_SERVICES (per_service_canon). The pilot
  instrumented 5 app services (catalog/catalog-gw/pricing/search/user) + 2 shared-
  resource series (host/mysql). The other 20 BASE_SERVICES have no telemetry columns,
  so they are structurally unobservable (robust-z 0 -> rank last). We fold them into
  the "unobservable" reporting tier rather than emitting 25 near-identical all-zero
  columns. catalog-gw is a K8S nginx gateway (separate deployment), not one of the 25
  RecWeb2 app BASE_SERVICES, but it IS telemetry-bearing and IS a realized root ->
  scored as an explicit on-graph candidate.

==================================================================================
METRICS (per carry-forward #6 + spec section 7):
  per-root Top@1 / Top@3 : for each true-root token in the row, did it land in the row's
                           top-1 / top-3? (rows with N roots contribute N judgements)
  Recall@root-set        : multi-root only. ALL true roots of the row appear in top-3.
                           (fixed TOPK=3, aligned with eval_dualroot_v2_rca.py:233 -- NOT
                           top-root_count)
  exact-set              : predicted top-(root_count) set == true-root set.
                           (None placeholders from all-zero scores never count as hits)

  STRATIFIED reporting:
    - by root tier: on-graph (catalog/catalog-gw/pricing/search/user) vs off-graph
      (host/mysql_items_lock) vs MIXED (a row whose root_set spans on+off, e.g. m3c2 =
      mysql_items_lock|catalog-gw). Mixed is reported SEPARATELY because the on-graph
      co-root can confound the off-graph miss.
    - by interaction_pattern: single_root / trigger_amplifier / fault_masking /
      independent_parallel.

==================================================================================
HONESTY (carry-forward #4/#6, written into results):
  - THIS SCRIPT scores per-root-TOKEN recall (U ranking); realized TOKEN domain = 5
    {catalog, catalog-gw, host, mysql_items_lock, user} (all appear as roots). NOTE:
    the S sibling (eval_k8s_supervised) root_cause_PRIMARY domain is DIFFERENT = 3
    real-only {catalog, host, mysql_items_lock} / 4 all (+catalog-gw); user is never
    primary (m3d catalog|user picks catalog by first-column tie-break). Do NOT conflate
    the two label spaces (auditU caught+corrected a v1 misstatement here).
  - off-graph roots: AS OF Phase 4, host/mysql_items_lock are ON-graph (real series) and
    SCORE -- the old "Top@k ~= 0 by construction (no series)" no longer applies to them.
    The remaining structural-miss tier is the 20 uninstrumented BASE_SERVICES (no cols).
    m3c2 (mysql_items_lock|catalog-gw) is now a PURE on_graph row (both roots observable)
    rather than a mixed on/off row; the "mixed" tier in stratified output reflects this
    (it is empty unless a row genuinely spans an on-graph root + an unobservable one).
  - m3d F1_only is telemetry-sparse (pod-kill: cAdvisor stops scraping while pods are
    down -> only 7/124 cols non-empty); its per-root Top@k carries that caveat.
  - N small (real=21 rows / 17 cases; all=39 rows). Per-tier numbers are trends.

==================================================================================
DETERMINISM: no randomness (pure scoring+ranking); ties broken by candidate name asc.
ASCII-only prints (Windows GBK). conda python, offline. Does NOT git commit.

USAGE (Windows):
    python3 scripts/chaos/ctk/eval_k8s_ranking.py
    # all-provenance secondary:
    ... eval_k8s_ranking.py --csv features_k8s_all.csv
"""
import argparse
import io
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---- reuse per_service_canon.BASE_SERVICES as the 25 on-graph definition space ----
HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
os.environ.setdefault("CHAOS_OUT_DIR", str(HERE))
from per_service_canon import BASE_SERVICES  # noqa: E402

# ---- paths ----
# ★ 2026-07-13: 数据根一律取自 dataset_registry(datasets/REGISTRY.json = 唯一真相源),
#   不再各脚本自己拼 "datasets"/"k8s_pilot"。--dataset-root/--pilot-dir 参数保留。
import dataset_registry as DR  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
# ★ 2026-07-13:曾有 PILOT_DIR = DR.NATIVE_ROOT 当 --pilot-dir 默认值(= 从 native 读派生 CSV)。
#   已删:特征 CSV 一律经 DR.feature_csv() 解析(默认 (runtime) features/)。
#   留着一个指向 native 的默认常量,迟早又被谁当成输出根用 —— 那正是 --help 打脏 native 的成因。

TOPK = 3
EPS = 1e-9

# ============================================================
# Candidate space (carry-forward #6, M4b Phase 4 update):
#   The pilot instrumented 5 per-service candidates with telemetry columns AND
#   (Phase 3) added REAL scraped host/mysql series so the off-graph roots are now
#   OBSERVABLE on-graph:
#     - host_cpu roots   -> svc_host__vm_cpu_saturation_ratio_p95 (4 strong 0.44-0.52
#       + 2 short-stage weak ~0.02; the rest of the corpus has a low ~0.008 baseline)
#     - db_lock roots    -> svc_mysql__items_lock_granted_count_p95 (value=2 lock signal
#       in 6 db_lock rows; real observed 0 elsewhere, NOT NaN/MNAR)
#   These are REAL scraped series (verified by make_k8s_feature_view.py assert(b)
#   semantic: svc_host__/svc_mysql__ need >=1 real non-NaN cell in real/reg rows;
#   sparse-but-real OK). They are NOT GT-derived -> safe to score as legitimate
#   telemetry (no leakage; this eval never reads root_cause_* for host/mysql scoring).
#
#   token <-> column-prefix mapping:
#     token "host"             <-> prefix "svc_host__"   (matches svc_{token}__ natively)
#     token "mysql_items_lock" <-> prefix "svc_mysql__"  (does NOT match svc_mysql_items_lock__;
#                                   the col is svc_mysql__items_lock_granted_count_p95).
#   We keep GT tokens unchanged (host / mysql_items_lock) and fix the mapping HERE
#   (eval-side only) via TOKEN_COLPREFIX. host maps to the default svc_{token}__ rule.
#
#   catalog-gw is a K8S nginx gateway (not a BASE_SERVICE) but telemetry-bearing and a
#   realized root -> scored as an explicit on-graph candidate. Candidate tokens are
#   KEPT IDENTICAL to the GT token vocabulary so ranking answers compare directly
#   (no fragile name bridge). The 20 uninstrumented BASE_SERVICES are folded into the
#   "unobservable" tier (reporting only).
# ============================================================
# The 5 per-service candidates that have feature columns in features_k8s.csv.
INSTRUMENTED = ["catalog", "catalog-gw", "pricing", "search", "user"]
# Shared-resource candidates with REAL scraped telemetry (Phase 3): now ON-graph.
# Empty list kept for API compatibility (selfcheck prints / tier reporting); there are
# currently NO structurally-unobservable off-graph roots in the realized token domain.
OFF_GRAPH = []

# token -> column prefix. host matches the default svc_{token}__ rule; mysql_items_lock
# does NOT (col is svc_mysql__...) so needs the explicit override. Add future off-graph /
# shared-resource roots here.
TOKEN_COLPREFIX = {
    "host": "svc_host__",             # native rule (svc_host__) -- explicit for clarity
    "mysql_items_lock": "svc_mysql__",  # OVERRIDE: col is svc_mysql__items_lock_..._p95
}


def _col_prefix(token: str) -> str:
    """Column prefix for a candidate token. TOKEN_COLPREFIX overrides the default
    svc_{token}__ rule (needed for mysql_items_lock -> svc_mysql__)."""
    if token in TOKEN_COLPREFIX:
        return TOKEN_COLPREFIX[token]
    return f"svc_{token}__"


# Realized root tokens across the pilot (real primary domain + secondary co-roots).
# All true-root tokens in the data are a subset of this; we score exactly these.
CANDIDATES = INSTRUMENTED + list(TOKEN_COLPREFIX.keys())  # 7 scored candidates
ON_GRAPH_CAND = set(CANDIDATES)        # all telemetry-bearing now (incl. host/mysql)
OFF_GRAPH_CAND = set(OFF_GRAPH)        # currently empty
CAND_IDX = {c: i for i, c in enumerate(CANDIDATES)}

# Tier classification for a root token (on-graph / off-graph / unknown).
def root_tier(token: str) -> str:
    if token in OFF_GRAPH_CAND:
        return "off_graph"
    if token in ON_GRAPH_CAND:
        return "on_graph"
    # any other token (e.g. an uninstrumented BASE_SERVICE) -> treated as off_graph
    # (no telemetry -> structurally unobservable, same miss behavior).
    return "off_graph"


def na(v) -> bool:
    s = str(v).strip()
    return s == "" or s.lower() == "nan"


def to_num(series) -> np.ndarray:
    """Empty/nan -> np.nan (preserve MNAR), else float."""
    arr = series.to_numpy() if hasattr(series, "to_numpy") else np.asarray(series)
    out = np.full(len(arr), np.nan, dtype=float)
    for i, v in enumerate(arr):
        if not na(v):
            try:
                out[i] = float(v)
            except ValueError:
                out[i] = np.nan
    return out


def robust_z_col(x: np.ndarray) -> np.ndarray:
    """Cross-row robust z = (x - median) / (1.4826 * MAD). All-NaN -> all 0.
    MAD=0 falls back to std, then to 1. NaN (MNAR) -> 0 (no anomaly evidence).
    Upper-tail only is applied by the caller via clip."""
    z = np.zeros(len(x))
    mask = ~np.isnan(x)
    if mask.sum() == 0:
        return z
    med = np.median(x[mask])
    mad = np.median(np.abs(x[mask] - med))
    scale = 1.4826 * mad
    if scale < EPS:
        sd = np.std(x[mask])
        scale = sd if sd > EPS else 1.0
    zc = (x - med) / scale
    zc[~mask] = 0.0
    return zc


def channel_max(zcols):
    """Per-row MAX across a list of robust-z columns, upper-tail clipped (>=0).
    Empty -> None (caller substitutes zeros)."""
    if not zcols:
        return None
    stacks = [np.clip(z, 0.0, None) for z in zcols]
    return np.max(np.vstack(stacks), axis=0)


def per_service_anomaly(df: pd.DataFrame) -> dict:
    """For each candidate, compute its own per-row anomaly = upper-tail MAX of
    cross-row robust-z over ALL its feature columns. Column prefix follows
    _col_prefix(token) so mysql_items_lock -> svc_mysql__* and host -> svc_host__*.
    A candidate with no columns -> all-zeros (still ranked, will MISS)."""
    anom = {}
    n = len(df)
    for svc in CANDIDATES:
        prefix = _col_prefix(svc)
        cols = [c for c in df.columns if c.startswith(prefix) and c.endswith("_p95")]
        zcols = [robust_z_col(to_num(df[c])) for c in cols]
        a = channel_max(zcols)
        anom[svc] = a if a is not None else np.zeros(n)
    return anom


def score_matrix(df: pd.DataFrame) -> np.ndarray:
    """Return score matrix [n_rows x n_candidates] (order == CANDIDATES).
    Each candidate scored from its own feature cols via _col_prefix(token).
    A candidate with no columns -> all-zero scores -> rank last -> Top@k MISS."""
    anom = per_service_anomaly(df)
    n = len(df)
    S = np.zeros((n, len(CANDIDATES)))
    for i, svc in enumerate(CANDIDATES):
        S[:, i] = anom[svc]
    return S


def rank_topk(score_row, k, credit_only_positive=True):
    """Rank by score DESC; ties broken by candidate name ASC (stable).
    Zero/negative-score candidates are filled with None placeholders so an all-zero
    ranking never manufactures fake recall (carry the v1/v2 honesty fix)."""
    order = sorted(range(len(CANDIDATES)), key=lambda i: (-score_row[i], CANDIDATES[i]))
    out = []
    for i in order[:k]:
        if credit_only_positive and score_row[i] <= 0:
            out.append(None)
        else:
            out.append(CANDIDATES[i])
    return out


# ============================================================
# Evaluation (generalized to N roots from root_cause_services split on '|')
# ============================================================
def parse_roots(val) -> list:
    """root_cause_services is pipe-delimited (e.g. 'mysql_items_lock|catalog-gw').
    Empty -> []. Preserves order (first = declared primary)."""
    s = "" if val is None else str(val).strip()
    if s == "":
        return []
    return [t for t in s.split("|") if t != ""]


def evaluate(df: pd.DataFrame):
    """Per-row ranking -> per-root Top@1/Top@3, Recall@root-set (multi-root), exact-set.
    Also accumulate per-tier (on/off/mixed) and per-interaction_pattern counts."""
    S = score_matrix(df)
    n = len(df)
    # per-root Top@1 / Top@3 accumulators keyed by root token
    root_top1 = {}
    root_top3 = {}
    # aggregate counters
    n_roots_judged = 0
    sum_top1 = 0
    sum_top3 = 0
    # multi-root set metrics
    n_multi = 0
    recall_set_hits = 0
    exact_set_hits = 0
    # tier-stratified per-root hit accumulators
    tier_top1 = {"on_graph": [0, 0], "off_graph": [0, 0], "mixed": [0, 0]}
    tier_top3 = {"on_graph": [0, 0], "off_graph": [0, 0], "mixed": [0, 0]}
    # interaction_pattern-stratified per-root hit accumulators
    pat_top1 = {}
    pat_top3 = {}
    # per-row records for debugging / stratified breakdown
    per_row = []

    for w in range(n):
        top1 = rank_topk(S[w], 1)
        top3 = rank_topk(S[w], TOPK)
        roots = parse_roots(df["root_cause_services"].iloc[w])
        n_root = len(roots)
        # classify the ROW's root set: mixed if spans on+off
        tiers = {root_tier(r) for r in roots}
        row_tier = "mixed" if ({"on_graph", "off_graph"}.issubset(tiers)) else (
            "off_graph" if tiers == {"off_graph"} else
            ("on_graph" if tiers == {"on_graph"} else "off_graph")
        )
        interaction = str(df["interaction_pattern"].iloc[w])
        is_multi = n_root >= 2

        row_top1_hits = 0
        row_top3_hits = 0
        for tr in roots:
            n_roots_judged += 1
            d1 = root_top1.setdefault(tr, [0, 0])
            d3 = root_top3.setdefault(tr, [0, 0])
            d1[1] += 1
            d3[1] += 1
            h1 = int(tr == top1[0])
            h3 = int(tr in top3)
            d1[0] += h1
            d3[0] += h3
            sum_top1 += h1
            sum_top3 += h3
            row_top1_hits += h1
            row_top3_hits += h3
            # tier accumulators
            t = root_tier(tr)
            tier_top1[t][0] += h1
            tier_top1[t][1] += 1
            tier_top3[t][0] += h3
            tier_top3[t][1] += 1
            # if the row is mixed, also tag the SAME root into the mixed bucket
            if row_tier == "mixed":
                tier_top1["mixed"][0] += h1
                tier_top1["mixed"][1] += 1
                tier_top3["mixed"][0] += h3
                tier_top3["mixed"][1] += 1
            # interaction accumulators
            p1 = pat_top1.setdefault(interaction, [0, 0])
            p3 = pat_top3.setdefault(interaction, [0, 0])
            p1[0] += h1
            p1[1] += 1
            p3[0] += h3
            p3[1] += 1

        # multi-root set metrics
        if is_multi:
            n_multi += 1
            top3_set = set(r for r in top3 if r is not None)
            true_set = set(roots)
            recall_set_hits += int(true_set.issubset(top3_set))
            # exact-set: predicted top-(root_count) set == true set
            topn = rank_topk(S[w], n_root)
            pred_set = set(r for r in topn if r is not None)
            exact_set_hits += int(pred_set == true_set)

        per_row.append({
            "case_id": df["case_id"].iloc[w],
            "window_id": df["window_id"].iloc[w],
            "group_id": df["group_id"].iloc[w],
            "roots": roots,
            "row_tier": row_tier,
            "interaction": interaction,
            "top1": top1[0],
            "top3": [r for r in top3],
            "n_root": n_root,
            "top1_hits": row_top1_hits,
            "top3_hits": row_top3_hits,
        })

    return {
        "n_rows": n,
        "n_roots_judged": n_roots_judged,
        "top1_rate": (sum_top1 / n_roots_judged) if n_roots_judged else 0.0,
        "top3_rate": (sum_top3 / n_roots_judged) if n_roots_judged else 0.0,
        "root_top1": root_top1,
        "root_top3": root_top3,
        "n_multi": n_multi,
        "recall_set": (recall_set_hits / n_multi) if n_multi else 0.0,
        "exact_set": (exact_set_hits / n_multi) if n_multi else 0.0,
        "tier_top1": tier_top1,
        "tier_top3": tier_top3,
        "pat_top1": pat_top1,
        "pat_top3": pat_top3,
        "per_row": per_row,
    }


# ============================================================
# Self-check (GROUND-TRUTH-NOT-IN-X, no off-graph cols, candidate coverage)
# ============================================================
def selfcheck(df: pd.DataFrame, label: str):
    print(f"[selfcheck:{label}] rows={df.shape[0]} candidates={len(CANDIDATES)} "
          f"(instrumented={len(INSTRUMENTED)} shared_resource={len(TOKEN_COLPREFIX)} "
          f"off_graph={len(OFF_GRAPH)})")
    # GT / labels must NEVER appear as scoring columns.
    GT = {"root_cause_services", "root_cause_primary", "root_cause_set", "fault_type",
          "fault_class", "fault_category", "composition_type", "interaction_pattern",
          "path_relation", "answer_type", "root_count", "affected_services",
          "group_id", "case_id", "window_id", "provenance", "system"}
    xcols = [c for c in df.columns if c.startswith("svc_") and c.endswith("_p95")]
    leaked = GT & set(xcols)
    assert not leaked, f"GROUND-TRUTH leaked into X scoring columns: {leaked}"
    print(f"[selfcheck:{label}] X cols={len(xcols)}; zero GT-in-X leak OK")
    # carrier-fingerprint *_isna must NOT be in X (K8S iron rule).
    isna_in_x = [c for c in xcols if c.endswith("_isna")]
    assert not isna_in_x, f"carrier-fingerprint *_isna cols forbidden in X: {isna_in_x}"
    print(f"[selfcheck:{label}] no *_isna carrier-fingerprint cols in X OK")
    # ALL scored candidates (instrumented + shared-resource host/mysql) must have at
    # least one feature column resolvable via their colprefix. (Phase 4: host/mysql are
    # now on-graph with REAL series; the old "off-graph must have NO cols" assertion is
    # STALE and would block them. We instead assert each candidate HAS cols.)
    candidates_missing_cols = []
    for svc in CANDIDATES:
        prefix = _col_prefix(svc)
        cols = [c for c in xcols if c.startswith(prefix)]
        if not cols:
            candidates_missing_cols.append((svc, prefix))
    assert not candidates_missing_cols, (
        f"scored candidate(s) missing feature cols: {candidates_missing_cols} "
        "(every on-graph candidate must resolve >=1 col via its token->prefix map)")
    print(f"[selfcheck:{label}] all {len(CANDIDATES)} candidates resolve feature cols via "
          f"token->prefix map OK (host->svc_host__, mysql_items_lock->svc_mysql__)")
    # host/mysql cols are REAL scraped series, NOT GT-derived: cross-check that the GT
    # root_cause_* columns are disjoint from the host/mysql col NAMES (a GT-derived col
    # would have to be named like a label to leak; the assert-disjoint above already
    # covers name leakage; this is a defensive note that scoring never reads root_cause_*
    # for host/mysql -- per_service_anomaly reads only svc_*__*_p95 cols).
    print(f"[selfcheck:{label}] host/mysql scored from REAL svc_*__*_p95 series only "
          f"(per_service_anomaly never reads root_cause_*); no GT leakage path")
    # GT-not-in-scoring-candidate-assertion (carry-forward #6: extend GT list).
    # True roots must be within our candidate vocabulary OR explicitly flagged off-graph.
    all_roots = set()
    for v in df["root_cause_services"]:
        all_roots |= set(parse_roots(v))
    unknown_roots = all_roots - set(CANDIDATES)
    if unknown_roots:
        print(f"[selfcheck:{label}] NOTE: true-root tokens outside scored candidate set: "
              f"{sorted(unknown_roots)} (will auto-miss; report as unobservable tier)")
    else:
        print(f"[selfcheck:{label}] all true-root tokens are within scored candidates OK")
    # assert the GT-not-in-scoring contract still holds (extend GT list per #6).
    scoring_set = set(xcols)
    for g in GT:
        assert g not in scoring_set, f"GROUND-TRUTH {g} would enter scoring"


def load_df(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    assert df.shape[0] > 0, f"empty csv {csv_path}"
    return df


# ============================================================
# Console + JSON-ish reporting
# ============================================================
def _rate(num, den):
    return (num / den) if den else float("nan")


def _fmt(x):
    return "  N/A " if (x != x) else f"{x:6.3f}"  # NaN check (x!=x)


def report(label: str, res: dict, fout):
    def P(*a):
        print(*a)
        if fout is not None:
            fout.write(" ".join(str(s) for s in a) + "\n")

    P(f"==================== {label} ====================")
    P(f"rows={res['n_rows']}  roots_judged={res['n_roots_judged']}  "
      f"multi_root_rows={res['n_multi']}")
    P("")
    P("=== Overall per-root ranking ===")
    P(f"  Top@1 = {_fmt(res['top1_rate'])}   Top@3 = {_fmt(res['top3_rate'])}")
    P(f"  Recall@root-set (multi-root, all roots in Top@3) = {_fmt(res['recall_set'])}   "
      f"[n_multi={res['n_multi']}]")
    P(f"  exact-set  (top-root_count set == true set)      = {_fmt(res['exact_set'])}   "
      f"[n_multi={res['n_multi']}]")
    P("")
    P("=== Per-root-token Recall@1 / Recall@3 ===")
    P(f"  {'root':<20} {'tier':<10} {'support':>7} {'R@1':>7} {'R@3':>7}")
    # stable order: CANDIDATES first, then any extra realized roots
    seen = set(res["root_top1"].keys()) | set(res["root_top3"].keys())
    ordered = [c for c in CANDIDATES if c in seen] + sorted(seen - set(CANDIDATES))
    for svc in ordered:
        t1 = res["root_top1"].get(svc, [0, 0])
        t3 = res["root_top3"].get(svc, [0, 0])
        sup = t1[1] or t3[1]
        P(f"  {svc:<20} {root_tier(svc):<10} {sup:>7} {_fmt(_rate(t1[0], t1[1])):>7} "
          f"{_fmt(_rate(t3[0], t3[1])):>7}")
    P("")
    P("=== Stratified by root TIER (per-root judgements) ===")
    P(f"  {'tier':<12} {'support':>7} {'R@1':>7} {'R@3':>7}")
    for tier in ["on_graph", "off_graph", "mixed"]:
        t1 = res["tier_top1"][tier]
        t3 = res["tier_top3"][tier]
        sup = t1[1] or t3[1]
        P(f"  {tier:<12} {sup:>7} {_fmt(_rate(t1[0], t1[1])):>7} {_fmt(_rate(t3[0], t3[1])):>7}")
    P("  (mixed = a row whose root_set spans an on-graph root AND an unobservable one;")
    P("   the on-graph co-root can confound the off-graph miss, so mixed is reported")
    P("   SEPARATELY. As of Phase 4 host/mysql are on-graph, so mixed is typically empty.)")
    P("  NOTE: mixed is an OVERLAP view -- its root-judgements are ALREADY counted in")
    P("   on_graph/off_graph above by each root's native tier. So on_graph+off_graph")
    P("   supports sum to roots_judged; mixed support is a subset of that sum, not extra.")
    P("")
    P("=== Stratified by interaction_pattern (per-root judgements) ===")
    P(f"  {'pattern':<22} {'support':>7} {'R@1':>7} {'R@3':>7}")
    # stable order: known patterns first then any extras
    pat_order = ["single_root", "trigger_amplifier", "fault_masking",
                 "independent_parallel"]
    pat_seen = set(res["pat_top1"].keys()) | set(res["pat_top3"].keys())
    for p in pat_order + sorted(pat_seen - set(pat_order)):
        t1 = res["pat_top1"].get(p, [0, 0])
        t3 = res["pat_top3"].get(p, [0, 0])
        sup = t1[1] or t3[1]
        P(f"  {p:<22} {sup:>7} {_fmt(_rate(t1[0], t1[1])):>7} {_fmt(_rate(t3[0], t3[1])):>7}")
    P("")
    P("=== Per-row ranking detail (top-1 | top-3 | true roots) ===")
    P(f"  {'case_id':<22} {'window':<8} {'tier':<10} {'n':>2} top1={'':<14} top3=true_roots")
    for r in res["per_row"]:
        top1 = r["top1"] if r["top1"] is not None else "(none)"
        top3 = ",".join((t if t is not None else "(none)") for t in r["top3"])
        roots = "|".join(r["roots"])
        P(f"  {r['case_id']:<22} {r['window_id']:<8} {r['row_tier']:<10} "
          f"{r['n_root']:>2} {top1:<18} {top3}  <= {roots}")
    P("")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="features_k8s.csv",
                    help="CSV filename to resolve (default: features_k8s.csv)")
    # ★ 2026-07-13:CSV 一律经 DR.feature_csv() 解析(默认 (runtime) features/),
    #   不再拼 native 路径。找不到 -> 报错并告诉你怎么生成,不静默往下跑。
    ap.add_argument("--features-dir", "--pilot-dir", dest="features_dir", default=None,
                    help="dir holding the feature CSVs "
                         "(default: (runtime) features; native k8s_pilot/ is read-only).")
    ap.add_argument("--also-all", action="store_true",
                    help="also run on features_k8s_all.csv (secondary) after the primary.")
    ap.add_argument("--out", default=None,
                    help="optional text dump path (ASCII); default: stdout only")
    args = ap.parse_args()

    primary = DR.feature_csv(args.csv, search_dir=args.features_dir)

    fout = None
    if args.out:
        DR.assert_not_native(args.out)
        fout = open(args.out, "w", encoding="ascii", errors="replace")

    df = load_df(primary)
    selfcheck(df, label=primary.name)
    res = evaluate(df)
    report(label=f"PRIMARY ({primary.name}, real-only)", res=res, fout=fout)

    if args.also_all:
        allcsv = DR.feature_csv("features_k8s_all.csv", required=False,
                                search_dir=args.features_dir)
        if allcsv is not None and allcsv != primary:
            df2 = load_df(allcsv)
            selfcheck(df2, label=allcsv.name)
            res2 = evaluate(df2)
            report(label=f"SECONDARY ({allcsv.name}, all-provenance)", res=res2, fout=fout)

    if fout is not None:
        fout.close()
        print(f"[written] text dump -> {fout.name}")


if __name__ == "__main__":
    # ASCII-only stdout on Windows GBK; non-ASCII -> '?'
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="ascii", errors="replace")
    main()
