# -*- coding: utf-8 -*-
"""anti_trivial_gate.py -- reusable "anti-trivial" verification gate for RCA benchmarks.

WHY IT EXISTS
------------
A recurring failure mode in microservice RCA benchmarks (cf. "Rethinking Microservice
RCA Evaluation", arXiv:2510.04711) is that a *trivial* baseline (GT-frequency
constant-prior, or a naive z-score / resource-delta heuristic) beats published SOTA
methods (BARO / RCD / Eadro) on Hit@K. When that happens, the benchmark has
**zero discriminative headroom**: a reviewer can reject the result without reading
the method, because the task is gameable by memorising "always pick the modal root".

This gate localises that diagnosis at **fault-type x root** granularity. It answers:
  * WHERE does a trivial baseline win  (cells are *trivial-dominated* / gameable)?
  * WHERE do SOTA methods beat EVERY trivial baseline by a real margin
    (cells are *anti-trivial* = genuine discriminative headroom)?

It is REUSABLE: point it at any per_case_scores.csv (+ the dataset registry for
case->fault/root mapping) and it re-derives the matrix. Nothing about the 140-case
answer is hardcoded.

ANTI-TRIVIAL DEFINITION (the one knob that matters)
---------------------------------------------------
A cell (fault_type, root) is **anti-trivial** iff:

      best_SOTA(cell) >= trivial_b(cell) + MARGIN   for EVERY trivial baseline b
      AND  n_cases(cell) >= MIN_N

where
  best_SOTA   = max over {BARO/full, BARO/resource, RCD-mean/full, RCD-mean/resource}
  trivial set = {random(analytic), const-prior(GT-aware oracle),
                 delta_z(best of full/resource), delta_ratio(best of full/resource)}

i.e. to count as headroom, the best SOTA method must clear the STRONGEST trivial
baseline by MARGIN (default 0.10 Hit@1). Beating only the random floor is NOT enough
-- const-prior / delta_z are the real bar. Cells with n<MIN_N are flagged
``low_power`` (no statistical power) and excluded from the headroom count.

CASE -> FAULT_TYPE -> ROOT -> INTENSITY MAPPING
----------------------------------------------
case_id is the directory name shared by per_case_scores.csv and the native k8s_pilot
trees. We use scripts/chaos/ctk/dataset_registry.py (cases(tree_ids=DENSE_TREES)) to
get, per case, the list of *legs* from groundtruth.json[component_ground_truth];
each leg carries (fault_type, target_component=root, intensity). A multi-root case
is attributed to EACH of its legs' (fault_type, root) cells -- the same macro-root
convention used by the repo's existing BASELINES_TABLES.md (table 3). A single-root
(|G|=1) cross-check is also emitted to quantify the multi-root attribution ambiguity.

INTENSITY NOTE
--------------
Intensity is read from each leg's `intensity` dict, but in this dataset it is FIXED
per fault_type+scenario (5 reps share identical intensity; there is no controlled
intensity sweep). The intensity axis therefore degenerates and the matrix collapses
to fault_type x root. The gate reports this honestly rather than manufacturing a
third axis.

Usage
-----
    python scripts/qa/anti_trivial_gate.py \\
        --scores (project docs)/m11_140_avgmrr/per_case_scores.csv \\
        --baselines (project docs)/m11_140_avgmrr/_baselines.json \\
        [--eadro-config-scores (project docs)/eadro/scores_config.json] \\
        [--margin 0.10] [--min-n 5] [--out-dir (project docs)/anti_trivial]

Outputs: anti_trivial_matrix.csv, per_fault_type.csv, per_root.csv,
gate_verdict.json, and a printed summary.
"""
from __future__ import annotations
import argparse
import csv
import json
import math
import os
import statistics
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "chaos" / "ctk"))

DEFAULT_SCORES = REPO / "(project docs)/m11_140_avgmrr/per_case_scores.csv"
DEFAULT_BASELINES = REPO / "(project docs)/m11_140_avgmrr/_baselines.json"

# SOTA method column-families present in per_case_scores.csv
SOTA_METHODS = ["BARO", "RCD"]          # RCD spans RCD_seed0..4 (averaged per case)
TRIVIAL_METHODS = ["delta_z", "delta_ratio"]
FEATURE_SETS = ["full", "resource"]

DENSE_TREES_DEFAULT = "single_dense,dual_dense,triple_dense"


# ---------------------------------------------------------------- features ----

def load_scores(path: Path):
    """Return dict[(case_id, method_id, feature_set)] -> row(dict).

    The ORIGINAL method id is kept verbatim as the key -- RCD_seedN rows are NOT
    collapsed into a single 'RCD' key (that would silently overwrite seeds 0..N-1
    and keep only the last, corrupting the per-case RCD mean). per_case_method_hit1
    averages RCD seeds per case by prefix-matching 'RCD_seed'.
    """
    rows = list(csv.DictReader(open(path, encoding="utf-8")))
    table = {}
    for r in rows:
        table[(r["case_id"], r["method"], r["feature_set"])] = r
    return rows, table


def per_case_method_hit1(table, cases, method_key, feature_set):
    """case_id -> hit@1 float. RCD seeds averaged per-case first (prefix RCD_seed*)."""
    out = {}
    for c in cases:
        cid = c["case_id"]
        if method_key == "RCD":
            seed_hits = [_f(row["hit@1"])
                         for (ccid, mkey, fset), row in table.items()
                         if ccid == cid and fset == feature_set
                         and mkey.startswith("RCD_seed")]
            out[cid] = statistics.fmean(seed_hits) if seed_hits else 0.0
        else:
            row = table.get((cid, method_key, feature_set))
            out[cid] = _f(row["hit@1"]) if row else 0.0
    return out


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------- baselines ---

def analytic_baselines_per_case(cases, n_cands, fixed_ranking):
    """random (closed-form) + const-prior (GT-aware oracle) per case.

    random Hit@1   = |G| / N            (top-1 falls in GT set by chance)
    const_prior    = 1 if fixed_ranking[0] in GT set else 0
                     (oracle always ranks the modal root #1)
    """
    top1 = fixed_ranking[0] if fixed_ranking else None
    rnd, cp = {}, {}
    for c in cases:
        cid = c["case_id"]
        g = c["gt_set"]
        nG = max(len(g), 1)
        rnd[cid] = nG / float(n_cands)
        cp[cid] = 1.0 if (top1 is not None and top1 in g) else 0.0
    return rnd, cp


def const_prior_loo_per_case(cases, fixed_ranking):
    """Leave-one-out const-prior per case (fairer trivial bar than in-sample oracle).

    For each case we recompute the modal root from the GT-frequency tally with THAT
    case's roots removed, then const_prior_loo = 1 if the LOO top-1 is in the case's
    GT set. Tie-break = fixed_ranking order (the in-sample modal order), stable.
    On the current 140-case set catalog-gw dominates so LOO top-1 == in-sample top-1
    for every case (LOO == in-sample here); LOO only diverges after a GT rebalance,
    which is exactly when the fairer bar matters. Kept so a future re-run is sound.
    """
    # global frequency tally over all cases' GT sets
    tally = defaultdict(int)
    for c in cases:
        for root in c["gt_set"]:
            tally[root] += 1
    order = {root: i for i, root in enumerate(fixed_ranking)}
    loo = {}
    for c in cases:
        g = set(c["gt_set"])
        # decrement this case's roots
        loo_tally = dict(tally)
        for root in g:
            loo_tally[root] = loo_tally.get(root, 0) - 1
        # ranking: freq desc, tie-break by in-sample fixed_ranking order
        ranked = sorted(loo_tally, key=lambda r: (-loo_tally.get(r, 0),
                                                   order.get(r, 999)))
        top1 = ranked[0] if ranked else None
        loo[c["case_id"]] = 1.0 if (top1 is not None and top1 in g) else 0.0
    return loo


def paired_sign_test(sota_vec, triv_vec, case_ids):
    """Two-sided sign test on per-case hit@1 win/loss (ties dropped).

    Returns (p_value, n_pos, n_neg, n_tie). Guards against n=5 fluke wins: a cell
    may not be certified anti-trivial unless SOTA beats the trivial on a REAL per-case
    majority, not just an aggregate-margin fluke (a 4/5-vs-3/5 fluke = margin 0.20
    but sign-test p=0.1875, not significant).
    """
    n_pos = n_neg = n_tie = 0
    for cid in case_ids:
        s = sota_vec.get(cid, 0.0)
        t = triv_vec.get(cid, 0.0)
        if s > t + 1e-9:
            n_pos += 1
        elif t > s + 1e-9:
            n_neg += 1
        else:
            n_tie += 1
    n = n_pos + n_neg
    if n == 0:
        return 1.0, n_pos, n_neg, n_tie  # no discordant cases -> cannot be significant
    k = min(n_pos, n_neg)
    # two-sided exact binomial p = 2 * sum_{i<=k} C(n,i) * 0.5^n
    p = 0.0
    for i in range(k + 1):
        p += math.comb(n, i)
    p *= (0.5 ** n) * 2.0
    p = min(p, 1.0)
    return p, n_pos, n_neg, n_tie


# ---------------------------------------------------------------- cell logic -

def build_cells(cases):
    """cases -> {(fault_type, root): [case_id,...]} via leg attribution.

    A multi-root case is attributed to every leg's (fault_type, root) cell
    (macro-root convention; see module docstring)."""
    cells = defaultdict(list)
    for c in cases:
        for (ft, root) in c["legs"]:
            cells[(ft, root)].append(c["case_id"])
    return cells


def cell_hit1(cell_case_ids, per_case_vectors):
    """mean per-case hit@1 over the cell's attributed cases for every method/baseline.

    per_case_vectors: {key: {case_id: hit1}} where key names a method/baseline.
    """
    out = {}
    for key, vec in per_case_vectors.items():
        vals = [vec[cid] for cid in cell_case_ids if cid in vec]
        out[key] = statistics.fmean(vals) if vals else 0.0
    return out


# ---------------------------------------------------------------- eadro ------

def load_eadro_per_root(path):
    """scores_config.json[per_root_pooled] -> {root: HR@1} (chunk-pooled, val-epoch).

    Caveats: chunk-pooled HR != case-macro Hit@1; host excluded by Eadro's node set.
    Used only as an aggregate reference column, NEVER in the anti-trivial decision.
    """
    try:
        d = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out = {}
    for row in d.get("per_root_pooled", []):
        out[row.get("root")] = row.get("HR@1")
    return out


# ---------------------------------------------------------------- main -------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scores", default=str(DEFAULT_SCORES))
    ap.add_argument("--baselines", default=str(DEFAULT_BASELINES))
    ap.add_argument("--eadro-config-scores", default="(project docs)/eadro/scores_config.json")
    ap.add_argument("--trees", default=DENSE_TREES_DEFAULT,
                    help="comma list of dataset_registry tree ids")
    ap.add_argument("--margin", type=float, default=0.10,
                    help="anti-trivial Hit@1 margin over EVERY trivial baseline")
    ap.add_argument("--min-n", type=int, default=5,
                    help="min attributed cases for a cell to have statistical power")
    ap.add_argument("--alpha", type=float, default=0.05,
                    help="sign-test alpha: anti-trivial also requires SOTA to beat the "
                         "strongest trivial on a real per-case majority (p<alpha)")
    ap.add_argument("--n-margin-floor", type=float, default=2.0,
                    help="effective margin = max(margin, n_margin_floor/n) so the bar is "
                         "never sub-case-resolution at small n (one case = 1/n)")
    ap.add_argument("--trivial-bar", default="both",
                    choices=["insample", "loo", "both"],
                    help="const-prior trivial bar: in-sample oracle / LOO / both "
                         "(both is strictest; LOO is the fairer bar post-rebalance)")
    ap.add_argument("--pass-anti-trivial-cells", type=int, default=1,
                    help="gate PASSES iff at least this many anti-trivial cells exist")
    ap.add_argument("--out-dir", default="(project docs)/anti_trivial")
    args = ap.parse_args()

    scores_path = Path(args.scores)
    base_path = Path(args.baselines)
    if not scores_path.is_absolute():
        scores_path = REPO / scores_path
    if not base_path.is_absolute():
        base_path = REPO / base_path

    # ---- load baselines.json (fixed ranking + candidate count) ----
    bj = json.load(open(base_path, encoding="utf-8"))
    n_cands = int(bj.get("n_cands", 14))
    fixed_ranking = bj.get("fixed_ranking") or list(bj.get("gt_dist", {}).keys())
    print("[gate] candidate set N = %d ; const-prior #1 = %s"
          % (n_cands, fixed_ranking[0] if fixed_ranking else "?"))

    # ---- load case metadata via dataset_registry ----
    import dataset_registry as dr
    tree_ids = [t.strip() for t in args.trees.split(",") if t.strip()]
    reg_cases = dr.cases(tree_ids=tree_ids)
    print("[gate] registry cases (%s): %d" % (",".join(tree_ids), len(reg_cases)))

    # ---- load scores; reconcile case set ----
    rows, table = load_scores(scores_path)
    score_case_ids = {r["case_id"] for r in rows}
    # attach gt_set + legs to each registry case
    cases = []
    for c in reg_cases:
        if c["case_id"] not in score_case_ids:
            continue
        gt = json.load(open(Path(c["case_dir"]) / "groundtruth.json", encoding="utf-8"))
        roots = gt.get("root_cause_services") or []
        gt_set = sorted(set(roots))
        legs = sorted({(leg["fault_type"], leg["target_component"])
                       for leg in gt.get("component_ground_truth", [])})
        cases.append({
            "case_id": c["case_id"],
            "arity": c["arity"],
            "tree": c["tree"],
            "fault_types": [l["fault_type"] for l in gt.get("component_ground_truth", [])],
            "gt_set": gt_set,
            "n_distinct": len(gt_set),
            "legs": legs,
        })
    print("[gate] cases joined with scores: %d" % len(cases))
    if not cases:
        sys.exit("[gate] FATAL: no cases overlap between scores and registry trees.")

    # ---- per-case hit@1 vectors ----
    vectors = {}
    for m in SOTA_METHODS:
        for fs in FEATURE_SETS:
            vectors["%s/%s" % (m, fs)] = per_case_method_hit1(table, cases, m, fs)
    for m in TRIVIAL_METHODS:
        for fs in FEATURE_SETS:
            vectors["%s/%s" % (m, fs)] = per_case_method_hit1(table, cases, m, fs)

    rnd_vec, cp_vec = analytic_baselines_per_case(cases, n_cands, fixed_ranking)
    vectors["random"] = rnd_vec
    vectors["const_prior"] = cp_vec
    vectors["const_prior_loo"] = const_prior_loo_per_case(cases, fixed_ranking)

    # RCD per-case = seed-averaged already; expose a single RCD/full, RCD/resource.
    # ---- define the SOTA / trivial "best" reducers per cell ----
    SOTA_KEYS = ["BARO/full", "BARO/resource", "RCD/full", "RCD/resource"]
    TRIVIAL_REDUCERS = {
        "random":      ["random"],
        "delta_z":     ["delta_z/full", "delta_z/resource"],
        "delta_ratio": ["delta_ratio/full", "delta_ratio/resource"],
    }
    if args.trivial_bar in ("insample", "both"):
        TRIVIAL_REDUCERS["const_prior"] = ["const_prior"]
    if args.trivial_bar in ("loo", "both"):
        TRIVIAL_REDUCERS["const_prior_loo"] = ["const_prior_loo"]

    # ---- build cells ----
    cells = build_cells(cases)
    # also single-root-only cells (clean attribution, no multi-root ambiguity)
    single_root_cases = [c for c in cases if c["n_distinct"] == 1]
    cells_sr = build_cells(single_root_cases)

    eadro_root = load_eadro_per_root(REPO / args.eadro_config_scores) if args.eadro_config_scores else {}

    # ---- per-cell table ----
    matrix_rows = []
    for (ft, root) in sorted(cells):
        cids = cells[(ft, root)]
        n = len(cids)
        h = cell_hit1(cids, vectors)
        best_sota = max(h[k] for k in SOTA_KEYS)
        best_sota_name = max(SOTA_KEYS, key=lambda k: h[k])

        trivial_best = {}
        trivial_best_key = {}
        for tname, keys in TRIVIAL_REDUCERS.items():
            present = [k for k in keys if k in h]
            trivial_best[tname] = max((h[k] for k in present), default=0.0)
            trivial_best_key[tname] = (max(present, key=lambda k: h[k])
                                       if present else (keys[0] if keys else None))

        # margin over EACH trivial baseline (anti-trivial must clear all)
        margins = {tname: best_sota - val for tname, val in trivial_best.items()}
        min_margin = min(margins.values()) if margins else -1.0
        strongest_trivial = max(trivial_best, key=trivial_best.get) if trivial_best else "?"
        strongest_trivial_val = trivial_best.get(strongest_trivial, 0.0)

        # n for single-root attribution (cross-check power)
        n_sr = len(cells_sr.get((ft, root), []))

        # effective margin is n-aware so the bar is never sub-case-resolution
        eff_margin = max(args.margin, args.n_margin_floor / n) if n > 0 else args.margin

        # ceiling-tie: best SOTA AND the strongest trivial both sit at the 1.0 ceiling
        # (everyone who can solve the cell does; trivially separable, NOT headroom).
        # The analytic `random` floor is excluded -- it is a chance lower bound, not a solver.
        is_ceiling = (best_sota >= 1.0 - 1e-9 and strongest_trivial_val >= 1.0 - 1e-9)

        # statistical guard: SOTA must beat the STRONGEST trivial on a real per-case
        # majority (paired sign test), not just an aggregate-margin fluke.
        sota_vec = vectors.get(best_sota_name, {})
        triv_vec = vectors.get(trivial_best_key.get(strongest_trivial) or "", {})
        sign_p, npos, nneg, ntie = paired_sign_test(sota_vec, triv_vec, cids)

        if n < args.min_n:
            verdict = "low_power"
        elif is_ceiling:
            verdict = "ceiling_tie"
        elif (min_margin >= args.margin
              and best_sota >= strongest_trivial_val + eff_margin
              and sign_p < args.alpha):
            verdict = "anti_trivial"
        else:
            verdict = "trivial_dominated"

        matrix_rows.append({
            "fault_type": ft, "root": root, "n_cases": n, "n_single_root": n_sr,
            "best_SOTA": round(best_sota, 4), "best_SOTA_method": best_sota_name,
            "random": round(h.get("random", 0.0), 4),
            "const_prior": round(h.get("const_prior", 0.0), 4),
            "const_prior_loo": round(h.get("const_prior_loo", 0.0), 4),
            "delta_z_best": round(trivial_best.get("delta_z", 0.0), 4),
            "delta_ratio_best": round(trivial_best.get("delta_ratio", 0.0), 4),
            "strongest_trivial": strongest_trivial,
            "strongest_trivial_val": round(strongest_trivial_val, 4),
            "min_margin_over_all_trivial": round(min_margin, 4),
            "eff_margin": round(eff_margin, 4),
            "sign_p_vs_strongest": round(sign_p, 4),
            "sign_pos_neg_tie": "%d/%d/%d" % (npos, nneg, ntie),
            "BARO/full": round(h["BARO/full"], 4), "BARO/resource": round(h["BARO/resource"], 4),
            "RCD/full": round(h["RCD/full"], 4), "RCD/resource": round(h["RCD/resource"], 4),
            "delta_z/full": round(h["delta_z/full"], 4), "delta_z/resource": round(h["delta_z/resource"], 4),
            "delta_ratio/full": round(h["delta_ratio/full"], 4), "delta_ratio/resource": round(h["delta_ratio/resource"], 4),
            "eadro_HR1_chunkpooled_ref": (round(eadro_root[root], 4) if root in eadro_root else ""),
            "verdict": verdict,
        })

    # ---- per-fault-type table (aggregate across the fault_type's roots) ----
    ft_rows = _aggregate(cases, vectors, SOTA_KEYS, TRIVIAL_REDUCERS, key="fault_type")
    # ---- per-root table (cross-check vs BASELINES table 3) ----
    root_rows = _aggregate(cases, vectors, SOTA_KEYS, TRIVIAL_REDUCERS, key="root",
                           extra=lambda root: {"eadro_HR1_ref": (round(eadro_root[root],4)
                                              if root in eadro_root else "")})

    # ---- gate verdict ----
    n_cells = len(matrix_rows)
    n_anti = sum(1 for r in matrix_rows if r["verdict"] == "anti_trivial")
    n_triv = sum(1 for r in matrix_rows if r["verdict"] == "trivial_dominated")
    n_low = sum(1 for r in matrix_rows if r["verdict"] == "low_power")
    n_ceil = sum(1 for r in matrix_rows if r["verdict"] == "ceiling_tie")
    headroom = sorted((r["min_margin_over_all_trivial"] for r in matrix_rows
                       if r["verdict"] not in ("low_power", "ceiling_tie")), reverse=True)
    headroom_dist = {
        "max": headroom[0] if headroom else None,
        "p50": headroom[len(headroom)//2] if headroom else None,
        "min": headroom[-1] if headroom else None,
        "n_positive_margin": sum(1 for m in headroom if m > 0),
        "n_ge_margin": sum(1 for m in headroom if m >= args.margin),
    }
    # fault types that carry ANY anti-trivial cell
    ft_with_headroom = sorted({r["fault_type"] for r in matrix_rows
                               if r["verdict"] == "anti_trivial"})
    # where trivial delta_z (best) outright beats best SOTA (gameable)
    delta_z_beats_sota = [r for r in matrix_rows
                          if r["delta_z_best"] > r["best_SOTA"] + 1e-9]
    delta_ratio_beats_sota = [r for r in matrix_rows
                              if r["delta_ratio_best"] > r["best_SOTA"] + 1e-9]

    verdict = {
        "config": {
            "scores": str(scores_path), "trees": tree_ids,
            "n_cases": len(cases), "n_cands": n_cands,
            "margin": args.margin, "min_n": args.min_n,
            "alpha": args.alpha, "n_margin_floor": args.n_margin_floor,
            "trivial_bar": args.trivial_bar,
            "const_prior_top1": fixed_ranking[0] if fixed_ranking else None,
        },
        "n_cells": n_cells,
        "n_anti_trivial": n_anti,
        "n_trivial_dominated": n_triv,
        "n_low_power": n_low,
        "n_ceiling_tie": n_ceil,
        "headroom_distribution(min_margin_over_all_trivial)": headroom_dist,
        "fault_types_with_headroom": ft_with_headroom,
        "delta_z_outright_beats_best_SOTA": [
            "%s@%s (dz=%.3f > sota=%.3f)" % (r["fault_type"], r["root"],
                                             r["delta_z_best"], r["best_SOTA"])
            for r in delta_z_beats_sota],
        "delta_ratio_outright_beats_best_SOTA": [
            "%s@%s (dr=%.3f > sota=%.3f)" % (r["fault_type"], r["root"],
                                             r["delta_ratio_best"], r["best_SOTA"])
            for r in delta_ratio_beats_sota],
        "gate_pass": n_anti >= args.pass_anti_trivial_cells,
        "pass_threshold_anti_trivial_cells": args.pass_anti_trivial_cells,
        "note": ("anti_trivial = best SOTA clears EVERY trivial baseline "
                 "(random, const-prior%s, delta_z, delta_ratio) by >= %.2f (and "
                 "n-aware floor %.1f/n) AND beats the strongest trivial on a real "
                 "per-case majority (sign-test p<%.2f) AND n>=%d. "
                 "ceiling_tie = everyone=1.0 (trivially separable, not headroom). "
                 "Eadro is chunk-pooled HR (excludes host) and is reference-only, "
                 "not in the decision." % ("/LOO" if args.trivial_bar == "both" else "",
                                           args.margin, args.n_margin_floor,
                                           args.alpha, args.min_n)),
    }

    # ---- write outputs ----
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = REPO / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "anti_trivial_matrix.csv", matrix_rows)
    _write_csv(out_dir / "per_fault_type.csv", ft_rows)
    _write_csv(out_dir / "per_root.csv", root_rows)
    json.dump(verdict, open(out_dir / "gate_verdict.json", "w", encoding="utf-8"),
              indent=2, ensure_ascii=False)

    # ---- print summary ----
    print("\n================ ANTI-TRIVIAL GATE VERDICT ================")
    print("cells: %d total | anti_trivial=%d  trivial_dominated=%d  ceiling_tie=%d  low_power(< %d cases)=%d"
          % (n_cells, n_anti, n_triv, n_ceil, args.min_n, n_low))
    print("headroom (best_SOTA - strongest_trivial, per cell; excludes ceiling_tie/low_power):")
    hd = verdict["headroom_distribution(min_margin_over_all_trivial)"]
    print("   max=%.3f  p50=%.3f  min=%.3f | cells with margin>0: %d | >=margin(%.2f): %d"
          % (hd["max"] or 0, hd["p50"] or 0, hd["min"] or 0,
             hd["n_positive_margin"], args.margin, hd["n_ge_margin"]))
    print("fault types with ANY anti-trivial cell: %s"
          % (ft_with_headroom if ft_with_headroom else "(none)"))
    print("cells where delta_z outright BEATS best SOTA: %d"
          % len(delta_z_beats_sota))
    for r in delta_z_beats_sota:
        print("   - %s@%s  dz=%.3f > sota=%.3f (%s)"
              % (r["fault_type"], r["root"], r["delta_z_best"], r["best_SOTA"],
                 r["best_SOTA_method"]))
    print("GATE %s (threshold: >= %d anti-trivial cell(s))"
          % ("PASS" if verdict["gate_pass"] else "FAIL", args.pass_anti_trivial_cells))
    print("outputs -> %s" % out_dir)
    print("============================================================")
    return verdict


def _aggregate(cases, vectors, SOTA_KEYS, TRIVIAL_REDUCERS, key, extra=None):
    """Aggregate Hit@1 by fault_type or by root (macro over attributed cases)."""
    # build membership: key_value -> [case_id] with macro attribution
    mem = defaultdict(list)
    for c in cases:
        if key == "fault_type":
            keys = sorted({ft for (ft, _r) in c["legs"]})
        else:  # root
            keys = c["gt_set"]
        for k in keys:
            mem[k].append(c["case_id"])
    out = []
    for k in sorted(mem):
        cids = mem[k]
        h = cell_hit1(cids, vectors)
        best_sota = max(h[sk] for sk in SOTA_KEYS)
        best_sota_name = max(SOTA_KEYS, key=lambda sk: h[sk])
        triv = {t: max((h[ck] for ck in cks), default=0.0)
                for t, cks in TRIVIAL_REDUCERS.items()}
        row = {
            key: k, "n_cases": len(cids),
            "best_SOTA": round(best_sota, 4), "best_SOTA_method": best_sota_name,
            "random": round(h["random"], 4),
            "const_prior": round(h["const_prior"], 4),
            "delta_z_best": round(triv["delta_z"], 4),
            "delta_ratio_best": round(triv["delta_ratio"], 4),
            "BARO/full": round(h["BARO/full"], 4), "BARO/resource": round(h["BARO/resource"], 4),
            "RCD/full": round(h["RCD/full"], 4), "RCD/resource": round(h["RCD/resource"], 4),
            "delta_z/full": round(h["delta_z/full"], 4), "delta_z/resource": round(h["delta_z/resource"], 4),
            "delta_ratio/full": round(h["delta_ratio/full"], 4), "delta_ratio/resource": round(h["delta_ratio/resource"], 4),
        }
        if extra:
            row.update(extra(k))
        out.append(row)
    return out


def _write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
