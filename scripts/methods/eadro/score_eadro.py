#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
score_eadro.py -- score Eadro's per-chunk prediction dumps (M6/M7) honestly.

Upstream prints ONE aggregate HR@k with a broken denominator (see M4) and no breakdown. This
recomputes everything from the raw per-chunk dump, so every number here is independently
reproducible from test_preds.json + adapter_report.json:

  * HR@k  with BOTH denominators:
      - HR@k        (upstream): hits / (TP+FN).  TP counts CORRECTLY-CLASSIFIED NORMAL chunks, so
                    this denominator is ~= the whole test set, not the faulty part -> deflated.
      - HRfix@k     (ours):     hits / (# faulty test chunks).   <-- the one to report.
    Both are recomputed from the dump; `scores` in the dump is the model's own report and is
    cross-checked against ours.
  * Baselines ON THE SAME FAULTY TEST CHUNKS, same denominator:
      - constant prior: rank nodes by their frequency among TRAIN faulty chunks (prior estimated
        on TRAIN only -- reading the test label distribution to build a "baseline" would itself
        be a leak), then HR@k = share of test faulty chunks whose culprit is in the top-k.
      - random: uniform ranking over the node set -> E[HR@k] = k / node_num.
  * per-root breakdown (the point of the exercise: catalog-gw is the new modal root).

A faulty chunk on which the DETECTOR says "normal" (rank == [-1], model.py:247) scores 0 at every
k. That is Eadro's own joint detect+localize semantics; we do not paper over it.

Usage:
    python score_eadro.py --results <result_dir> --adapter-report <adapter_report.json>
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import statistics


def hr_at_k(preds, k):
    """preds: list of dicts with culprit / rank. Returns (hits, n_faulty, TP+FN)."""
    hits = n_faulty = 0
    TP = FP = FN = 0
    for p in preds:
        c, det, rank = p["culprit"], p["detected"], p["rank"]
        if c == -1:
            if det:
                FP += 1
            else:
                TP += 1
            continue
        n_faulty += 1
        if not det:
            FN += 1          # detector missed it -> 0 hit at every k
            continue
        TP += 1
        if c in rank[:k]:
            hits += 1
    return hits, n_faulty, TP + FN


def score_run(dump, prior_rank, node_num):
    preds = dump["preds"]
    out = {"n_test_chunks": len(preds), "selected_epoch": dump.get("selected_epoch"),
           "split_seed": dump.get("split_seed"), "random_seed": dump.get("random_seed"),
           "select_on": dump.get("select_on"), "n_test_cases": len(dump.get("test_cases") or [])}
    for k in (1, 3, 5):
        hits, n_faulty, pos = hr_at_k(preds, k)
        out["HRfix@%d" % k] = hits / n_faulty if n_faulty else 0.0
        out["HR@%d(upstream_denom)" % k] = hits / pos if pos else 0.0
        out["_hits@%d" % k] = hits
    out["n_faulty"] = hr_at_k(preds, 1)[1]
    out["n_normal"] = out["n_test_chunks"] - out["n_faulty"]

    # ---- baselines on the SAME faulty test chunks --------------------------------------------
    faulty = [p["culprit"] for p in preds if p["culprit"] != -1]
    for k in (1, 3, 5):
        topk = set(prior_rank[:k])
        out["prior@%d" % k] = (sum(1 for c in faulty if c in topk) / len(faulty)) if faulty else 0.0
        out["random@%d" % k] = k / node_num
    return out


def per_root(preds, names):
    agg = collections.defaultdict(lambda: {"n": 0, "h1": 0, "h3": 0, "h5": 0, "undetected": 0})
    for p in preds:
        c = p["culprit"]
        if c == -1:
            continue
        a = agg[c]
        a["n"] += 1
        if not p["detected"]:
            a["undetected"] += 1
            continue
        for k in (1, 3, 5):
            if c in p["rank"][:k]:
                a["h%d" % k] += 1
    rows = []
    for c, a in sorted(agg.items(), key=lambda kv: -kv[1]["n"]):
        rows.append({"root": names[c], "nid": c, "n_chunks": a["n"],
                     "HR@1": a["h1"] / a["n"], "HR@3": a["h3"] / a["n"], "HR@5": a["h5"] / a["n"],
                     "undetected": a["undetected"]})
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True, help="dir containing <hash>/test_preds.json (globbed)")
    ap.add_argument("--adapter-report", required=True)
    args = ap.parse_args()

    rep = json.load(open(args.adapter_report, encoding="utf-8"))
    names = rep["node_names"]
    node_num = len(names)
    nid = {s: i for i, s in enumerate(names)}
    # per-case: first root nid + how many faulty chunks it contributes (labels come from roots[0];
    # align.py:32 `break`s at the first overlapping fault -> only roots[0] ever labels a chunk)
    case_info = {c["case"]: {"root0": (nid[c["roots"][0]] if c["roots"] and c["roots"][0] in nid else -1),
                             "faulty": c["faulty_chunks"], "roots": c["roots"],
                             "fault_type": c["fault_type"]}
                 for c in rep["cases"]}

    dumps = []
    for f in sorted(glob.glob(os.path.join(args.results, "*", "test_preds.json"))):
        dumps.append(json.load(open(f, encoding="utf-8")))
    if not dumps:
        raise SystemExit("no test_preds.json under " + args.results)

    runs, pooled = [], []
    for d in dumps:
        # constant prior estimated on TRAIN cases only (never on test)
        test_cases = set(d.get("test_cases") or [])
        train_counts = collections.Counter()
        for cname, ci in case_info.items():
            if cname in test_cases or ci["root0"] < 0:
                continue
            train_counts[ci["root0"]] += ci["faulty"]
        prior_rank = [c for c, _ in train_counts.most_common()]
        r = score_run(d, prior_rank, node_num)
        r["prior_top5"] = [names[c] for c in prior_rank[:5]]
        r["culprit_mismatch"] = d.get("culprit_mismatch")
        r["model_reported_HR@1"] = d.get("scores", {}).get("HR@1")
        r["model_reported_HRfix@1"] = d.get("scores", {}).get("HRfix@1")
        runs.append(r)
        pooled += d["preds"]

    def ms(key):
        v = [r[key] for r in runs]
        return (statistics.mean(v), statistics.stdev(v) if len(v) > 1 else 0.0)

    print("=" * 96)
    print("EADRO on RecShop -- {} run(s), split={}, selection={}".format(
        len(runs), dumps[0].get("split_mode"), dumps[0].get("select_on")))
    print("=" * 96)
    print("{:<28} {:>18} {:>18} {:>18}".format("", "k=1", "k=3", "k=5"))
    for label, keyf in [("Eadro HRfix@k (ours)", "HRfix@%d"),
                        ("Eadro HR@k (upstream denom)", "HR@%d(upstream_denom)"),
                        ("constant prior @k", "prior@%d"),
                        ("random ranking @k", "random@%d")]:
        cells = []
        for k in (1, 3, 5):
            m, s = ms(keyf % k)
            cells.append("{:.4f} +/- {:.4f}".format(m, s) if len(runs) > 1 else "{:.4f}".format(m))
        print("{:<28} {:>18} {:>18} {:>18}".format(label, *cells))
    print()
    print("test chunks/run: {:.0f} (faulty {:.0f}, normal {:.0f}) | test cases/run: {:.0f}".format(
        ms("n_test_chunks")[0], ms("n_faulty")[0], ms("n_normal")[0], ms("n_test_cases")[0]))
    print("selected epochs: {}".format([r["selected_epoch"] for r in runs]))
    print("culprit_mismatch (pred/order alignment check, must be 0): {}".format(
        [r["culprit_mismatch"] for r in runs]))
    print("train-prior node order (top5): {}".format(runs[0]["prior_top5"]))
    print()
    print("--- per-root breakdown (POOLED over {} run(s), chunk-level) ---".format(len(runs)))
    print("{:<14} {:>9} {:>9} {:>9} {:>9} {:>12}".format(
        "root", "n_chunks", "HR@1", "HR@3", "HR@5", "undetected"))
    for row in per_root(pooled, names):
        print("{:<14} {:>9} {:>9.4f} {:>9.4f} {:>9.4f} {:>12}".format(
            row["root"], row["n_chunks"], row["HR@1"], row["HR@3"], row["HR@5"], row["undetected"]))

    out = {"runs": runs, "per_root_pooled": per_root(pooled, names), "node_names": names}
    dst = os.path.join(args.results, "eadro_scores.json")
    json.dump(out, open(dst, "w", encoding="utf-8"), indent=2)
    print("\n-> {}".format(dst))


if __name__ == "__main__":
    main()
