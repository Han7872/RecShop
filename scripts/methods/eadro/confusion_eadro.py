#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
confusion_eadro.py -- top-1 confusion matrix over Eadro's per-chunk test predictions.

Purpose: test, rather than assert, WHERE the localizer's errors go. The trace channel is
callee-attributed (single_process.py:102), so a fault at catalog-gw inflates the observed latency
of catalog-gw AND of every ancestor that routes through it (pricing, checkout...). The prediction
this makes is that top-1 errors concentrate on the caller/callee neighbours of the true root, not
uniformly over the 13 nodes. This script checks that.

Rows = true root (culprit). Cols = predicted top-1. "MISS" = detector said 'normal' (rank == [-1]).

Usage:
    python confusion_eadro.py --results <result_dir> --adapter-report <adapter_report.json>
"""
import argparse
import collections
import glob
import json
import os


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True)
    ap.add_argument("--adapter-report", required=True)
    args = ap.parse_args()

    names = json.load(open(args.adapter_report, encoding="utf-8"))["node_names"]

    cm = collections.defaultdict(collections.Counter)
    for f in sorted(glob.glob(os.path.join(args.results, "*", "test_preds.json"))):
        for p in json.load(open(f, encoding="utf-8"))["preds"]:
            c = p["culprit"]
            if c == -1:
                continue
            pred = "MISS" if not p["detected"] else names[p["rank"][0]]
            cm[names[c]][pred] += 1

    cols = sorted({k for r in cm.values() for k in r}, key=lambda x: (x == "MISS", x))
    w = max(len(c) for c in cols + ["true \\ pred"]) + 2
    print("TOP-1 CONFUSION  (rows = true root, cols = predicted top-1; pooled over all runs)")
    print("{:<14}".format("true \\ pred") + "".join("{:>{w}}".format(c, w=w) for c in cols) + "{:>8}".format("n"))
    for r in sorted(cm, key=lambda x: -sum(cm[x].values())):
        tot = sum(cm[r].values())
        cells = []
        for c in cols:
            v = cm[r][c]
            cells.append("{:>{w}}".format("{} ({:.0f}%)".format(v, 100.0 * v / tot) if v else ".", w=w))
        print("{:<14}".format(r) + "".join(cells) + "{:>8}".format(tot))


if __name__ == "__main__":
    main()
