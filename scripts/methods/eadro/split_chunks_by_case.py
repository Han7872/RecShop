#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
split_chunks_by_case.py -- re-split a prebuilt chunks_all.pkl into train/test for a given seed.

Chunks are a stride-1 sliding window (length `chunk_lenth`) over ONE case, so neighbouring chunks
of the same case share 90% of their input. Splitting by CHUNK (upstream align.py:135's
np.random.shuffle) therefore puts a near-duplicate of every test chunk into train. We split by
CASE, stratified on the case's culprit.

`--mode chunk` reproduces upstream's leaky split ON THE SAME CORPUS, so the leak can be quantified
rather than asserted.

Usage:
    python split_chunks_by_case.py --chunks <chunks/RS> --out <chunks/RS_s1> --seed 1 \
                                   [--mode case|chunk] [--test-ratio 0.3]
"""
import argparse
import collections
import json
import os
import pickle

import numpy as np


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks", required=True, help="dir with chunks_all.pkl + metadata.json")
    ap.add_argument("--out", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--mode", default="case", choices=["case", "chunk", "config"])
    ap.add_argument("--test-ratio", type=float, default=0.3)
    args = ap.parse_args()

    with open(os.path.join(args.chunks, "chunks_all.pkl"), "rb") as fr:
        chunks = pickle.load(fr)
    md = json.load(open(os.path.join(args.chunks, "metadata.json"), encoding="utf-8"))

    # Trace tensor must be [node_num, chunk_lenth, 1]: deal_traces allocates a trailing dim of 2
    # but only writes channel 0 (single_process.py:90/106) and TraceModel is ConvNet(num_inputs=1).
    # align.py is patched (P6) to slice; a chunks_all.pkl built before the adapter carried the same
    # fix still has 2 channels, so repair it here rather than re-run the (slow) Hawkes preprocess.
    n_fixed = 0
    for v in chunks.values():
        if v["traces"].shape[-1] == 2:
            v["traces"] = v["traces"][:, :, 0:1]
            n_fixed += 1
    if n_fixed:
        print("[fix] sliced dead trace channel 1 on {} chunks -> shape {}".format(
            n_fixed, next(iter(chunks.values()))["traces"].shape))

    # A CASE is one rep. Every fault CONFIG was replayed 5x (r1..r5), so a case-level split still
    # puts r1..r3 of a config in train and r4/r5 of the SAME config in test: same fault, same
    # target service, same workload, different run. That is not window leakage (the runs are
    # independent), but it does mean the model never has to generalise to an UNSEEN fault config.
    # `--mode config` groups all reps of a config together, so a config is wholly in train or
    # wholly in test -- the strict "localise a fault you have never seen" test.
    import re
    cfg_of = lambda case: re.sub(r"_r\d+$", "", case)
    if args.mode == "config":
        for v in chunks.values():
            v["_grp"] = cfg_of(v["case"])
    else:
        for v in chunks.values():
            v["_grp"] = v["batch"]

    rng = np.random.RandomState(args.seed)
    if args.mode == "chunk":
        keys = list(chunks.keys())
        order = list(range(len(keys)))
        rng.shuffle(order)
        ntr = int((1 - args.test_ratio) * len(keys))
        tr_k = [keys[i] for i in order[:ntr]]
        te_k = [keys[i] for i in order[ntr:]]
    else:
        by_batch = collections.defaultdict(list)
        for k, v in chunks.items():
            by_batch[v["_grp"]].append(k)
        root_of = {}
        for b, ks in by_batch.items():
            labs = [chunks[k]["culprit"] for k in ks if chunks[k]["culprit"] != -1]
            root_of[b] = labs[0] if labs else -1
        strata = collections.defaultdict(list)
        for b in sorted(by_batch, key=str):
            strata[root_of[b]].append(b)
        te_b = []
        for r in sorted(strata):
            bs = list(strata[r])
            rng.shuffle(bs)
            n = int(round(args.test_ratio * len(bs)))
            if len(bs) >= 2:
                n = max(1, n)
            te_b += list(bs[:n])
        te_b = set(te_b)
        tr_k = [k for k, v in chunks.items() if v["_grp"] not in te_b]
        te_k = [k for k, v in chunks.items() if v["_grp"] in te_b]

    test_cases = sorted({chunks[k]["case"] for k in te_k})
    train_cases = sorted({chunks[k]["case"] for k in tr_k})
    overlap = sorted(set(test_cases) & set(train_cases))
    test_cfgs = sorted({cfg_of(c) for c in test_cases})
    train_cfgs = sorted({cfg_of(c) for c in train_cases})
    cfg_overlap = sorted(set(test_cfgs) & set(train_cfgs))

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "chunk_train.pkl"), "wb") as fw:
        pickle.dump({k: chunks[k] for k in tr_k}, fw)
    with open(os.path.join(args.out, "chunk_test.pkl"), "wb") as fw:
        pickle.dump({k: chunks[k] for k in te_k}, fw)
    md = dict(md)
    md.update({"chunk_num": len(chunks), "split_mode": args.mode, "split_seed": args.seed,
               "test_cases": test_cases, "n_train_cases": len(train_cases),
               "case_overlap_train_test": overlap, "test_configs": test_cfgs,
               "config_overlap_train_test": cfg_overlap})
    json.dump(md, open(os.path.join(args.out, "metadata.json"), "w", encoding="utf-8"), indent=2)

    n_faulty_te = sum(1 for k in te_k if chunks[k]["culprit"] != -1)
    print("[split seed={} mode={}] train={} test={} chunks | train_cases={} test_cases={} | "
          "faulty_test={}\n    CASE   overlap train/test = {} {}\n    CONFIG overlap train/test = {} "
          "({} test configs: {})".format(
              args.seed, args.mode, len(tr_k), len(te_k), len(train_cases), len(test_cases),
              n_faulty_te, len(overlap), "<-- LEAK" if overlap else "(none)",
              len(cfg_overlap),
              len(test_cfgs), ",".join(test_cfgs) if args.mode == "config" else "n/a"))


if __name__ == "__main__":
    main()
