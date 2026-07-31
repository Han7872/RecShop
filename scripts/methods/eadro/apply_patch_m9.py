#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
apply_patch_m9.py -- stage 2 patches on top of apply_patch.py (crash fixes).

apply_patch.py fixes the 11 places where official Eadro simply does not run.
These M-patches are the ones needed to run it on RecShop data AT ALL, plus the
one methodological fix without which any number it prints is a bubble:

  M1  util.Info is HARDCODED to TrainTicket / SocialNetwork (util.py:12-42) --
      service list, edges and the 7 cAdvisor metric names. RecShop is neither.
      -> Info() honours $EADRO_INFO_JSON (written by m9_eadro_adapter.py).

  M2  align.get_chunks() throws away which CASE a chunk came from (chunk ids are
      random 8-char strings). Needed for M3.  NB: inside get_chunks the batch id
      `idx` is shadowed by the loop variable `idx` -- capture it first.

  M3  *** LEAKAGE ***  align.py:135 does np.random.shuffle over CHUNKS, and the
      chunks are a stride-1 sliding window of length 10 -> adjacent chunks share
      90% of their input. A random chunk split puts near-duplicates of test
      chunks into train. Upstream's own numbers are therefore optimistic.
      -> default EADRO_SPLIT=case: split by CASE, stratified on the case's root.
         EADRO_SPLIT=chunk reproduces upstream's leaky split for contrast.

Usage:
    python apply_patch_m9.py --workdir <dir>     # runs apply_patch.py first
"""
import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

M_PATCHES = [
    (
        # *** M4: upstream's HR@k / ndcg@k DENOMINATOR IS WRONG. ***
        # base.py:37 increments TP when a NORMAL chunk (culprit==-1) is correctly predicted
        # normal. So `pos = TP+FN` (base.py:50) is NOT "the number of faulty chunks" -- it is
        # (correctly-classified normals) + (detected faults) + (missed faults) ~= the whole
        # test set. base.py:57-58 then divides the localization hit counts by that.
        # Effect on our data: 1323 test chunks of which only 453 are faulty -> every HR@k is
        # deflated by ~2.9x. We keep upstream's number (HR@k, so the run is comparable to what
        # their code prints) and ADD the corrected one (HRfix@k = hits / faulty chunks).
        # Verified independently: reloading the checkpoint and counting top-1 by hand
        # reproduces HRfix, not HR.
        "M4",
        "codes/base.py",
        """        pos = TP+FN
        eval_results = {""",
        """        pos = TP+FN
        pos_faulty = int(sum(1 for _t in _all_culprits if _t != -1))  # OUR M9 PATCH (M4)
        eval_results = {""",
    ),
    (
        "M4b",
        "codes/base.py",
        """        for j in [1, 3, 5]:  # OUR FIX: guard pos==0 (test split with no faulty chunk)
            eval_results["HR@"+str(j)] = hrs[j-1]*1.0/pos if pos > 0 else 0.0
            eval_results["ndcg@"+str(j)] = ndcgs[j-1]*1.0/pos if pos > 0 else 0.0""",
        """        for j in [1, 3, 5]:  # OUR FIX: guard pos==0 (test split with no faulty chunk)
            eval_results["HR@"+str(j)] = hrs[j-1]*1.0/pos if pos > 0 else 0.0
            eval_results["ndcg@"+str(j)] = ndcgs[j-1]*1.0/pos if pos > 0 else 0.0
            # OUR M9 PATCH (M4): correct denominator = number of FAULTY test chunks
            eval_results["HRfix@"+str(j)] = hrs[j-1]*1.0/pos_faulty if pos_faulty > 0 else 0.0
            eval_results["ndcgfix@"+str(j)] = ndcgs[j-1]*1.0/pos_faulty if pos_faulty > 0 else 0.0""",
    ),
    (
        "M4c",
        "codes/base.py",
        """        TP, FP, FN = 0, 0, 0
        batch_cnt, epoch_loss = 0, 0.0 """,
        """        TP, FP, FN = 0, 0, 0
        _all_culprits = []  # OUR M9 PATCH (M4)
        batch_cnt, epoch_loss = 0, 0.0 """,
    ),
    (
        "M4d",
        "codes/base.py",
        """                for idx, faulty_nodes in enumerate(res["y_pred"]):
                    culprit = ground_truths[idx].item()""",
        """                for idx, faulty_nodes in enumerate(res["y_pred"]):
                    culprit = ground_truths[idx].item()
                    _all_culprits.append(culprit)  # OUR M9 PATCH (M4)""",
    ),
    (
        "M1",
        "codes/preprocess/util.py",
        "class Info():\n    def __init__(self, bench='TrainTicket'):\n        \n        if bench.lower() == 'trainticket':",
        """class Info():
    def __init__(self, bench='TrainTicket'):
        # OUR M9 PATCH (M1): upstream hardcodes the whole system definition (services,
        # edges, the 7 cAdvisor metric names) for TrainTicket / SocialNetwork only.
        # RecShop is loaded from a JSON emitted by m9_eadro_adapter.py.
        _p = os.environ.get("EADRO_INFO_JSON")
        if _p:
            _i = json.loads(open(_p, encoding="utf-8").read())
            self.metric_names = _i["metric_names"]
            self.service_names = _i["service_names"]
            self.service2nid = {s: i for i, s in enumerate(self.service_names)}
            self.node_num = len(self.service_names)
            self.edges = _i["edges"]
            self.edge_info = {}
            self.metadata = {"node_num": self.node_num,
                             "metric_num": len(self.metric_names)}
            return

        if bench.lower() == 'trainticket':""",
    ),
    (
        "M2",
        "codes/preprocess/align.py",
        """    print("*** Aligning multi-source data...")
    chunks = defaultdict(dict)
    for idx in range(len(intervals)):""",
        """    print("*** Aligning multi-source data...")
    batch_idx = idx  # OUR M9 PATCH (M2): the batch id `idx` is shadowed by the loop var below
    chunks = defaultdict(dict)
    for idx in range(len(intervals)):""",
    ),
    (
        "M2b",
        "codes/preprocess/align.py",
        """        chunks[chunk_id]['culprit'] = labels[idx]

    return chunks""",
        """        chunks[chunk_id]['culprit'] = labels[idx]
        chunks[chunk_id]['batch'] = batch_idx  # OUR M9 PATCH (M2): needed for a case-wise split

    return chunks""",
    ),
    (
        "M3",
        "codes/preprocess/align.py",
        """    chunk_num = len(chunks)
    chunk_hashids = np.array(list(chunks.keys()))
    chunk_idx = list(range(chunk_num))

    train_num = int((1 - test_ratio) * chunk_num)
    test_num = int(test_ratio * chunk_num)
    np.random.shuffle(chunk_idx)

    train_idx = chunk_idx[:train_num]
    test_idx = chunk_idx[train_num:train_num+test_num]

    train_chunks = {k:chunks[k] for k in chunk_hashids[train_idx]}
    test_chunks = {k:chunks[k] for k in chunk_hashids[test_idx]}""",
        """    chunk_num = len(chunks)
    split_mode = os.environ.get("EADRO_SPLIT", "case")
    rng = np.random.RandomState(int(os.environ.get("EADRO_SPLIT_SEED", "42")))

    if split_mode == "chunk":
        # UPSTREAM behaviour, kept only for the leakage contrast: chunks are a stride-1
        # sliding window (90% overlap between neighbours), so shuffling CHUNKS leaks.
        chunk_hashids = np.array(list(chunks.keys()))
        chunk_idx = list(range(chunk_num))
        rng.shuffle(chunk_idx)
        train_num = int((1 - test_ratio) * chunk_num)
        test_num = int(test_ratio * chunk_num)
        train_keys = list(chunk_hashids[chunk_idx[:train_num]])
        test_keys = list(chunk_hashids[chunk_idx[train_num:train_num+test_num]])
    else:
        # OUR M9 PATCH (M3): split by CASE, stratified on the case's culprit node.
        by_batch = defaultdict(list)
        for k, v in chunks.items():
            by_batch[v['batch']].append(k)
        root_of = {}
        for b, ks in by_batch.items():
            labs = [chunks[k]['culprit'] for k in ks if chunks[k]['culprit'] != -1]
            root_of[b] = labs[0] if labs else -1
        strata = defaultdict(list)
        for b in sorted(by_batch):
            strata[root_of[b]].append(b)
        test_batches = []
        for r in sorted(strata):
            bs = list(strata[r])
            rng.shuffle(bs)
            n_te = int(round(test_ratio * len(bs)))
            if len(bs) >= 2:
                n_te = max(1, n_te)
            test_batches += list(bs[:n_te])
        test_batches = set(test_batches)
        train_keys = [k for k, v in chunks.items() if v['batch'] not in test_batches]
        test_keys = [k for k, v in chunks.items() if v['batch'] in test_batches]
        print("# split=case (no window leakage). test batches:", sorted(test_batches))

    train_chunks = {k: chunks[k] for k in train_keys}
    test_chunks = {k: chunks[k] for k in test_keys}
    train_num, test_num = len(train_chunks), len(test_chunks)""",
    ),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    args = ap.parse_args()

    r = subprocess.call([sys.executable, os.path.join(HERE, "apply_patch.py"),
                         "--workdir", args.workdir])
    if r != 0:
        sys.exit("apply_patch.py failed")

    for pid, rel, old, new in M_PATCHES:
        path = os.path.join(args.workdir, rel)
        src = open(path, encoding="utf-8").read()
        if src.count(old) != 1:
            sys.exit("FATAL {} {}: expected 1 match, found {}".format(pid, rel, src.count(old)))
        open(path, "w", encoding="utf-8").write(src.replace(old, new, 1))
        print("  [{}] {}".format(pid, rel))
    print("OK: {} M-patches applied on top.".format(len(M_PATCHES)))


if __name__ == "__main__":
    main()
