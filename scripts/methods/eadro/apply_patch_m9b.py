#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
apply_patch_m9b.py -- stage 3 patches, on top of apply_patch_m9.py.

apply_patch.py   = the 12 places official Eadro simply crashes.
apply_patch_m9.py = M1 (Info from JSON) / M2 (chunk->case provenance) / M3 (case-wise split,
                    kills the 90%-overlap window leak) / M4 (HR denominator).

This file adds the three that are needed to make the reported number HONEST rather than merely
runnable:

  M5  *** TEST-SET MODEL SELECTION ***  base.py:103 does
          if test_results["HR@1"] > best_hr1: best_hr1, eval_res = ..., test_results
      i.e. it evaluates on TEST every `evaluation_epoch`, keeps the BEST-SCORING epoch, and
      reports that. The test set is thus used to pick the checkpoint, and the reported HR@k is a
      max over 5 evaluations -- an optimistic, non-held-out number.
      -> carve a VALIDATION split out of TRAIN (by CASE, never by chunk) and select the epoch on
         VAL HRfix@1. TEST is then only ever read at the epoch VAL already chose.
         EADRO_SELECT=test restores upstream's behaviour so both can be reported.

  M6  per-chunk prediction dump. Needed for a per-root breakdown (upstream only ever prints one
      aggregate number). evaluate() records (culprit, top-5 ranked nodes) for every test chunk, in
      DataLoader order; test_dl has shuffle=False, so this aligns 1:1 with the key order of
      chunk_test.pkl, which carries `case`/`batch` (M2). -> we can attribute every prediction back
      to the case and root that produced it.

  M7  main.py: build the val loader (by case), pass it to fit(), dump predictions + the resolved
      split to result_dir.

Usage:
    python apply_patch_m9b.py --workdir <dir>     # runs apply_patch_m9.py (hence apply_patch.py) first
"""
import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

PATCHES = [
    # ---------------------------------------------------------------- M6: per-chunk pred dump
    (
        "M6",
        "codes/base.py",
        """        _all_culprits = []  # OUR M9 PATCH (M4)
        batch_cnt, epoch_loss = 0, 0.0 """,
        """        _all_culprits = []  # OUR M9 PATCH (M4)
        _preds = []  # OUR M9 PATCH (M6): per-chunk (culprit, ranked nodes) for a per-root breakdown
        batch_cnt, epoch_loss = 0, 0.0 """,
    ),
    (
        "M6b",
        "codes/base.py",
        """                    culprit = ground_truths[idx].item()
                    _all_culprits.append(culprit)  # OUR M9 PATCH (M4)""",
        """                    culprit = ground_truths[idx].item()
                    _all_culprits.append(culprit)  # OUR M9 PATCH (M4)
                    # OUR M9 PATCH (M6). faulty_nodes == [-1] when the DETECTOR called the chunk
                    # normal (model.py:247); otherwise it is the full node ranking, best first.
                    _preds.append({"culprit": int(culprit),
                                   "detected": int(faulty_nodes[0] != -1),
                                   "rank": [int(x) for x in list(faulty_nodes)[:5]]})""",
    ),
    (
        "M6c",
        "codes/base.py",
        """        logging.info("{} -- {}".format(datatype, ", ".join([k+": "+str(f"{v:.4f}") for k, v in eval_results.items()])))

        return eval_results""",
        """        logging.info("{} -- {}".format(datatype, ", ".join([k+": "+str(f"{v:.4f}") for k, v in eval_results.items()])))

        self.last_preds = _preds  # OUR M9 PATCH (M6)
        return eval_results""",
    ),
    # ---------------------------------------------------- M5: selection on VAL, not on TEST
    (
        "M5",
        "codes/base.py",
        """    def fit(self, train_loader, test_loader=None, evaluation_epoch=10):
        best_hr1, coverage, best_state, eval_res = -1, None, None, None # evaluation""",
        """    def fit(self, train_loader, test_loader=None, evaluation_epoch=10, val_loader=None):
        best_hr1, coverage, best_state, eval_res = -1, None, None, None # evaluation
        self.best_preds = []  # OUR M9 PATCH (M6)""",
    ),
    (
        "M5b",
        "codes/base.py",
        """            ####### Evaluate test data during training #######
            if (epoch+1) % evaluation_epoch == 0:
                test_results = self.evaluate(test_loader, datatype="Test")
                if test_results["HR@1"] > best_hr1:
                    best_hr1, eval_res, coverage  = test_results["HR@1"], test_results, epoch
                    best_state = copy.deepcopy(self.model.state_dict())

                self.save_model(best_state)""",
        """            ####### Evaluate test data during training #######
            if (epoch+1) % evaluation_epoch == 0:
                # *** OUR M9 PATCH (M5): MODEL SELECTION ***
                # Upstream selects the epoch on the TEST set ("if test_results['HR@1'] > best_hr1")
                # and reports that epoch's TEST score -- the test set picks the checkpoint, so the
                # number is a max over evaluations, not a held-out estimate. We select on a VAL
                # split carved out of TRAIN by CASE. EADRO_SELECT=test restores upstream.
                select_on = os.environ.get("EADRO_SELECT", "val")
                val_results = None
                if val_loader is not None and select_on != "test":
                    val_results = self.evaluate(val_loader, datatype="Val")
                test_results = self.evaluate(test_loader, datatype="Test")
                test_preds = self.last_preds  # evaluate(test) ran last -> these are TEST preds

                sel = val_results if val_results is not None else test_results
                sel_key = sel.get("HRfix@1", sel["HR@1"])
                if sel_key > best_hr1:
                    best_hr1, eval_res, coverage = sel_key, test_results, epoch
                    self.best_preds = test_preds
                    best_state = copy.deepcopy(self.model.state_dict())

                self.save_model(best_state)""",
    ),
]

MAIN_PATCHES = [(
    "M7a",
    "codes/main.py",
    """    train_chunks, test_chunks = load_chunks(data_dir)

    edges = metadata["edges"]
    train_data = chunkDataset(train_chunks, node_num, edges)
    test_data = chunkDataset(test_chunks, node_num, edges)""",
    """    train_chunks, test_chunks = load_chunks(data_dir)

    # *** OUR M9 PATCH (M7) ***  Carve a VALIDATION split out of TRAIN, BY CASE (chunks carry
    # `batch` = case index, added by the adapter). Splitting val off by CHUNK would re-introduce
    # exactly the 90%-overlap leak we removed from the train/test split: a val chunk would share
    # 90% of its window with a train chunk, and selection on it would be meaningless.
    import numpy as _np, json as _json, collections as _c
    _val_ratio = float(os.environ.get("EADRO_VAL_RATIO", "0.2"))
    _rng = _np.random.RandomState(int(os.environ.get("EADRO_SPLIT_SEED", "42")) + 1000)
    _by_batch = _c.defaultdict(list)
    for _k, _v in train_chunks.items():
        _by_batch[_v["batch"]].append(_k)
    _root_of = {}
    for _b, _ks in _by_batch.items():
        _labs = [train_chunks[_k]["culprit"] for _k in _ks if train_chunks[_k]["culprit"] != -1]
        _root_of[_b] = _labs[0] if _labs else -1
    _strata = _c.defaultdict(list)
    for _b in sorted(_by_batch):
        _strata[_root_of[_b]].append(_b)
    _val_b = []
    for _r in sorted(_strata):
        _bs = list(_strata[_r])
        _rng.shuffle(_bs)
        _n = int(round(_val_ratio * len(_bs)))
        if len(_bs) >= 2:
            _n = max(1, _n)
        _val_b += list(_bs[:_n])
    _val_b = set(_val_b)
    val_chunks = {_k: _v for _k, _v in train_chunks.items() if _v["batch"] in _val_b}
    train_chunks = {_k: _v for _k, _v in train_chunks.items() if _v["batch"] not in _val_b}
    logging.info("M7 split: train={} val={} test={} chunks | val cases={}".format(
        len(train_chunks), len(val_chunks), len(test_chunks), sorted(_val_b)))

    edges = metadata["edges"]
    train_data = chunkDataset(train_chunks, node_num, edges)
    val_data = chunkDataset(val_chunks, node_num, edges)
    test_data = chunkDataset(test_chunks, node_num, edges)""",
), (
    "M7b",
    "codes/main.py",
    """    test_dl = DataLoader(test_data, batch_size=params["batch_size"], shuffle=False, collate_fn=collate, pin_memory=True)

    model = BaseModel(event_num, metric_num, node_num, device, **params)
    scores, converge = model.fit(train_dl, test_dl, evaluation_epoch=evaluation_epoch)

    dump_scores(params["result_dir"], hash_id, scores, converge)""",
    """    test_dl = DataLoader(test_data, batch_size=params["batch_size"], shuffle=False, collate_fn=collate, pin_memory=True)
    val_dl = DataLoader(val_data, batch_size=params["batch_size"], shuffle=False, collate_fn=collate, pin_memory=True)

    model = BaseModel(event_num, metric_num, node_num, device, **params)
    scores, converge = model.fit(train_dl, test_dl, evaluation_epoch=evaluation_epoch,
                                 val_loader=val_dl)

    import json as _json
    # OUR M9 PATCH (M6/M7): dump per-chunk predictions ALONGSIDE the test chunks' provenance, in
    # the SAME order (test_dl has shuffle=False, and chunkDataset iterates chunks.keys()).
    _order = [{"chunk_id": _k, "case": test_chunks[_k].get("case"),
               "batch": test_chunks[_k].get("batch"),
               "culprit": int(test_chunks[_k]["culprit"])} for _k in test_chunks.keys()]
    _preds = getattr(model, "best_preds", [])
    _mismatch = sum(1 for _a, _b in zip(_order, _preds) if _a["culprit"] != _b["culprit"])
    with open(os.path.join(params["result_dir"], hash_id, "test_preds.json"), "w") as _fw:
        _json.dump({"order": _order, "preds": _preds, "n_order": len(_order),
                    "n_preds": len(_preds), "culprit_mismatch": _mismatch,
                    "selected_epoch": converge, "scores": scores,
                    "split_mode": metadata.get("split_mode"),
                    "split_seed": metadata.get("split_seed"),
                    "select_on": os.environ.get("EADRO_SELECT", "val"),
                    "test_cases": metadata.get("test_cases"),
                    "val_batches": sorted(_val_b),
                    "random_seed": params["random_seed"]}, _fw, indent=1)
    logging.info("M7 dumped test_preds.json (order={} preds={} culprit_mismatch={})".format(
        len(_order), len(_preds), _mismatch))

    dump_scores(params["result_dir"], hash_id, scores, converge)""",
)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--skip-base", action="store_true",
                    help="workdir is already patched by apply_patch_m9.py")
    args = ap.parse_args()

    if not args.skip_base:
        r = subprocess.call([sys.executable, os.path.join(HERE, "apply_patch_m9.py"),
                             "--workdir", args.workdir])
        if r != 0:
            sys.exit("apply_patch_m9.py failed")

    for pid, rel, old, new in PATCHES + MAIN_PATCHES:
        path = os.path.join(args.workdir, rel)
        src = open(path, encoding="utf-8").read()
        if src.count(old) != 1:
            sys.exit("FATAL {} {}: expected 1 match, found {}".format(pid, rel, src.count(old)))
        open(path, "w", encoding="utf-8").write(src.replace(old, new, 1))
        print("  [{}] {}".format(pid, rel))
    print("OK: {} honesty patches applied on top.".format(len(PATCHES) + len(MAIN_PATCHES)))


if __name__ == "__main__":
    main()
