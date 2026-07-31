#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
apply_patch.py -- Copy third_party/Eadro to a working dir and apply OUR fixes.

third_party/Eadro is READ-ONLY. We never edit it in place. This script copies it
to <workdir>/ and applies the patches below to the COPY.

Every patch is an exact-match replacement with an assertion: if upstream source
does not contain the expected snippet verbatim, we abort loudly rather than
silently producing a differently-patched tree.

*** ALL PATCHES BELOW ARE OUR MODIFICATIONS TO THE OFFICIAL EADRO CODE. ***
*** They must be declared as such in any paper / release. ***

Usage:
  python apply_patch.py                      # default workdir (scratchpad)
  python apply_patch.py --workdir <path>
  python apply_patch.py --check              # verify only, do not write
"""
import argparse
import os
import shutil
import sys
import tempfile

# Repo root = 4 levels up from scripts/methods/eadro/apply_patch.py
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
SRC = os.path.join(REPO_ROOT, "third_party", "Eadro")
DEFAULT_WORKDIR = os.path.join(tempfile.gettempdir(), "recshop_eadro_run")

# (id, relative file, old snippet, new snippet, why it crashes, class)
PATCHES = [
    (
        "P1",
        "codes/main.py",
        """    train_data = chunkDataset(train_chunks, node_num)
    test_data = chunkDataset(test_chunks, node_num)""",
        """    edges = metadata["edges"]
    train_data = chunkDataset(train_chunks, node_num, edges)
    test_data = chunkDataset(test_chunks, node_num, edges)""",
        "chunkDataset.__init__(self, chunks, node_num, edges) takes 3 args (main.py:5) "
        "but is called with 2 -> TypeError: __init__() missing 1 required positional "
        "argument: 'edges'. The graph topology is written into metadata.json by "
        "align.py:97 (info.add_info('edges', info.edges)) but main.py never reads it.",
        "CRASH (upstream bug)",
    ),
    (
        "P1b",
        "codes/main.py",
        """            graph = dgl.graph(edges, num_nodes=node_num)""",
        """            graph = dgl.graph((torch.tensor(edges[0]), torch.tensor(edges[1])),
                              num_nodes=node_num)""",
        "metadata['edges'] round-trips through JSON as [[src...],[dst...]] (a list of "
        "two lists). dgl.graph() wants a (src, dst) tuple of tensors/lists; a bare list "
        "of lists is not a documented input form. Make the conversion explicit.",
        "CRASH (follow-on from P1)",
    ),
    (
        "P2a",
        "codes/model.py",
        """        self.net = ConvNet(1, num_channels=trace_hiddens, kernel_sizes=trace_kernel_sizes, \n                    dev=device, dropout=trace_dropout)""",
        """        self.net = ConvNet(1, num_channels=trace_hiddens, kernel_sizes=trace_kernel_sizes,\n                    dev=device)""",
        "trace_dropout is never defined anywhere in the repo -> NameError: name "
        "'trace_dropout' is not defined. Even if it were defined, ConvNet.__init__ "
        "(model.py:47) has signature (num_inputs, num_channels, kernel_sizes, dilation, "
        "dev) and accepts no 'dropout' kwarg -> TypeError. ConvNet implements no dropout "
        "at all, so the faithful fix is to drop the argument.",
        "CRASH (upstream bug)",
    ),
    (
        "P2b",
        "codes/model.py",
        """        self.net = ConvNet(num_inputs=in_dim, num_channels=metric_hiddens, kernel_sizes=metric_kernel_sizes, \n                            dev=device, dropout=metric_dropout)""",
        """        self.net = ConvNet(num_inputs=in_dim, num_channels=metric_hiddens, kernel_sizes=metric_kernel_sizes,\n                            dev=device)""",
        "Same as P2a on the metric branch: metric_dropout is undefined -> NameError, "
        "and ConvNet takes no 'dropout' kwarg -> TypeError.",
        "CRASH (upstream bug)",
    ),
    (
        "P3",
        "codes/model.py",
        """        locate_logits = self.locator(embeddings)
        locate_loss = self.locator_criterion(locate_logits, fault_indexs.to(self.device))
        detect_logits = self.detector(embeddings)
        detect_loss = self.decoder_criterion(detect_logits, y_anomaly) """,
        """        locate_logits = self.localizer(embeddings)
        locate_loss = self.localizer_criterion(locate_logits, fault_indexs.to(self.device))
        detect_logits = self.detecter(embeddings)
        detect_loss = self.detecter_criterion(detect_logits, y_anomaly)""",
        "MainModel.__init__ (model.py:211-214) defines self.detecter / "
        "self.detecter_criterion / self.localizer / self.localizer_criterion, but "
        "forward() (model.py:230-233) calls self.locator / self.locator_criterion / "
        "self.detector / self.decoder_criterion -- four names that do not exist -> "
        "AttributeError: 'MainModel' object has no attribute 'locator'. Pure "
        "spelling drift between __init__ and forward; we rename the *uses* to match "
        "the *definitions* (no semantic change).",
        "CRASH (upstream bug)",
    ),
    (
        "P4",
        "codes/base.py",
        """import torch
from torch import nn
import logging""",
        """import torch
from torch import nn
import logging
import numpy as np  # OUR FIX: base.py:27 uses np.zeros but never imported numpy""",
        "base.py:27 `hrs, ndcgs = np.zeros(5), np.zeros(5)` uses np, but base.py never "
        "imports numpy -> NameError: name 'np' is not defined. Fires on the first call "
        "to evaluate().",
        "CRASH (upstream bug)",
    ),
    (
        "P5",
        "codes/preprocess/align.py",
        """    elif os.path.exists(os.path.join("../chunks", name, "chunks.pkl")):\n        with open(os.path.join("../chunks", name, "chunks.pkl"), "rb") as fr: \n            chunks.update(pickle.load(fr))""",
        """    elif os.path.exists(os.path.join("../chunks", name, "chunks.pkl")):\n        chunks = {}  # OUR FIX: chunks was never initialised on this branch\n        with open(os.path.join("../chunks", name, "chunks.pkl"), "rb") as fr:\n            chunks.update(pickle.load(fr))""",
        "split_chunks(): on the `concat` branch chunks is initialised at align.py:110, "
        "but on this elif branch (the cache-hit path, align.py:122-124) chunks is used "
        "via .update() without ever being bound -> UnboundLocalError: local variable "
        "'chunks' referenced before assignment. Fires whenever a cached chunks.pkl "
        "exists, i.e. on every re-run.",
        "CRASH (upstream bug)",
    ),
    (
        "P6",
        "codes/preprocess/align.py",
        """        chunks[chunk_id]["traces"] = traces["latency"][idx] #[node_num, chunk_lenth, 2]""",
        """        # OUR FIX: keep only channel 0 (mean latency). deal_traces allocates a
        # trailing dim of 2 (single_process.py:90) but only ever writes index [..., 0]
        # (single_process.py:106); channel 1 is dead all-zeros. TraceModel builds
        # ConvNet(num_inputs=1), so feeding 2 channels is a hard shape error.
        chunks[chunk_id]["traces"] = traces["latency"][idx][:, :, 0:1] #[node_num, chunk_lenth, 1]""",
        "MODALITY SHAPE MISMATCH. deal_traces (single_process.py:90) allocates "
        "latency = np.zeros((n_intervals, node_num, chunk_lenth, 2)) but only ever "
        "assigns [...][0] (single_process.py:106) -- the 2nd channel is dead all-zeros. "
        "align.py:64 then stores all 2 channels as chunk['traces']. But TraceModel "
        "(model.py:105) constructs ConvNet(1, ...) i.e. num_inputs=1, and ConvNet.forward "
        "permutes [bz, T, in_dim] -> [bz, in_dim, T] and feeds Conv1d(in_channels=1). "
        "With in_dim=2 this raises RuntimeError: Given groups=1, weight of size "
        "[h,1,k], expected input[N,1,T] but got input[N,2,T]. Slicing to channel 0 is "
        "the only reading consistent with the model definition.",
        "CRASH (upstream bug -- NOT in the originally-reported 5)",
    ),
    (
        "P7",
        "codes/base.py",
        """        if coverage > 5:""",
        """        # OUR FIX (P7 + P10): fit() only evaluates inside
        #   `if (epoch+1) % evaluation_epoch == 0`.
        # If that never fires (epoches < evaluation_epoch, or an early stop before the
        # first eval), then eval_res and coverage are both still None here, which gives
        #   (a) TypeError: '>' not supported between 'NoneType' and 'int'   <- next line
        #   (b) AttributeError: 'NoneType' object has no attribute 'items'  <- later, in
        #       utils.dump_scores(), because fit() returns eval_res=None.
        # Guarantee that at least one evaluation has happened before we report.
        if eval_res is None and test_loader is not None:
            logging.info("No evaluation epoch reached; running a final evaluation.")
            eval_res = self.evaluate(test_loader, datatype="Test")
            best_hr1, coverage = eval_res["HR@1"], self.epoches
            self.save_model(copy.deepcopy(self.model.state_dict()))

        if coverage is not None and coverage > 5:""",
        "fit() initialises eval_res=None and coverage=None (base.py:65) and only assigns "
        "them inside `if (epoch+1) % evaluation_epoch == 0` (base.py:101). If training "
        "early-stops before the first evaluation epoch -- or if epoches < "
        "evaluation_epoch -- both are still None afterwards, giving TWO failures: "
        "(a) base.py:109 `coverage > 5` -> TypeError: '>' not supported between "
        "instances of 'NoneType' and 'int'; and (b) fit() returns eval_res=None, so "
        "utils.dump_scores() (utils.py:32) does None.items() -> AttributeError. "
        "VERIFIED by running the patched tree with --epoches 4 (< evaluation_epoch=10). "
        "Fix: force one final evaluation if none ever ran.",
        "CRASH x2 (upstream bug -- NOT in the originally-reported 5)",
    ),
    (
        "P8",
        "codes/base.py",
        """        for j in [1, 3, 5]:
            eval_results["HR@"+str(j)] = hrs[j-1]*1.0/pos
            eval_results["ndcg@"+str(j)] = ndcgs[j-1]*1.0/pos""",
        """        for j in [1, 3, 5]:  # OUR FIX: guard pos==0 (test split with no faulty chunk)
            eval_results["HR@"+str(j)] = hrs[j-1]*1.0/pos if pos > 0 else 0.0
            eval_results["ndcg@"+str(j)] = ndcgs[j-1]*1.0/pos if pos > 0 else 0.0""",
        "evaluate(): pos = TP+FN is the number of faulty chunks in the test split. "
        "base.py:52-54 carefully guards division by zero for F1/Rec/Pre, but "
        "base.py:57-58 divides by pos unguarded -> ZeroDivisionError whenever a test "
        "split happens to contain no faulty chunk (likely on small/imbalanced data).",
        "CRASH (upstream bug -- NOT in the originally-reported 5)",
    ),
    (
        "P9",
        "codes/preprocess/align.py",
        """    aim_dir = os.path.join("../chunks", name)
    if not os.path.exists(aim_dir): os.mkdir(aim_dir)
    ############## Concat all chunks ##############""",
        """    aim_dir = os.path.join("../chunks", name)
    os.makedirs(aim_dir, exist_ok=True)  # OUR FIX: mkdir fails if ../chunks itself is absent
    ############## Concat all chunks ##############""",
        "os.mkdir() is not recursive: on a clean checkout ../chunks does not exist, so "
        "creating ../chunks/<name> raises FileNotFoundError. Same one-level assumption "
        "at align.py:46. Cosmetic but blocks the very first run.",
        "ROBUSTNESS (blocks first run)",
    ),
    (
        "P9b",
        "codes/preprocess/align.py",
        """    aim_dir = os.path.join("../chunks", name, idx)
    if not os.path.exists(aim_dir): os.mkdir(aim_dir)""",
        """    aim_dir = os.path.join("../chunks", name, idx)
    os.makedirs(aim_dir, exist_ok=True)  # OUR FIX: see P9""",
        "Same non-recursive os.mkdir issue at align.py:45-46.",
        "ROBUSTNESS (blocks first run)",
    ),
]


def _force_rm(func, path, _exc):
    """rmtree onerror: clear read-only bit (Windows) and retry."""
    import stat
    os.chmod(path, stat.S_IWRITE)
    func(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default=DEFAULT_WORKDIR)
    ap.add_argument("--check", action="store_true",
                    help="verify all patches still match upstream; write nothing")
    args = ap.parse_args()

    if not os.path.isdir(SRC):
        sys.exit("FATAL: upstream not found: " + SRC)

    if args.check:
        target = SRC  # match against pristine upstream
    else:
        if os.path.exists(args.workdir):
            # upstream .git packs are read-only on Windows -> plain rmtree hits WinError 5
            shutil.rmtree(args.workdir, onerror=_force_rm)
        shutil.copytree(SRC, args.workdir,
                        ignore=shutil.ignore_patterns(".git", "__pycache__"))
        print("copied  {} -> {}".format(SRC, args.workdir))
        target = args.workdir

    failed = []
    for pid, rel, old, new, why, cls in PATCHES:
        path = os.path.join(target, rel)
        with open(path, "r", encoding="utf-8") as f:
            src = f.read()
        n = src.count(old)
        if n != 1:
            failed.append("{} {}: expected exactly 1 match, found {}".format(pid, rel, n))
            continue
        if not args.check:
            with open(path, "w", encoding="utf-8") as f:
                f.write(src.replace(old, new, 1))
        print("  [{}] {:<28} {}  -- {}".format(pid, rel, cls, why.split(".")[0]))

    if failed:
        print("\nFAILED (upstream source drifted from what these patches expect):")
        for m in failed:
            print("  " + m)
        sys.exit(1)

    print("\nOK: {} patches {}.".format(
        len(PATCHES), "verified against upstream" if args.check else "applied"))
    if not args.check:
        print("Patched tree: " + args.workdir)
        print("third_party/Eadro was NOT modified.")


if __name__ == "__main__":
    main()
