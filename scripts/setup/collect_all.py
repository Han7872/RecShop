#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""collect_all.py — one-click FULL data collection (traditional 255 + agent 108).

WARNING (read before running):
  - Overnight-scale (8h+); don't run casually.
  - Needs full 25-service K8S stack + Chaos Mesh + large assets + pfwd guards +
    kubectl proxy (:8001) all up (each collect script pre-flights these).
  - Agent collection calls DeepSeek API (run_collect_agentfault.sh --yes = paid).
  datasets/ already ships pre-collected outputs -- this script is for reproducing /
  extending collection only. To validate the stack first, run a single script, e.g.:
      bash scripts/chaos/ctk/collect-single-dense.sh
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
CTK = ROOT / "scripts" / "chaos" / "ctk"
AGT = ROOT / "scripts" / "chaos" / "agentfault"

STEPS = [
    ("single dense (40)", "collect-single-dense.sh"),
    ("dual dense (80)", "collect-dual-dense.sh"),
    ("triple dense (20)", "collect-triple-dense.sh"),
    ("single spread (55)", "collect-single-spread.sh"),
    ("single recagent (15)", "collect-single-recagent.sh"),
    ("G2ext - dual_ext 25 + triple_ext 20", "collect-g2ext.sh"),
]


def main():
    print("==============================================================")
    print(" FULL COLLECTION - traditional 255 + agent 108")
    print(" overnight; needs K8S + Chaos Mesh + assets; datasets/ ships pre-collected")
    print("==============================================================")
    ans = input("Run full collection? (type yes to continue): ").strip()
    if ans != "yes":
        print("cancelled.")
        return
    for i, (label, script) in enumerate(STEPS, 1):
        print("\n### [%d/7] %s" % (i, label))
        subprocess.run(["bash", str(CTK / script)], cwd=ROOT, check=True)
    print("\n### [7/7] agent semantic faults (108, --yes paid)")
    subprocess.run(["bash", str(AGT / "run_collect_agentfault.sh"), "--yes"], cwd=ROOT, check=True)
    print("\n==============================================================")
    print(" FULL COLLECTION DONE - traditional 255 + agent 108")
    print("==============================================================")


if __name__ == "__main__":
    main()
