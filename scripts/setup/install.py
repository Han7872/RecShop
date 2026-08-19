#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""install.py — one-click install Python dependencies.

Checks the Python version, installs requirements.txt, verifies the large model
assets, and prints the non-Python prerequisites. See ENVIRONMENT.md for the
pinned platform versions the datasets were collected on.
Usage:  python scripts/setup/install.py
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent  # scripts/setup/ -> repo root

ASSETS = [
    ("services/sasrec_api/standard_cache.pkl", "~9.2 GB"),
    ("services/sasrec_api/SASRec-*.pth", "~260 MB"),
    ("services/recommendation_agent/electronics.inter", "~2.4 GB"),
    ("shared/data/electronics.item", "~1.2 GB"),
]


def check_python():
    v = sys.version_info
    if v < (3, 10):
        sys.exit(f"FATAL: Python 3.10+ required (collection env was 3.10.20); you are on {v.major}.{v.minor}")
    if v >= (3, 12):
        print(f"NOTE: collection env was 3.10.20; {v.major}.{v.minor} is untested with the pinned deps")


def install_deps():
    print("=== pip install -r requirements.txt ===")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(ROOT / "requirements.txt")],
        check=True,
    )


def torch_note():
    print("\n=== torch platform note ===")
    print("requirements.txt leaves `torch` unpinned (platform variant). The pip install")
    print("above pulled the CPU build — which matches the collection environment")
    print("(sasrec inference ran with cuda=False). For a CUDA build instead:")
    print("  pip install torch --index-url https://download.pytorch.org/whl/cu121")


def asset_report():
    print("\n=== large model assets (NOT in repo; download links in README) ===")
    for rel, size in ASSETS:
        if "*" in rel:
            found = any(Path(ROOT).glob(rel.split("*")[0] + "*"))
        else:
            found = (ROOT / rel).exists()
        print(f"  [{'OK' if found else 'MISSING'}] {rel}  ({size})")


def prereq_note():
    print("\n=== non-Python prerequisites (versions pinned in ENVIRONMENT.md) ===")
    print("  - MySQL 8.0 reachable as DB_HOST (seed: python scripts/setup/init_db.py)")
    print("  - Docker Desktop (K8s enabled) + Chaos Mesh — only needed for fault collection,")
    print("    not for running the 25 services or the evaluation")
    print("\nNext:  python scripts/setup/init_db.py   ->   python scripts/setup/start.py")


def main():
    check_python()
    install_deps()
    torch_note()
    asset_report()
    prereq_note()
    print("\n=== done ===")


if __name__ == "__main__":
    main()
