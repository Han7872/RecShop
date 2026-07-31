#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""install.py — one-click install Python dependencies.

Large model assets (too big for the repo) must be supplied separately — see README.
Usage:  python scripts/setup/install.py
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent  # scripts/setup/ -> repo root


def main():
    print("=== pip install -r requirements.txt ===")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(ROOT / "requirements.txt")],
        check=True,
    )
    print("\n=== Python deps installed ===\n")
    print("Large model assets (NOT in repo) must be placed at:")
    print("  services/sasrec_api/standard_cache.pkl           (~9.2 GB)")
    print("  services/sasrec_api/SASRec-*.pth                  (~260 MB)")
    print("  services/recommendation_agent/electronics.inter   (~2.4 GB)")
    print("  shared/data/electronics.item                      (~1.2 GB)")
    print("\nNext:  python scripts/setup/init_db.py   ->   python scripts/setup/start.py")


if __name__ == "__main__":
    main()
