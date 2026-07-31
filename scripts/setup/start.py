#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""start.py — one-click start all 25 microservices (wraps start_all.py).

Usage:
  python scripts/setup/start.py              start (offline mode by default)
  python scripts/setup/start.py --no-docker  skip Docker OTel stack
  python scripts/setup/start.py --stop       stop all
With Nacos installed:  NACOS_ENABLED=true python scripts/setup/start.py
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


def main():
    os.environ.setdefault("NACOS_ENABLED", "false")   # offline: 127.0.0.1 fixed ports
    os.environ.setdefault("NO_PROXY", "*")            # bypass Clash proxy
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    print("=== start RecShop 25 services (NACOS_ENABLED=%s) ===" % os.environ["NACOS_ENABLED"])
    subprocess.run(
        [sys.executable, str(ROOT / "start_all.py")] + sys.argv[1:],
        cwd=ROOT, check=True,
    )


if __name__ == "__main__":
    main()
