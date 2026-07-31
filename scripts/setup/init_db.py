#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""init_db.py — one-click DB init: build schema -> load items -> demo seed.

Prerequisites: MySQL 8.0+ running + large asset shared/data/electronics.item in place.
Usage:  DB_PASSWORD=<pwd> python scripts/setup/init_db.py
Env:    DB_USER (default root) / DB_NAME (default shopify2) / ITEM_FILE
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


def run_mysql(sql_file, db_name=None):
    db_user = os.environ.get("DB_USER", "root")
    db_pass = os.environ["DB_PASSWORD"]
    cmd = ["mysql", "-u" + db_user, "-p" + db_pass]
    if db_name:
        cmd.append(db_name)
    with open(sql_file, encoding="utf-8") as f:
        subprocess.run(cmd, stdin=f, cwd=ROOT, check=True)


def main():
    if not os.environ.get("DB_PASSWORD"):
        sys.exit("DB_PASSWORD env var required, e.g.  "
                 "DB_PASSWORD=xxx python scripts/setup/init_db.py")
    db_name = os.environ.get("DB_NAME", "shopify2")
    item_file = os.environ.get("ITEM_FILE", str(ROOT / "shared" / "data" / "electronics.item"))

    print("=== 1/3 build schema (scripts/build_database.sql, idempotent) ===")
    run_mysql(ROOT / "scripts" / "build_database.sql")

    print("\n=== 2/3 load items (scripts/import_data.py) ===")
    if not Path(item_file).is_file():
        sys.exit("Large asset not found: %s\n"
                 "Place electronics.item at shared/data/ (see README) and re-run." % item_file)
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "import_data.py"),
         "--user", os.environ.get("DB_USER", "root"),
         "--password", os.environ["DB_PASSWORD"],
         "--database", db_name,
         "--item-file", item_file],
        cwd=ROOT, check=True,
    )
    # NOTE: interactions are not loaded by default (--limit-interactions=0).
    # For the recommendation chain's history, add --inter-file <electronics.inter>.

    print("\n=== 3/3 demo seed (scripts/seed_demo_data.sql) ===")
    run_mysql(ROOT / "scripts" / "seed_demo_data.sql", db_name=db_name)

    print("\n=== init_db done ===\nNext:  python scripts/setup/start.py")


if __name__ == "__main__":
    main()
