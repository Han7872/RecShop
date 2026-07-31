#!/usr/bin/env python
# instance_check.py <case_dir> <root_svc>   root_svc = catalog | inventory | pricing
# Verifies groundtruth root_cause_instances pod(s) for the rolling root service
# are present in during_fault telemetry (the runner-fix invariant).
import json, io, os, sys

case, root_svc = sys.argv[1], sys.argv[2]

def is_target(p):
    if root_svc == "catalog":
        return p.startswith("catalog-") and not p.startswith("catalog-gw")
    return p.startswith(root_svc + "-")

gt = json.load(io.open(os.path.join(case, "groundtruth.json"), encoding="utf-8"))
rci = [str(x) for x in gt.get("root_cause_instances", [])]
pinned = set(p for p in rci if is_target(p))

during = set()
with io.open(os.path.join(case, "raw", "metrics", "metrics_v2.jsonl"), encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        r = json.loads(line)
        if r.get("stage") == "during_fault":
            p = str((r.get("labels", {}) or {}).get("pod", ""))
            if is_target(p):
                during.add(p)

ok = bool(pinned) and pinned.issubset(during)
print(f"INSTANCE({root_svc}): pinned={pinned} during_fault={during} -> {'OK' if ok else 'FAIL'}")
sys.exit(0 if ok else 1)
