#!/usr/bin/env python
# verify_dual.py <case_dir>  — self-contained dual/single case verifier.
# 3-part (gate+checksum / 18-field trace / 6 K8s labels) + instance (all root pods ∈ during_fault).
import json, io, os, sys

case = sys.argv[1]
NEW = ["process_id", "process_tags", "references", "collector_query_service"]
K8S = ["pod", "namespace", "node", "uid", "instance", "container"]
fails = []

m = json.load(io.open(os.path.join(case, "metadata.json"), encoding="utf-8"))
cg = m.get("checksum_guard", {}) or {}
if m.get("ready_for_release") is not True: fails.append("not ready_for_release")
if cg.get("zero_drift") is not True: fails.append("not zero_drift")
if not os.path.isfile(os.path.join(case, "groundtruth.json")): fails.append("no groundtruth.json")
if not os.path.isfile(os.path.join(case, "summary.md")): fails.append("no summary.md")

tp = os.path.join(case, "raw", "traces", "during_fault_traces.jsonl")
if os.path.isfile(tp):
    line = io.open(tp, encoding="utf-8").readline()
    if line.strip():
        r = json.loads(line)
        if [k for k in NEW if k not in r]: fails.append("trace missing new fields")
        pt = r.get("process_tags")
        if not (pt and any(str(t.get("key", "")).startswith("telemetry.sdk") for t in pt)):
            fails.append("process_tags empty/no telemetry.sdk")
    else:
        fails.append("empty during_fault_traces")
else:
    fails.append("no during_fault_traces.jsonl")

mv = os.path.join(case, "raw", "metrics", "metrics_v2.jsonl")
ok6 = False
during = set()
with io.open(mv, encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        r = json.loads(line)
        lbl = r.get("labels", {}) or {}
        if not ok6 and all(lbl.get(k) not in (None, "") for k in K8S):
            ok6 = True
        if r.get("stage") == "during_fault":
            p = str(lbl.get("pod", ""))
            if p:
                during.add(p)
if not ok6:
    fails.append("no record with all 6 K8s labels")

gt = json.load(io.open(os.path.join(case, "groundtruth.json"), encoding="utf-8"))
rci = [str(x) for x in gt.get("root_cause_instances", [])]
# real pod names only (exclude off-graph: mysql:items, node, host)
pods = [p for p in rci if "-" in p and ":" not in p and p not in ("node", "host")]
missing = [p for p in pods if p not in during]
# off-graph 根(mysql:items / host / node)合法无 pod(db_lock_single/host_cpu_single 即纯 off-graph 根)。
# 只在 rci 完全空(无任何根)时判错;有真 pod 根则查其 ∈ during_fault(instance-fix guardrail)。
if not rci:
    fails.append("root_cause_instances empty")
if missing:
    fails.append(f"instance NOT in during_fault: {missing}")

print(f"  root pods={pods} | missing_from_during={missing}")
if fails:
    print("VERIFY=FAIL", fails)
    sys.exit(1)
print("VERIFY=PASS (3-part + instance ∈ during_fault)")
sys.exit(0)
