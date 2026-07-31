#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
m9_eadro_adapter.py — RecShop (RecWeb2) K8S chaos cases  ->  Eadro `parsed_data/<name>/` inputs.

Eadro (ISSRE'23) does NOT eat a CSV wide table. Its preprocess (third_party/Eadro/codes/preprocess/)
reads a directory `./parsed_data/<name>/` containing, per batch index `idx` (0,1,2,...):

    records<idx>.json      {"faults":[{"s":<slot>,"e":<slot>,"service":<name>}], "start":<slot>, "end":<slot>}
    metrics<idx>/<svc>.csv  one row per service per slot; cols = timestamp + metric_names
                            HARD ASSERT (single_process.py:68): df.loc[s:e] .shape == (chunk_lenth, metric_num)
    traces<idx>.json       {"<slot>": {"<src_nid>-<dst_nid>": [lat, lat, ...]}}
                            latency is attributed to the CALLEE  (single_process.py:102 int(k.split('-')[-1]))
    logs<idx>.csv          cols = timestamp,service,event   (event = Drain template string)
    templates.json         list[str], shared across all idx  (single_process.py:18)

align.py then chunks these with a sliding window (chunk_lenth slots, stride 1) and labels each chunk
with the culprit NODE ID (-1 = normal).  The graph is a HARDCODED dict in util.py (NOT mined from
traces) -> we hand-author the RecShop topology below and also cross-check it against the traces.

-------------------------------------------------------------------------------------------------
DECLARED DEVIATIONS FROM THE ORIGINAL EADRO PAPER/CODE (all recorded into `adapter_report.json`)
-------------------------------------------------------------------------------------------------
D1  SLOT = 2 s, not 1 s.  Our cAdvisor poll cadence is 2 s (48 snapshots / case, all 12 services
    share an identical timestamp set -- verified).  Eadro's "1 slot = 1 s" is an artifact of its
    collector, and nothing in its code depends on the physical unit: `intervals` are plain ints.
    Consequence: chunk_lenth=10 spans 20 s of wall time, not 10 s.

D2  METRIC COLUMNS = our 16 cAdvisor columns, not Eadro's 7.
    Eadro/TrainTicket: cpu_usage_{system,total,user}, memory_usage, memory_working_set, rx/tx_bytes.
    We emit what we actually have (see CADVISOR_METRICS).  `metric_num` is a free parameter in
    metadata.json, so this is legal.  Overlap is partial (cpu/mem/net all present, but ours are
    rates/cores rather than counters, plus 9 extra columns incl. container_start_time_seconds --
    which is the cleanest pod_failure signal we have: it jumps on restart).

D3  BATCH idx = ONE CASE (all 3 stages concatenated onto a contiguous slot axis, inter-stage gaps
    COLLAPSED), *not* one stage per batch.  This matters and is not cosmetic: deal_metrics z-scores
    each metrics<idx>/<svc>.csv file as a whole (single_process.py:64).  If a batch were a single
    stage, the during_fault batch would be z-scored *within the fault*, normalizing the anomaly
    clean away.  Concatenating the 3 stages keeps pre/post as the reference distribution, so the
    fault survives z-scoring.  Cost: the ~13 s (chaos-apply) and ~5 s (recovery-settle) gaps between
    stages are removed, so a handful of chunks straddle a discontinuity.  Counted in the report.

D4  LOG TIMESTAMPS are quantized to the integer slot index.  Forced by deal_logs' own filter
    (single_process.py:33: `df.timestamp >= s & <= e` with integer s,e) -- a float sub-slot
    timestamp of e.g. slot+0.5 in the last slot `e` would be silently DROPPED.  Upstream has the
    same quantization (integer unix seconds); ours is 2 s-coarse instead of 1 s.  Hawkes' identical-
    timestamp collision is handled by upstream's own 1e-5 jitter (single_process.py:45).

D5  OFF-GRAPH ROOT CAUSES.  Eadro's localizer is an N-way classifier over service nodes; two of our
    8 single-fault types have a root that is not a container:
      * db_lock    -> root_cause_services = ["mysql_items_lock"]  -> mapped to node `mysql`.
        `mysql` is a REAL node for us: it has no cAdvisor row (metric channel zero-filled) but it
        DOES have a genuine trace channel (993 mysql client spans in one case) and is a callee of
        10 services.  Learnable, though metric-blind.
      * host_cpu   -> root_cause_services = ["host"]              -> node `host`.
        `host` has NO metric, NO trace, NO log channel and NO edges: it is UNLEARNABLE BY
        CONSTRUCTION.  Included only under --include-host (default: EXCLUDED).  Do not report a
        host_cpu localization number without this caveat.

D6  Trace channel is genuinely reconstructable: 100% of client spans resolve to a callee
    (http.url hostname, or db.system==mysql).  No fabrication needed.  See report `client_spans`.

-------------------------------------------------------------------------------------------------
CORPUS SELECTION (2026-07-13 -- this used to be a silent bug)
-------------------------------------------------------------------------------------------------
Case discovery goes through `dataset_registry` (datasets/REGISTRY.json = single source of truth).
Until 2026-07-13 this file built its source path as `"{}_dense".format(arity)`, which means the
`single_spread` tree -- 55 real, active cases -- was INVISIBLE to Eadro.  No warning, no error:
the run just quietly trained on 140 and printed a clean-looking table.

    --corpus dense  (default)  the 3 *_dense trees  = 140 case  <- historical corpus, numbers
                               stay directly comparable with every earlier Eadro run.
    --corpus all               every active tree     = 195 case  <- + single_spread's 55.
    --family <signal_class>    optional, per-case from groundtruth.json; unknown -> hard error.

    ⚠ --family is NOT how you reproduce the 140.  family is a property of the *fault_type*
      (see REGISTRY fault_classes), not of the tree: propagation is only 25 cases.
      Use --corpus dense.

Usage (run under the ISOLATED `eadro` env -- NOT recweb2):
    python3 m9_eadro_adapter.py \
        --arity single --out <parsed_data_root> --name R0 [--include-host] [--build-chunks]

datasets/ and third_party/ are READ-ONLY here: this script only ever writes under --out.
"""

from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dataset_registry as DR   # noqa: E402  (datasets/ 唯一真相源)

# --------------------------------------------------------------------------------------------
# 1. RecShop topology  (hand-authored: Eadro's graph is a hardcoded dict, util.py:12-25/:32-42)
#    Derived from the project architecture tree, restricted to the 12 services actually deployed
#    in the K8S dense environment (= the 12 that emit cAdvisor), + `mysql` (+ optional `host`).
#    Cross-checked against the real client spans at runtime; diffs land in the report.
# --------------------------------------------------------------------------------------------
CADVISOR_SERVICES = [
    "checkout", "cart", "inventory", "backend", "sasrec", "order",
    "search", "review-query", "user", "pricing", "catalog-gw", "catalog",
]
MYSQL_NODE = "mysql"
HOST_NODE = "host"

# adjacency: caller -> [callees].  Self-loops added for every node (Eadro's dicts include self).
EDGE_INFO = {
    "checkout":     ["cart", "pricing", "inventory"],
    "pricing":      ["catalog-gw", "catalog"],
    "cart":         ["catalog-gw", "catalog"],
    "order":        ["catalog-gw", "catalog"],
    "search":       ["catalog-gw", "catalog"],
    "review-query": ["catalog-gw", "catalog"],
    "inventory":    ["catalog-gw", "catalog"],
    "catalog-gw":   ["catalog"],
    "backend":      ["sasrec"],
    "catalog":      [],
    "sasrec":       [],
    "user":         [],
}
# every service with a mysql-connector / SQLAlchemy pool talks to the shared shopify2 DB
MYSQL_CALLERS = ["checkout", "cart", "catalog", "pricing", "inventory", "order",
                 "search", "review-query", "user", "backend"]

# our 16 cAdvisor columns (D2).  Order is fixed: it defines the metric axis.
CADVISOR_METRICS = [
    "container_cpu_usage_cores",
    "container_cpu_throttled_periods_rate",
    "container_cpu_throttled_seconds_rate",
    "container_memory_usage_bytes",
    "container_memory_working_set_bytes",
    "container_memory_rss_bytes",
    "container_memory_failcnt",
    "container_start_time_seconds",
    "container_network_receive_bytes_rate",
    "container_network_transmit_bytes_rate",
    "container_network_receive_packets_rate",
    "container_network_transmit_packets_rate",
    "container_network_receive_dropped_rate",
    "container_network_transmit_dropped_rate",
    "container_network_receive_errors_rate",
    "container_network_transmit_errors_rate",
]

# trace uses long OTel service.name, metrics/logs use the k8s short name.  (NOT per_service_canon.py:
# that is the Toxiproxy-era long-name table and is not on the M9 path.)
LONG2SHORT = {
    "checkout_service": "checkout",
    "cart_service": "cart",
    "catalog_service": "catalog",
    "pricing_service": "pricing",
    "inventory_service": "inventory",
    "backend_api": "backend",
    "sasrec_api": "sasrec",
    "order_service": "order",
    "search_service": "search",
    "review_query_service": "review-query",
    "user_service": "user",
    "catalog_gw": "catalog-gw",
}
# groundtruth root_cause_services -> node name  (D5)
ROOT2NODE = {"mysql_items_lock": MYSQL_NODE, "host": HOST_NODE}

STAGES = ["pre_fault", "during_fault", "post_recovery"]


def build_nodes(include_host: bool):
    nodes = list(CADVISOR_SERVICES) + [MYSQL_NODE]
    if include_host:
        nodes.append(HOST_NODE)
    return nodes


def build_edges(nodes, observed_pairs, mode="union"):
    """Return ([src_nids],[dst_nids]) plus the static/observed diff for the report."""
    nid = {s: i for i, s in enumerate(nodes)}
    static = set()
    for src, dsts in EDGE_INFO.items():
        for d in dsts:
            static.add((src, d))
    for src in MYSQL_CALLERS:
        static.add((src, MYSQL_NODE))

    obs = {(s, d) for (s, d) in observed_pairs if s in nid and d in nid}
    if mode == "static":
        use = set(static)
    elif mode == "trace":
        use = set(obs)
    else:
        use = static | obs
    use |= {(n, n) for n in nodes}          # self-loops, per Eadro's own edge_info convention

    src_l, dst_l = [], []
    for s, d in sorted(use):
        src_l.append(nid[s]); dst_l.append(nid[d])
    diff = {
        "static_only": sorted("->".join(e) for e in (static - obs)),
        "observed_only": sorted("->".join(e) for e in (obs - static)),
        "both": sorted("->".join(e) for e in (static & obs)),
    }
    return [src_l, dst_l], diff


# --------------------------------------------------------------------------------------------
# 2. helpers
# --------------------------------------------------------------------------------------------
def parse_ts(s):
    """'2026-07-11T09:18:11.036Z' -> epoch float (UTC)."""
    if s is None:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s).timestamp()


# Our pods emit THREE different log formats.  Handling only the first (werkzeug) silently empties
# the log channel of catalog-gw and sasrec -- and catalog-gw sits on the fault path of every
# net_* case, so that would have been a quiet, load-bearing hole.
#   1. werkzeug (the 10 Flask services):
#      [pod/catalog-.../catalog] 2026-07-11 09:18:11,007 [INFO] werkzeug: 127.0.0.1 - - [...] "GET /x" 200 -
LOG_RE = re.compile(
    r"^\[pod/(?P<pod>[^/\]]+)/(?P<container>[^\]]+)\]\s+"
    r"(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+"
    r"\[(?P<level>[A-Z]+)\]\s+(?P<rest>.*)$"
)
#   2. nginx combined (catalog-gw), 1 s resolution, explicit +0000:
#      [pod/catalog-gw-.../nginx] 10.244.0.9 - - [11/Jul/2026:14:42:41 +0000] "GET /api/items/x" 200 365 ...
NGINX_RE = re.compile(
    r"^\[pod/(?P<pod>[^/\]]+)/(?P<container>[^\]]+)\]\s+"
    r"(?P<ip>\S+) \S+ \S+ \[(?P<ts>\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})\]\s+(?P<rest>.*)$"
)
#   3. uvicorn (sasrec):  [pod/sasrec-.../sasrec] INFO:     10.244.0.1:41216 - "GET /health" 200 OK
#      -> carries NO TIMESTAMP AT ALL, so it cannot be binned to a slot.  We DROP these rather than
#      spread them evenly over the stage (that would be fabricated timing).  Consequence: sasrec's
#      log channel is empty (all-zero Hawkes baseline).  Reported as `log_channel_empty`.


def parse_log_ts(s):
    """Log clocks are UTC (verified: first during_fault line 09:18:11,007 vs metric window start
    09:18:11.036Z).  Handles both the werkzeug and the nginx-combined stamps."""
    if "," in s:
        d = dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=dt.timezone.utc)
    else:
        d = dt.datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")
    return d.timestamp()


class Masker:
    """Drain3 if available; otherwise a declared regex fallback (recorded in the report)."""

    def __init__(self):
        self.backend = "regex_fallback"
        self.miner = None
        try:
            from drain3 import TemplateMiner
            from drain3.template_miner_config import TemplateMinerConfig
            cfg = TemplateMinerConfig()
            cfg.drain_sim_th = 0.4
            cfg.drain_depth = 4
            self.miner = TemplateMiner(config=cfg)
            self.backend = "drain3"
        except Exception:
            pass

    _num = re.compile(r"\b\d+\b")
    _ip = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
    _hex = re.compile(r"\b[0-9a-f]{8,}\b", re.I)

    def add(self, msg):
        if self.miner is not None:
            r = self.miner.add_log_message(msg)
            return r["template_mined"]
        m = self._ip.sub("<IP>", msg)
        m = self._hex.sub("<HEX>", m)
        m = self._num.sub("<NUM>", m)
        return m


# --------------------------------------------------------------------------------------------
# 3. per-case extraction
# --------------------------------------------------------------------------------------------
def load_case(case_dir, nodes, masker, report):
    """Return a dict with the slot grid, per-node metrics, trace latencies, log events, fault window."""
    nid = {s: i for i, s in enumerate(nodes)}
    name = os.path.basename(case_dir)
    meta = json.load(open(os.path.join(case_dir, "metadata.json"), encoding="utf-8"))
    gt = json.load(open(os.path.join(case_dir, "groundtruth.json"), encoding="utf-8"))

    # ---- 3a. slot grid from cAdvisor timestamps (2 s cadence; D1) --------------------------
    cad = collections.defaultdict(dict)        # (svc, ts) -> {metric: value}
    ts_by_stage = collections.defaultdict(set)
    for line in open(os.path.join(case_dir, "raw/metrics/metrics_v2.jsonl"), encoding="utf-8"):
        r = json.loads(line)
        if r.get("source") != "cadvisor":
            continue
        svc, ts = r["service"], r["timestamp"]
        if r["metric"] in CADVISOR_METRICS:
            cad[(svc, ts)][r["metric"]] = r["value"]
            ts_by_stage[r["stage"]].add(ts)

    # contiguous slot axis: stages in order, inter-stage gaps collapsed (D3)
    slot_ts, slot_stage = [], []
    for st in STAGES:
        for ts in sorted(ts_by_stage[st]):
            slot_ts.append(ts); slot_stage.append(st)
    n_slots = len(slot_ts)
    if n_slots == 0:
        raise RuntimeError(f"{name}: no cAdvisor samples")
    slot_epoch = [parse_ts(t) for t in slot_ts]
    ts2slot = {t: i for i, t in enumerate(slot_ts)}

    # ---- 3b. metrics: exactly 1 row per node per slot (zero-fill misses; hard assert in Eadro) --
    metrics = {}               # node -> list[n_slots] of list[16]
    zfill = collections.Counter()
    for node in nodes:
        rows = []
        for i, ts in enumerate(slot_ts):
            cell = cad.get((node, ts))
            if cell is None:
                rows.append([0.0] * len(CADVISOR_METRICS))
                zfill[node] += 1                      # mysql/host: always; pod_failure: if cAdvisor stops
            else:
                rows.append([float(cell.get(m, 0.0)) for m in CADVISOR_METRICS])
        metrics[node] = rows

    # ---- 3c. traces: client spans -> callee latency  (the key channel) ----------------------
    inv = collections.defaultdict(list)        # (slot, src_nid, dst_nid) -> [lat_ms]
    span_stat = collections.Counter()
    obs_pairs = set()
    for f in sorted(glob.glob(os.path.join(case_dir, "raw/traces/*_traces.jsonl"))):
        for line in open(f, encoding="utf-8"):
            r = json.loads(line)
            tags = {t["key"]: t["value"] for t in r.get("tags", [])}
            span_stat["total"] += 1
            kind = tags.get("span.kind")
            span_stat["kind_" + str(kind)] += 1
            if kind != "client":
                continue
            # callee resolution
            if tags.get("db.system") == "mysql":
                callee = MYSQL_NODE
                span_stat["client_db"] += 1
            elif "http.url" in tags:
                host = urlparse(tags["http.url"]).hostname
                callee = LONG2SHORT.get(host, host)
                span_stat["client_http"] += 1
            else:
                span_stat["client_unresolved"] += 1
                continue
            caller = LONG2SHORT.get(r.get("service"), r.get("service"))
            if callee not in nid:
                span_stat["client_callee_offgraph"] += 1
                continue
            obs_pairs.add((caller, callee))
            slot = ts2slot.get(r["timestamp"])
            if slot is None:                    # span not on a cAdvisor tick -> bin by epoch
                e = parse_ts(r["timestamp"])
                slot = nearest_slot(e, slot_epoch, slot_stage)
            if slot is None:
                span_stat["client_outside_window"] += 1
                continue
            src_nid = nid.get(caller, nid[callee])       # off-graph caller -> self-edge on callee
            inv[(slot, src_nid, nid[callee])].append(float(r["duration_ms"]))
            span_stat["client_binned"] += 1

    traces_json = collections.defaultdict(dict)
    for (slot, s, d), lats in inv.items():
        traces_json[str(slot)]["{}-{}".format(s, d)] = lats

    # ---- 3d. logs: werkzeug access lines -> Drain templates ---------------------------------
    log_rows = []                              # (slot:int, service, template)
    log_stat = collections.Counter()
    per_svc_kept = collections.Counter()
    for f in sorted(glob.glob(os.path.join(case_dir, "raw/logs/*.log"))):
        base = os.path.basename(f)[:-4]
        if "__" not in base:
            continue
        _stage, svc = base.split("__", 1)
        if svc not in nid:
            log_stat["svc_offgraph"] += 1
            continue
        for line in open(f, encoding="utf-8", errors="replace"):
            line = line.strip()
            if not line:
                continue
            m = LOG_RE.match(line) or NGINX_RE.match(line)
            if not m:
                # uvicorn/sasrec lines land here: no timestamp -> unbinnable (see format note above)
                log_stat["no_timestamp_dropped"] += 1
                log_stat["no_ts__" + svc] += 1
                continue
            slot = nearest_slot(parse_log_ts(m.group("ts")), slot_epoch, slot_stage)
            if slot is None:
                log_stat["outside_window"] += 1     # falls in a collapsed inter-stage gap
                continue
            log_rows.append((slot, svc, masker.add(m.group("rest"))))   # D4: integer slot
            log_stat["kept"] += 1
            per_svc_kept[svc] += 1

    # ---- 3e. label: root cause -> node id ---------------------------------------------------
    roots = gt.get("root_cause_services") or []
    root_nodes = [ROOT2NODE.get(r, r) for r in roots]
    fw = (gt.get("component_fault_windows") or {}).get("F1") or {}
    f_s, f_e = parse_ts(fw.get("start")), parse_ts(fw.get("end"))

    faults = []
    for rn in root_nodes:
        if rn not in nid:
            report["skipped_roots"].append({"case": name, "root": rn,
                                            "reason": "root not in node set (use --include-host?)"})
            continue
        if f_s is None or f_e is None:
            continue
        slots = [i for i in range(n_slots) if f_s <= slot_epoch[i] <= f_e]
        if not slots:
            report["skipped_roots"].append({"case": name, "root": rn,
                                            "reason": "fault window covers no observed slot"})
            continue
        faults.append({"s": slots[0], "e": slots[-1], "service": rn})

    return {
        "name": name, "n_slots": n_slots, "slot_stage": slot_stage,
        "metrics": metrics, "traces": dict(traces_json), "logs": log_rows,
        "faults": faults, "roots": root_nodes,
        "fault_type": (gt.get("fault_types") or [None])[0],
        "zfill": dict(zfill), "span_stat": dict(span_stat), "log_stat": dict(log_stat),
        "obs_pairs": obs_pairs,
        "log_channel_empty": sorted(n for n in nodes if per_svc_kept[n] == 0),
        "straddle": sum(1 for i in range(n_slots - 1) if slot_stage[i] != slot_stage[i + 1]),
    }


def nearest_slot(epoch, slot_epoch, slot_stage, tol=None):
    """Bin an event epoch to the slot whose cAdvisor tick covers it.  Slots are 2 s apart *within*
    a stage; across a collapsed stage boundary the distance is large, so we reject events that fall
    into the removed gap (they belong to no observed slot)."""
    if epoch is None:
        return None
    best, bd = None, None
    for i, se in enumerate(slot_epoch):
        d = abs(epoch - se)
        if bd is None or d < bd:
            best, bd = i, d
    if bd is None or bd > (tol or 1.5):        # 1.5 s > half of the 2 s cadence
        return None
    return best


# --------------------------------------------------------------------------------------------
# 4. emit parsed_data/<name>/
# --------------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="RecShop chaos cases -> Eadro parsed_data/")
    ap.add_argument("--dataset-root", default=None,
                    help="(deprecated) case discovery now goes through dataset_registry; "
                         "kept only so old command lines don't break. Ignored.")
    ap.add_argument("--arity", default="single", choices=["single", "dual", "triple", "all"],
                    help="`all` = single+dual+triple concatenated into ONE Eadro dataset "
                         "(batch idx is global; Eadro needs one corpus to train on)")
    ap.add_argument("--corpus", default="dense", choices=["dense", "all"],
                    help="dense = the 3 *_dense trees (140 case) = the historical corpus, "
                         "numbers stay comparable with every earlier Eadro run. "
                         "all = every active tree (195 case, i.e. + single_spread's 55). "
                         "★ Before 2026-07-13 this script hardcoded '{arity}_dense' and therefore "
                         "could NOT see single_spread at all -- silently, with no warning.")
    ap.add_argument("--family", default=None,
                    help="optional signal_class filter (root_local/propagation/off_graph/mixed), "
                         "computed per case from groundtruth.json. Unknown value -> hard error. "
                         "NOTE: family is NOT a way to reproduce the 140 -- propagation is only 25 "
                         "cases. Use --corpus dense for that.")
    ap.add_argument("--out", required=True, help="root that will contain parsed_data/<name>/")
    ap.add_argument("--name", default="R0", help="Eadro dataset name (parsed_data/<name>)")
    ap.add_argument("--include-host", action="store_true",
                    help="include the unlearnable `host` node + host_cpu case (D5)")
    ap.add_argument("--edges", default="union", choices=["union", "static", "trace"])
    ap.add_argument("--chunk-lenth", type=int, default=10)
    ap.add_argument("--build-chunks", action="store_true",
                    help="also run Eadro's own deal_* + chunking to produce chunk_{train,test}.pkl")
    ap.add_argument("--eadro-code", default=None, help="path to the PATCHED Eadro codes/ (for --build-chunks)")
    ap.add_argument("--test-ratio", type=float, default=0.3)
    args = ap.parse_args()

    nodes = build_nodes(args.include_host)
    nid = {s: i for i, s in enumerate(nodes)}
    report = {"deviations": ["D1 slot=2s", "D2 metric_num=16 cAdvisor cols (not Eadro's 7)",
                             "D3 batch=case, stages concatenated, gaps collapsed (z-score integrity)",
                             "D4 log ts quantized to slot (forced by deal_logs int filter)",
                             "D5 off-graph roots: db_lock->mysql, host_cpu->host",
                             "D6 trace channel reconstructed from client spans"],
              "skipped_roots": [], "cases": [], "node_names": nodes}

    arities = ["single", "dual", "triple"] if args.arity == "all" else [args.arity]
    report["arities"] = arities

    # ★ 2026-07-13:case 发现走 dataset_registry,不再 glob "{arity}_dense"。
    #   旧写法 src_root = dataset_root/"{}_dense".format(arity) 【物理上看不见 single_spread】
    #   —— 55 个 case 从来没进过 Eadro 语料,而且不 warn、不报错、跑完还给你一张漂亮的表。
    #   registry 是唯一真相源:加一棵树 = 改一处 json,这里自动跟上。
    tree_ids = DR.DENSE_TREES if args.corpus == "dense" else None
    case_dirs = []
    for arity in arities:
        for c in DR.cases(arity=arity, family=args.family, tree_ids=tree_ids):
            case_dirs.append(c["case_dir"])   # registry 已保证有 groundtruth.json + case_id 升序
    report["corpus"] = args.corpus
    report["family_filter"] = args.family
    report["n_cases_discovered"] = len(case_dirs)

    masker = Masker()
    report["log_template_backend"] = masker.backend

    cases = []
    for p in case_dirs:
        c = load_case(p, nodes, masker, report)
        if not args.include_host and HOST_NODE in c["roots"]:
            report["skipped_roots"].append({"case": c["name"], "root": HOST_NODE,
                                            "reason": "host node excluded by default (D5); pass --include-host"})
            continue
        cases.append(c)

    # templates.json must be global (deal_logs reads one file for all idx)
    templates = sorted({t for c in cases for (_, _, t) in c["logs"]})
    tmpl_idx = {t: i for i, t in enumerate(templates)}

    obs_pairs = set().union(*[c["obs_pairs"] for c in cases]) if cases else set()
    edges, edge_diff = build_edges(nodes, obs_pairs, args.edges)

    # Eadro's deal_* use RELATIVE paths from codes/preprocess/: "./parsed_data/<name>" and
    # "../chunks/<name>/<idx>".  We reproduce that layout exactly so its own code runs unmodified:
    #     <out>/work/parsed_data/<name>/...      (cwd for --build-chunks is <out>/work)
    #     <out>/chunks/<name>/...
    workdir = os.path.join(args.out, "work")
    pdir = os.path.join(workdir, "parsed_data", args.name)
    os.makedirs(pdir, exist_ok=True)
    written = []

    with open(os.path.join(pdir, "templates.json"), "w", encoding="utf-8") as fw:
        json.dump(templates, fw, indent=2, ensure_ascii=False)
    written.append("templates.json")

    for idx, c in enumerate(cases):
        si = str(idx)
        # records
        rec = {"faults": c["faults"], "start": 0, "end": c["n_slots"]}
        with open(os.path.join(pdir, "records{}.json".format(si)), "w", encoding="utf-8") as fw:
            json.dump(rec, fw, indent=2)
        # metrics<idx>/<svc>.csv
        mdir = os.path.join(pdir, "metrics{}".format(si))
        os.makedirs(mdir, exist_ok=True)
        for node in nodes:
            with open(os.path.join(mdir, node + ".csv"), "w", newline="", encoding="utf-8") as fw:
                w = csv.writer(fw)
                w.writerow(["timestamp"] + CADVISOR_METRICS)
                for slot, row in enumerate(c["metrics"][node]):
                    w.writerow([slot] + row)
        # traces<idx>.json
        with open(os.path.join(pdir, "traces{}.json".format(si)), "w", encoding="utf-8") as fw:
            json.dump(c["traces"], fw)
        # logs<idx>.csv
        with open(os.path.join(pdir, "logs{}.csv".format(si)), "w", newline="", encoding="utf-8") as fw:
            w = csv.writer(fw)
            w.writerow(["timestamp", "service", "event"])
            for slot, svc, tmpl in sorted(c["logs"]):
                w.writerow([slot, svc, tmpl])
        written += ["records{}.json".format(si), "metrics{}/*.csv".format(si),
                    "traces{}.json".format(si), "logs{}.csv".format(si)]

        n_chunks = max(0, c["n_slots"] - args.chunk_lenth + 1)
        faulty = sum(1 for s in range(n_chunks)
                     for f in c["faults"]
                     if not (s + args.chunk_lenth - 1 < f["s"] or s > f["e"]))
        report["cases"].append({
            "idx": idx, "case": c["name"], "fault_type": c["fault_type"],
            "roots": c["roots"], "root_nids": [nid[r] for r in c["roots"] if r in nid],
            "n_slots": c["n_slots"], "n_chunks": n_chunks, "faulty_chunks": faulty,
            "fault_slots": [[f["s"], f["e"]] for f in c["faults"]],
            "stage_boundaries_collapsed": c["straddle"],
            "metric_zero_filled_slots": c["zfill"],
            "spans": c["span_stat"], "logs": c["log_stat"],
            "log_channel_empty": c["log_channel_empty"],
        })

    # RecShop `Info` payload: Eadro's util.Info is hardcoded to TrainTicket/SocialNetwork, so the
    # patch layer must build its Info from this instead of guessing.
    info = {
        "node_num": len(nodes), "metric_num": len(CADVISOR_METRICS),
        "chunk_lenth": args.chunk_lenth, "event_num": len(templates) + 1,
        "edges": edges, "service_names": nodes, "metric_names": CADVISOR_METRICS,
        "service2nid": nid,
    }
    with open(os.path.join(pdir, "recshop_info.json"), "w", encoding="utf-8") as fw:
        json.dump(info, fw, indent=2)
    written.append("recshop_info.json")

    report["edges"] = {"mode": args.edges, "n_edges": len(edges[0]), "diff_static_vs_observed": edge_diff}
    report["totals"] = {
        "cases": len(cases),
        "node_num": len(nodes), "metric_num": len(CADVISOR_METRICS),
        "n_templates": len(templates),
        "chunks": sum(c["n_chunks"] for c in report["cases"]),
        "faulty_chunks": sum(c["faulty_chunks"] for c in report["cases"]),
        "client_spans_total": sum(c["spans"].get("kind_client", 0) for c in report["cases"]),
        "client_spans_binned": sum(c["spans"].get("client_binned", 0) for c in report["cases"]),
        "client_spans_unresolved": sum(c["spans"].get("client_unresolved", 0) for c in report["cases"]),
        "label_distribution": dict(collections.Counter(
            r for c in report["cases"] for r in c["roots"])),
    }
    with open(os.path.join(args.out, "adapter_report.json"), "w", encoding="utf-8") as fw:
        json.dump(report, fw, indent=2, ensure_ascii=False)

    print("[m9_eadro_adapter] parsed_data -> {}".format(pdir))
    print("  cases={cases}  node_num={node_num}  metric_num={metric_num}  templates={n_templates}"
          .format(**report["totals"]))
    print("  chunks={chunks} (faulty={faulty_chunks})  client_spans={client_spans_total} "
          "binned={client_spans_binned} unresolved={client_spans_unresolved}"
          .format(**report["totals"]))
    print("  labels: {}".format(report["totals"]["label_distribution"]))
    print("  report -> {}".format(os.path.join(args.out, "adapter_report.json")))

    if args.build_chunks:
        build_chunks(args, workdir, info, cases)


# --------------------------------------------------------------------------------------------
# 5. optional: drive Eadro's OWN deal_* functions (proves the format is byte-compatible)
# --------------------------------------------------------------------------------------------
def build_chunks(args, workdir, info, _CASES_EMITTED):
    """Run Eadro's OWN deal_traces/deal_metrics/deal_logs (from the PATCHED copy) over what we just
    wrote.  This is the real format check: if our files are wrong, its asserts fire."""
    import pickle
    import numpy as np
    if not args.eadro_code:
        raise SystemExit("--build-chunks requires --eadro-code <path to patched Eadro codes/>")
    sys.path.insert(0, os.path.join(args.eadro_code, "preprocess"))
    os.chdir(workdir)                        # deal_* read "./parsed_data", write "../chunks"
    import single_process as sp              # noqa  (this is Eadro's own, unmodified logic)

    # ! DO NOT `import align`: align.py runs argparse at MODULE level (align.py:167-176), so merely
    # importing it hijacks our own sys.argv and aborts.  get_basic/get_chunkid are pure helpers, so
    # we reproduce them verbatim (align.py:5-13 and :16-35) rather than patch upstream.
    import random
    import string
    _seen = set()

    def get_chunkid():
        while True:
            c = "".join(random.sample(string.ascii_letters + string.digits, 8))
            if c not in _seen:
                _seen.add(c)
                return c

    def get_basic(info, idx, name, chunk_lenth=10, threshold=1):
        with open(os.path.join("./parsed_data", name, "records" + idx + ".json")) as f:
            records = json.load(f)
        faults = [(r["s"], r["e"], info.service2nid[r["service"]]) for r in records["faults"]]
        start, end = records["start"], records["end"]
        intervals = [(s, s + chunk_lenth - 1) for s in range(start, end - chunk_lenth + 1)]
        labels = [-1] * len(intervals)
        for ci, (s, e) in enumerate(intervals):
            for (fs, fe, culprit) in faults:
                overlap = 0
                if fs <= s <= fe:
                    overlap = fe - s + 1
                elif fs <= e <= fe:
                    overlap = e - fs + 1
                if overlap >= threshold:
                    labels[ci] = culprit
                if overlap > 0:
                    break
        return intervals, labels

    class RecShopInfo:                       # duck-type of util.Info (which is hardcoded to TT/SN)
        pass
    I = RecShopInfo()
    I.service_names = info["service_names"]
    I.service2nid = info["service2nid"]
    I.node_num = info["node_num"]
    I.metric_names = info["metric_names"]
    I.edges = info["edges"]

    aim = os.path.join(os.path.dirname(workdir), "chunks", args.name)
    case_names = [c["name"] for c in _CASES_EMITTED]
    chunks = {}
    idx = 0
    while os.path.exists(os.path.join(workdir, "parsed_data", args.name,
                                      "records{}.json".format(idx))):
        si = str(idx)
        os.makedirs(os.path.join(aim, si), exist_ok=True)
        intervals, labels = get_basic(I, si, args.name, chunk_lenth=args.chunk_lenth)
        tr = sp.deal_traces(intervals, I, si, args.name, args.chunk_lenth)
        me = sp.deal_metrics(intervals, I, si, args.name, args.chunk_lenth)
        lo = sp.deal_logs(intervals, I, si, args.name)
        for k in range(len(intervals)):
            # traces: keep ONLY channel 0 (mean latency).  deal_traces allocates a trailing dim
            # of 2 (single_process.py:90) but only ever writes [..., 0] (single_process.py:106) --
            # channel 1 is dead all-zeros -- while TraceModel builds ConvNet(num_inputs=1).
            # Feeding 2 channels is a hard shape error.  align.py's get_chunks is patched (P6) to
            # slice this; THIS code path does not go through align.py, so it must slice too.
            chunks[get_chunkid()] = {"traces": tr["latency"][k][:, :, 0:1], "metrics": me[k],
                                     "logs": lo[k], "culprit": labels[k],
                                     # OUR FIX (see below): carry provenance so the split can be
                                     # done BY CASE.  Upstream align.py's split_chunks is patched
                                     # (M3) to do this, but THIS code path never calls align.py
                                     # (it cannot: align.py runs argparse at module level), so it
                                     # must not re-introduce the leak it is meant to avoid.
                                     "batch": idx, "case": case_names[idx]}
        idx += 1

    # ----------------------------------------------------------------------------------------
    # *** SPLIT ***  Chunks are a stride-1 sliding window of length `chunk_lenth` over ONE case,
    # so two neighbouring chunks of the same case share (chunk_lenth-1)/chunk_lenth = 90% of their
    # input.  Shuffling CHUNKS therefore puts near-duplicates of every test chunk into train --
    # that is upstream's align.py:135 (np.random.shuffle) and it leaks.  Default here: split by
    # CASE, stratified on the case's first root.  EADRO_SPLIT=chunk reproduces the leaky split so
    # the two can be reported side by side.
    # ----------------------------------------------------------------------------------------
    split_mode = os.environ.get("EADRO_SPLIT", "case")
    rng = np.random.RandomState(int(os.environ.get("EADRO_SPLIT_SEED", "42")))
    if split_mode == "chunk":
        keys = np.array(list(chunks.keys()))
        order = list(range(len(keys)))
        rng.shuffle(order)
        ntr = int((1 - args.test_ratio) * len(keys))
        tr_k, te_k = list(keys[order[:ntr]]), list(keys[order[ntr:]])
        test_cases = sorted({chunks[k]["case"] for k in te_k})
    else:
        by_batch = collections.defaultdict(list)
        for k, v in chunks.items():
            by_batch[v["batch"]].append(k)
        root_of = {}
        for b, ks in by_batch.items():
            labs = [chunks[k]["culprit"] for k in ks if chunks[k]["culprit"] != -1]
            root_of[b] = labs[0] if labs else -1
        strata = collections.defaultdict(list)
        for b in sorted(by_batch):
            strata[root_of[b]].append(b)
        te_b = []
        for r in sorted(strata):
            bs = list(strata[r])
            rng.shuffle(bs)
            n_te = int(round(args.test_ratio * len(bs)))
            if len(bs) >= 2:
                n_te = max(1, n_te)
            te_b += list(bs[:n_te])
        te_b = set(te_b)
        tr_k = [k for k, v in chunks.items() if v["batch"] not in te_b]
        te_k = [k for k, v in chunks.items() if v["batch"] in te_b]
        test_cases = sorted(case_names[b] for b in te_b)

    with open(os.path.join(aim, "chunk_train.pkl"), "wb") as fw:
        pickle.dump({k: chunks[k] for k in tr_k}, fw)
    with open(os.path.join(aim, "chunk_test.pkl"), "wb") as fw:
        pickle.dump({k: chunks[k] for k in te_k}, fw)
    with open(os.path.join(aim, "chunks_all.pkl"), "wb") as fw:
        pickle.dump(chunks, fw)                      # re-splittable without re-running deal_*
    md = dict(info)
    md["chunk_num"] = len(chunks)
    md["split_mode"] = split_mode
    md["split_seed"] = int(os.environ.get("EADRO_SPLIT_SEED", "42"))
    md["test_cases"] = test_cases
    with open(os.path.join(aim, "metadata.json"), "w", encoding="utf-8") as fw:
        json.dump(md, fw, indent=2)
    print("[m9_eadro_adapter] chunks -> {}  train={} test={}  split={} ({} test cases)"
          .format(aim, len(tr_k), len(te_k), split_mode, len(test_cases)))


if __name__ == "__main__":
    main()
