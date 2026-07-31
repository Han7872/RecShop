# -*- coding: utf-8 -*-
"""eval_k8s_trace -- K8S pilot TRACE-based propagation-aware RCA baseline (the T baseline).

M4b-5a: a THIRD channel after U=unsupervised metric ranking (eval_k8s_ranking.py) and
S=supervised (eval_k8s_supervised.py). It tests whether propagation structure in the
trace call-tree beats per-service metric ranking on the TRACED on-graph roots.

==================================================================================
WHAT THIS BASELINE DOES (headline T):
  For each scoring window (case x fault_window_membership bucket in
  {F1_only, F2_only, overlap}) -- iterated over the OTel trace JSONL spans -- and per
  TRACED service, compute three anomaly signals:
     error_rate   = fraction of that service's spans that are error spans
                    (http.status_code 5xx OR otel.status_code==ERROR OR error tag true)
     span_count z = robust-z of the service's span count vs the dataset median/MAD
                    (drop = negative spike; spike = positive)
     p95 dur z    = robust-z of the service's p95 duration_ms vs dataset median/MAD
  A service is ANOMALOUS if any of its signals crosses a robust-z threshold (ANOM_Z).

  Build the per-window call graph from parent_span_id edges (parent service -> child
  service). Compute the anomaly-ORIGIN score:
     * a service that is anomalous AND is the highest ancestor of an anomalous subtree
       (its own parent is NOT anomalous, but its descendants are) gets a high root score;
     * NETWORK-VS-SERVICE CREDIT: when a service's own server span is FLAT (not p95-z
       anomalous) but it is the CHILD of an anomalous cross-service edge whose parent
       (carrier/client) span IS p95-spiked, credit the child as the origin (the delay is
       on the network the client span covers, not in the child's own code). This is the
       exact trap the recon flagged (m2b1_single: pricing client span 1974ms vs catalog
       server span 4.6ms -> naive node ranking picks pricing (WRONG), the carrier-credit
       rule picks catalog (RIGHT)).

  WINDOWS WITH 0 CROSS-SERVICE EDGES (e.g. m2b2_podfail2 catalog pod killed -> only
  pricing spans remain; m3c1_dblock_* all same-service DB-client spans): the
  highest-ancestor rule has no graph to walk. Fall back to per-service self-anomaly
  (max of the 3 z-signals) with NO propagation credit -- degrades to a U-style ranking
  on trace signals alone.

  Rank all candidates by score (ties broken by candidate name ascending, stable).

  >>> GT/labels (root_cause_services etc.) are used ONLY to score the ranking AFTER it is
  >>> produced -- NEVER enter the scoring (GROUND-TRUTH-NOT-IN-X). group_id = fault_type.

==================================================================================
CANDIDATE SPACE (27, identical to U for direct comparison):
  25 BASE_SERVICES (per_service_canon.BASE_SERVICES) + host + mysql_items_lock.
  Only 4 services appear as OTel-instrumented trace nodes and are therefore scoreable:
     pricing_service->pricing, catalog_service->catalog, user_service->user,
     search_service->search.
  The other 23 candidates (21 uninstrumented BASE_SERVICES + host + mysql_items_lock)
  have NO trace node -> score 0 -> rank LAST -> Top@k MISS by construction.

  HONEST CAVEAT: catalog-gw (nginx) is a realized root token but is NOT OTel-instrumented
  -> it has NO trace node -> structurally UNTRACEABLE -> MISS. So the T baseline tests
  propagation on the TRACED on-graph roots (catalog, user); it does NOT rescue
  catalog-gw / host / mysql_items_lock. This is reported honestly.

  EXCLUSION: catalog_service_bad (the chaos-mesh-injected "bad catalog deployment",
  ~2007ms latency, present only in the 4 dual-root catalog-gw|catalog-gw cases) is NOT
  a GT root token (GT says catalog-gw) and is EXCLUDED from the candidate space entirely.
  Aliasing it to catalog would manufacture a false hit on a catalog-gw root.

==================================================================================
METRICS (mirror U baseline exactly; TOPK=3):
  per-root Top@1 / Top@3 : for each true-root token in the window, did it land in the
                           window's top-1 / top-3? (windows with N roots contribute N)
  Recall@root-set        : multi-root only. ALL true roots of the window appear in top-3.
  exact-set              : predicted top-(root_count) set == true-root set.
                           (None placeholders from all-zero scores never count as hits.)

  STRATIFIED reporting (mirrors U + the trace-relevant split):
    - by root tier: on_graph_traced (catalog/user) vs on_graph_untraced (catalog-gw +
      the 21 uninstrumented BASE_SERVICES folded) vs off_graph (host/mysql_items_lock)
      vs MIXED. For T the headline split is on_graph_traced vs on_graph_untraced: the
      former is what T CAN localize, the latter is structurally 0 (no node).
    - by interaction_pattern.

==================================================================================
HONESTY (written into results):
  - Off-graph (host/mysql) AND untraced-on-graph (catalog-gw) roots: Top@k ~= 0 by
    construction (no trace node). This is the FINDING, not a defect: trace propagation
    cannot localize roots that leave no OTel span. The honest comparison to U is ONLY
    on the on_graph_traced tier.
  - NETWORK-DELAY TRAP: on single-side network-delay roots (m2b1_single, m3a_reg_netdelay,
    net_loss), the root's own server span is flat and the carrier's client span is the
    anomaly. The carrier-credit rule recovers catalog where naive node ranking picks
    pricing (wrong). Single-side net roots are the regime where the trace T-baseline is
    most fragile; the carrier-credit rule is the honest mitigation (NOT a cheat toward
    catalog -- it fires only when the child's own server span is flat AND a cross-service
    parent edge is p95-spiked, which is exactly the network-delay signature).
  - m3d F1_only is trace-sparse (pod-kill: catalog has only 5 spans, user 6) -> its
    per-root Top@k carries that caveat.
  - N small (real=21 windows / 17 cases; all=39+ windows / 27 cases). Per-tier numbers
    are trends.

==================================================================================
DETERMINISM: no randomness (pure scoring+ranking); ties broken by candidate name asc.
ASCII-only prints (Windows GBK). conda python, offline. Does NOT git commit.

USAGE (Windows):
    python3 scripts/chaos/ctk/eval_k8s_trace.py
    # all-provenance secondary:
    ... eval_k8s_trace.py --all
"""
import argparse
import io
import os
import sys
from collections import defaultdict, Counter
from pathlib import Path

import numpy as np

# ---- reuse per_service_canon.BASE_SERVICES as the 25 on-graph definition space ----
HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
os.environ.setdefault("CHAOS_OUT_DIR", str(HERE))
from per_service_canon import BASE_SERVICES  # noqa: E402

# ---- paths ----
# ★ 2026-07-13: 数据根一律取自 dataset_registry(datasets/REGISTRY.json = 唯一真相源),
#   不再各脚本自己拼 "datasets"/"k8s_pilot"。--dataset-root/--pilot-dir 参数保留。
import dataset_registry as DR  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
PILOT_DIR = DR.NATIVE_ROOT

TOPK = 3
EPS = 1e-9

# Anomaly robust-z threshold (upper-tail magnitude). Robust-z >= ANOM_Z => anomalous.
# 3.0 = a conservative "3 robust-sigma" cut (median/MAD scale). Applied to the upper
# tail of |z| for span_count-drop, span_count-spike, p95-dur-spike, and error-rate-z.
ANOM_Z = 3.0

# ============================================================
# Candidate space (mirror U for direct comparison: 27 candidates).
#   25 BASE_SERVICES + host + mysql_items_lock = 27.
#   Only the 4 OTel-instrumented trace services are scoreable from traces.
# ============================================================
# The 4 services that ACTUALLY appear as trace nodes (recon-confirmed census of 6856
# spans). Map trace `service` field -> short candidate token.
TRACE_SVC_TO_SHORT = {
    "pricing_service": "pricing",
    "catalog_service": "catalog",
    "user_service": "user",
    "search_service": "search",
}
# Short tokens that map back to a BASE_SERVICE for the 25-base candidate ordering.
SHORT_TO_BASE = {
    "pricing": "pricing_service",
    "catalog": "catalog_service",
    "user": "user_service",
    "search": "search_service",
}
# trace services that are NOT candidates (excluded from scoring).
# catalog_service_bad = the chaos-mesh-injected bad-catalog deployment; GT root token is
# catalog-gw (untraceable), so aliasing it to catalog would be a false hit. EXCLUDE.
EXCLUDE_FROM_CANDIDATES = {"catalog_service_bad"}

OFF_GRAPH = ["host", "mysql_items_lock"]

# Candidate space = 25 BASE_SERVICES + host + mysql_items_lock (== U's 27).
CANDIDATES = list(BASE_SERVICES) + OFF_GRAPH   # 27 candidates
CAND_IDX = {c: i for i, c in enumerate(CANDIDATES)}

# Scoreable-from-traces short tokens (the 4 OTel-instrumented nodes).
TRACED_SHORT = set(TRACE_SVC_TO_SHORT.values())
# Candidate tokens that are traceable -> map back to base name in CANDIDATES.
TRACED_BASE = {SHORT_TO_BASE[s] for s in TRACED_SHORT}

# Carrier-credit edge-asymmetry trigger (within-window, scale-independent).
# A cross-service edge parent->child fires carrier-credit when the parent's p95 is at
# least CARRIER_EDGE_RATIO x the child's p95 AND the child's own server span is flat
# (not p95-z anomalous). This is the network-vs-service distinction (the recon's
# m2b1_single trap: pricing client 1974ms vs catalog server 4.6ms = ~430x) -- a
# propagation signal independent of the (heterogeneous, DB-lock-inflated) dataset MAD.
CARRIER_EDGE_RATIO = 10.0


def root_token_of(candidate: str):
    """Map a ranked candidate (BASE_SERVICE long name OR host/mysql_items_lock) to the
    short root-token form used by GT (catalog, user, ...). host/mysql_items_lock map to
    themselves. Uninstrumented BASE_SERVICES (no short token, never a realized root)
    map to None so they never accidentally hit a GT token."""
    if candidate in SHORT_TO_BASE.values():
        for short, base in SHORT_TO_BASE.items():
            if base == candidate:
                return short
    if candidate in OFF_GRAPH:
        return candidate
    return None

# Off-graph (no node anywhere).
OFF_GRAPH_CAND = set(OFF_GRAPH)

# realized root tokens that are on-graph but UNTRACED (no OTel node) -> structurally MISS.
# catalog-gw is nginx (not instrumented). The 21 uninstrumented BASE_SERVICES are folded
# into the same untraced tier (none are realized roots anyway).
ON_GRAPH_UNTRACED_TOKENS = {"catalog-gw"}


def root_tier(token: str) -> str:
    """Classify a true-root token into on_graph_traced / on_graph_untraced / off_graph.

    on_graph_traced   = the root token maps to an OTel-instrumented trace node (catalog,
                        user, pricing, search) -- T CAN localize it.
    on_graph_untraced = catalog-gw (nginx, not instrumented) or an uninstrumented
                        BASE_SERVICE -> structurally untraceable -> MISS.
    off_graph         = host / mysql_items_lock (shared resource, no node).
    """
    if token in OFF_GRAPH_CAND:
        return "off_graph"
    # a root token is traceable iff it is one of the 4 short tokens (catalog/user/...)
    if token in TRACED_SHORT:
        return "on_graph_traced"
    # everything else (catalog-gw, uninstrumented bases) -> untraced on-graph
    return "on_graph_untraced"


# ============================================================
# Trace loading
# ============================================================
def load_spans(case_dir: Path, stage: str):
    """Load spans for (case, stage) from JSONL. Returns list[dict]."""
    p = case_dir / "raw" / "traces" / f"{stage}_traces.jsonl"
    out = []
    if not p.exists():
        return out
    with open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(__import__("json").loads(line))
    return out


def span_is_error(span: dict) -> bool:
    """Error span = http.status_code 5xx OR otel.status_code==ERROR OR error tag true.
    (recon confirmed these three co-occur where present.)"""
    tags = span.get("tags") or []
    for t in tags:
        k = t.get("key")
        v = t.get("value")
        if k == "http.status_code" and isinstance(v, (int, float)) and 500 <= int(v) < 600:
            return True
        if k == "otel.status_code" and str(v).upper() == "ERROR":
            return True
        if k == "error" and v in (True, "true", "True", 1, "1"):
            return True
    return False


def span_is_5xx(span: dict) -> bool:
    """Explicit 5xx status-code flag (used for error_rate numerator mirror of recon)."""
    tags = span.get("tags") or []
    for t in tags:
        if t.get("key") == "http.status_code":
            v = t.get("value")
            if isinstance(v, (int, float)) and 500 <= int(v) < 600:
                return True
    return False


# ============================================================
# Per-(case, bucket) window feature extraction
# ============================================================
# scoring windows = (case, membership bucket) for bucket in {F1_only, F2_only, overlap}.
# (baseline bucket is NOT a fault window -> excluded from scoring, mirroring U which only
# scores fault windows via window_id in {F1_only,F2_only,overlap}.)
SCORE_BUCKETS = {"F1_only", "F2_only", "overlap"}


def p95(vals):
    if not vals:
        return float("nan")
    a = sorted(vals)
    k = (len(a) - 1) * 0.95
    f = int(k)
    c = min(f + 1, len(a) - 1)
    return a[f] + (a[c] - a[f]) * (k - f)


def window_features(spans):
    """From a list of spans belonging to ONE (case, bucket) window, extract per-service
    raw signals + the call graph.

    Returns dict:
        svc_signals: {short_token: {n_spans, n_errors, error_rate, p95_ms, durations[]}}
        edges: list[(parent_short, child_short)] cross-service edges (resolved parent)
        same_service_parent_frac: fraction of spans with a parent in the same window
    Untraced/excluded services are skipped (not scored).
    """
    by_id = {s["span_id"]: s for s in spans if s.get("span_id")}
    svc_durations = defaultdict(list)
    svc_err = defaultdict(int)
    svc_n = defaultdict(int)
    edges = []
    n_parent_resolved = 0
    n_parent_total = 0
    for s in spans:
        svc = s.get("service")
        if svc in EXCLUDE_FROM_CANDIDATES:
            continue
        if svc not in TRACE_SVC_TO_SHORT:
            continue  # unknown service (shouldn't happen post-census); skip
        short = TRACE_SVC_TO_SHORT[svc]
        svc_n[short] += 1
        svc_durations[short].append(float(s.get("duration_ms") or 0.0))
        if span_is_error(s):
            svc_err[short] += 1
        psid = s.get("parent_span_id")
        if psid:
            n_parent_total += 1
            p = by_id.get(psid)
            if p is not None:
                n_parent_resolved += 1
                psvc = p.get("service")
                if psvc in TRACE_SVC_TO_SHORT and psvc != svc:
                    edges.append((TRACE_SVC_TO_SHORT[psvc], short))
    svc_signals = {}
    for short, ds in svc_durations.items():
        n = svc_n[short]
        svc_signals[short] = {
            "n_spans": n,
            "n_errors": svc_err[short],
            "error_rate": (svc_err[short] / n) if n else 0.0,
            "p95_ms": p95(ds),
            "durations": ds,
        }
    same_parent_frac = (n_parent_resolved / n_parent_total) if n_parent_total else 0.0
    return {
        "svc_signals": svc_signals,
        "edges": edges,
        "n_parent_resolved": n_parent_resolved,
        "n_parent_total": n_parent_total,
        "same_parent_frac": same_parent_frac,
    }


# ============================================================
# Dataset-level robust-z baselines (median/MAD over ALL scoring windows).
# Mirrors U's robust_z_col: z = (x - median) / (1.4826 * MAD); MAD=0 -> std -> 1.
# ============================================================
def robust_z_array(arr):
    """Robust-z over a 1-D array (NaN-aware)."""
    a = np.asarray(arr, dtype=float)
    z = np.zeros(len(a))
    mask = ~np.isnan(a)
    if mask.sum() == 0:
        return z
    med = np.median(a[mask])
    mad = np.median(np.abs(a[mask] - med))
    scale = 1.4826 * mad
    if scale < EPS:
        sd = np.std(a[mask])
        scale = sd if sd > EPS else 1.0
    zc = (a - med) / scale
    zc[~mask] = 0.0
    return zc


def channel_max_z(lists_of_z):
    """Per-window MAX across several z-signal vectors (all same length = n_windows)."""
    if not lists_of_z:
        return np.zeros(0)
    return np.max(np.vstack(lists_of_z), axis=0)


# ============================================================
# Anomaly-origin scoring
# ============================================================
def score_windows(windows):
    """Given the list of scoring windows (each = dict from window_features + meta),
    compute the per-window, per-candidate score matrix [n_windows x 27].

    Step 1: build dataset-level robust-z baselines for the 3 signals per traced service.
    Step 2: per window, mark each traced service anomalous (any |z| >= ANOM_Z).
    Step 3: anomaly-origin: highest-ancestor-of-anomalous-subtree gets propagation credit.
    Step 4: network-vs-service carrier-credit (the network-delay trap fix).
    Step 5: fallback for 0-xsvc-edge windows (per-service self-anomaly, no propagation).
    """
    n = len(windows)
    # ---- Step 1: dataset-level z baselines (one vector per (service, signal)) ----
    # Collect raw signals per traced short token across windows.
    traced = sorted(TRACED_SHORT)
    raw = {s: {"span_count": [], "error_rate": [], "p95_ms": []} for s in traced}
    for w in windows:
        sig = w["svc_signals"]
        for s in traced:
            if s in sig:
                raw[s]["span_count"].append(float(sig[s]["n_spans"]))
                raw[s]["error_rate"].append(float(sig[s]["error_rate"]))
                raw[s]["p95_ms"].append(float(sig[s]["p95_ms"]))
            else:
                # service absent from this window -> span_count 0, error_rate 0, p95 NaN.
                raw[s]["span_count"].append(0.0)
                raw[s]["error_rate"].append(0.0)
                raw[s]["p95_ms"].append(float("nan"))

    # robust-z per (service, signal). For span_count we care about BOTH drop (negative)
    # and spike (positive) -> keep signed z; magnitude for anomaly test.
    z = {s: {} for s in traced}
    for s in traced:
        z[s]["span_count"] = robust_z_array(raw[s]["span_count"])
        z[s]["error_rate"] = robust_z_array(raw[s]["error_rate"])
        z[s]["p95_ms"] = robust_z_array(raw[s]["p95_ms"])

    # ---- Step 2+3+4+5: per-window scoring ----
    S = np.zeros((n, len(CANDIDATES)))
    meta_out = []
    for wi, w in enumerate(windows):
        sig = w["svc_signals"]
        edges = w["edges"]
        # per-service per-signal z at this window
        wz = {}
        for s in traced:
            wz[s] = {
                "span_count": z[s]["span_count"][wi],
                "error_rate": z[s]["error_rate"][wi],
                "p95_ms": z[s]["p95_ms"][wi],
            }
        # anomaly test: |z| >= ANOM_Z on any signal. Use p95 upper-tail (spike) for
        # the carrier-credit logic; error_rate upper-tail; span_count signed (drop<0 or
        # spike>0). A service is "anomalous" if any of these fires.
        anomalous = {}
        for s in traced:
            sc_z = wz[s]["span_count"]
            er_z = wz[s]["error_rate"]
            p95_z = wz[s]["p95_ms"]
            is_anom = (
                abs(sc_z) >= ANOM_Z or
                er_z >= ANOM_Z or
                p95_z >= ANOM_Z
            )
            anomalous[s] = {
                "is_anom": is_anom,
                "sc_z": sc_z,
                "er_z": er_z,
                "p95_z": p95_z,
                # self-anomaly magnitude = upper-tail-clipped max of the 3 signals
                "self_mag": max(0.0, abs(sc_z), er_z, p95_z),
                "present": s in sig,
            }

        # ---- Step 3: anomaly-origin (highest-ancestor of anomalous subtree) ----
        # Build adjacency: parent -> set(children) over cross-service edges.
        children = defaultdict(set)
        parents = defaultdict(set)
        for (psvc, csvc) in edges:
            children[psvc].add(csvc)
            parents[csvc].add(psvc)
        # reachable anomalous descendants via BFS down the cross-service tree.
        def anom_descendants(s):
            seen = set()
            stack = list(children.get(s, set()))
            while stack:
                x = stack.pop()
                if x in seen:
                    continue
                seen.add(x)
                stack.extend(children.get(x, set()))
            return seen

        # ---- Step 4: network-vs-service CARRIER CREDIT (the trap fix) ----
        # For each cross-service edge (parent->child) BOTH present in the window: if the
        # parent's p95 is at least CARRIER_EDGE_RATIO x the child's p95 AND the child's
        # OWN p95-z is FLAT (not dataset-anomalous), the delay lives on the network the
        # parent's client span covers, not in the child's code -> credit the child.
        # This uses the WITHIN-WINDOW edge asymmetry (scale-independent) rather than the
        # parent's dataset-z (which the DB-lock windows inflate -> net-delay spikes look
        # small in dataset-z but are unambiguous in edge-ratio). This is the propagation
        # signal the recon flagged for the m2b1_single trap (pricing client 1974ms vs
        # catalog server 4.6ms = ~430x); it is NOT a cheat toward catalog because it fires
        # ONLY on the flat-child + slow-parent edge signature, which is exactly the
        # network-vs-service distinction.
        carrier_credit = {}   # child_short -> max edge ratio credited
        for (psvc, csvc) in edges:
            if csvc not in sig or psvc not in sig:
                continue
            child_p95 = sig[csvc]["p95_ms"]
            parent_p95 = sig[psvc]["p95_ms"]
            if child_p95 is None or parent_p95 is None:
                continue
            if child_p95 <= 0:
                ratio = float("inf") if parent_p95 > 0 else 1.0
            else:
                ratio = parent_p95 / child_p95
            child_flat = anomalous.get(csvc, {}).get("p95_z", 0.0) < ANOM_Z
            # only credit when the child is NOT itself p95-anomalous (else its own spike
            # already explains it) and the edge shows the network-delay asymmetry.
            if ratio >= CARRIER_EDGE_RATIO and child_flat:
                carrier_credit[csvc] = max(carrier_credit.get(csvc, 0.0), ratio)

        # ---- assemble per-service root score ----
        # Origin score design (honest, not a cheat toward catalog):
        #   - propagation credit: a service gets credit if it is anomalous AND it is the
        #     highest ancestor of an anomalous subtree (its own parents are NOT anomalous,
        #     but it HAS anomalous descendants). This is the "origin" signature.
        #   - carrier-credit ADDS to a flat child that sits under a slow-parent cross-svc
        #     edge (network-delay root). Without this the net-delay trap picks the carrier.
        #   - if NO cross-service edges in the window (0-xsvc fallback): every service
        #     gets only its self-anomaly magnitude (no propagation, no carrier credit).
        has_xsvc = len(edges) > 0
        scores = {}
        for s in traced:
            a = anomalous[s]
            base = a["self_mag"]
            bonus = 0.0
            method = "none"
            if not a["present"]:
                scores[s] = 0.0
                continue
            if has_xsvc:
                # propagation credit: anomalous + highest ancestor of anomalous subtree.
                desc = anom_descendants(s)
                anom_desc = [d for d in desc if anomalous.get(d, {}).get("is_anom")]
                my_parents = parents.get(s, set())
                parent_anom = any(anomalous.get(p, {}).get("is_anom") for p in my_parents)
                if a["is_anom"] and anom_desc and not parent_anom:
                    # highest-ancestor-of-anomalous-subtree -> strong origin signal
                    bonus += 2.0
                    method = "origin"
                # carrier-credit (network-delay trap). Credit magnitude = the edge ratio
                # (logged) so a stronger asymmetry ranks higher; the child's own flat span
                # carries no z-magnitude, so the edge ratio IS the evidence.
                if s in carrier_credit:
                    bonus += float(np.log1p(carrier_credit[s]))  # log(1+ratio), monotone
                    method = "carrier_credit" if method == "none" else method + "+carrier"
                # plain anomalous node (anomaly present but not an origin): keep self_mag.
                if bonus == 0.0 and a["is_anom"]:
                    method = "self_anom"
            else:
                # ---- Step 5: 0-xsvc fallback -> self-anomaly only, no propagation ----
                if a["is_anom"]:
                    method = "fallback_self"
                # base self_mag stands; no bonus.
            scores[s] = base + bonus if (a["is_anom"] or bonus > 0) else 0.0
            # stash method for transparency
            anomalous[s]["method"] = method

        # write into the 27-candidate matrix at the matching BASE_SERVICE index.
        # only the 4 traced services get nonzero scores; the other 23 stay 0.
        for s in traced:
            base_name = SHORT_TO_BASE[s]
            if base_name in CAND_IDX:
                S[wi, CAND_IDX[base_name]] = scores.get(s, 0.0)

        meta_out.append({
            "wi": wi,
            "anomalous": {s: anomalous[s] for s in traced},
            "has_xsvc": has_xsvc,
            "edges": edges,
            "scores": scores,
        })

    return S, meta_out


# ============================================================
# Ranking + evaluation (mirrors U)
# ============================================================
def rank_topk(score_row, k, credit_only_positive=True):
    """Rank by score DESC; ties broken by candidate name ASC (stable). Zero/negative ->
    None placeholder so an all-zero ranking never manufactures fake recall.

    Returns the ranked candidates as ROOT TOKENS (catalog, user, host, ...) -- the same
    vocabulary GT uses -- so the comparison `true_root in top3` is direct. Candidates
    with no root-token form (the 21 uninstrumented BASE_SERVICES) map to None and are
    carried as None placeholders (they can never hit a GT token by construction)."""
    order = sorted(range(len(CANDIDATES)), key=lambda i: (-score_row[i], CANDIDATES[i]))
    out = []
    for i in order[:k]:
        if credit_only_positive and score_row[i] <= 0:
            out.append(None)
        else:
            # map to root-token form for GT comparison; None if the candidate has no
            # root-token (uninstrumented BASE_SERVICE -- never a realized root).
            out.append(root_token_of(CANDIDATES[i]))
    return out


def short_token_for_candidate(cand: str):
    """Map a candidate BASE name back to its trace short token (or None)."""
    for short, base in SHORT_TO_BASE.items():
        if base == cand:
            return short
    return None


def parse_roots(val) -> list:
    s = "" if val is None else str(val).strip()
    if s == "":
        return []
    return [t for t in s.split("|") if t != ""]


def evaluate(windows_meta, S):
    """Per-window ranking -> per-root Top@1/Top@3, Recall@root-set, exact-set, stratified."""
    n = len(windows_meta)
    root_top1 = {}
    root_top3 = {}
    n_roots_judged = 0
    sum_top1 = 0
    sum_top3 = 0
    n_multi = 0
    recall_set_hits = 0
    exact_set_hits = 0
    # tier-stratified per-root hit accumulators
    tier_top1 = {"on_graph_traced": [0, 0], "on_graph_untraced": [0, 0],
                 "off_graph": [0, 0], "mixed": [0, 0]}
    tier_top3 = {"on_graph_traced": [0, 0], "on_graph_untraced": [0, 0],
                 "off_graph": [0, 0], "mixed": [0, 0]}
    pat_top1 = {}
    pat_top3 = {}
    per_row = []

    for w in range(n):
        top1 = rank_topk(S[w], 1)
        top3 = rank_topk(S[w], TOPK)
        roots = windows_meta[w]["roots"]
        n_root = len(roots)
        tiers = {root_tier(r) for r in roots}
        # mixed = a row whose root_set spans traced + untraced/off
        traced_present = "on_graph_traced" in tiers
        untraced_present = ("on_graph_untraced" in tiers) or ("off_graph" in tiers)
        row_tier = "mixed" if (traced_present and untraced_present) else (
            "on_graph_traced" if tiers == {"on_graph_traced"} else (
                "on_graph_untraced" if "on_graph_untraced" in tiers and not traced_present else (
                    "off_graph" if tiers == {"off_graph"} else "mixed")))
        interaction = str(windows_meta[w]["interaction"])
        is_multi = n_root >= 2

        row_top1_hits = 0
        row_top3_hits = 0
        for tr in roots:
            n_roots_judged += 1
            d1 = root_top1.setdefault(tr, [0, 0])
            d3 = root_top3.setdefault(tr, [0, 0])
            d1[1] += 1
            d3[1] += 1
            h1 = int(tr == top1[0])
            h3 = int(tr in top3)
            d1[0] += h1
            d3[0] += h3
            sum_top1 += h1
            sum_top3 += h3
            row_top1_hits += h1
            row_top3_hits += h3
            t = root_tier(tr)
            tier_top1[t][0] += h1
            tier_top1[t][1] += 1
            tier_top3[t][0] += h3
            tier_top3[t][1] += 1
            if row_tier == "mixed":
                tier_top1["mixed"][0] += h1
                tier_top1["mixed"][1] += 1
                tier_top3["mixed"][0] += h3
                tier_top3["mixed"][1] += 1
            p1 = pat_top1.setdefault(interaction, [0, 0])
            p3 = pat_top3.setdefault(interaction, [0, 0])
            p1[0] += h1
            p1[1] += 1
            p3[0] += h3
            p3[1] += 1

        if is_multi:
            n_multi += 1
            top3_set = set(r for r in top3 if r is not None)
            true_set = set(roots)
            recall_set_hits += int(true_set.issubset(top3_set))
            topn = rank_topk(S[w], n_root)
            pred_set = set(r for r in topn if r is not None)
            exact_set_hits += int(pred_set == true_set)

        per_row.append({
            "case_id": windows_meta[w]["case_id"],
            "bucket": windows_meta[w]["bucket"],
            "group_id": windows_meta[w]["group_id"],
            "provenance": windows_meta[w]["provenance"],
            "roots": roots,
            "row_tier": row_tier,
            "interaction": interaction,
            "top1": top1[0],
            "top3": [r for r in top3],
            "n_root": n_root,
            "top1_hits": row_top1_hits,
            "top3_hits": row_top3_hits,
            "has_xsvc": windows_meta[w]["has_xsvc"],
        })

    return {
        "n_rows": n,
        "n_roots_judged": n_roots_judged,
        "top1_rate": (sum_top1 / n_roots_judged) if n_roots_judged else 0.0,
        "top3_rate": (sum_top3 / n_roots_judged) if n_roots_judged else 0.0,
        "root_top1": root_top1,
        "root_top3": root_top3,
        "n_multi": n_multi,
        "recall_set": (recall_set_hits / n_multi) if n_multi else 0.0,
        "exact_set": (exact_set_hits / n_multi) if n_multi else 0.0,
        "tier_top1": tier_top1,
        "tier_top3": tier_top3,
        "pat_top1": pat_top1,
        "pat_top3": pat_top3,
        "per_row": per_row,
    }


# ============================================================
# Case enumeration + window assembly
# ============================================================
def discover_case_dirs(pilot_dir):
    """Recursive case discovery (mirrors make_k8s_feature_view.py L415-439).

    A "case dir" = a dir containing BOTH metadata.json AND
    raw/traces/during_fault_traces.jsonl, found at ANY depth under pilot_dir.
    Supports the nested cases/<root_cause_primary>/<case_id>/ layout introduced in
    commit 32b7ab8 (not just the pre-existing flat pilot_dir/<case_id>/ layout).

    The <case>/mr2/ subdir has metadata.json but NO raw/traces/during_fault_traces.jsonl
    -> the AND-filter excludes it (mr2/ must NOT be picked up as a case).

    Returns a dict {case_id: Path(case_dir)}; case_id = basename of the case dir
    (path-independent, matching make_k8s_feature_view). Duplicate basenames across
    depths are kept first-seen (logged to stderr) so case_id keying stays unique.
    """
    seen_ids = set()
    out = {}
    for dirpath, _dirnames, filenames in os.walk(str(pilot_dir)):
        if "metadata.json" not in filenames:
            continue
        case_dir = Path(dirpath)
        trace_path = case_dir / "raw" / "traces" / "during_fault_traces.jsonl"
        if not trace_path.exists():
            continue  # e.g. <case>/mr2/ : metadata.json present but no traces -> not a case
        case_id = os.path.basename(dirpath)
        if case_id in seen_ids:
            print(f"[discover_case_dirs] duplicate case_id skipped (already seen): "
                  f"{case_id} @ {case_dir}", file=sys.stderr)
            continue
        seen_ids.add(case_id)
        out[case_id] = case_dir
    return out


def load_case_windows(case_ids, pilot_dir=PILOT_DIR, case_dirs=None):
    """For each case, read metadata.json (GT/labels + config) + during_fault traces.
    Build one scoring window per (case, membership bucket in SCORE_BUCKETS).

    GT/labels are attached to the window meta but used ONLY for scoring AFTER ranking.

    Path resolution (nested-layout fix):
      - If case_dirs (dict {case_id: Path}) is provided, the case dir is taken from it
        (the recursive discovery result). This is REQUIRED for the post-32b7ab8 nested
        layout cases/<root>/<case>/ -- a flat pilot_dir/<cid> lookup misses them.
      - If case_dirs is None, falls back to pilot_dir / cid (flat-layout back-compat
        ONLY; deprecated for the current dataset, kept so old callers still work).
    main() always passes case_dirs=discover_case_dirs(pilot_dir); never rely on the
    flat fallback for the live k8s_pilot tree.
    """
    import json
    windows = []
    for cid in case_ids:
        if case_dirs is not None:
            if cid not in case_dirs:
                print(f"[skip] {cid}: not in discovered case_dirs")
                continue
            cdir = case_dirs[cid]
        else:
            cdir = pilot_dir / cid
        mpath = cdir / "metadata.json"
        if not mpath.exists():
            print(f"[skip] {cid}: no metadata.json")
            continue
        meta = json.load(open(mpath, encoding="utf-8"))
        gt = meta.get("ground_truth") or {}
        roots = list(gt.get("root_cause_services") or [])
        if not roots:
            # fallback: root_causes list
            rcs = meta.get("root_causes") or []
            roots = [r.get("service") for r in rcs if r.get("service")]
        interaction = gt.get("interaction_pattern") or meta.get("interaction_pattern") or ""
        group_id = (meta.get("config") or {}).get("fault") or meta.get("group_id") or ""
        provenance = meta.get("provenance") or _infer_provenance(cid)
        spans = load_spans(cdir, "during_fault")
        # group spans by membership bucket
        by_bucket = defaultdict(list)
        for s in spans:
            b = s.get("fault_window_membership")
            if b in SCORE_BUCKETS:
                by_bucket[b].append(s)
        # scoring window ordering: F1_only, F2_only, overlap (stable)
        for b in ["F1_only", "F2_only", "overlap"]:
            if b not in by_bucket:
                continue
            feats = window_features(by_bucket[b])
            windows.append({
                "case_id": cid,
                "bucket": b,
                "group_id": group_id,
                "provenance": provenance,
                "roots": roots,
                "interaction": interaction,
                "svc_signals": feats["svc_signals"],
                "edges": feats["edges"],
                "n_parent_resolved": feats["n_parent_resolved"],
                "n_parent_total": feats["n_parent_total"],
                "same_parent_frac": feats["same_parent_frac"],
                "has_xsvc": len(feats["edges"]) > 0,
            })
    return windows


# provenance classification mirroring features_k8s_all.csv (real-only is the headline).
# prefix-based (the dataset's provenance token is not stored in metadata.json, so we
# infer from the case_id naming convention used by the pilot).
_REG_CASES = {"m3a_reg_netdelay", "m3b1_reg_svccpu", "reg_m1", "reg_m2a", "reg_m2a_v2"}
_SMOKE_CASES = {"smoke_m1_dual02", "smoke_m1_dual03", "smoke_m1_dual04"}
_FIX_CASES = {"fix01", "fix02"}


def _infer_provenance(cid):
    if cid in _REG_CASES:
        return "reg"
    if cid in _SMOKE_CASES:
        return "smoke"
    if cid in _FIX_CASES:
        return "fix"
    return "real"


# ============================================================
# Self-check (GROUND-TRUTH-NOT-IN-X, candidate coverage, exclusion)
# ============================================================
def selfcheck(windows, label):
    print(f"[selfcheck:{label}] windows={len(windows)} candidates={len(CANDIDATES)} "
          f"(traced={len(TRACED_SHORT)} off_graph={len(OFF_GRAPH)})")
    # GT/labels must NEVER enter the per-service signals (signals derive from spans only).
    GT_KEYS = {"root_cause_services", "root_cause_primary", "root_cause_set", "fault_type",
               "interaction_pattern", "path_relation", "answer_type", "root_count",
               "affected_services", "group_id", "case_id", "provenance",
               "component_ground_truth", "root_causes", "ground_truth"}
    leaked = set()
    for w in windows:
        for k in w["svc_signals"]:
            if k in GT_KEYS:
                leaked.add(k)
    assert not leaked, f"GROUND-TRUTH token leaked into svc_signals keys: {leaked}"
    print(f"[selfcheck:{label}] zero GT-in-signals OK (signals derive from spans only)")
    # off-graph candidates never appear as trace nodes.
    off_in_signals = set()
    for w in windows:
        for short in w["svc_signals"]:
            if short in OFF_GRAPH:
                off_in_signals.add(short)
    assert not off_in_signals, f"off-graph candidate appeared as a trace node: {off_in_signals}"
    print(f"[selfcheck:{label}] off-graph {OFF_GRAPH} never appear as trace nodes OK")
    # excluded services never enter candidate scoring.
    for w in windows:
        # edges may reference excluded svc? they shouldn't (window_features skips them).
        for (p, c) in w["edges"]:
            assert p in TRACED_SHORT, f"excluded/unknown parent in edge: {p}"
            assert c in TRACED_SHORT, f"excluded/unknown child in edge: {c}"
    print(f"[selfcheck:{label}] all edges resolved to traced candidates OK "
          f"(catalog_service_bad EXCLUDED)")
    # true-root tokens: assert each is either traceable, off-graph, or untraced-on-graph.
    all_roots = set()
    for w in windows:
        all_roots |= set(w["roots"])
    unknown = all_roots - TRACED_SHORT - OFF_GRAPH_CAND - ON_GRAPH_UNTRACED_TOKENS
    # uninstrumented BASE_SERVICES would be untraced-on-graph by root_tier(); flag if any.
    extra_bases = all_roots - TRACED_SHORT - OFF_GRAPH_CAND - ON_GRAPH_UNTRACED_TOKENS
    extra_bases = {t for t in extra_bases if t in set(BASE_SERVICES)}
    if extra_bases:
        print(f"[selfcheck:{label}] NOTE: true-root tokens = uninstrumented BASE_SERVICES "
              f"(untraced on-graph, will MISS): {sorted(extra_bases)}")
    print(f"[selfcheck:{label}] true-root token tier census: "
          f"traced={sorted(t for t in all_roots if root_tier(t)=='on_graph_traced')} "
          f"untraced={sorted(t for t in all_roots if root_tier(t)=='on_graph_untraced')} "
          f"off_graph={sorted(t for t in all_roots if root_tier(t)=='off_graph')}")


# ============================================================
# Console reporting
# ============================================================
def _rate(num, den):
    return (num / den) if den else float("nan")


def _fmt(x):
    return "  N/A " if (x != x) else f"{x:6.3f}"


def report(label, windows, S, res, fout, meta_out=None):
    def P(*a):
        print(*a)
        if fout is not None:
            fout.write(" ".join(str(s) for s in a) + "\n")

    P(f"==================== {label} ====================")
    P(f"windows={res['n_windows']}  roots_judged={res['n_roots_judged']}  "
      f"multi_root_windows={res['n_multi']}")
    P("")
    P("=== Overall per-root ranking (trace T-baseline) ===")
    P(f"  Top@1 = {_fmt(res['top1_rate'])}   Top@3 = {_fmt(res['top3_rate'])}")
    P(f"  Recall@root-set (multi-root, all roots in Top@3) = {_fmt(res['recall_set'])}   "
      f"[n_multi={res['n_multi']}]")
    P(f"  exact-set  (top-root_count set == true set)      = {_fmt(res['exact_set'])}   "
      f"[n_multi={res['n_multi']}]")
    P("")
    P("=== Per-root-token Recall@1 / Recall@3 ===")
    P(f"  {'root':<22} {'tier':<18} {'support':>7} {'R@1':>7} {'R@3':>7}")
    seen = set(res["root_top1"].keys()) | set(res["root_top3"].keys())
    # stable order: traced first, then untraced-on-graph, then off-graph
    order_pref = (sorted(t for t in seen if root_tier(t) == "on_graph_traced")
                  + sorted(t for t in seen if root_tier(t) == "on_graph_untraced")
                  + sorted(t for t in seen if root_tier(t) == "off_graph"))
    for svc in order_pref:
        t1 = res["root_top1"].get(svc, [0, 0])
        t3 = res["root_top3"].get(svc, [0, 0])
        sup = t1[1] or t3[1]
        P(f"  {svc:<22} {root_tier(svc):<18} {sup:>7} "
          f"{_fmt(_rate(t1[0], t1[1])):>7} {_fmt(_rate(t3[0], t3[1])):>7}")
    P("")
    P("=== Stratified by root TIER (per-root judgements) ===")
    P(f"  {'tier':<22} {'support':>7} {'R@1':>7} {'R@3':>7}")
    for tier in ["on_graph_traced", "on_graph_untraced", "off_graph", "mixed"]:
        t1 = res["tier_top1"][tier]
        t3 = res["tier_top3"][tier]
        sup = t1[1] or t3[1]
        P(f"  {tier:<22} {sup:>7} {_fmt(_rate(t1[0], t1[1])):>7} "
          f"{_fmt(_rate(t3[0], t3[1])):>7}")
    P("  (on_graph_traced = catalog/user -> the ONLY tier T can localize from traces.)")
    P("  (on_graph_untraced = catalog-gw (nginx, not OTel-instrumented) + any uninstrumented")
    P("   BASE_SERVICE -> structurally MISS. off_graph = host/mysql -> MISS.)")
    P("  (mixed = a window whose root_set spans traced AND untraced/off; reported as an")
    P("   OVERLAP view -- its root-judgements are ALREADY counted in the native tiers.)")
    P("")
    P("=== Stratified by interaction_pattern (per-root judgements) ===")
    P(f"  {'pattern':<24} {'support':>7} {'R@1':>7} {'R@3':>7}")
    pat_order = ["single_root", "trigger_amplifier", "fault_masking",
                 "independent_parallel"]
    pat_seen = set(res["pat_top1"].keys()) | set(res["pat_top3"].keys())
    for p in pat_order + sorted(pat_seen - set(pat_order)):
        t1 = res["pat_top1"].get(p, [0, 0])
        t3 = res["pat_top3"].get(p, [0, 0])
        sup = t1[1] or t3[1]
        P(f"  {p:<24} {sup:>7} {_fmt(_rate(t1[0], t1[1])):>7} "
          f"{_fmt(_rate(t3[0], t3[1])):>7}")
    P("")
    P("=== Per-window ranking detail (top-1 | top-3 | true roots | xsvc?) ===")
    P(f"  {'case_id':<22} {'bucket':<9} {'tier':<18} {'n':>2} xsvc  top1={'':<16} "
      f"top3=true_roots")
    for r in res["per_row"]:
        top1 = r["top1"] if r["top1"] is not None else "(none)"
        top3 = ",".join((t if t is not None else "(none)") for t in r["top3"])
        roots = "|".join(r["roots"])
        xs = "Y" if r["has_xsvc"] else "n"
        P(f"  {r['case_id']:<22} {r['bucket']:<9} {r['row_tier']:<18} {r['n_root']:>2}  {xs}   "
          f"{top1:<18} {top3}  <= {roots}")
    P("")
    # network-delay trap transparency: which windows used carrier_credit vs origin vs fallback
    if meta_out is not None:
        P("=== Scoring-method census (transparency for the network-delay trap) ===")
        method_counts = Counter()
        for mo in meta_out:
            for s in sorted(TRACED_SHORT):
                m = mo["anomalous"].get(s, {}).get("method", "absent")
                if mo["anomalous"].get(s, {}).get("present", False):
                    method_counts[m] += 1
        for m, c in sorted(method_counts.items(), key=lambda kv: (-kv[1], kv[0])):
            P(f"  {m:<20} {c}")
        P("  (origin = anomalous + highest ancestor of anomalous subtree [propagation])")
        P("  (carrier_credit = child's own span flat but parent cross-svc edge p95-spiked")
        P("   -> network-delay root, credit the child [the m2b1_single trap fix])")
        P("  (self_anom / fallback_self = anomalous node, no propagation credit)")
        P("")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true",
                    help="also run the all-provenance case set (secondary) after real-only.")
    ap.add_argument("--pilot-dir", default=str(PILOT_DIR),
                    help="dataset root to scan for case dirs "
                         "(default: <output-root>/k8s_pilot; "
                         "may point at k8s_pilot/{single,dual,triple}).")
    ap.add_argument("--out", default=None,
                    help="optional text dump path (ASCII); default: stdout only")
    args = ap.parse_args()

    pilot_dir = Path(args.pilot_dir)

    fout = None
    if args.out:
        # ★ 2026-07-13:本脚本【读】native case 目录是合法的(它只吃 raw traces,不吃 features_k8s.csv),
        #   但 --out 是派生物 —— 不许落进 native。默认落 (runtime) scores/。
        DR.assert_not_native(args.out)
        fout = open(args.out, "w", encoding="ascii", errors="replace")

    # ---- enumerate case dirs that have traces (recursive, nested-layout aware) ----
    # Cases live at cases/<root_cause_primary>/<case_id>/ since commit 32b7ab8; a flat
    # pilot_dir.iterdir() finds 0 cases there. discover_case_dirs walks the tree and
    # requires BOTH metadata.json AND raw/traces/during_fault_traces.jsonl (AND-filter
    # excludes <case>/mr2/ which has metadata.json but no traces).
    case_dirs = discover_case_dirs(pilot_dir)
    all_case_ids = sorted(case_dirs.keys())
    real_case_ids = [c for c in all_case_ids if _infer_provenance(c) == "real"]

    def run(label, case_ids):
        windows = load_case_windows(case_ids, pilot_dir=pilot_dir, case_dirs=case_dirs)
        selfcheck(windows, label=label)
        S, meta_out = score_windows(windows)
        res = evaluate(windows, S)
        res["n_windows"] = len(windows)
        report(label, windows, S, res, fout, meta_out=meta_out)
        return res

    res_real = run(f"PRIMARY (real-only, {len(real_case_ids)} cases)", real_case_ids)

    if args.all:
        res_all = run(f"SECONDARY (all-provenance, {len(all_case_ids)} cases)",
                      all_case_ids)

    if fout is not None:
        fout.close()
        print(f"[written] text dump -> {fout.name}")


if __name__ == "__main__":
    # ASCII-only stdout on Windows GBK; non-ASCII -> '?'
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="ascii", errors="replace")
    main()
