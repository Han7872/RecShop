#!/usr/bin/env python
"""package_for_delivery.py - package RecWeb2 k8s_pilot cases into shijie's
per-case delivery layout (M4b Phase 5, delivery packaging).

PURPOSE
  Hand RecWeb2 k8s_pilot RCA benchmark to shijie. Her format (authoritative:
  docs/blackboard/REF-shijie-k8s-v2-sample-schema.md) = per-case flat folder
  + raw/{metrics,traces,logs,operations} + metadata.json + groundtruth.json +
  summary.md. Our raw/ modality structure + top-level files are ALREADY aligned
  (M3-M4b rebuild). The schema delta is resolved by adapter mr2_load_adapter.py
  (record source->prometheus + quality=observed + container + unit normalize;
  metadata v1.2 + observation_stages object + root_causes string-list +
  component_fault_windows start_time/end_time + validation passed +
  quality.json stages). This script assembles the final delivery tree.

WHAT IT DOES (per case in <pilot-dir>/cases/*/*/):
  1. Run adapter (reuse mr2_load_adapter.adapt_case) into
     datasets/_runtime/package/<arity>/<case_id>/  -- NOT into <case>/mr2/.
     ★ ALWAYS regenerated (2026-07-13). The old "reuse mr2/ if it exists unless --force"
       shortcut is how 15 stale-GT cases got copied into a delivery package: the adapter
       output was cached, the GT behind it had been fixed, nobody re-ran it. Regenerating
       one case costs ~19MB / 30k lines. Correctness wins.
  2. Derive shijie-style folder name:
       mr{root_count}_{cat_short}_{class_short}-{fault_type}-k8s-v2-formal-r1-{ts}
     fallback = case's sample_id/formal_slot_id (NEVER empty/dup; collision -> _r2).
  3. Assemble flat per-case folder <out>/<folder-name>/:
       metadata.json            <- mr2/metadata.json  (adapter-transformed)
       groundtruth.json         <- mr2/groundtruth.json (adapter-normalized to 上游
                                    13-key top + 10-key component_ground_truth +
                                    start_time/end_time windows; falls back to native
                                    groundtruth.json if adapter produced none)
       summary.md               <- GENERATED 上游 6-bullet format (derived from
                                    metadata + metrics_v2/logs + validation gate)
       raw/metrics/metrics_v2.jsonl <- mr2/metrics_v2.jsonl (transformed)
       raw/metrics/quality.json     <- mr2/quality.json    (transformed)
       raw/metrics/manifest.json    <- mr2/manifest.json (adapter-normalized to 上游
                                    metrics-manifest.v2.1 shape; falls back to native
                                    raw/metrics/manifest.json if adapter produced none)
       raw/traces/                  <- native raw/traces/ (verbatim, all files)
       raw/logs/                    <- native raw/logs/   (verbatim)
       raw/operations/              <- native raw/operations/ (verbatim)
       scripts/collect-<sample_id>.ps1 <- reconstructed chaos_k8s_runner.py command
                                    (reconstructed from metadata.config)
     (NO mr2/ subdir - merged into raw/.)
  4. Top-level in <out>/ (SKIPPED under --bare):
       _delivery_README.md    (delivery notes for shijie)
       adapter/mr2_load_adapter.py + adapter/README.md  (for her to re-run/verify)
       MANIFEST.json          (machine-readable case index + mapping)
     (--bare: skip all three top-level; per-case folders lay flat under <out>/.)

CONSTRAINTS (IRON)
  - READ-ONLY on datasets/k8s_pilot/ source (never mutate). Enforced, not merely
    promised: dataset_registry.assert_not_native() gates both --out and the adapter
    runtime dir. --keep-source additionally skips the adapter write entirely
    (runtime dir must already be populated, else that case errors).
  - Reuse mr2_load_adapter transform logic (do NOT reimplement).
  - utf-8 for JSON/MD (Chinese bytes); idempotent (rerun overwrites same content).
  - ASCII-only stdout (Windows GBK); py_compile clean.
  - Name-derivation failure falls back to case_id; NEVER empty/duplicate names.

ADDITIVE EXTRAS (2026-07-13, all default OFF -> flagless run is BYTE-IDENTICAL to
the accepted 20260709 deliveries; she has already written a loader against those)
  --with-calltree     -> NEW raw/traces_calltree/ (native call-tree: non-null
                         parent_span_id + CHILD_OF references + README +
                         calltree_stats.json). raw/traces/ (flat) untouched.
                         Also flips traces manifest + trace_profile
                         cross_service_span_graph_available to true (honest now).
  --with-eval         -> NEW eval/ (data.csv wide table + inject_time.txt +
                         build_info.json + README.md; m9_adapter.build_wide,
                         gap_aware, full column universe).
  --with-gt-distinct  -> NEW groundtruth key n_distinct_root_services (= |G|).
  NOTHING existing is renamed, dropped, or re-valued by any of the three.

USAGE
  python package_for_delivery.py                      # combined -> default out
  python package_for_delivery.py --pilot-dir datasets/k8s_pilot/single \
      --out datasets/k8s_pilot_delivery/single
  python package_for_delivery.py --limit 3            # smoke (3 cases)
  python package_for_delivery.py --dry-run            # plan only, no writes
"""
import argparse
import csv
import datetime as _dt
import json
import re
import shutil
import sys
from pathlib import Path

# Reuse adapter (same dir on sys.path when run as script / imported).
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
import mr2_load_adapter as adapter  # noqa: E402
import dataset_registry as DR  # noqa: E402  (派生物落盘位置 + native 写入闸)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Delivery tag (matches shijie's "...-k8s-v2-formal-r1-<ts>" folder pattern).
DELIVERY_FORMAL_TAG = "k8s-v2-formal-r1"

# metadata.category short tokens (intra_class/cross_class/single ->
# intra/cross/single). Unknown -> raw slugified value (never empty).
CATEGORY_SHORT = {
    "single": "single",
    "intra_class": "intra",
    "cross_class": "cross",
}

# metadata.faults[0].fault_class short tokens (configuration/network/resource/
# lifecycle -> config/net/res/lif). Unknown -> raw slugified value.
FAULT_CLASS_SHORT = {
    "configuration": "config",
    "network": "net",
    "resource": "res",
    "lifecycle": "lif",
}

# Fallback revision counter start (collision suffix _r2/_r3/...).
COLLISION_START = 2

# Subdirs/files copied VERBATIM from raw/ (no transform).
RAW_VERBATIM_SUBDIRS = ("traces", "logs", "operations")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slugify(s, fallback):
    """ASCII-safe slug for folder-name tokens. Non [A-Za-z0-9_-] -> '_'.
    Empty/whitespace-only result -> fallback (never empty)."""
    if s is None:
        return fallback
    out = re.sub(r"[^A-Za-z0-9_-]+", "_", str(s).strip())
    out = out.strip("_")
    return out if out else fallback


def _iso_to_compact(iso_str):
    """ISO8601 'YYYY-MM-DDTHH:MM:SS.fffZ' -> 'YYYYMMDDHHMMSS' for folder ts.
    Falls back to '' on parse failure (caller handles)."""
    if not iso_str or not isinstance(iso_str, str):
        return ""
    s = iso_str.strip().replace("Z", "+00:00")
    try:
        dt = _dt.datetime.fromisoformat(s)
    except ValueError:
        # Last-ditch: try first 19 chars YYYY-MM-DDTHH:MM:SS.
        try:
            dt = _dt.datetime.strptime(iso_str[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return ""
    return dt.strftime("%Y%m%d%H%M%S")


def derive_folder_name(meta, case_dir, used_names):
    """Derive shijie-style folder name from metadata. Returns unique name.

    Layout: mr{root_count}_{cat_short}_{class_short}-{fault_type}-{tag}-{ts}

    - root_count: meta['root_count'] (1->mr1, 2->mr2, else 'mr<n>' or 'mr0').
    - cat_short: CATEGORY_SHORT[meta['category']] else slugify(category).
    - class_short: FAULT_CLASS_SHORT[faults[0].fault_class] else slugify.
    - fault_type: meta['config']['fault'] (cleanest single slug, e.g.
      'dual_timeout_retry' / 'net_delay_x_cfg_connect' / 'net_delay_single').
      Fallback: faults[*].fault_type joined by '_x_', else sample_id.
    - ts: _iso_to_compact(meta['created_at']) else mtime of case dir.

    Collision (rare): appends '_r2', '_r3', ... Uniqueness tracked in used_names.
    On total derivation failure: fallback = sample_id | formal_slot_id | dir name;
    if THAT also collides, append _r2/_r3 (still never empty/dup).
    """
    root_count = meta.get("root_count")
    if isinstance(root_count, int) and root_count > 0:
        rc_tok = "mr%d" % root_count
    elif isinstance(root_count, str) and root_count.isdigit():
        rc_tok = "mr%s" % root_count
    else:
        rc_tok = "mr0"

    cat_raw = meta.get("category")
    cat_tok = CATEGORY_SHORT.get(cat_raw) or _slugify(cat_raw, "unkcat")

    faults = meta.get("faults") or []
    f0 = faults[0] if faults else {}
    cls_raw = f0.get("fault_class") if isinstance(f0, dict) else None
    cls_tok = FAULT_CLASS_SHORT.get(cls_raw) or _slugify(cls_raw, "unkcls")

    # fault_type: prefer config.fault (single clean slug), else join fault_types.
    cfg = meta.get("config") or {}
    ft_tok = cfg.get("fault") if isinstance(cfg, dict) else None
    if not ft_tok:
        ftypes = [f.get("fault_type") for f in faults if isinstance(f, dict)
                  and f.get("fault_type")]
        ft_tok = "_x_".join(ftypes) if ftypes else None
    ft_tok = _slugify(ft_tok, "unkftype")

    ts = _iso_to_compact(meta.get("created_at"))
    if not ts:
        # Fallback to case_dir mtime (epoch compact).
        try:
            ts = _dt.datetime.fromtimestamp(
                case_dir.stat().st_mtime).strftime("%Y%m%d%H%M%S")
        except OSError:
            ts = "00000000000000"

    name = "%s_%s_%s-%s-%s-%s" % (
        rc_tok, cat_tok, cls_tok, ft_tok, DELIVERY_FORMAL_TAG, ts)

    # Validate non-empty (sanity; all tokens have fallbacks so should hold).
    if not name or name.strip("_-") == "":
        name = meta.get("sample_id") or meta.get("formal_slot_id") or case_dir.name

    # Uniqueness against used_names.
    final = name
    n = COLLISION_START
    while final in used_names:
        final = "%s_r%d" % (name, n)
        n += 1
    used_names.add(final)
    return final


def _copy_tree_verbatim(src_dir, dst_dir):
    """Copy entire directory tree verbatim (files + subdirs). dst created.
    Raises FileNotFoundError if src missing."""
    if not src_dir.is_dir():
        raise FileNotFoundError("source dir missing: %s" % src_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)
    for entry in src_dir.iterdir():
        d = dst_dir / entry.name
        if entry.is_dir():
            shutil.copytree(entry, d, dirs_exist_ok=True)
        else:
            shutil.copy2(entry, d)


def _copy_file_utf8(src, dst):
    """Copy a single file preserving utf-8 bytes (manifest etc.)."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _normalize_logs_manifest_v21(manifest_path):
    """raw/logs/manifest.json: runner 写 by_stage 嵌套(与 metrics/traces v2.1 不一致);
    上游 loader 期望 v2.1 扁平(同 metrics/traces manifest 形状)。归一:
      by_stage.<stage>.<svc>.{file,lines}
      -> {schema_version, storage_layout, artifact_root, files[{artifact,stage,service}], validation}
    幂等: 已 v2.1(无 by_stage 或已有 files[])则不动。"""
    p = Path(manifest_path)
    if not p.exists():
        return
    try:
        m = json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return
    if "by_stage" not in m or "files" in m:
        return
    bs = m.get("by_stage") or {}
    files = []
    for stage in ("pre_fault", "during_fault", "post_recovery"):
        if stage not in bs:
            continue
        for svc, info in (bs[stage] or {}).items():
            if isinstance(info, dict) and info.get("file"):
                files.append({"artifact": "raw/logs/" + info["file"],
                              "stage": stage, "service": svc})
    out = {"schema_version": "logs-manifest.v2.1",
           "storage_layout": "single_dir_stage_tagged",
           "artifact_root": "raw/logs",
           "files": files,
           "validation": {"valid": True}}
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _flatten_traces_jsonl(path):
    """Flatten one *_traces.jsonl IN-PLACE to the shijie flat shape: exactly 1
    row per trace_id (parent_span_id forced null - a single-span trace has no
    parent). Per trace_id keep the root span (parent_span_id null); if there is
    no single root, keep the earliest-start span; child spans are dropped.
    Idempotent: an already-flat file (1 row/trace) is unchanged. Returns
    (rows_kept, sorted_services). Native source (full call-tree) is NOT touched -
    only this delivery copy is rewritten."""
    rows_by_trace = {}
    order = []
    noid = 0
    with open(path, encoding="utf-8-sig") as f:  # utf-8-sig: tolerate/strip BOM on read
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if not isinstance(r, dict):
                continue  # valid non-object JSON (str/num/bool/list/null) - not a trace record
            tid = r.get("trace_id")
            if tid is None:
                tid = "__noid_%d__" % noid
                noid += 1
            if tid not in rows_by_trace:
                rows_by_trace[tid] = []
                order.append(tid)
            rows_by_trace[tid].append(r)

    def _key(r):
        return str(r.get("start_time") or r.get("timestamp") or "")

    kept = []
    for tid in order:
        grp = rows_by_trace[tid]
        if len(grp) == 1:
            chosen = grp[0]
        else:
            roots = [r for r in grp if r.get("parent_span_id") in (None, "")]
            pool = roots if roots else grp
            chosen = sorted(pool, key=_key)[0]
        chosen = dict(chosen)
        # flat shape (matches shijie sample): a 1-span trace has no parent, and
        # span_id == trace_id (her collector emits span_id==trace_id for single-
        # span traces). Native span_id stays in the repo's native source.
        chosen["parent_span_id"] = None
        if chosen.get("trace_id") is not None:
            chosen["span_id"] = chosen["trace_id"]
        # M8 18-field alignment: the flat delivery shape drops the call-tree refs
        # (parallel to parent_span_id=None above) -- the ONLY non-additive change.
        # process_id / process_tags / collector_query_service pass through unchanged.
        chosen["references"] = []
        kept.append(chosen)

    # No BOM: match our accepted dual delivery (no-BOM; 上游 loader is BOM-tolerant,
    # she accepted no-BOM dual). BOM on JSONL also breaks naive json.loads(line).
    # Read above uses utf-8-sig (tolerates BOM if re-flattening idempotently).
    with open(path, "w", encoding="utf-8") as f:
        for r in kept:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    services = sorted({str(r.get("service")) for r in kept if r.get("service")})
    return len(kept), services


def _flatten_case_traces(out_case_dir, calltree_shipped=False):
    """Flatten all raw/traces/*_traces.jsonl in a delivery case and keep the case
    internally consistent: reshape traces/manifest.json to traces-manifest.v2.1
    (files+validation), resync metadata.trace_stats.*.total to the post-flatten
    row counts, and annotate validation_results[trace_policy_satisfied].detail
    (delivery_traces_flattened + delivery_during_trace_count; native span counts
    kept truthful). Call AFTER the verbatim trace copy, only under --flat-traces.
    Native source (call-tree) stays untouched in the repo.

    calltree_shipped (--with-calltree): the delivery ALSO carries the untouched
    call-tree under raw/traces_calltree/. The manifest's
    validation.cross_service_span_graph_available is a statement about THE
    DELIVERY, so it flips to True in that case (+ additive
    cross_service_span_graph_artifact_root pointing at the call-tree root, and
    flat_projection=True so the flat dir stays self-describing). Default False =
    byte-identical to the accepted 20260709 deliveries."""
    traces_dir = out_case_dir / "raw" / "traces"
    counts = {}
    services_all = set()
    for tj in sorted(traces_dir.glob("*_traces.jsonl")):
        stage = tj.name[: -len("_traces.jsonl")]
        n, svcs = _flatten_traces_jsonl(tj)
        counts[stage] = n
        services_all |= set(svcs)

    # reshape traces/manifest.json to the shijie traces-manifest.v2.1 form
    # (files + validation) - closes the structural gap (native was traces.v1
    # counts+services). Per-stage span counts live in metadata.trace_stats;
    # services are not part of her manifest form. Only the delivery copy is
    # reshaped; native source keeps its traces.v1 manifest.
    man_p = traces_dir / "manifest.json"
    files = [{"stage": st, "artifact": "raw/traces/%s_traces.jsonl" % st}
             for st in ("pre_fault", "during_fault", "post_recovery")
             if (traces_dir / ("%s_traces.jsonl" % st)).exists()]
    # G2: register trace_profile.json as a 4th files[] entry (key "kind", mirroring
    # shijie mr3). Unconditional append — _write_trace_profile runs after this fn, so
    # the file is not yet on disk here; both are gated by traces_dir being a dir.
    files.append({"kind": "trace_capture_profile",
                  "artifact": "raw/traces/trace_profile.json"})
    # G3: cross_service_span_graph_available (before valid) mirrors shijie mr3.
    # It is a statement about THE DELIVERY, not about raw/traces/ alone:
    #   - no --with-calltree: the only trace artifact is the FLAT projection
    #     (trace_id==span_id, references=[], parent null) -> False.
    #   - --with-calltree: raw/traces_calltree/ ships the untouched native
    #     call-tree (real W3C propagation on instrumented service->service edges:
    #     measured dual01 30/106 during-window cross-service traces incl. a
    #     pricing->catalog chain; dual12 37/244 = 29 pricing->catalog +
    #     8 backend->sasrec) -> True, + the artifact root so a consumer can find
    #     it. probe-originated chains still break at the curl probe (no
    #     traceparent) and the nginx catalog-gw (no OTel).
    validation = {"cross_service_span_graph_available": bool(calltree_shipped),
                  "valid": True}
    if calltree_shipped:
        validation["cross_service_span_graph_artifact_root"] = "raw/traces_calltree"
        validation["flat_projection"] = True
    man_v21 = {
        "schema_version": "traces-manifest.v2.1",
        "storage_layout": "single_dir_stage_tagged",
        "artifact_root": "raw/traces",
        "files": files,
        "validation": validation,
    }
    man_p.write_text(
        json.dumps(man_v21, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    meta_p = out_case_dir / "metadata.json"
    if meta_p.exists():
        try:
            meta = json.loads(meta_p.read_text(encoding="utf-8"))
        except ValueError:
            meta = {}
        ts = meta.get("trace_stats") or {}
        if isinstance(ts, dict):
            for stage, n in counts.items():
                if isinstance(ts.get(stage), dict):
                    ts[stage]["total"] = n
            meta["trace_stats"] = ts
        # R3 fix: validation_results[trace_policy_satisfied].detail carries
        # during_span_count / overlap_5xx_spans that describe the native CALL-TREE
        # collection (e.g. 42 spans / 7 overlap-5xx child spans). The delivery
        # traces are a flattened reshape (fewer rows), so flag it + record the flat
        # delivery row count alongside. We do NOT overwrite the native counts - they
        # are truthful collection provenance (the repo's native metadata keeps them;
        # only this delivery copy gets the annotation, so a consumer is not confused
        # by metadata-vs-file row mismatch).
        vr = meta.get("validation_results")
        if isinstance(vr, list):
            for gate in vr:
                if isinstance(gate, dict) and "trace" in str(gate.get("id", "")).lower():
                    det = gate.get("detail")
                    if not isinstance(det, dict):
                        det = {}
                        gate["detail"] = det
                    det["delivery_traces_flattened"] = True
                    det["delivery_during_trace_count"] = counts.get("during_fault")
                    break
            meta["validation_results"] = vr
        meta_p.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8")
    return counts


# M8 triple-delivery trace_profile.json (schema trace-profile.v2.1). Honest
# provenance: every field is derived from the trace records actually on disk --
# no hardcoded service lists, no faked hostnames. known_limitations describe the
# FLAT DELIVERY form (item [2] corrected 2026-07-11: native retains propagation).
_TRACE_PROFILE_LIMITATIONS = [
    "catalog-gw is nginx and does not emit OTLP spans (metric/log only)",
    "Delivery traces are a FLAT projection (--flat-traces, mirroring the "
    "recipient's own sample format): every row trace_id==span_id, references=[], "
    "parent null -- no cross-service call graph IN THIS DELIVERY. The native "
    "collection retains real W3C trace-context propagation on instrumented "
    "service->service edges (measured: 30/106 during-window cross-service traces "
    "in a dual01 case incl. a pricing->catalog chain; 37/244 cross-service in a "
    "dual12 case = 29 pricing->catalog + 8 backend->sasrec); probe-originated chains break at the curl probe (no "
    "traceparent) and the nginx catalog-gw (no OTel). A call-tree delivery can "
    "be regenerated from the same native data without recollection",
    "process_tags carry OTel SDK resource attributes (telemetry.sdk.*) for all "
    "services; catalog_service additionally carries k8s downward-API attributes "
    "(k8s.pod.name/k8s.namespace.name/k8s.node.name) on 100% of its spans, so "
    "catalog spans CAN be pod-attributed via process_tags -- k8s.pod.name equals "
    "groundtruth.root_cause_instances for catalog-rooted cases (the during_fault-"
    "dominant catalog pod is the injected root; catalog may show >1 pod identity "
    "across stages due to collection-watchdog restarts). The other 10 services "
    "expose only telemetry.sdk.* and cannot be pod-attributed via process_tags; "
    "use groundtruth.root_cause_instances/service for them",
]

# --with-calltree replacement for limitation [2]. The "no cross-service call
# graph IN THIS DELIVERY" sentence becomes FALSE once raw/traces_calltree/ ships,
# so the string is swapped (never left stale).
_TRACE_LIMITATION_FLAT_IDX = 1
_TRACE_LIMITATION_CALLTREE = (
    "TWO trace views ship. raw/traces/ is the FLAT projection (--flat-traces, "
    "mirroring the recipient's own sample format): every row trace_id==span_id, "
    "references=[], parent null -- no call graph. raw/traces_calltree/ is the "
    "UNTOUCHED native call-tree from the same collection (non-null "
    "parent_span_id + references[{refType:CHILD_OF,traceID,spanID}]), i.e. real "
    "W3C trace-context propagation on instrumented service->service edges "
    "(observed edges include pricing->catalog, checkout->cart|pricing|inventory, "
    "review_query->catalog, search->catalog, backend->sasrec). Exact per-case "
    "span / trace / cross-service-trace counts and edge weights are MEASURED in "
    "raw/traces_calltree/calltree_stats.json -- read them there, they are not "
    "restated here. Use raw/traces_calltree/ for any trace-DAG method (e.g. "
    "Eadro); use raw/traces/ if your loader expects one row per trace. Caveat: "
    "probe-originated chains break at the curl probe (no traceparent) and at the "
    "nginx catalog-gw (no OTel), so the call graph covers instrumented "
    "service->service edges only, not every request path"
)


def _write_trace_profile(out_case_dir, calltree_shipped=False):
    """Write raw/traces/trace_profile.json (trace-profile.v2.1) for one delivery
    case. Derived purely from the trace jsonl records on disk + the nginx gateway
    log filename. Returns the profile dict (or None if no traces dir).

    - collector_query_service: single string. Derived as the most common span
      ``collector_query_service`` in the during_fault stage (or all stages if
      during empty, or "n/a" if no spans). Under our multi-service dedup this is
      the primary query target -- NOT "fixed" to span.service.
    - available_jaeger_services: sorted distinct collector_query_service values
      across all span records (the services QUERIED). Empty list for pre-Phase-1
      cases that lack the field (honest).
    - observed_span_services: sorted distinct span.service (services that
      actually emitted spans).
    - cross_service_span_graph_available: False for the flat delivery reshape
      (native retains service->service propagation; see limitation [2]).
    - known_limitations: the 3 strings above (describe the FLAT delivery form).
    - proxy_log_evidence: path to the nginx gateway access log if present, else
      "n/a". catalog-gw IS the nginx reverse proxy; its per-stage log is the
      proxy access log evidence.
    """
    traces_dir = out_case_dir / "raw" / "traces"
    if not traces_dir.is_dir():
        return None

    # Scan all stage jsonls; tally collector_query_service + span.service.
    cqs_counter = {}   # collector_query_service -> count
    cqs_during = {}    # same but during_fault stage only (for primary pick)
    observed = set()
    available = set()
    during_path = traces_dir / "during_fault_traces.jsonl"
    during_only_mode = during_path.is_file()
    for tj in sorted(traces_dir.glob("*_traces.jsonl")):
        stage = tj.name[: -len("_traces.jsonl")]
        try:
            with tj.open(encoding="utf-8-sig") as f:  # tolerate BOM if present
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        continue
                    if not isinstance(rec, dict):
                        continue
                    svc = rec.get("service")
                    if svc:
                        observed.add(str(svc))
                    cq = rec.get("collector_query_service")
                    if cq:
                        available.add(str(cq))
                        cqs_counter[cq] = cqs_counter.get(cq, 0) + 1
                        if during_only_mode and stage == "during_fault":
                            cqs_during[cq] = cqs_during.get(cq, 0) + 1
        except OSError:
            continue

    # Pick the single profile-level collector_query_service.
    pick_src = cqs_during if cqs_during else cqs_counter
    if pick_src:
        collector = sorted(pick_src.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    else:
        collector = "n/a"

    # Proxy (nginx gateway) access log evidence: catalog-gw IS the nginx proxy.
    proxy_evidence = "n/a"
    logs_dir = out_case_dir / "raw" / "logs"
    if logs_dir.is_dir():
        for stage in ("during_fault", "pre_fault", "post_recovery"):
            cand = logs_dir / ("%s__catalog-gw.log" % stage)
            if cand.exists():
                proxy_evidence = "raw/logs/%s__catalog-gw.log" % stage
                break

    limitations = list(_TRACE_PROFILE_LIMITATIONS)
    if calltree_shipped:
        limitations[_TRACE_LIMITATION_FLAT_IDX] = _TRACE_LIMITATION_CALLTREE
    profile = {
        "schema_version": "trace-profile.v2.1",
        "collector_query_service": collector,
        "available_jaeger_services": sorted(available),
        "observed_span_services": sorted(observed),
        "cross_service_span_graph_available": bool(calltree_shipped),
        "known_limitations": limitations,
        "proxy_log_evidence": proxy_evidence,
    }
    if calltree_shipped:
        profile["cross_service_span_graph_artifact_root"] = "raw/traces_calltree"
        profile["flat_trace_artifact_root"] = "raw/traces"
    # No BOM: match our accepted dual delivery convention (no-BOM across all files;
    # 上游 loader is BOM-tolerant). Consistent with metadata.json/groundtruth.json.
    out_p = traces_dir / "trace_profile.json"
    out_p.write_text(
        json.dumps(profile, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8")

    # P2c: mirror the 3 profile subkeys into metadata.trace_stats (additive --
    # per-stage {total} dicts stay untouched). 上游 trace_stats is a mixed object:
    # {stage:{total}} + available_jaeger_services / observed_span_services /
    # cross_service_span_graph_available. Independent of --flat-traces.
    meta_p = out_case_dir / "metadata.json"
    if meta_p.exists():
        try:
            meta = json.loads(meta_p.read_text(encoding="utf-8-sig"))
        except ValueError:
            meta = {}
        if isinstance(meta, dict):
            ts = meta.get("trace_stats")
            if not isinstance(ts, dict):
                ts = {}
            ts["available_jaeger_services"] = sorted(available)
            ts["observed_span_services"] = sorted(observed)
            ts["cross_service_span_graph_available"] = bool(calltree_shipped)
            meta["trace_stats"] = ts
            meta_p.write_text(
                json.dumps(meta, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8")
    return profile


# ---------------------------------------------------------------------------
# ADDITIVE delivery artifacts (2026-07-13, 140-case delivery)
#   All three are OFF by default so a flagless run stays byte-identical to the
#   accepted 20260709 deliveries (her loader already reads those).
#     --with-calltree  -> raw/traces_calltree/  (NEW dir; raw/traces/ untouched)
#     --with-eval      -> eval/                 (NEW dir; RCAEval-ready wide table)
#     --with-gt-distinct -> groundtruth.n_distinct_root_services (NEW key only)
# ---------------------------------------------------------------------------

CALLTREE_README = """# raw/traces_calltree/ - native call-tree traces (ADDITIVE, new in this delivery)

`raw/traces/` (unchanged, what previous deliveries shipped) is a **flat
projection**: one row per trace_id, `span_id == trace_id`, `parent_span_id: null`,
`references: []`. It was flattened to mirror your own sample format.

`raw/traces_calltree/` is the **same collection, untouched**: every span row kept,
with real parent/child links:
- `parent_span_id`: non-null on child spans
- `references`: `[{"refType": "CHILD_OF", "traceID": ..., "spanID": ...}]`

Same 18-field record shape and the same 3 stage files
(`pre_fault_traces.jsonl` / `during_fault_traces.jsonl` /
`post_recovery_traces.jsonl`), so a loader written for `raw/traces/` reads these
too - it just now sees N spans per trace instead of 1.

**Use this directory for any trace-DAG / service-dependency-graph method
(e.g. Eadro).** `raw/traces/` cannot yield a call graph by construction.

`calltree_stats.json` (per case, measured from the files here) reports
spans / spans-with-parent / traces / cross-service traces and the observed
service->service call edges with counts.

Honest caveat: propagation exists on **instrumented service->service edges only**.
Probe-originated chains break at the curl probe (no `traceparent` header) and at
the nginx `catalog-gw` gateway (no OTel SDK), so cross-service traces are a
minority of all traces - the edges that DO appear are real, but the graph is not
a complete request-path DAG.
"""


def _write_calltree(case_dir, out_case_dir):
    """Copy the NATIVE (call-tree) raw/traces/ into <delivery case>/raw/traces_calltree/
    verbatim + write README.md + calltree_stats.json (measured from the copied
    rows: spans / spans_with_parent / traces / cross_service_traces / edges).

    NEW directory: raw/traces/ (the flat projection her loader already reads) is
    NOT touched. Source native tree is read-only.
    Returns the stats dict (or None if the case has no raw/traces/)."""
    src = Path(case_dir) / "raw" / "traces"
    if not src.is_dir():
        return None
    dst = Path(out_case_dir) / "raw" / "traces_calltree"
    _copy_tree_verbatim(src, dst)

    spans = 0
    with_parent = 0
    span_svc = {}          # span_id -> service
    parent_of = {}         # span_id -> parent_span_id
    trace_svcs = {}        # trace_id -> set(service)
    for tj in sorted(dst.glob("*_traces.jsonl")):
        with tj.open(encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(r, dict):
                    continue
                spans += 1
                sid, tid = r.get("span_id"), r.get("trace_id")
                svc, par = r.get("service"), r.get("parent_span_id")
                if sid:
                    span_svc[sid] = svc
                if par:
                    with_parent += 1
                    if sid:
                        parent_of[sid] = par
                if tid:
                    trace_svcs.setdefault(tid, set()).add(svc)

    edges = {}
    for sid, par in parent_of.items():
        csvc, psvc = span_svc.get(sid), span_svc.get(par)
        if csvc and psvc and csvc != psvc:
            edges["%s->%s" % (psvc, csvc)] = edges.get("%s->%s" % (psvc, csvc), 0) + 1
    cross = sum(1 for s in trace_svcs.values() if len(s) > 1)

    stats = {
        "schema_version": "calltree-stats.v1",
        "spans": spans,
        "spans_with_parent": with_parent,
        "traces": len(trace_svcs),
        "cross_service_traces": cross,
        "service_call_edges": dict(sorted(edges.items(), key=lambda kv: (-kv[1], kv[0]))),
        "note": ("Measured from the files in this directory. An edge parent_service"
                 "->child_service is counted once per child span whose parent span "
                 "belongs to a different service. Spans whose parent was not "
                 "captured (root of a broken chain) contribute no edge."),
    }
    (dst / "README.md").write_text(CALLTREE_README, encoding="utf-8")
    (dst / "calltree_stats.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return stats


EVAL_README = """# eval/ - ready-to-run evaluation view (ADDITIVE, new in this delivery)

Optional convenience. Nothing here is a new measurement - it is a **derived view
of `raw/metrics/metrics_v2.jsonl`**, shipped so you do not have to re-write the
adapter (and so our numbers and yours come from the same table).

## Files
- `data.csv` - wide table, RCAEval-compatible. First column `time` (epoch
  seconds), every other column is `<service>__<metric>` (split on the FIRST
  `__` to get the service name). No NaN. Numeric only.
- `inject_time.txt` - a single epoch-second integer: the fault injection time,
  = the first `during_fault` sample, snapped to the same time grid as `data.csv`.
  Rows with `time <  inject_time` are the normal (pre_fault) window.
  Rows with `time >= inject_time` are the anomalous (during_fault) window.
- `build_info.json` - how this table was built (bucket, column/row counts,
  services, and the blind-gap audit below).

## Two things that will otherwise bite you
1. **Blind gap.** The collector fills the pre_fault window, then executes the
   injection primitive (a rollout can take ~50s), then reopens collection for
   during_fault. So there is a **zero-sample gap** between the last pre_fault
   sample and the first during_fault sample (measured median ~15s, max ~113s).
   `data.csv` is built **per stage**: each stage gets its own grid and is
   ffill/bfill-ed **within the stage only**. Buckets inside the gap are NOT
   generated. Interpolating across the gap fabricates "normal" rows (measured:
   up to 62.8% of a case's rows) and crushes column IQRs to 0.
2. **Column universe = full.** All observed columns are kept, including latency
   (usable as an SLI), matching RCAEval's own input convention. Nothing is
   pre-filtered for you.

Grid = 2s buckets (the collector's poll cadence); multiple samples in a bucket
are averaged. Built by `scripts/chaos/ctk/m9_adapter.py::build_wide(gap_aware=True)`.
"""


def _write_eval(case_dir, out_case_dir):
    """Write <delivery case>/eval/{data.csv,inject_time.txt,build_info.json,README.md}
    via m9_adapter.build_wide(gap_aware=True) over the NATIVE case dir.

    NEW directory - touches nothing her loader reads.

    Built from the NATIVE raw/metrics/metrics_v2.jsonl (not the mr2-transformed
    delivery copy) ON PURPOSE: the mr2 transform aliases every emit source to
    `source=prometheus` (originals under labels.source_raw), and build_wide's
    channel routing keys off the ORIGINAL source (http_probe panel vs carrier vs
    prom). Values are identical; only the routing label differs.

    The `stage` column build_wide appends is DROPPED from data.csv so the file is
    purely numeric (a string column breaks naive `df.drop(columns=['time'])`
    numeric pipelines). Nothing is lost: stage is exactly
    `time < inject_time -> pre_fault else during_fault`, and inject_time.txt is
    shipped next to it.

    Returns the info dict, or None if the case yields no usable table.
    Import of m9_adapter is LAZY (it needs pandas/numpy) so a flagless run of this
    packager keeps working in a bare env."""
    import m9_adapter  # lazy: pandas/numpy only needed under --with-eval

    df, inject, info = m9_adapter.build_wide(str(case_dir), bucket=2.0,
                                             gap_aware=True)
    if df is None or inject is None:
        return None
    eval_dir = Path(out_case_dir) / "eval"
    eval_dir.mkdir(parents=True, exist_ok=True)

    data = df.drop(columns=["stage"]) if "stage" in df.columns else df
    data.to_csv(eval_dir / "data.csv", index=False, encoding="utf-8",
                lineterminator="\n")

    inj = float(inject)
    inj_str = str(int(inj)) if inj.is_integer() else ("%.3f" % inj)
    (eval_dir / "inject_time.txt").write_text(inj_str + "\n", encoding="utf-8")

    build_info = {
        "schema_version": "eval-view.v1",
        "built_by": "scripts/chaos/ctk/m9_adapter.py::build_wide",
        "source": "raw/metrics/metrics_v2.jsonl (native, pre-mr2-transform)",
        "bucket_seconds": 2.0,
        "gap_aware": True,
        "column_universe": "full",
        "inject_time": inj,
        "n_rows": info.get("n_rows"),
        "n_cols": info.get("n_cols"),
        "pre_points": info.get("pre_points"),
        "during_points": info.get("during_points"),
        "services": info.get("services"),
        "blind_gap_sec": info.get("blind_gap_sec"),
        "dropped_all_nan": info.get("dropped_all_nan"),
        "dropped_cross_stage_only": info.get("dropped_cross_stage_only"),
    }
    (eval_dir / "build_info.json").write_text(
        json.dumps(build_info, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8")
    (eval_dir / "README.md").write_text(EVAL_README, encoding="utf-8")
    return info


def _add_distinct_root_key(gt):
    """ADD groundtruth.n_distinct_root_services = len(set(root_cause_services)).
    Pure addition: no existing key's value is read-modified, none dropped.

    WHY: the arity in the folder/dir name (single/dual/triple) is the count of
    INJECTED FAULTS, not the count of distinct root SERVICES - two faults can land
    on the same service. 30/140 cases differ (15 dual cases are |G|=1; 15 triple
    cases are |G|=2; only triple01 is a true |G|=3). MRCBench's R = |G| and
    NDCG's IDCG upper bound = min(K, |G|), so deriving R from the folder name
    mis-scores Recall@R and NDCG@K on those 30 cases. This key is the authoritative
    |G|.

    Inserted right after root_cause_services for readability (JSON key ORDER is
    not semantic; no existing key is renamed/removed/re-valued). Returns a new
    dict; no-op if root_cause_services is missing or not a list."""
    svcs = gt.get("root_cause_services")
    if not isinstance(svcs, list):
        return gt
    n = len({str(s) for s in svcs})
    out = {}
    for k, v in gt.items():
        out[k] = v
        if k == "root_cause_services":
            out["n_distinct_root_services"] = n
    if "n_distinct_root_services" not in out:  # defensive (key absent -> append)
        out["n_distinct_root_services"] = n
    return out


def _rewrite_sample_id(meta, folder_name):
    """(N1 fix) Align sample_id to the formal delivery folder name + carry the
    original case id as additive provenance `source_case_id`.

    Mutates a COPY of meta (caller passes a fresh dict); returns the copy.
    - source_case_id = the pre-rewrite sample_id (original case id, e.g.
      m4b_m1_dual_01 / m2a_netcfg_read01) -- preserved so MANIFEST/audit can map
      folder name back to the source case.
    - sample_id = folder_name (the formal delivery id, e.g.
      mr2_intra_config-dual_timeout_retry-...). This satisfies shijie's contract
      that metadata.sample_id == groundtruth.sample_id == folder name.
    Idempotent: if sample_id already equals folder_name (rerun over an already-
    rewritten tree), source_case_id is kept as-is (never overwritten with the
    folder name).
    """
    out = dict(meta)
    orig = meta.get("source_case_id") or meta.get("sample_id")
    if orig is not None:
        out["source_case_id"] = orig
    out["sample_id"] = folder_name
    # also align the NESTED metadata.ground_truth.sample_id (adapter adds it; 上游 contract:
    # metadata.sample_id == ground_truth.sample_id == groundtruth.sample_id == folder name)
    gt = out.get("ground_truth")
    if isinstance(gt, dict):
        gt = dict(gt)
        gt["sample_id"] = folder_name
        out["ground_truth"] = gt
    return out


# Regex matching the per-stage per-service log file naming: {stage}__{svc}.log
# (e.g. during_fault__catalog-gw.log). Used to count distinct stages / services.
_LOG_NAME_RE = re.compile(r"^(.+?)__(.+)\.log$")

# Stage label -> english number word for the `logs:` bullet. All k8s_pilot cases
# use exactly three stages (pre_fault / during_fault / post_recovery); the map is
# keyed by stage count so the wording is honest if a non-3 case ever appears.
_STAGE_COUNT_WORD = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five"}


def _count_metrics_distinct(metrics_jsonl_path):
    """Count distinct `metric` values in a long-format metrics_v2.jsonl.
    Returns None if the file is missing / unreadable (caller handles)."""
    p = Path(metrics_jsonl_path)
    if not p.is_file():
        return None
    kinds = set()
    try:
        with p.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                m = rec.get("metric") if isinstance(rec, dict) else None
                if m:
                    kinds.add(m)
    except OSError:
        return None
    return len(kinds)


def _count_log_stages_services(logs_dir):
    """Scan raw/logs/ for `{stage}__{svc}.log` files. Returns (n_stages, n_svcs).
    Returns (0, 0) if dir missing. Distinct over ALL files (stages × services)."""
    d = Path(logs_dir)
    if not d.is_dir():
        return (0, 0)
    stages, svcs = set(), set()
    for entry in d.iterdir():
        if not entry.is_file():
            continue
        m = _LOG_NAME_RE.match(entry.name)
        if m:
            stages.add(m.group(1))
            svcs.add(m.group(2))
    return (len(stages), len(svcs))


def _format_ratio(v):
    """Render a ratio value: floats that are whole numbers drop the trailing
    '.0' (1.0 -> '1', 0.4615 -> '0.4615'). Matches 上游 sample's `=1` style while
    preserving honest precision for fractional values."""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def _format_bool(v):
    """Render ready_for_release: True/False/None. 上游 sample uses Python-style
    capitalized True/False. Missing -> '' (honest, not fabricated)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "True" if v else "False"
    # Non-bool truthy JSON value: keep as-is (string repr).
    return str(v)


# Max length for the root_metric_contract.notes fallback string when surfacing
# it as the `root evidence` bullet (host_cpu / db_lock cases have no
# _points/_ratio keys, so their root signal lives in the notes string). Truncate
# to keep the one-line bullet readable while still carrying the key signals.
_NOTES_TRUNCATE_LEN = 200


def _truncate_notes(notes):
    """Trim a notes string to ~_NOTES_TRUNCATE_LEN chars, append '...' if cut."""
    s = notes.strip()
    if len(s) > _NOTES_TRUNCATE_LEN:
        s = s[:_NOTES_TRUNCATE_LEN].rstrip() + "..."
    return s


def _extract_root_evidence(meta, native_summary_text):
    """Extract the `root evidence` bullet value as `key=value;key=value;...`.

    Source of truth = metadata.validation_results[*] where id ==
    'each_root_signal_present' (its detail dict carries per-root point counts +
    ratio values, e.g. net_delay_points / connect_timeout_points /
    F1_only_ok_ratio / overlap_error_ratio). Falls back to the same keys in the
    'root_metric_contract' gate detail if each_root_signal_present is absent.

    Keys surfaced (additive, only those actually present per case - never
    fabricated):
      - per-root point counts: any key ending in `_points`
        (net_delay_points / connect_timeout_points / net_loss_points /
         read_timeout_points / retry_disabled_points / timeout_points)
      - ratio keys: F1_only_ok_ratio / overlap_error_ratio / F1_only_error_ratio
        / f1_only_error_ratio / f2_window_error_ratio / f1_only_p95_ratio
        / carrier1_{baseline,f1,f2}_error_ratio / net_gw_direct_ratio /
        net_sep_ratio

    FALLBACK (host_cpu / db_lock): these fault classes carry no _points/_ratio
    keys, so the primary scan yields ''. In that case the root signal lives in
    the `root_metric_contract.notes` string (already `key=value;key=value;...`
    format, e.g. 'host_root_present=True;catalog_root_throttle_present=True;...'
    for host_cpu, or 'items_lock_root_present=True;n_error_burst=3;...' for
    db_lock). We surface that notes string (cleaned + truncated to
    ~_NOTES_TRUNCATE_LEN chars) as the root evidence line - honest passthrough,
    no fabrication. Searched in priority order:
      1. validation_results gate detail.notes where id == 'root_metric_contract'
      2. top-level meta.root_metric_contract.notes

    Returns '' only if BOTH the _points/_ratio scan AND the notes fallback are
    empty (honest empty; never fabricated).
    native_summary_text is currently unused for extraction (the metadata gate
    detail / notes is the authoritative + machine-readable source); it is
    accepted to keep the signature stable for future fallback parsing.
    """
    order = [
        # per-root point counts first (most diagnostic), then ratios.
        "timeout_points", "retry_disabled_points", "net_delay_points",
        "net_loss_points", "read_timeout_points", "connect_timeout_points",
        "F1_only_ok_ratio", "overlap_error_ratio",
        "F1_only_error_ratio", "f1_only_error_ratio",
        "f2_window_error_ratio", "f1_only_p95_ratio",
        "carrier1_baseline_error_ratio", "carrier1_f1_error_ratio",
        "carrier1_f2_error_ratio",
        "net_gw_direct_ratio", "net_sep_ratio",
    ]
    is_ratio = {
        "F1_only_ok_ratio", "overlap_error_ratio",
        "F1_only_error_ratio", "f1_only_error_ratio",
        "f2_window_error_ratio", "f1_only_p95_ratio",
        "carrier1_baseline_error_ratio", "carrier1_f1_error_ratio",
        "carrier1_f2_error_ratio",
        "net_gw_direct_ratio", "net_sep_ratio",
    }

    # Collect the gate detail dicts in priority order.
    gate_details = []
    notes_sources = []  # candidate root_metric_contract.notes (host_cpu/db_lock)
    vr = meta.get("validation_results") if isinstance(meta, dict) else None
    if isinstance(vr, list):
        for g in vr:
            if not isinstance(g, dict):
                continue
            if g.get("id") in ("each_root_signal_present", "root_metric_contract"):
                d = g.get("detail")
                if isinstance(d, dict):
                    gate_details.append(d)
                    if g.get("id") == "root_metric_contract":
                        notes_sources.append(d.get("notes"))
    # Fallback: top-level root_metric_contract object.
    rmc = meta.get("root_metric_contract") if isinstance(meta, dict) else None
    if isinstance(rmc, dict):
        gate_details.append(rmc)
        notes_sources.append(rmc.get("notes"))

    parts = []
    seen = set()
    for key in order:
        if key in seen:
            continue
        for d in gate_details:
            if key in d:
                v = d[key]
                if v is None:
                    continue
                parts.append("%s=%s" % (key, _format_ratio(v) if key in is_ratio
                                        else str(v)))
                seen.add(key)
                break
    primary = ";".join(parts)
    if primary:
        # config/net fault: _points/_ratio keys present -> use them (do NOT let
        # the notes fallback override the diagnostic primary).
        return primary

    # host_cpu / db_lock: no _points/_ratio keys -> fall back to the
    # root_metric_contract.notes string (key=value;key=value;...). Honest
    # passthrough of whatever notes carries; cleaned + truncated.
    for notes in notes_sources:
        if isinstance(notes, str) and notes.strip():
            return _truncate_notes(notes)
    return ""


def build_shijie_summary(out_case_dir, meta, native_summary_text, case_dir, mr2_dir):
    """Build 上游's 6-bullet summary.md content from derived metadata.

    Layout (authoritative: docs/blackboard/REF-shijie-k8s-v2-sample-schema.md):
        # <folder name>
        <blank>
        - storage_layout: <single_dir_stage_tagged | ...>
        - sample_status: <ready_for_release | ...>
        - ready_for_release: <True | False>
        - observed metric kinds: <N>
        - logs: <three> stages x <N> services
        - root evidence: <key=value;key=value;... | (empty)>

    Derivation (per task spec):
      - H1: out_case_dir.name (= N1-rewritten sample_id; equals folder name).
      - storage_layout: meta.storage_layout, default single_dir_stage_tagged.
      - sample_status: meta.sample_status (raw value; '' if missing).
      - ready_for_release: meta.ready_for_release (True/False; '' if missing).
      - observed metric kinds: distinct `metric` count in this case's
        metrics_v2.jsonl (prefer mr2/, the file copied to delivery; fall back to
        raw/metrics/). Real observed count (NOT the required-metric baseline 27).
      - logs: '{word} stages x {N} services' counted from raw/logs/ over distinct
        {stage}__{svc}.log files (honest: 3 / 4 / 5 services depending on carrier).
      - root evidence: per-root point counts + ratio keys extracted from
        metadata.validation_results 'each_root_signal_present' / 'root_metric_contract'
        gate detail (see _extract_root_evidence). Empty if none present.

    Idempotent: same inputs -> identical output. utf-8 (no BOM) written by caller.
    native_summary_text accepted for signature stability / future fallback but
    the metadata gate detail is the authoritative extraction source.
    case_dir = source case dir (has mr2/ + raw/); used to locate metrics_v2/logs.
    """
    folder_name = Path(out_case_dir).name
    case_dir = Path(case_dir)

    # sample_status: map our source vocab (ready_for_release/blocked) onto
    # shijie's summary vocab (ready/blocked). ready_for_release -> ready;
    # blocked -> blocked (unchanged); any other value passes through as-is
    # (honest; only the one known mismatch is reconciled). '' if missing.
    _SAMPLE_STATUS_MAP = {"ready_for_release": "ready"}
    storage_layout = meta.get("storage_layout") or "single_dir_stage_tagged"
    sample_status = meta.get("sample_status")
    if sample_status is None:
        sample_status = ""
    else:
        sample_status = _SAMPLE_STATUS_MAP.get(sample_status, sample_status)
    ready = _format_bool(meta.get("ready_for_release"))

    # observed metric kinds: prefer mr2/metrics_v2.jsonl (what gets copied to the
    # delivery); fall back to raw/metrics/metrics_v2.jsonl. Real distinct count.
    mr2_metrics = Path(mr2_dir) / "metrics_v2.jsonl"
    raw_metrics = case_dir / "raw" / "metrics" / "metrics_v2.jsonl"
    n_metrics = _count_metrics_distinct(mr2_metrics)
    if n_metrics is None:
        n_metrics = _count_metrics_distinct(raw_metrics)
    metric_kinds_str = str(n_metrics) if n_metrics is not None else ""

    # logs: count distinct stages x distinct services from raw/logs/.
    # Both counts rendered as english number words to mirror shijie's
    # "three stages x three services" style (digit fallback only if >5).
    n_stages, n_svcs = _count_log_stages_services(case_dir / "raw" / "logs")
    if n_stages > 0 and n_svcs > 0:
        stage_word = _STAGE_COUNT_WORD.get(n_stages, str(n_stages))
        svc_word = _STAGE_COUNT_WORD.get(n_svcs, str(n_svcs))
        logs_str = "%s stages x %s services" % (stage_word, svc_word)
    else:
        logs_str = ""

    root_ev = _extract_root_evidence(meta, native_summary_text)

    lines = [
        "# %s" % folder_name,
        "",
        "- storage_layout: %s" % storage_layout,
        "- sample_status: %s" % sample_status,
    ]
    if ready != "":
        lines.append("- ready_for_release: %s" % ready)
    else:
        lines.append("- ready_for_release:")
    lines.append("- observed metric kinds: %s" % metric_kinds_str)
    lines.append("- logs: %s" % logs_str)
    lines.append("- root evidence: %s" % root_ev)
    return "\n".join(lines) + "\n"


def assemble_case(case_dir, out_case_dir, mr2_dir, dry_run=False, meta=None,
                  flat_traces=False, with_calltree=False, with_eval=False,
                  gt_distinct=False):
    """Assemble one delivery per-case folder. case_dir = source case (has mr2/).
    out_case_dir = <out>/<folder-name>/. meta = the mr2 metadata dict (used to write
    the reconstructed collect-<sample_id>.ps1 script).

    Reads mr2/{metadata,metrics_v2,quality,manifest}.json + native groundtruth/summary +
    raw/{traces,logs,operations}.

    N1 fix: before writing metadata.json / groundtruth.json to the delivery
    folder, both files' `sample_id` is aligned to the formal folder name
    (out_case_dir.name) and an additive `source_case_id` provenance key (the
    original case id) is added. This satisfies shijie's contract that
    metadata.sample_id == groundtruth.sample_id == folder name while preserving
    the source case id for MANIFEST/audit.

    summary.md: GENERATED (not copied). 上游's 6-bullet minimal format is
    derived from metadata + per-case metrics_v2/logs + validation_results gate
    detail (see build_shijie_summary). The native summary.md is NOT shipped to
    delivery (it references affected_services / intensity that the adapter
    stripped from groundtruth.json - previously needed a B7 reconciling footnote);
    the source native summary.md stays untouched. Idempotent pure-function.

    Returns dict of written paths (or planned paths if dry_run).
    """
    case_dir = Path(case_dir)
    out_case_dir = Path(out_case_dir)
    # ★ 2026-07-13: adapter 产物不再住在 native 树里 (<case>/mr2/), 由调用方显式传入
    #   (datasets/_runtime/package/<tag>/<case_id>/)。没有 native 缺省值 —— 不留后门。
    mr2 = Path(mr2_dir)
    raw = case_dir / "raw"
    folder_name = out_case_dir.name

    written = {}

    def _plan(p):
        written[p.name] = str(p)
        return p

    # --- top-level metadata.json (N1: rewrite sample_id + source_case_id) ---
    src = mr2 / "metadata.json"
    if not src.exists() and not dry_run:
        # dry-run 时 adapter 没跑过, runtime 目录当然是空的 -> 那是预期,不是错误。
        # (真跑时缺 adapter 产物 = 真错误,照抛。)
        raise FileNotFoundError("adapter output missing: %s" % src)
    if not dry_run:
        out_meta = _rewrite_sample_id(
            json.loads(src.read_text(encoding="utf-8")), folder_name)
        (out_case_dir / "metadata.json").write_text(
            json.dumps(out_meta, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8")
    _plan(out_case_dir / "metadata.json")

    # --- groundtruth + summary ---
    # groundtruth.json: prefer mr2/groundtruth.json (adapter-normalized to 上游
    # 13-key top + 10-key component_ground_truth + start_time/end_time windows;
    # answers byte-identical, only key names + drop additive provenance keys);
    # fall back to native groundtruth.json if adapter produced none.
    # N1: same sample_id/source_case_id rewrite applied here too.
    gt_src = mr2 / "groundtruth.json"
    if not gt_src.exists():
        gt_src = case_dir / "groundtruth.json"
    if gt_src.exists():
        if not dry_run:
            gt = json.loads(gt_src.read_text(encoding="utf-8"))
            gt = _rewrite_sample_id(gt, folder_name)
            if gt_distinct:
                gt = _add_distinct_root_key(gt)
            (out_case_dir / "groundtruth.json").write_text(
                json.dumps(gt, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8")
        _plan(out_case_dir / "groundtruth.json")

    # summary.md: GENERATE 上游's 6-bullet minimal format (H1 = folder name +
    # 6 `- key: value` bullets), derived from metadata + the per-case
    # metrics_v2/logs + validation_results gate detail. The native summary.md is
    # NOT shipped to delivery (it carries rich narrative affected_services /
    # intensity fields the adapter strips from groundtruth.json, which previously
    # needed a B7 reconciling footnote); the source native summary.md is left
    # untouched (preserves full detail in datasets/k8s_pilot/). The delivery
    # summary.md is an idempotent pure-function of metadata + raw/ files, so it
    # never references affected_services and has no B7 contradiction.
    native_summary_text = ""
    native_summary_path = case_dir / "summary.md"
    if native_summary_path.exists():
        try:
            native_summary_text = native_summary_path.read_text(encoding="utf-8")
        except OSError:
            native_summary_text = ""
    if not dry_run:
        # Read the (un-rewritten) source metadata for the derived fields. The
        # N1 sample_id rewrite on metadata.json above only changes sample_id /
        # source_case_id - none of the summary-derived fields - so re-reading the
        # source here is fine and keeps the summary independent of write order.
        src_meta_path = mr2 / "metadata.json"
        if not src_meta_path.exists():
            src_meta_path = case_dir / "metadata.json"
        try:
            summary_meta = json.loads(src_meta_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            summary_meta = {}
        summary_text = build_shijie_summary(
            out_case_dir, summary_meta, native_summary_text, case_dir, mr2)
        (out_case_dir / "summary.md").write_text(summary_text, encoding="utf-8")
    _plan(out_case_dir / "summary.md")

    # --- raw/metrics/ (mr2-transformed metrics + quality + manifest) ---
    # manifest: prefer mr2/manifest.json (adapter-normalized to 上游 metrics-manifest.v2.1
    # shape); fall back to raw/metrics/manifest.json if adapter produced none.
    for fname, srcroot in (
        ("metrics_v2.jsonl", mr2),
        ("quality.json", mr2),
        ("manifest.json", mr2),
    ):
        s = srcroot / fname
        if fname == "manifest.json" and not s.exists():
            s = raw / "metrics" / fname  # fallback
        if s.exists():
            if not dry_run:
                _copy_file_utf8(s, out_case_dir / "raw" / "metrics" / fname)
            _plan(out_case_dir / "raw" / "metrics" / fname)

    # --- raw/{traces,logs,operations}/ verbatim ---
    for sub in RAW_VERBATIM_SUBDIRS:
        s = raw / sub
        if s.is_dir():
            if not dry_run:
                _copy_tree_verbatim(s, out_case_dir / "raw" / sub)
            else:
                # Plan: list files.
                for f in s.rglob("*"):
                    if f.is_file():
                        _plan((out_case_dir / "raw" / sub / f.relative_to(s)))
            _plan(out_case_dir / "raw" / sub)

    # --- raw/logs/manifest.json: by_stage -> v2.1 flat (上游 strict mirror) ---
    # runner 写 logs manifest 为 by_stage(与 metrics/traces v2.1 不一致); 此处归一使交付对齐上游 loader。
    if not dry_run:
        _normalize_logs_manifest_v21(out_case_dir / "raw" / "logs" / "manifest.json")

    # --- raw/traces/ optional flatten to shijie flat shape (--flat-traces) ---
    # Native source keeps the full call-tree; only this delivery copy is flattened
    # to 1 row/trace_id (parent_span_id null, span_id==trace_id). Also reshapes
    # traces/manifest.json to traces-manifest.v2.1 + resyncs metadata.trace_stats
    # + annotates validation_results so the case stays consistent.
    # --- raw/traces_calltree/ (--with-calltree): NEW dir, native call-tree copy ---
    # Written BEFORE the flatten/profile steps so those can honestly declare that
    # a cross-service span graph IS available in this delivery. raw/traces/ is not
    # touched by this - her existing loader sees zero change.
    if with_calltree and not dry_run:
        ct = _write_calltree(case_dir, out_case_dir)
        if ct is not None:
            _plan(out_case_dir / "raw" / "traces_calltree")

    if flat_traces and not dry_run:
        _flatten_case_traces(out_case_dir, calltree_shipped=with_calltree)

    # --- raw/traces/trace_profile.json (M8 triple-delivery, trace-profile.v2.1) ---
    # Generated for every case (additive). Derived purely from on-disk trace
    # records + nginx gateway log -- no hardcoded service lists. Honors the BOM
    # convention internally. Independent of --flat-traces.
    if not dry_run:
        prof = _write_trace_profile(out_case_dir, calltree_shipped=with_calltree)
        if prof is not None:
            _plan(out_case_dir / "raw" / "traces" / "trace_profile.json")

    # --- eval/ (--with-eval): NEW dir, RCAEval-ready wide table + inject_time ---
    if with_eval and not dry_run:
        if _write_eval(case_dir, out_case_dir) is not None:
            _plan(out_case_dir / "eval")

    # --- scripts/collect-<sample_id>.ps1 (reconstructed chaos_k8s_runner command) ---
    if meta is not None:
        collect_p = write_collect_script(out_case_dir, meta, dry_run=dry_run)
        if collect_p is not None:
            _plan(collect_p)

    return written


def find_cases(pilot_dir):
    """Discover source case dirs = cases/*/*/ containing metadata.json AND
    raw/metrics/metrics_v2.jsonl (same AND-filter as adapter's --all).
    Accepts BOTH layouts: <pilot>/cases/** (existing k8s_pilot convention) AND
    flat <pilot>/** (e.g. dual_v2/dualNN_uni_rR/)."""
    pilot_dir = Path(pilot_dir)
    cases_root = pilot_dir / "cases"
    if cases_root.is_dir():
        root = cases_root
    elif pilot_dir.is_dir():
        root = pilot_dir  # flat layout
    else:
        raise FileNotFoundError("no cases/ dir (and not a dir) under %s" % pilot_dir)
    out = []
    for mp in sorted(root.glob("**/metadata.json")):
        # skip adapter output: a prior packaging writes an mr2/ subdir (with its own
        # metadata.json + raw/) INTO each source case dir; recursive glob would else
        # double-count native(80) + mr2/(80)=160. Only enumerate native top-level cases.
        if "mr2" in mp.parts:
            continue
        cd = mp.parent
        if (cd / "raw" / "metrics" / "metrics_v2.jsonl").exists():
            out.append(cd)
    return out


# ---------------------------------------------------------------------------
# P0-8 release gate (HANDOFF-2026-08-10 §7 P0-8, §15 forbidden list)
# ---------------------------------------------------------------------------
# v1's packager did NOT check ready_for_release at all (Diagnosis §2.6,
# key-finding #7): it admitted 2 known-bad cases (podfail_cart_r4,
# podfail_recagent_r2) and a dual08_uni_r5 whose aggregate gate masked an
# unrecovered leg. This gate reads metadata.json and rejects any case whose
# release fields are false OR missing (handoff §7: "any false/missing release
# field immediately rejects the case"). `--no-strict-gate` is a legacy-debug
# escape hatch that keeps the old permissive behavior (warn but still copy).

def check_release_gate(case_dir):
    """P0-8 release gate for one case (HANDOFF §7 P0-8).

    Reads `case_dir/metadata.json` and returns (ok, reason):
      - ready_for_release missing or False            -> (False, "ready_for_release=false or missing")
      - sample_status == "blocked"                    -> (False, "sample_status=blocked")
      - checksum_guard missing                        -> (False, "checksum_guard missing")
      - checksum_guard.zero_drift is False            -> (False, "checksum drift")
      - metadata.json missing or unparseable          -> (False, "metadata unreadable: <error>")
      - otherwise                                     -> (True, "")

    Strict semantics (HANDOFF §7): a MISSING ready_for_release is a rejection,
    not a silent pass. checksum_guard follows the same rule — a missing
    checksum_guard is itself a rejection under the strict gate, so v1 legacy
    cases that never recorded one cannot sneak through unverified; an explicit
    zero_drift=False is a checksum-drift rejection. Callers that genuinely need
    the legacy permissive path must pass --no-strict-gate.

    `case_dir` may be a str or Path. The case is NEVER mutated.
    """
    case_dir = Path(case_dir)
    meta_path = case_dir / "metadata.json"
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        return (False, "metadata unreadable: %s" % e)
    except (OSError, ValueError) as e:
        return (False, "metadata unreadable: %s" % e)

    # 1. ready_for_release — explicit True required; missing is fail-closed.
    if meta.get("ready_for_release") is not True:
        return (False, "ready_for_release=false or missing")

    # 2. sample_status — "blocked" means a human or upstream gate held it back.
    if meta.get("sample_status") == "blocked":
        return (False, "sample_status=blocked")

    # 3. checksum_guard — a missing checksum_guard is itself a rejection under
    #    the strict gate (HANDOFF §7 "missing release field immediately
    #    rejects"). An explicit zero_drift=False is a checksum-drift rejection.
    cg = meta.get("checksum_guard")
    if cg is None:
        return (False, "checksum_guard missing")
    if isinstance(cg, dict) and cg.get("zero_drift") is False:
        return (False, "checksum drift")

    return (True, "")


def load_eval_case_ids(features_dir=None):
    """features_k8s.csv -> set of case_id values (the eval set, provenance=real).
    Used by --eval-only to exclude dev artifacts (reg/fix/smoke that exist under
    cases/ but are NOT in the eval feature view).

    ★ 2026-07-13 fail-loud:以前 csv 缺失时 return None,调用方就【不过滤、把全集打进包】,
      只在 stdout 印一行提示。使用者以为拿到的是"评测集",实际是全集 —— 静默失败,最坏的一种。
      现在:DR.feature_csv(required=True) 找不到就 raise(并直说怎么生成它)。
    """
    feat = DR.feature_csv("features_k8s.csv", search_dir=features_dir)
    ids = set()
    with feat.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cid = (row.get("case_id") or "").strip()
            if cid:
                ids.add(cid)
    return ids


# ---------------------------------------------------------------------------
# Top-level writers
# ---------------------------------------------------------------------------

ADAPTER_README = """# adapter/ - mr2 load adapter (pre-applied)

`mr2_load_adapter.py` normalizes RecWeb2 k8s_pilot cases into your mr2 consumer's
expected load shape. It has ALREADY been applied to every `<case>/raw/metrics/`
in this delivery (source=prometheus, quality=observed, container, unit-normalized;
metadata schema_version=v1.2, observation_stages keyed object, root_causes as
string-list, component_fault_windows start_time/end_time, validation_results
status=passed; quality.json stages wrapper + schema_version).

You normally do NOT need to re-run it. It is shipped here only so you can audit
the transform or re-run it on the source tree if you change your consumer's
expectations.

## Re-run (if ever needed)

```bash
# single case (a source case dir containing raw/ + metadata.json):
python mr2_load_adapter.py --in <source_case_dir>

# all cases under a source root:
python mr2_load_adapter.py --in <source_root> --all
```

Output lands in `<source_case>/mr2/` (READ-ONLY on raw/). The shipped delivery
already merges that output into each `<case>/raw/metrics/`.

## What the adapter does NOT touch
- `raw/traces/`, `raw/logs/`, `raw/operations/` are byte-identical to source
  (already aligned to your per-record shape).
- `raw/metrics/manifest.json` is structural (no source/quality fields); copied
  verbatim, not transformed.
- Trace shape is flatten-on-demand: native raw/traces/*.jsonl are call-tree
  (non-null parent_span_id); `--flat-traces` reshapes the DELIVERY copy to your
  flat form (1 row/trace_id, parent_span_id null, span_id==trace_id). Native
  call-tree is preserved in the repo either way. See open_question #4 in
  `_delivery_README.md`.
"""


DELIVERY_README_TMPL = """# RecWeb2 k8s_pilot RCA benchmark - delivery for shijie

## What this is
RecWeb2 k8s_pilot **multi-root-cause RCA benchmark**, packaged in your per-case
folder layout. Carrier = 25-microservice e-commerce recommender (Flask services
+ FastAPI SASRec sequential recommender + LangGraph multi-agent + DeepSeek LLM
rerank), deployed on docker-desktop Kubernetes with Chaos Mesh fault injection.

This tree: **{n_cases} cases, {n_fault_types} distinct fault_type slugs**.

## Format alignment status (your consumer can load directly)
- `raw/` modality structure (`metrics/` + `traces/` + `logs/` + `operations/`)
  is **aligned to your per-case format** (REF-shijie-k8s-v2-sample-schema.md).
- `adapter/mr2_load_adapter.py` is **pre-applied** to every case. Your consumer
  can directly load `<case>/raw/metrics/metrics_v2.jsonl` and:
  - `filter(record['source'] == 'prometheus')` returns NON-zero rows (all our
    emit sources - cadvisor/kube_state/otel/http_probe/nginx_config/mysql/host -
    are aliased to `prometheus`; original kept under `labels.source_raw`).
  - `record['quality']` and `record['container']` are present on every record
    (no KeyError). `unit` is normalized to your vocab
    (boolean/attempts/replicas/restarts/events/epoch_seconds/
    periods_per_second/milliseconds); units outside vocab + REF-silent are kept
    as-is (listed in adapter WARN).
- `metadata.json` is in your shape: `schema_version=v1.2`,
  `observation_stages` is an **object keyed by stage** with
  `window_start_at`/`window_end_at`/`window_seconds`/`poll_interval_seconds`
  plus per-modality `*_manifest` path + `*_filter` + `*_validation_status`
  (metrics/traces/logs) + `gate_passed` + `status`, `root_causes` is a
  **string list** (service names), `component_fault_windows` uses
  `start_time`/`end_time`, `validation_results` uses `status=passed`.
- `quality.json` uses the `stages` wrapper + `schema_version=metrics-quality.v2.1`
  (per-stage 27-metric `required_metrics` block byte-identical to source).
- `groundtruth.json` is adapter-normalized to your shape: top-level is exactly
  your 13-key set (`sample_id`/`answer_type`/`root_count`/`fault_category`/
  `composition_type`/`interaction_pattern`/`root_cause_services`/
  `root_cause_instances`/`fault_types`/`injection_faults`/
  `component_ground_truth`/`component_fault_windows`/`overlap_window`),
  `component_ground_truth[]` items carry exactly your 10-key set (no
  `chaos_engine`/`crd`/`intensity`), and `overlap_window` /
  `component_fault_windows` use `start_time`/`end_time`. **Answer values are
  byte-identical to source** (only key names + dropped additive provenance
  keys changed). `summary.md` is **GENERATED** in your 6-bullet minimal format
  (H1 = folder name + storage_layout / sample_status / ready_for_release /
  observed metric kinds / logs / root evidence), derived from metadata +
  per-case metrics_v2/logs + validation gate detail.

## open questions for your feedback (affect final format)
1. **code/cadvisor units** - a few metric `unit` values
   (e.g. code/bytes/cores/errors_per_second/packets_per_second/bytes_per_second)
   are outside your known vocab and REF-silent. Adapter keeps them as-is + lists
   in WARN. Confirm or hand me the canonical name.
2. **observation_stages key names** - **RESOLVED (this delivery aligned to your
   sample):** we emit `window_start_at`/`window_end_at`/`window_seconds` +
   `poll_interval_seconds` + per-modality `*_manifest`/`*_filter`/`*_validation_status`,
   matching your sample's stage object shape byte-for-byte.
3. **quality.json schema_version** - **RESOLVED (this delivery aligned to your
   sample):** `metrics-quality.v2.1` (read from your sample's `quality.json`),
   set identically at top + per-stage.
4. **Trace shape** - our native `raw/traces/*.jsonl` are call-tree (non-null
   `parent_span_id`); under `--flat-traces` the DELIVERY copy is flattened to
   your form (1 row/trace_id, parent_span_id null, span_id==trace_id). **Does
   your algorithm consume `line == trace` (one span per record) or group by
   `trace_id`?** (Decides whether the flat delivery suffices or you want the
   call-tree version, which we keep in native.)

## Folder naming convention
Folders are **derived** (not your original slot id):
`mr{{root_count}}_{{cat}}_{{class}}-{{fault_type}}-k8s-v2-formal-r1-{{ts}}`
- `cat`: single / intra / cross (from metadata.category).
- `class`: net / config / res / lif (short for faults[0].fault_class).
- `fault_type`: `metadata.config.fault` slug (e.g. `dual_timeout_retry`,
  `net_delay_x_cfg_connect`, `net_delay_single`).
- `ts`: metadata.created_at -> YYYYMMDDHHMMSS.
Collisions get `_r2`/`_r3`. **Folder-name <-> original case_id mapping is in
`MANIFEST.json`** (`folder_name`, `source_case_id`). Name-derivation failures
fall back to the original `sample_id`.

## Re-running the adapter
See `adapter/README.md`. Normally unnecessary (already pre-applied).

## Pointers
- Machine-readable case index + mapping: **`MANIFEST.json`**.
- Format authority: `docs/blackboard/REF-shijie-k8s-v2-sample-schema.md`
  (shipped separately in the RecWeb2 repo, not in this delivery).
- Adapter transform spec + acceptance: `docs/blackboard/archive/TASK-K8S-M4b-impl-spec.md`.
"""


def write_delivery_readme(out_dir, n_cases, n_fault_types, dry_run=False):
    p = out_dir / "_delivery_README.md"
    content = DELIVERY_README_TMPL.format(
        n_cases=n_cases, n_fault_types=n_fault_types)
    if not dry_run:
        p.write_text(content, encoding="utf-8")
    return p


def write_adapter_dir(out_dir, dry_run=False):
    """Copy mr2_load_adapter.py + write adapter/README.md."""
    d = out_dir / "adapter"
    written = []
    if not dry_run:
        d.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_THIS_DIR / "mr2_load_adapter.py", d / "mr2_load_adapter.py")
        (d / "README.md").write_text(ADAPTER_README, encoding="utf-8")
    written.append(d / "mr2_load_adapter.py")
    written.append(d / "README.md")
    return written


def write_manifest(out_dir, entries, meta_summary, dry_run=False):
    """entries: list of dicts (one per case). meta_summary: aggregate dict."""
    p = out_dir / "MANIFEST.json"
    doc = {
        "delivery_format": "shijie_k8s_v2_per_case",
        "delivery_formal_tag": DELIVERY_FORMAL_TAG,
        "source_pilot_dir": str(meta_summary.get("source_pilot_dir")),
        "generated_by": "scripts/chaos/ctk/package_for_delivery.py",
        "case_count": len(entries),
        "n_fault_types": meta_summary.get("n_fault_types"),
        "cases": entries,
    }
    if not dry_run:
        p.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n",
                     encoding="utf-8")
    return p


# Fault slugs that are NET-class (need --catalog-direct-base for isolation gate).
# (Matches chaos_k8s_runner --fault choices: net_delay_* / net_loss_* .)
NET_FAULT_PREFIXES = ("net_delay", "net_loss")


def write_collect_script(out_case_dir, meta, dry_run=False):
    """Write scripts/collect-<sample_id>.ps1 = reconstructed chaos_k8s_runner.py
    command for THIS case. Source of truth = metadata.config (the params the case
    was actually run with).

    Flags emitted:
      --case-id=<sample_id>
      --fault=<config.fault>
      --item=<config.carrier_item>  (default 0071341196)
      --stage-seconds=<config.stage_seconds>
      --poll=<config.poll>
      dual (config.f2_offset_seconds present): --f2-offset-seconds / --f2-duration-seconds
      config.user_token present: --user-token=<...>
      NET-class fault (net_delay/net_loss): --catalog-direct-base http://127.0.0.1:5005
      config.keep_carrier truthy: --keep-carrier

    Intensity params (net_delay_ms / net_jitter_ms / f2_connect_timeout_ms /
    db_lock hold/gap / host_cpu workers / etc.) are NOT CLI flags - they are
    embedded in the runner's fault profile and live in metadata.config (NOT
    passed on the command line; not echoed in the .ps1).

    Returns the path (or None if meta lacks config.fault / sample_id).
    """
    cfg = meta.get("config") if isinstance(meta, dict) else None
    if not isinstance(cfg, dict):
        return None
    sample_id = meta.get("sample_id")
    fault = cfg.get("fault")
    if not sample_id or not fault:
        return None

    runner_rel = "scripts/chaos/ctk/chaos_k8s_runner.py"
    item = cfg.get("carrier_item") or "0071341196"
    stage_seconds = cfg.get("stage_seconds")
    poll = cfg.get("poll")

    # Build CLI flag list (order stable).
    flags = [
        "--case-id=%s" % sample_id,
        "--fault=%s" % fault,
        "--item=%s" % item,
    ]
    if stage_seconds is not None:
        flags.append("--stage-seconds=%s" % stage_seconds)
    if poll is not None:
        flags.append("--poll=%s" % poll)
    # dual fault windows (present in cross/dual-root cases).
    f2_off = cfg.get("f2_offset_seconds")
    f2_dur = cfg.get("f2_duration_seconds")
    if f2_off is not None:
        flags.append("--f2-offset-seconds=%s" % f2_off)
    if f2_dur is not None:
        flags.append("--f2-duration-seconds=%s" % f2_dur)
    # user_token (host_cpu / db_lock scenarios require a disjoint user).
    if cfg.get("user_token"):
        flags.append("--user-token=%s" % cfg.get("user_token"))
    # catalog-direct-base: explicit (config provenance) for deep non-NET, default for NET.
    cdb = cfg.get("catalog_direct_base")
    if not cdb and any(fault == p or fault.startswith(p) for p in NET_FAULT_PREFIXES):
        cdb = "http://127.0.0.1:5005"
    if cdb:
        flags.append("--catalog-direct-base=%s" % cdb)
    # keep_carrier (fault leaves carrier-side residue).
    if cfg.get("keep_carrier"):
        flags.append("--keep-carrier")
    # --deep (deep-gate combos; runner FATAL without it — inferred from fault).
    DEEP_FAULTS = {"host_cpu_x_svccpu", "dual_podfail_staggered", "net_delay_x_cfg_connect",
                   "net_delay_x_inv_latency", "sasrec_cpu_x_catalog_netdelay",
                   "inv_latency_x_runtime_exc", "catalog_latency_x_cfg_timeout",
                   "catalog_latency_x_net_loss", "net_delay_x_svc_cpu",
                   "catalog_latency_x_svc_cpu"}
    if fault in DEEP_FAULTS:
        flags.append("--deep")
    # multi-carrier spec + inventory-direct + cart token (config provenance).
    if cfg.get("carriers_spec"):
        flags.append("--carriers=%s" % cfg.get("carriers_spec"))
    if cfg.get("inventory_direct_base"):
        flags.append("--inventory-direct-base=%s" % cfg.get("inventory_direct_base"))
    if cfg.get("cart_user_token"):
        flags.append("--cart-user-token=%s" % cfg.get("cart_user_token"))

    # Title-only header (no prose/notes block) - bare style. Intensity values
    # stay in metadata.config (source of truth), not echoed here.
    header = "# collect-%s.ps1\n" % sample_id

    body = "python %s %s\n" % (runner_rel, " ".join(flags))
    content = header + body

    scripts_dir = out_case_dir / "scripts"
    fname = "collect-%s.ps1" % sample_id
    p = scripts_dir / fname
    if not dry_run:
        scripts_dir.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Package RecWeb2 k8s_pilot into shijie per-case delivery tree.")
    ap.add_argument("--pilot-dir", default="datasets/k8s_pilot/combined",
                    help="source pilot dir (default combined; also single/dual/triple)")
    ap.add_argument("--out", default=None,
                    help="output dir (default <repo>/datasets/k8s_pilot_delivery/<arity>/)")
    ap.add_argument("--keep-source", action="store_true",
                    help="skip the adapter write step entirely; reuse whatever is already "
                         "in datasets/_runtime/package/<arity>/<case_id>/ (else that case errors).")
    ap.add_argument("--force", action="store_true",
                    help="(no-op since 2026-07-13) the adapter now ALWAYS regenerates; "
                         "there is no stale-reuse path left to force past. Kept so existing "
                         "command lines / collect-*.sh keep working.")
    ap.add_argument("--limit", type=int, default=None,
                    help="process only first N cases (smoke test).")
    ap.add_argument("--dry-run", action="store_true",
                    help="plan only: print folder names + planned writes, "
                         "write nothing (does run adapter unless --keep-source).")
    ap.add_argument("--bare", action="store_true",
                    help="bare mode: skip top-level _delivery_README.md / adapter/ / "
                         "MANIFEST.json writes; per-case folders lay flat under <out>/. "
                         "Default off (combined behavior: write all three top-level).")
    ap.add_argument("--eval-only", action="store_true",
                    help="only include cases whose case_id (folder name) is in "
                         "features_k8s.csv (the real eval set); excludes dev artifacts "
                         "(reg/fix/smoke runs that live under cases/ but are not in the "
                         "eval feature view). "
                         "★ No features_k8s.csv -> HARD ERROR (was: silently package the "
                         "FULL set while the caller believed it was the eval set).")
    ap.add_argument("--features-dir", default=None,
                    help="dir holding features_k8s.csv for --eval-only "
                         "(default: datasets/_runtime/features; see dataset_registry.feature_csv).")
    ap.add_argument("--flat-traces", action="store_true",
                    help="flatten raw/traces/*_traces.jsonl in the DELIVERY to shijie "
                         "flat shape (1 row per trace_id, parent_span_id null, span_id==trace_id) "
                         "+ reshape traces/manifest.json to traces-manifest.v2.1 + resync "
                         "metadata.trace_stats + annotate validation_results. "
                         "Native source keeps the full call-tree; only the delivery copy "
                         "is flattened. Default off (ship native call-tree traces).")
    # --- ADDITIVE-ONLY delivery extras (2026-07-13). All default OFF: a flagless
    #     run stays BYTE-IDENTICAL to the accepted 20260709 deliveries. ---
    ap.add_argument("--with-calltree", action="store_true",
                    help="ADDITIVE: also ship raw/traces_calltree/ = the NATIVE call-tree "
                         "traces (non-null parent_span_id + CHILD_OF references) + README "
                         "+ calltree_stats.json. raw/traces/ (flat) is untouched, so an "
                         "existing loader sees no change; trace-DAG methods (Eadro) get a "
                         "real call graph. Also makes traces manifest/trace_profile declare "
                         "cross_service_span_graph_available=true (it now IS).")
    ap.add_argument("--with-eval", action="store_true",
                    help="ADDITIVE: also ship eval/ = RCAEval-ready wide table "
                         "(data.csv: time + <svc>__<metric>, full column universe) + "
                         "inject_time.txt + build_info.json + README.md, built by "
                         "m9_adapter.build_wide(gap_aware=True) from the NATIVE metrics. "
                         "Needs pandas/numpy.")
    ap.add_argument("--with-gt-distinct", action="store_true",
                    help="ADDITIVE: add groundtruth.n_distinct_root_services = "
                         "len(set(root_cause_services)). Folder arity counts injected "
                         "FAULTS, not distinct root SERVICES (30/140 cases differ), and "
                         "MRCBench R=|G| / IDCG=min(K,|G|) need |G|. Pure key add.")
    # --- P0-8 release gate (HANDOFF §7 P0-8). Default ON (safe): any case whose
    #     ready_for_release / sample_status / checksum_guard is false OR missing
    #     is rejected and NOT copied. --no-strict-gate is a legacy-debug escape
    #     hatch that warns but still copies (DO NOT use for real deliveries). ---
    gate = ap.add_mutually_exclusive_group()
    gate.add_argument("--strict-gate", dest="strict_gate", action="store_true",
                      default=True,
                      help="(default) P0-8: reject cases whose release fields are "
                           "false/missing; they are excluded and NOT copied.")
    gate.add_argument("--no-strict-gate", dest="strict_gate", action="store_false",
                      help="legacy-debug ONLY: warn on release-field failures but still "
                           "copy the case. Do NOT use for real deliveries.")
    a = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    pilot_dir = (repo_root / a.pilot_dir).resolve() if not Path(a.pilot_dir).is_absolute() \
        else Path(a.pilot_dir).resolve()
    if not pilot_dir.is_dir():
        print("[pkg] ERROR pilot-dir not found: %s" % pilot_dir, file=sys.stderr)
        return 2

    arity = pilot_dir.name  # combined/single/dual/triple
    out_dir = Path(a.out).resolve() if a.out else \
        (repo_root / ("datasets/k8s_pilot_delivery/%s" % arity)).resolve()

    # ★ native 采集树是只读的:交付包和 adapter 产物都【不许】落进 datasets/k8s_pilot/。
    #   一次性收口, 早炸早好(以前是写完才发现自己污染了源数据)。
    DR.assert_not_native(out_dir)
    # dry-run 不建目录(dry-run 就该一个字节都不写)
    pkg_runtime_root = DR.runtime_dir("package", arity, make=not a.dry_run)
    DR.assert_not_native(pkg_runtime_root)
    print("[pkg] adapter runtime dir: %s" % pkg_runtime_root)

    cases = find_cases(pilot_dir)
    if not cases:
        print("[pkg] ERROR no cases found under %s" % pilot_dir, file=sys.stderr)
        return 2
    if a.eval_only:
        # ★ fail-loud:csv 缺失 -> 直接退出。绝不"没过滤器就打全集"(见 load_eval_case_ids)。
        try:
            eval_ids = load_eval_case_ids(getattr(a, "features_dir", None))
        except FileNotFoundError as e:
            print("[pkg] ERROR --eval-only 需要 features_k8s.csv,但没找到 —— 拒绝打包。\n"
                  "      (若不过滤就打包,你会拿到【全集】却以为是评测集。)\n%s" % e,
                  file=sys.stderr)
            return 2
        before = len(cases)
        cases = [c for c in cases if c.name in eval_ids]
        print("[pkg] --eval-only: %d/%d cases in features_k8s.csv (excluded %d dev artifacts)"
              % (len(cases), before, before - len(cases)))
        if not cases:
            print("[pkg] ERROR --eval-only 过滤后【0 个 case】—— features_k8s.csv 与 pilot-dir 不匹配?",
                  file=sys.stderr)
            return 2
    if a.limit:
        cases = cases[:a.limit]
    print("[pkg] pilot=%s arity=%s cases=%d out=%s dry_run=%s keep_source=%s force=%s"
          % (pilot_dir, arity, len(cases), out_dir, a.dry_run, a.keep_source, a.force))

    if not a.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    used_names = set()
    entries = []
    fault_types_seen = set()
    errors = []
    global_stats = {"unmapped_sources": set(), "unmapped_units": set()}
    # P0-8 excluded ledger — cases rejected by the release gate. Printed as a
    # summary at the end (count + reason). A copy is NOT written to disk here;
    # build_full_delivery.py owns the cross-tree excluded_ledger.json.
    excluded = []  # list of {"case_id", "reason"}

    for case_dir in cases:
        # ----- P0-8 release gate (HANDOFF §7 P0-8). -----
        # Run BEFORE the adapter write / copy: a rejected case must not be
        # assembled into the delivery at all. --no-strict-gate downgrades the
        # hard reject to a warn-but-still-copy (legacy debug only).
        gate_ok, gate_reason = check_release_gate(case_dir)
        if not gate_ok:
            if a.strict_gate:
                print("[pkg] GATE-REJECT %s: %s (excluded; not copied)"
                      % (case_dir.name, gate_reason))
                excluded.append({"case_id": case_dir.name, "reason": gate_reason})
                continue
            else:
                print("[pkg] GATE-WARN %s: %s (--no-strict-gate: copied anyway)"
                      % (case_dir.name, gate_reason))
        try:
            # 1. Adapter -> datasets/_runtime/package/<tag>/<case_id>/  (NOT <case>/mr2/).
            #    ★ 2026-07-13 两处改动, 都是被真事故逼出来的:
            #    (a) 落点搬出 native 树 (assert_not_native 在 adapt_case 里把门).
            #    (b) 【总是重生】—— 旧逻辑是 "mr2/ 存在就复用, 除非 --force". 于是一份
            #        stale 的 adapter 产物(带着已经修好的旧 GT)会被原样抄进交付包:
            #        15 个错 GT 进包正是走的这条机械路径. 单 case 重生 ~19MB/30k 行, 成本
            #        可接受; 用正确性换这点时间, 换.
            mr2_dir = pkg_runtime_root / case_dir.name
            if a.keep_source:
                if not (mr2_dir / "metadata.json").exists():
                    raise FileNotFoundError(
                        "--keep-source set but %s/metadata.json missing; "
                        "run adapter first or drop --keep-source" % mr2_dir)
            elif not a.dry_run:
                adapter.adapt_case(case_dir, global_stats, out_dir=mr2_dir)

            # Read metadata (prefer adapter output if exists else native) for naming.
            meta_path = mr2_dir / "metadata.json" if (mr2_dir / "metadata.json").exists() \
                else case_dir / "metadata.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8"))

            # 2. Derive folder name.
            folder_name = derive_folder_name(meta, case_dir, used_names)
            out_case_dir = out_dir / folder_name

            # 3. Assemble.
            if not a.dry_run:
                out_case_dir.mkdir(parents=True, exist_ok=True)
            written = assemble_case(case_dir, out_case_dir, mr2_dir,
                                    dry_run=a.dry_run, meta=meta,
                                    flat_traces=a.flat_traces,
                                    with_calltree=a.with_calltree,
                                    with_eval=a.with_eval,
                                    gt_distinct=a.with_gt_distinct)

            # Track fault_type + entry.
            cfg = meta.get("config") or {}
            ft = cfg.get("fault") if isinstance(cfg, dict) else None
            if ft:
                fault_types_seen.add(ft)
            entries.append({
                "folder_name": folder_name,
                "source_case_id": meta.get("sample_id")
                                  or meta.get("formal_slot_id")
                                  or case_dir.name,
                "source_case_relpath": str(case_dir.relative_to(repo_root))
                                       if str(case_dir).startswith(str(repo_root))
                                       else str(case_dir),
                "root_count": meta.get("root_count"),
                "category": meta.get("category"),
                "fault_class_first": ((meta.get("faults") or [{}])[0]
                                      .get("fault_class") if meta.get("faults") else None),
                "fault_type": ft,
                "created_at": meta.get("created_at"),
                # adapter 现在【总是重生】(除非 --keep-source / --dry-run):不再有"复用旧 mr2"这条路。
                "adapter_reran": not (a.keep_source or a.dry_run),
            })
            print("[pkg] OK %s <- %s (rc=%s cat=%s ft=%s)"
                  % (folder_name, case_dir.name, meta.get("root_count"),
                     meta.get("category"), ft))
        except Exception as e:
            errors.append({"case": str(case_dir), "error": repr(e)})
            print("[pkg] ERR %s: %s" % (case_dir.name, e), file=sys.stderr)

    # 4. Top-level (skipped under --bare: per-case folders lay flat).
    if not a.dry_run and not a.bare:
        write_delivery_readme(out_dir, len(entries), len(fault_types_seen))
        write_adapter_dir(out_dir)
        write_manifest(out_dir, entries, {
            "source_pilot_dir": str(pilot_dir),
            "n_fault_types": len(fault_types_seen),
        })
        print("[pkg] top-level: _delivery_README.md, adapter/, MANIFEST.json")
    else:
        print("[pkg] top-level: skipped (--bare or --dry-run)")

    # WARN lists (mirror adapter's).
    if global_stats["unmapped_sources"]:
        print("[pkg] WARN unmapped sources (adapter aliased->prometheus, kept "
              "under labels.source_raw): "
              + ", ".join(sorted(global_stats["unmapped_sources"])))
    if global_stats["unmapped_units"]:
        print("[pkg] WARN unmapped units (REF silent, kept as-is): "
              + ", ".join(sorted(global_stats["unmapped_units"])))

    # P0-8 excluded-ledger summary (HANDOFF §7 P0-8: "any false/missing release
    # field immediately rejects the case"). Print how many cases were excluded
    # and why, so a human can see exactly what the gate swallowed.
    if excluded:
        from collections import Counter
        reason_counts = Counter(e["reason"] for e in excluded)
        print("[pkg] GATE excluded %d case(s) by reason:" % len(excluded))
        for reason, cnt in sorted(reason_counts.items()):
            print("      %3d x %s" % (cnt, reason))
        for e in excluded:
            print("      - %s: %s" % (e["case_id"], e["reason"]))
    else:
        print("[pkg] GATE: 0 cases excluded (all passed release gate)")

    print("[pkg] DONE cases_ok=%d cases_err=%d fault_types=%d out=%s"
          % (len(entries), len(errors), len(fault_types_seen), out_dir))
    if errors:
        print("[pkg] %d ERRORS (see stderr above):" % len(errors), file=sys.stderr)
        for e in errors:
            print("  - %s: %s" % (e["case"], e["error"]), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
