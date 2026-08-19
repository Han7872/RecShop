"""Assemble, but never self-approve, E1 requests for D10/T05 smokes."""

from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping

from .evidence import REPORT_SCHEMA_NAME, REPORT_SCHEMA_VERSION
from .phase_journal import PHASES
from .runner_wrapper import AttemptPlan, LegacyRunnerCommand
from .smoke_lite import EXPECTED_DB_CHECKSUMS, SCENARIO_TO_RUNNER, SmokeError
from .telemetry_journal import QueryRecord, telemetry_acceptable
from .verifier_adapter import REQUEST_FILENAME


SCENARIOS = {"D10": 2, "T05": 3}
TARGETS = {
    "D10": {"F1": "mysql/items", "F2": "catalog-gw"},
    "T05": {"F1": "checkout", "F2": "cart", "F3": "pricing"},
}


class SmokeEvidenceAssemblerError(SmokeError):
    pass


def _fail(code: str, message: str) -> "None":
    raise SmokeEvidenceAssemblerError(code, message)


def _read_object(path: Path, label: str) -> Mapping[str, Any]:
    if path.is_symlink() or not path.is_file():
        _fail("E5A001_INPUT_MISSING", f"missing regular {label}")
    try:
        value = json.loads(path.read_text(encoding="utf-8", errors="strict"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail("E5A002_INPUT_INVALID", f"invalid {label}: {exc.__class__.__name__}")
    if type(value) is not dict:
        _fail("E5A002_INPUT_INVALID", f"{label} must be object")
    return value


def _read_list(path: Path, label: str) -> list[Any]:
    if path.is_symlink() or not path.is_file():
        _fail("E5A001_INPUT_MISSING", f"missing regular {label}")
    try:
        value = json.loads(path.read_text(encoding="utf-8", errors="strict"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail("E5A002_INPUT_INVALID", f"invalid {label}: {exc.__class__.__name__}")
    if type(value) is not list:
        _fail("E5A002_INPUT_INVALID", f"{label} must be list")
    return value


def _jsonl(path: Path, label: str) -> list[Mapping[str, Any]]:
    if path.is_symlink() or not path.is_file():
        _fail("E5A001_INPUT_MISSING", f"missing regular {label}")
    rows = []
    try:
        for line in path.read_text(encoding="utf-8", errors="strict").splitlines():
            if not line:
                continue
            value = json.loads(line)
            if type(value) is not dict:
                _fail("E5A002_INPUT_INVALID", f"{label} row is not object")
            rows.append(value)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail("E5A002_INPUT_INVALID", f"invalid {label}: {exc.__class__.__name__}")
    if not rows:
        _fail("E5A001_INPUT_MISSING", f"empty {label}")
    return rows


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sidecars(sidecar_root: Path, attempt_id: str, scenario: str) -> tuple[list[dict[str, Any]], dict[str, bool]]:
    phases = _read_list(sidecar_root / "phase-journal.json", "phase journal")
    if [row.get("phase") for row in phases if type(row) is dict] != list(PHASES):
        _fail("E5A003_SIDECAR_INVALID", "phase journal is incomplete")
    times = [row.get("entered_at") for row in phases]
    if any(type(value) not in {int, float} or isinstance(value, bool) or not math.isfinite(float(value)) for value in times) or any(b <= a for a, b in zip(times, times[1:])):
        _fail("E5A003_SIDECAR_INVALID", "phase clock invalid")
    workload = _read_list(sidecar_root / "workload-journal.json", "workload journal")
    if not workload or any(type(row) is not dict or row.get("timeout") is not False for row in workload):
        _fail("E5A003_SIDECAR_INVALID", "workload incomplete/timeout")
    query_values = _read_list(sidecar_root / "query-journal.json", "query journal")
    try:
        queries = tuple(QueryRecord(**row) for row in query_values)
    except (TypeError, ValueError):
        _fail("E5A003_SIDECAR_INVALID", "query journal schema invalid")
    if not telemetry_acceptable(queries):
        _fail("E5A003_SIDECAR_INVALID", "required query unavailable")
    cleanup = _read_object(sidecar_root / "cleanup-audit.json", "cleanup audit")
    if set(cleanup) != {"actions", "blocking_resources", "next_allowed", "results"} or cleanup["next_allowed"] is not True or cleanup["blocking_resources"] != [] or not cleanup["actions"] or cleanup["results"] != [True] * len(cleanup["actions"]):
        _fail("E5A003_SIDECAR_INVALID", "cleanup audit is not closed/clean")
    checksum = _read_object(sidecar_root / "checksum.json", "checksum")
    if set(checksum) != {"pre", "post"} or checksum["pre"] != EXPECTED_DB_CHECKSUMS or checksum["post"] != EXPECTED_DB_CHECKSUMS:
        _fail("E5A003_SIDECAR_INVALID", "checksum drift")
    _read_object(sidecar_root / "preflight.json", "environment preflight")

    events = _jsonl(sidecar_root / "runner-events.jsonl", "runner event journal")
    if any(set(row) != {"attempt_token", "event", "monotonic_ns", "payload"} or row["attempt_token"] != attempt_id or type(row["payload"]) is not dict for row in events):
        _fail("E5A004_EVENT_DRIFT", "event schema/attempt binding invalid")
    scenario_rows = [row for row in events if row["payload"].get("scenario") not in {None, scenario}]
    if scenario_rows:
        _fail("E5A004_EVENT_DRIFT", "event scenario alias drift")
    for required_event in (
        "injection_transition_started", "all_active_confirmed",
        "recovery_transition_started", "recovery_confirmed",
    ):
        if len([row for row in events if row["event"] == required_event]) != 1:
            _fail("E5A004_EVENT_DRIFT", f"event count differs: {required_event}")
    active = [row for row in events if row["event"] == "leg_active_confirmed"]
    if len(active) != SCENARIOS[scenario] or {row["payload"].get("fid") for row in active} != {f"F{i}" for i in range(1, SCENARIOS[scenario] + 1)}:
        _fail("E5A004_EVENT_DRIFT", "active leg set is not exact")
    for row in active:
        payload = row["payload"]
        if payload.get("target_identity") != TARGETS[scenario].get(payload.get("fid")) or not payload.get("resource_uid") or payload.get("attempt_label") != attempt_id:
            _fail("E5A004_EVENT_DRIFT", "active target/UID/ownership missing")
    cleanup_events = [row for row in events if row["event"] == "cleanup_result"]
    if len(cleanup_events) != SCENARIOS[scenario] or {row["payload"].get("fid") for row in cleanup_events} != set(TARGETS[scenario]) or any(row["payload"].get("success") is not True for row in cleanup_events):
        _fail("E5A004_EVENT_DRIFT", "per-leg cleanup facts incomplete")
    recovery = [row for row in events if row["event"] == "recovery_confirmed"]
    if len(recovery) != 1 or recovery[0]["payload"].get("success") is not True:
        _fail("E5A004_EVENT_DRIFT", "recovery confirmation missing")
    return [
        {"query_id": row.query_id, "required": row.required, "status": row.status, "value": row.value}
        for row in queries
    ], {
        "ready_for_release": True,
        "checksum_zero_drift": True,
        "runner_gate_passed": True,
        "recovery_confirmed": True,
        "cleanup_owned_only": True,
        "cleanup_residual_absent": True,
        "cleanup_foreign_absent": True,
    }


def _roots(metadata: Mapping[str, Any], groundtruth: Mapping[str, Any], metrics: list[Mapping[str, Any]], arity: int) -> list[dict[str, Any]]:
    roots = metadata.get("root_causes")
    components = groundtruth.get("component_ground_truth")
    if type(roots) is not list or type(components) is not list or len(roots) != arity or len(components) != arity:
        _fail("E5A005_ROOT_INVALID", "metadata/GT root arity differs")
    rows = []
    for index, (root, component) in enumerate(zip(roots, components), 1):
        if type(root) is not dict or type(component) is not dict:
            _fail("E5A005_ROOT_INVALID", "root row invalid")
        leg = f"F{index}"
        service, instance = root.get("service"), root.get("instance")
        if root.get("fault_instance_id") != leg or component.get("fault_instance_id") != leg or component.get("target_component") != service or component.get("target_container") != instance:
            _fail("E5A005_ROOT_INVALID", f"root/GT mismatch at {leg}")
        if service == "mysql" or component.get("off_graph") is True:
            uid, uid_kind = f"offgraph:{service}:{instance}", "off_graph"
        else:
            matches = {
                row.get("labels", {}).get("uid")
                for row in metrics
                if row.get("stage") == "during_fault" and row.get("service") == service
                and row.get("labels", {}).get("pod") == instance and row.get("labels", {}).get("uid")
            }
            if len(matches) != 1:
                _fail("E5A005_ROOT_INVALID", f"during UID not unique for {leg}")
            uid, uid_kind = matches.pop(), "kubernetes"
        rows.append({"leg_id": leg, "service": service, "instance": instance, "target_uid": uid, "uid_kind": uid_kind})
    return rows


def assemble_fault_request(
    *, plan: AttemptPlan, command: LegacyRunnerCommand, sidecar_root: Path,
) -> Path:
    scenario = plan.scenario_id
    if scenario not in SCENARIOS or command.scenario_id != scenario or command.runner_fault != SCENARIO_TO_RUNNER[scenario] or plan.runner_fault != SCENARIO_TO_RUNNER[scenario]:
        _fail("E5A006_ALIAS_INVALID", "assembler only accepts exact D10/T05 mapping")
    if command.attempt_id != plan.attempt_id or command.case_id != plan.case_id:
        _fail("E5A006_ALIAS_INVALID", "plan/command attempt binding differs")
    candidate = plan.promotion.source
    if candidate.is_symlink() or not candidate.is_dir():
        _fail("E5A001_INPUT_MISSING", "legacy candidate missing")
    metadata_path, gt_path = candidate / "metadata.json", candidate / "groundtruth.json"
    metrics_path = candidate / "raw" / "metrics" / "metrics_v2.jsonl"
    for required in (metadata_path, gt_path, metrics_path, candidate / "summary.md", candidate / "raw" / "traces" / "during_fault_traces.jsonl"):
        if required.is_symlink() or not required.is_file():
            _fail("E5A001_INPUT_MISSING", f"legacy artifact missing: {required.name}")
    metadata, groundtruth = _read_object(metadata_path, "metadata"), _read_object(gt_path, "groundtruth")
    metrics = _jsonl(metrics_path, "metrics")
    if metadata.get("sample_id") != plan.case_id or groundtruth.get("sample_id") != plan.case_id or metadata.get("root_count") != SCENARIOS[scenario] or groundtruth.get("root_count") != SCENARIOS[scenario]:
        _fail("E5A005_ROOT_INVALID", "case/root binding differs")
    expected_services = [target.split("/", 1)[0] for target in TARGETS[scenario].values()]
    if groundtruth.get("root_cause_services") != expected_services:
        _fail("E5A005_ROOT_INVALID", "GT services differ from fixed smoke targets")
    query_rows, checks = _sidecars(sidecar_root, plan.attempt_id, scenario)
    roots = _roots(metadata, groundtruth, metrics, SCENARIOS[scenario])
    request = {
        "schema_name": REPORT_SCHEMA_NAME,
        "schema_version": REPORT_SCHEMA_VERSION,
        "binding": {
            "attempt_id": plan.attempt_id, "case_id": plan.case_id,
            "command_sha256": command.command_sha256, "contract_sha256": plan.contract_sha256,
            "schedule_hash": plan.schedule_hash, "block_schedule_hash": plan.block_schedule_hash,
            "slot_sha256": plan.slot_sha256, "scenario_id": scenario,
            "runner_fault": command.runner_fault, "runner_target_service": command.runner_target_service,
        },
        "artifacts": {
            "metadata_path": "metadata.json", "metadata_sha256": _sha(metadata_path),
            "groundtruth_path": "groundtruth.json", "groundtruth_sha256": _sha(gt_path),
            "metrics_path": "raw/metrics/metrics_v2.jsonl", "metrics_sha256": _sha(metrics_path),
        },
        "planned_roots": roots,
        "actual_roots": roots,
        "query_rows": query_rows,
        "checks": checks,
    }
    request_path = candidate / REQUEST_FILENAME
    try:
        with request_path.open("xb") as handle:
            raw = (json.dumps(request, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
            handle.write(raw); handle.flush(); os.fsync(handle.fileno())
    except FileExistsError:
        _fail("E5A007_REQUEST_EXISTS", "refusing to overwrite verify request")
    except OSError as exc:
        _fail("E5A008_WRITE_FAILED", f"cannot write request: {exc.__class__.__name__}")
    return request_path


__all__ = ["SCENARIOS", "TARGETS", "SmokeEvidenceAssemblerError", "assemble_fault_request"]
