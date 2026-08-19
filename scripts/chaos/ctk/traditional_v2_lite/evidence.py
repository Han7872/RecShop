"""Fail-closed, offline evidence evaluation for a Lite-1 candidate case.

The frozen legacy runner does not record the wrapper attempt id, command hash,
Kubernetes UID binding, query outcome taxonomy, or cleanup ownership.  This
module therefore never infers those facts from legacy metadata.  A later live
adapter must persist ``verify-report.json`` using the closed schema below.

Evaluation is existing-files-only.  It does not execute ``verify_dual.py``,
spawn a process, access Kubernetes, or use the network.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Mapping, Protocol

from .contract import FROZEN_SCENARIO_UNIVERSE
from .runner_wrapper import (
    AttemptPlan,
    EvidenceBundle,
    ExecutionResult,
    LegacyRunnerCommand,
    LiteWrapperError,
)
from .verifier_adapter import (
    MANIFEST_FILENAME,
    MANIFEST_SCHEMA_NAME,
    MANIFEST_SCHEMA_VERSION,
    REQUEST_FILENAME,
    RESULT_FILENAME,
    RESULT_SCHEMA_NAME,
    RESULT_SCHEMA_VERSION,
    STDERR_FILENAME,
    STDOUT_FILENAME,
)


REPORT_FILENAME = "verify-report.json"
REPORT_SCHEMA_NAME = "traditional_v2_lite_strict_evidence"
REPORT_SCHEMA_VERSION = "1.2.0"
VERIFIER_KIND = "verify_dual.py/3-part+instance"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_K8S_UID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
_LEG_RE = re.compile(r"F[1-9][0-9]*\Z")
_SCENARIO_BY_ID = {spec.scenario_id: spec for spec in FROZEN_SCENARIO_UNIVERSE}

_REPORT_KEYS = {
    "schema_name",
    "schema_version",
    "binding",
    "artifacts",
    "verifier",
    "planned_roots",
    "actual_roots",
    "query_rows",
    "checks",
}
_BINDING_KEYS = {
    "attempt_id",
    "case_id",
    "command_sha256",
    "contract_sha256",
    "schedule_hash",
    "block_schedule_hash",
    "slot_sha256",
    "scenario_id",
    "runner_fault",
    "runner_target_service",
}
_ARTIFACT_KEYS = {
    "metadata_path",
    "metadata_sha256",
    "groundtruth_path",
    "groundtruth_sha256",
    "metrics_path",
    "metrics_sha256",
}
_VERIFIER_KEYS = {
    "kind",
    "passed",
    "request_path",
    "request_sha256",
    "verifier_path",
    "verifier_sha256",
    "argv",
    "shell",
    "exit_code",
    "stdout_path",
    "stdout_sha256",
    "stderr_path",
    "stderr_sha256",
    "candidate_manifest_path",
    "candidate_manifest_sha256",
    "result_path",
    "result_sha256",
}
_MANIFEST_KEYS = {"schema_name", "schema_version", "entries"}
_MANIFEST_ENTRY_KEYS = {"path", "size", "sha256"}
_RESULT_KEYS = {
    "schema_name",
    "schema_version",
    "request_sha256",
    "verifier_path",
    "verifier_sha256",
    "argv",
    "shell",
    "exit_code",
    "stdout_sha256",
    "stderr_sha256",
    "candidate_manifest_sha256",
    "passed",
}
_VERIFIER_INPUT_PATHS = (
    "metadata.json",
    "groundtruth.json",
    "summary.md",
    "raw/traces/during_fault_traces.jsonl",
    "raw/metrics/metrics_v2.jsonl",
)
_ROOT_KEYS = {"leg_id", "service", "instance", "target_uid", "uid_kind"}
_QUERY_KEYS = {"query_id", "required", "status", "value"}
_CHECK_KEYS = {
    "ready_for_release",
    "checksum_zero_drift",
    "runner_gate_passed",
    "recovery_confirmed",
    "cleanup_owned_only",
    "cleanup_residual_absent",
    "cleanup_foreign_absent",
}
_QUERY_STATUSES = {
    "value",
    "zero",
    "no_series",
    "query_error",
    "parse_error",
    "timeout",
}
_QUERY_FAILURES = {"no_series", "query_error", "parse_error", "timeout"}

# Union observed in the frozen k8s.v2.1 runner output.  Scenario-specific keys
# remain optional, but a new unknown top-level key is rejected until reviewed.
_METADATA_ALLOWED = {
    "arity",
    "artifacts",
    "case_id",
    "category",
    "chaos_engine",
    "checksum_guard",
    "collector",
    "component_fault_windows",
    "composition_type",
    "config",
    "created_at",
    "faults",
    "formal_slot_id",
    "ground_truth",
    "interaction_pattern",
    "isolation_check",
    "isolation_degraded",
    "kubernetes_context",
    "layer_design",
    "metric_schema_version",
    "namespace",
    "observation_stages",
    "overlap_window",
    "overlap_windows",
    "path_relation",
    "phase",
    "platform",
    "probe_path",
    "prometheus",
    "ready_for_release",
    "recagent_code_caveat",
    "recagent_image",
    "root_causes",
    "root_count",
    "root_metric_contract",
    "run_id",
    "sample_id",
    "sample_status",
    "scenario_name",
    "schema_version",
    "stage_layout",
    "storage_layout",
    "system",
    "trace_stats",
    "traffic_error_stats",
    "updated_at",
    "validation_complete",
    "validation_results",
}
_METADATA_REQUIRED = {
    "schema_version",
    "stage_layout",
    "sample_id",
    "run_id",
    "root_count",
    "faults",
    "root_causes",
    "observation_stages",
    "validation_results",
    "root_metric_contract",
    "ground_truth",
    "artifacts",
    "checksum_guard",
    "sample_status",
    "ready_for_release",
    "validation_complete",
    "formal_slot_id",
}
_GROUNDTRUTH_ALLOWED = {
    "_agent_layer_note",
    "affected_services",
    "answer_type",
    "case_id",
    "component_fault_windows",
    "component_ground_truth",
    "composition_type",
    "fault_category",
    "fault_types",
    "injection_faults",
    "interaction_pattern",
    "isolation_degraded",
    "n_distinct_root_services",
    "off_graph_metadata",
    "overlap_window",
    "overlap_windows",
    "path_relation",
    "root_cause_instances",
    "root_cause_services",
    "root_count",
    "root_metric_contract",
    "run_id",
    "sample_id",
    "sli_gate",
}
_GROUNDTRUTH_REQUIRED = {
    "sample_id",
    "run_id",
    "root_count",
    "root_cause_services",
    "root_cause_instances",
    "fault_types",
    "injection_faults",
    "component_ground_truth",
    "root_metric_contract",
    "sli_gate",
}
_EMBEDDED_GT_ALLOWED = {
    "affected_services",
    "answer_type",
    "component_fault_windows",
    "component_ground_truth",
    "composition_type",
    "fault_category",
    "fault_types",
    "injection_faults",
    "interaction_pattern",
    "interaction_pattern_fallback",
    "interaction_pattern_hedged",
    "off_graph_metadata",
    "overlap_window",
    "overlap_windows",
    "root_cause_instances",
    "root_cause_services",
    "root_count",
}
_EMBEDDED_GT_REQUIRED = {
    "component_ground_truth",
    "fault_types",
    "injection_faults",
    "root_cause_instances",
    "root_cause_services",
    "root_count",
}
_FAULT_KEYS = {
    "component_ground_truth",
    "fault_class",
    "fault_instance_id",
    "fault_type",
    "injected_at",
    "injection_fault",
    "recovered_at",
    "role",
    "status",
    "target_component",
    "target_container",
}
_ROOT_CAUSE_KEYS = {"fault_instance_id", "instance", "role", "service"}
_COMPONENT_GT_ALLOWED = {
    "carrier_observed_status_code",
    "carrier_observed_status_code_basis",
    "chaos_engine",
    "crd",
    "fault_class",
    "fault_instance_id",
    "fault_type",
    "gateway_status_code",
    "gateway_status_code_basis",
    "injected_at",
    "injection_fault",
    "intensity",
    "multi_victim",
    "off_graph",
    "recovered_at",
    "role",
    "signature",
    "status",
    "target_component",
    "target_container",
}
_COMPONENT_GT_REQUIRED = {
    "fault_instance_id",
    "fault_class",
    "fault_type",
    "injection_fault",
    "target_component",
    "role",
    "injected_at",
    "recovered_at",
    "status",
}
_VALIDATION_KEYS = {"id", "status", "detail", "notes"}
_CHECKSUM_KEYS = {"pre", "post", "baseline", "zero_drift"}
_OBSERVATION_KEYS = {
    "stage",
    "window_start",
    "window_end",
    "seconds",
    "poll_interval_seconds",
    "expected_snapshots",
    "observed_snapshots",
    "gate_passed",
}
_ROOT_METRIC_KEYS = {"F1", "F2", "F3", "notes", "valid"}
_SLI_GATE_KEYS = {"gate_passed", "evidence"}
_METRIC_KEYS = {
    "schema_version",
    "timestamp",
    "stage",
    "run_id",
    "source",
    "entity_type",
    "entity",
    "service",
    "metric",
    "value",
    "unit",
    "metric_type",
    "labels",
    "fault_window_membership",
}
_METRIC_LABEL_KEYS = {
    "carrier_name",
    "carrier_role",
    "container",
    "deployment",
    "instance",
    "namespace",
    "node",
    "pod",
    "pod_regex",
    "status_code",
    "success",
    "target_service",
    "uid",
}


class StrictEvidenceError(LiteWrapperError):
    """Stable fail-closed error from the existing-only evaluator."""


class ProvenanceWriter(Protocol):
    def write_report(
        self,
        *,
        plan: AttemptPlan,
        command: LegacyRunnerCommand,
        result: ExecutionResult,
    ) -> Path:
        """Persist one closed verifier report before strict re-evaluation."""


def _fail(code: str, message: str) -> "None":
    raise StrictEvidenceError(code, message)


def _object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail("L1E003_DUPLICATE_KEY", f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> "None":
    _fail("L1E004_NONFINITE_NUMBER", f"non-finite JSON number: {value}")


def _decode_json(raw: bytes, label: str) -> Any:
    if raw.startswith(b"\xef\xbb\xbf"):
        _fail("L1E002_INVALID_UTF8", f"{label} contains a UTF-8 BOM")
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        _fail("L1E002_INVALID_UTF8", f"{label} is not strict UTF-8: {exc}")
    try:
        return json.loads(
            text,
            object_pairs_hook=_object_no_duplicates,
            parse_constant=_reject_constant,
        )
    except StrictEvidenceError:
        raise
    except (json.JSONDecodeError, RecursionError) as exc:
        _fail("L1E005_INVALID_JSON", f"{label} is not valid JSON: {exc}")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if type(value) is not dict:
        _fail("L1E006_SCHEMA_INVALID", f"{label} must be an object")
    return value


def _require_list(value: Any, label: str) -> list[Any]:
    if type(value) is not list:
        _fail("L1E006_SCHEMA_INVALID", f"{label} must be an array")
    return value


def _closed_keys(
    value: Mapping[str, Any],
    *,
    allowed: set[str],
    required: set[str] | None,
    label: str,
) -> None:
    keys = set(value)
    unknown = sorted(keys - allowed)
    missing = sorted((required or set()) - keys)
    if unknown:
        _fail("L1E007_UNKNOWN_KEY", f"{label} unknown keys: {unknown}")
    if missing:
        _fail("L1E006_SCHEMA_INVALID", f"{label} missing keys: {missing}")


def _string(value: Any, label: str) -> str:
    if type(value) is not str or not value or "\x00" in value:
        _fail("L1E006_SCHEMA_INVALID", f"{label} must be a non-empty string")
    return value


def _boolean_true(value: Any, label: str) -> None:
    if value is not True:
        _fail("L1E016_REQUIRED_CHECK_FAILED", f"{label} must be true")


def _hash_string(value: Any, label: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        _fail("L1E006_SCHEMA_INVALID", f"{label} must be lowercase SHA-256")
    return value


def _inside_regular_file(candidate: Path, relative: str, label: str) -> Path:
    if type(relative) is not str or not relative or "\\" in relative:
        _fail("L1E006_SCHEMA_INVALID", f"{label} must be a canonical relative path")
    rel = Path(relative)
    if rel.is_absolute() or ".." in rel.parts or rel.as_posix() != relative:
        _fail("L1E006_SCHEMA_INVALID", f"{label} escapes the candidate")
    path = candidate / rel
    if path.is_symlink() or not path.is_file():
        _fail("L1E001_EVIDENCE_MISSING", f"{label} is missing or not a regular file")
    try:
        path.resolve(strict=True).relative_to(candidate.resolve(strict=True))
    except (OSError, ValueError):
        _fail("L1E006_SCHEMA_INVALID", f"{label} resolves outside the candidate")
    return path


def _command_flag(command: LegacyRunnerCommand, flag: str) -> str | None:
    found: list[str] = []
    for index, token in enumerate(command.argv):
        if token == flag:
            if index + 1 >= len(command.argv):
                _fail("L1E008_BINDING_MISMATCH", f"command has dangling {flag}")
            found.append(command.argv[index + 1])
        elif token.startswith(flag + "="):
            found.append(token.split("=", 1)[1])
    if len(found) > 1:
        _fail("L1E008_BINDING_MISMATCH", f"command repeats {flag}")
    return found[0] if found else None


def _validate_metadata_shape(metadata: Mapping[str, Any]) -> None:
    _closed_keys(
        metadata,
        allowed=_METADATA_ALLOWED,
        required=_METADATA_REQUIRED,
        label="metadata",
    )
    for index, fault in enumerate(_require_list(metadata["faults"], "metadata.faults")):
        obj = _require_mapping(fault, f"metadata.faults[{index}]")
        _closed_keys(obj, allowed=_FAULT_KEYS, required=_FAULT_KEYS, label=f"metadata.faults[{index}]")
        cgt = _require_mapping(obj["component_ground_truth"], f"metadata.faults[{index}].component_ground_truth")
        _closed_keys(cgt, allowed=_COMPONENT_GT_ALLOWED, required=_COMPONENT_GT_REQUIRED, label=f"metadata.faults[{index}].component_ground_truth")
        for key in _COMPONENT_GT_REQUIRED:
            _string(cgt[key], f"metadata.faults[{index}].component_ground_truth.{key}")
        for key in _FAULT_KEYS - {"component_ground_truth"}:
            _string(obj[key], f"metadata.faults[{index}].{key}")
    for index, root in enumerate(_require_list(metadata["root_causes"], "metadata.root_causes")):
        obj = _require_mapping(root, f"metadata.root_causes[{index}]")
        _closed_keys(obj, allowed=_ROOT_CAUSE_KEYS, required=_ROOT_CAUSE_KEYS, label=f"metadata.root_causes[{index}]")
        for key in _ROOT_CAUSE_KEYS:
            _string(obj[key], f"metadata.root_causes[{index}].{key}")
    for index, row in enumerate(_require_list(metadata["validation_results"], "metadata.validation_results")):
        obj = _require_mapping(row, f"metadata.validation_results[{index}]")
        _closed_keys(obj, allowed=_VALIDATION_KEYS, required={"id", "status", "detail"}, label=f"metadata.validation_results[{index}]")
        _string(obj["id"], f"metadata.validation_results[{index}].id")
        _string(obj["status"], f"metadata.validation_results[{index}].status")
    for index, row in enumerate(_require_list(metadata["observation_stages"], "metadata.observation_stages")):
        obj = _require_mapping(row, f"metadata.observation_stages[{index}]")
        _closed_keys(obj, allowed=_OBSERVATION_KEYS, required=_OBSERVATION_KEYS, label=f"metadata.observation_stages[{index}]")
        _string(obj["stage"], f"metadata.observation_stages[{index}].stage")
    checksum = _require_mapping(metadata["checksum_guard"], "metadata.checksum_guard")
    _closed_keys(checksum, allowed=_CHECKSUM_KEYS, required=_CHECKSUM_KEYS, label="metadata.checksum_guard")
    embedded = _require_mapping(metadata["ground_truth"], "metadata.ground_truth")
    _closed_keys(
        embedded,
        allowed=_EMBEDDED_GT_ALLOWED,
        required=_EMBEDDED_GT_REQUIRED,
        label="metadata.ground_truth",
    )
    root_metric = _require_mapping(metadata["root_metric_contract"], "metadata.root_metric_contract")
    _closed_keys(root_metric, allowed=_ROOT_METRIC_KEYS, required={"valid"}, label="metadata.root_metric_contract")


def _validate_groundtruth_shape(groundtruth: Mapping[str, Any]) -> None:
    _closed_keys(
        groundtruth,
        allowed=_GROUNDTRUTH_ALLOWED,
        required=_GROUNDTRUTH_REQUIRED,
        label="groundtruth",
    )
    for index, cgt in enumerate(_require_list(groundtruth["component_ground_truth"], "groundtruth.component_ground_truth")):
        obj = _require_mapping(cgt, f"groundtruth.component_ground_truth[{index}]")
        _closed_keys(obj, allowed=_COMPONENT_GT_ALLOWED, required=_COMPONENT_GT_REQUIRED, label=f"groundtruth.component_ground_truth[{index}]")
        for key in _COMPONENT_GT_REQUIRED:
            _string(obj[key], f"groundtruth.component_ground_truth[{index}].{key}")
    root_metric = _require_mapping(groundtruth["root_metric_contract"], "groundtruth.root_metric_contract")
    _closed_keys(root_metric, allowed=_ROOT_METRIC_KEYS, required={"valid"}, label="groundtruth.root_metric_contract")
    sli_gate = _require_mapping(groundtruth["sli_gate"], "groundtruth.sli_gate")
    _closed_keys(sli_gate, allowed=_SLI_GATE_KEYS, required=_SLI_GATE_KEYS, label="groundtruth.sli_gate")
    _require_mapping(sli_gate["evidence"], "groundtruth.sli_gate.evidence")


def _root_rows(value: Any, label: str, expected_arity: int) -> list[Mapping[str, Any]]:
    rows = _require_list(value, label)
    if len(rows) != expected_arity:
        _fail("L1E011_ROOT_BINDING_MISMATCH", f"{label} arity mismatch")
    normalized: list[Mapping[str, Any]] = []
    expected_legs = [f"F{index}" for index in range(1, expected_arity + 1)]
    for index, row in enumerate(rows):
        obj = _require_mapping(row, f"{label}[{index}]")
        _closed_keys(obj, allowed=_ROOT_KEYS, required=_ROOT_KEYS, label=f"{label}[{index}]")
        leg = _string(obj["leg_id"], f"{label}[{index}].leg_id")
        if _LEG_RE.fullmatch(leg) is None:
            _fail("L1E011_ROOT_BINDING_MISMATCH", f"invalid leg id: {leg}")
        _string(obj["service"], f"{label}[{index}].service")
        _string(obj["instance"], f"{label}[{index}].instance")
        uid = _string(obj["target_uid"], f"{label}[{index}].target_uid")
        kind = _string(obj["uid_kind"], f"{label}[{index}].uid_kind")
        if kind == "kubernetes":
            if _K8S_UID_RE.fullmatch(uid) is None:
                _fail("L1E011_ROOT_BINDING_MISMATCH", f"invalid Kubernetes UID for {leg}")
        elif kind == "off_graph":
            expected_uid = f"offgraph:{obj['service']}:{obj['instance']}"
            if uid != expected_uid:
                _fail("L1E011_ROOT_BINDING_MISMATCH", f"non-canonical off-graph UID for {leg}")
        else:
            _fail("L1E011_ROOT_BINDING_MISMATCH", f"unknown uid_kind for {leg}")
        normalized.append(obj)
    if [row["leg_id"] for row in normalized] != expected_legs:
        _fail("L1E011_ROOT_BINDING_MISMATCH", f"{label} legs must be {expected_legs}")
    return normalized


def _validate_queries(value: Any) -> None:
    rows = _require_list(value, "report.query_rows")
    if not rows:
        _fail("L1E014_QUERY_EVIDENCE_INVALID", "query_rows cannot be empty")
    seen: set[str] = set()
    required_count = 0
    for index, row in enumerate(rows):
        obj = _require_mapping(row, f"report.query_rows[{index}]")
        _closed_keys(obj, allowed=_QUERY_KEYS, required=_QUERY_KEYS, label=f"report.query_rows[{index}]")
        query_id = _string(obj["query_id"], f"report.query_rows[{index}].query_id")
        if query_id in seen:
            _fail("L1E014_QUERY_EVIDENCE_INVALID", f"duplicate query_id: {query_id}")
        seen.add(query_id)
        if type(obj["required"]) is not bool:
            _fail("L1E006_SCHEMA_INVALID", f"query required flag is not bool: {query_id}")
        required_count += int(obj["required"])
        status = _string(obj["status"], f"report.query_rows[{index}].status")
        if status not in _QUERY_STATUSES:
            _fail("L1E014_QUERY_EVIDENCE_INVALID", f"unknown query status: {query_id}={status}")
        query_value = obj["value"]
        if status == "zero":
            if type(query_value) not in {int, float} or isinstance(query_value, bool) or not math.isfinite(float(query_value)) or float(query_value) != 0.0:
                _fail("L1E014_QUERY_EVIDENCE_INVALID", f"zero query must carry numeric zero: {query_id}")
        elif status == "value":
            if type(query_value) not in {int, float} or isinstance(query_value, bool) or not math.isfinite(float(query_value)) or float(query_value) == 0.0:
                _fail("L1E014_QUERY_EVIDENCE_INVALID", f"value query must carry finite non-zero value: {query_id}")
        elif query_value is not None:
            _fail("L1E014_QUERY_EVIDENCE_INVALID", f"failed/missing query may not carry a value: {query_id}")
        if obj["required"] is True and status in _QUERY_FAILURES:
            _fail("L1E015_REQUIRED_QUERY_UNAVAILABLE", f"required query {query_id} ended as {status}")
    if required_count == 0:
        _fail("L1E014_QUERY_EVIDENCE_INVALID", "query_rows contains no required query")


def _parse_metrics(raw: bytes, run_id: str) -> list[Mapping[str, Any]]:
    if raw.startswith(b"\xef\xbb\xbf"):
        _fail("L1E002_INVALID_UTF8", "metrics contains a UTF-8 BOM")
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        _fail("L1E002_INVALID_UTF8", f"metrics is not strict UTF-8: {exc}")
    records: list[Mapping[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(
                line,
                object_pairs_hook=_object_no_duplicates,
                parse_constant=_reject_constant,
            )
        except StrictEvidenceError:
            raise
        except (json.JSONDecodeError, RecursionError) as exc:
            _fail("L1E005_INVALID_JSON", f"metrics line {line_number} invalid: {exc}")
        obj = _require_mapping(value, f"metrics[{line_number}]")
        _closed_keys(obj, allowed=_METRIC_KEYS, required=_METRIC_KEYS, label=f"metrics[{line_number}]")
        labels = _require_mapping(obj["labels"], f"metrics[{line_number}].labels")
        _closed_keys(labels, allowed=_METRIC_LABEL_KEYS, required=None, label=f"metrics[{line_number}].labels")
        for key in (
            "schema_version",
            "timestamp",
            "stage",
            "run_id",
            "source",
            "entity_type",
            "entity",
            "service",
            "metric",
            "unit",
            "metric_type",
            "fault_window_membership",
        ):
            _string(obj[key], f"metrics[{line_number}].{key}")
        metric_value = obj["value"]
        if (
            type(metric_value) not in {int, float}
            or isinstance(metric_value, bool)
            or not math.isfinite(float(metric_value))
        ):
            _fail("L1E006_SCHEMA_INVALID", f"metrics line {line_number} value is not finite numeric")
        if obj["run_id"] != run_id:
            _fail("L1E010_CASE_ARTIFACT_MISMATCH", f"metrics line {line_number} run_id mismatch")
        records.append(obj)
    if not records:
        _fail("L1E001_EVIDENCE_MISSING", "metrics file is empty")
    return records


def _validate_verifier_provenance(
    *,
    candidate: Path,
    report: Mapping[str, Any],
    verifier: Mapping[str, Any],
    artifacts: Mapping[str, Any],
) -> None:
    """Re-read every adapter output and close its exact bindings."""

    expected_static_paths = {
        "request_path": REQUEST_FILENAME,
        "stdout_path": STDOUT_FILENAME,
        "stderr_path": STDERR_FILENAME,
        "candidate_manifest_path": MANIFEST_FILENAME,
        "result_path": RESULT_FILENAME,
    }
    for key, expected in expected_static_paths.items():
        if verifier[key] != expected:
            _fail("L1E017_PROVENANCE_INVALID", f"non-canonical verifier {key}")

    request_path = _inside_regular_file(candidate, verifier["request_path"], "verifier request")
    request_raw = request_path.read_bytes()
    if _sha256(request_raw) != _hash_string(verifier["request_sha256"], "request_sha256"):
        _fail("L1E017_PROVENANCE_INVALID", "verifier request hash mismatch")
    request = _require_mapping(_decode_json(request_raw, "verifier request"), "verifier request")
    request_keys = _REPORT_KEYS - {"verifier"}
    _closed_keys(request, allowed=request_keys, required=request_keys, label="verifier request")
    if dict(request) != {key: report[key] for key in request_keys}:
        _fail("L1E017_PROVENANCE_INVALID", "verifier request differs from closed report")

    manifest_path = _inside_regular_file(
        candidate,
        verifier["candidate_manifest_path"],
        "candidate evidence manifest",
    )
    manifest_raw = manifest_path.read_bytes()
    if _sha256(manifest_raw) != _hash_string(
        verifier["candidate_manifest_sha256"],
        "candidate_manifest_sha256",
    ):
        _fail("L1E017_PROVENANCE_INVALID", "candidate manifest hash mismatch")
    manifest = _require_mapping(
        _decode_json(manifest_raw, "candidate evidence manifest"),
        "candidate evidence manifest",
    )
    _closed_keys(manifest, allowed=_MANIFEST_KEYS, required=_MANIFEST_KEYS, label="candidate manifest")
    if (
        manifest["schema_name"] != MANIFEST_SCHEMA_NAME
        or manifest["schema_version"] != MANIFEST_SCHEMA_VERSION
    ):
        _fail("L1E017_PROVENANCE_INVALID", "candidate manifest schema mismatch")
    entries = _require_list(manifest["entries"], "candidate manifest.entries")
    expected_entries: list[dict[str, Any]] = []
    for relative in _VERIFIER_INPUT_PATHS:
        path = _inside_regular_file(candidate, relative, f"verifier input {relative}")
        raw = path.read_bytes()
        expected_entries.append(
            {
                "path": relative,
                "size": len(raw),
                "sha256": _sha256(raw),
            }
        )
    expected_entries.sort(key=lambda row: row["path"])
    normalized_entries: list[dict[str, Any]] = []
    for index, row in enumerate(entries):
        obj = _require_mapping(row, f"candidate manifest.entries[{index}]")
        _closed_keys(
            obj,
            allowed=_MANIFEST_ENTRY_KEYS,
            required=_MANIFEST_ENTRY_KEYS,
            label=f"candidate manifest.entries[{index}]",
        )
        if type(obj["size"]) is not int or obj["size"] < 0:
            _fail("L1E017_PROVENANCE_INVALID", "candidate manifest size is invalid")
        _hash_string(obj["sha256"], f"candidate manifest.entries[{index}].sha256")
        normalized_entries.append(dict(obj))
    if normalized_entries != expected_entries:
        _fail("L1E017_PROVENANCE_INVALID", "candidate evidence manifest drifted")

    log_hashes: dict[str, str] = {}
    for stem in ("stdout", "stderr"):
        path = _inside_regular_file(candidate, verifier[f"{stem}_path"], f"verifier {stem}")
        digest = _sha256(path.read_bytes())
        if digest != _hash_string(verifier[f"{stem}_sha256"], f"{stem}_sha256"):
            _fail("L1E017_PROVENANCE_INVALID", f"verifier {stem} hash mismatch")
        log_hashes[stem] = digest

    verifier_path_value = _string(verifier["verifier_path"], "verifier_path")
    verifier_path = Path(verifier_path_value)
    if not verifier_path.is_absolute() or verifier_path.is_symlink() or not verifier_path.is_file():
        _fail("L1E017_PROVENANCE_INVALID", "verifier path is missing or not a regular file")
    verifier_sha256 = _hash_string(verifier["verifier_sha256"], "verifier_sha256")
    if _sha256(verifier_path.read_bytes()) != verifier_sha256:
        _fail("L1E017_PROVENANCE_INVALID", "verifier code hash mismatch")
    argv = _require_list(verifier["argv"], "verifier.argv")
    if len(argv) != 3 or any(type(token) is not str or not token or "\x00" in token for token in argv):
        _fail("L1E017_PROVENANCE_INVALID", "verifier argv must contain exactly three strings")
    python_path = Path(argv[0])
    if not python_path.is_absolute() or not python_path.is_file():
        _fail("L1E017_PROVENANCE_INVALID", "verifier Python executable is missing")
    if argv[1] != verifier_path_value or argv[2] != str(candidate.resolve(strict=True)):
        _fail("L1E017_PROVENANCE_INVALID", "verifier exact argv binding mismatch")
    if verifier["shell"] is not False:
        _fail("L1E017_PROVENANCE_INVALID", "verifier shell flag must be false")
    if type(verifier["exit_code"]) is not int or verifier["exit_code"] != 0:
        _fail("L1E013_VERIFIER_NOT_PASS", "verifier exit code is nonzero")

    result_path = _inside_regular_file(candidate, verifier["result_path"], "verifier result")
    result_raw = result_path.read_bytes()
    if _sha256(result_raw) != _hash_string(verifier["result_sha256"], "result_sha256"):
        _fail("L1E017_PROVENANCE_INVALID", "verifier result hash mismatch")
    result = _require_mapping(_decode_json(result_raw, "verifier result"), "verifier result")
    _closed_keys(result, allowed=_RESULT_KEYS, required=_RESULT_KEYS, label="verifier result")
    expected_result = {
        "schema_name": RESULT_SCHEMA_NAME,
        "schema_version": RESULT_SCHEMA_VERSION,
        "request_sha256": verifier["request_sha256"],
        "verifier_path": verifier_path_value,
        "verifier_sha256": verifier_sha256,
        "argv": argv,
        "shell": False,
        "exit_code": 0,
        "stdout_sha256": log_hashes["stdout"],
        "stderr_sha256": log_hashes["stderr"],
        "candidate_manifest_sha256": verifier["candidate_manifest_sha256"],
        "passed": True,
    }
    if dict(result) != expected_result or verifier["passed"] is not True:
        _fail("L1E013_VERIFIER_NOT_PASS", "verifier result is not an accepted PASS")


class StrictEvidenceEvaluator:
    """Evaluate one already-written candidate and return a bound bundle."""

    def __init__(self, provenance_writer: ProvenanceWriter | None = None) -> None:
        self._provenance_writer = provenance_writer

    def evaluate(
        self,
        *,
        plan: AttemptPlan,
        command: LegacyRunnerCommand,
        result: ExecutionResult,
    ) -> EvidenceBundle:
        if result.returncode != 0:
            _fail("L1E008_BINDING_MISMATCH", "evidence requires runner exit zero")
        if command.attempt_id != plan.attempt_id or command.case_id != plan.case_id:
            _fail("L1E008_BINDING_MISMATCH", "plan and command binding differ")
        for field in (
            "contract_sha256",
            "schedule_hash",
            "block_schedule_hash",
            "slot_sha256",
        ):
            if getattr(command, field) != getattr(plan, field):
                _fail("L1E008_BINDING_MISMATCH", f"plan and command {field} differ")
        candidate = plan.promotion.source
        if candidate.is_symlink() or not candidate.is_dir():
            _fail("L1E001_EVIDENCE_MISSING", "candidate case directory does not exist")
        if self._provenance_writer is not None:
            self._provenance_writer.write_report(
                plan=plan,
                command=command,
                result=result,
            )

        report_path = _inside_regular_file(candidate, REPORT_FILENAME, "verify report")
        report_raw = report_path.read_bytes()
        report = _require_mapping(_decode_json(report_raw, "verify report"), "verify report")
        _closed_keys(report, allowed=_REPORT_KEYS, required=_REPORT_KEYS, label="report")
        if report["schema_name"] != REPORT_SCHEMA_NAME or report["schema_version"] != REPORT_SCHEMA_VERSION:
            _fail("L1E006_SCHEMA_INVALID", "verify report schema mismatch")

        binding = _require_mapping(report["binding"], "report.binding")
        _closed_keys(binding, allowed=_BINDING_KEYS, required=_BINDING_KEYS, label="report.binding")
        scenario_id = _string(binding["scenario_id"], "report.binding.scenario_id")
        if scenario_id not in _SCENARIO_BY_ID:
            _fail("L1E008_BINDING_MISMATCH", "unknown scenario_id")
        spec = _SCENARIO_BY_ID[scenario_id]
        if spec.disposition == "deferred":
            _fail("L1E008_BINDING_MISMATCH", "deferred scenario cannot produce Lite-1 evidence")
        expected_binding = {
            "attempt_id": plan.attempt_id,
            "case_id": plan.case_id,
            "command_sha256": command.command_sha256,
            "contract_sha256": plan.contract_sha256,
            "schedule_hash": plan.schedule_hash,
            "block_schedule_hash": plan.block_schedule_hash,
            "slot_sha256": plan.slot_sha256,
            "scenario_id": scenario_id,
            "runner_fault": spec.runner_fault,
            "runner_target_service": spec.runner_target_service,
        }
        if dict(binding) != expected_binding:
            _fail("L1E008_BINDING_MISMATCH", "verify report does not bind this attempt/case/command")
        if _command_flag(command, "--fault") != spec.runner_fault:
            _fail("L1E008_BINDING_MISMATCH", "command fault differs from scenario")
        if _command_flag(command, "--target-service") != spec.runner_target_service:
            _fail("L1E008_BINDING_MISMATCH", "command target differs from scenario")

        artifacts = _require_mapping(report["artifacts"], "report.artifacts")
        _closed_keys(artifacts, allowed=_ARTIFACT_KEYS, required=_ARTIFACT_KEYS, label="report.artifacts")
        expected_paths = {
            "metadata_path": "metadata.json",
            "groundtruth_path": "groundtruth.json",
            "metrics_path": "raw/metrics/metrics_v2.jsonl",
        }
        for key, expected in expected_paths.items():
            if artifacts[key] != expected:
                _fail("L1E006_SCHEMA_INVALID", f"non-canonical {key}")
        metadata_path = _inside_regular_file(candidate, artifacts["metadata_path"], "metadata")
        groundtruth_path = _inside_regular_file(candidate, artifacts["groundtruth_path"], "groundtruth")
        metrics_path = _inside_regular_file(candidate, artifacts["metrics_path"], "metrics")
        metadata_raw = metadata_path.read_bytes()
        groundtruth_raw = groundtruth_path.read_bytes()
        metrics_raw = metrics_path.read_bytes()
        for name, raw in (
            ("metadata", metadata_raw),
            ("groundtruth", groundtruth_raw),
            ("metrics", metrics_raw),
        ):
            expected_hash = _hash_string(artifacts[f"{name}_sha256"], f"{name}_sha256")
            if _sha256(raw) != expected_hash:
                _fail("L1E009_ARTIFACT_HASH_MISMATCH", f"{name} hash mismatch")

        metadata = _require_mapping(_decode_json(metadata_raw, "metadata"), "metadata")
        groundtruth = _require_mapping(_decode_json(groundtruth_raw, "groundtruth"), "groundtruth")
        _validate_metadata_shape(metadata)
        _validate_groundtruth_shape(groundtruth)
        run_id = _string(metadata["run_id"], "metadata.run_id")
        metrics = _parse_metrics(metrics_raw, run_id)

        if metadata["sample_id"] != plan.case_id or metadata["formal_slot_id"] != plan.case_id:
            _fail("L1E010_CASE_ARTIFACT_MISMATCH", "metadata case id mismatch")
        if groundtruth["sample_id"] != plan.case_id or groundtruth["run_id"] != run_id:
            _fail("L1E010_CASE_ARTIFACT_MISMATCH", "groundtruth case/run mismatch")
        arity = spec.fault_instance_arity
        if (
            type(metadata["root_count"]) is not int
            or type(groundtruth["root_count"]) is not int
            or metadata["root_count"] != arity
            or groundtruth["root_count"] != arity
        ):
            _fail("L1E011_ROOT_BINDING_MISMATCH", "root_count differs from frozen fault-leg arity")
        faults = metadata["faults"]
        roots = metadata["root_causes"]
        components = groundtruth["component_ground_truth"]
        if not (len(faults) == len(roots) == len(components) == arity):
            _fail("L1E011_ROOT_BINDING_MISMATCH", "metadata/GT root arity mismatch")
        meta_components = [row["component_ground_truth"] for row in faults]
        if meta_components != components:
            _fail("L1E011_ROOT_BINDING_MISMATCH", "metadata and GT component roots differ")
        embedded = _require_mapping(metadata["ground_truth"], "metadata.ground_truth")
        services = _require_list(groundtruth["root_cause_services"], "groundtruth.root_cause_services")
        instances = _require_list(groundtruth["root_cause_instances"], "groundtruth.root_cause_instances")
        fault_types = _require_list(groundtruth["fault_types"], "groundtruth.fault_types")
        injection_faults = _require_list(groundtruth["injection_faults"], "groundtruth.injection_faults")
        if not (len(services) == len(instances) == len(fault_types) == len(injection_faults) == arity):
            _fail("L1E011_ROOT_BINDING_MISMATCH", "groundtruth root arrays have wrong arity")
        for key in (
            "root_count",
            "root_cause_services",
            "root_cause_instances",
            "fault_types",
            "injection_faults",
            "component_ground_truth",
        ):
            if embedded.get(key) != groundtruth[key]:
                _fail("L1E011_ROOT_BINDING_MISMATCH", f"embedded ground truth differs at {key}")

        planned = _root_rows(report["planned_roots"], "report.planned_roots", arity)
        actual = _root_rows(report["actual_roots"], "report.actual_roots", arity)
        if planned != actual:
            _fail("L1E011_ROOT_BINDING_MISMATCH", "planned and actual root rows differ")
        expected_legs = [f"F{index}" for index in range(1, arity + 1)]
        if [row["fault_instance_id"] for row in roots] != expected_legs:
            _fail("L1E011_ROOT_BINDING_MISMATCH", "metadata root legs are not canonical")
        for index, row in enumerate(actual):
            root = roots[index]
            component = components[index]
            if (
                row["leg_id"] != root["fault_instance_id"]
                or row["leg_id"] != component["fault_instance_id"]
                or row["service"] != root["service"]
                or row["service"] != component["target_component"]
                or row["instance"] != root["instance"]
                or row["instance"] != component.get("target_container")
                or row["service"] != services[index]
                or row["instance"] != instances[index]
                or component["fault_type"] != fault_types[index]
                or component["injection_fault"] != injection_faults[index]
            ):
                _fail("L1E011_ROOT_BINDING_MISMATCH", f"root evidence mismatch at {row['leg_id']}")
            if row["uid_kind"] == "kubernetes":
                found = any(
                    metric["stage"] == "during_fault"
                    and metric["service"] == row["service"]
                    and metric["labels"].get("pod") == row["instance"]
                    and metric["labels"].get("uid") == row["target_uid"]
                    for metric in metrics
                )
                if not found:
                    _fail("L1E012_UID_NOT_IN_DURING_TELEMETRY", f"{row['leg_id']} UID is absent from during telemetry")

        verifier = _require_mapping(report["verifier"], "report.verifier")
        _closed_keys(verifier, allowed=_VERIFIER_KEYS, required=_VERIFIER_KEYS, label="report.verifier")
        if verifier["kind"] != VERIFIER_KIND:
            _fail("L1E013_VERIFIER_NOT_PASS", "persisted verifier result is not an accepted PASS")
        _validate_verifier_provenance(
            candidate=candidate,
            report=report,
            verifier=verifier,
            artifacts=artifacts,
        )
        _validate_queries(report["query_rows"])

        checks = _require_mapping(report["checks"], "report.checks")
        _closed_keys(checks, allowed=_CHECK_KEYS, required=_CHECK_KEYS, label="report.checks")
        for key in sorted(_CHECK_KEYS):
            _boolean_true(checks[key], f"report.checks.{key}")

        validations = metadata["validation_results"]
        if not validations or any(row["status"] != "pass" for row in validations):
            _fail("L1E016_REQUIRED_CHECK_FAILED", "metadata validation_results are not all pass")
        validation_by_id = {row["id"]: row for row in validations}
        for validation_id in (
            "each_root_signal_present",
            "root_metric_contract",
            "recovery_confirmed",
            "checksum_zero_business_write",
        ):
            if validation_by_id.get(validation_id, {}).get("status") != "pass":
                _fail("L1E016_REQUIRED_CHECK_FAILED", f"missing/pass=false validation: {validation_id}")
        during = [row for row in metadata["observation_stages"] if row["stage"] == "during_fault"]
        post = [row for row in metadata["observation_stages"] if row["stage"] == "post_recovery"]
        if len(during) != 1 or during[0]["gate_passed"] is not True:
            _fail("L1E016_REQUIRED_CHECK_FAILED", "during runner gate is not true")
        if len(post) != 1 or type(post[0]["observed_snapshots"]) is not int or post[0]["observed_snapshots"] <= 0:
            _fail("L1E016_REQUIRED_CHECK_FAILED", "post-recovery evidence is absent")
        if groundtruth["sli_gate"].get("gate_passed") is not True:
            _fail("L1E016_REQUIRED_CHECK_FAILED", "groundtruth runner gate is not true")
        if metadata["root_metric_contract"].get("valid") is not True or groundtruth["root_metric_contract"].get("valid") is not True:
            _fail("L1E016_REQUIRED_CHECK_FAILED", "root metric contract is not valid")
        checksum = metadata["checksum_guard"]
        if checksum["zero_drift"] is not True or checksum["pre"] != checksum["post"] or checksum["pre"] != checksum["baseline"]:
            _fail("L1E016_REQUIRED_CHECK_FAILED", "checksum zero-drift equality is not proven")
        if metadata["ready_for_release"] is not True or metadata["validation_complete"] is not True or metadata["sample_status"] != "ready_for_release":
            _fail("L1E016_REQUIRED_CHECK_FAILED", "metadata is not ready_for_release")
        for index, fault in enumerate(faults):
            if fault["status"] != "recovered" or not fault["injected_at"] or not fault["recovered_at"]:
                _fail("L1E016_REQUIRED_CHECK_FAILED", f"fault F{index + 1} lacks recovery evidence")

        return EvidenceBundle(
            attempt_id=plan.attempt_id,
            case_id=plan.case_id,
            command_sha256=command.command_sha256,
            contract_sha256=plan.contract_sha256,
            schedule_hash=plan.schedule_hash,
            block_schedule_hash=plan.block_schedule_hash,
            slot_sha256=plan.slot_sha256,
            metadata_path=metadata_path.resolve(strict=True),
            metadata_sha256=_sha256(metadata_raw),
            metadata_passed=True,
            verifier_report_path=report_path.resolve(strict=True),
            verifier_report_sha256=_sha256(report_raw),
            verifier_passed=True,
        )


__all__ = [
    "REPORT_FILENAME",
    "REPORT_SCHEMA_NAME",
    "REPORT_SCHEMA_VERSION",
    "StrictEvidenceError",
    "StrictEvidenceEvaluator",
    "VERIFIER_KIND",
]
