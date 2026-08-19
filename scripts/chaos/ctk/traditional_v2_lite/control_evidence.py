"""Closed zero-root evidence for the independent no-fault engineering smoke."""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .phase_journal import PHASES
from .smoke_lite import EXPECTED_DB_CHECKSUMS
from .telemetry_journal import QueryRecord, telemetry_acceptable


REPORT_FILENAME = "control-verify-report.json"
MANIFEST_FILENAME = "control-provenance/manifest.json"
SCHEMA_NAME = "traditional_v2_lite_no_fault_control_evidence"
SCHEMA_VERSION = "1.0.0"
REQUIRED_ARTIFACT_PATHS = (
    "raw/metrics/control_metrics.jsonl",
    "raw/traces/control_traces.jsonl",
    "raw/logs/control_logs.jsonl",
    "phase-journal.json",
    "workload-journal.json",
    "query-journal.json",
    "preflight.json",
    "checksum.json",
    "control-result.json",
)
_REPORT_KEYS = {
    "schema_name", "schema_version", "attempt_id", "scenario", "zero_root",
    "manifest_path", "manifest_sha256", "environment_sha256", "checks", "passed",
}
_CHECK_KEYS = {
    "three_modalities_present", "phase_complete", "workload_complete",
    "queries_acceptable", "checksum_zero_drift", "environment_bound",
    "zero_runner_calls", "zero_fault_calls", "zero_chaos_resources",
}
_RESULT_KEYS = {"attempt_id", "scenario", "status", "runner_invocations", "fault_calls", "chaos_resources"}


class ControlEvidenceError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> "None":
    raise ControlEvidenceError(code, message)


def _canonical(value: Any) -> bytes:
    try:
        return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
    except (TypeError, ValueError) as exc:
        _fail("E5C003_SCHEMA_INVALID", f"cannot canonicalize: {exc}")


def _object(raw: bytes, label: str) -> Mapping[str, Any]:
    if raw.startswith(b"\xef\xbb\xbf"):
        _fail("E5C003_SCHEMA_INVALID", f"{label} has BOM")
    try:
        value = json.loads(raw.decode("utf-8", errors="strict"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        _fail("E5C003_SCHEMA_INVALID", f"{label} is not strict JSON: {exc}")
    if type(value) is not dict:
        _fail("E5C003_SCHEMA_INVALID", f"{label} must be object")
    return value


def _inside(root: Path, relative: str) -> tuple[Path, bytes]:
    rel = Path(relative)
    if rel.is_absolute() or ".." in rel.parts or rel.as_posix() != relative:
        _fail("E5C001_ARTIFACT_MISSING", "artifact path is not canonical")
    path = root / rel
    if path.is_symlink() or not path.is_file():
        _fail("E5C001_ARTIFACT_MISSING", f"missing regular artifact: {relative}")
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
        return path, resolved.read_bytes()
    except (OSError, ValueError) as exc:
        _fail("E5C001_ARTIFACT_MISSING", f"cannot read {relative}: {exc.__class__.__name__}")


def _write_new(path: Path, value: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as handle:
            handle.write(_canonical(value)); handle.flush(); os.fsync(handle.fileno())
    except FileExistsError:
        _fail("E5C002_PROVENANCE_EXISTS", f"refusing to overwrite {path.name}")
    except OSError as exc:
        _fail("E5C004_WRITE_FAILED", f"cannot write provenance: {exc.__class__.__name__}")


def _jsonl(raw: bytes, label: str) -> list[Mapping[str, Any]]:
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeError:
        _fail("E5C003_SCHEMA_INVALID", f"{label} is not UTF-8")
    rows = []
    for line in text.splitlines():
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            _fail("E5C003_SCHEMA_INVALID", f"{label} contains invalid JSONL")
        if type(value) is not dict:
            _fail("E5C003_SCHEMA_INVALID", f"{label} row must be object")
        rows.append(value)
    if not rows:
        _fail("E5C005_MODALITY_MISSING", f"{label} is empty")
    return rows


def _manifest(root: Path) -> tuple[dict[str, Any], str]:
    entries = []
    for relative in REQUIRED_ARTIFACT_PATHS:
        _path, raw = _inside(root, relative)
        entries.append({"path": relative, "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    document = {"schema_name": "traditional_v2_lite_control_manifest", "schema_version": "1.0.0", "entries": entries}
    return document, hashlib.sha256(_canonical(document)).hexdigest()


def _validate(root: Path, attempt_id: str, environment_sha256: str) -> dict[str, bool]:
    if not attempt_id or len(environment_sha256) != 64:
        _fail("E5C003_SCHEMA_INVALID", "attempt/environment binding invalid")
    for modality in ("metrics", "traces", "logs"):
        rows = _jsonl(_inside(root, f"raw/{modality}/control_{modality}.jsonl")[1], modality)
        for row in rows:
            if set(row) != {"attempt_id", "observed_at", "payload"} or row["attempt_id"] != attempt_id or type(row["payload"]) is not dict:
                _fail("E5C005_MODALITY_MISSING", f"{modality} row binding invalid")
            payload = row["payload"]
            if set(payload) != {"status", "record_count", "response_sha256", "response"} or payload["status"] != "value" or type(payload["record_count"]) is not int or payload["record_count"] <= 0 or type(payload["response_sha256"]) is not str or len(payload["response_sha256"]) != 64:
                _fail("E5C005_MODALITY_MISSING", f"{modality} contains no actual records")
            if hashlib.sha256(_canonical(payload["response"])).hexdigest() != payload["response_sha256"]:
                _fail("E5C005_MODALITY_MISSING", f"{modality} response hash differs")
            observed = row["observed_at"]
            if type(observed) not in {int, float} or isinstance(observed, bool) or not math.isfinite(float(observed)):
                _fail("E5C005_MODALITY_MISSING", f"{modality} clock invalid")

    phases = json.loads(_inside(root, "phase-journal.json")[1].decode("utf-8"))
    if type(phases) is not list or [row.get("phase") for row in phases if type(row) is dict] != list(PHASES):
        _fail("E5C006_PHASE_INVALID", "phase journal is not exact")
    phase_times = [row.get("entered_at") for row in phases]
    if any(type(value) not in {int, float} or isinstance(value, bool) or not math.isfinite(float(value)) for value in phase_times) or any(b <= a for a, b in zip(phase_times, phase_times[1:])):
        _fail("E5C006_PHASE_INVALID", "phase time is not strictly monotonic")

    workload = json.loads(_inside(root, "workload-journal.json")[1].decode("utf-8"))
    if type(workload) is not list or not workload or any(type(row) is not dict or row.get("timeout") is not False for row in workload):
        _fail("E5C007_WORKLOAD_INVALID", "workload journal is unavailable/timeout")

    query_values = json.loads(_inside(root, "query-journal.json")[1].decode("utf-8"))
    try:
        queries = tuple(QueryRecord(**row) for row in query_values)
    except (TypeError, ValueError):
        _fail("E5C008_QUERY_INVALID", "query journal schema invalid")
    if not telemetry_acceptable(queries):
        _fail("E5C008_QUERY_INVALID", "required query unavailable")

    preflight_raw = _inside(root, "preflight.json")[1]
    if hashlib.sha256(preflight_raw).hexdigest() != environment_sha256:
        _fail("E5C009_ENVIRONMENT_DRIFT", "preflight environment hash differs")
    checksum = _object(_inside(root, "checksum.json")[1], "checksum")
    if set(checksum) != {"pre", "post"} or checksum["pre"] != EXPECTED_DB_CHECKSUMS or checksum["post"] != EXPECTED_DB_CHECKSUMS:
        _fail("E5C010_CHECKSUM_DRIFT", "checksum is not zero-drift baseline")
    result = _object(_inside(root, "control-result.json")[1], "control result")
    if set(result) != _RESULT_KEYS or result != {
        "attempt_id": attempt_id, "scenario": "no-fault", "status": "completed",
        "runner_invocations": 0, "fault_calls": 0, "chaos_resources": [],
    }:
        _fail("E5C011_FAULT_SURFACE_USED", "no-fault control contains runner/fault/resource facts")
    return {key: True for key in _CHECK_KEYS}


@dataclass(frozen=True, slots=True)
class ControlEvidenceBundle:
    attempt_id: str
    report_sha256: str
    manifest_sha256: str


class ControlEvidenceAdapter:
    def write_report(self, *, root: Path, attempt_id: str, environment_sha256: str) -> Path:
        if root.is_symlink() or not root.is_dir():
            _fail("E5C001_ARTIFACT_MISSING", "control attempt root missing")
        report_path = root / REPORT_FILENAME
        manifest_path = root / MANIFEST_FILENAME
        if report_path.exists() or manifest_path.exists():
            _fail("E5C002_PROVENANCE_EXISTS", "control provenance already exists")
        checks = _validate(root, attempt_id, environment_sha256)
        manifest, manifest_sha = _manifest(root)
        _write_new(manifest_path, manifest)
        report = {
            "schema_name": SCHEMA_NAME, "schema_version": SCHEMA_VERSION,
            "attempt_id": attempt_id, "scenario": "no-fault", "zero_root": True,
            "manifest_path": MANIFEST_FILENAME, "manifest_sha256": manifest_sha,
            "environment_sha256": environment_sha256, "checks": checks, "passed": True,
        }
        _write_new(report_path, report)
        return report_path


class ControlEvidenceEvaluator:
    def evaluate(self, *, root: Path, attempt_id: str, environment_sha256: str) -> ControlEvidenceBundle:
        report_path, report_raw = _inside(root, REPORT_FILENAME)
        report = _object(report_raw, "control report")
        if set(report) != _REPORT_KEYS or report.get("schema_name") != SCHEMA_NAME or report.get("schema_version") != SCHEMA_VERSION:
            _fail("E5C003_SCHEMA_INVALID", "control report schema is not closed")
        if report.get("attempt_id") != attempt_id or report.get("scenario") != "no-fault" or report.get("zero_root") is not True or report.get("passed") is not True:
            _fail("E5C012_REPORT_NOT_PASS", "control report binding/pass invalid")
        checks = _validate(root, attempt_id, environment_sha256)
        if report.get("checks") != checks or report.get("environment_sha256") != environment_sha256:
            _fail("E5C012_REPORT_NOT_PASS", "control checks/environment differ")
        manifest, manifest_sha = _manifest(root)
        manifest_path, manifest_raw = _inside(root, MANIFEST_FILENAME)
        if manifest_raw != _canonical(manifest) or report.get("manifest_path") != MANIFEST_FILENAME or report.get("manifest_sha256") != manifest_sha:
            _fail("E5C013_MANIFEST_DRIFT", "control manifest/content drift")
        return ControlEvidenceBundle(attempt_id, hashlib.sha256(report_raw).hexdigest(), manifest_sha)


__all__ = [
    "ControlEvidenceAdapter", "ControlEvidenceBundle", "ControlEvidenceError",
    "ControlEvidenceEvaluator", "MANIFEST_FILENAME", "REPORT_FILENAME",
    "REQUIRED_ARTIFACT_PATHS",
]
