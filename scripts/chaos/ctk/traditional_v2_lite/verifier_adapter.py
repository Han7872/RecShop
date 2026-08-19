"""Foreground, shell-free provenance writer for the frozen Lite verifier.

The adapter is intentionally narrow: it consumes an existing
``verify-request.json`` from one candidate case, executes exactly one frozen
``verify_dual.py`` file, and writes a closed set of provenance files.  It does
not inspect Kubernetes, build collection commands, or promote a directory.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .runner_wrapper import (
    AttemptPlan,
    ExecutionResult,
    LegacyRunnerCommand,
    LiteWrapperError,
)


REQUEST_FILENAME = "verify-request.json"
REPORT_FILENAME = "verify-report.json"
PROVENANCE_DIRNAME = "verifier-provenance"
MANIFEST_FILENAME = f"{PROVENANCE_DIRNAME}/candidate-manifest.json"
STDOUT_FILENAME = f"{PROVENANCE_DIRNAME}/stdout.log"
STDERR_FILENAME = f"{PROVENANCE_DIRNAME}/stderr.log"
RESULT_FILENAME = f"{PROVENANCE_DIRNAME}/result.json"
REQUEST_SCHEMA_NAME = "traditional_v2_lite_strict_evidence"
REQUEST_SCHEMA_VERSION = "1.2.0"
MANIFEST_SCHEMA_NAME = "traditional_v2_lite_verifier_manifest"
MANIFEST_SCHEMA_VERSION = "1.0.0"
RESULT_SCHEMA_NAME = "traditional_v2_lite_verifier_result"
RESULT_SCHEMA_VERSION = "1.0.0"
VERIFIER_KIND = "verify_dual.py/3-part+instance"
FROZEN_VERIFY_DUAL_SHA256 = "70de0f4cb86074bab153e665768d39c01d9c2fbba5a1aaf33cd4a8b523caa591"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_REQUEST_KEYS = {
    "schema_name",
    "schema_version",
    "binding",
    "artifacts",
    "planned_roots",
    "actual_roots",
    "query_rows",
    "checks",
}
_VERIFIER_INPUT_PATHS = (
    "metadata.json",
    "groundtruth.json",
    "summary.md",
    "raw/traces/during_fault_traces.jsonl",
    "raw/metrics/metrics_v2.jsonl",
)


class VerifierAdapterError(LiteWrapperError):
    """Stable fail-closed error from provenance generation."""


def _fail(code: str, message: str) -> "None":
    raise VerifierAdapterError(code, message)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical_json(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        _fail("L1V006_REQUEST_INVALID", f"cannot canonicalize verifier data: {exc}")


def _decode_object(raw: bytes, label: str) -> Mapping[str, Any]:
    if raw.startswith(b"\xef\xbb\xbf"):
        _fail("L1V006_REQUEST_INVALID", f"{label} contains a UTF-8 BOM")
    try:
        value = json.loads(raw.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("L1V006_REQUEST_INVALID", f"{label} is not strict JSON: {exc}")
    if type(value) is not dict:
        _fail("L1V006_REQUEST_INVALID", f"{label} must be an object")
    return value


def _read_regular_inside(candidate: Path, relative: str, label: str) -> tuple[Path, bytes]:
    if type(relative) is not str or not relative or "\\" in relative:
        _fail("L1V006_REQUEST_INVALID", f"{label} path is not canonical")
    rel = Path(relative)
    if rel.is_absolute() or ".." in rel.parts or rel.as_posix() != relative:
        _fail("L1V006_REQUEST_INVALID", f"{label} path escapes candidate")
    path = candidate / rel
    if path.is_symlink() or not path.is_file():
        _fail("L1V001_INPUT_MISSING", f"{label} is missing or not a regular file")
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(candidate.resolve(strict=True))
        return path, resolved.read_bytes()
    except (OSError, ValueError) as exc:
        _fail("L1V001_INPUT_MISSING", f"cannot read {label}: {exc.__class__.__name__}")


def _write_new(path: Path, raw: bytes, label: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        _fail("L1V002_PROVENANCE_EXISTS", f"{label} already exists")
    except OSError as exc:
        _fail("L1V003_PROVENANCE_WRITE_FAILED", f"cannot write {label}: {exc.__class__.__name__}")


@dataclass(frozen=True, slots=True)
class VerifierAdapter:
    """Run one exact verifier command and persist its closed provenance."""

    python_executable: Path
    verifier_path: Path
    verifier_sha256: str
    cwd: Path
    timeout_seconds: float = 120.0

    def __post_init__(self) -> None:
        for field, path in (
            ("python_executable", self.python_executable),
            ("verifier_path", self.verifier_path),
            ("cwd", self.cwd),
        ):
            if not isinstance(path, Path) or not path.is_absolute() or path != path.resolve(strict=False):
                _fail("L1V004_CONFIGURATION_INVALID", f"{field} must be an absolute normalized Path")
        if type(self.verifier_sha256) is not str or _SHA256_RE.fullmatch(self.verifier_sha256) is None:
            _fail("L1V004_CONFIGURATION_INVALID", "verifier_sha256 must be lowercase SHA-256")
        if (
            type(self.timeout_seconds) not in {int, float}
            or isinstance(self.timeout_seconds, bool)
            or not math.isfinite(float(self.timeout_seconds))
            or float(self.timeout_seconds) <= 0
        ):
            _fail("L1V004_CONFIGURATION_INVALID", "timeout_seconds must be finite and positive")

    def write_report(
        self,
        *,
        plan: AttemptPlan,
        command: LegacyRunnerCommand,
        result: ExecutionResult,
    ) -> Path:
        if result.returncode != 0:
            _fail("L1V005_RUNNER_NONZERO", "verifier provenance requires runner exit zero")
        candidate = plan.promotion.source
        if candidate.is_symlink() or not candidate.is_dir():
            _fail("L1V001_INPUT_MISSING", "candidate directory is missing")
        request_path, request_raw = _read_regular_inside(candidate, REQUEST_FILENAME, "verify request")
        request = _decode_object(request_raw, "verify request")
        if set(request) != _REQUEST_KEYS:
            _fail("L1V006_REQUEST_INVALID", "verify request keys are not closed")
        if request.get("schema_name") != REQUEST_SCHEMA_NAME or request.get("schema_version") != REQUEST_SCHEMA_VERSION:
            _fail("L1V006_REQUEST_INVALID", "verify request schema mismatch")

        verifier = self.verifier_path
        if verifier.is_symlink() or not verifier.is_file():
            _fail("L1V001_INPUT_MISSING", "frozen verifier is missing or not a regular file")
        verifier_raw = verifier.read_bytes()
        if _sha256(verifier_raw) != self.verifier_sha256:
            _fail("L1V007_VERIFIER_DRIFT", "frozen verifier SHA-256 drifted before execution")
        if not self.python_executable.is_file() or not self.cwd.is_dir():
            _fail("L1V004_CONFIGURATION_INVALID", "python executable or cwd is missing")

        artifacts = request.get("artifacts")
        if type(artifacts) is not dict:
            _fail("L1V006_REQUEST_INVALID", "request.artifacts must be an object")
        expected_artifact_paths = {
            artifacts.get("metadata_path"),
            artifacts.get("groundtruth_path"),
            artifacts.get("metrics_path"),
        }
        if expected_artifact_paths != {
            "metadata.json",
            "groundtruth.json",
            "raw/metrics/metrics_v2.jsonl",
        }:
            _fail("L1V006_REQUEST_INVALID", "request artifact paths are not canonical")
        entries: list[dict[str, Any]] = []
        for relative in _VERIFIER_INPUT_PATHS:
            path, raw = _read_regular_inside(candidate, relative, f"verifier input {relative}")
            entries.append(
                {
                    "path": path.relative_to(candidate).as_posix(),
                    "size": len(raw),
                    "sha256": _sha256(raw),
                }
            )
        entries.sort(key=lambda row: row["path"])
        manifest = {
            "schema_name": MANIFEST_SCHEMA_NAME,
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "entries": entries,
        }
        manifest_raw = _canonical_json(manifest)
        manifest_path = candidate / MANIFEST_FILENAME
        _write_new(manifest_path, manifest_raw, "candidate manifest")

        stdout_path = candidate / STDOUT_FILENAME
        stderr_path = candidate / STDERR_FILENAME
        try:
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_handle = stdout_path.open("xb")
            stderr_handle = stderr_path.open("xb")
        except FileExistsError:
            _fail("L1V002_PROVENANCE_EXISTS", "verifier stdout/stderr already exists")
        except OSError as exc:
            _fail("L1V003_PROVENANCE_WRITE_FAILED", f"cannot open verifier logs: {exc.__class__.__name__}")

        argv = (str(self.python_executable), str(verifier), str(candidate))
        try:
            with stdout_handle, stderr_handle:
                completed = subprocess.run(
                    list(argv),
                    cwd=str(self.cwd),
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    shell=False,
                    check=False,
                    timeout=float(self.timeout_seconds),
                )
                stdout_handle.flush()
                stderr_handle.flush()
                os.fsync(stdout_handle.fileno())
                os.fsync(stderr_handle.fileno())
        except subprocess.TimeoutExpired as exc:
            _fail("L1V008_VERIFIER_TIMEOUT", f"verifier timed out after {exc.timeout} seconds")
        except OSError as exc:
            _fail("L1V009_VERIFIER_EXECUTION_FAILED", f"cannot execute verifier: {exc.__class__.__name__}")

        verifier_after = verifier.read_bytes()
        if _sha256(verifier_after) != self.verifier_sha256:
            _fail("L1V007_VERIFIER_DRIFT", "frozen verifier SHA-256 drifted during execution")
        stdout_raw = stdout_path.read_bytes()
        stderr_raw = stderr_path.read_bytes()
        request_sha256 = _sha256(request_raw)
        manifest_sha256 = _sha256(manifest_raw)
        result_document = {
            "schema_name": RESULT_SCHEMA_NAME,
            "schema_version": RESULT_SCHEMA_VERSION,
            "request_sha256": request_sha256,
            "verifier_path": str(verifier),
            "verifier_sha256": self.verifier_sha256,
            "argv": list(argv),
            "shell": False,
            "exit_code": completed.returncode,
            "stdout_sha256": _sha256(stdout_raw),
            "stderr_sha256": _sha256(stderr_raw),
            "candidate_manifest_sha256": manifest_sha256,
            "passed": completed.returncode == 0,
        }
        result_raw = _canonical_json(result_document)
        result_path = candidate / RESULT_FILENAME
        _write_new(result_path, result_raw, "verifier result")

        report = dict(request)
        report["verifier"] = {
            "kind": VERIFIER_KIND,
            "passed": completed.returncode == 0,
            "request_path": request_path.relative_to(candidate).as_posix(),
            "request_sha256": request_sha256,
            "verifier_path": str(verifier),
            "verifier_sha256": self.verifier_sha256,
            "argv": list(argv),
            "shell": False,
            "exit_code": completed.returncode,
            "stdout_path": STDOUT_FILENAME,
            "stdout_sha256": _sha256(stdout_raw),
            "stderr_path": STDERR_FILENAME,
            "stderr_sha256": _sha256(stderr_raw),
            "candidate_manifest_path": MANIFEST_FILENAME,
            "candidate_manifest_sha256": manifest_sha256,
            "result_path": RESULT_FILENAME,
            "result_sha256": _sha256(result_raw),
        }
        report_path = candidate / REPORT_FILENAME
        _write_new(report_path, _canonical_json(report), "verify report")
        return report_path


__all__ = [
    "FROZEN_VERIFY_DUAL_SHA256",
    "MANIFEST_FILENAME",
    "MANIFEST_SCHEMA_NAME",
    "MANIFEST_SCHEMA_VERSION",
    "PROVENANCE_DIRNAME",
    "REPORT_FILENAME",
    "REQUEST_FILENAME",
    "REQUEST_SCHEMA_NAME",
    "REQUEST_SCHEMA_VERSION",
    "RESULT_FILENAME",
    "RESULT_SCHEMA_NAME",
    "RESULT_SCHEMA_VERSION",
    "STDERR_FILENAME",
    "STDOUT_FILENAME",
    "VERIFIER_KIND",
    "VerifierAdapter",
    "VerifierAdapterError",
]
