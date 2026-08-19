"""Narrow E2 manifest for the protocol's required candidate artifacts.

This module enumerates an exact, closed path tuple.  It deliberately does not
walk a directory or claim protection against hostile filesystem objects.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Mapping

from .runner_wrapper import LiteWrapperError


ARTIFACT_MANIFEST_FILENAME = "artifact-manifest.json"
ARTIFACT_MANIFEST_SCHEMA_NAME = "traditional_v2_lite_required_artifact_manifest"
ARTIFACT_MANIFEST_SCHEMA_VERSION = "1.0.0"
REQUIRED_ARTIFACT_PATHS = (
    "groundtruth.json",
    "metadata.json",
    "raw/metrics/metrics_v2.jsonl",
    "raw/traces/during_fault_traces.jsonl",
    "summary.md",
    "verifier-provenance/candidate-manifest.json",
    "verifier-provenance/result.json",
    "verifier-provenance/stderr.log",
    "verifier-provenance/stdout.log",
    "verify-report.json",
    "verify-request.json",
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_DOCUMENT_KEYS = {"schema_name", "schema_version", "entries"}
_ENTRY_KEYS = {"path", "size", "sha256"}


class ArtifactManifestError(LiteWrapperError):
    """Stable fail-closed error from required-artifact revalidation."""


def _fail(code: str, message: str) -> "None":
    raise ArtifactManifestError(code, message)


def _canonical_json(value: Any) -> bytes:
    try:
        return (
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        _fail("L2M001_MANIFEST_INVALID", f"manifest cannot be canonicalized: {exc}")


def _read_required(root: Path, relative: str) -> bytes:
    path = root / relative
    if path.is_symlink() or not path.is_file():
        _fail("L2M002_REQUIRED_ARTIFACT_MISSING", f"required artifact missing: {relative}")
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
        return resolved.read_bytes()
    except (OSError, ValueError) as exc:
        _fail("L2M002_REQUIRED_ARTIFACT_MISSING", f"required artifact unreadable: {relative} ({exc.__class__.__name__})")


def build_manifest_document(root: Path) -> dict[str, Any]:
    if not isinstance(root, Path) or not root.is_absolute() or root.is_symlink() or not root.is_dir():
        _fail("L2M001_MANIFEST_INVALID", "manifest root must be an absolute existing directory")
    entries = []
    for relative in REQUIRED_ARTIFACT_PATHS:
        raw = _read_required(root, relative)
        entries.append({"path": relative, "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    return {
        "schema_name": ARTIFACT_MANIFEST_SCHEMA_NAME,
        "schema_version": ARTIFACT_MANIFEST_SCHEMA_VERSION,
        "entries": entries,
    }


def write_manifest(root: Path, manifest_path: Path) -> str:
    document = build_manifest_document(root)
    raw = _canonical_json(document)
    path = manifest_path
    if not path.is_absolute() or path.name != ARTIFACT_MANIFEST_FILENAME or path.parent == root:
        _fail("L2M001_MANIFEST_INVALID", "artifact manifest must use a fixed path outside candidate root")
    try:
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        _fail("L2M003_MANIFEST_EXISTS", "artifact manifest already exists")
    except OSError as exc:
        _fail("L2M004_MANIFEST_WRITE_FAILED", f"cannot write artifact manifest: {exc.__class__.__name__}")
    return hashlib.sha256(raw).hexdigest()


def revalidate_manifest(root: Path, manifest_path: Path, expected_sha256: str) -> str:
    if type(expected_sha256) is not str or _SHA256_RE.fullmatch(expected_sha256) is None:
        _fail("L2M001_MANIFEST_INVALID", "expected manifest SHA-256 is invalid")
    path = manifest_path
    if not path.is_absolute() or path.name != ARTIFACT_MANIFEST_FILENAME or path.parent == root:
        _fail("L2M001_MANIFEST_INVALID", "artifact manifest path is invalid")
    if path.is_symlink() or not path.is_file():
        _fail("L2M002_REQUIRED_ARTIFACT_MISSING", "artifact manifest is missing")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        _fail("L2M005_MANIFEST_DRIFT", "artifact manifest file drifted")
    try:
        value = json.loads(raw.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("L2M001_MANIFEST_INVALID", f"artifact manifest is invalid JSON: {exc}")
    if type(value) is not dict or set(value) != _DOCUMENT_KEYS:
        _fail("L2M001_MANIFEST_INVALID", "artifact manifest document keys are not closed")
    if value["schema_name"] != ARTIFACT_MANIFEST_SCHEMA_NAME or value["schema_version"] != ARTIFACT_MANIFEST_SCHEMA_VERSION:
        _fail("L2M001_MANIFEST_INVALID", "artifact manifest schema mismatch")
    entries = value["entries"]
    if type(entries) is not list or len(entries) != len(REQUIRED_ARTIFACT_PATHS):
        _fail("L2M001_MANIFEST_INVALID", "artifact manifest entry count mismatch")
    actual = build_manifest_document(root)
    if value != actual:
        _fail("L2M005_MANIFEST_DRIFT", "required artifact content drifted")
    return expected_sha256


__all__ = [
    "ARTIFACT_MANIFEST_FILENAME",
    "ARTIFACT_MANIFEST_SCHEMA_NAME",
    "ARTIFACT_MANIFEST_SCHEMA_VERSION",
    "ArtifactManifestError",
    "REQUIRED_ARTIFACT_PATHS",
    "build_manifest_document",
    "revalidate_manifest",
    "write_manifest",
]
