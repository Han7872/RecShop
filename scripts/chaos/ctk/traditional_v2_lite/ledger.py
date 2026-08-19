"""Fail-closed append-only attempt ledger for traditional-v2-lite.

The ledger is deliberately small and independent of the Kubernetes runner.
Each attempt gets one JSONL file created with ``O_EXCL``.  Every event has a
closed schema, canonical JSON bytes, a monotonically increasing sequence, and
a SHA-256 link to the preceding event.  Records are flushed with ``fsync``
before control returns to the caller.

This module does not execute subprocesses, access the network, or authorize a
live collection.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LEDGER_SCHEMA_NAME = "traditional_v2_lite_attempt_ledger"
LEDGER_SCHEMA_VERSION = "1.1.0"
ZERO_HASH = "0" * 64

_EVENT_KEYS = frozenset(
    {
        "schema_name",
        "schema_version",
        "attempt_id",
        "sequence",
        "recorded_at_utc",
        "event",
        "previous_hash",
        "payload",
        "event_hash",
    }
)
_PAYLOAD_KEYS = {
    "created": frozenset(
        {
            "block_id",
            "run_ordinal",
            "slot_type",
            "case_id",
            "contract_sha256",
            "schedule_hash",
            "block_schedule_hash",
            "slot_sha256",
        }
    ),
    "prepared": frozenset(
        {
            "attempt_root",
            "runner_out_dir",
            "promotion_source",
            "promotion_destination",
            "command_sha256",
        }
    ),
    "running": frozenset({"executor_name"}),
    "runner_exited": frozenset({"returncode"}),
    "evidence_accepted": frozenset(
        {
            "metadata_sha256",
            "verifier_report_sha256",
            "metadata_passed",
            "verifier_passed",
            "artifact_manifest_sha256",
        }
    ),
    "promotion_started": frozenset(
        {"promotion_source", "promotion_destination", "promotion_method", "artifact_manifest_sha256"}
    ),
    "promoted": frozenset(
        {"promotion_destination", "promotion_method", "artifact_manifest_sha256"}
    ),
    "promotion_uncertain": frozenset(
        {"failure_code", "failure_message", "promotion_destination", "artifact_manifest_sha256"}
    ),
    "failed": frozenset(
        {"failure_code", "failure_message", "failed_from"}
    ),
}
_NEXT_EVENTS = {
    None: frozenset({"created"}),
    "created": frozenset({"prepared", "failed"}),
    "prepared": frozenset({"running", "failed"}),
    "running": frozenset({"runner_exited", "failed"}),
    "runner_exited": frozenset({"evidence_accepted", "failed"}),
    "evidence_accepted": frozenset({"promotion_started", "failed"}),
    "promotion_started": frozenset({"promoted", "promotion_uncertain"}),
    "promoted": frozenset(),
    "promotion_uncertain": frozenset(),
    "failed": frozenset(),
}
_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_BLOCK_RE = re.compile(r"B[1-5]\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_UTC_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z"
)


class LedgerError(RuntimeError):
    """Stable rejection raised by the Lite-1 ledger."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> "None":
    raise LedgerError(code, message)


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        _fail("L1L001_NON_CANONICAL_EVENT", f"invalid JSON value: {exc}")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _validate_text(value: Any, field: str, *, max_length: int = 4096) -> str:
    if (
        type(value) is not str
        or not value
        or "\x00" in value
        or len(value) > max_length
    ):
        _fail("L1L002_SCHEMA_INVALID", f"{field} must be non-empty bounded text")
    return value


def _validate_id(value: Any, field: str) -> str:
    if type(value) is not str or _ID_RE.fullmatch(value) is None:
        _fail("L1L002_SCHEMA_INVALID", f"{field} is not a safe identifier")
    return value


def _validate_sha256(value: Any, field: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        _fail("L1L002_SCHEMA_INVALID", f"{field} must be lowercase SHA-256")
    return value


def _validate_payload(event: str, payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != _PAYLOAD_KEYS[event]:
        actual = set(payload) if isinstance(payload, Mapping) else set()
        expected = _PAYLOAD_KEYS[event]
        _fail(
            "L1L002_SCHEMA_INVALID",
            f"{event} payload missing={sorted(expected - actual)!r} "
            f"extra={sorted(actual - expected)!r}",
        )
    value = dict(payload)
    if event == "created":
        if type(value["block_id"]) is not str or _BLOCK_RE.fullmatch(value["block_id"]) is None:
            _fail("L1L002_SCHEMA_INVALID", "created.block_id must be B1-B5")
        if type(value["run_ordinal"]) is not int or not 1 <= value["run_ordinal"] <= 46:
            _fail("L1L002_SCHEMA_INVALID", "created.run_ordinal must be 1..46")
        if value["slot_type"] not in {"fault", "control"}:
            _fail("L1L002_SCHEMA_INVALID", "created.slot_type is invalid")
        _validate_id(value["case_id"], "created.case_id")
        for field in (
            "contract_sha256",
            "schedule_hash",
            "block_schedule_hash",
            "slot_sha256",
        ):
            _validate_sha256(value[field], f"created.{field}")
            if value[field] == ZERO_HASH:
                _fail("L1L002_SCHEMA_INVALID", f"created.{field} cannot be a placeholder")
    elif event == "prepared":
        for field in (
            "attempt_root",
            "runner_out_dir",
            "promotion_source",
            "promotion_destination",
        ):
            _validate_text(value[field], f"prepared.{field}")
        _validate_sha256(value["command_sha256"], "prepared.command_sha256")
    elif event == "running":
        _validate_text(value["executor_name"], "running.executor_name", max_length=128)
    elif event == "runner_exited":
        if type(value["returncode"]) is not int:
            _fail("L1L002_SCHEMA_INVALID", "runner_exited.returncode must be int")
    elif event == "evidence_accepted":
        _validate_sha256(value["metadata_sha256"], "evidence.metadata_sha256")
        _validate_sha256(
            value["verifier_report_sha256"], "evidence.verifier_report_sha256"
        )
        if value["metadata_sha256"] == ZERO_HASH or value["verifier_report_sha256"] == ZERO_HASH:
            _fail("L1L006_EVIDENCE_INVALID", "evidence hashes cannot be placeholders")
        if value["metadata_passed"] is not True or value["verifier_passed"] is not True:
            _fail("L1L006_EVIDENCE_INVALID", "only independently passing evidence may be accepted")
        _validate_sha256(value["artifact_manifest_sha256"], "evidence.artifact_manifest_sha256")
    elif event == "promotion_started":
        _validate_text(value["promotion_source"], "promotion_started.source")
        _validate_text(value["promotion_destination"], "promotion_started.destination")
        if value["promotion_method"] != "same-volume-atomic-directory-rename":
            _fail("L1L002_SCHEMA_INVALID", "unexpected promotion method")
        _validate_sha256(value["artifact_manifest_sha256"], "promotion_started.artifact_manifest_sha256")
    elif event == "promoted":
        _validate_text(value["promotion_destination"], "promoted.destination")
        if value["promotion_method"] != "same-volume-atomic-directory-rename":
            _fail("L1L002_SCHEMA_INVALID", "unexpected promotion method")
        _validate_sha256(value["artifact_manifest_sha256"], "promoted.artifact_manifest_sha256")
    elif event == "promotion_uncertain":
        _validate_id(value["failure_code"], "promotion_uncertain.failure_code")
        _validate_text(value["failure_message"], "promotion_uncertain.failure_message")
        _validate_text(value["promotion_destination"], "promotion_uncertain.destination")
        _validate_sha256(value["artifact_manifest_sha256"], "promotion_uncertain.artifact_manifest_sha256")
    elif event == "failed":
        _validate_id(value["failure_code"], "failed.failure_code")
        _validate_text(value["failure_message"], "failed.failure_message")
        if value["failed_from"] not in {
            "created",
            "prepared",
            "running",
            "runner_exited",
            "evidence_accepted",
        }:
            _fail("L1L002_SCHEMA_INVALID", "failed.failed_from is invalid")
    return value


def _utc_now(clock: Callable[[], datetime]) -> str:
    value = clock()
    if not isinstance(value, datetime) or value.tzinfo is None:
        _fail("L1L002_SCHEMA_INVALID", "ledger clock must return an aware datetime")
    value = value.astimezone(timezone.utc)
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


class _DuplicateKeyDecoder(json.JSONDecoder):
    def __init__(self) -> None:
        super().__init__(object_pairs_hook=self._pairs, parse_constant=self._constant)

    @staticmethod
    def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("L1L001_NON_CANONICAL_EVENT", f"duplicate JSON key: {key}")
            result[key] = value
        return result

    @staticmethod
    def _constant(value: str) -> "None":
        _fail("L1L001_NON_CANONICAL_EVENT", f"non-finite JSON value: {value}")


def _validate_event(
    record: Any,
    *,
    expected_attempt_id: str | None,
    expected_sequence: int,
    expected_previous_hash: str,
    previous_event: str | None,
    previous_returncode: int | None,
) -> tuple[dict[str, Any], int | None]:
    if not isinstance(record, Mapping) or set(record) != _EVENT_KEYS:
        _fail("L1L002_SCHEMA_INVALID", "ledger event has unknown or missing fields")
    value = dict(record)
    if value["schema_name"] != LEDGER_SCHEMA_NAME or value["schema_version"] != LEDGER_SCHEMA_VERSION:
        _fail("L1L002_SCHEMA_INVALID", "ledger schema identity mismatch")
    attempt_id = _validate_id(value["attempt_id"], "attempt_id")
    if expected_attempt_id is not None and attempt_id != expected_attempt_id:
        _fail("L1L003_ATTEMPT_ID_MISMATCH", "ledger mixes attempt IDs")
    if value["sequence"] != expected_sequence or type(value["sequence"]) is not int:
        _fail("L1L004_CHAIN_BROKEN", "ledger sequence is not contiguous")
    if type(value["recorded_at_utc"]) is not str or _UTC_RE.fullmatch(value["recorded_at_utc"]) is None:
        _fail("L1L002_SCHEMA_INVALID", "recorded_at_utc is not canonical UTC")
    event = value["event"]
    if type(event) is not str or event not in _PAYLOAD_KEYS:
        _fail("L1L002_SCHEMA_INVALID", "unknown ledger event")
    if event not in _NEXT_EVENTS[previous_event]:
        _fail(
            "L1L005_STATE_TRANSITION_INVALID",
            f"cannot append {event!r} after {previous_event!r}",
        )
    if value["previous_hash"] != expected_previous_hash:
        _fail("L1L004_CHAIN_BROKEN", "previous_hash does not match prior event")
    _validate_sha256(value["previous_hash"], "previous_hash")
    payload = _validate_payload(event, value["payload"])
    if event == "failed" and payload["failed_from"] != previous_event:
        _fail("L1L005_STATE_TRANSITION_INVALID", "failed_from does not match current state")
    if event == "evidence_accepted" and previous_returncode != 0:
        _fail(
            "L1L006_EVIDENCE_INVALID",
            "evidence cannot be accepted unless runner exit was zero",
        )
    _validate_sha256(value["event_hash"], "event_hash")
    core = dict(value)
    del core["event_hash"]
    if value["event_hash"] != _sha256(core):
        _fail("L1L004_CHAIN_BROKEN", "event_hash does not match canonical event")
    if event == "runner_exited":
        previous_returncode = payload["returncode"]
    return value, previous_returncode


def read_ledger(path: os.PathLike[str] | str) -> tuple[dict[str, Any], ...]:
    """Read and independently validate a complete canonical JSONL ledger."""

    ledger_path = Path(path)
    try:
        raw = ledger_path.read_bytes()
    except OSError as exc:
        _fail("L1L007_IO_ERROR", f"cannot read ledger: {exc.__class__.__name__}")
    if not raw or not raw.endswith(b"\n") or b"\r" in raw:
        _fail("L1L001_NON_CANONICAL_EVENT", "ledger must be non-empty LF-terminated JSONL")
    records: list[dict[str, Any]] = []
    attempt_id: str | None = None
    previous_hash = ZERO_HASH
    previous_event: str | None = None
    previous_returncode: int | None = None
    artifact_manifest_sha256: str | None = None
    for sequence, line in enumerate(raw.splitlines(), start=1):
        try:
            text = line.decode("utf-8", errors="strict")
            decoded = json.loads(text, cls=_DuplicateKeyDecoder)
        except LedgerError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            _fail("L1L001_NON_CANONICAL_EVENT", f"invalid ledger JSON: {exc.__class__.__name__}")
        record, previous_returncode = _validate_event(
            decoded,
            expected_attempt_id=attempt_id,
            expected_sequence=sequence,
            expected_previous_hash=previous_hash,
            previous_event=previous_event,
            previous_returncode=previous_returncode,
        )
        if line != _canonical_json_bytes(record):
            _fail("L1L001_NON_CANONICAL_EVENT", "ledger line is not canonical JSON")
        attempt_id = record["attempt_id"]
        previous_hash = record["event_hash"]
        previous_event = record["event"]
        if record["event"] in {
            "evidence_accepted",
            "promotion_started",
            "promoted",
            "promotion_uncertain",
        }:
            current_manifest = record["payload"]["artifact_manifest_sha256"]
            if artifact_manifest_sha256 is None:
                artifact_manifest_sha256 = current_manifest
            elif current_manifest != artifact_manifest_sha256:
                _fail("L1L006_EVIDENCE_INVALID", "promotion events bind different artifact manifests")
        records.append(record)
    return tuple(records)


class AttemptLedger:
    """One-process append handle for a single immutable attempt ledger."""

    def __init__(
        self,
        *,
        path: Path,
        fd: int,
        attempt_id: str,
        clock: Callable[[], datetime],
    ) -> None:
        self.path = path
        self.attempt_id = attempt_id
        self._fd = fd
        self._clock = clock
        self._lock = threading.RLock()
        self._sequence = 0
        self._previous_hash = ZERO_HASH
        self._state: str | None = None
        self._runner_returncode: int | None = None
        self._artifact_manifest_sha256: str | None = None
        self._closed = False

    @classmethod
    def create(
        cls,
        path: os.PathLike[str] | str,
        *,
        attempt_id: str,
        block_id: str,
        run_ordinal: int,
        slot_type: str,
        case_id: str,
        contract_sha256: str,
        schedule_hash: str,
        block_schedule_hash: str,
        slot_sha256: str,
        clock: Callable[[], datetime] | None = None,
    ) -> "AttemptLedger":
        """Create a new attempt ledger without any overwrite path."""

        _validate_id(attempt_id, "attempt_id")
        ledger_path = Path(path)
        try:
            ledger_path.parent.mkdir(parents=True, exist_ok=True)
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_APPEND
                | getattr(os, "O_BINARY", 0)
            )
            fd = os.open(ledger_path, flags, 0o600)
        except FileExistsError:
            _fail("L1L008_ATTEMPT_ALREADY_EXISTS", f"ledger already exists: {ledger_path}")
        except OSError as exc:
            _fail("L1L007_IO_ERROR", f"cannot create ledger: {exc.__class__.__name__}")
        ledger = cls(
            path=ledger_path,
            fd=fd,
            attempt_id=attempt_id,
            clock=clock or (lambda: datetime.now(timezone.utc)),
        )
        try:
            ledger.append(
                "created",
                {
                    "block_id": block_id,
                    "run_ordinal": run_ordinal,
                    "slot_type": slot_type,
                    "case_id": case_id,
                    "contract_sha256": contract_sha256,
                    "schedule_hash": schedule_hash,
                    "block_schedule_hash": block_schedule_hash,
                    "slot_sha256": slot_sha256,
                },
            )
        except Exception:
            ledger.close()
            raise
        return ledger

    @property
    def state(self) -> str | None:
        return self._state

    @property
    def is_terminal(self) -> bool:
        return self._state in {"promoted", "promotion_uncertain", "failed"}

    @property
    def promotion_outcome_uncertain(self) -> bool:
        """Whether promotion started but lacks a durable terminal outcome record."""

        return self._state == "promotion_started"

    def append(self, event: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self._closed:
                _fail("L1L007_IO_ERROR", "cannot append to a closed ledger")
            if event not in _PAYLOAD_KEYS:
                _fail("L1L002_SCHEMA_INVALID", f"unknown event: {event!r}")
            if event not in _NEXT_EVENTS[self._state]:
                _fail(
                    "L1L005_STATE_TRANSITION_INVALID",
                    f"cannot append {event!r} after {self._state!r}",
                )
            checked_payload = _validate_payload(event, payload)
            if event == "failed" and checked_payload["failed_from"] != self._state:
                _fail("L1L005_STATE_TRANSITION_INVALID", "failed_from must match current state")
            if event == "evidence_accepted" and self._runner_returncode != 0:
                _fail("L1L006_EVIDENCE_INVALID", "runner exit zero is required before evidence")
            if event in {
                "evidence_accepted",
                "promotion_started",
                "promoted",
                "promotion_uncertain",
            }:
                manifest_sha256 = checked_payload["artifact_manifest_sha256"]
                if self._artifact_manifest_sha256 is not None and manifest_sha256 != self._artifact_manifest_sha256:
                    _fail("L1L006_EVIDENCE_INVALID", "promotion events must bind one artifact manifest")
            core: dict[str, Any] = {
                "schema_name": LEDGER_SCHEMA_NAME,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "attempt_id": self.attempt_id,
                "sequence": self._sequence + 1,
                "recorded_at_utc": _utc_now(self._clock),
                "event": event,
                "previous_hash": self._previous_hash,
                "payload": checked_payload,
            }
            record = dict(core)
            record["event_hash"] = _sha256(core)
            raw = _canonical_json_bytes(record) + b"\n"
            try:
                written = 0
                while written < len(raw):
                    count = os.write(self._fd, raw[written:])
                    if count <= 0:
                        raise OSError("short ledger write")
                    written += count
                os.fsync(self._fd)
            except OSError as exc:
                _fail("L1L007_IO_ERROR", f"cannot persist ledger event: {exc.__class__.__name__}")
            self._sequence += 1
            self._previous_hash = record["event_hash"]
            self._state = event
            if event == "runner_exited":
                self._runner_returncode = checked_payload["returncode"]
            if event == "evidence_accepted":
                self._artifact_manifest_sha256 = checked_payload["artifact_manifest_sha256"]
            return record

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                os.close(self._fd)
            finally:
                self._closed = True

    def __enter__(self) -> "AttemptLedger":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


__all__ = [
    "AttemptLedger",
    "LEDGER_SCHEMA_NAME",
    "LEDGER_SCHEMA_VERSION",
    "LedgerError",
    "ZERO_HASH",
    "read_ledger",
]
