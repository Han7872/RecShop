"""Narrow, fail-closed engineering-smoke coordinator for no-fault/D10/T05.

The module contains no implicit live action.  ``main`` validates the complete
double-confirmation CLI before constructing the fixed production backend.
Tests inject one small fake backend; production does not accept a backend or
evaluator plugin from argv.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol, Sequence

from .cleanup_audit import CleanupAction, CleanupAuditResult, ResourceRecord, audit_cleanup
from .phase_journal import PHASES, PhaseJournal, PhaseRecord
from .telemetry_journal import QueryRecord, telemetry_acceptable
from .workload_driver import Completion, CompletionHandle, WorkloadRecord, run_open_loop


SCENARIO_TO_RUNNER = {
    "no-fault": None,
    "D10": "db_lock_x_netdelay",
    "T05": "checkout_podfail_x_cart_cpu_x_pricing_cpu",
}
EXPECTED_RUNNER_SHA256 = "ae2963c2ea319744aa8bb00b8a6a8abb788aef8a358e1f753e13ab1027f71663"
EXPECTED_CONTEXT = "docker-desktop"
EXPECTED_NAMESPACE = "recweb-chaos"
EXPECTED_DB_CHECKSUMS = {"inventory": 3935678504, "items": 3849590678}
REQUIRED_DEPLOYMENTS = frozenset(
    {"catalog-gw", "catalog", "search", "user", "pricing", "checkout", "cart"}
)
FROZEN_COMPONENT_SHA256 = {
    "artifact_manifest.py": "a31a26b0d2b3d31a3afed1111d7486dd2bf890e483e6f76ff77a72c73b5d3a05",
    "cleanup_audit.py": "6aab6ce349dbdadff3842a8eed37f1656db1930c28aa8e9dc24fd78389892c24",
    "control_adapter.py": "17e696a3834e42c1d0b453deffefabb090a0af8ca4352b5ff64a98be37c36dfc",
    "evidence.py": "40bdd8a5854db52a62800c458109875a3f9d534d9ab00dcfeea925560146dafe",
    "phase_journal.py": "d91853aa2e7a1878ebfa718592a9d81d76242eb1036cfc77c6068c7ab24c10be",
    "telemetry_journal.py": "92a84894b7bab28e0458ad75d6da1c5f7e01dcc3e8ca077f8a1fa2cbd4859a86",
    "verifier_adapter.py": "5a04ca6e3d4541d9cfa3a9f904c5eac6b139a549aa870314d36ae517fc53ecc4",
    "workload_driver.py": "35deb7ba03ac00da30f934fb61c34e7556ecea50b572415fe7c0464116875ae3",
}
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_K8S_UID = re.compile(r"[0-9a-f]{8}-[0-9a-f-]{27,63}\Z")

# ---------- strict51 shared telemetry query contract (acceptance A3) ----------
# Fixed, time-bounded, read-only queries against the three required backends.
# Execution scheduling stays monotonic; the query window is a separate frozen
# UTC wall-clock interval carried as integer Unix microseconds.  HTTP timeout
# is 20s with exactly one fixed retry (2s backoff) for transport timeouts only.
STRICT51_QUERY_HTTP_TIMEOUT_SEC = 20.0
STRICT51_QUERY_RETRY_BACKOFF_SEC = 2.0
STRICT51_QUERY_ATTEMPT_SCHEMA = "strict51-query-attempt.v1"
STRICT51_QUERY_SUMMARY_SCHEMA = "strict51-query-summary.v1"
STRICT51_QUERY_COUNT_PATHS = MappingProxyType({
    "prometheus.up": ("data", "result"),
    "jaeger.traces": ("data",),
    "loki.logs": ("data", "result"),
})
STRICT51_QUERY_SELECTOR = '{k8s_namespace_name="recweb-chaos"}'


def strict51_query_urls(window_start_us: int, window_end_us: int) -> dict[str, str]:
    """Frozen per-backend URLs from one integer UTC microsecond window.

    Seconds/microseconds/nanoseconds derive mechanically from the same frozen
    integers; the Prometheus time parameter is fixed six-decimal seconds built
    by integer division (no float round-trip, no monotonic values)."""
    for name, value in (("window_start_us", window_start_us), ("window_end_us", window_end_us)):
        if type(value) is not int or value <= 0 or window_end_us < window_start_us:
            _fail("E5S010_INVALID_EVIDENCE", f"invalid strict51 query {name}")
    end_seconds = f"{window_end_us // 1_000_000}.{window_end_us % 1_000_000:06d}"
    selector = urllib.parse.quote(STRICT51_QUERY_SELECTOR, safe="")
    return {
        "prometheus.up": f"http://127.0.0.1:9090/api/v1/query?query=up&time={end_seconds}",
        "jaeger.traces": (
            f"http://127.0.0.1:16686/api/traces?service=pricing_service&limit=20"
            f"&start={window_start_us}&end={window_end_us}"
        ),
        "loki.logs": (
            f"http://127.0.0.1:3100/loki/api/v1/query_range?query={selector}"
            f"&start={window_start_us * 1000}&end={window_end_us * 1000}"
            f"&limit=20&direction=BACKWARD"
        ),
    }


def _strict51_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    return isinstance(exc, urllib.error.URLError) and isinstance(
        exc.reason, (TimeoutError, socket.timeout)
    )


def _strict51_fetch(url: str, timeout: float, count_path: tuple[str, ...]) -> dict[str, Any]:
    """One bounded HTTP GET -> closed status dict (no journal side effects)."""
    request = urllib.request.Request(url, method="GET", headers={"User-Agent": "recshop-lite-smoke/1"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            http_status: int | None = int(response.status)
    except Exception as exc:
        retryable = _strict51_retryable(exc)
        return {
            "status": "timeout" if retryable else "query_error",
            "http_status": None,
            "record_count": None,
            "error_class": exc.__class__.__name__,
            "retryable": retryable,
        }
    if not raw:
        return {"status": "no_series", "http_status": http_status, "record_count": 0,
                "error_class": None, "retryable": False}
    try:
        parsed = json.loads(raw.decode("utf-8", errors="strict"))
        records: Any = parsed
        for key in count_path:
            records = records[key]
    except Exception as exc:
        return {"status": "parse_error", "http_status": http_status, "record_count": None,
                "error_class": exc.__class__.__name__, "retryable": False}
    if type(records) is not list or not records:
        return {"status": "no_series", "http_status": http_status, "record_count": 0,
                "error_class": None, "retryable": False}
    return {"status": "value", "http_status": http_status, "record_count": len(records),
            "error_class": None, "retryable": False}


def _strict51_write_new_json(path: Path, value: Mapping[str, Any]) -> None:
    raw = _canonical(value)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        _fail("E5S007_ATTEMPT_EXISTS", f"refusing overwrite {path}")
    except OSError as exc:
        _fail("E5S008_WRITE_FAILED", f"cannot write {path.name}: {exc.__class__.__name__}")


def run_strict51_query(
    query_id: str,
    url: str,
    window_start_us: int,
    window_end_us: int,
    journal_dir: Path,
    *,
    fetch: Callable[[str, float, tuple[str, ...]], dict[str, Any]] | None = None,
    sleep: Callable[[float], None] | None = None,
    clock_us: Callable[[], int] | None = None,
) -> QueryRecord:
    """One required backend query under the strict51 A3 contract.

    - at most two HTTP attempts; the single retry fires only for transport
      timeout classes (TimeoutError / socket.timeout / URLError wrapping them)
      and reuses the identical frozen URL and window;
    - every attempt is journalled O_EXCL to ``<journal_dir>/attempt-NN.json``
      before any verdict, and one ``summary.json`` closes the directory;
    - no_series / parse_error / unexpected HTTP are never retried."""
    if query_id not in STRICT51_QUERY_COUNT_PATHS:
        _fail("E5S010_INVALID_EVIDENCE", f"unknown strict51 query id: {query_id}")
    fetch = fetch or _strict51_fetch
    sleeper = sleep or time.sleep
    clock = clock_us or (lambda: time.time_ns() // 1000)
    count_path = STRICT51_QUERY_COUNT_PATHS[query_id]
    attempts: list[dict[str, Any]] = []
    for ordinal in (1, 2):
        started_at_us = clock()
        result = fetch(url, STRICT51_QUERY_HTTP_TIMEOUT_SEC, count_path)
        ended_at_us = clock()
        elapsed_ms = round((ended_at_us - started_at_us) / 1000.0, 3)
        attempt = {
            "schema_version": STRICT51_QUERY_ATTEMPT_SCHEMA,
            "query_id": query_id,
            "url": url,
            "window_start_us": window_start_us,
            "window_end_us": window_end_us,
            "attempt_ordinal": ordinal,
            "started_at_us": started_at_us,
            "ended_at_us": ended_at_us,
            "elapsed_ms": elapsed_ms,
            "http_status": result["http_status"],
            "record_count": result["record_count"],
            "status": result["status"],
            "error_class": result["error_class"],
        }
        _strict51_write_new_json(journal_dir / f"attempt-{ordinal:02d}.json", attempt)
        attempts.append(attempt)
        if result["status"] != "timeout":
            break
        if ordinal == 1:
            sleeper(STRICT51_QUERY_RETRY_BACKOFF_SEC)
    final = attempts[-1]
    summary = {
        "schema_version": STRICT51_QUERY_SUMMARY_SCHEMA,
        "query_id": query_id,
        "url": url,
        "window_start_us": window_start_us,
        "window_end_us": window_end_us,
        "required": True,
        "attempt_count": len(attempts),
        "final_status": final["status"],
        "final_http_status": final["http_status"],
        "final_record_count": final["record_count"],
        "final_error_class": final["error_class"],
    }
    _strict51_write_new_json(journal_dir / "summary.json", summary)
    status = final["status"]
    value: float | None = None
    if status == "value":
        value = float(final["record_count"])
    observed_us = clock()
    backend_summary = (
        f"http={final['http_status']};records={final['record_count']}"
        f";attempts={len(attempts)};error={final['error_class']}"
    )
    return QueryRecord(
        query_id=query_id,
        request=url,
        window_start=float(window_start_us),
        window_end=float(window_end_us),
        backend_summary=backend_summary,
        observed_at=float(observed_us),
        required=True,
        status=status,
        value=value,
    )


class SmokeError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> "None":
    raise SmokeError(code, message)


def _canonical(value: Any) -> bytes:
    try:
        return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
    except (TypeError, ValueError) as exc:
        _fail("E5S010_INVALID_EVIDENCE", f"non-canonical value: {exc}")


def _sha_file(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        _fail("E5S002_IDENTITY_DRIFT", f"missing regular file: {path.name}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_new(path: Path, value: Any) -> None:
    raw = _canonical(value)
    try:
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        _fail("E5S007_ATTEMPT_EXISTS", f"refusing to overwrite {path.name}")
    except OSError as exc:
        _fail("E5S008_WRITE_FAILED", f"cannot write {path.name}: {exc.__class__.__name__}")


@dataclass(frozen=True, slots=True)
class SmokeRequest:
    repo_root: Path
    dataset_root: Path
    attempt_id: str
    scenario: str
    stage_seconds: int
    poll_seconds: float
    workload_count: int
    workload_interval_seconds: float

    def __post_init__(self) -> None:
        if self.scenario not in SCENARIO_TO_RUNNER:
            _fail("E5S001_NOT_AUTHORIZED", "scenario is outside no-fault/D10/T05")
        if _SAFE_ID.fullmatch(self.attempt_id) is None:
            _fail("E5S001_NOT_AUTHORIZED", "attempt id is not safe")
        for path in (self.repo_root, self.dataset_root):
            if not path.is_absolute() or path != path.resolve(strict=False):
                _fail("E5S001_NOT_AUTHORIZED", "roots must be absolute and normalized")
        if type(self.stage_seconds) is not int or self.stage_seconds <= 0:
            _fail("E5S001_NOT_AUTHORIZED", "stage seconds must be positive")
        if type(self.workload_count) is not int or self.workload_count <= 0:
            _fail("E5S001_NOT_AUTHORIZED", "workload count must be positive")
        for value in (self.poll_seconds, self.workload_interval_seconds):
            if type(value) not in {int, float} or isinstance(value, bool) or not math.isfinite(float(value)) or value <= 0:
                _fail("E5S001_NOT_AUTHORIZED", "timing values must be finite and positive")


@dataclass(frozen=True, slots=True)
class DeploymentIdentity:
    name: str
    uid: str
    desired: int
    ready: int
    image: str
    config_sha256: str


@dataclass(frozen=True, slots=True)
class PreflightSnapshot:
    repo_root: str
    runner_sha256: str
    component_sha256: Mapping[str, str]
    context: str
    namespace: str
    namespace_uid: str
    deployments: tuple[DeploymentIdentity, ...]
    backends: Mapping[str, str]
    db_checksums: Mapping[str, int]
    foreign_residuals: tuple[str, ...]
    observed_monotonic: float


@dataclass(frozen=True, slots=True)
class ScenarioActivity:
    runner_invocations: int
    fault_calls: int
    finish: Callable[[], int]
    candidate_path: Path | None = None


@dataclass(frozen=True, slots=True)
class SmokeOutcome:
    attempt_id: str
    scenario: str
    verdict: str
    runner_fault: str | None
    preflight_sha256: str
    phase_count: int
    workload_count: int
    query_count: int
    cleanup_action_count: int
    no_fault_calls: bool
    review_sha256: str
    promoted: bool = False


class SmokeBackend(Protocol):
    def preflight(self, request: SmokeRequest) -> PreflightSnapshot: ...
    def monotonic(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...
    def begin(self, request: SmokeRequest, event_sink: Path) -> ScenarioActivity: ...
    def submit_workload(self, ordinal: int) -> CompletionHandle: ...
    def query(self, window_start: float, window_end: float) -> tuple[QueryRecord, ...]: ...
    def resources(self, attempt_id: str) -> tuple[ResourceRecord, ...]: ...
    def cleanup(self, action: CleanupAction) -> bool: ...
    def post_db_checksums(self) -> Mapping[str, int]: ...
    def identity_sha256(self) -> str: ...


def _preflight_identity_document(snapshot: PreflightSnapshot) -> dict[str, Any]:
    """Stable pre/post identity; observation time is evidence, not identity."""

    value = asdict(snapshot)
    value.pop("observed_monotonic")
    return value


def validate_preflight(request: SmokeRequest, snapshot: PreflightSnapshot,
                       expected_runner_sha256: str = EXPECTED_RUNNER_SHA256) -> str:
    if snapshot.repo_root != str(request.repo_root) or snapshot.runner_sha256 != expected_runner_sha256:
        _fail("E5S002_IDENTITY_DRIFT", "repo or runner identity differs")
    if dict(snapshot.component_sha256) != FROZEN_COMPONENT_SHA256:
        _fail("E5S002_IDENTITY_DRIFT", "E1-E4 component identity differs")
    if snapshot.context != EXPECTED_CONTEXT or snapshot.namespace != EXPECTED_NAMESPACE or _K8S_UID.fullmatch(snapshot.namespace_uid) is None:
        _fail("E5S003_PREFLIGHT_FAILED", "context/namespace/UID is unknown or different")
    by_name = {row.name: row for row in snapshot.deployments}
    if set(by_name) != REQUIRED_DEPLOYMENTS:
        _fail("E5S003_PREFLIGHT_FAILED", "deployment census is not exact")
    for row in by_name.values():
        if _K8S_UID.fullmatch(row.uid) is None or row.desired <= 0 or row.ready != row.desired or not row.image or re.fullmatch(r"[0-9a-f]{64}", row.config_sha256) is None:
            _fail("E5S003_PREFLIGHT_FAILED", f"deployment is not exact-ready: {row.name}")
    if dict(snapshot.backends) != {"jaeger": "ready", "loki": "ready", "prometheus": "ready"}:
        _fail("E5S003_PREFLIGHT_FAILED", "telemetry backend readiness is incomplete")
    if dict(snapshot.db_checksums) != EXPECTED_DB_CHECKSUMS or snapshot.foreign_residuals:
        _fail("E5S003_PREFLIGHT_FAILED", "DB checksum or residual census failed")
    if not math.isfinite(snapshot.observed_monotonic):
        _fail("E5S003_PREFLIGHT_FAILED", "preflight clock is invalid")
    return hashlib.sha256(_canonical(_preflight_identity_document(snapshot))).hexdigest()


def _events(event_sink: Path, attempt_id: str, expected: tuple[str, ...]) -> dict[str, Mapping[str, Any]]:
    if event_sink.is_symlink() or not event_sink.is_file():
        _fail("E5S004_PHASE_INCOMPLETE", "runner event journal is missing")
    records: list[Mapping[str, Any]] = []
    try:
        for line in event_sink.read_text(encoding="utf-8", errors="strict").splitlines():
            value = json.loads(line)
            if type(value) is not dict or set(value) != {"attempt_token", "event", "monotonic_ns", "payload"}:
                _fail("E5S004_PHASE_INCOMPLETE", "runner event schema is not closed")
            if value["attempt_token"] != attempt_id or type(value["monotonic_ns"]) is not int or value["monotonic_ns"] <= 0 or type(value["payload"]) is not dict:
                _fail("E5S004_PHASE_INCOMPLETE", "runner event binding is invalid")
            records.append(value)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail("E5S004_PHASE_INCOMPLETE", f"cannot parse runner event journal: {exc.__class__.__name__}")
    rows: dict[str, Mapping[str, Any]] = {}
    for record in records:
        event = record.get("event")
        if event in expected and event not in rows:
            rows[event] = record
    if set(rows) != set(expected):
        _fail("E5S004_PHASE_INCOMPLETE", "runner event journal is incomplete")
    return rows


def _strict_tick(clock: Callable[[], float], previous: float) -> float:
    now = float(clock())
    if not math.isfinite(now):
        _fail("E5S004_PHASE_INCOMPLETE", "non-finite clock")
    return now if now > previous else previous + 1e-9


def _read_json_object(path: Path, label: str) -> Mapping[str, Any]:
    if path.is_symlink() or not path.is_file():
        _fail("E5S010_INVALID_EVIDENCE", f"missing regular {label}")
    try:
        value = json.loads(path.read_text(encoding="utf-8", errors="strict"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail("E5S010_INVALID_EVIDENCE", f"invalid {label}: {exc.__class__.__name__}")
    if type(value) is not dict:
        _fail("E5S010_INVALID_EVIDENCE", f"{label} must be an object")
    return value


def _direct_review(
    request: SmokeRequest,
    activity: ScenarioActivity,
    queries: tuple[QueryRecord, ...],
    cleanup: CleanupAuditResult,
) -> str:
    if {row.query_id for row in queries} != {
        "prometheus.up", "jaeger.traces", "loki.logs",
    } or any(row.required is not True or row.status != "value" for row in queries):
        _fail("E5S010_INVALID_EVIDENCE", "three-backend telemetry review is not exact")
    facts: dict[str, Any] = {
        "scenario": request.scenario,
        "query_status": {row.query_id: row.status for row in queries},
        "cleanup_count": len(cleanup.actions),
    }
    if request.scenario == "no-fault":
        if activity.candidate_path is not None or activity.runner_invocations or activity.fault_calls:
            _fail("E5S005_CONTROL_FAULT_FORBIDDEN", "no-fault direct review found fault activity")
        facts["zero_fault"] = True
        return hashlib.sha256(_canonical(facts)).hexdigest()

    expected_arity = {"D10": 2, "T05": 3}[request.scenario]
    expected_services = {
        "D10": ["mysql_items_lock", "catalog-gw"],
        "T05": ["checkout", "cart", "pricing"],
    }[request.scenario]
    candidate = activity.candidate_path
    if candidate is None or candidate.is_symlink() or not candidate.is_dir():
        _fail("E5S010_INVALID_EVIDENCE", "legacy candidate directory is missing")
    metadata = _read_json_object(candidate / "metadata.json", "metadata")
    groundtruth = _read_json_object(candidate / "groundtruth.json", "groundtruth")
    roots = metadata.get("root_causes")
    if (
        metadata.get("root_count") != expected_arity
        or groundtruth.get("root_count") != expected_arity
        or type(roots) is not list
        or [row.get("service") for row in roots if type(row) is dict] != expected_services
        or groundtruth.get("root_cause_services") != expected_services
        or metadata.get("sample_id") != candidate.name
        or groundtruth.get("sample_id") != candidate.name
        or metadata.get("sample_status") != "ready_for_release"
        or metadata.get("ready_for_release") is not True
        or metadata.get("validation_complete") is not True
    ):
        _fail("E5S010_INVALID_EVIDENCE", "legacy metadata/GT does not match fixed smoke")
    validation_results = metadata.get("validation_results")
    if (
        type(validation_results) is not list
        or not validation_results
        or any(type(row) is not dict or row.get("status") != "pass" for row in validation_results)
    ):
        _fail("E5S010_INVALID_EVIDENCE", "legacy validation results are not all pass")
    facts.update({
        "case_id": candidate.name,
        "root_count": expected_arity,
        "root_services": expected_services,
        "metadata_sha256": _sha_file(candidate / "metadata.json"),
        "groundtruth_sha256": _sha_file(candidate / "groundtruth.json"),
    })
    return hashlib.sha256(_canonical(facts)).hexdigest()


def run_one_smoke(request: SmokeRequest, backend: SmokeBackend) -> SmokeOutcome:
    """Run exactly one attempt; preflight occurs before the first filesystem write."""

    snapshot = backend.preflight(request)
    preflight_sha = validate_preflight(request, snapshot)
    identity_before = backend.identity_sha256()
    if identity_before != preflight_sha:
        _fail("E5S002_IDENTITY_DRIFT", "backend identity differs from validated preflight")

    attempt_root = request.dataset_root / ".engineering-smoke" / ".attempts" / f"{request.attempt_id}.tmp"
    try:
        attempt_root.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        _fail("E5S007_ATTEMPT_EXISTS", "retry requires a new attempt id")
    _write_new(attempt_root / "preflight.json", asdict(snapshot))

    phases = PhaseJournal()
    t = _strict_tick(backend.monotonic, snapshot.observed_monotonic)
    phases.enter("reset", t)
    t = _strict_tick(backend.monotonic, t); phases.enter("warmup", t)
    t = _strict_tick(backend.monotonic, t); phases.enter("pre_fault", t)
    event_sink = attempt_root / "runner-events.jsonl"
    activity = backend.begin(request, event_sink)
    if request.scenario == "no-fault":
        if activity.runner_invocations != 0 or activity.fault_calls != 0 or event_sink.exists():
            _fail("E5S005_CONTROL_FAULT_FORBIDDEN", "no-fault used a runner/fault surface")
        t = _strict_tick(backend.monotonic, t); phases.enter("injection_transition", t)
        t = _strict_tick(backend.monotonic, t); phases.enter("during_fault", t)
    else:
        rows = _events(event_sink, request.attempt_id, (
            "injection_transition_started", "all_active_confirmed",
        ))
        if activity.runner_invocations != 1 or activity.fault_calls != {"D10": 2, "T05": 3}[request.scenario]:
            _fail("E5S006_RUNNER_MAPPING_INVALID", "fault call count or runner count differs")
        injection = float(rows["injection_transition_started"]["monotonic_ns"]) / 1e9
        active = float(rows["all_active_confirmed"]["monotonic_ns"]) / 1e9
        if injection <= t:
            injection = t + 1e-9
        if active <= injection:
            active = injection + 1e-9
        phases.enter("injection_transition", injection)
        phases.enter("during_fault", active)
        t = active

    workload = run_open_loop(
        count=request.workload_count,
        interval_seconds=request.workload_interval_seconds,
        clock=backend.monotonic,
        sleeper=backend.sleep,
        submit=backend.submit_workload,
    )
    runner_exit = activity.finish()
    if type(runner_exit) is not int or runner_exit != 0:
        _fail("E5S009_RUNNER_FAILED", "smoke runner/control failed")
    if request.scenario == "no-fault":
        t = _strict_tick(backend.monotonic, t); phases.enter("recovery_transition", t)
        t = _strict_tick(backend.monotonic, t); phases.enter("post_recovery", t)
    else:
        rows = _events(event_sink, request.attempt_id, (
            "injection_transition_started", "all_active_confirmed",
            "recovery_transition_started", "recovery_confirmed",
        ))
        recovery = float(rows["recovery_transition_started"]["monotonic_ns"]) / 1e9
        recovered = float(rows["recovery_confirmed"]["monotonic_ns"]) / 1e9
        if recovery <= t:
            recovery = t + 1e-9
        if recovered <= recovery:
            recovered = recovery + 1e-9
        phases.enter("recovery_transition", recovery)
        phases.enter("post_recovery", recovered)
        t = recovered

    queries = backend.query(injection if request.scenario != "no-fault" else t - 2e-9, t)
    if not telemetry_acceptable(queries):
        _fail("E5S011_QUERY_FAILED", "required telemetry query is unavailable")
    resources = backend.resources(request.attempt_id)
    if request.scenario == "no-fault":
        if resources:
            _fail("E5S005_CONTROL_FAULT_FORBIDDEN", "no-fault reported injected resources")
        cleanup = CleanupAuditResult((), (), True)
    else:
        cleanup = audit_cleanup(request.attempt_id, resources)
    cleanup_results = [backend.cleanup(action) for action in cleanup.actions]
    if not all(cleanup_results) or not cleanup.next_allowed:
        _fail("E5S012_CLEANUP_FAILED", "cleanup ownership/residual audit failed")
    post_checksums = dict(backend.post_db_checksums())
    if post_checksums != EXPECTED_DB_CHECKSUMS or backend.identity_sha256() != identity_before:
        _fail("E5S013_POSTCHECK_FAILED", "checksum or environment identity drifted")
    t = _strict_tick(backend.monotonic, t); phases.enter("final_cleanup", t)
    phase_rows = phases.finalize()

    _write_new(attempt_root / "phase-journal.json", [asdict(row) for row in phase_rows])
    _write_new(attempt_root / "workload-journal.json", [asdict(row) for row in workload])
    _write_new(attempt_root / "query-journal.json", [asdict(row) for row in queries])
    _write_new(attempt_root / "cleanup-audit.json", {
        "actions": [asdict(row) for row in cleanup.actions],
        "blocking_resources": [asdict(row) for row in cleanup.blocking_resources],
        "next_allowed": cleanup.next_allowed,
        "results": cleanup_results,
    })
    _write_new(attempt_root / "checksum.json", {
        "pre": dict(snapshot.db_checksums),
        "post": post_checksums,
    })
    review_sha = _direct_review(request, activity, queries, cleanup)
    outcome = SmokeOutcome(
        attempt_id=request.attempt_id,
        scenario=request.scenario,
        verdict="ENGINEERING_SMOKE_CANDIDATE",
        runner_fault=SCENARIO_TO_RUNNER[request.scenario],
        preflight_sha256=preflight_sha,
        phase_count=len(phase_rows),
        workload_count=len(workload),
        query_count=len(queries),
        cleanup_action_count=len(cleanup.actions),
        no_fault_calls=(request.scenario != "no-fault" or activity.fault_calls == 0),
        review_sha256=review_sha,
    )
    _write_new(attempt_root / "outcome.json", asdict(outcome))
    return outcome


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RecShop Lite permanent engineering smoke")
    parser.add_argument("--engineering-smoke", action="store_true")
    parser.add_argument("--scenario", choices=tuple(SCENARIO_TO_RUNNER))
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[4]))
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--stage-seconds", type=int, default=60)
    parser.add_argument("--poll", type=float, default=2.0)
    parser.add_argument("--workload-count", type=int, default=30)
    parser.add_argument("--workload-interval", type=float, default=1.0)
    return parser


def parse_authorized_request(argv: Sequence[str]) -> SmokeRequest:
    args = _parser().parse_args(argv)
    if args.engineering_smoke is not True or args.yes is not True or args.scenario not in SCENARIO_TO_RUNNER:
        _fail("E5S001_NOT_AUTHORIZED", "requires --engineering-smoke --scenario no-fault|D10|T05 --yes")
    return SmokeRequest(
        repo_root=Path(args.repo_root).resolve(strict=False),
        dataset_root=Path(args.dataset_root).resolve(strict=False),
        attempt_id=args.attempt_id,
        scenario=args.scenario,
        stage_seconds=args.stage_seconds,
        poll_seconds=args.poll,
        workload_count=args.workload_count,
        workload_interval_seconds=args.workload_interval,
    )


class _CurlCompletion:
    def __init__(self, process: subprocess.Popen[str], started_at: float) -> None:
        self._process = process
        self._started_at = started_at

    def collect(self) -> Completion:
        try:
            stdout, _stderr = self._process.communicate(timeout=15)
            status = stdout.strip() or "curl_error"
            timed_out = False
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.communicate()
            status = "timeout"
            timed_out = True
        return Completion(time.monotonic(), timed_out, status)


class LocalSmokeBackend:
    """One fixed Windows/local-cluster backend; no replaceable evaluator surface."""

    def __init__(self, request: SmokeRequest) -> None:
        self._request = request
        self._kubectl = os.environ.get("KUBECTL", "kubectl")
        self._user_token = os.environ.get("LITE_SMOKE_USER_TOKEN", "")
        # strict51 (A0.2): the coordinator may retarget the runner identity
        # expectation to the strict51 contract artifact binding; the legacy
        # default keeps the old protocol byte-identical.
        self.expected_runner_sha256 = EXPECTED_RUNNER_SHA256

    def _run(self, argv: Sequence[str], timeout: float = 30) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(tuple(argv), shell=False, text=True, encoding="utf-8", errors="strict", capture_output=True, timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired, UnicodeError) as exc:
            _fail("E5S003_PREFLIGHT_FAILED", f"command failed: {exc.__class__.__name__}")

    def _kubectl_json(self, args: Sequence[str]) -> Mapping[str, Any]:
        cp = self._run((self._kubectl, *args), 45)
        if cp.returncode != 0:
            _fail("E5S003_PREFLIGHT_FAILED", f"kubectl read failed: {' '.join(args[:3])}")
        try:
            value = json.loads(cp.stdout)
        except json.JSONDecodeError:
            _fail("E5S003_PREFLIGHT_FAILED", "kubectl returned non-JSON")
        if type(value) is not dict:
            _fail("E5S003_PREFLIGHT_FAILED", "kubectl JSON root is not an object")
        return value

    def _checksums(self) -> Mapping[str, int]:
        if not os.environ.get("DB_PASSWORD"):
            _fail("E5S003_PREFLIGHT_FAILED", "DB_PASSWORD is required for checksums")
        try:
            import mysql.connector
            connection = mysql.connector.connect(
                host=os.environ.get("DB_HOST", "127.0.0.1"),
                port=int(os.environ.get("DB_PORT", "3306")),
                user=os.environ.get("DB_USER", "root"),
                password=os.environ.get("DB_PASSWORD", ""),
                database=os.environ.get("DB_NAME", "shopify2"),
                connection_timeout=10,
            )
            cursor = connection.cursor()
            result = {}
            for table in ("inventory", "items"):
                cursor.execute(f"CHECKSUM TABLE `{table}`")
                row = cursor.fetchone()
                result[table] = int(row[1])
            cursor.close(); connection.close()
            return result
        except Exception as exc:
            _fail("E5S003_PREFLIGHT_FAILED", f"DB checksum unavailable: {exc.__class__.__name__}")

    @staticmethod
    def _http_ready(url: str) -> bool:
        request = urllib.request.Request(url, method="GET", headers={"User-Agent": "recshop-lite-smoke/1"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return 200 <= response.status < 300
        except Exception:
            return False

    def preflight(self, request: SmokeRequest) -> PreflightSnapshot:
        package = request.repo_root / "scripts" / "chaos" / "ctk" / "traditional_v2_lite"
        runner = request.repo_root / "scripts" / "chaos" / "ctk" / "chaos_k8s_runner.py"
        components = {name: _sha_file(package / name) for name in sorted(FROZEN_COMPONENT_SHA256)}
        context = self._run((self._kubectl, "config", "current-context"), 20)
        if context.returncode != 0:
            _fail("E5S003_PREFLIGHT_FAILED", "cannot read current context")
        namespace_obj = self._kubectl_json(("get", "namespace", EXPECTED_NAMESPACE, "-o", "json"))
        deployments = []
        for name in sorted(REQUIRED_DEPLOYMENTS):
            obj = self._kubectl_json(("-n", EXPECTED_NAMESPACE, "get", "deployment", name, "-o", "json"))
            metadata = obj.get("metadata") or {}
            spec = obj.get("spec") or {}
            status = obj.get("status") or {}
            template = spec.get("template") or {}
            containers = ((template.get("spec") or {}).get("containers") or [])
            image = "|".join(str(row.get("image", "")) for row in containers)
            deployments.append(DeploymentIdentity(
                name=name, uid=str(metadata.get("uid", "")), desired=int(spec.get("replicas") or 0),
                ready=int(status.get("readyReplicas") or 0), image=image,
                config_sha256=hashlib.sha256(_canonical(template)).hexdigest(),
            ))
        residuals = []
        for kind in ("networkchaos", "podchaos", "stresschaos"):
            obj = self._kubectl_json(("-n", EXPECTED_NAMESPACE, "get", kind, "-o", "json"))
            for item in obj.get("items") or []:
                metadata = item.get("metadata") or {}
                residuals.append(f"{kind}/{metadata.get('name', '?')}@{metadata.get('uid', '?')}")
        backends = {
            "prometheus": "ready" if self._http_ready("http://127.0.0.1:9090/-/ready") else "unknown",
            "jaeger": "ready" if self._http_ready("http://127.0.0.1:16686/api/services") else "unknown",
            "loki": "ready" if self._http_ready("http://127.0.0.1:3100/ready") else "unknown",
        }
        snapshot = PreflightSnapshot(
            repo_root=str(request.repo_root), runner_sha256=_sha_file(runner), component_sha256=components,
            context=context.stdout.strip(), namespace=EXPECTED_NAMESPACE,
            namespace_uid=str((namespace_obj.get("metadata") or {}).get("uid", "")),
            deployments=tuple(deployments), backends=backends, db_checksums=self._checksums(),
            foreign_residuals=tuple(sorted(residuals)), observed_monotonic=time.monotonic(),
        )
        return snapshot

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def begin(self, request: SmokeRequest, event_sink: Path) -> ScenarioActivity:
        if request.scenario == "no-fault":
            return ScenarioActivity(0, 0, lambda: 0)
        if not self._user_token:
            _fail("E5S003_PREFLIGHT_FAILED", "LITE_SMOKE_USER_TOKEN is required for D10/T05")
        case_id = f"engineering-smoke-{request.scenario}-{request.attempt_id}"
        runner_out = event_sink.parent / "runner-out"
        argv = [
            sys.executable,
            str(request.repo_root / "scripts" / "chaos" / "ctk" / "chaos_k8s_runner.py"),
            "--case-id", case_id,
            "--fault", str(SCENARIO_TO_RUNNER[request.scenario]),
            "--stage-seconds", str(request.stage_seconds),
            "--poll", format(request.poll_seconds, ".15g"),
            "--out-dir", str(runner_out),
            "--user-token", self._user_token,
            "--lite-smoke-event-sink", str(event_sink),
            "--lite-smoke-attempt-token", request.attempt_id,
            "--deep", "--keep-carrier",
        ]
        if request.scenario == "D10":
            argv.extend(("--catalog-direct-base", "http://127.0.0.1:5005"))
        stdout = (event_sink.parent / "runner.stdout.log").open("x", encoding="utf-8", newline="\n")
        stderr = (event_sink.parent / "runner.stderr.log").open("x", encoding="utf-8", newline="\n")
        runner_env = os.environ.copy()
        runner_env.update({
            "PROM_URL": "http://127.0.0.1:9090",
            "JAEGER_URL": "http://127.0.0.1:16686",
            "LOKI_URL": "http://127.0.0.1:3100",
            "NO_PROXY": "*",
            "no_proxy": "*",
        })
        try:
            process = subprocess.Popen(
                tuple(argv), cwd=str(request.repo_root), env=runner_env, shell=False,
                text=True, stdout=stdout, stderr=stderr,
            )
        except OSError as exc:
            stdout.close(); stderr.close()
            _fail("E5S009_RUNNER_FAILED", f"cannot start runner: {exc.__class__.__name__}")
        deadline = time.monotonic() + request.stage_seconds * 4 + 300
        while time.monotonic() < deadline:
            if event_sink.exists() and "all_active_confirmed" in event_sink.read_text(encoding="utf-8", errors="strict"):
                break
            if process.poll() is not None:
                stdout.close(); stderr.close()
                _fail("E5S009_RUNNER_FAILED", f"runner exited before active: {process.returncode}")
            time.sleep(0.2)
        else:
            process.kill(); process.wait(); stdout.close(); stderr.close()
            _fail("E5S009_RUNNER_FAILED", "runner did not confirm all active")

        def finish() -> int:
            try:
                returncode = process.wait(timeout=request.stage_seconds * 4 + 600)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
                returncode = 124
            finally:
                stdout.close(); stderr.close()
            return returncode

        return ScenarioActivity(
            1, {"D10": 2, "T05": 3}[request.scenario], finish,
            candidate_path=runner_out / case_id,
        )

    def submit_workload(self, ordinal: int) -> CompletionHandle:
        process = subprocess.Popen(
            ("curl.exe", "--noproxy", "*", "--max-time", "10", "-sS", "-o", "NUL", "-w", "%{http_code}",
             "http://127.0.0.1:5014/api/pricing/0071341196"),
            shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        return _CurlCompletion(process, time.monotonic())

    def query(self, window_start: float, window_end: float) -> tuple[QueryRecord, ...]:
        endpoints = (
            ("prometheus.up", "http://127.0.0.1:9090/api/v1/query?query=up", ("data", "result")),
            ("jaeger.traces", "http://127.0.0.1:16686/api/traces?service=pricing_service&limit=20&lookback=1h", ("data",)),
            (
                "loki.logs",
                "http://127.0.0.1:3100/loki/api/v1/query_range?query="
                + urllib.parse.quote('{k8s_namespace_name="recweb-chaos"}', safe="") + "&limit=20",
                ("data", "result"),
            ),
        )
        rows = []
        for query_id, url, count_path in endpoints:
            observed = max(time.monotonic(), window_end)
            status, value, summary = "query_error", None, "unavailable"
            try:
                with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=10) as response:
                    raw = response.read()
                    if not raw:
                        status, summary = "no_series", "empty"
                    else:
                        parsed = json.loads(raw.decode("utf-8", errors="strict"))
                        records: Any = parsed
                        for key in count_path:
                            records = records[key]
                        if type(records) is not list or not records:
                            status, summary = "no_series", "empty-result"
                        else:
                            status, value, summary = "value", float(len(records)), f"http={response.status};records={len(records)}"
            except TimeoutError:
                status, summary = "timeout", "timeout"
            except Exception as exc:
                status, summary = "query_error", exc.__class__.__name__
            rows.append(QueryRecord(query_id, url, window_start, window_end, summary, observed, True, status, value))
        return tuple(rows)

    def strict51_query(self, attempt_root: Path, window_start_us: int, window_end_us: int) -> tuple[QueryRecord, ...]:
        """A3 strict51 three-backend query over one frozen UTC window.

        Journals land attempt-local under ``<attempt_root>/query-journal/<query_id>/``
        (attempt-01/02.json + summary.json, all O_EXCL); the returned records
        close field-by-field with the three summaries."""
        urls = strict51_query_urls(window_start_us, window_end_us)
        rows = []
        for query_id in ("prometheus.up", "jaeger.traces", "loki.logs"):
            rows.append(
                run_strict51_query(
                    query_id,
                    urls[query_id],
                    window_start_us,
                    window_end_us,
                    Path(attempt_root) / "query-journal" / query_id,
                )
            )
        return tuple(rows)

    def _event_rows(self) -> tuple[Mapping[str, Any], ...]:
        path = (
            self._request.dataset_root / ".engineering-smoke" / ".attempts"
            / f"{self._request.attempt_id}.tmp" / "runner-events.jsonl"
        )
        return tuple(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())

    def resources(self, attempt_id: str) -> tuple[ResourceRecord, ...]:
        if self._request.scenario == "no-fault":
            return ()
        active = [row for row in self._event_rows() if row.get("event") == "leg_active_confirmed"]
        cleanup = {row.get("payload", {}).get("fid"): row for row in self._event_rows() if row.get("event") == "cleanup_result"}
        result = []
        for ordinal, row in enumerate(active, 1):
            payload = row.get("payload") or {}
            fid = payload.get("fid")
            closed = cleanup.get(fid, {}).get("payload") or {}
            result.append(ResourceRecord(
                ordinal, str(payload.get("resource_kind") or payload.get("kind")),
                str(payload.get("resource_name") or payload.get("target_identity")),
                str(payload.get("resource_uid") or "unknown"), payload.get("attempt_label"),
                "OWNED" if payload.get("attempt_label") == attempt_id and closed.get("success") is True else "UNKNOWN",
                residual=False,
            ))
        return tuple(result)

    def cleanup(self, action: CleanupAction) -> bool:
        if action.resource_kind == "offgraph":
            return True
        cp = self._run((self._kubectl, "-n", EXPECTED_NAMESPACE, "get", action.resource_kind, action.name, "-o", "name"), 20)
        return cp.returncode != 0

    def post_db_checksums(self) -> Mapping[str, int]:
        return self._checksums()

    def identity_sha256(self) -> str:
        return validate_preflight(
            self._request, self.preflight(self._request),
            expected_runner_sha256=self.expected_runner_sha256)

def build_production_backend(request: SmokeRequest) -> SmokeBackend:
    return LocalSmokeBackend(request)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        request = parse_authorized_request(tuple(sys.argv[1:] if argv is None else argv))
        outcome = run_one_smoke(request, build_production_backend(request))
    except SmokeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(_canonical(asdict(outcome)).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DeploymentIdentity", "FROZEN_COMPONENT_SHA256", "PreflightSnapshot",
    "SCENARIO_TO_RUNNER", "ScenarioActivity", "SmokeBackend", "SmokeError",
    "SmokeOutcome", "SmokeRequest", "build_production_backend", "parse_authorized_request",
    "run_one_smoke", "validate_preflight",
]
