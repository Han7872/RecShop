"""Offline-safe Lite-1 wrapper core for the frozen legacy runner.

The module builds an exact argv tuple, plans an attempt-local output tree and
same-volume promotion, and coordinates injected executor/evidence/workspace
interfaces.  The default implementations are intentionally disabled: merely
importing or calling the pure builders cannot start the legacy runner, contact
Kubernetes, or promote a candidate.

An exit code of zero is only an execution fact.  Promotion additionally
requires separately supplied metadata and verifier evidence bound to the same
attempt, case, and command hash.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from .contract import (
    ACTIVE_SCENARIO_IDS,
    FROZEN_SCENARIO_UNIVERSE,
    canonical_sha256,
    validate_contract_document,
    validate_schedule_manifest,
)
from .ledger import AttemptLedger, LedgerError, ZERO_HASH


LEGACY_RUNNER_RELATIVE_PATH = Path("scripts/chaos/ctk/chaos_k8s_runner.py")
PROMOTION_METHOD = "same-volume-atomic-directory-rename"
_SAFE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_BANNED_FLAGS = frozenset({"--skip-checksum", "--keep-carrier", "--force"})
_ALLOWED_EXTRA_FLAGS = frozenset({"--deep", "--full-telemetry"})
_MANAGED_FLAGS = frozenset(
    {
        "--case-id",
        "--fault",
        "--target-service",
        "--stage-seconds",
        "--poll",
        "--out-dir",
    }
)
_FAULT_SLOT_KEYS = frozenset(
    {
        "run_ordinal",
        "slot_type",
        "scenario_id",
        "fault_leg_family",
        "fault_instance_arity",
        "planned_service_arity",
        "runner_fault",
        "runner_target_service",
        "planned_track",
        "track_detail",
    }
)
_SPEC_BY_ID = {spec.scenario_id: spec for spec in FROZEN_SCENARIO_UNIVERSE}


class LiteWrapperError(RuntimeError):
    """Stable rejection raised by the Lite-1 wrapper."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> "None":
    raise LiteWrapperError(code, message)


def _safe_id(value: Any, field: str) -> str:
    if type(value) is not str or _SAFE_ID_RE.fullmatch(value) is None:
        _fail("L1W001_INVALID_ID", f"{field} is not a safe identifier")
    return value


def _absolute_path(value: os.PathLike[str] | str, field: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or "\x00" in str(path):
        _fail("L1W002_INVALID_PATH", f"{field} must be an absolute path")
    return path.resolve(strict=False)


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _volume_identity(path: Path) -> str:
    drive, _ = os.path.splitdrive(str(path))
    return (drive or path.anchor).casefold()


def _validate_sha256(value: Any, field: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None or value == ZERO_HASH:
        _fail("L1W009_EVIDENCE_INVALID", f"{field} must be a non-placeholder SHA-256")
    return value


def _validate_binding_sha256(value: Any, field: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None or value == ZERO_HASH:
        _fail("L1W006_SLOT_CONTRACT_MISMATCH", f"{field} must be a bound SHA-256")
    return value


def hash_existing_runner_file(path: os.PathLike[str] | str) -> str:
    """Hash one exact existing regular non-symlink runner file, or fail closed."""

    runner = Path(path)
    if not runner.is_absolute() or "\x00" in str(runner):
        _fail("L1W015_RUNNER_IDENTITY_INVALID", "runner path must be absolute")
    try:
        if runner.is_symlink():
            _fail("L1W015_RUNNER_IDENTITY_INVALID", "runner may not be a symlink")
        resolved = runner.resolve(strict=True)
        if resolved != runner:
            _fail(
                "L1W015_RUNNER_IDENTITY_INVALID",
                "runner path may not traverse a symlink",
            )
        if not stat.S_ISREG(runner.lstat().st_mode):
            _fail("L1W015_RUNNER_IDENTITY_INVALID", "runner must be a regular file")
        digest = hashlib.sha256()
        with runner.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except LiteWrapperError:
        raise
    except (OSError, RuntimeError) as exc:
        _fail(
            "L1W015_RUNNER_IDENTITY_INVALID",
            f"runner cannot be resolved and read: {exc.__class__.__name__}",
        )
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class PromotionPlan:
    source: Path
    destination: Path
    method: str = PROMOTION_METHOD
    source_and_destination_same_volume: bool = True
    destination_must_not_exist: bool = True

    def __post_init__(self) -> None:
        if self.method != PROMOTION_METHOD:
            _fail("L1W003_PROMOTION_PLAN_INVALID", "unknown promotion method")
        if not self.source.is_absolute() or not self.destination.is_absolute():
            _fail("L1W003_PROMOTION_PLAN_INVALID", "promotion paths must be absolute")
        actually_same = _volume_identity(self.source) == _volume_identity(self.destination)
        if self.source_and_destination_same_volume is not True or not actually_same:
            _fail("L1W003_PROMOTION_PLAN_INVALID", "promotion must stay on one volume")
        if self.destination_must_not_exist is not True:
            _fail("L1W003_PROMOTION_PLAN_INVALID", "promotion may not overwrite a case")


@dataclass(frozen=True, slots=True)
class AttemptPlan:
    dataset_root: Path
    attempt_id: str
    block_id: str
    run_ordinal: int
    case_id: str
    slot_type: str
    contract_sha256: str
    schedule_hash: str
    block_schedule_hash: str
    slot_sha256: str
    runner_sha256: str
    scenario_id: str | None
    runner_fault: str | None
    runner_target_service: str | None
    attempt_root: Path
    runner_out_dir: Path
    ledger_path: Path
    promotion: PromotionPlan

    def __post_init__(self) -> None:
        root = _absolute_path(self.dataset_root, "dataset_root")
        if root != self.dataset_root:
            _fail("L1W003_PROMOTION_PLAN_INVALID", "dataset_root must be normalized")
        _safe_id(self.attempt_id, "attempt_id")
        _safe_id(self.case_id, "case_id")
        if self.block_id not in {"B1", "B2", "B3", "B4", "B5"}:
            _fail("L1W001_INVALID_ID", "block_id must be B1-B5")
        if type(self.run_ordinal) is not int or not 1 <= self.run_ordinal <= 46:
            _fail("L1W001_INVALID_ID", "run_ordinal must be 1..46")
        if self.slot_type not in {"fault", "control"}:
            _fail("L1W001_INVALID_ID", "slot_type must be fault/control")
        for field in (
            "contract_sha256",
            "schedule_hash",
            "block_schedule_hash",
            "slot_sha256",
            "runner_sha256",
        ):
            _validate_binding_sha256(getattr(self, field), field)
        if self.slot_type == "fault":
            if self.scenario_id not in ACTIVE_SCENARIO_IDS:
                _fail("L1W006_SLOT_CONTRACT_MISMATCH", "plan scenario is not active")
            spec = _SPEC_BY_ID[self.scenario_id]
            if self.runner_fault != spec.runner_fault:
                _fail("L1W006_SLOT_CONTRACT_MISMATCH", "plan fault differs from roster")
            if self.runner_target_service != spec.runner_target_service:
                _fail("L1W006_SLOT_CONTRACT_MISMATCH", "plan target differs from roster")
            if self.case_id != (
                f"lite-{self.block_id}-{self.run_ordinal:03d}-{self.scenario_id}"
            ):
                _fail("L1W006_SLOT_CONTRACT_MISMATCH", "plan case differs from slot identity")
            if canonical_sha256(_expected_fault_slot(self, spec)) != self.slot_sha256:
                _fail("L1W006_SLOT_CONTRACT_MISMATCH", "plan slot hash differs from semantics")
        elif any(
            value is not None
            for value in (self.scenario_id, self.runner_fault, self.runner_target_service)
        ):
            _fail("L1W006_SLOT_CONTRACT_MISMATCH", "control plan contains fault semantics")
        for field, path in (
            ("attempt_root", self.attempt_root),
            ("runner_out_dir", self.runner_out_dir),
            ("ledger_path", self.ledger_path),
            ("promotion.source", self.promotion.source),
            ("promotion.destination", self.promotion.destination),
        ):
            if not path.is_absolute() or not _is_within(path, root):
                _fail("L1W003_PROMOTION_PLAN_INVALID", f"{field} escapes dataset_root")
        if self.attempt_root.suffix != ".tmp":
            _fail("L1W003_PROMOTION_PLAN_INVALID", "attempt_root must have .tmp suffix")
        if self.runner_out_dir != self.attempt_root / "runner-out":
            _fail("L1W003_PROMOTION_PLAN_INVALID", "runner_out_dir is not attempt-local")
        if self.promotion.source != self.runner_out_dir / self.case_id:
            _fail("L1W003_PROMOTION_PLAN_INVALID", "promotion source is not runner case output")
        if self.promotion.destination != self.dataset_root / "cases" / self.case_id:
            _fail("L1W003_PROMOTION_PLAN_INVALID", "promotion destination is not canonical")


def build_attempt_plan(
    *,
    dataset_root: os.PathLike[str] | str,
    attempt_id: str,
    block_id: str,
    run_ordinal: int,
    contract_document: Mapping[str, Any],
    schedule_manifest: Mapping[str, Any],
    case_id: str | None = None,
    slot_type: str | None = None,
) -> AttemptPlan:
    """Bind one exact validated Lite-0 schedule slot to an attempt plan."""

    validate_contract_document(contract_document)
    validate_schedule_manifest(schedule_manifest)
    contract_sha256 = contract_document["contract_sha256"]
    if schedule_manifest["identity"]["lite_contract_sha256"] != contract_sha256:
        _fail(
            "L1W006_SLOT_CONTRACT_MISMATCH",
            "schedule identity is bound to another Lite-0 contract",
        )
    if block_id not in {"B1", "B2", "B3", "B4", "B5"}:
        _fail("L1W001_INVALID_ID", "block_id must be B1-B5")
    if type(run_ordinal) is not int or not 1 <= run_ordinal <= 46:
        _fail("L1W001_INVALID_ID", "run_ordinal must be 1..46")
    block = next(
        item for item in schedule_manifest["blocks"] if item["block_id"] == block_id
    )
    slot = block["slots"][run_ordinal - 1]
    if slot["run_ordinal"] != run_ordinal:
        _fail("L1W006_SLOT_CONTRACT_MISMATCH", "schedule slot ordinal is not exact")
    bound_slot_type = slot["slot_type"]
    slot_identity = slot["scenario_id"] if bound_slot_type == "fault" else slot["control_id"]
    canonical_case_id = f"lite-{block_id}-{run_ordinal:03d}-{slot_identity}"
    if case_id is not None and _safe_id(case_id, "case_id") != canonical_case_id:
        _fail("L1W006_SLOT_CONTRACT_MISMATCH", "caller case_id differs from frozen slot")
    if slot_type is not None and slot_type != bound_slot_type:
        _fail("L1W006_SLOT_CONTRACT_MISMATCH", "caller slot_type differs from frozen slot")
    root = _absolute_path(dataset_root, "dataset_root")
    attempt_id = _safe_id(attempt_id, "attempt_id")
    case_id = canonical_case_id
    attempt_root = root / ".attempts" / f"{attempt_id}.tmp"
    runner_out_dir = attempt_root / "runner-out"
    destination = root / "cases" / case_id
    return AttemptPlan(
        dataset_root=root,
        attempt_id=attempt_id,
        block_id=block_id,
        run_ordinal=run_ordinal,
        case_id=case_id,
        slot_type=bound_slot_type,
        contract_sha256=contract_sha256,
        schedule_hash=schedule_manifest["schedule_hash"],
        block_schedule_hash=block["block_schedule_hash"],
        slot_sha256=canonical_sha256(dict(slot)),
        runner_sha256=schedule_manifest["identity"]["runner_sha256"],
        scenario_id=slot.get("scenario_id"),
        runner_fault=(slot.get("runner_fault") if bound_slot_type == "fault" else None),
        runner_target_service=(
            slot.get("runner_target_service") if bound_slot_type == "fault" else None
        ),
        attempt_root=attempt_root,
        runner_out_dir=runner_out_dir,
        ledger_path=root / ".ledger" / f"{attempt_id}.jsonl",
        promotion=PromotionPlan(
            source=runner_out_dir / case_id,
            destination=destination,
        ),
    )


@dataclass(frozen=True, slots=True)
class LegacyRunnerCommand:
    argv: tuple[str, ...]
    cwd: Path
    runner_path: Path
    attempt_id: str
    case_id: str
    runner_out_dir: Path
    command_sha256: str
    contract_sha256: str
    schedule_hash: str
    block_schedule_hash: str
    slot_sha256: str
    runner_sha256: str
    block_id: str
    run_ordinal: int
    scenario_id: str
    runner_fault: str
    runner_target_service: str | None

    def __post_init__(self) -> None:
        _safe_id(self.attempt_id, "command.attempt_id")
        _safe_id(self.case_id, "command.case_id")
        if not self.argv or any(type(token) is not str or not token or "\x00" in token for token in self.argv):
            _fail("L1W004_COMMAND_INVALID", "argv contains an invalid token")
        if not self.cwd.is_absolute() or not self.runner_path.is_absolute():
            _fail("L1W004_COMMAND_INVALID", "command paths must be absolute")
        if not self.runner_out_dir.is_absolute():
            _fail("L1W004_COMMAND_INVALID", "command output path must be absolute")
        if self.runner_path != self.cwd / LEGACY_RUNNER_RELATIVE_PATH:
            _fail("L1W004_COMMAND_INVALID", "runner path is not the frozen legacy runner")
        if self.command_sha256 != canonical_sha256(list(self.argv)):
            _fail("L1W004_COMMAND_INVALID", "command hash does not bind argv")
        for field in (
            "contract_sha256",
            "schedule_hash",
            "block_schedule_hash",
            "slot_sha256",
            "runner_sha256",
        ):
            _validate_binding_sha256(getattr(self, field), field)
        if self.block_id not in {"B1", "B2", "B3", "B4", "B5"}:
            _fail("L1W004_COMMAND_INVALID", "command block_id is invalid")
        if type(self.run_ordinal) is not int or not 1 <= self.run_ordinal <= 46:
            _fail("L1W004_COMMAND_INVALID", "command run_ordinal is invalid")
        if self.scenario_id not in ACTIVE_SCENARIO_IDS:
            _fail("L1W004_COMMAND_INVALID", "command scenario_id is invalid")
        if type(self.runner_fault) is not str or not self.runner_fault:
            _fail("L1W004_COMMAND_INVALID", "command runner_fault is invalid")
        if self.runner_target_service is not None and (
            type(self.runner_target_service) is not str or not self.runner_target_service
        ):
            _fail("L1W004_COMMAND_INVALID", "command runner_target_service is invalid")


def _expected_fault_slot(plan: AttemptPlan, spec: Any) -> dict[str, Any]:
    return {
        "run_ordinal": plan.run_ordinal,
        "slot_type": "fault",
        "scenario_id": spec.scenario_id,
        "fault_leg_family": spec.fault_leg_family,
        "fault_instance_arity": spec.fault_instance_arity,
        "planned_service_arity": None,
        "runner_fault": spec.runner_fault,
        "runner_target_service": spec.runner_target_service,
        "planned_track": spec.disposition,
        "track_detail": spec.disposition_reason if spec.disposition == "auxiliary" else None,
    }


def _validate_fault_slot(slot: Mapping[str, Any], plan: AttemptPlan) -> None:
    if slot.get("slot_type") == "control" or plan.slot_type == "control":
        _fail("L1W005_CONTROL_FAULT_MAPPING_FORBIDDEN", "control slots have no legacy fault command")
    if set(slot) != _FAULT_SLOT_KEYS or slot.get("slot_type") != "fault":
        _fail("L1W006_SLOT_CONTRACT_MISMATCH", "fault slot is not the closed Lite-0 schema")
    if canonical_sha256(dict(slot)) != plan.slot_sha256:
        _fail("L1W006_SLOT_CONTRACT_MISMATCH", "slot hash differs from attempt plan")
    if slot["run_ordinal"] != plan.run_ordinal:
        _fail("L1W006_SLOT_CONTRACT_MISMATCH", "slot ordinal differs from attempt plan")
    scenario_id = slot["scenario_id"]
    if scenario_id not in ACTIVE_SCENARIO_IDS:
        _fail("L1W006_SLOT_CONTRACT_MISMATCH", "scenario is not active in Lite-1")
    expected_case_id = f"lite-{plan.block_id}-{plan.run_ordinal:03d}-{scenario_id}"
    if plan.case_id != expected_case_id:
        _fail(
            "L1W006_SLOT_CONTRACT_MISMATCH",
            "case_id does not bind block, run ordinal, and scenario",
        )
    spec = _SPEC_BY_ID[scenario_id]
    expected = _expected_fault_slot(plan, spec)
    if dict(slot) != expected:
        _fail("L1W006_SLOT_CONTRACT_MISMATCH", "slot semantics drift from frozen roster")


def _validate_extra_args(extra_args: Sequence[str]) -> tuple[str, ...]:
    if isinstance(extra_args, (str, bytes)):
        _fail("L1W004_COMMAND_INVALID", "extra_args must be an argv sequence")
    normalized: list[str] = []
    for token in extra_args:
        if type(token) is not str or not token or "\x00" in token:
            _fail("L1W004_COMMAND_INVALID", "extra_args contains an invalid token")
        flag = token.split("=", 1)[0]
        if any(item.startswith(flag) or flag.startswith(item) for item in _BANNED_FLAGS):
            _fail("L1W007_UNSAFE_FLAG_FORBIDDEN", f"unsafe legacy flag forbidden: {flag}")
        if any(item.startswith(flag) or flag.startswith(item) for item in _MANAGED_FLAGS):
            _fail("L1W004_COMMAND_INVALID", f"wrapper-managed flag repeated: {flag}")
        if token not in _ALLOWED_EXTRA_FLAGS:
            _fail("L1W004_COMMAND_INVALID", f"extra legacy flag is not allowlisted: {token}")
        normalized.append(token)
    return tuple(normalized)


def build_legacy_runner_command(
    *,
    plan: AttemptPlan,
    slot: Mapping[str, Any],
    python_executable: os.PathLike[str] | str,
    repo_root: os.PathLike[str] | str,
    stage_seconds: int,
    poll_seconds: float,
    extra_args: Sequence[str] = (),
) -> LegacyRunnerCommand:
    """Build a shell-free, deterministic legacy-runner argv tuple."""

    _validate_fault_slot(slot, plan)
    python_path = _absolute_path(python_executable, "python_executable")
    root = _absolute_path(repo_root, "repo_root")
    if type(stage_seconds) is not int or stage_seconds <= 0:
        _fail("L1W004_COMMAND_INVALID", "stage_seconds must be a positive int")
    if type(poll_seconds) not in {int, float} or isinstance(poll_seconds, bool):
        _fail("L1W004_COMMAND_INVALID", "poll_seconds must be finite and positive")
    poll_value = float(poll_seconds)
    if not math.isfinite(poll_value) or poll_value <= 0:
        _fail("L1W004_COMMAND_INVALID", "poll_seconds must be finite and positive")
    extras = _validate_extra_args(extra_args)
    target = slot["runner_target_service"]
    if target not in {None, "catalog"} and "--deep" in extras:
        _fail("L1W004_COMMAND_INVALID", "legacy retarget and --deep are mutually exclusive")
    runner_path = root / LEGACY_RUNNER_RELATIVE_PATH
    runner_sha256 = hash_existing_runner_file(runner_path)
    if runner_sha256 != plan.runner_sha256:
        _fail(
            "L1W015_RUNNER_IDENTITY_INVALID",
            "runner SHA-256 differs from frozen schedule identity",
        )
    argv: list[str] = [
        str(python_path),
        str(runner_path),
        "--case-id",
        plan.case_id,
        "--fault",
        slot["runner_fault"],
    ]
    if target is not None:
        argv.extend(("--target-service", target))
    argv.extend(
        (
            "--stage-seconds",
            str(stage_seconds),
            "--poll",
            format(poll_value, ".15g"),
            "--out-dir",
            str(plan.runner_out_dir),
        )
    )
    argv.extend(extras)
    for token in argv:
        flag = token.split("=", 1)[0]
        if flag in _BANNED_FLAGS:
            _fail("L1W007_UNSAFE_FLAG_FORBIDDEN", f"unsafe legacy flag forbidden: {flag}")
    argv_tuple = tuple(argv)
    return LegacyRunnerCommand(
        argv=argv_tuple,
        cwd=root,
        runner_path=runner_path,
        attempt_id=plan.attempt_id,
        case_id=plan.case_id,
        runner_out_dir=plan.runner_out_dir,
        command_sha256=canonical_sha256(list(argv_tuple)),
        contract_sha256=plan.contract_sha256,
        schedule_hash=plan.schedule_hash,
        block_schedule_hash=plan.block_schedule_hash,
        slot_sha256=plan.slot_sha256,
        runner_sha256=runner_sha256,
        block_id=plan.block_id,
        run_ordinal=plan.run_ordinal,
        scenario_id=slot["scenario_id"],
        runner_fault=slot["runner_fault"],
        runner_target_service=slot["runner_target_service"],
    )


def _validate_command_against_plan(
    command: LegacyRunnerCommand,
    plan: AttemptPlan,
) -> None:
    """Rebuild the closed managed argv grammar and bind it to one exact plan."""

    if not isinstance(command, LegacyRunnerCommand):
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "command has the wrong type")
    if plan.slot_type != "fault" or plan.scenario_id not in ACTIVE_SCENARIO_IDS:
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "legacy command requires an active fault plan")
    exact_fields = (
        "attempt_id",
        "case_id",
        "runner_out_dir",
        "contract_sha256",
        "schedule_hash",
        "block_schedule_hash",
        "slot_sha256",
        "runner_sha256",
        "block_id",
        "run_ordinal",
        "scenario_id",
        "runner_fault",
        "runner_target_service",
    )
    for field in exact_fields:
        if getattr(command, field) != getattr(plan, field):
            _fail("L1W016_COMMAND_PLAN_MISMATCH", f"command {field} differs from plan")
    if command.runner_path != command.cwd / LEGACY_RUNNER_RELATIVE_PATH:
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "command runner path is not canonical")
    if command.command_sha256 != canonical_sha256(list(command.argv)):
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "command hash differs from argv")

    tokens = command.argv
    cursor = 0

    def take(expected: str, field: str) -> None:
        nonlocal cursor
        if cursor >= len(tokens) or tokens[cursor] != expected:
            _fail("L1W016_COMMAND_PLAN_MISMATCH", f"managed argv {field} mismatch")
        cursor += 1

    if len(tokens) < 12:
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "managed argv is incomplete")
    python_path = Path(tokens[0])
    if not python_path.is_absolute() or python_path.resolve(strict=False) != python_path:
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "python executable path is not normalized")
    cursor = 1
    take(str(command.runner_path), "runner_path")
    take("--case-id", "case_id flag")
    take(plan.case_id, "case_id")
    take("--fault", "fault flag")
    take(plan.runner_fault, "fault")
    if plan.runner_target_service is not None:
        take("--target-service", "target flag")
        take(plan.runner_target_service, "target")
    take("--stage-seconds", "stage flag")
    if cursor >= len(tokens):
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "stage value is missing")
    try:
        stage_seconds = int(tokens[cursor])
    except ValueError:
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "stage value is not an integer")
    if stage_seconds <= 0 or str(stage_seconds) != tokens[cursor]:
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "stage value is not canonical and positive")
    cursor += 1
    take("--poll", "poll flag")
    if cursor >= len(tokens):
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "poll value is missing")
    try:
        poll_seconds = float(tokens[cursor])
    except ValueError:
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "poll value is not numeric")
    if (
        not math.isfinite(poll_seconds)
        or poll_seconds <= 0
        or format(poll_seconds, ".15g") != tokens[cursor]
    ):
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "poll value is not canonical and positive")
    cursor += 1
    take("--out-dir", "out-dir flag")
    take(str(plan.runner_out_dir), "out-dir")
    extras = _validate_extra_args(tokens[cursor:])
    if plan.runner_target_service not in {None, "catalog"} and "--deep" in extras:
        _fail("L1W016_COMMAND_PLAN_MISMATCH", "target and --deep are incompatible")

    actual_runner_sha256 = hash_existing_runner_file(command.runner_path)
    if actual_runner_sha256 != plan.runner_sha256:
        _fail(
            "L1W015_RUNNER_IDENTITY_INVALID",
            "runner SHA-256 drifted after command construction",
        )


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    returncode: int

    def __post_init__(self) -> None:
        if type(self.returncode) is not int:
            _fail("L1W008_EXECUTOR_RESULT_INVALID", "returncode must be an int")


@dataclass(frozen=True, slots=True)
class EvidenceBundle:
    attempt_id: str
    case_id: str
    command_sha256: str
    contract_sha256: str
    schedule_hash: str
    block_schedule_hash: str
    slot_sha256: str
    metadata_path: Path
    metadata_sha256: str
    metadata_passed: bool
    verifier_report_path: Path
    verifier_report_sha256: str
    verifier_passed: bool


@runtime_checkable
class Executor(Protocol):
    name: str

    def execute(self, command: LegacyRunnerCommand) -> ExecutionResult:
        """Run one foreground command and return only after it exits."""


@runtime_checkable
class EvidenceEvaluator(Protocol):
    def evaluate(
        self,
        *,
        plan: AttemptPlan,
        command: LegacyRunnerCommand,
        result: ExecutionResult,
    ) -> EvidenceBundle:
        """Return independently parsed metadata and verifier evidence."""


@runtime_checkable
class AttemptWorkspace(Protocol):
    def validate(self, plan: AttemptPlan) -> None:
        """Validate scope and path ownership without creating or changing anything."""

    def prepare(self, plan: AttemptPlan) -> None:
        """Create a new temp attempt tree without reusing an old attempt."""

    def validate_promotion(
        self,
        promotion: PromotionPlan,
        *,
        manifest_path: Path,
        manifest_sha256: str,
    ) -> None:
        """Revalidate manifest and rename preconditions without renaming."""

    def promote(
        self,
        promotion: PromotionPlan,
        *,
        manifest_path: Path,
        manifest_sha256: str,
    ) -> None:
        """Atomically rename the verified source; never replace destination."""


@dataclass(frozen=True, slots=True)
class ProductionComposition:
    """The sole production component bundle; replacement hooks are absent."""

    executor: Executor
    evidence_evaluator: EvidenceEvaluator
    workspace: AttemptWorkspace


def build_production_composition(
    *,
    plan: AttemptPlan,
    repo_root: Path,
    scope_root: Path,
    python_executable: Path,
    timeout_seconds: float,
    environment_overlay: Mapping[str, str] | None = None,
) -> ProductionComposition:
    """Build the fixed Windows-local production stack for one attempt.

    This function intentionally has no executor/evaluator/workspace parameters.
    The protocol-based injection points on :func:`execute_attempt` remain test
    seams and are not a live composition API.
    """

    from .evidence import StrictEvidenceEvaluator
    from .local_adapter import ForegroundExecutor, LocalAttemptWorkspace
    from .verifier_adapter import FROZEN_VERIFY_DUAL_SHA256, VerifierAdapter

    if not isinstance(repo_root, Path) or not repo_root.is_absolute():
        _fail("L1W017_PRODUCTION_CONFIG_INVALID", "repo_root must be an absolute Path")
    if not isinstance(scope_root, Path) or not scope_root.is_absolute():
        _fail("L1W017_PRODUCTION_CONFIG_INVALID", "scope_root must be an absolute Path")
    if not isinstance(python_executable, Path) or not python_executable.is_absolute():
        _fail("L1W017_PRODUCTION_CONFIG_INVALID", "python_executable must be an absolute Path")
    repo_root = repo_root.resolve(strict=True)
    scope_root = scope_root.resolve(strict=True)
    python_executable = python_executable.resolve(strict=True)
    verifier_path = (repo_root / "scripts" / "chaos" / "ctk" / "verify_dual.py").resolve(strict=True)
    provenance_writer = VerifierAdapter(
        python_executable=python_executable,
        verifier_path=verifier_path,
        verifier_sha256=FROZEN_VERIFY_DUAL_SHA256,
        cwd=repo_root,
        timeout_seconds=timeout_seconds,
    )
    return ProductionComposition(
        executor=ForegroundExecutor(
            attempt_root=plan.attempt_root,
            timeout_seconds=timeout_seconds,
            environment_overlay=environment_overlay,
        ),
        evidence_evaluator=StrictEvidenceEvaluator(provenance_writer),
        workspace=LocalAttemptWorkspace(scope_root=scope_root),
    )


class DisabledExecutor:
    """Safe default; a production foreground executor is a later Lite gate."""

    name = "disabled-no-live-executor"

    def execute(self, command: LegacyRunnerCommand) -> ExecutionResult:
        _fail("L1W010_EXECUTOR_NOT_CONFIGURED", "no live executor is installed")


class DisabledEvidenceEvaluator:
    def evaluate(
        self,
        *,
        plan: AttemptPlan,
        command: LegacyRunnerCommand,
        result: ExecutionResult,
    ) -> EvidenceBundle:
        _fail("L1W011_EVIDENCE_EVALUATOR_NOT_CONFIGURED", "no evidence evaluator is installed")


class DisabledAttemptWorkspace:
    def validate(self, plan: AttemptPlan) -> None:
        _fail("L1W012_WORKSPACE_NOT_CONFIGURED", "no attempt workspace is installed")

    def prepare(self, plan: AttemptPlan) -> None:
        _fail("L1W012_WORKSPACE_NOT_CONFIGURED", "no attempt workspace is installed")

    def validate_promotion(
        self,
        promotion: PromotionPlan,
        *,
        manifest_path: Path,
        manifest_sha256: str,
    ) -> None:
        _fail("L1W012_WORKSPACE_NOT_CONFIGURED", "no attempt workspace is installed")

    def promote(
        self,
        promotion: PromotionPlan,
        *,
        manifest_path: Path,
        manifest_sha256: str,
    ) -> None:
        _fail("L1W012_WORKSPACE_NOT_CONFIGURED", "no attempt workspace is installed")


def _verify_evidence_file(
    path: Path,
    *,
    source: Path,
    expected_sha256: str,
    field: str,
) -> None:
    """Require one immutable-looking regular file inside the candidate and hash it."""

    if not path.is_absolute():
        _fail("L1W009_EVIDENCE_INVALID", f"{field} must be an absolute path")
    try:
        relative = path.relative_to(source)
    except ValueError:
        _fail("L1W009_EVIDENCE_INVALID", f"{field} is outside the candidate case")
    if not relative.parts or ".." in relative.parts:
        _fail("L1W009_EVIDENCE_INVALID", f"{field} is not a candidate-local file")
    try:
        if source.is_symlink():
            _fail("L1W009_EVIDENCE_INVALID", "candidate case may not be a symlink")
        source_resolved = source.resolve(strict=True)
        if not source_resolved.is_dir():
            _fail("L1W009_EVIDENCE_INVALID", "candidate case is not a directory")

        cursor = path
        while cursor != source:
            if cursor.is_symlink():
                _fail("L1W009_EVIDENCE_INVALID", f"{field} may not traverse a symlink")
            cursor = cursor.parent

        resolved = path.resolve(strict=True)
        if not _is_within(resolved, source_resolved):
            _fail("L1W009_EVIDENCE_INVALID", f"{field} resolves outside the candidate case")
        if not stat.S_ISREG(path.lstat().st_mode):
            _fail("L1W009_EVIDENCE_INVALID", f"{field} must be a regular file")
        digest = hashlib.sha256()
        with resolved.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except (OSError, RuntimeError) as exc:
        _fail(
            "L1W009_EVIDENCE_INVALID",
            f"{field} cannot be resolved and read: {exc.__class__.__name__}",
        )
    if digest.hexdigest() != expected_sha256:
        _fail("L1W009_EVIDENCE_INVALID", f"{field} SHA-256 mismatch")


def _validate_evidence(
    evidence: EvidenceBundle,
    *,
    plan: AttemptPlan,
    command: LegacyRunnerCommand,
) -> None:
    if not isinstance(evidence, EvidenceBundle):
        _fail("L1W009_EVIDENCE_INVALID", "evaluator returned the wrong type")
    if evidence.attempt_id != plan.attempt_id or evidence.case_id != plan.case_id:
        _fail("L1W009_EVIDENCE_INVALID", "evidence is bound to another attempt/case")
    if evidence.command_sha256 != command.command_sha256:
        _fail("L1W009_EVIDENCE_INVALID", "evidence command hash mismatch")
    for field in (
        "contract_sha256",
        "schedule_hash",
        "block_schedule_hash",
        "slot_sha256",
    ):
        if getattr(evidence, field) != getattr(plan, field):
            _fail("L1W009_EVIDENCE_INVALID", f"evidence {field} mismatch")
    _validate_sha256(evidence.metadata_sha256, "metadata_sha256")
    _validate_sha256(evidence.verifier_report_sha256, "verifier_report_sha256")
    _verify_evidence_file(
        evidence.metadata_path,
        source=plan.promotion.source,
        expected_sha256=evidence.metadata_sha256,
        field="metadata_path",
    )
    _verify_evidence_file(
        evidence.verifier_report_path,
        source=plan.promotion.source,
        expected_sha256=evidence.verifier_report_sha256,
        field="verifier_report_path",
    )
    if evidence.metadata_passed is not True or evidence.verifier_passed is not True:
        _fail(
            "L1W009_EVIDENCE_INVALID",
            "runner exit zero is insufficient; metadata and verifier must both pass",
        )


@dataclass(frozen=True, slots=True)
class AttemptOutcome:
    attempt_id: str
    case_id: str
    ledger_path: Path
    final_dir: Path
    command_sha256: str
    metadata_sha256: str
    verifier_report_sha256: str
    artifact_manifest_sha256: str


def execute_attempt(
    *,
    plan: AttemptPlan,
    command: LegacyRunnerCommand,
    executor: Executor | None = None,
    evidence_evaluator: EvidenceEvaluator | None = None,
    workspace: AttemptWorkspace | None = None,
) -> AttemptOutcome:
    """Run the fail-closed attempt state machine using injected side effects."""

    _validate_command_against_plan(command, plan)
    chosen_executor = executor or DisabledExecutor()
    chosen_evaluator = evidence_evaluator or DisabledEvidenceEvaluator()
    chosen_workspace = workspace or DisabledAttemptWorkspace()
    executor_name = getattr(chosen_executor, "name", None)
    if type(executor_name) is not str or not executor_name or len(executor_name) > 128:
        _fail("L1W008_EXECUTOR_RESULT_INVALID", "executor must expose a bounded name")

    # This is deliberately before O_EXCL ledger creation: rejecting a plan's
    # scope must be a pure, zero-path side effect operation.
    chosen_workspace.validate(plan)

    ledger = AttemptLedger.create(
        plan.ledger_path,
        attempt_id=plan.attempt_id,
        block_id=plan.block_id,
        run_ordinal=plan.run_ordinal,
        slot_type=plan.slot_type,
        case_id=plan.case_id,
        contract_sha256=plan.contract_sha256,
        schedule_hash=plan.schedule_hash,
        block_schedule_hash=plan.block_schedule_hash,
        slot_sha256=plan.slot_sha256,
    )
    try:
        try:
            chosen_workspace.prepare(plan)
            ledger.append(
                "prepared",
                {
                    "attempt_root": str(plan.attempt_root),
                    "runner_out_dir": str(plan.runner_out_dir),
                    "promotion_source": str(plan.promotion.source),
                    "promotion_destination": str(plan.promotion.destination),
                    "command_sha256": command.command_sha256,
                },
            )
            ledger.append("running", {"executor_name": executor_name})
            result = chosen_executor.execute(command)
            if not isinstance(result, ExecutionResult):
                _fail("L1W008_EXECUTOR_RESULT_INVALID", "executor returned the wrong type")
            ledger.append("runner_exited", {"returncode": result.returncode})
            if result.returncode != 0:
                _fail("L1W013_RUNNER_NONZERO", f"legacy runner exited {result.returncode}")
            evidence = chosen_evaluator.evaluate(
                plan=plan,
                command=command,
                result=result,
            )
            _validate_evidence(evidence, plan=plan, command=command)
            from .artifact_manifest import ARTIFACT_MANIFEST_FILENAME, write_manifest

            artifact_manifest_path = plan.attempt_root / ARTIFACT_MANIFEST_FILENAME
            artifact_manifest_sha256 = write_manifest(
                plan.promotion.source,
                artifact_manifest_path,
            )
            ledger.append(
                "evidence_accepted",
                {
                    "metadata_sha256": evidence.metadata_sha256,
                    "verifier_report_sha256": evidence.verifier_report_sha256,
                    "metadata_passed": True,
                    "verifier_passed": True,
                    "artifact_manifest_sha256": artifact_manifest_sha256,
                },
            )
            chosen_workspace.validate_promotion(
                plan.promotion,
                manifest_path=artifact_manifest_path,
                manifest_sha256=artifact_manifest_sha256,
            )
            ledger.append(
                "promotion_started",
                {
                    "promotion_source": str(plan.promotion.source),
                    "promotion_destination": str(plan.promotion.destination),
                    "promotion_method": PROMOTION_METHOD,
                    "artifact_manifest_sha256": artifact_manifest_sha256,
                },
            )
            try:
                chosen_workspace.promote(
                    plan.promotion,
                    manifest_path=artifact_manifest_path,
                    manifest_sha256=artifact_manifest_sha256,
                )
                ledger.append(
                    "promoted",
                    {
                        "promotion_destination": str(plan.promotion.destination),
                        "promotion_method": PROMOTION_METHOD,
                        "artifact_manifest_sha256": artifact_manifest_sha256,
                    },
                )
            except Exception as exc:
                code = getattr(exc, "code", "L1W014_PROMOTION_UNCERTAIN")
                if type(code) is not str or _SAFE_ID_RE.fullmatch(code) is None:
                    code = "L1W014_PROMOTION_UNCERTAIN"
                message = (str(exc) or exc.__class__.__name__).replace("\x00", "?")[:4096]
                ledger.append(
                    "promotion_uncertain",
                    {
                        "failure_code": code,
                        "failure_message": message,
                        "promotion_destination": str(plan.promotion.destination),
                        "artifact_manifest_sha256": artifact_manifest_sha256,
                    },
                )
                if isinstance(exc, (LiteWrapperError, LedgerError)):
                    raise
                raise LiteWrapperError(
                    "L1W014_PROMOTION_UNCERTAIN",
                    f"{exc.__class__.__name__}: {exc}",
                ) from exc
            return AttemptOutcome(
                attempt_id=plan.attempt_id,
                case_id=plan.case_id,
                ledger_path=plan.ledger_path,
                final_dir=plan.promotion.destination,
                command_sha256=command.command_sha256,
                metadata_sha256=evidence.metadata_sha256,
                verifier_report_sha256=evidence.verifier_report_sha256,
                artifact_manifest_sha256=artifact_manifest_sha256,
            )
        except Exception as exc:
            # Once promotion_started is durable, the directory rename may have
            # succeeded.  A later ledger/fsync failure must retain that original
            # error and must not be masked by an invalid transition to failed.
            if not ledger.is_terminal and not ledger.promotion_outcome_uncertain:
                code = getattr(exc, "code", "L1W099_INTERNAL_FAILURE")
                if type(code) is not str or _SAFE_ID_RE.fullmatch(code) is None:
                    code = "L1W099_INTERNAL_FAILURE"
                message = str(exc) or exc.__class__.__name__
                message = message.replace("\x00", "?")[:4096]
                ledger.append(
                    "failed",
                    {
                        "failure_code": code,
                        "failure_message": message,
                        "failed_from": ledger.state,
                    },
                )
            if isinstance(exc, (LiteWrapperError, LedgerError)):
                raise
            raise LiteWrapperError(
                "L1W099_INTERNAL_FAILURE", f"{exc.__class__.__name__}: {exc}"
            ) from exc
    finally:
        ledger.close()


__all__ = [
    "AttemptOutcome",
    "AttemptPlan",
    "AttemptWorkspace",
    "DisabledAttemptWorkspace",
    "DisabledEvidenceEvaluator",
    "DisabledExecutor",
    "EvidenceBundle",
    "EvidenceEvaluator",
    "ExecutionResult",
    "Executor",
    "LEGACY_RUNNER_RELATIVE_PATH",
    "LegacyRunnerCommand",
    "LiteWrapperError",
    "PROMOTION_METHOD",
    "PromotionPlan",
    "ProductionComposition",
    "build_attempt_plan",
    "build_legacy_runner_command",
    "build_production_composition",
    "execute_attempt",
]
