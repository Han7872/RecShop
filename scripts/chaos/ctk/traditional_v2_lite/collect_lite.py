"""Dry-run-only assembly entrypoint for traditional-v2-lite E0-E4."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from scripts.chaos.ctk.traditional_v2_lite.cleanup_audit import OWNERSHIP_STATES
from scripts.chaos.ctk.traditional_v2_lite.contract import (
    DEFAULT_SEED,
    FreezeIdentity,
    build_contract_document,
    build_schedule_manifest,
    canonical_json_bytes,
)
from scripts.chaos.ctk.traditional_v2_lite.control_adapter import build_control_plans
from scripts.chaos.ctk.traditional_v2_lite.phase_journal import PHASES
from scripts.chaos.ctk.traditional_v2_lite.runner_wrapper import (
    build_attempt_plan,
    build_legacy_runner_command,
    build_production_composition,
)
from scripts.chaos.ctk.traditional_v2_lite.telemetry_journal import QUERY_STATUSES


REPORT_SCHEMA_NAME = "traditional_v2_lite_dry_run_preflight"
REPORT_SCHEMA_VERSION = "1.0.0"
OFFLINE_IDENTITY_VECTOR_ID = "e4-offline-assembly-current-files-v1"


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _offline_identity(repo_root: Path, contract: dict[str, Any]) -> FreezeIdentity:
    lite = repo_root / "scripts" / "chaos" / "ctk" / "traditional_v2_lite"
    return FreezeIdentity(
        runner_sha256=_file_sha256(repo_root / "scripts" / "chaos" / "ctk" / "chaos_k8s_runner.py"),
        lite_contract_sha256=contract["contract_sha256"],
        workload_config_sha256=_file_sha256(lite / "workload_driver.py"),
        query_registry_sha256=_file_sha256(lite / "telemetry_journal.py"),
        threshold_registry_sha256=_file_sha256(lite / "control_adapter.py"),
        image_manifest_sha256=_file_sha256(lite / "cleanup_audit.py"),
        schema_bundle_sha256=_file_sha256(lite / "evidence.py"),
        environment_contract_sha256=_file_sha256(lite / "phase_journal.py"),
    )


def build_dry_run_report(
    *,
    repo_root: Path,
    dataset_root: Path,
    python_executable: Path,
) -> dict[str, Any]:
    repo_root = repo_root.resolve(strict=True)
    python_executable = python_executable.resolve(strict=True)
    dataset_root = dataset_root.resolve(strict=False)
    try:
        dataset_root.relative_to(repo_root)
    except ValueError as exc:
        raise ValueError("dry-run dataset_root must remain under repo_root") from exc
    contract = build_contract_document()
    identity = _offline_identity(repo_root, contract)
    schedule = build_schedule_manifest(identity, seed=DEFAULT_SEED)
    first_fault = next(
        (block["block_id"], slot)
        for block in schedule["blocks"]
        for slot in block["slots"]
        if slot["slot_type"] == "fault"
    )
    block_id, slot = first_fault
    plan = build_attempt_plan(
        dataset_root=dataset_root,
        attempt_id="dry-run-no-execution",
        block_id=block_id,
        run_ordinal=slot["run_ordinal"],
        contract_document=contract,
        schedule_manifest=schedule,
    )
    command = build_legacy_runner_command(
        plan=plan,
        slot=slot,
        python_executable=python_executable,
        repo_root=repo_root,
        stage_seconds=30,
        poll_seconds=2,
    )
    composition = build_production_composition(
        plan=plan,
        repo_root=repo_root,
        scope_root=repo_root,
        python_executable=python_executable,
        timeout_seconds=120,
    )
    controls = build_control_plans(contract, schedule)
    report = {
        "schema_name": REPORT_SCHEMA_NAME,
        "schema_version": REPORT_SCHEMA_VERSION,
        "verdict": "DRY_RUN_ASSEMBLED_NO_EXECUTION",
        "dry_run": True,
        "live_authorized": False,
        "formal_claim_allowed": False,
        "identity_freeze_ready": False,
        "offline_identity_vector_id": OFFLINE_IDENTITY_VECTOR_ID,
        "contract_sha256": contract["contract_sha256"],
        "schedule_hash": schedule["schedule_hash"],
        "sample_plan": {
            "attempt_id": plan.attempt_id,
            "block_id": plan.block_id,
            "run_ordinal": plan.run_ordinal,
            "case_id": plan.case_id,
            "slot_type": plan.slot_type,
            "command_sha256": command.command_sha256,
            "argv": list(command.argv),
        },
        "production_composition": {
            "executor": type(composition.executor).__name__,
            "evidence_evaluator": type(composition.evidence_evaluator).__name__,
            "workspace": type(composition.workspace).__name__,
        },
        "scientific_sidecars": {
            "control_plan_count": len(controls),
            "query_statuses": sorted(QUERY_STATUSES),
            "phases": list(PHASES),
            "cleanup_ownership_states": sorted(OWNERSHIP_STATES),
            "workload_api": "fixed-cadence-submit-then-collect",
        },
        "side_effects": {
            "ledger_created": False,
            "attempt_directory_created": False,
            "executor_called": False,
            "verifier_called": False,
            "legacy_runner_called": False,
            "network_called": False,
        },
        "not_run": [
            "live",
            "Kubernetes",
            "network",
            "engineering_smoke",
            "B1",
            "collection",
            "packaging",
            "release",
        ],
    }
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--dataset-root", type=Path)
    parser.add_argument("--python-executable", type=Path, default=Path(sys.executable))
    args = parser.parse_args(argv)
    if args.dry_run is not True:
        parser.error("E4 permits --dry-run only; live execution is not implemented or authorized")
    repo_root = args.repo_root.resolve(strict=True)
    dataset_root = args.dataset_root or repo_root / ".traditional-v2-lite-dry-run"
    report = build_dry_run_report(
        repo_root=repo_root,
        dataset_root=dataset_root,
        python_executable=args.python_executable,
    )
    sys.stdout.buffer.write(canonical_json_bytes(report) + b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["OFFLINE_IDENTITY_VECTOR_ID", "REPORT_SCHEMA_NAME", "REPORT_SCHEMA_VERSION", "build_dry_run_report", "main"]
