"""Unique offline acceptance entry point for traditional-v2-lite Lite-0.

Exit 0 means only that the frozen roster and deterministic block plan contract
passed offline checks.  It never authorizes Kubernetes access, collection, or
a formal-dataset claim.
"""

from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping


REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.chaos.ctk.traditional_v2_lite.contract import (  # noqa: E402
    CONTRACT_RELATIVE_PATH,
    CONTROL_POSITIONS,
    FROZEN_BLOCK_IDS,
    FreezeIdentity,
    LiteContractError,
    build_contract_document,
    build_schedule_manifest,
    canonical_json_bytes,
    canonical_sha256,
    extract_runner_fault_choices,
    load_contract,
    validate_contract_document,
    validate_schedule_manifest,
    verify_frozen_sources,
)


REPORT_SCHEMA = "traditional_v2_lite_lite0_acceptance_report"
REPORT_VERSION = "1.0.0"
OFFLINE_TEST_VECTOR_ID = "lite0-offline-identity-vector-v1"

_REQUIRED_ARTIFACTS = (
    "scripts/chaos/ctk/traditional_v2_lite/__init__.py",
    "scripts/chaos/ctk/traditional_v2_lite/contract.py",
    "scripts/chaos/ctk/traditional_v2_lite/verify_lite0.py",
    CONTRACT_RELATIVE_PATH,
    "tests/unit/test_traditional_v2_lite_plan.py",
    "tests/qa/test_traditional_v2_lite_verifier.py",
    "docs/acceptance/traditional-v2-lite-20260813.md",
)

_FORBIDDEN_IMPORT_ROOTS = frozenset(
    {
        "kubernetes",
        "requests",
        "socket",
        "subprocess",
        "urllib",
        "httpx",
    }
)
_FORBIDDEN_CALL_ROOTS = frozenset(
    {
        "kubectl",
        "requests",
        "socket",
        "subprocess",
        "urllib",
        "httpx",
    }
)
_FORBIDDEN_DYNAMIC_CALLS = frozenset(
    {
        "__import__",
        "builtins.__import__",
        "compile",
        "eval",
        "exec",
        "getattr",
        "importlib.import_module",
        "setattr",
    }
)
_FORBIDDEN_WRITE_METHODS = frozenset(
    {
        "hardlink_to",
        "mkdir",
        "rename",
        "replace",
        "rmdir",
        "symlink_to",
        "touch",
        "unlink",
        "write_bytes",
        "write_text",
    }
)


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _read_repo_file(relative_path: str) -> bytes:
    candidate = (REPO_ROOT / relative_path).resolve()
    try:
        candidate.relative_to(REPO_ROOT)
    except ValueError as exc:
        raise RuntimeError(f"artifact escaped repository: {relative_path}") from exc
    return candidate.read_bytes()


def _artifact_manifest() -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    for relative_path in sorted(_REQUIRED_ARTIFACTS):
        raw = _read_repo_file(relative_path)
        rows.append(
            {
                "path": relative_path,
                "byte_count": len(raw),
                "sha256": _sha256_bytes(raw),
            }
        )
    return tuple(rows)


def _dotted_name(node: ast.AST) -> str | None:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return ".".join(reversed(parts))
    return None


def _import_aliases(tree: ast.AST) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local = alias.asname or alias.name.split(".", 1)[0]
                aliases[local] = alias.name if alias.asname else local
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                if alias.name == "*":
                    continue
                local = alias.asname or alias.name
                aliases[local] = f"{node.module}.{alias.name}"
    return aliases


def _resolve_alias(name: str, aliases: Mapping[str, str]) -> str:
    root, separator, suffix = name.partition(".")
    resolved = aliases.get(root, root)
    return f"{resolved}.{suffix}" if separator else resolved


def _open_requests_write(node: ast.Call, resolved_name: str) -> bool:
    if resolved_name not in {"open", "builtins.open", "io.open"}:
        return False
    mode: Any = "r"
    if len(node.args) >= 2:
        try:
            mode = ast.literal_eval(node.args[1])
        except (TypeError, ValueError):
            return True
    for keyword in node.keywords:
        if keyword.arg == "mode":
            try:
                mode = ast.literal_eval(keyword.value)
            except (TypeError, ValueError):
                return True
    return type(mode) is not str or any(flag in mode for flag in "wax+")


def scan_python_source(source: str, *, label: str) -> tuple[str, ...]:
    """Return deterministic offline-boundary violations for Python source."""

    try:
        tree = ast.parse(source, filename=label)
    except SyntaxError as exc:
        return (f"syntax:{exc.lineno}",)
    violations: set[str] = set()
    aliases = _import_aliases(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".", 1)[0]
                if root in _FORBIDDEN_IMPORT_ROOTS:
                    violations.add(f"import:{alias.name}")
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            root = module.split(".", 1)[0]
            if root in _FORBIDDEN_IMPORT_ROOTS:
                violations.add(f"import:{module}")
        elif isinstance(node, ast.Call):
            name = _dotted_name(node.func)
            if (
                name is None
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in _FORBIDDEN_WRITE_METHODS
            ):
                violations.add(f"call:*.{node.func.attr}")
                continue
            if name is None:
                continue
            resolved_name = _resolve_alias(name, aliases)
            root = resolved_name.split(".", 1)[0]
            method = resolved_name.rsplit(".", 1)[-1]
            if (
                root in _FORBIDDEN_CALL_ROOTS
                or resolved_name in _FORBIDDEN_DYNAMIC_CALLS
                or resolved_name in {"os.system", "os.popen", "os.open", "os.write"}
                or method in _FORBIDDEN_WRITE_METHODS
                or _open_requests_write(node, resolved_name)
            ):
                violations.add(f"call:{resolved_name}")
    return tuple(sorted(violations))


def _scan_offline_production() -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    for relative_path in (
        "scripts/chaos/ctk/traditional_v2_lite/__init__.py",
        "scripts/chaos/ctk/traditional_v2_lite/contract.py",
        "scripts/chaos/ctk/traditional_v2_lite/verify_lite0.py",
    ):
        source = _read_repo_file(relative_path).decode("utf-8", errors="strict")
        rows.append(
            {
                "path": relative_path,
                "violations": list(scan_python_source(source, label=relative_path)),
            }
        )
    return tuple(rows)


def _offline_test_identity() -> FreezeIdentity:
    fields = tuple(FreezeIdentity.__dataclass_fields__)
    values = {
        field: hashlib.sha256(
            f"{OFFLINE_TEST_VECTOR_ID}\x00{field}".encode("utf-8")
        ).hexdigest()
        for field in fields
    }
    return FreezeIdentity(**values)


def _expect_rejection(
    label: str,
    expected_code: str,
    operation: Any,
) -> dict[str, Any]:
    try:
        operation()
    except LiteContractError as exc:
        return {
            "label": label,
            "pass": exc.code == expected_code,
            "observed_code": exc.code,
            "expected_code": expected_code,
        }
    except Exception as exc:  # pragma: no cover - reported, never accepted
        return {
            "label": label,
            "pass": False,
            "observed_code": exc.__class__.__name__,
            "expected_code": expected_code,
        }
    return {
        "label": label,
        "pass": False,
        "observed_code": "ACCEPTED",
        "expected_code": expected_code,
    }


def _contract_mutation_traps() -> tuple[dict[str, Any], ...]:
    base = build_contract_document()
    traps: list[dict[str, Any]] = []

    def check(mutator: Any, code: str, label: str) -> None:
        candidate = copy.deepcopy(base)
        mutator(candidate)
        core = copy.deepcopy(candidate)
        core.pop("contract_sha256", None)
        candidate["contract_sha256"] = canonical_sha256(core)
        traps.append(
            _expect_rejection(
                label,
                code,
                lambda: validate_contract_document(candidate),
            )
        )

    check(
        lambda value: value["scenarios"].pop(),
        "L0C002_DUPLICATE_OR_MISSING_ID",
        "missing-scenario",
    )
    check(
        lambda value: value["scenarios"].__setitem__(1, copy.deepcopy(value["scenarios"][0])),
        "L0C002_DUPLICATE_OR_MISSING_ID",
        "duplicate-scenario",
    )
    check(
        lambda value: value["scenarios"][0].__setitem__("planned_service_arity", 1),
        "L0C007_DISPOSITION_POLICY_VIOLATION",
        "planned-service-g",
    )

    def mutate_d06(value: dict[str, Any]) -> None:
        row = next(item for item in value["scenarios"] if item["scenario_id"] == "D06")
        row["disposition"] = "strict"
        row["disposition_reason"] = "lite-v1-in-scope"

    check(mutate_d06, "L0C007_DISPOSITION_POLICY_VIOLATION", "d06-strict")

    def mutate_deferred(value: dict[str, Any]) -> None:
        row = next(item for item in value["scenarios"] if item["scenario_id"] == "S07")
        row["disposition"] = "strict"
        row["disposition_reason"] = "lite-v1-in-scope"

    check(
        mutate_deferred,
        "L0C007_DISPOSITION_POLICY_VIOLATION",
        "deferred-promoted-to-active",
    )
    check(
        lambda value: value["control_contract"].__setitem__("fault_mapping_allowed", True),
        "L0C010_CONTROL_FAULT_MAPPING_FORBIDDEN",
        "control-as-fault",
    )
    check(
        lambda value: value["source_bindings"][0].__setitem__("sha256", "1" * 64),
        "L0C008_SOURCE_HASH_DRIFT",
        "source-hash-drift",
    )
    check(
        lambda value: value["source_bindings"][0].__setitem__("path", "../escape.md"),
        "L0C009_SECRET_OR_ABSOLUTE_PATH_IN_CONTRACT",
        "source-path-escape",
    )
    check(
        lambda value: value.__setitem__("live_authorized", True),
        "L0C010_CONTROL_FAULT_MAPPING_FORBIDDEN",
        "live-elevation",
    )
    return tuple(traps)


def _schedule_mutation_traps() -> tuple[dict[str, Any], ...]:
    base = build_schedule_manifest(_offline_test_identity())
    candidate = copy.deepcopy(base)
    first = candidate["blocks"][0]["slots"][0]
    second = candidate["blocks"][0]["slots"][1]
    candidate["blocks"][0]["slots"][0] = second
    candidate["blocks"][0]["slots"][1] = first
    trap = _expect_rejection(
        "slot-order-drift",
        "L0C003_UNIVERSE_OR_COUNT_MISMATCH",
        lambda: validate_schedule_manifest(candidate),
    )
    zero = _expect_rejection(
        "placeholder-freeze-identity",
        "L0C008_SOURCE_HASH_DRIFT",
        lambda: FreezeIdentity(**{field: "0" * 64 for field in FreezeIdentity.__dataclass_fields__}),
    )
    seed = copy.deepcopy(base)
    seed["seed"] = "mutated-seed"
    seed_trap = _expect_rejection(
        "seed-with-stale-order-and-hash",
        "L0C003_UNIVERSE_OR_COUNT_MISMATCH",
        lambda: validate_schedule_manifest(seed),
    )
    digest = copy.deepcopy(base)
    digest["schedule_hash"] = "1" * 64
    digest_trap = _expect_rejection(
        "schedule-hash-drift",
        "L0C008_SOURCE_HASH_DRIFT",
        lambda: validate_schedule_manifest(digest),
    )
    return trap, seed_trap, digest_trap, zero


def _scanner_mutation_traps() -> tuple[dict[str, Any], ...]:
    fixtures = {
        "bare-dynamic-import": "__import__('subprocess')\n",
        "forbidden-import": "import subprocess\n",
        "forbidden-from": "from kubernetes import client\n",
        "forbidden-call": "import os\nos.system('kubectl get pods')\n",
        "dynamic-import": "import importlib\nimportlib.import_module('socket')\n",
        "aliased-forbidden-call": "import os as x\nx.system('kubectl get pods')\n",
        "aliased-dynamic-import": (
            "from importlib import import_module as f\nf('socket')\n"
        ),
        "file-write": "open('datasets/x', 'w').write('bad')\n",
        "pathlib-write": (
            "from pathlib import Path\nPath('datasets/x').write_text('bad')\n"
        ),
    }
    return tuple(
        {
            "label": label,
            "pass": bool(scan_python_source(source, label=label)),
            "violations": list(scan_python_source(source, label=label)),
        }
        for label, source in sorted(fixtures.items())
    )


def run_selftest() -> dict[str, Any]:
    contract_traps = _contract_mutation_traps()
    schedule_traps = _schedule_mutation_traps()
    scanner_traps = _scanner_mutation_traps()
    rows = (*contract_traps, *schedule_traps, *scanner_traps)
    return {
        "schema_name": "traditional_v2_lite_lite0_selftest",
        "schema_version": "1.0.0",
        "checks": list(rows),
        "check_count": len(rows),
        "all_pass": all(row["pass"] is True for row in rows),
        "live_authorized": False,
        "formal_claim_allowed": False,
    }


def run_candidate() -> dict[str, Any]:
    gates: list[dict[str, Any]] = []

    def gate(gate_id: str, passed: bool, detail: Mapping[str, Any]) -> None:
        gates.append({"gate_id": gate_id, "pass": passed is True, "detail": dict(detail)})

    contract_path = REPO_ROOT / CONTRACT_RELATIVE_PATH
    loaded = load_contract(contract_path)
    generated = build_contract_document()
    artifacts = _artifact_manifest()
    gate(
        "L0.1-contract-schema-and-bytes",
        loaded == generated,
        {
            "contract_sha256": loaded["contract_sha256"],
            "artifact_sha256": _sha256_bytes(contract_path.read_bytes()),
            "artifact_census": list(artifacts),
        },
    )
    counts = loaded["expected_counts"]
    gate(
        "L0.2-exact-universe",
        counts == {
            "universe": 51,
            "strict": 39,
            "auxiliary": 1,
            "deferred": 11,
            "active_per_block": 40,
            "controls_per_block": 6,
            "slots_per_block": 46,
        },
        {"counts": counts},
    )
    d06 = next(row for row in loaded["scenarios"] if row["scenario_id"] == "D06")
    planned_g_values = sorted(
        {repr(row["planned_service_arity"]) for row in loaded["scenarios"]}
    )
    gate(
        "L0.3-disposition-and-service-g",
        d06["disposition"] == "auxiliary"
        and d06["disposition_reason"] == "config-state-only"
        and planned_g_values == ["None"],
        {"d06": d06, "planned_service_arity_values": planned_g_values},
    )
    runner_choices = extract_runner_fault_choices(
        REPO_ROOT / "scripts/chaos/ctk/chaos_k8s_runner.py"
    )
    active_aliases = {
        row["runner_fault"]
        for row in loaded["scenarios"]
        if row["disposition"] != "deferred"
    }
    gate(
        "L0.4-runner-alias-closure",
        active_aliases <= runner_choices,
        {
            "active_alias_count": len(active_aliases),
            "unknown_aliases": sorted(active_aliases - runner_choices),
        },
    )
    source_rows = verify_frozen_sources(REPO_ROOT)
    gate("L0.5-source-freeze", len(source_rows) == 5, {"sources": list(source_rows)})

    schedule = build_schedule_manifest(_offline_test_identity())
    validate_schedule_manifest(schedule)
    block_summary = [
        {
            "block_id": block["block_id"],
            "slot_count": block["slot_count"],
            "strict_slot_count": block["strict_slot_count"],
            "auxiliary_slot_count": block["auxiliary_slot_count"],
            "control_slot_count": block["control_slot_count"],
            "control_positions": [
                slot["run_ordinal"]
                for slot in block["slots"]
                if slot["slot_type"] == "control"
            ],
        }
        for block in schedule["blocks"]
    ]
    control_document = loaded["control_contract"]
    gate(
        "L0.6-control-contract",
        control_document["positions"] == list(CONTROL_POSITIONS)
        and control_document["per_block"] == 6
        and control_document["fault_mapping_allowed"] is False
        and control_document["execution_adapter_status"] == "required-not-implemented"
        and all(row["control_positions"] == list(CONTROL_POSITIONS) for row in block_summary),
        {"control_contract": control_document, "blocks": block_summary},
    )
    gate(
        "L0.7-deterministic-blocks",
        schedule == build_schedule_manifest(_offline_test_identity()),
        {
            "offline_test_vector_id": OFFLINE_TEST_VECTOR_ID,
            "schedule_hash": schedule["schedule_hash"],
            "blocks": block_summary,
        },
    )

    identity = _offline_test_identity()
    gate(
        "L0.8-freeze-identity-shape",
        len(identity.to_dict()) == 8
        and all(value != "0" * 64 for value in identity.to_dict().values()),
        {
            "offline_test_vector_id": OFFLINE_TEST_VECTOR_ID,
            "field_names": sorted(identity.to_dict()),
            "identity_freeze_ready": False,
        },
    )

    scans = _scan_offline_production()
    gate(
        "L0.9-zero-live-and-conclusion-lock",
        all(not row["violations"] for row in scans)
        and loaded["live_authorized"] is False
        and loaded["formal_claim_allowed"] is False
        and schedule["live_authorized"] is False
        and schedule["formal_claim_allowed"] is False,
        {
            "files": list(scans),
            "live_authorized": False,
            "formal_claim_allowed": False,
        },
    )
    selftest = run_selftest()
    gate(
        "L0.10-mutation-selftest",
        selftest["all_pass"] is True,
        {"check_count": selftest["check_count"], "checks": selftest["checks"]},
    )

    manifest_basis = {
        "schema_name": "traditional_v2_lite_lite0_implementation_manifest",
        "schema_version": "1.0.0",
        "artifacts": list(artifacts),
    }
    all_pass = all(row["pass"] is True for row in gates)
    return {
        "schema_name": REPORT_SCHEMA,
        "schema_version": REPORT_VERSION,
        "verdict": "LITE0_OFFLINE_PASS" if all_pass else "LITE0_OFFLINE_FAIL",
        "all_pass": all_pass,
        "gates": gates,
        "gate_count": len(gates),
        "implementation_bundle_sha256": canonical_sha256(manifest_basis),
        "identity_freeze_ready": False,
        "runner_wrapper_ready": False,
        "live_authorized": False,
        "formal_claim_allowed": False,
        "collection_started": False,
        "not_run": [
            "live",
            "Kubernetes",
            "network",
            "collection",
            "qualification_block",
            "packaging",
            "release",
        ],
    }


def _emit(payload: Mapping[str, Any]) -> None:
    raw = canonical_json_bytes(payload) + b"\n"
    sys.stdout.buffer.write(raw)


def _parser() -> argparse.ArgumentParser:
    class _UsageParser(argparse.ArgumentParser):
        def error(self, message: str) -> "None":
            self.print_usage(sys.stderr)
            print(f"L0V400_USAGE_ERROR: {message}", file=sys.stderr)
            raise SystemExit(4)

    parser = _UsageParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help="candidate repository root; must be this verifier's containing repository",
    )
    parser.add_argument("--selftest", action="store_true")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        requested_root = args.repo_root.resolve(strict=True)
    except OSError:
        print("L0V400_USAGE_ERROR: repo root does not exist", file=sys.stderr)
        return 4
    if requested_root != REPO_ROOT:
        print(
            "L0V400_USAGE_ERROR: repo root does not contain this verifier",
            file=sys.stderr,
        )
        return 4
    try:
        payload = run_selftest() if args.selftest else run_candidate()
    except LiteContractError as exc:
        payload = {
            "schema_name": REPORT_SCHEMA,
            "schema_version": REPORT_VERSION,
            "verdict": "LITE0_OFFLINE_FAIL",
            "all_pass": False,
            "error_code": exc.code,
            "error": str(exc),
            "live_authorized": False,
            "formal_claim_allowed": False,
        }
        _emit(payload)
        return 1
    except (OSError, UnicodeError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        payload = {
            "schema_name": REPORT_SCHEMA,
            "schema_version": REPORT_VERSION,
            "verdict": "LITE0_INTERNAL_ERROR",
            "all_pass": False,
            "error_code": "L0V500_INTERNAL_ERROR",
            "error": exc.__class__.__name__,
            "live_authorized": False,
            "formal_claim_allowed": False,
        }
        _emit(payload)
        return 5
    _emit(payload)
    return 0 if payload["all_pass"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "main",
    "run_candidate",
    "run_selftest",
    "scan_python_source",
]
