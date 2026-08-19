"""Unique offline E0-E4 live-preflight verifier; it never authorizes live."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from scripts.chaos.ctk.traditional_v2_lite.artifact_manifest import (
    ARTIFACT_MANIFEST_FILENAME,
    REQUIRED_ARTIFACT_PATHS,
    ArtifactManifestError,
    build_manifest_document,
    revalidate_manifest,
    write_manifest,
)
from scripts.chaos.ctk.traditional_v2_lite.cleanup_audit import ResourceRecord, audit_cleanup
from scripts.chaos.ctk.traditional_v2_lite.collect_lite import build_dry_run_report
from scripts.chaos.ctk.traditional_v2_lite.contract import (
    FreezeIdentity,
    build_contract_document,
    build_schedule_manifest,
    canonical_json_bytes,
)
from scripts.chaos.ctk.traditional_v2_lite.control_adapter import build_control_plans
from scripts.chaos.ctk.traditional_v2_lite.telemetry_journal import QueryRecord, telemetry_acceptable


REPORT_SCHEMA_NAME = "traditional_v2_lite_e0_e4_offline_acceptance"
REPORT_SCHEMA_VERSION = "1.0.0"
VERDICT = "LITE_E0_E4_OFFLINE_PASS"
LIVE_VERDICT = "NO_GO_LIVE"
EXPECTED_PACKAGE_FILES = (
    "__init__.py",
    "artifact_manifest.py",
    "cleanup_audit.py",
    "collect_lite.py",
    "contract.py",
    "control_adapter.py",
    "evidence.py",
    "ledger.py",
    "local_adapter.py",
    "phase_journal.py",
    "runner_wrapper.py",
    "telemetry_journal.py",
    "verifier_adapter.py",
    "verify_lite0.py",
    "verify_lite_live.py",
    "workload_driver.py",
)
TEST_MODULES = (
    "tests.unit.test_traditional_v2_lite_plan",
    "tests.qa.test_traditional_v2_lite_verifier",
    "tests.unit.test_traditional_v2_lite_wrapper",
    "tests.unit.test_traditional_v2_lite_evidence",
    "tests.unit.test_traditional_v2_lite_verifier_adapter",
    "tests.unit.test_traditional_v2_lite_local_adapter",
    "tests.unit.test_traditional_v2_lite_integration",
    "tests.unit.test_traditional_v2_lite_scientific_sidecars",
)
_FORBIDDEN_IMPORTS = {"subprocess", "socket", "requests", "urllib", "kubernetes"}
_FORBIDDEN_CALLS = {"execute_attempt", "system", "popen", "kubectl"}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _static_violations(source: str) -> list[str]:
    tree = ast.parse(source)
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".", 1)[0] in _FORBIDDEN_IMPORTS:
                    violations.append("import:" + alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module.split(".", 1)[0] in _FORBIDDEN_IMPORTS:
                violations.append("import:" + node.module)
        elif isinstance(node, ast.Call):
            name = None
            if isinstance(node.func, ast.Name):
                name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                name = node.func.attr
            if name and name.lower() in _FORBIDDEN_CALLS:
                violations.append("call:" + name)
    return sorted(violations)


def _run_author_tests(repo_root: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, "-m", "unittest", *TEST_MODULES, "-q"],
        cwd=str(repo_root),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        check=False,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=120,
    )
    combined = completed.stdout + completed.stderr
    match = re.search(r"Ran (\d+) tests", combined)
    count = int(match.group(1)) if match else 0
    if completed.returncode != 0 or count < 99:
        raise RuntimeError("cumulative Lite author tests failed or count regressed")
    return {"returncode": completed.returncode, "test_count": count, "module_count": len(TEST_MODULES)}


def _mutation_selftests(repo_root: Path) -> list[dict[str, Any]]:
    checks = []
    with tempfile.TemporaryDirectory() as temporary:
        scope = Path(temporary).resolve()
        candidate = scope / "candidate"
        candidate.mkdir()
        for relative in REQUIRED_ARTIFACT_PATHS:
            if relative == "verifier-provenance/result.json":
                continue
            path = candidate / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((relative + "\n").encode("utf-8"))
        try:
            build_manifest_document(candidate)
        except ArtifactManifestError:
            checks.append({"id": "missing-provenance", "pass": True})
        else:
            checks.append({"id": "missing-provenance", "pass": False})

    with tempfile.TemporaryDirectory() as temporary:
        scope = Path(temporary).resolve()
        candidate = scope / "candidate"
        candidate.mkdir()
        for relative in REQUIRED_ARTIFACT_PATHS:
            path = candidate / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((relative + "\n").encode("utf-8"))
        manifest = scope / ARTIFACT_MANIFEST_FILENAME
        digest = write_manifest(candidate, manifest)
        (candidate / "summary.md").write_text("drift\n", encoding="utf-8", newline="\n")
        try:
            revalidate_manifest(candidate, manifest, digest)
        except ArtifactManifestError:
            checks.append({"id": "manifest-drift", "pass": True})
        else:
            checks.append({"id": "manifest-drift", "pass": False})

    unavailable = QueryRecord("required", "sum(x)", 0, 1, "timeout", 1, True, "timeout", None)
    checks.append({"id": "required-query-failure", "pass": telemetry_acceptable((unavailable,)) is False})
    residual = ResourceRecord(1, "Chaos", "x", "uid-x", "attempt-1", "OWNED", True)
    checks.append({"id": "cleanup-residual", "pass": audit_cleanup("attempt-1", (residual,)).next_allowed is False})

    contract = build_contract_document()
    identity = FreezeIdentity(
        _sha(repo_root / "scripts" / "chaos" / "ctk" / "chaos_k8s_runner.py"),
        contract["contract_sha256"],
        "3" * 64,
        "4" * 64,
        "5" * 64,
        "6" * 64,
        "7" * 64,
        "8" * 64,
    )
    schedule = build_schedule_manifest(identity)
    schedule["blocks"][0]["slots"] = schedule["blocks"][0]["slots"][1:]
    try:
        build_control_plans(contract, schedule)
    except Exception:
        checks.append({"id": "control-count-error", "pass": True})
    else:
        checks.append({"id": "control-count-error", "pass": False})
    forbidden_samples = {
        "forbidden-subprocess": "import subprocess\nsubprocess.run([])\n",
        "forbidden-network": "import socket\nsocket.socket()\n",
        "forbidden-k8s": "import kubernetes\nkubernetes.client()\n",
    }
    checks.extend(
        {"id": identifier, "pass": bool(_static_violations(source))}
        for identifier, source in forbidden_samples.items()
    )
    return checks


def verify(repo_root: Path) -> dict[str, Any]:
    repo_root = repo_root.resolve(strict=True)
    package = repo_root / "scripts" / "chaos" / "ctk" / "traditional_v2_lite"
    actual_files = tuple(sorted(path.name for path in package.glob("*.py")))
    if actual_files != EXPECTED_PACKAGE_FILES:
        raise RuntimeError("Lite package census drifted")
    census = [{"path": str((package / name).relative_to(repo_root)).replace("\\", "/"), "sha256": _sha(package / name)} for name in actual_files]

    scanned = (
        "collect_lite.py",
        "control_adapter.py",
        "telemetry_journal.py",
        "workload_driver.py",
        "phase_journal.py",
        "cleanup_audit.py",
    )
    static = {name: _static_violations((package / name).read_text(encoding="utf-8")) for name in scanned}
    if any(static.values()):
        raise RuntimeError("forbidden live dependency in dry-run/sidecar modules")

    dataset_root = repo_root / ".traditional-v2-lite-preflight-never-created"
    before = dataset_root.exists()
    dry_run = build_dry_run_report(
        repo_root=repo_root,
        dataset_root=dataset_root,
        python_executable=Path(sys.executable),
    )
    after = dataset_root.exists()
    if before or after or dry_run["side_effects"] != {
        "ledger_created": False,
        "attempt_directory_created": False,
        "executor_called": False,
        "verifier_called": False,
        "legacy_runner_called": False,
        "network_called": False,
    }:
        raise RuntimeError("dry-run caused or claimed a side effect")

    mutations = _mutation_selftests(repo_root)
    if not mutations or not all(item["pass"] is True for item in mutations):
        raise RuntimeError("mutation selftest failed")
    acceptance_files = (
        repo_root / "docs" / "acceptance" / "traditional-v2-lite-live-20260813.md",
        repo_root / "tests" / "qa" / "test_traditional_v2_lite_live_verifier.py",
    )
    if not all(path.is_file() for path in acceptance_files):
        raise RuntimeError("E4 acceptance or independent verifier test is missing")
    readme = (repo_root / "docs" / "acceptance" / "README.md").read_text(encoding="utf-8")
    if "traditional-v2-lite-live-20260813.md" not in readme:
        raise RuntimeError("E4 acceptance route is missing")
    tests = _run_author_tests(repo_root)
    return {
        "schema_name": REPORT_SCHEMA_NAME,
        "schema_version": REPORT_SCHEMA_VERSION,
        "verdict": VERDICT,
        "live_verdict": LIVE_VERDICT,
        "all_pass": True,
        "live_authorized": False,
        "formal_claim_allowed": False,
        "dry_run_only": True,
        "file_census": census,
        "e4_census": [
            {
                "path": str(path.relative_to(repo_root)).replace("\\", "/"),
                "sha256": _sha(path),
            }
            for path in acceptance_files
        ],
        "static_violations": static,
        "dry_run_report_sha256": hashlib.sha256(canonical_json_bytes(dry_run)).hexdigest(),
        "author_tests": tests,
        "mutation_selftests": mutations,
        "not_run": ["live", "Kubernetes", "network", "engineering_smoke", "B1", "collection", "packaging", "release"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args(argv)
    try:
        report = verify(args.repo_root)
    except Exception as exc:
        failure = {
            "schema_name": REPORT_SCHEMA_NAME,
            "schema_version": REPORT_SCHEMA_VERSION,
            "verdict": "LITE_E0_E4_OFFLINE_FAIL",
            "live_verdict": LIVE_VERDICT,
            "all_pass": False,
            "live_authorized": False,
            "formal_claim_allowed": False,
            "failure": f"{exc.__class__.__name__}: {exc}",
            "not_run": ["live", "Kubernetes", "network", "engineering_smoke", "B1", "collection", "packaging", "release"],
        }
        sys.stdout.buffer.write(canonical_json_bytes(failure) + b"\n")
        return 1
    sys.stdout.buffer.write(canonical_json_bytes(report) + b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "EXPECTED_PACKAGE_FILES",
    "LIVE_VERDICT",
    "REPORT_SCHEMA_NAME",
    "REPORT_SCHEMA_VERSION",
    "TEST_MODULES",
    "VERDICT",
    "main",
    "verify",
]
