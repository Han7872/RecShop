"""Minimal, first-attempt-only traditional-v2-lite block coordinator.

This module does not define fault primitives.  Every fault slot is an exact
argv projection of a previously collected ``chaos_k8s_runner.py`` alias.  Sham
controls perform only exact absent-to-absent env-unset operations and then
prove that deployment/pod identity, telemetry, checksums and Chaos census did
not change; they create no service fault, Chaos resource, or runner call.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Protocol, Sequence

from .contract import (
    ACTIVE_SCENARIO_IDS,
    CONTRACT_RELATIVE_PATH,
    DEFAULT_SEED,
    DEFERRED_SCENARIO_IDS,
    FROZEN_BLOCK_IDS,
    FreezeIdentity,
    STRICT51_BLOCK_IDS,
    STRICT51_CONTRACT_RELATIVE_PATH,
    build_schedule_manifest,
    build_strict51_logical_schedule,
    extract_runner_fault_choices,
    load_contract,
    load_strict51_contract,
    validate_schedule_manifest,
)
from .smoke_lite import (
    EXPECTED_DB_CHECKSUMS,
    EXPECTED_RUNNER_SHA256,
    LocalSmokeBackend,
    SmokeRequest,
    _canonical,
    _sha_file,
    run_strict51_query,
    strict51_query_urls,
    validate_preflight,
)
from .local_adapter import LocalAdapterError, _stop_process_tree_windows
from .telemetry_journal import telemetry_acceptable
from .workload_driver import run_open_loop


FROZEN_ITEM = "0071341196"
FROZEN_POLL = "2.0"
FROZEN_CONTRACT_ARTIFACT_SHA256 = "df704cd0f340f45b6949ed411e6d156ef43f659392934527a733877f4eed3948"
FROZEN_CONTRACT_SHA256 = "19be018bba20d56736f6baaceca73c51b71b233085d5adcf54bee1724dcb471f"
FROZEN_B1_BLOCK_HASH = "6d3cd566235ebf8c6d6d532ca6c20b817b1be4cdeb9544819b029dd45afad77a"
FROZEN_B1_FAULT_ORDER_HASH = "a465de1e3dcedbc6408792a07fd2b6678ca8a3db946640a2ca2b559e1d4da4e7"
FROZEN_BLOCK_HASHES = MappingProxyType({
    "B1": (FROZEN_B1_BLOCK_HASH, FROZEN_B1_FAULT_ORDER_HASH),
    "B2": (
        "7c038a1faf110232a383a0697b7c63fc7f648fc6a2afddad37b66df8e03f4b07",
        "dc22db10481e3205e2f97855520f43203922458f61d5fde01e1f8f85659f52aa",
    ),
    "B3": (
        "7dd6305602b9e0eff1490dfd9e0b78e9b1018056330a8adbef86901d16c9f066",
        "788fa4bcdb1142a6fba4df9f0a9a608d46a3bbf45a98d1739af858c485d321a6",
    ),
    "B4": (
        "ec8b42ba1ad84bc22af37588c70afbda1909a68cf104e588f12855171f4bf49f",
        "5d13c681c5c2bd00ac681e0f53ef1b8316acb66b0befc8cb5651abc907787ab8",
    ),
    "B5": (
        "ee7ad2a8cd1f2caa09f9f621066a0ef613ff35f1c9323793f72e376a1ab6a000",
        "62260fb65eb6f9243b83a9f210d54964c297ec6f738caa52b23588fd3787ff63",
    ),
})
DEFERRED_BLOCK_ID = "B6_DEFERRED"
# S51-AMD is NOT a schedule block: it only routes the coordinator backend into
# the strict51 protocol (panel-owner flag, bounded-query review, artifact-bound
# runner identity) for disclosed single-scenario amendment attempts driven by
# tests/qa/run_strict51_amendment.py.  run_block/build_block_slots reject it
# (no such block in any frozen schedule) — it can never be mined as a block.
AMENDMENT_BLOCK_ID = "S51-AMD"
SUPPORTED_BLOCK_IDS = (*FROZEN_BLOCK_IDS, DEFERRED_BLOCK_ID, *STRICT51_BLOCK_IDS, AMENDMENT_BLOCK_ID)
# strict51 frozen identities.  The artifact file SHA is frozen unilaterally by
# this coordinator (never written back into the artifact); the freeze report
# SHA is filled only after the A10 freeze exists and gates formal mining.
FROZEN_STRICT51_CONTRACT_ARTIFACT_SHA256 = "fb773c108771c58687bad222b5baefb8e79e32b933fcb090b261044dd47ec955"
# A10 freeze report lives at this fixed repo-relative path; its SHA is
# resolved at failure time by reading the frozen file (external one-way
# binding — a back-filled constant would change b1_lite.py AFTER the freeze
# report recorded its SHA, i.e. a circular identity).  Before the freeze
# exists the extension reports null (resume_allowed stays fail-closed).
STRICT51_FREEZE_REPORT_RELATIVE_PATH = (
    "docs/acceptance/contracts/traditional-v2-lite-strict51-20260816/strict51-freeze-report.json"
)
STRICT51_FAILURE_CLASSIFICATIONS = ("SCIENTIFIC_ATTRITION", "TECHNICAL_FAILURE", "SAFETY_FAILURE")


def _is_strict51_block(block_id: str) -> bool:
    return block_id in STRICT51_BLOCK_IDS or block_id == AMENDMENT_BLOCK_ID
FROZEN_B6_SCENARIO_ORDER = (
    "S07", "S08", "D07", "D11", "D12", "D13", "D15", "D17", "T01", "T02", "T04",
)
FROZEN_B6_SCHEDULE_SHA256 = "e10214426cb2370aec8e90a2ef35a1a6574474a3abb80bda5b3557cd308407ff"
FROZEN_B1_ORDER = (
    "NF_START", "T06", "D01", "D09", "T07", "D04", "S02", "S12", "D20",
    "SHAM_EXCEPTION", "S09", "S22", "D18", "S16", "S15", "T08", "S11", "S13",
    "SHAM_COMBINED", "S17", "S14", "D16", "S10", "S01", "D19", "S20", "D06",
    "NF_MID", "T03", "S03", "D02", "S19", "S18", "S06", "D10", "S04",
    "SHAM_DELAY", "D14", "D05", "D21", "S05", "D03", "T05", "S21", "D08", "NF_END",
)
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_DB_RUNTIME_KEYS = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")


class B1Error(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> "None":
    raise B1Error(code, message)


@dataclass(frozen=True, slots=True)
class ScenarioCommandSpec:
    scenario_id: str
    runner_fault: str
    target_service: str | None
    fault_instance_arity: int
    disposition: str
    args: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class B1Slot:
    run_ordinal: int
    slot_id: str
    slot_type: str
    planned_track: str


@dataclass(frozen=True, slots=True)
class SlotReview:
    slot_id: str
    review_sha256: str
    runner_invocations: int
    fault_calls: int
    query_count: int
    checksum_zero_drift: bool
    identity_unchanged: bool
    cleanup_clear: bool


class QualificationBackend(Protocol):
    def preflight(self) -> str: ...
    def run_fault(self, slot: B1Slot, spec: ScenarioCommandSpec, argv: tuple[str, ...], attempt_root: Path) -> SlotReview: ...
    def run_control(self, slot: B1Slot, action: Mapping[str, Any], attempt_root: Path) -> SlotReview: ...


def _common(stage: int, *, offset: int = 14, duration: int = 31, wide: bool = False) -> tuple[str, ...]:
    args = (
        "--item", FROZEN_ITEM, "--stage-seconds", str(stage), "--poll", FROZEN_POLL,
        "--f2-offset-seconds", str(offset), "--f2-duration-seconds", str(duration), "--keep-carrier",
    )
    return args + (("--wide-metrics",) if wide else ())


def _user() -> tuple[str, ...]:
    return ("--user-token", "{LITE_SMOKE_USER_TOKEN}")


def _catalog() -> tuple[str, ...]:
    return ("--catalog-direct-base", "http://127.0.0.1:5005")


def _scenario_args() -> dict[str, tuple[str, ...]]:
    rows: dict[str, tuple[str, ...]] = {
        "S01": _common(30, wide=True),
        "S02": _common(90, wide=True),
        "S03": _common(30, wide=True),
        "S04": _common(30, wide=True),
        "S05": ("--carriers", "s1_hostcpu") + _user() + _common(30, wide=True),
        "S06": ("--carriers", "s2_dblock") + _catalog() + _user() + _common(32, wide=True),
        "D01": _common(60, wide=True),
        "D02": _common(60, wide=True),
        "D03": ("--deep", "--carriers", "s3_checkout_fanin") + _user() + _common(30),
        "D04": ("--deep",) + _user() + _common(48),
        "D05": ("--deep", "--carriers", "s3_checkout_fanin") + _user() + _common(30, offset=8, duration=14),
        "D06": ("--carriers", "s1_hostcfg") + _user() + _catalog() + _common(60, wide=True),
        "D08": ("--carriers", "s_netpod_cross") + _user() + _catalog() + _common(140, wide=True),
        "D09": ("--deep", "--carriers", "s_dk12_sasrec_net") + _user() + _catalog() + _common(60),
        "D10": ("--carriers", "s2_dblock_combo") + _user() + _catalog() + _common(32, wide=True),
        "D14": ("--deep", "--carriers", "s_dk17_netdelay_svccpu") + _user() + _catalog() + _common(90),
        "D16": ("--carriers", "s_podfail_netdelay") + _user() + _catalog() + _common(200, offset=20, wide=True),
        "T03": (
            "--deep", "--carriers", "s2_dblock_combo",
        ) + _catalog() + _user() + _common(150, offset=20, duration=90) + (
            "--f3-offset-seconds", "50", "--f3-duration-seconds", "30",
        ),
    }
    for sid, target, fault, stage in (
        ("S09", "order", "service_cpu_single", 30), ("S10", "cart", "service_cpu_single", 30),
        ("S11", "review-query", "service_cpu_single", 30), ("S12", "backend", "service_cpu_single", 30),
        ("S13", "checkout", "service_cpu_single", 30), ("S14", "order", "pod_failure_single", 30),
        ("S15", "cart", "pod_failure_single", 30), ("S16", "review-query", "pod_failure_single", 30),
        ("S17", "backend", "pod_failure_single", 30), ("S18", "checkout", "pod_failure_single", 30),
        ("S19", "search", "pod_failure_single", 30),
    ):
        del fault
        rows[sid] = ("--target-service", target) + _common(stage, wide=True)
    rec_common = (
        "--target-service", "rec-agent", "--recagent-seq",
        "B000PGJ7SA,B000HKMM4A,B00F0RD86G,B01C2O7YNC", "--recagent-top-k", "5",
        "--recagent-recommend-timeout", "150",
    )
    rows["S20"] = rec_common + _common(240, wide=True)
    rows["S21"] = rec_common + ("--net-delay-ms", "450", "--net-jitter-ms", "90") + _common(120, wide=True)
    rows["S22"] = rec_common + _common(30, wide=True)
    for sid in ("D18", "D19", "D20", "D21", "T05", "T06", "T07", "T08"):
        rows[sid] = ("--deep",) + _user() + _common(240)
    return rows


def _deferred_scenario_args() -> dict[str, tuple[str, ...]]:
    inventory = ("--inventory-direct-base", "http://127.0.0.1:5013")
    cat_delay = ("--cat-delay-ms", "2000")
    inv_delay = ("--inv-delay-ms", "2000")
    return {
        "S07": _catalog() + _common(30),
        "S08": _catalog() + cat_delay + _common(30),
        "D07": ("--deep", "--carriers", "deep_dual_edge") + _user() + _catalog() + inventory + inv_delay + _common(300, offset=40),
        "D11": ("--deep", "--carriers", "s_dk13_inv_run") + _user() + _catalog() + inventory + inv_delay + _common(90),
        "D12": ("--deep", "--carriers", "s_dk14_catlat_cfg") + _user() + _catalog() + cat_delay + _common(60),
        "D13": ("--deep", "--carriers", "s_dk15_catlat_loss") + _user() + _catalog() + cat_delay + _common(300),
        "D15": ("--deep", "--carriers", "s_dk18_catlat_svccpu") + _user() + _catalog() + cat_delay + _common(90),
        "D17": ("--deep",) + _user() + _common(300),
        "T01": ("--deep", "--carriers", "s_triple01_pricing_cat_cfg") + _catalog() + _user() + cat_delay + _common(120, offset=40, duration=40),
        "T02": ("--deep", "--carriers", "s_t1_inv_cfg_retry") + _user() + inv_delay + _common(120, offset=40, duration=40),
        "T04": (
            "--deep", "--carriers", "s_triple_lif_dep_cfg",
        ) + _catalog() + _user() + (
            "--item", FROZEN_ITEM, "--cat-delay-ms", "2000", "--poll", FROZEN_POLL,
            "--stage-seconds", "250", "--f2-offset-seconds", "25",
            "--cfg-carve-seconds", "20", "--f3-dwell-seconds", "60", "--keep-carrier",
        ),
    }


def build_scenario_specs(repo_root: Path, *, include_deferred: bool = False,
                         protocol: str = "legacy") -> Mapping[str, ScenarioCommandSpec]:
    if protocol == "strict51":
        return _build_strict51_scenario_specs(repo_root)
    contract_path = repo_root / CONTRACT_RELATIVE_PATH
    if _sha_file(contract_path) != FROZEN_CONTRACT_ARTIFACT_SHA256:
        _fail("B1E001_MAPPING_INCOMPLETE", "frozen contract artifact SHA differs")
    contract = load_contract(contract_path)
    runner_binding = next(
        (row for row in contract["source_bindings"] if row["path"] == "scripts/chaos/ctk/chaos_k8s_runner.py"),
        None,
    )
    if (
        contract.get("contract_sha256") != FROZEN_CONTRACT_SHA256
        or contract.get("execution_status") != "offline-contract-only"
        or contract.get("live_authorized") is not False
        or contract.get("formal_claim_allowed") is not False
        or type(runner_binding) is not dict
        or runner_binding.get("sha256") != EXPECTED_RUNNER_SHA256
    ):
        _fail("B1E001_MAPPING_INCOMPLETE", "frozen contract identity differs")
    args_by_id = _scenario_args()
    selected = [row for row in contract["scenarios"] if row["disposition"] in {"strict", "auxiliary"}]
    expected_ids = set(ACTIVE_SCENARIO_IDS)
    if include_deferred:
        args_by_id.update(_deferred_scenario_args())
        selected = [row for row in contract["scenarios"] if row["disposition"] in {"strict", "auxiliary", "deferred"}]
        expected_ids.update(DEFERRED_SCENARIO_IDS)
    if {row["scenario_id"] for row in selected} != expected_ids or set(args_by_id) != expected_ids:
        _fail("B1E001_MAPPING_INCOMPLETE", "active roster and argv mapping differ")
    choices = extract_runner_fault_choices(repo_root / "scripts/chaos/ctk/chaos_k8s_runner.py")
    specs: dict[str, ScenarioCommandSpec] = {}
    for row in selected:
        sid = row["scenario_id"]
        if row["runner_fault"] not in choices:
            _fail("B1E001_MAPPING_INCOMPLETE", f"runner lacks alias for {sid}")
        target_args = args_by_id[sid]
        expected_target = row["runner_target_service"]
        observed_target = None
        if "--target-service" in target_args:
            observed_target = target_args[target_args.index("--target-service") + 1]
        if (expected_target not in {None, "catalog"} and observed_target != expected_target) or (
            expected_target in {None, "catalog"} and observed_target is not None
        ):
            _fail("B1E001_MAPPING_INCOMPLETE", f"target mapping differs for {sid}")
        specs[sid] = ScenarioCommandSpec(
            sid, row["runner_fault"], expected_target, row["fault_instance_arity"], row["disposition"], target_args,
        )
    return MappingProxyType(specs)


def _build_strict51_scenario_specs(repo_root: Path) -> Mapping[str, ScenarioCommandSpec]:
    """strict51: all 51 scenarios are strict; argv = legacy strict + deferred union."""
    if FROZEN_STRICT51_CONTRACT_ARTIFACT_SHA256 is None:
        _fail("B1E001_MAPPING_INCOMPLETE", "strict51 contract artifact identity is not frozen yet")
    contract_path = repo_root / STRICT51_CONTRACT_RELATIVE_PATH
    if _sha_file(contract_path) != FROZEN_STRICT51_CONTRACT_ARTIFACT_SHA256:
        _fail("B1E001_MAPPING_INCOMPLETE", "frozen strict51 contract artifact SHA differs")
    contract = load_strict51_contract(contract_path)
    args_by_id = _scenario_args()
    args_by_id.update(_deferred_scenario_args())
    selected = [row for row in contract["scenarios"] if row["disposition"] == "strict"]
    expected_ids = set(args_by_id)
    if {row["scenario_id"] for row in selected} != expected_ids or len(selected) != 51:
        _fail("B1E001_MAPPING_INCOMPLETE", "strict51 roster and argv mapping differ")
    choices = extract_runner_fault_choices(repo_root / "scripts/chaos/ctk/chaos_k8s_runner.py")
    specs: dict[str, ScenarioCommandSpec] = {}
    for row in selected:
        sid = row["scenario_id"]
        if row["runner_fault"] not in choices:
            _fail("B1E001_MAPPING_INCOMPLETE", f"runner lacks alias for {sid}")
        target_args = args_by_id[sid]
        expected_target = row["runner_target_service"]
        observed_target = None
        if "--target-service" in target_args:
            observed_target = target_args[target_args.index("--target-service") + 1]
        if (expected_target not in {None, "catalog"} and observed_target != expected_target) or (
            expected_target in {None, "catalog"} and observed_target is not None
        ):
            _fail("B1E001_MAPPING_INCOMPLETE", f"target mapping differs for {sid}")
        specs[sid] = ScenarioCommandSpec(
            sid, row["runner_fault"], expected_target, row["fault_instance_arity"], row["disposition"], target_args,
        )
    return MappingProxyType(specs)


def build_block_slots(repo_root: Path, block_id: str) -> tuple[B1Slot, ...]:
    if block_id not in SUPPORTED_BLOCK_IDS:
        _fail("B1E002_SCHEDULE_DRIFT", f"unknown block: {block_id}")
    contract = load_contract(repo_root / CONTRACT_RELATIVE_PATH)
    if _is_strict51_block(block_id):
        schedule = build_strict51_logical_schedule()
        block = next((row for row in schedule["blocks"] if row["block_id"] == block_id), None)
        if type(block) is not dict:
            _fail("B1E002_SCHEDULE_DRIFT", f"missing strict51 block: {block_id}")
        rows = []
        for slot in block["slots"]:
            slot_id = slot.get("control_id") or slot.get("scenario_id")
            rows.append(B1Slot(slot["run_ordinal"], slot_id, slot["slot_type"], slot.get("planned_track", "control")))
        return tuple(rows)
    if block_id == DEFERRED_BLOCK_ID:
        observed_deferred = tuple(
            row["scenario_id"] for row in contract["scenarios"] if row["disposition"] == "deferred"
        )
        if observed_deferred != DEFERRED_SCENARIO_IDS:
            _fail("B1E002_SCHEDULE_DRIFT", "deferred roster differs")
        rows = [B1Slot(1, "NF_START", "control", "control")]
        ordinal = 2
        for repeat in range(1, 6):
            for scenario_id in FROZEN_B6_SCENARIO_ORDER:
                rows.append(B1Slot(ordinal, f"{scenario_id}_R{repeat}", "fault", "deferred"))
                ordinal += 1
        rows.append(B1Slot(ordinal, "NF_END", "control", "control"))
        schedule_sha256 = hashlib.sha256(_canonical([asdict(row) for row in rows])).hexdigest()
        if schedule_sha256 != FROZEN_B6_SCHEDULE_SHA256:
            _fail("B1E002_SCHEDULE_DRIFT", "B6 deferred schedule hash differs")
        return tuple(rows)
    identity = FreezeIdentity(*[str(number) * 64 for number in range(1, 9)])
    schedule = build_schedule_manifest(identity, seed=DEFAULT_SEED)
    validate_schedule_manifest(schedule)
    block = next((row for row in schedule["blocks"] if row["block_id"] == block_id), None)
    if type(block) is not dict:
        _fail("B1E002_SCHEDULE_DRIFT", f"missing block: {block_id}")
    expected_block_hash, expected_fault_hash = FROZEN_BLOCK_HASHES[block_id]
    if block["block_schedule_hash"] != expected_block_hash or block["fault_order_hash"] != expected_fault_hash:
        _fail("B1E002_SCHEDULE_DRIFT", f"{block_id} block hash differs")
    rows = []
    for slot in block["slots"]:
        slot_id = slot.get("control_id") or slot.get("scenario_id")
        rows.append(B1Slot(slot["run_ordinal"], slot_id, slot["slot_type"], slot.get("planned_track", "control")))
    if block_id == "B1" and tuple(row.slot_id for row in rows) != FROZEN_B1_ORDER:
        _fail("B1E002_SCHEDULE_DRIFT", "B1 slot order differs")
    if contract["expected_counts"] != {
        "active_per_block": 40, "auxiliary": 1, "controls_per_block": 6,
        "deferred": 11, "slots_per_block": 46, "strict": 39, "universe": 51,
    }:
        _fail("B1E002_SCHEDULE_DRIFT", "contract counts differ")
    return tuple(rows)


def build_b1_slots(repo_root: Path) -> tuple[B1Slot, ...]:
    """Compatibility wrapper for the exact already-collected B1 schedule."""
    return build_block_slots(repo_root, "B1")


def build_runner_argv(
    repo_root: Path,
    attempt_root: Path,
    slot: B1Slot,
    spec: ScenarioCommandSpec,
    *,
    block_id: str = "B1",
) -> tuple[str, ...]:
    if block_id not in SUPPORTED_BLOCK_IDS:
        _fail("B1E003_COMMAND_DRIFT", f"unknown block: {block_id}")
    case_id = f"{block_id.lower()}-{slot.run_ordinal:02d}-{slot.slot_id.lower()}"
    user_token = os.environ.get("LITE_SMOKE_USER_TOKEN", "")
    if "{LITE_SMOKE_USER_TOKEN}" in spec.args and (not user_token or _SAFE_ID.fullmatch(user_token) is None):
        _fail("B1E007_PREFLIGHT_FAILED", "LITE_SMOKE_USER_TOKEN is required and must be safe")
    resolved_args = tuple(user_token if value == "{LITE_SMOKE_USER_TOKEN}" else value for value in spec.args)
    # ★strict51(§5.3): coordinator 只对 D06/D12/D13 三个 exact alias 传入 panel-owner 开关。
    owner_args: tuple[str, ...] = ()
    if _is_strict51_block(block_id) and spec.scenario_id in ("D06", "D12", "D13"):
        owner_args = ("--strict51-panel-owner",)
    argv = (
        sys.executable, str(repo_root / "scripts/chaos/ctk/chaos_k8s_runner.py"),
        "--case-id", case_id, "--fault", spec.runner_fault, *resolved_args, *owner_args,
        "--out-dir", str(attempt_root / "runner-out"),
    )
    if any(flag in argv for flag in ("--force", "--skip-checksum", "--lite-smoke-event-sink")):
        _fail("B1E003_COMMAND_DRIFT", "forbidden runner flag")
    return argv


def _scenario_id_for_slot(slot: B1Slot, block_id: str) -> str:
    if _is_strict51_block(block_id):
        return slot.slot_id
    if block_id != DEFERRED_BLOCK_ID:
        return slot.slot_id
    match = re.fullmatch(r"(S07|S08|D07|D11|D12|D13|D15|D17|T01|T02|T04)_R[1-5]", slot.slot_id)
    if match is None:
        _fail("B1E002_SCHEDULE_DRIFT", f"invalid B6 slot id: {slot.slot_id}")
    return match.group(1)


def execute_control_action(control_id: str) -> Mapping[str, Any]:
    """Return only the frozen requested action; it is not evidence of success."""
    if control_id.startswith("NF_"):
        if control_id not in {"NF_START", "NF_MID", "NF_END"}:
            _fail("B1E004_CONTROL_INVALID", "unknown no-fault control")
        return {"control_id": control_id, "requested_env_unsets": []}
    if control_id not in {"SHAM_EXCEPTION", "SHAM_COMBINED", "SHAM_DELAY"}:
        _fail("B1E004_CONTROL_INVALID", "unknown control")
    requested = {
        "SHAM_EXCEPTION": ["FAULT_RAISE-"],
        "SHAM_COMBINED": ["FAULT_RAISE-", "FAULT_DELAY_MS-"],
        "SHAM_DELAY": ["FAULT_DELAY_MS-"],
    }[control_id]
    return {"control_id": control_id, "requested_env_unsets": requested}


def _write_new(path: Path, value: Any) -> None:
    raw = _canonical(value)
    try:
        with path.open("xb") as handle:
            handle.write(raw); handle.flush(); os.fsync(handle.fileno())
    except FileExistsError:
        _fail("B1E005_FIRST_ATTEMPT_ONLY", f"refusing overwrite: {path.name}")


def _read_object(path: Path) -> Mapping[str, Any]:
    if path.is_symlink() or not path.is_file():
        _fail("B1E006_REVIEW_FAILED", f"missing {path.name}")
    try:
        value = json.loads(path.read_text(encoding="utf-8", errors="strict"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail("B1E006_REVIEW_FAILED", f"invalid {path.name}: {exc.__class__.__name__}")
    if type(value) is not dict:
        _fail("B1E006_REVIEW_FAILED", f"{path.name} is not object")
    return value


def _read_existing_review(path: Path, slot: B1Slot) -> SlotReview:
    if not (path / "outcome.json").is_file():
        _fail("B1E005_FIRST_ATTEMPT_ONLY", f"existing outcome missing: {slot.run_ordinal}")
    value = _read_object(path / "outcome.json")
    expected_keys = {field.name for field in fields(SlotReview)}
    if set(value) != expected_keys or value.get("slot_id") != slot.slot_id:
        _fail("B1E005_FIRST_ATTEMPT_ONLY", f"existing outcome differs: {slot.run_ordinal}")
    try:
        review = SlotReview(**value)
    except TypeError:
        _fail("B1E005_FIRST_ATTEMPT_ONLY", f"existing outcome invalid: {slot.run_ordinal}")
    if (
        review.query_count != 3
        or not all((review.checksum_zero_drift, review.identity_unchanged, review.cleanup_clear))
    ):
        _fail("B1E005_FIRST_ATTEMPT_ONLY", f"existing outcome incomplete: {slot.run_ordinal}")
    return review


def _read_existing_failure(path: Path, slot: B1Slot) -> None:
    if not (path / "failure.json").is_file():
        _fail("B1E005_FIRST_ATTEMPT_ONLY", f"existing failure missing: {slot.run_ordinal}")
    value = _read_object(path / "failure.json")
    if value.get("slot") != {"run_ordinal": slot.run_ordinal, "slot_id": slot.slot_id}:
        _fail("B1E005_FIRST_ATTEMPT_ONLY", f"existing failure differs: {slot.run_ordinal}")
    if type(value.get("code")) is not str or not value["code"]:
        _fail("B1E005_FIRST_ATTEMPT_ONLY", f"existing failure invalid: {slot.run_ordinal}")


def review_fault_candidate(candidate: Path, spec: ScenarioCommandSpec, repo_root: Path) -> str:
    metadata = _read_object(candidate / "metadata.json")
    groundtruth = _read_object(candidate / "groundtruth.json")
    roots = metadata.get("root_causes")
    services = [row.get("service") for row in roots] if type(roots) is list and all(type(row) is dict for row in roots) else None
    instances = [row.get("instance") for row in roots] if services is not None else None
    validations = metadata.get("validation_results")
    root_metric = metadata.get("root_metric_contract") or {}
    if type(validations) is list:
        validation_statuses = {
            row.get("id"): row.get("status") for row in validations if type(row) is dict
        }
    else:
        validation_statuses = {}
    if spec.disposition == "auxiliary":
        validation_ok = bool(validations) and all(
            type(row) is dict
            and (
                row.get("status") == "pass"
                or (row.get("id") == "cfg_validity_footprint" and row.get("status") == "degraded")
            )
            for row in validations
        )
        manifestation_ok = (
            type(root_metric) is dict
            and root_metric.get("F1") is True
            and root_metric.get("F2") is True
            and isinstance(root_metric.get("notes"), str)
            and "cfg_state_arm=True" in root_metric["notes"]
        )
    else:
        validation_ok = bool(validations) and all(
            type(row) is dict and row.get("status") == "pass" for row in validations
        )
        manifestation_ok = type(root_metric) is dict and root_metric.get("valid") is True
    if (
        metadata.get("sample_id") != candidate.name or groundtruth.get("sample_id") != candidate.name
        or metadata.get("root_count") != spec.fault_instance_arity or groundtruth.get("root_count") != spec.fault_instance_arity
        or services != groundtruth.get("root_cause_services") or instances != groundtruth.get("root_cause_instances")
        or not services or any(type(value) is not str or not value for value in (*services, *instances))
        or metadata.get("sample_status") != "ready_for_release" or metadata.get("ready_for_release") is not True
        or metadata.get("validation_complete") is not True or not validation_ok
        or (metadata.get("checksum_guard") or {}).get("zero_drift") is not True
        or not manifestation_ok
    ):
        _fail("B1E006_REVIEW_FAILED", f"metadata/GT/gates differ for {spec.scenario_id}")
    verifier = repo_root / "scripts/chaos/ctk/verify_dual.py"
    cp = subprocess.run((sys.executable, str(verifier), str(candidate)), shell=False, text=True, capture_output=True, timeout=120, check=False)
    if cp.returncode != 0 or "VERIFY=PASS" not in cp.stdout:
        _fail("B1E006_REVIEW_FAILED", f"verify_dual rejected {spec.scenario_id}")
    facts = {
        "scenario_id": spec.scenario_id, "runner_fault": spec.runner_fault,
        "metadata_sha256": _sha_file(candidate / "metadata.json"),
        "groundtruth_sha256": _sha_file(candidate / "groundtruth.json"),
        "root_services": services, "root_instances": instances,
        "disposition": spec.disposition,
        "review_mode": "config-state-only" if spec.disposition == "auxiliary" else "strict-manifestation",
        "validation_statuses": validation_statuses,
    }
    return hashlib.sha256(_canonical(facts)).hexdigest()


class LocalB1Backend:
    """Fixed local backend.  It has no retry, plugin, or alternate evaluator."""

    def __init__(self, repo_root: Path, dataset_root: Path, block_id: str = "B1") -> None:
        if block_id not in SUPPORTED_BLOCK_IDS:
            _fail("B1E002_SCHEDULE_DRIFT", f"unknown block: {block_id}")
        self.repo_root = repo_root
        self.dataset_root = dataset_root
        self.block_id = block_id
        self.request = SmokeRequest(
            repo_root, dataset_root, f"{block_id.lower()}-preflight", "no-fault", 30, 2.0, 30, 1.0,
        )
        self.smoke = LocalSmokeBackend(self.request)
        self.identity: str | None = None
        self.user_token_sha256: str | None = None

    @staticmethod
    def _db_runtime_env() -> dict[str, str]:
        values = {key: os.environ[key] for key in _DB_RUNTIME_KEYS if os.environ.get(key)}
        if not values.get("DB_PASSWORD"):
            _fail("B1E007_PREFLIGHT_FAILED", "DB_PASSWORD is required in the launcher environment")
        return values

    def _expected_runner_sha256(self) -> str:
        if not _is_strict51_block(getattr(self, "block_id", "")):
            return EXPECTED_RUNNER_SHA256
        if FROZEN_STRICT51_CONTRACT_ARTIFACT_SHA256 is None:
            _fail("B1E001_MAPPING_INCOMPLETE", "strict51 contract artifact identity is not frozen yet")
        contract_path = self.repo_root / STRICT51_CONTRACT_RELATIVE_PATH
        if _sha_file(contract_path) != FROZEN_STRICT51_CONTRACT_ARTIFACT_SHA256:
            _fail("B1E001_MAPPING_INCOMPLETE", "frozen strict51 contract artifact SHA differs")
        contract = load_strict51_contract(contract_path)
        runner_binding = next(
            (row for row in contract["source_bindings"]
             if row.get("path") == "scripts/chaos/ctk/chaos_k8s_runner.py"),
            None,
        )
        if type(runner_binding) is not dict:
            _fail("B1E001_MAPPING_INCOMPLETE", "strict51 contract lacks the runner binding")
        return str(runner_binding["sha256"])

    def preflight(self) -> str:
        user_token = os.environ.get("LITE_SMOKE_USER_TOKEN", "")
        if not user_token or _SAFE_ID.fullmatch(user_token) is None:
            _fail("B1E007_PREFLIGHT_FAILED", "LITE_SMOKE_USER_TOKEN is required and must be safe")
        self.user_token_sha256 = hashlib.sha256(user_token.encode("utf-8")).hexdigest()
        self._db_runtime_env()
        # strict51: retarget the backend's runner-identity expectation to the
        # artifact binding BEFORE any snapshot validation runs (legacy keeps
        # the frozen EXPECTED_RUNNER_SHA256 default).
        self.smoke.expected_runner_sha256 = self._expected_runner_sha256()
        snapshot = self.smoke.preflight(self.request)
        if snapshot.runner_sha256 != self._expected_runner_sha256():
            _fail("B1E007_PREFLIGHT_FAILED", "runner SHA differs")
        self.identity = self.smoke.identity_sha256()
        return self.identity

    @staticmethod
    def _redact_argv(argv: tuple[str, ...]) -> tuple[str, ...]:
        values = list(argv)
        if "--user-token" in values:
            values[values.index("--user-token") + 1] = "<redacted:sha256>"
        return tuple(values)

    def _common_review(self, started: float, *, attempt_root: Path | None = None,
                       window_us: tuple[int, int] | None = None) -> tuple[int, bool, bool]:
        if _is_strict51_block(getattr(self, "block_id", "")):
            if attempt_root is None or window_us is None:
                _fail("B1E008_QUERY_FAILED", "strict51 requires the frozen UTC attempt window")
            queries = self.smoke.strict51_query(attempt_root, window_us[0], window_us[1])
        else:
            ended = time.monotonic()
            queries = self.smoke.query(started, ended)
        if not telemetry_acceptable(queries) or {row.query_id for row in queries} != {"prometheus.up", "jaeger.traces", "loki.logs"}:
            _fail("B1E008_QUERY_FAILED", "required telemetry failed")
        checksums = self.smoke.post_db_checksums()
        checksum_ok = dict(checksums) == EXPECTED_DB_CHECKSUMS
        identity_ok = self.smoke.identity_sha256() == self.identity
        if not checksum_ok or not identity_ok:
            _fail("B1E009_POSTCHECK_FAILED", "checksum or identity drift")
        return len(queries), checksum_ok, identity_ok

    def _catalog_control_state(self) -> Mapping[str, Any]:
        deployment = self.smoke._kubectl_json((
            "-n", "recweb-chaos", "get", "deployment", "catalog", "-o", "json",
        ))
        pods = self.smoke._kubectl_json((
            "-n", "recweb-chaos", "get", "pods", "-l", "app=catalog", "-o", "json",
        ))
        metadata = deployment.get("metadata") or {}
        spec = deployment.get("spec") or {}
        template = spec.get("template") or {}
        containers = ((template.get("spec") or {}).get("containers") or [])
        if type(containers) is not list or not containers:
            _fail("B1E007_PREFLIGHT_FAILED", "catalog container identity missing")
        env_names = {
            str(env.get("name"))
            for container in containers if type(container) is dict
            for env in (container.get("env") or []) if type(env) is dict
        }
        if {"FAULT_RAISE", "FAULT_DELAY_MS"} & env_names:
            _fail("B1E004_CONTROL_INVALID", "sham requires both env keys absent")
        pod_rows = []
        for item in pods.get("items") or []:
            meta = item.get("metadata") or {}; status = item.get("status") or {}
            restarts = sum(int(row.get("restartCount") or 0) for row in (status.get("containerStatuses") or []))
            pod_rows.append((str(meta.get("name", "")), str(meta.get("uid", "")), restarts))
        if not pod_rows:
            _fail("B1E007_PREFLIGHT_FAILED", "catalog pod identity missing")
        return {
            "deployment_uid": str(metadata.get("uid", "")),
            "generation": int(metadata.get("generation") or 0),
            "resource_version": str(metadata.get("resourceVersion", "")),
            "template_sha256": hashlib.sha256(_canonical(template)).hexdigest(),
            "images": [str(row.get("image", "")) for row in containers],
            "pods": sorted(pod_rows),
            "fault_env_absent": True,
        }

    def run_fault(self, slot: B1Slot, spec: ScenarioCommandSpec, argv: tuple[str, ...], attempt_root: Path) -> SlotReview:
        # ★strict51(A3.1): attempt 窗以 UTC wall-clock 捕获 —— start 紧邻 runner 调用前,
        #   end 在 runner 返回+direct review 之后、任何 query 之前; 各调用一次, 不从 monotonic 反推。
        attempt_start_us = time.time_ns() // 1000
        db_runtime = self._db_runtime_env()
        _write_new(attempt_root / "command.json", {
            "argv": list(self._redact_argv(argv)), "scenario_id": spec.scenario_id,
            "user_token_present": "--user-token" in argv,
            "user_token_sha256": self.user_token_sha256 if "--user-token" in argv else None,
            "db_password_present": True,
        })
        stdout = (attempt_root / "runner.stdout.log").open("x", encoding="utf-8", newline="\n")
        stderr = (attempt_root / "runner.stderr.log").open("x", encoding="utf-8", newline="\n")
        env = os.environ.copy(); env.update({
            "PROM_URL": "http://127.0.0.1:9090", "JAEGER_URL": "http://127.0.0.1:16686",
            "LOKI_URL": "http://127.0.0.1:3100", "NO_PROXY": "*", "no_proxy": "*",
            # ★GBK 免疫(2026-08-17): 子进程 Python 对继承管道用 locale(GBK)编码, 任何
            #   print 含非 GBK 字符都会 UnicodeEncodeError 崩非零退出(B6 D12 实证)。
            #   强制 UTF-8 一劳永逸, 与父进程 utf-8 文件句柄一致。
            "PYTHONIOENCODING": "utf-8",
        })
        env.update(db_runtime)
        try:
            process = subprocess.Popen(argv, cwd=str(self.repo_root), env=env, shell=False, text=True, stdout=stdout, stderr=stderr)
            timeout = int(spec.args[spec.args.index("--stage-seconds") + 1]) * 4 + 900
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            try:
                _stop_process_tree_windows(process, confirmation_timeout=10.0)
            except LocalAdapterError as stop_exc:
                _fail("B1E010_RUNNER_FAILED", f"runner tree stop uncertain: {stop_exc.code}")
            _fail("B1E010_RUNNER_FAILED", f"runner failed: {exc.__class__.__name__}")
        except OSError as exc:
            _fail("B1E010_RUNNER_FAILED", f"runner failed: {exc.__class__.__name__}")
        finally:
            stdout.close(); stderr.close()
        if returncode != 0:
            _fail("B1E010_RUNNER_FAILED", "runner failed")
        candidate = attempt_root / "runner-out" / f"{self.block_id.lower()}-{slot.run_ordinal:02d}-{slot.slot_id.lower()}"
        attempt_end_us = time.time_ns() // 1000
        if _is_strict51_block(getattr(self, "block_id", "")):
            # ★strict51: 三后端 query 先于 direct review 落 journal(A10.1 要求 SCIENTIFIC_ATTRITION
            #   的 query 3/3 在 slot 证据内闭合; 观察窗仍只罩 runner 调用, 不受此顺序影响)。
            query_count, checksum_ok, identity_ok = self._common_review(
                time.monotonic(), attempt_root=attempt_root, window_us=(attempt_start_us, attempt_end_us))
            review = review_fault_candidate(candidate, spec, self.repo_root)
        else:
            review = review_fault_candidate(candidate, spec, self.repo_root)
            # This is deliberately a postflight backend-health check, not a claim
            # that these queries sampled the legacy runner's scientific window.
            query_count, checksum_ok, identity_ok = self._common_review(time.monotonic())
        return SlotReview(slot.slot_id, review, 1, spec.fault_instance_arity, query_count, checksum_ok, identity_ok, True)

    def run_control(self, slot: B1Slot, action: Mapping[str, Any], attempt_root: Path) -> SlotReview:
        attempt_start_us = time.time_ns() // 1000
        _write_new(attempt_root / "control-action.json", action)
        before = self._catalog_control_state()
        control_id = str(action.get("control_id"))
        expected_env_keys = {
            "NF_START": (), "NF_MID": (), "NF_END": (),
            "SHAM_EXCEPTION": ("FAULT_RAISE-",),
            "SHAM_COMBINED": ("FAULT_RAISE-", "FAULT_DELAY_MS-"),
            "SHAM_DELAY": ("FAULT_DELAY_MS-",),
        }.get(control_id)
        if expected_env_keys is None or action.get("requested_env_unsets") != list(expected_env_keys):
            _fail("B1E004_CONTROL_INVALID", "unknown control action")
        for env_key in expected_env_keys:
            cp = self.smoke._run((
                self.smoke._kubectl, "-n", "recweb-chaos", "set", "env", "deployment/catalog", env_key,
            ), 45)
            if cp.returncode != 0:
                _fail("B1E004_CONTROL_INVALID", f"sham env-unset failed: {env_key}")
        if expected_env_keys:
            rollout = self.smoke._run((
                self.smoke._kubectl, "-n", "recweb-chaos", "rollout", "status", "deployment/catalog", "--timeout=60s",
            ), 75)
            if rollout.returncode != 0:
                _fail("B1E004_CONTROL_INVALID", "sham rollout/readback failed")
        after = self._catalog_control_state()
        if after != before:
            _fail("B1E004_CONTROL_INVALID", "sham changed deployment or pod identity")
        started = time.monotonic()
        # ★clock-quantum fix(2026-08-17, S51-B1 slot57 事故): Windows 的 time.monotonic
        #   分辨率 15.625ms(GetTickCount64 量子), time.sleep 可在单调钟刻度尚未跨过
        #   offered 死线时醒来 → workload_driver 的严格 started_at>=offered_at 守卫偶发
        #   ValueError(6 控制×30 检查命中 1 次)。给 sleeper 加 20ms 超睡(>1 个量子)使
        #   醒来时刻必然越过死线; 控制负载节拍 1s, 20ms 偏移对其判据(all-200)无影响。
        workload = run_open_loop(count=30, interval_seconds=1.0, clock=time.monotonic,
                                 sleeper=lambda delay: time.sleep(delay + 0.02),
                                 submit=self.smoke.submit_workload)
        if len(workload) != 30 or any(row.timeout or row.status != "200" for row in workload):
            _fail("B1E010_RUNNER_FAILED", "control workload failed")
        attempt_end_us = time.time_ns() // 1000
        if _is_strict51_block(getattr(self, "block_id", "")):
            query_count, checksum_ok, identity_ok = self._common_review(
                started, attempt_root=attempt_root, window_us=(attempt_start_us, attempt_end_us))
        else:
            query_count, checksum_ok, identity_ok = self._common_review(started)
        facts = {
            "slot_id": slot.slot_id, "action": dict(action), "workload_count": len(workload),
            "query_count": query_count, "catalog_identity_sha256": hashlib.sha256(_canonical(after)).hexdigest(),
        }
        return SlotReview(slot.slot_id, hashlib.sha256(_canonical(facts)).hexdigest(), 0, 0, query_count, checksum_ok, identity_ok, True)


def _strict51_query_closure(attempt_root: Path) -> bool:
    """slot 内三后端 query journal 是否 3/3 final_status=value(A10.1 query_closed)。"""
    journal = Path(attempt_root) / "query-journal"
    for query_id in ("prometheus.up", "jaeger.traces", "loki.logs"):
        summary_path = journal / query_id / "summary.json"
        if not summary_path.is_file():
            return False
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8", errors="strict"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return False
        if type(summary) is not dict or summary.get("final_status") != "value":
            return False
    return True


def _strict51_gt_closure(attempt_root: Path, slot: B1Slot, block_id: str) -> bool:
    """runner-out 唯一 case 目录内 metadata/groundtruth 是否齐(A10.1 gt_closed)。"""
    case_dir = Path(attempt_root) / "runner-out" / f"{block_id.lower()}-{slot.run_ordinal:02d}-{slot.slot_id.lower()}"
    return (case_dir / "metadata.json").is_file() and (case_dir / "groundtruth.json").is_file()


def _strict51_freeze_report_sha256(backend: "LocalB1Backend") -> str | None:
    """SHA of the frozen A10 report, read from its fixed path when present."""
    try:
        path = Path(backend.repo_root) / STRICT51_FREEZE_REPORT_RELATIVE_PATH
        if path.is_file():
            return hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception:
        pass
    return None


def _strict51_failure_extensions(backend: "LocalB1Backend", frozen_identity: str, code: str,
                                 slot: B1Slot, attempt_root: Path, block_id: str) -> dict[str, Any]:
    """A10.1: failure.json 的 exact 扩展字段(classification/post_failure_audit/closures/resume_allowed)。

    一次性失败后安全复核; 任何复核环节自身异常 → 对应 closure=False(resume_allowed 恒 fail-closed)。"""
    if code == "B1E006_REVIEW_FAILED" and slot.slot_type == "fault":
        classification = "SCIENTIFIC_ATTRITION"
    elif code in {"B1E008_QUERY_FAILED", "B1E010_RUNNER_FAILED", "B1E012_UNEXPECTED_FAILURE"} and slot.slot_type == "fault":
        classification = "TECHNICAL_FAILURE"
    else:
        classification = "SAFETY_FAILURE"
    audit: dict[str, Any] = {
        "chaos_residual_count": None, "foreign_resource_count": None,
        "deployments_ready": None, "deployments_desired": None,
        "prometheus_ready": None, "jaeger_ready": None, "loki_ready": None,
        "items_checksum": None, "inventory_checksum": None,
    }
    identity_ok = False
    try:
        snapshot = backend.smoke.preflight(backend.request)
        audit.update({
            "chaos_residual_count": len(snapshot.foreign_residuals),
            "foreign_resource_count": len(snapshot.foreign_residuals),
            "deployments_ready": sum(row.ready for row in snapshot.deployments),
            "deployments_desired": sum(row.desired for row in snapshot.deployments),
            "prometheus_ready": snapshot.backends.get("prometheus") == "ready",
            "jaeger_ready": snapshot.backends.get("jaeger") == "ready",
            "loki_ready": snapshot.backends.get("loki") == "ready",
            "items_checksum": snapshot.db_checksums.get("items"),
            "inventory_checksum": snapshot.db_checksums.get("inventory"),
        })
        # ★复审修复(2026-08-17): 必须用协议感知的 runner 期望(backend.smoke 上已由
        # preflight 注入), 否则 strict51 下 identity 复核恒 False → resume_allowed 恒 False
        # → 科学 attrition 永远无法续跑(每次 gate 失败都会假性硬停 epoch)。
        identity_ok = validate_preflight(
            backend.request, snapshot,
            expected_runner_sha256=getattr(
                backend.smoke, "expected_runner_sha256", EXPECTED_RUNNER_SHA256),
        ) == frozen_identity
    except Exception:
        pass
    gt_closed = _strict51_gt_closure(attempt_root, slot, block_id)
    query_closed = _strict51_query_closure(attempt_root)
    cleanup_closed = bool(
        audit["chaos_residual_count"] == 0 and audit["foreign_resource_count"] == 0)
    checksum_closed = bool(
        audit["items_checksum"] == EXPECTED_DB_CHECKSUMS.get("items")
        and audit["inventory_checksum"] == EXPECTED_DB_CHECKSUMS.get("inventory"))
    resume_allowed = bool(
        classification == "SCIENTIFIC_ATTRITION"
        and gt_closed and query_closed and cleanup_closed
        and checksum_closed and identity_ok)
    return {
        "classification": classification,
        "freeze_report_sha256": _strict51_freeze_report_sha256(backend),
        "environment_identity_sha256": (frozen_identity if identity_ok else None),
        "post_failure_audit": audit,
        "gt_closed": gt_closed,
        "query_closed": query_closed,
        "cleanup_closed": cleanup_closed,
        "checksum_closed": checksum_closed,
        "identity_closed": identity_ok,
        "resume_allowed": resume_allowed,
    }


def run_block(
    repo_root: Path,
    dataset_root: Path,
    backend: QualificationBackend,
    block_id: str,
) -> tuple[SlotReview, ...]:
    specs = build_scenario_specs(
        repo_root,
        include_deferred=block_id == DEFERRED_BLOCK_ID,
        protocol=("strict51" if _is_strict51_block(block_id) else "legacy"),
    )
    slots = build_block_slots(repo_root, block_id)
    frozen_identity = backend.preflight()
    attempts = dataset_root / ".qualification" / block_id / ".attempts"
    reviews = []
    for slot in slots:
        stem = f"{slot.run_ordinal:02d}-{slot.slot_id.lower()}"
        source = attempts / f"{stem}.tmp"; destination = attempts / stem
        if source.exists() and destination.exists():
            _fail("B1E005_FIRST_ATTEMPT_ONLY", f"ambiguous slot state: {slot.run_ordinal}")
        if destination.exists():
            reviews.append(_read_existing_review(destination, slot))
            continue
        if source.exists():
            _read_existing_failure(source, slot)
            continue
        source.mkdir(parents=True, exist_ok=False)
        try:
            if slot.slot_type == "control":
                action = execute_control_action(slot.slot_id)
                review = backend.run_control(slot, action, source)
                if review.runner_invocations != 0 or review.fault_calls != 0:
                    _fail("B1E004_CONTROL_INVALID", "control used fault surface")
            else:
                spec = specs[_scenario_id_for_slot(slot, block_id)]
                argv = build_runner_argv(repo_root, source, slot, spec, block_id=block_id)
                review = backend.run_fault(slot, spec, argv, source)
                if review.runner_invocations != 1 or review.fault_calls != spec.fault_instance_arity:
                    _fail("B1E006_REVIEW_FAILED", "fault execution count differs")
            if review.query_count != 3 or not all((review.checksum_zero_drift, review.identity_unchanged, review.cleanup_clear)):
                _fail("B1E006_REVIEW_FAILED", "slot direct review incomplete")
            _write_new(source / "outcome.json", asdict(review))
            os.rename(source, destination)
            reviews.append(review)
        except Exception as exc:
            token = os.environ.get("LITE_SMOKE_USER_TOKEN", "")
            db_password = os.environ.get("DB_PASSWORD", "")
            if isinstance(exc, B1Error):
                code = exc.code
                reason = str(exc)
            else:
                code = "B1E012_UNEXPECTED_FAILURE"
                reason = exc.__class__.__name__
            if token:
                reason = reason.replace(token, "<redacted>")
            if db_password:
                reason = reason.replace(db_password, "<redacted>")
            failure_payload: dict[str, Any] = {
                "code": code,
                "reason": reason[:512],
                "slot": {"run_ordinal": slot.run_ordinal, "slot_id": slot.slot_id},
            }
            strict51_continue = False
            if _is_strict51_block(block_id):
                # ★strict51 A10.1: 失败永久保留 + exact 扩展字段; 只有 SCIENTIFIC_ATTRITION 且
                #   全 closure 闭合才允许同 identity 继续下一未运行 slot(原失败永不算 primary 分子);
                #   技术/安全失败 hard-stop, 由 launcher -Resume 在同一冻结 identity 下跳过已有失败。
                extensions = _strict51_failure_extensions(
                    backend, frozen_identity, code, slot, source, block_id)
                failure_payload.update(extensions)
                strict51_continue = bool(
                    extensions["classification"] == "SCIENTIFIC_ATTRITION"
                    and extensions["resume_allowed"] is True)
            _write_new(source / "failure.json", failure_payload)
            if strict51_continue:
                continue
            if (
                block_id == DEFERRED_BLOCK_ID
                and slot.slot_type == "fault"
                and code in {"B1E006_REVIEW_FAILED", "B1E008_QUERY_FAILED", "B1E010_RUNNER_FAILED"}
            ):
                if backend.preflight() != frozen_identity:
                    _fail("B1E009_POSTCHECK_FAILED", "post-failure environment identity differs")
                continue
            raise
    return tuple(reviews)


def run_b1(repo_root: Path, dataset_root: Path, backend: QualificationBackend) -> tuple[SlotReview, ...]:
    """Compatibility wrapper for the exact already-collected B1 block."""
    return run_block(repo_root, dataset_root, backend, "B1")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one exact traditional-v2-lite block once, serially")
    parser.add_argument("--qualification-block", choices=SUPPORTED_BLOCK_IDS)
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--dataset-root", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(tuple(sys.argv[1:] if argv is None else argv))
    if args.qualification_block not in SUPPORTED_BLOCK_IDS or args.yes is not True:
        print("B1E011_NOT_AUTHORIZED: requires an exact supported block and --yes", file=sys.stderr)
        return 2
    repo_root = Path(args.repo_root).resolve(strict=False); dataset_root = Path(args.dataset_root).resolve(strict=False)
    if not repo_root.is_absolute() or not dataset_root.is_absolute():
        print("B1E011_NOT_AUTHORIZED: roots must be absolute", file=sys.stderr); return 2
    try:
        reviews = run_block(
            repo_root,
            dataset_root,
            LocalB1Backend(repo_root, dataset_root, args.qualification_block),
            args.qualification_block,
        )
    except B1Error as exc:
        print(str(exc), file=sys.stderr); return 2
    attempts = dataset_root / ".qualification" / args.qualification_block / ".attempts"
    failure_count = len(tuple(attempts.glob("*.tmp/failure.json"))) if attempts.is_dir() else 0
    print(_canonical({
        "block_id": args.qualification_block,
        "failure_count": failure_count,
        "slot_count": len(reviews),
        "reviews": [asdict(row) for row in reviews],
    }).decode(), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "B1Error", "B1Slot", "DEFERRED_BLOCK_ID", "FROZEN_B1_ORDER", "FROZEN_B6_SCENARIO_ORDER",
    "FROZEN_B6_SCHEDULE_SHA256", "FROZEN_BLOCK_HASHES", "LocalB1Backend", "SUPPORTED_BLOCK_IDS",
    "QualificationBackend", "ScenarioCommandSpec", "SlotReview", "build_b1_slots",
    "build_block_slots", "build_runner_argv", "build_scenario_specs", "execute_control_action",
    "review_fault_candidate", "run_b1", "run_block",
]
