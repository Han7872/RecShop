"""Pure offline contract and schedule for traditional-v2-lite.

This module intentionally does not import the legacy runner, Kubernetes,
requests, sockets, subprocess, or any delivery code.  Lite-0 proves only that
the reduced roster and seeded randomized-block schedule are closed and
reproducible.  It does *not* authorize live collection.

The schedule distinguishes design-time fault-leg family (single/dual/triple)
from the delivery-time service-level G.  ``planned_service_arity`` is therefore
required to remain ``None`` until actual ground truth is observed and audited.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Sequence


CONTRACT_SCHEMA_NAME = "traditional_v2_lite_contract"
CONTRACT_SCHEMA_VERSION = "1.0.0"
SCHEDULE_SCHEMA_NAME = "traditional_v2_lite_schedule"
SCHEDULE_SCHEMA_VERSION = "1.0.0"
PROTOCOL_ID = "recshop-traditional-v2-lite"
PROTOCOL_VERSION = "0.1.0"
ROSTER_VERSION = "traditional-v2-lite-roster/1.0.0"
CONTRACT_RELATIVE_PATH = (
    "docs/acceptance/contracts/traditional-v2-lite-20260813/"
    "lite0-contract.json"
)
CANONICALIZATION_ID = "json-utf8-sort-keys-compact-v1"
ORDER_ALGORITHM = "sha256-key-sort-v1"
DEFAULT_SEED = "recshop-traditional-v2-lite-20260813-rcbd-v1"
FROZEN_BLOCK_IDS = ("B1", "B2", "B3", "B4", "B5")
CONTROL_POSITIONS = (1, 10, 19, 28, 37, 46)


class LiteContractError(ValueError):
    """Stable, machine-testable rejection from the Lite-0 contract."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> "None":
    raise LiteContractError(code, message)


def _scenario_id(prefix: str, number: int) -> str:
    return f"{prefix}{number:02d}"


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    scenario_id: str
    fault_leg_family: str
    fault_instance_arity: int
    runner_fault: str
    runner_target_service: str | None
    disposition: str
    disposition_reason: str
    planned_service_arity: None = None
    identity_origin: str = "catalog_and_protocol"

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "fault_leg_family": self.fault_leg_family,
            "fault_instance_arity": self.fault_instance_arity,
            "runner_fault": self.runner_fault,
            "runner_target_service": self.runner_target_service,
            "disposition": self.disposition,
            "disposition_reason": self.disposition_reason,
            "planned_service_arity": self.planned_service_arity,
            "identity_origin": self.identity_origin,
        }


_RUNNER_FAULTS = {
    "S01": "net_delay_single",
    "S02": "net_loss_single",
    "S03": "pod_failure_single",
    "S04": "service_cpu_single",
    "S05": "host_cpu_single",
    "S06": "db_lock_single",
    "S07": "runtime_exception_single",
    "S08": "catalog_latency_single",
    **{_scenario_id("S", index): "service_cpu_single" for index in range(9, 14)},
    **{_scenario_id("S", index): "pod_failure_single" for index in range(14, 20)},
    "S20": "service_cpu_single",
    "S21": "net_delay_single",
    "S22": "pod_failure_single",
    "D01": "dual_timeout_retry",
    "D02": "net_delay_x_net_loss",
    "D03": "host_cpu_x_svccpu",
    "D04": "dual_podfail_staggered",
    "D05": "net_delay_x_cfg_connect",
    "D06": "host_cpu_x_cfg_timeout",
    "D07": "net_delay_x_inv_latency",
    "D08": "net_delay_x_podfail",
    "D09": "sasrec_cpu_x_catalog_netdelay",
    "D10": "db_lock_x_netdelay",
    "D11": "inv_latency_x_runtime_exc",
    "D12": "catalog_latency_x_cfg_timeout",
    "D13": "catalog_latency_x_net_loss",
    "D14": "net_delay_x_svc_cpu",
    "D15": "catalog_latency_x_svc_cpu",
    "D16": "pod_failure_x_net_delay",
    "D17": "checkout_podfail_x_inv_latency",
    "D18": "cart_cpu_x_order_cpu",
    "D19": "search_podfail_x_reviewquery_cpu",
    "D20": "recagent_cpu_x_backend_cpu",
    "D21": "user_podfail_x_backend_cpu",
    "T01": "pricing_cpu_x_catalog_latency_x_cfg_timeout",
    "T02": "inv_latency_x_cfg_timeout_x_retry",
    "T03": "net_delay_x_net_loss_x_db_lock",
    "T04": "pod_failure_x_catalog_latency_x_cfg_timeout",
    "T05": "checkout_podfail_x_cart_cpu_x_pricing_cpu",
    "T06": "backend_cpu_x_sasrec_cpu_x_gw_netdelay",
    "T07": "recagent_netdelay_x_sasrec_cpu_x_catalog_podfail",
    "T08": "order_podfail_x_reviewquery_cpu_x_catalog_cpu",
}

_SINGLE_TARGETS = {
    "S01": "catalog",
    "S02": "catalog",
    "S03": "catalog",
    "S04": "catalog",
    "S05": None,
    "S06": None,
    "S07": "catalog",
    "S08": "catalog",
    "S09": "order",
    "S10": "cart",
    "S11": "review-query",
    "S12": "backend",
    "S13": "checkout",
    "S14": "order",
    "S15": "cart",
    "S16": "review-query",
    "S17": "backend",
    "S18": "checkout",
    "S19": "search",
    "S20": "rec-agent",
    "S21": "rec-agent",
    "S22": "rec-agent",
}

_DEFERRED = frozenset(
    {
        "S07",
        "S08",
        "D07",
        "D11",
        "D12",
        "D13",
        "D15",
        "D17",
        "T01",
        "T02",
        "T04",
    }
)
_AUXILIARY = frozenset({"D06"})


def _make_scenario(scenario_id: str) -> ScenarioSpec:
    prefix = scenario_id[0]
    family = {"S": "single", "D": "dual", "T": "triple"}[prefix]
    arity = {"S": 1, "D": 2, "T": 3}[prefix]
    if scenario_id in _DEFERRED:
        disposition = "deferred"
        reason = "stable-pod-required"
    elif scenario_id in _AUXILIARY:
        disposition = "auxiliary"
        reason = "config-state-only"
    else:
        disposition = "strict"
        reason = "lite-v1-in-scope"
    return ScenarioSpec(
        scenario_id=scenario_id,
        fault_leg_family=family,
        fault_instance_arity=arity,
        runner_fault=_RUNNER_FAULTS[scenario_id],
        runner_target_service=_SINGLE_TARGETS.get(scenario_id),
        disposition=disposition,
        disposition_reason=reason,
        planned_service_arity=None,
        identity_origin=(
            "lite-stable-recagent-id"
            if scenario_id in {"S20", "S21", "S22"}
            else "catalog_and_protocol"
        ),
    )


FROZEN_SCENARIO_UNIVERSE = tuple(
    _make_scenario(_scenario_id(prefix, number))
    for prefix, upper in (("S", 22), ("D", 21), ("T", 8))
    for number in range(1, upper + 1)
)
_SPEC_BY_ID = MappingProxyType(
    {spec.scenario_id: spec for spec in FROZEN_SCENARIO_UNIVERSE}
)
STRICT_SCENARIO_IDS = tuple(
    spec.scenario_id
    for spec in FROZEN_SCENARIO_UNIVERSE
    if spec.disposition == "strict"
)
AUXILIARY_SCENARIO_IDS = tuple(
    spec.scenario_id
    for spec in FROZEN_SCENARIO_UNIVERSE
    if spec.disposition == "auxiliary"
)
DEFERRED_SCENARIO_IDS = tuple(
    spec.scenario_id
    for spec in FROZEN_SCENARIO_UNIVERSE
    if spec.disposition == "deferred"
)
ACTIVE_SCENARIO_IDS = tuple(
    spec.scenario_id
    for spec in FROZEN_SCENARIO_UNIVERSE
    if spec.disposition != "deferred"
)

_CONTROL_ROTATIONS = MappingProxyType(
    {
        "B1": (
            "NF_START",
            "SHAM_EXCEPTION",
            "SHAM_COMBINED",
            "NF_MID",
            "SHAM_DELAY",
            "NF_END",
        ),
        "B2": (
            "NF_START",
            "NF_MID",
            "SHAM_DELAY",
            "SHAM_EXCEPTION",
            "SHAM_COMBINED",
            "NF_END",
        ),
        "B3": (
            "NF_START",
            "SHAM_COMBINED",
            "NF_MID",
            "SHAM_DELAY",
            "SHAM_EXCEPTION",
            "NF_END",
        ),
        "B4": (
            "NF_START",
            "SHAM_DELAY",
            "SHAM_EXCEPTION",
            "NF_MID",
            "SHAM_COMBINED",
            "NF_END",
        ),
        "B5": (
            "NF_START",
            "SHAM_EXCEPTION",
            "NF_MID",
            "SHAM_COMBINED",
            "SHAM_DELAY",
            "NF_END",
        ),
    }
)

_SOURCE_BINDINGS = (
    {
        "path": "docs/blackboard/archive/TASK-K8S-traditional-v2-lite-full-2026-08-14-pre-compaction.md",
        "mode": "prefix",
        "byte_count": 8931,
        "sha256": "cd08f27da39922ad5cf92ee9864f777e86050b29505b61c3548a782f6b1b4702",
    },
    {
        "path": "docs/fault-catalog/single-root-catalog.md",
        "mode": "exact",
        "byte_count": 21210,
        "sha256": "289b193768cdcf78319d03b71e09fcdbff7d2cce89c86674749b36cbb427370f",
    },
    {
        "path": "docs/fault-catalog/dual-root-catalog.md",
        "mode": "exact",
        "byte_count": 22167,
        "sha256": "ebd95df271e34ddf595e506d39e53c2fb82830e578e62ae07e5d1333339a7745",
    },
    {
        "path": "docs/fault-catalog/triple-root-catalog.md",
        "mode": "exact",
        "byte_count": 16945,
        "sha256": "d146d2a19c3a249c2d4ff4bfb50db2769c26c10654e3dc4db64ccb743241cf2d",
    },
    {
        "path": "scripts/chaos/ctk/chaos_k8s_runner.py",
        "mode": "exact",
        "byte_count": 1477801,
        "sha256": "ae2963c2ea319744aa8bb00b8a6a8abb788aef8a358e1f753e13ab1027f71663",
    },
)


@dataclass(frozen=True, slots=True)
class FreezeIdentity:
    runner_sha256: str
    lite_contract_sha256: str
    workload_config_sha256: str
    query_registry_sha256: str
    threshold_registry_sha256: str
    image_manifest_sha256: str
    schema_bundle_sha256: str
    environment_contract_sha256: str

    def __post_init__(self) -> None:
        for name, value in self.to_dict().items():
            _validate_sha256(value, name)
            if value == "0" * 64:
                _fail("L0C008_SOURCE_HASH_DRIFT", f"{name} cannot be a placeholder")

    def to_dict(self) -> dict[str, str]:
        return {
            "runner_sha256": self.runner_sha256,
            "lite_contract_sha256": self.lite_contract_sha256,
            "workload_config_sha256": self.workload_config_sha256,
            "query_registry_sha256": self.query_registry_sha256,
            "threshold_registry_sha256": self.threshold_registry_sha256,
            "image_manifest_sha256": self.image_manifest_sha256,
            "schema_bundle_sha256": self.schema_bundle_sha256,
            "environment_contract_sha256": self.environment_contract_sha256,
        }


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        _fail("L0C001_SCHEMA_INVALID", f"non-canonical JSON value: {exc}")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _validate_sha256(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or re.fullmatch(r"[0-9a-f]{64}", value) is None
    ):
        _fail("L0C008_SOURCE_HASH_DRIFT", f"{field} must be lowercase SHA-256")
    return value


def _expect_exact_keys(value: Mapping[str, Any], expected: set[str], context: str) -> None:
    actual = set(value)
    if actual != expected:
        _fail(
            "L0C001_SCHEMA_INVALID",
            f"{context} missing={sorted(expected - actual)!r} extra={sorted(actual - expected)!r}",
        )


def _validate_source_binding_paths(bindings: Any) -> None:
    if not isinstance(bindings, list):
        _fail("L0C001_SCHEMA_INVALID", "source_bindings must be a list")
    for ordinal, binding in enumerate(bindings, start=1):
        if not isinstance(binding, Mapping):
            _fail("L0C001_SCHEMA_INVALID", f"source binding {ordinal} must be an object")
        path = binding.get("path")
        if type(path) is not str or not path or "\\" in path or "\x00" in path:
            _fail(
                "L0C009_SECRET_OR_ABSOLUTE_PATH_IN_CONTRACT",
                f"source binding {ordinal} path is not a canonical relative path",
            )
        pure = Path(path)
        if pure.is_absolute() or path.startswith("/") or any(part in {"", ".", ".."} for part in pure.parts):
            _fail(
                "L0C009_SECRET_OR_ABSOLUTE_PATH_IN_CONTRACT",
                f"source binding {ordinal} path escapes the contract root",
            )


def build_contract_document() -> dict[str, Any]:
    scenarios = [spec.to_dict() for spec in FROZEN_SCENARIO_UNIVERSE]
    core: dict[str, Any] = {
        "schema_name": CONTRACT_SCHEMA_NAME,
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "protocol_id": PROTOCOL_ID,
        "protocol_version": PROTOCOL_VERSION,
        "roster_version": ROSTER_VERSION,
        "canonicalization_id": CANONICALIZATION_ID,
        "execution_status": "offline-contract-only",
        "live_authorized": False,
        "formal_claim_allowed": False,
        "source_bindings": [dict(item) for item in _SOURCE_BINDINGS],
        "expected_counts": {
            "universe": 51,
            "strict": 39,
            "auxiliary": 1,
            "deferred": 11,
            "active_per_block": 40,
            "controls_per_block": 6,
            "slots_per_block": 46,
        },
        "control_contract": {
            "positions": list(CONTROL_POSITIONS),
            "execution_adapter_status": "required-not-implemented",
            "fault_mapping_allowed": False,
            "per_block": 6,
            "rotations": {
                block_id: list(rotation)
                for block_id, rotation in _CONTROL_ROTATIONS.items()
            },
        },
        "scenarios": scenarios,
    }
    document = dict(core)
    document["contract_sha256"] = canonical_sha256(core)
    validate_contract_document(document)
    return document


def validate_contract_document(document: Mapping[str, Any]) -> None:
    if not isinstance(document, Mapping):
        _fail("L0C001_SCHEMA_INVALID", "contract must be an object")
    root_keys = {
        "schema_name",
        "schema_version",
        "protocol_id",
        "protocol_version",
        "roster_version",
        "canonicalization_id",
        "execution_status",
        "live_authorized",
        "formal_claim_allowed",
        "source_bindings",
        "expected_counts",
        "control_contract",
        "scenarios",
        "contract_sha256",
    }
    _expect_exact_keys(document, root_keys, "contract")
    if document["schema_name"] != CONTRACT_SCHEMA_NAME:
        _fail("L0C001_SCHEMA_INVALID", "unknown contract schema")
    if document["schema_version"] != CONTRACT_SCHEMA_VERSION:
        _fail("L0C001_SCHEMA_INVALID", "unknown contract version")
    if document["protocol_id"] != PROTOCOL_ID or document["protocol_version"] != PROTOCOL_VERSION:
        _fail("L0C001_SCHEMA_INVALID", "protocol identity mismatch")
    if document["roster_version"] != ROSTER_VERSION:
        _fail("L0C001_SCHEMA_INVALID", "roster identity mismatch")
    if document["canonicalization_id"] != CANONICALIZATION_ID:
        _fail("L0C001_SCHEMA_INVALID", "canonicalization identity mismatch")
    if document["execution_status"] != "offline-contract-only":
        _fail("L0C001_SCHEMA_INVALID", "Lite-0 execution status was elevated")
    if document["live_authorized"] is not False or document["formal_claim_allowed"] is not False:
        _fail("L0C010_CONTROL_FAULT_MAPPING_FORBIDDEN", "Lite-0 cannot authorize live/formal")
    _validate_source_binding_paths(document["source_bindings"])

    scenarios = document["scenarios"]
    if not isinstance(scenarios, list):
        _fail("L0C001_SCHEMA_INVALID", "scenarios must be a list")
    scenario_keys = set(ScenarioSpec.__dataclass_fields__)
    normalized: list[Mapping[str, Any]] = []
    for ordinal, item in enumerate(scenarios, start=1):
        if not isinstance(item, Mapping):
            _fail("L0C001_SCHEMA_INVALID", f"scenario {ordinal} must be an object")
        _expect_exact_keys(item, scenario_keys, f"scenario {ordinal}")
        if type(item["scenario_id"]) is not str:
            _fail("L0C001_SCHEMA_INVALID", f"scenario {ordinal} has invalid ID")
        normalized.append(item)
    ids = [item["scenario_id"] for item in normalized]
    expected_scenarios = [spec.to_dict() for spec in FROZEN_SCENARIO_UNIVERSE]
    expected_ids = [item["scenario_id"] for item in expected_scenarios]
    if len(ids) != len(set(ids)) or set(ids) != set(expected_ids):
        _fail(
            "L0C002_DUPLICATE_OR_MISSING_ID",
            "scenario IDs must be the exact unique frozen 51",
        )
    if ids != expected_ids:
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "scenario order differs from frozen roster")
    expected_by_id = {item["scenario_id"]: item for item in expected_scenarios}
    for item in normalized:
        expected_item = expected_by_id[item["scenario_id"]]
        if item["planned_service_arity"] is not None:
            _fail(
                "L0C007_DISPOSITION_POLICY_VIOLATION",
                "service-level G cannot be inferred at planning time",
            )
        if any(
            item[field] != expected_item[field]
            for field in ("disposition", "disposition_reason", "identity_origin")
        ):
            _fail(
                "L0C007_DISPOSITION_POLICY_VIOLATION",
                f"{item['scenario_id']} disposition policy drift",
            )
        if item["runner_fault"] != expected_item["runner_fault"]:
            _fail(
                "L0C005_UNKNOWN_RUNNER_FAULT",
                f"{item['scenario_id']} runner alias drift",
            )
        if item != expected_item:
            _fail(
                "L0C003_UNIVERSE_OR_COUNT_MISMATCH",
                f"{item['scenario_id']} semantic definition drift",
            )
    counts = {
        "universe": len(ids),
        "strict": sum(item["disposition"] == "strict" for item in normalized),
        "auxiliary": sum(item["disposition"] == "auxiliary" for item in normalized),
        "deferred": sum(item["disposition"] == "deferred" for item in normalized),
        "active_per_block": len(ACTIVE_SCENARIO_IDS),
        "controls_per_block": len(CONTROL_POSITIONS),
        "slots_per_block": len(ACTIVE_SCENARIO_IDS) + len(CONTROL_POSITIONS),
    }
    if document["expected_counts"] != counts:
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "contract counts differ from frozen values")
    controls = document["control_contract"]
    if controls != build_contract_document_core_controls():
        _fail("L0C010_CONTROL_FAULT_MAPPING_FORBIDDEN", "control contract mismatch")
    if document["source_bindings"] != [dict(item) for item in _SOURCE_BINDINGS]:
        _fail("L0C008_SOURCE_HASH_DRIFT", "source bindings differ from frozen prefix/files")
    _validate_sha256(document["contract_sha256"], "contract_sha256")
    core = copy.deepcopy(dict(document))
    del core["contract_sha256"]
    if document["contract_sha256"] != canonical_sha256(core):
        _fail("L0C008_SOURCE_HASH_DRIFT", "contract_sha256 does not match contract")


def build_contract_document_core_controls() -> dict[str, Any]:
    return {
        "positions": list(CONTROL_POSITIONS),
        "execution_adapter_status": "required-not-implemented",
        "fault_mapping_allowed": False,
        "per_block": 6,
        "rotations": {
            block_id: list(rotation)
            for block_id, rotation in _CONTROL_ROTATIONS.items()
        },
    }


class _DuplicateKeyDecoder(json.JSONDecoder):
    def __init__(self) -> None:
        super().__init__(object_pairs_hook=self._pairs, parse_constant=self._constant)

    @staticmethod
    def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("L0C001_SCHEMA_INVALID", f"duplicate JSON key: {key}")
            result[key] = value
        return result

    @staticmethod
    def _constant(value: str) -> "None":
        _fail("L0C001_SCHEMA_INVALID", f"non-finite JSON value: {value}")


def load_contract(path: os.PathLike[str] | str) -> Mapping[str, Any]:
    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        _fail("L0C001_SCHEMA_INVALID", f"cannot read contract: {exc.__class__.__name__}")
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        _fail("L0C001_SCHEMA_INVALID", "contract is not strict UTF-8")
    try:
        document = json.loads(text, cls=_DuplicateKeyDecoder)
    except LiteContractError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        _fail("L0C001_SCHEMA_INVALID", f"invalid JSON: {exc.__class__.__name__}")
    validate_contract_document(document)
    if raw != canonical_json_bytes(document) + b"\n":
        _fail(
            "L0C001_SCHEMA_INVALID",
            "contract bytes must be canonical compact JSON with one final LF",
        )
    return document


def _validate_block_id(block_id: Any) -> str:
    if type(block_id) is not str or block_id not in FROZEN_BLOCK_IDS:
        _fail("L0C001_SCHEMA_INVALID", f"unknown block_id: {block_id!r}")
    return block_id


def _validate_seed(seed: Any) -> str:
    if type(seed) is not str or not seed or seed != seed.strip() or "\x00" in seed:
        _fail("L0C001_SCHEMA_INVALID", "seed must be non-empty trimmed text")
    return seed


def _fault_sort_key(seed: str, block_id: str, scenario_id: str) -> tuple[bytes, str]:
    material = f"{seed}\x00{block_id}\x00{scenario_id}".encode("utf-8")
    return hashlib.sha256(material).digest(), scenario_id


def _fault_order(block_id: str, seed: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            ACTIVE_SCENARIO_IDS,
            key=lambda scenario_id: _fault_sort_key(seed, block_id, scenario_id),
        )
    )


def _build_block(block_id: str, seed: str) -> dict[str, Any]:
    block_id = _validate_block_id(block_id)
    order = _fault_order(block_id, seed)
    controls = dict(zip(CONTROL_POSITIONS, _CONTROL_ROTATIONS[block_id]))
    slots: list[dict[str, Any]] = []
    fault_index = 0
    for run_ordinal in range(1, 47):
        if run_ordinal in controls:
            slots.append(
                {
                    "run_ordinal": run_ordinal,
                    "slot_type": "control",
                    "control_id": controls[run_ordinal],
                    "control_kind": (
                        "no_fault" if controls[run_ordinal].startswith("NF_") else "sham"
                    ),
                }
            )
            continue
        scenario_id = order[fault_index]
        fault_index += 1
        spec = _SPEC_BY_ID[scenario_id]
        slots.append(
            {
                "run_ordinal": run_ordinal,
                "slot_type": "fault",
                "scenario_id": scenario_id,
                "fault_leg_family": spec.fault_leg_family,
                "fault_instance_arity": spec.fault_instance_arity,
                "planned_service_arity": None,
                "runner_fault": spec.runner_fault,
                "runner_target_service": spec.runner_target_service,
                "planned_track": spec.disposition,
                "track_detail": (
                    spec.disposition_reason if spec.disposition == "auxiliary" else None
                ),
            }
        )
    core: dict[str, Any] = {
        "block_id": block_id,
        "replicate_id": int(block_id[1:]),
        "slot_count": 46,
        "fault_slot_count": 40,
        "strict_slot_count": 39,
        "auxiliary_slot_count": 1,
        "control_slot_count": 6,
        "fault_order_hash": canonical_sha256(list(order)),
        "slots": slots,
    }
    block = dict(core)
    block["block_schedule_hash"] = canonical_sha256(core)
    return block


def build_schedule_manifest(
    identity: FreezeIdentity,
    *,
    seed: str = DEFAULT_SEED,
    block_ids: Sequence[str] = FROZEN_BLOCK_IDS,
    manual_orders: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    seed = _validate_seed(seed)
    if tuple(block_ids) != FROZEN_BLOCK_IDS:
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "blocks must be exact B1-B5")
    if manual_orders is not None:
        _fail("L0C001_SCHEMA_INVALID", "manual schedule overrides are forbidden")
    core: dict[str, Any] = {
        "schema_name": SCHEDULE_SCHEMA_NAME,
        "schema_version": SCHEDULE_SCHEMA_VERSION,
        "protocol_id": PROTOCOL_ID,
        "protocol_version": PROTOCOL_VERSION,
        "roster_version": ROSTER_VERSION,
        "canonicalization_id": CANONICALIZATION_ID,
        "order_algorithm": ORDER_ALGORITHM,
        "seed": seed,
        "block_ids": list(FROZEN_BLOCK_IDS),
        "identity": identity.to_dict(),
        "roster_hash": canonical_sha256(
            [
                _SPEC_BY_ID[scenario_id].to_dict()
                for scenario_id in ACTIVE_SCENARIO_IDS
            ]
        ),
        "deferred_scenario_ids": list(DEFERRED_SCENARIO_IDS),
        "blocks": [_build_block(block_id, seed) for block_id in FROZEN_BLOCK_IDS],
        "live_authorized": False,
        "formal_claim_allowed": False,
    }
    manifest = dict(core)
    manifest["schedule_hash"] = canonical_sha256(core)
    validate_schedule_manifest(manifest)
    return manifest


def validate_schedule_manifest(manifest: Mapping[str, Any]) -> None:
    if not isinstance(manifest, Mapping):
        _fail("L0C001_SCHEMA_INVALID", "schedule must be an object")
    expected_keys = {
        "schema_name",
        "schema_version",
        "protocol_id",
        "protocol_version",
        "roster_version",
        "canonicalization_id",
        "order_algorithm",
        "seed",
        "block_ids",
        "identity",
        "roster_hash",
        "deferred_scenario_ids",
        "blocks",
        "live_authorized",
        "formal_claim_allowed",
        "schedule_hash",
    }
    _expect_exact_keys(manifest, expected_keys, "schedule")
    if (
        manifest["schema_name"] != SCHEDULE_SCHEMA_NAME
        or manifest["schema_version"] != SCHEDULE_SCHEMA_VERSION
        or manifest["protocol_id"] != PROTOCOL_ID
        or manifest["protocol_version"] != PROTOCOL_VERSION
        or manifest["roster_version"] != ROSTER_VERSION
        or manifest["canonicalization_id"] != CANONICALIZATION_ID
        or manifest["order_algorithm"] != ORDER_ALGORITHM
    ):
        _fail("L0C001_SCHEMA_INVALID", "schedule identity mismatch")
    if manifest["live_authorized"] is not False or manifest["formal_claim_allowed"] is not False:
        _fail("L0C010_CONTROL_FAULT_MAPPING_FORBIDDEN", "Lite-0 schedule cannot authorize live/formal")
    seed = _validate_seed(manifest["seed"])
    if manifest["block_ids"] != list(FROZEN_BLOCK_IDS):
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "schedule block IDs mismatch")
    if manifest["deferred_scenario_ids"] != list(DEFERRED_SCENARIO_IDS):
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "deferred roster mismatch")
    expected_roster_hash = canonical_sha256(
        [_SPEC_BY_ID[scenario_id].to_dict() for scenario_id in ACTIVE_SCENARIO_IDS]
    )
    if manifest["roster_hash"] != expected_roster_hash:
        _fail("L0C008_SOURCE_HASH_DRIFT", "active roster hash mismatch")
    identity = manifest["identity"]
    if not isinstance(identity, Mapping):
        _fail("L0C001_SCHEMA_INVALID", "identity must be an object")
    expected_identity_keys = set(FreezeIdentity.__dataclass_fields__)
    _expect_exact_keys(identity, expected_identity_keys, "identity")
    FreezeIdentity(**dict(identity))
    blocks = manifest["blocks"]
    if not isinstance(blocks, list) or len(blocks) != 5:
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "schedule requires five blocks")
    for expected_block_id, block in zip(FROZEN_BLOCK_IDS, blocks):
        expected_block = _build_block(expected_block_id, seed)
        if block != expected_block:
            _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", f"{expected_block_id} schedule drift")
    _validate_sha256(manifest["schedule_hash"], "schedule_hash")
    core = copy.deepcopy(dict(manifest))
    del core["schedule_hash"]
    if manifest["schedule_hash"] != canonical_sha256(core):
        _fail("L0C008_SOURCE_HASH_DRIFT", "schedule_hash mismatch")


def extract_runner_fault_choices(runner_path: os.PathLike[str] | str) -> frozenset[str]:
    try:
        source = Path(runner_path).read_text(encoding="utf-8", errors="strict")
        tree = ast.parse(source)
    except (OSError, UnicodeError, SyntaxError) as exc:
        _fail("L0C005_UNKNOWN_RUNNER_FAULT", f"cannot parse runner: {exc.__class__.__name__}")
    matches: list[frozenset[str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "add_argument" or not node.args:
            continue
        try:
            first = ast.literal_eval(node.args[0])
        except (ValueError, TypeError):
            continue
        if first != "--fault":
            continue
        choices_node = next((kw.value for kw in node.keywords if kw.arg == "choices"), None)
        if choices_node is None:
            continue
        try:
            choices = ast.literal_eval(choices_node)
        except (ValueError, TypeError):
            _fail("L0C005_UNKNOWN_RUNNER_FAULT", "runner fault choices are not static literals")
        if not isinstance(choices, list) or not all(type(item) is str for item in choices):
            _fail("L0C005_UNKNOWN_RUNNER_FAULT", "runner fault choices are not a string list")
        matches.append(frozenset(choices))
    if len(matches) != 1:
        _fail("L0C005_UNKNOWN_RUNNER_FAULT", "runner must expose one static --fault choice list")
    return matches[0]


def verify_frozen_sources(repo_root: os.PathLike[str] | str) -> tuple[dict[str, Any], ...]:
    root = Path(repo_root).resolve()
    results: list[dict[str, Any]] = []
    for binding in _SOURCE_BINDINGS:
        candidate = (root / binding["path"]).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            _fail("L0C009_SECRET_OR_ABSOLUTE_PATH_IN_CONTRACT", "source escaped repo root")
        try:
            raw = candidate.read_bytes()
        except OSError as exc:
            _fail("L0C008_SOURCE_HASH_DRIFT", f"cannot read {binding['path']}: {exc.__class__.__name__}")
        expected_count = int(binding["byte_count"])
        if binding["mode"] == "exact":
            if len(raw) != expected_count:
                _fail("L0C008_SOURCE_HASH_DRIFT", f"{binding['path']} byte count drift")
            hashed = raw
        elif binding["mode"] == "prefix":
            if len(raw) < expected_count:
                _fail("L0C008_SOURCE_HASH_DRIFT", f"{binding['path']} lost frozen prefix")
            hashed = raw[:expected_count]
        else:
            _fail("L0C001_SCHEMA_INVALID", "unknown source binding mode")
        actual = hashlib.sha256(hashed).hexdigest()
        if actual != binding["sha256"]:
            _fail("L0C008_SOURCE_HASH_DRIFT", f"{binding['path']} SHA drift")
        results.append(
            {
                "path": binding["path"],
                "mode": binding["mode"],
                "byte_count": expected_count,
                "sha256": actual,
            }
        )
    runner = root / "scripts/chaos/ctk/chaos_k8s_runner.py"
    choices = extract_runner_fault_choices(runner)
    unknown = sorted(set(_RUNNER_FAULTS.values()) - choices)
    if unknown:
        _fail("L0C005_UNKNOWN_RUNNER_FAULT", f"runner lacks aliases: {unknown!r}")
    return tuple(results)


# ============================================================
# strict51 unified protocol (2026-08-16) — prospective only.
#
# Legacy protocol above stays byte-identical: the old artifact, the
# 39 strict + D06 auxiliary + 11 deferred roster, the 46-slot B1-B5 and
# 57-slot B6_DEFERRED schedules remain the historical authority.  The
# strict51 section below only adds a parallel, explicit opt-in protocol
# (51 strict scenarios x 5 complete randomized blocks = 255 fault slots
# + 30 controls).  Nothing in this section mutates legacy state.
# ============================================================
STRICT51_CONTRACT_SCHEMA_NAME = "traditional_v2_lite_strict51_contract"
STRICT51_CONTRACT_SCHEMA_VERSION = "1.0.0"
STRICT51_LOGICAL_SCHEMA_NAME = "traditional-v2-lite-strict51-logical-schedule"
STRICT51_LOGICAL_SCHEMA_VERSION = 1
STRICT51_PROTOCOL_ID = "traditional-v2-lite-strict51"
STRICT51_PROTOCOL_VERSION = "2026-08-17.v3"
STRICT51_ROSTER_VERSION = "traditional-v2-lite-strict51-roster/1.0.0"
STRICT51_CONTRACT_RELATIVE_PATH = (
    "docs/acceptance/contracts/traditional-v2-lite-strict51-20260816/"
    "strict51-contract.json"
)
STRICT51_CANONICALIZATION_ID = "utf8-sort-keys-compact-v1"
STRICT51_ORDER_ALGORITHM = "sha256-seed-block-scenario-v1"
STRICT51_DEFAULT_SEED = "recshop-traditional-v2-lite-strict51-20260816-rcbd-v1"
STRICT51_BLOCK_IDS = ("S51-B1", "S51-B2", "S51-B3", "S51-B4", "S51-B5")
STRICT51_CONTROL_POSITIONS = (1, 12, 23, 34, 45, 57)
STRICT51_PANEL_OWNER_SCENARIO_IDS = frozenset({"D06", "D12", "D13"})

# Independently frozen schedule hashes (acceptance A2).  The published
# acceptance text carries two transcription slips in the S51-B4/B5 block
# hashes (63 chars / a flipped last hex digit); the values below are the
# ones cryptographically implied by the frozen logical-schedule hash,
# which embeds all five block hashes, and are recorded in the acceptance
# erratum appendix written during implementation.
FROZEN_STRICT51_SCHEDULE = MappingProxyType({
    "S51-B1": (
        "eb91412d19e4d8b79a81e39dd6a289b838c75b29ceb413aaf735a2356d1f4092",
        "872b3b91655c576d86a74ec1dc84b09024f58e01ddc7de730cd2cb5426a4ab14",
    ),
    "S51-B2": (
        "576d0122442fb124529347b0c6389b60a032deafa66756ba71eef2383702a955",
        "dbc03f5e9b78c57475b901bc5b12183b0f6f920e866a4e3a1a5c441e969f81b7",
    ),
    "S51-B3": (
        "0cecafa63847b43d37dc43d49c1853786ed276c171038359f75953ec200df4df",
        "b96ac8080620473674da384d3f43920218b7e0c1305662911f3fbf3e68d6a21f",
    ),
    "S51-B4": (
        "e888251eb77e6958e99d01dcfc09422f5f6c812da7b5f2c87f22ddb4e0988828",
        "530f9181c784348a94e78ef44413d493f7fc057bfb0ae4f371daf43a117992aa",
    ),
    "S51-B5": (
        "3d53ccfdf0e785f066a6fbb50501f47ddd570fa070dea1169efbbc82dbf71b6f",
        "6432d58119ff5c0a907063740888baa5eb7eb185cbc9bf2473ae4fd234ea3c7c",
    ),
})
FROZEN_STRICT51_LOGICAL_SCHEDULE_SHA256 = (
    "db4c0dbe6e5f327e675bc31be2a05621a33d377feba67de10362dbf1211d8f48"
)

# strict51 source bindings are fixed at artifact-generation time (runner +
# query helper only: they never reference the artifact, so the SHA DAG stays
# acyclic; b1_lite.py and the launcher are frozen externally in the A10
# freeze report and must NOT appear here).
STRICT51_SOURCE_BINDINGS: tuple[dict[str, Any], ...] = ()


def _strict51_scenario(scenario_id: str) -> ScenarioSpec:
    prefix = scenario_id[0]
    family = {"S": "single", "D": "dual", "T": "triple"}[prefix]
    arity = {"S": 1, "D": 2, "T": 3}[prefix]
    return ScenarioSpec(
        scenario_id=scenario_id,
        fault_leg_family=family,
        fault_instance_arity=arity,
        runner_fault=_RUNNER_FAULTS[scenario_id],
        runner_target_service=_SINGLE_TARGETS.get(scenario_id),
        disposition="strict",
        disposition_reason="strict51-in-scope",
        planned_service_arity=None,
        identity_origin=(
            "lite-stable-recagent-id"
            if scenario_id in {"S20", "S21", "S22"}
            else "catalog_and_protocol"
        ),
    )


STRICT51_SCENARIO_UNIVERSE = tuple(
    _strict51_scenario(_scenario_id(prefix, number))
    for prefix, upper in (("S", 22), ("D", 21), ("T", 8))
    for number in range(1, upper + 1)
)
STRICT51_SCENARIO_IDS = tuple(spec.scenario_id for spec in STRICT51_SCENARIO_UNIVERSE)


def _validate_strict51_block_id(block_id: Any) -> str:
    if type(block_id) is not str or block_id not in STRICT51_BLOCK_IDS:
        _fail("L0C001_SCHEMA_INVALID", f"unknown strict51 block_id: {block_id!r}")
    return block_id


def _strict51_fault_sort_key(seed: str, block_id: str, scenario_id: str) -> tuple[str, str]:
    material = f"{seed}\x00{block_id}\x00{scenario_id}".encode("utf-8")
    return hashlib.sha256(material).hexdigest(), scenario_id


def _strict51_fault_order(block_id: str, seed: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            STRICT51_SCENARIO_IDS,
            key=lambda scenario_id: _strict51_fault_sort_key(seed, block_id, scenario_id),
        )
    )


def _build_strict51_block(block_id: str, seed: str) -> dict[str, Any]:
    block_id = _validate_strict51_block_id(block_id)
    order = _strict51_fault_order(block_id, seed)
    rotation = _CONTROL_ROTATIONS[f"B{block_id.split('-B')[1]}"]
    controls = dict(zip(STRICT51_CONTROL_POSITIONS, rotation))
    slots: list[dict[str, Any]] = []
    fault_index = 0
    for run_ordinal in range(1, 58):
        if run_ordinal in controls:
            slots.append(
                {
                    "run_ordinal": run_ordinal,
                    "slot_type": "control",
                    "control_id": controls[run_ordinal],
                    "control_kind": (
                        "no_fault" if controls[run_ordinal].startswith("NF_") else "sham"
                    ),
                }
            )
            continue
        scenario_id = order[fault_index]
        fault_index += 1
        spec = _strict51_scenario(scenario_id)
        slots.append(
            {
                "run_ordinal": run_ordinal,
                "slot_type": "fault",
                "scenario_id": scenario_id,
                "fault_leg_family": spec.fault_leg_family,
                "fault_instance_arity": spec.fault_instance_arity,
                "planned_service_arity": None,
                "runner_fault": spec.runner_fault,
                "runner_target_service": spec.runner_target_service,
                "planned_track": "strict",
                "track_detail": None,
            }
        )
    core: dict[str, Any] = {
        "block_id": block_id,
        "replicate_id": int(block_id.split("-B")[1]),
        "slot_count": 57,
        "fault_slot_count": 51,
        "strict_slot_count": 51,
        "auxiliary_slot_count": 0,
        "control_slot_count": 6,
        "fault_order_hash": canonical_sha256(list(order)),
        "slots": slots,
    }
    block = dict(core)
    block["block_schedule_hash"] = canonical_sha256(core)
    return block


def build_strict51_logical_schedule(
    *, seed: str = STRICT51_DEFAULT_SEED
) -> dict[str, Any]:
    """Exact A2 logical schedule: 5 x 57 slots, hash-stable, PRNG-free."""
    seed = _validate_seed(seed)
    if seed != STRICT51_DEFAULT_SEED:
        _fail("L0C001_SCHEMA_INVALID", "strict51 seed is frozen")
    core: dict[str, Any] = {
        "schema_name": STRICT51_LOGICAL_SCHEMA_NAME,
        "schema_version": STRICT51_LOGICAL_SCHEMA_VERSION,
        "protocol_id": STRICT51_PROTOCOL_ID,
        "protocol_version": STRICT51_PROTOCOL_VERSION,
        "canonicalization_id": STRICT51_CANONICALIZATION_ID,
        "order_algorithm": STRICT51_ORDER_ALGORITHM,
        "seed": seed,
        "block_ids": list(STRICT51_BLOCK_IDS),
        "blocks": [
            _build_strict51_block(block_id, seed) for block_id in STRICT51_BLOCK_IDS
        ],
    }
    manifest = dict(core)
    manifest["logical_schedule_hash"] = canonical_sha256(core)
    validate_strict51_logical_schedule(manifest)
    return manifest


def validate_strict51_logical_schedule(manifest: Mapping[str, Any]) -> None:
    if not isinstance(manifest, Mapping):
        _fail("L0C001_SCHEMA_INVALID", "strict51 schedule must be an object")
    expected_keys = {
        "schema_name",
        "schema_version",
        "protocol_id",
        "protocol_version",
        "canonicalization_id",
        "order_algorithm",
        "seed",
        "block_ids",
        "blocks",
        "logical_schedule_hash",
    }
    _expect_exact_keys(manifest, expected_keys, "strict51 schedule")
    if (
        manifest["schema_name"] != STRICT51_LOGICAL_SCHEMA_NAME
        or manifest["schema_version"] != STRICT51_LOGICAL_SCHEMA_VERSION
        or manifest["protocol_id"] != STRICT51_PROTOCOL_ID
        or manifest["protocol_version"] != STRICT51_PROTOCOL_VERSION
        or manifest["canonicalization_id"] != STRICT51_CANONICALIZATION_ID
        or manifest["order_algorithm"] != STRICT51_ORDER_ALGORITHM
    ):
        _fail("L0C001_SCHEMA_INVALID", "strict51 schedule identity mismatch")
    seed = _validate_seed(manifest["seed"])
    if seed != STRICT51_DEFAULT_SEED:
        _fail("L0C001_SCHEMA_INVALID", "strict51 seed is frozen")
    if manifest["block_ids"] != list(STRICT51_BLOCK_IDS):
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "strict51 block ids mismatch")
    blocks = manifest["blocks"]
    if not isinstance(blocks, list) or len(blocks) != 5:
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "strict51 requires five blocks")
    for expected_block_id, block in zip(STRICT51_BLOCK_IDS, blocks):
        expected_block = _build_strict51_block(expected_block_id, seed)
        if block != expected_block:
            _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", f"{expected_block_id} schedule drift")
        frozen = FROZEN_STRICT51_SCHEDULE[expected_block_id]
        if (
            block["fault_order_hash"] != frozen[0]
            or block["block_schedule_hash"] != frozen[1]
        ):
            _fail("L0C008_SOURCE_HASH_DRIFT", f"{expected_block_id} frozen hash mismatch")
    _validate_sha256(manifest["logical_schedule_hash"], "logical_schedule_hash")
    if manifest["logical_schedule_hash"] != FROZEN_STRICT51_LOGICAL_SCHEDULE_SHA256:
        _fail("L0C008_SOURCE_HASH_DRIFT", "strict51 logical schedule hash mismatch")
    core = copy.deepcopy(dict(manifest))
    del core["logical_schedule_hash"]
    if manifest["logical_schedule_hash"] != canonical_sha256(core):
        _fail("L0C008_SOURCE_HASH_DRIFT", "strict51 logical schedule hash mismatch")


def build_strict51_contract_document(
    source_bindings: Sequence[Mapping[str, Any]] = STRICT51_SOURCE_BINDINGS,
) -> dict[str, Any]:
    scenarios = [spec.to_dict() for spec in STRICT51_SCENARIO_UNIVERSE]
    core: dict[str, Any] = {
        "schema_name": STRICT51_CONTRACT_SCHEMA_NAME,
        "schema_version": STRICT51_CONTRACT_SCHEMA_VERSION,
        "protocol_id": STRICT51_PROTOCOL_ID,
        "protocol_version": STRICT51_PROTOCOL_VERSION,
        "roster_version": STRICT51_ROSTER_VERSION,
        "canonicalization_id": STRICT51_CANONICALIZATION_ID,
        "execution_status": "offline-contract-only",
        "live_authorized": False,
        "formal_claim_allowed": False,
        "seed": STRICT51_DEFAULT_SEED,
        "order_algorithm": STRICT51_ORDER_ALGORITHM,
        "source_bindings": [dict(item) for item in source_bindings],
        "expected_counts": {
            "universe": 51,
            "strict": 51,
            "auxiliary": 0,
            "deferred": 0,
            "blocks": 5,
            "faults_per_block": 51,
            "controls_per_block": 6,
            "slots_per_block": 57,
            "total_fault_slots": 255,
            "total_control_slots": 30,
            "total_slots": 285,
        },
        "control_contract": {
            "positions": list(STRICT51_CONTROL_POSITIONS),
            "per_block": 6,
            "rotations": {
                block_id: list(_CONTROL_ROTATIONS[f"B{block_id.split('-B')[1]}"])
                for block_id in STRICT51_BLOCK_IDS
            },
            "execution_adapter_status": "implemented-b1-lite-controls",
            "fault_mapping_allowed": False,
        },
        "scenarios": scenarios,
    }
    document = dict(core)
    document["contract_sha256"] = canonical_sha256(core)
    validate_strict51_contract_document(document)
    return document


def validate_strict51_contract_document(document: Mapping[str, Any]) -> None:
    if not isinstance(document, Mapping):
        _fail("L0C001_SCHEMA_INVALID", "strict51 contract must be an object")
    root_keys = {
        "schema_name",
        "schema_version",
        "protocol_id",
        "protocol_version",
        "roster_version",
        "canonicalization_id",
        "execution_status",
        "live_authorized",
        "formal_claim_allowed",
        "seed",
        "order_algorithm",
        "source_bindings",
        "expected_counts",
        "control_contract",
        "scenarios",
        "contract_sha256",
    }
    _expect_exact_keys(document, root_keys, "strict51 contract")
    if document["schema_name"] != STRICT51_CONTRACT_SCHEMA_NAME:
        _fail("L0C001_SCHEMA_INVALID", "unknown strict51 contract schema")
    if document["schema_version"] != STRICT51_CONTRACT_SCHEMA_VERSION:
        _fail("L0C001_SCHEMA_INVALID", "unknown strict51 contract version")
    if (
        document["protocol_id"] != STRICT51_PROTOCOL_ID
        or document["protocol_version"] != STRICT51_PROTOCOL_VERSION
    ):
        _fail("L0C001_SCHEMA_INVALID", "strict51 protocol identity mismatch")
    if document["roster_version"] != STRICT51_ROSTER_VERSION:
        _fail("L0C001_SCHEMA_INVALID", "strict51 roster identity mismatch")
    if document["canonicalization_id"] != STRICT51_CANONICALIZATION_ID:
        _fail("L0C001_SCHEMA_INVALID", "strict51 canonicalization identity mismatch")
    if document["execution_status"] != "offline-contract-only":
        _fail("L0C001_SCHEMA_INVALID", "strict51 contract execution status was elevated")
    if document["live_authorized"] is not False or document["formal_claim_allowed"] is not False:
        _fail("L0C010_CONTROL_FAULT_MAPPING_FORBIDDEN", "strict51 contract cannot authorize live/formal")
    if document["seed"] != STRICT51_DEFAULT_SEED or document["order_algorithm"] != STRICT51_ORDER_ALGORITHM:
        _fail("L0C001_SCHEMA_INVALID", "strict51 seed/order identity mismatch")
    _validate_source_binding_paths(document["source_bindings"])
    for binding in document["source_bindings"]:
        if Path(str(binding.get("path"))).name in {"b1_lite.py", "run-traditional-v2-lite.ps1"}:
            _fail(
                "L0C009_SECRET_OR_ABSOLUTE_PATH_IN_CONTRACT",
                "strict51 contract must not bind coordinator or launcher (A10 freeze report owns them)",
            )
    scenarios = document["scenarios"]
    if not isinstance(scenarios, list):
        _fail("L0C001_SCHEMA_INVALID", "strict51 scenarios must be a list")
    scenario_keys = set(ScenarioSpec.__dataclass_fields__)
    normalized: list[Mapping[str, Any]] = []
    for ordinal, item in enumerate(scenarios, start=1):
        if not isinstance(item, Mapping):
            _fail("L0C001_SCHEMA_INVALID", f"strict51 scenario {ordinal} must be an object")
        _expect_exact_keys(item, scenario_keys, f"strict51 scenario {ordinal}")
        normalized.append(item)
    expected = [spec.to_dict() for spec in STRICT51_SCENARIO_UNIVERSE]
    if normalized != expected:
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "strict51 scenario roster drift")
    if document["expected_counts"] != _strict51_expected_counts():
        _fail("L0C003_UNIVERSE_OR_COUNT_MISMATCH", "strict51 counts differ from frozen values")
    if document["control_contract"] != {
        "positions": list(STRICT51_CONTROL_POSITIONS),
        "per_block": 6,
        "rotations": {
            block_id: list(_CONTROL_ROTATIONS[f"B{block_id.split('-B')[1]}"])
            for block_id in STRICT51_BLOCK_IDS
        },
        "execution_adapter_status": "implemented-b1-lite-controls",
        "fault_mapping_allowed": False,
    }:
        _fail("L0C010_CONTROL_FAULT_MAPPING_FORBIDDEN", "strict51 control contract mismatch")
    _validate_sha256(document["contract_sha256"], "contract_sha256")
    core = copy.deepcopy(dict(document))
    del core["contract_sha256"]
    if document["contract_sha256"] != canonical_sha256(core):
        _fail("L0C008_SOURCE_HASH_DRIFT", "strict51 contract_sha256 does not match contract")


def _strict51_expected_counts() -> dict[str, int]:
    return {
        "universe": 51,
        "strict": 51,
        "auxiliary": 0,
        "deferred": 0,
        "blocks": 5,
        "faults_per_block": 51,
        "controls_per_block": 6,
        "slots_per_block": 57,
        "total_fault_slots": 255,
        "total_control_slots": 30,
        "total_slots": 285,
    }


def load_strict51_contract(path: os.PathLike[str] | str) -> Mapping[str, Any]:
    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        _fail("L0C001_SCHEMA_INVALID", f"cannot read strict51 contract: {exc.__class__.__name__}")
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        _fail("L0C001_SCHEMA_INVALID", "strict51 contract is not strict UTF-8")
    try:
        document = json.loads(text, cls=_DuplicateKeyDecoder)
    except LiteContractError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        _fail("L0C001_SCHEMA_INVALID", f"invalid strict51 JSON: {exc.__class__.__name__}")
    validate_strict51_contract_document(document)
    if raw != canonical_json_bytes(document) + b"\n":
        _fail(
            "L0C001_SCHEMA_INVALID",
            "strict51 contract bytes must be canonical compact JSON with one final LF",
        )
    return document


__all__ = [
    "ACTIVE_SCENARIO_IDS",
    "AUXILIARY_SCENARIO_IDS",
    "CANONICALIZATION_ID",
    "CONTRACT_SCHEMA_NAME",
    "CONTRACT_SCHEMA_VERSION",
    "CONTRACT_RELATIVE_PATH",
    "CONTROL_POSITIONS",
    "DEFAULT_SEED",
    "DEFERRED_SCENARIO_IDS",
    "FROZEN_BLOCK_IDS",
    "FROZEN_SCENARIO_UNIVERSE",
    "FROZEN_STRICT51_LOGICAL_SCHEDULE_SHA256",
    "FROZEN_STRICT51_SCHEDULE",
    "FreezeIdentity",
    "LiteContractError",
    "ScenarioSpec",
    "STRICT51_BLOCK_IDS",
    "STRICT51_CANONICALIZATION_ID",
    "STRICT51_CONTRACT_RELATIVE_PATH",
    "STRICT51_CONTRACT_SCHEMA_NAME",
    "STRICT51_CONTRACT_SCHEMA_VERSION",
    "STRICT51_CONTROL_POSITIONS",
    "STRICT51_DEFAULT_SEED",
    "STRICT51_LOGICAL_SCHEMA_NAME",
    "STRICT51_LOGICAL_SCHEMA_VERSION",
    "STRICT51_ORDER_ALGORITHM",
    "STRICT51_PANEL_OWNER_SCENARIO_IDS",
    "STRICT51_PROTOCOL_ID",
    "STRICT51_PROTOCOL_VERSION",
    "STRICT51_ROSTER_VERSION",
    "STRICT51_SCENARIO_IDS",
    "STRICT51_SCENARIO_UNIVERSE",
    "STRICT51_SOURCE_BINDINGS",
    "STRICT_SCENARIO_IDS",
    "build_contract_document",
    "build_schedule_manifest",
    "build_strict51_contract_document",
    "build_strict51_logical_schedule",
    "canonical_json_bytes",
    "canonical_sha256",
    "extract_runner_fault_choices",
    "load_contract",
    "load_strict51_contract",
    "validate_contract_document",
    "validate_schedule_manifest",
    "validate_strict51_contract_document",
    "validate_strict51_logical_schedule",
    "verify_frozen_sources",
]
