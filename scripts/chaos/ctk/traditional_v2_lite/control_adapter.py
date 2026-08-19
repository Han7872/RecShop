"""Pure validation adapter for frozen no-fault and sham control slots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .contract import validate_contract_document, validate_schedule_manifest


@dataclass(frozen=True, slots=True)
class ControlPlan:
    block_id: str
    run_ordinal: int
    control_id: str
    control_kind: str
    action: str


def build_control_plans(
    contract: Mapping[str, Any], schedule: Mapping[str, Any]
) -> tuple[ControlPlan, ...]:
    validate_contract_document(contract)
    validate_schedule_manifest(schedule)
    controls = contract["control_contract"]
    if controls["fault_mapping_allowed"] is not False or controls["per_block"] != 6:
        raise ValueError("control contract may not map or execute faults")
    plans = []
    positions = tuple(controls["positions"])
    for block in schedule["blocks"]:
        block_id = block["block_id"]
        slots = [slot for slot in block["slots"] if slot["slot_type"] == "control"]
        if tuple(slot["run_ordinal"] for slot in slots) != positions:
            raise ValueError(f"control positions drifted in {block_id}")
        if tuple(slot["control_id"] for slot in slots) != tuple(controls["rotations"][block_id]):
            raise ValueError(f"control rotation drifted in {block_id}")
        for slot in slots:
            if set(slot) != {"run_ordinal", "slot_type", "control_id", "control_kind"}:
                raise ValueError("control slot has fault-mapping fields")
            control_id = slot["control_id"]
            kind = slot["control_kind"]
            expected_kind = "no_fault" if control_id.startswith("NF_") else "sham"
            if kind != expected_kind:
                raise ValueError("control kind does not match frozen control id")
            plans.append(ControlPlan(block_id, slot["run_ordinal"], control_id, kind, control_id.lower()))
    return tuple(plans)


__all__ = ["ControlPlan", "build_control_plans"]
