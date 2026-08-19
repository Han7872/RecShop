"""Exact eight-phase monotonic journal for Lite E3."""

from __future__ import annotations

import math
from dataclasses import dataclass


PHASES = (
    "reset",
    "warmup",
    "pre_fault",
    "injection_transition",
    "during_fault",
    "recovery_transition",
    "post_recovery",
    "final_cleanup",
)


@dataclass(frozen=True, slots=True)
class PhaseRecord:
    phase: str
    entered_at: float


class PhaseJournal:
    def __init__(self) -> None:
        self._records: list[PhaseRecord] = []

    def enter(self, phase: str, entered_at: float) -> None:
        expected = PHASES[len(self._records)] if len(self._records) < len(PHASES) else None
        if phase != expected:
            raise ValueError(f"expected phase {expected}, got {phase}")
        if type(entered_at) not in {int, float} or isinstance(entered_at, bool) or not math.isfinite(float(entered_at)):
            raise ValueError("phase time must be finite")
        if self._records and entered_at <= self._records[-1].entered_at:
            raise ValueError("phase clock must increase strictly")
        self._records.append(PhaseRecord(phase, float(entered_at)))

    def finalize(self) -> tuple[PhaseRecord, ...]:
        if tuple(record.phase for record in self._records) != PHASES:
            raise ValueError("phase journal is incomplete")
        return tuple(self._records)


__all__ = ["PHASES", "PhaseJournal", "PhaseRecord"]
