"""Minimal open-loop submit/collect driver with injected time."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Protocol


@dataclass(frozen=True, slots=True)
class Completion:
    completed_at: float
    timeout: bool
    status: str


class CompletionHandle(Protocol):
    def collect(self) -> Completion:
        """Return completion after submissions are no longer cadence-blocked."""


@dataclass(frozen=True, slots=True)
class WorkloadRecord:
    ordinal: int
    offered_at: float
    started_at: float
    completed_at: float
    timeout: bool
    status: str
    latency: float


def run_open_loop(
    *,
    count: int,
    interval_seconds: float,
    clock: Callable[[], float],
    sleeper: Callable[[float], None],
    submit: Callable[[int], CompletionHandle],
) -> tuple[WorkloadRecord, ...]:
    if type(count) is not int or count <= 0 or not math.isfinite(interval_seconds) or interval_seconds <= 0:
        raise ValueError("invalid open-loop schedule")
    epoch = float(clock())
    pending: list[tuple[int, float, float, CompletionHandle]] = []
    for ordinal in range(count):
        offered_at = epoch + ordinal * interval_seconds
        delay = offered_at - float(clock())
        if delay > 0:
            sleeper(delay)
        started_at = float(clock())
        if not all(math.isfinite(value) for value in (offered_at, started_at)) or started_at < offered_at:
            raise ValueError("submit clock is invalid")
        handle = submit(ordinal)
        if not callable(getattr(handle, "collect", None)):
            raise ValueError("submit must return a completion handle")
        pending.append((ordinal, offered_at, started_at, handle))

    records = []
    for ordinal, offered_at, started_at, handle in pending:
        completion = handle.collect()
        if not isinstance(completion, Completion) or not completion.status or type(completion.timeout) is not bool:
            raise ValueError("completion is invalid")
        completed_at = float(completion.completed_at)
        if not math.isfinite(completed_at) or completed_at < started_at:
            raise ValueError("completion clock is invalid")
        records.append(
            WorkloadRecord(
                ordinal,
                offered_at,
                started_at,
                completed_at,
                completion.timeout,
                completion.status,
                completed_at - started_at,
            )
        )
    return tuple(records)


__all__ = ["Completion", "CompletionHandle", "WorkloadRecord", "run_open_loop"]
