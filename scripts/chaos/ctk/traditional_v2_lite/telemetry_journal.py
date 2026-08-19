"""Closed, in-memory telemetry query journal for Lite E3."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


QUERY_STATUSES = frozenset({"value", "zero", "no_series", "query_error", "parse_error", "timeout"})
UNAVAILABLE_STATUSES = frozenset({"no_series", "query_error", "parse_error", "timeout"})


@dataclass(frozen=True, slots=True)
class QueryRecord:
    query_id: str
    request: str
    window_start: float
    window_end: float
    backend_summary: str
    observed_at: float
    required: bool
    status: str
    value: float | int | None

    def __post_init__(self) -> None:
        if self.status not in QUERY_STATUSES or not self.query_id or not self.request or not self.backend_summary:
            raise ValueError("invalid closed telemetry record")
        for value in (self.window_start, self.window_end, self.observed_at):
            if type(value) not in {int, float} or isinstance(value, bool) or not math.isfinite(float(value)):
                raise ValueError("telemetry time must be finite")
        if self.window_end < self.window_start or self.observed_at < self.window_end:
            raise ValueError("telemetry time order is invalid")
        if self.status == "zero":
            if type(self.value) not in {int, float} or isinstance(self.value, bool) or float(self.value) != 0.0:
                raise ValueError("zero status requires numeric zero")
        elif self.status == "value":
            if type(self.value) not in {int, float} or isinstance(self.value, bool) or not math.isfinite(float(self.value)) or float(self.value) == 0.0:
                raise ValueError("value status requires finite nonzero number")
        elif self.value is not None:
            raise ValueError("unavailable status may not carry a value")


def telemetry_acceptable(records: tuple[QueryRecord, ...]) -> bool:
    if not records or len({record.query_id for record in records}) != len(records):
        return False
    return not any(record.required and record.status in UNAVAILABLE_STATUSES for record in records)


__all__ = ["QUERY_STATUSES", "QueryRecord", "UNAVAILABLE_STATUSES", "telemetry_acceptable"]
