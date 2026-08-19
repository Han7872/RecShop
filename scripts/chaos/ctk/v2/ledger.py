"""P0-7: Append-only attempt / attrition ledger (HANDOFF §7 P0-7, §10).

The ledger records every attempt's lifecycle:
    planned -> precheck_failed | injection_failed | activation_failed
            -> manifestation_failed | telemetry_failed | recovery_failed
            -> recollected | excluded | retained_strict | retained_auxiliary

Rules (HANDOFF §7 P0-7, §15):
  - NEVER overwrite an old attempt row.
  - NEVER delete a failed attempt directory.
  - NEVER keep only the "best" retry.
  - Each attempt has a unique attempt_id; retries are new rows that link via
    `retry_of_attempt_id`.
  - Retry reason, time, code version, and denominator inclusion are explicit.

The ledger is JSONL: one AttemptRecord per line. This format is append-only
friendly (no rewrite needed) and streamable for audit.

This module does NOT import the v1 runner; it is unit-testable without K8s.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .schema import (
    AttemptRecord,
    ATTEMPT_STATES,
    EPOCH_ENDING_REASONS,
    PROTOCOL_VERSION,
)


class LedgerError(Exception):
    """Raised on ledger integrity violations (overwrite, bad state transition, etc.)."""


# Valid state transitions (HANDOFF §7 P0-7). planned -> any *_failed terminal
# or -> recollected/excluded/retained_*_*. Failed states are terminal (a retry
# is a NEW attempt row, not a state change of the failed row).
_VALID_TRANSITIONS: Dict[str, frozenset] = {
    "planned": frozenset(ATTEMPT_STATES) - {"planned"},
    # all other states are terminal — no outgoing edges
}


def _validate_transition(old: str, new: str) -> None:
    """Fail-closed: only planned -> terminal is allowed within one row."""
    if old == new:
        raise LedgerError(f"no-op transition {old!r} -> {new!r}")
    if old not in ATTEMPT_STATES:
        raise LedgerError(f"unknown old state {old!r}")
    if new not in ATTEMPT_STATES:
        raise LedgerError(f"unknown new state {new!r}; valid: {ATTEMPT_STATES}")
    allowed = _VALID_TRANSITIONS.get(old, frozenset())
    if new not in allowed:
        raise LedgerError(
            f"invalid transition {old!r} -> {new!r}; "
            f"state {old!r} is terminal. A retry must be a NEW attempt row, "
            f"not a mutation of this one (HANDOFF §7 P0-7)."
        )


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


class AttemptLedger:
    """Append-only JSONL ledger of attempt records.

    Usage:
        ledger = AttemptLedger(Path("manifests/attempts.jsonl"))
        rec = ledger.open_attempt("epoch-1", "dual01_uni", block_id="B1", replicate_id=1)
        # ... runner does work ...
        ledger.transition(rec.attempt_id, "retained_strict")
        # or on failure:
        ledger.transition(rec.attempt_id, "injection_failed",
                          failure_reason="AllInjected never True",
                          failure_detail="chaos-controller pod crashloop")

    The ledger file is created on first write. Each transition APPENDS a new
    line — it does NOT rewrite the original planned row. The "current state"
    of an attempt is the state in its LATEST row. This gives a full audit
    trail of every state change (HANDOFF §7 P0-7: "do not overwrite old
    attempt" — append-one-line-per-transition keeps the file truly append-only
    and never rewritten).

    For the common case (planned -> terminal in one step, because failed
    states are terminal), there will be exactly 2 rows per attempt: one
    `planned` open, one terminal close.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # in-memory index: attempt_id -> last known state (for transition validation)
        self._index: Dict[str, str] = {}
        if self.path.exists():
            self._reindex()

    def _reindex(self) -> None:
        """Rebuild the in-memory index from the existing file (crash recovery)."""
        with self.path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                aid = rec["attempt_id"]
                self._index[aid] = rec["state"]

    def _append(self, rec: AttemptRecord) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")
        self._index[rec.attempt_id] = rec.state

    def open_attempt(self, collection_epoch: str, scenario_id: str,
                     block_id: Optional[str] = None,
                     replicate_id: Optional[int] = None,
                     protocol_version: str = PROTOCOL_VERSION) -> AttemptRecord:
        """Record the opening of a new attempt (state='planned')."""
        rec = AttemptRecord(
            attempt_id=_new_attempt_id(),
            collection_epoch=collection_epoch,
            block_id=block_id,
            scenario_id=scenario_id,
            replicate_id=replicate_id,
            protocol_version=protocol_version,
            state="planned",
            started_at_utc=_now_utc(),
        )
        self._append(rec)
        return rec

    def transition(self, attempt_id: str, new_state: str,
                   failure_reason: Optional[str] = None,
                   failure_detail: Optional[str] = None,
                   counts_toward_denominator: bool = True,
                   environment_card_path: Optional[str] = None,
                   retry_of_attempt_id: Optional[str] = None) -> AttemptRecord:
        """Append a transition row for an existing attempt.

        Validates the transition is legal (planned -> terminal only within a row).
        Raises LedgerError on illegal transition (e.g. trying to change a
        terminal state — a retry must be a new open_attempt, not a mutation).
        """
        old_state = self._index.get(attempt_id)
        if old_state is None:
            raise LedgerError(
                f"attempt_id {attempt_id!r} not in ledger; call open_attempt first"
            )
        _validate_transition(old_state, new_state)

        # Read the original planned row to carry forward immutable fields
        base = self._read_first_row(attempt_id)
        rec = AttemptRecord(
            attempt_id=attempt_id,
            collection_epoch=base.collection_epoch,
            block_id=base.block_id,
            scenario_id=base.scenario_id,
            replicate_id=base.replicate_id,
            protocol_version=base.protocol_version,
            state=new_state,
            started_at_utc=base.started_at_utc,
            ended_at_utc=_now_utc(),
            failure_reason=failure_reason,
            failure_detail=failure_detail,
            retry_of_attempt_id=retry_of_attempt_id,
            counts_toward_denominator=counts_toward_denominator,
            environment_card_path=environment_card_path,
        )
        self._append(rec)
        return rec

    def _read_first_row(self, attempt_id: str) -> AttemptRecord:
        """Read the first (planned) row for an attempt_id."""
        with self.path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                if d["attempt_id"] == attempt_id:
                    return AttemptRecord(**{k: v for k, v in d.items()
                                            if k in AttemptRecord.__dataclass_fields__})
        raise LedgerError(f"attempt_id {attempt_id!r} index hit but row not found")

    def current_state(self, attempt_id: str) -> str:
        return self._index.get(attempt_id, "planned")

    def attrition_summary(self) -> Dict[str, int]:
        """Summarize retained/excluded/failed counts by terminal state.

        Returns a dict like:
            {"planned": 3, "injection_failed": 2, "retained_strict": 48, ...}
        Counts the LATEST row per attempt_id (so a planned->retained_strict
        attempt counts once as retained_strict, not once as planned).
        """
        # latest state per attempt_id is already in self._index
        counts: Dict[str, int] = {}
        for state in self._index.values():
            counts[state] = counts.get(state, 0) + 1
        return counts

    def attrition_flow(self) -> Dict[str, int]:
        """HANDOFF §11.3 / §18.5: planned/attempted/failed/retried/excluded/retained.

        Returns the attrition flow counts for the dataset-level report:
            planned      = total attempts opened
            attempted     = attempts that left 'planned' (any terminal)
            failed        = attempts in a *_failed state
            retried       = attempts whose retry_of_attempt_id is set
            excluded      = attempts in 'excluded'
            retained_strict    = attempts in 'retained_strict'
            retained_auxiliary = attempts in 'retained_auxiliary'
        """
        flow = {
            "planned": 0, "attempted": 0, "failed": 0, "retried": 0,
            "excluded": 0, "retained_strict": 0, "retained_auxiliary": 0,
            "recollected": 0,
        }
        retried_ids = set()
        with self.path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                if d.get("retry_of_attempt_id"):
                    retried_ids.add(d["attempt_id"])
        for state in self._index.values():
            flow["planned"] += 1
            if state != "planned":
                flow["attempted"] += 1
            if state.endswith("_failed"):
                flow["failed"] += 1
            if state == "excluded":
                flow["excluded"] += 1
            elif state == "retained_strict":
                flow["retained_strict"] += 1
            elif state == "retained_auxiliary":
                flow["retained_auxiliary"] += 1
            elif state == "recollected":
                flow["recollected"] += 1
        flow["retried"] = len(retried_ids)
        return flow


def _new_attempt_id() -> str:
    import uuid
    return uuid.uuid4().hex


def is_epoch_ending_reason(reason: str) -> bool:
    """HANDOFF §10 rule 7: these reasons end a protocol epoch."""
    return reason in EPOCH_ENDING_REASONS
