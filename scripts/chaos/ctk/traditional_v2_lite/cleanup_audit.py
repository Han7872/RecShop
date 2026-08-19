"""Pure ownership audit and reverse cleanup planner for Lite E3."""

from __future__ import annotations

from dataclasses import dataclass


OWNERSHIP_STATES = frozenset({"OWNED", "FOREIGN", "UNKNOWN", "MISSING"})


@dataclass(frozen=True, slots=True)
class ResourceRecord:
    invocation_ordinal: int
    resource_kind: str
    name: str
    uid: str
    attempt_label: str | None
    ownership: str
    residual: bool = False

    def __post_init__(self) -> None:
        if self.ownership not in OWNERSHIP_STATES or self.invocation_ordinal < 1 or not self.resource_kind or not self.name or not self.uid:
            raise ValueError("invalid resource record")


@dataclass(frozen=True, slots=True)
class CleanupAction:
    resource_kind: str
    name: str
    uid: str


@dataclass(frozen=True, slots=True)
class CleanupAuditResult:
    actions: tuple[CleanupAction, ...]
    blocking_resources: tuple[ResourceRecord, ...]
    next_allowed: bool


def audit_cleanup(attempt_id: str, resources: tuple[ResourceRecord, ...]) -> CleanupAuditResult:
    if not attempt_id or not resources:
        raise ValueError("attempt and actual invocations are required")
    ordinals = [resource.invocation_ordinal for resource in resources]
    if len(set(ordinals)) != len(ordinals):
        raise ValueError("actual invocation order must be unique")
    owned = tuple(
        resource
        for resource in resources
        if resource.ownership == "OWNED"
        and resource.attempt_label == attempt_id
        and not resource.residual
    )
    blocking = tuple(resource for resource in resources if resource not in owned)
    actions = tuple(
        CleanupAction(resource.resource_kind, resource.name, resource.uid)
        for resource in sorted(owned, key=lambda item: item.invocation_ordinal, reverse=True)
    )
    return CleanupAuditResult(actions, blocking, not blocking)


__all__ = [
    "CleanupAction",
    "CleanupAuditResult",
    "OWNERSHIP_STATES",
    "ResourceRecord",
    "audit_cleanup",
]
