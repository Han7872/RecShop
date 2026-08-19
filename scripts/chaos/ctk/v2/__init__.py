"""RecShop traditional v2 protocol layer.

Per HANDOFF-2026-08-10 §8.1: a small v2 orchestration/state-machine layer that
calls existing validated atomic fault primitives, rather than refactoring the
~16.8k-line v1 runner in place.

This package contains ONLY:
  - Schema / dataclass contracts (frozen before any code changes)
  - Gate evaluators (pure functions over metadata dicts)
  - Schema validators (reject-tests for packager / builder)
  - Attempt ledger (append-only)

It does NOT import or mutate the v1 runner. The v1 runner is imported by the
v2 state-machine driver only at integration time (not at schema-definition time),
so these modules remain unit-testable without K8s.

v2 protocol version history:
  - "v2.0.0-draft" — initial schema freeze (this commit). Not yet collected;
    any collection under this version is engineering qualification, not a
    formal replicate (HANDOFF §9.3).
"""
__version__ = "v2.0.0-draft"
PROTOCOL_VERSION = "v2.0.0-draft"
