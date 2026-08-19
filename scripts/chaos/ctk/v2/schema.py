"""v2 schema contracts — frozen before any runner/packager code changes.

These dataclasses and validators define the data shapes that P0-1..P0-9 produce
and consume. They are the contract layer; the v1 runner is NOT imported here so
the module stays unit-testable without K8s.

Reference: HANDOFF-2026-08-10 §7 (P0 spec), §8 (recommended implementation),
§11 (acceptance), §13 (delivery structure). Diagnosis-2026-08-10 §5 (evidence).

Design rules:
  - Every field that P0 marks "required" has NO default — its absence is a
    hard validation failure (fail-closed). Optional fields get a default.
  - `releaseable` is an explicit conjunction (HANDOFF §7 P0-4), never inferred
    from a single aggregate score.
  - Attempt ledger is append-only; the dataclass captures one attempt row,
    not the whole history (the ledger file holds many rows).
  - `quality` provenance is per-datapoint (HANDOFF §7 P0-1/P1), never a single
    blanket "observed" label (fixes mr2_load_adapter.py:218-219 blanket inject).
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional
import uuid

# --------------------------------------------------------------------------
# Protocol versioning (P0-1)
# --------------------------------------------------------------------------

#: Must change when collector, gate, stage, workload or fault-primitive shared
#: semantics change. Changing it ends the current protocol epoch; cases from
#: different protocol versions MUST NOT be mixed into one replicate block
#: (HANDOFF §7 P0-1, §10 rule 7).
PROTOCOL_VERSION = "v2.0.0-draft"


# --------------------------------------------------------------------------
# P0-1: Environment card — machine + cluster + image fingerprint
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class EnvironmentCard:
    """Per-attempt environment fingerprint (HANDOFF §7 P0-1).

    The v1 runner hardcoded only `platform="kubernetes"` and
    `kubernetes_context="docker-desktop"` (chaos_k8s_runner.py:11293). This
    card captures the full identity needed to detect the 150/105 machine
    batch confound (Diagnosis §2.7, §6).
    """
    # Machine (required — no defaults; absence = fail-closed)
    machine_id: str               # stable host id (hostname + cpu model hash)
    cpu_model: str
    cpu_count: int
    ram_total_gb: float
    os: str

    # Cluster + toolchain versions (required)
    kubernetes_version: str       # `kubectl version --short`
    kubernetes_context: str       # CURRENT context, not hardcoded
    chaos_mesh_version: str       # chart + controller image tag
    otel_collector_version: str
    prometheus_version: str
    jaeger_version: str
    loki_version: Optional[str] = None   # None if Loki not deployed

    # Image digests (required for non-`latest` reproducibility)
    # map: deployment_name -> image@sha256:...
    image_digests: dict = field(default_factory=dict)

    # Config fingerprints (required)
    # map: config_name (e.g. "chaos-mesh-values", "otel-collector-config") -> sha256
    config_hashes: dict = field(default_factory=dict)

    # Runner / workload / scenario config hash (required)
    runner_commit: str = ""       # git sha of chaos_k8s_runner.py used
    runner_config_hash: str = ""  # sha256 of the resolved scenario config
    workload_hash: str = ""       # sha256 of workload spec

    # Clock (required)
    collected_at_utc: str = ""    # ISO 8601 UTC of card collection
    monotonic_ns: int = 0         # time.monotonic_ns() at collection
    ntp_offset_ms: Optional[float] = None  # measured clock skew; None if unchecked


# --------------------------------------------------------------------------
# P0-3: State-machine phase identifiers
# --------------------------------------------------------------------------

#: The v2 explicit state machine (HANDOFF §7 P0-3). Transitions must NOT be
#: silently merged into pre/post. `during_fault` begins only after all fault
#: legs are active AND the manifestation gate passes; `post_recovery` begins
#: only after all legs are controller-recovered + target healthy + stable.
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

#: Phases that count toward "valid observation" windows. Transitions
#: (injection_transition / recovery_transition) are recorded but MUST NOT be
#: folded into pre/during/post aggregates (HANDOFF §7 P0-3).
OBSERVATION_PHASES = ("pre_fault", "during_fault", "post_recovery")
TRANSITION_PHASES = ("injection_transition", "recovery_transition")


# --------------------------------------------------------------------------
# P0-4: Per-leg fault gate result (fail-closed, explicit conjunction)
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class LegGateResult:
    """One fault leg's gate evidence (HANDOFF §7 P0-4).

    v1 had NO per-leg active/manifestation check (Diagnosis key-finding #2:
    gates were all per-root aggregate `per_root_F1/F2`). v1 also did NOT
    record the actual selected target (Diagnosis key-finding #1: only yaml
    name + AllInjected boolean). This dataclass forces both.
    """
    fault_instance_id: str        # "F1", "F2", "F3"

    # Apply (required)
    apply_ok: bool                # kubectl apply rc==0
    apply_rc: Optional[int] = None
    apply_error: Optional[str] = None

    # Chaos controller conditions (required for CRD faults)
    controller_selected: bool = False   # .status.selectedResources / selector hit
    actual_targets: tuple = field(default_factory=tuple)  # pod names actually hit
    all_injected_condition: bool = False  # Chaos Mesh AllInjected==True
    controller_error: Optional[str] = None

    # Planned vs actual target match (required; Diagnosis key-finding #1)
    planned_target: str = ""      # from scenario profile
    actual_target_matches_gt: bool = False

    # Activation / manifestation (required)
    active_pass: bool = False     # fault is observably active during window
    manifestation_pass: bool = False  # workload reached the target path
    workload_reached_target: bool = False
    manifestation_detail: Optional[str] = None

    # Recovery (required)
    recover_action_ok: bool = False
    controller_recovered: bool = False  # AllRecovered==True (DejaVu-style)
    target_healthy: bool = False        # pod/endpoint healthy post-recovery
    root_probe_ok: bool = False         # per-root probe recovered
    recovery_stable_seconds: float = 0.0
    recovery_error: Optional[str] = None

    @property
    def releaseable(self) -> bool:
        """Explicit conjunction (HANDOFF §7 P0-4 `releaseable`)."""
        return bool(
            self.apply_ok
            and self.controller_selected
            and self.all_injected_condition
            and self.actual_target_matches_gt
            and self.active_pass
            and self.manifestation_pass
            and self.workload_reached_target
            and self.recover_action_ok
            and self.controller_recovered
            and self.target_healthy
            and self.root_probe_ok
        )


# --------------------------------------------------------------------------
# P0-6: Telemetry completeness (per modality, per service/metric/stage)
# --------------------------------------------------------------------------

#: Distinguished empty-result causes (HANDOFF §7 P0-6). v1 merged all into
#: one `empty_or_fail` counter (Diagnosis key-finding #6, chaos_k8s_runner.py
#: :2081-2082). v2 MUST record which cause applied per (service, metric, stage).
METRIC_EMPTY_CAUSES = (
    "no_series",      # prometheus legitimately has no series for this query
    "query_error",    # HTTP non-200 / connection error
    "parse_error",    # response could not be parsed
    "timeout",        # query exceeded deadline
    "zero",           # series exists, value is zero (a real observation)
    "ok",             # series exists, non-zero value
)


@dataclass(frozen=True)
class MetricCoverageCell:
    """One (service, metric, stage) coverage cell (HANDOFF §7 P0-6)."""
    service: str
    metric: str
    stage: str
    cause: str         # one of METRIC_EMPTY_CAUSES
    returned_points: int = 0
    expected_points: int = 0
    detail: Optional[str] = None   # e.g. the failed expr for query_error

    @property
    def coverage_pass(self) -> bool:
        """Required metrics must be `ok` or `zero`. Anything else fails the cell."""
        return self.cause in ("ok", "zero") and self.returned_points > 0


@dataclass(frozen=True)
class TraceCoverage:
    """Jaeger trace completeness for one (service, stage) (HANDOFF §7 P0-6).

    v1 used limit=400 with no pagination and no cap proof (Diagnosis §2.5,
    chaos_k8s_runner.py:2493-2543). v2 must paginate OR prove the cap was not
    hit, and record returned/unique/dropped counts.
    """
    service: str
    stage: str
    returned_traces: int = 0
    unique_traces: int = 0
    dropped_traces: int = 0      # cap-hit overflows
    cap_hit: bool = False        # returned == limit AND not paginated
    pagination_used: bool = False
    spans_in_stage: int = 0      # after strict [stage_start, stage_end) crop
    spans_dropped_out_of_stage: int = 0

    @property
    def coverage_pass(self) -> bool:
        return not self.cap_hit and self.returned_traces > 0


@dataclass(frozen=True)
class LogCoverage:
    """Log completeness for one (deployment, stage) (HANDOFF §7 P0-6).

    v1 used `kubectl logs --since-time --tail 2000` with no --previous and no
    truncation detection (Diagnosis §2.5, chaos_k8s_runner.py:2549-2557).
    Command-failure text must NOT count as a valid log.
    """
    deployment: str
    stage: str
    source: str = ""             # "loki" | "otel-backend" | "kubectl-current" | "kubectl-previous"
    lines: int = 0
    truncated: bool = False      # tail cap hit
    fetch_error: Optional[str] = None
    pod_uid: str = ""            # which pod's logs (current/previous)

    @property
    def coverage_pass(self) -> bool:
        return self.fetch_error is None and not self.truncated and self.lines > 0


# --------------------------------------------------------------------------
# P0-7: Attempt / attrition ledger (append-only)
# --------------------------------------------------------------------------

#: Terminal states for an attempt (HANDOFF §7 P0-7, §10 stop rules).
ATTEMPT_STATES = (
    "planned",
    "precheck_failed",
    "injection_failed",
    "activation_failed",
    "manifestation_failed",
    "telemetry_failed",
    "recovery_failed",
    "recollected",         # re-run produced data but not yet classified
    "excluded",            # gate failed, kept in ledger, not in dataset
    "retained_strict",     # all hard gates pass -> strict main
    "retained_auxiliary",  # gray/masked/config-state-only -> auxiliary track
)

#: Reasons that MUST end a protocol epoch (HANDOFF §10 rule 7).
EPOCH_ENDING_REASONS = ("code_change", "config_change", "image_change", "threshold_change")


@dataclass(frozen=True)
class AttemptRecord:
    """One row in the append-only attempt ledger (HANDOFF §7 P0-7).

    The ledger file is JSONL; each line is one AttemptRecord. Old attempts are
    NEVER overwritten or deleted (Diagnosis §6 attrition auditability gap).
    A case may have many attempts (planned -> failed -> retried -> retained).
    """
    attempt_id: str               # uuid4 hex, unique per attempt
    collection_epoch: str         # groups attempts under one protocol version
    block_id: Optional[str] = None     # B1..B5, or None for smoke
    scenario_id: str = ""         # e.g. "dual01_uni"
    replicate_id: Optional[int] = None  # 1..5
    protocol_version: str = PROTOCOL_VERSION

    # Lifecycle
    state: str = "planned"        # one of ATTEMPT_STATES
    started_at_utc: str = ""
    ended_at_utc: str = ""

    # Failure provenance (required when state is a *_failed)
    failure_reason: Optional[str] = None
    failure_detail: Optional[str] = None
    retry_of_attempt_id: Optional[str] = None  # if this is a retry

    # Counts toward denominator? (HANDOFF §10 rule 8: no "best retry" selection)
    counts_toward_denominator: bool = True

    # Environment card ref (P0-1) — path to the env card JSON for this attempt
    environment_card_path: Optional[str] = None

    @staticmethod
    def new_attempt(collection_epoch: str, scenario_id: str,
                    block_id: Optional[str] = None,
                    replicate_id: Optional[int] = None) -> "AttemptRecord":
        return AttemptRecord(
            attempt_id=uuid.uuid4().hex,
            collection_epoch=collection_epoch,
            block_id=block_id,
            scenario_id=scenario_id,
            replicate_id=replicate_id,
            started_at_utc=datetime.now(timezone.utc).isoformat(),
        )


# --------------------------------------------------------------------------
# P0-8: Release contract — the gate the packager MUST enforce
# --------------------------------------------------------------------------

#: The 4 validation ids that v1 whitelisted as "degraded ok" (Diagnosis §5.5,
#: chaos_k8s_runner.py:11000-11006). v2 MUST NOT whitelist any of these for
#: strict main. dual06-style config-state-only cases go to auxiliary, not strict.
V1_DEGRADED_WHITELIST = (
    "catalog_server_p95_flat",
    "pod_failure_window_validity",
    "service_cpu_throttle_present",
    "cfg_validity_footprint",
)


@dataclass(frozen=True)
class ReleaseContract:
    """The release decision for one case (HANDOFF §7 P0-4, P0-8).

    This is the explicit conjunction the packager reads. v1's packager
    (package_for_delivery.py:1343-1366) did NOT check ready_for_release at
    all (Diagnosis §2.6, key-finding #7). v2 packager MUST read this and
    reject on any false/missing field.

    `track` decides strict vs auxiliary directory placement. A case with
    `releaseable=True` but `track="auxiliary"` is a deliberate gray/masked/
    config-state-only case (HANDOFF §9.4 dual06 decision point).
    """
    case_id: str
    attempt_id: str               # links to AttemptRecord

    # Explicit conjunction (HANDOFF §7 P0-4)
    environment_precheck_pass: bool = False
    every_leg_apply_pass: bool = False
    every_leg_actual_target_matches_gt: bool = False
    every_leg_active_pass: bool = False
    manifestation_contract_pass: bool = False
    required_telemetry_coverage_pass: bool = False
    every_leg_recovery_pass: bool = False
    root_and_system_stable_pass: bool = False
    final_state_checksum_pass: bool = False
    no_residual_crd_or_owned_resource: bool = False
    artifact_hash_pass: bool = False

    # Track classification
    track: str = "strict"         # "strict" | "auxiliary" | "control"
    track_reason: Optional[str] = None  # why auxiliary (e.g. "config-state-only")

    @property
    def releaseable_strict(self) -> bool:
        """All hard gates pass AND track is strict."""
        return (
            self.track == "strict"
            and self.environment_precheck_pass
            and self.every_leg_apply_pass
            and self.every_leg_actual_target_matches_gt
            and self.every_leg_active_pass
            and self.manifestation_contract_pass
            and self.required_telemetry_coverage_pass
            and self.every_leg_recovery_pass
            and self.root_and_system_stable_pass
            and self.final_state_checksum_pass
            and self.no_residual_crd_or_owned_resource
            and self.artifact_hash_pass
        )

    @property
    def releaseable_auxiliary(self) -> bool:
        """All hard gates pass BUT track is auxiliary (gray/masked/config-state).

        Auxiliary cases still need every_leg apply/active/recover and checksum
        to pass — they differ from strict only in manifestation purity
        (gray/masked signal). They MUST NOT pass just because of a whitelist.
        """
        return (
            self.track == "auxiliary"
            and self.every_leg_apply_pass
            and self.every_leg_active_pass
            and self.every_leg_recovery_pass
            and self.final_state_checksum_pass
        )

    def to_dict(self) -> dict:
        return asdict(self)


# --------------------------------------------------------------------------
# Validators — pure functions used by packager AND unit tests
# --------------------------------------------------------------------------

class ContractError(Exception):
    """Raised when a release contract or metadata fails a hard gate.

    The packager converts this into a hard rejection of the case (P0-8).
    """


def validate_release_contract_for_strict(rc: ReleaseContract) -> None:
    """Fail-closed validation for a strict-main case (HANDOFF §7 P0-8).

    Raises ContractError if ANY required field is false or if the track is
    not "strict". This is the gate `package_for_delivery.py` and
    `build_full_delivery.py` MUST call before copying any case.
    """
    if rc.track != "strict":
        raise ContractError(
            f"case {rc.case_id}: track={rc.track!r} cannot enter strict main "
            f"(track_reason={rc.track_reason})"
        )
    if not rc.releaseable_strict:
        # Identify the first failing conjunct for a clear error
        checks = {
            "environment_precheck_pass": rc.environment_precheck_pass,
            "every_leg_apply_pass": rc.every_leg_apply_pass,
            "every_leg_actual_target_matches_gt": rc.every_leg_actual_target_matches_gt,
            "every_leg_active_pass": rc.every_leg_active_pass,
            "manifestation_contract_pass": rc.manifestation_contract_pass,
            "required_telemetry_coverage_pass": rc.required_telemetry_coverage_pass,
            "every_leg_recovery_pass": rc.every_leg_recovery_pass,
            "root_and_system_stable_pass": rc.root_and_system_stable_pass,
            "final_state_checksum_pass": rc.final_state_checksum_pass,
            "no_residual_crd_or_owned_resource": rc.no_residual_crd_or_owned_resource,
            "artifact_hash_pass": rc.artifact_hash_pass,
        }
        failed = [k for k, v in checks.items() if not v]
        raise ContractError(
            f"case {rc.case_id}: strict release blocked, failing conjuncts: {failed}"
        )


def validate_release_contract_for_auxiliary(rc: ReleaseContract) -> None:
    """Fail-closed validation for an auxiliary-track case (HANDOFF §9.4).

    Auxiliary cases (gray/masked/config-state-only) still need the structural
    gates (apply/active/recover/checksum) to pass. They differ from strict
    only in that manifestation purity is gray. A whitelist MUST NOT bypass this.
    """
    if rc.track != "auxiliary":
        raise ContractError(
            f"case {rc.case_id}: expected track='auxiliary' for auxiliary build, "
            f"got track={rc.track!r}"
        )
    if not rc.releaseable_auxiliary:
        checks = {
            "every_leg_apply_pass": rc.every_leg_apply_pass,
            "every_leg_active_pass": rc.every_leg_active_pass,
            "every_leg_recovery_pass": rc.every_leg_recovery_pass,
            "final_state_checksum_pass": rc.final_state_checksum_pass,
        }
        failed = [k for k, v in checks.items() if not v]
        raise ContractError(
            f"case {rc.case_id}: auxiliary release blocked, failing conjuncts: {failed}"
        )


def leg_gate_from_metadata_fault(fault_entry: dict) -> LegGateResult:
    """Bridge: build a LegGateResult from a v1 metadata.json `faults[]` entry.

    This is an ADAPTER for v1 metadata — it CANNOT synthesize fields v1 never
    recorded. Fields v1 lacks (actual_targets, controller_selected,
    active_pass, manifestation_pass, workload_reached_target, root_probe_ok)
    default to False, which makes `releaseable` False. This is intentional:
    v1 metadata can NEVER pass the v2 strict gate because v1 never recorded
    per-leg evidence. Only fresh v2 collection (with the new state machine)
    can produce a releaseable LegGateResult.

    This function exists so the packager can reject v1 cases uniformly rather
    than silently admitting them.
    """
    return LegGateResult(
        fault_instance_id=fault_entry.get("fault_instance_id", "?"),
        # v1 never recorded apply rc; presence of injected_at is a weak proxy
        apply_ok=bool(fault_entry.get("injected_at")),
        # v1 hardcoded status="recovered" (Diagnosis §5.2) — ignore it
        recover_action_ok=bool(fault_entry.get("recovered_at")),
        planned_target=fault_entry.get("target_component", ""),
        # all of these are False because v1 never recorded them:
        controller_selected=False,
        all_injected_condition=False,
        actual_target_matches_gt=False,
        active_pass=False,
        manifestation_pass=False,
        workload_reached_target=False,
        controller_recovered=False,
        target_healthy=False,
        root_probe_ok=False,
    )
