"""per_service_canon.py — BASE_SERVICES (25) + EXPLICIT CANON_MAP + canonicalize().

WS1a of the PER-SERVICE TELEMETRY UPGRADE. The per-service telemetry collectors
(per_service_metrics/traces/logs) group emitters by their OTEL/exported_job /
trace serviceName / Loki service_name. Temp-instance chaos runners emit under
*distinct* names (so a temp instance does NOT pollute the persistent service's
exported_job). canonicalize(otel_name) folds every such emitter back into its
BASE service column.

MUST-FIX #1 (mandated by review): do NOT rely on a regex alone. CANON_MAP is an
EXPLICIT alias table, programmatically DERIVED from ALL actual emitters by
mirroring the exact f-string naming in:

  * chaos6x18_v3_runner.py  — single-position chaos:
        otel_name = f"{svc}_{short}_{OTEL_SHORT[svc]}"
        (svc = carrier_service ∈ V3_CARRIERS, short ∈ V3_SHORTS,
         OTEL_SHORT[svc] = the short suffix)
        + an OLDER truncated generation observed live: f"{svc}_{short}"
          (base + short, no trailing OTEL_SHORT suffix — present in Prometheus
           as stale exported_job, e.g. catalog_service_applat).

  * chaos_dualroot_runner.py — dual-root chaos:
        single_temp / single_temp_pool / dr02 / dual_sasrec_order:
            otel_name = f"{svc}_{combo_id}_{OTEL_SHORT.get(svc, svc)}"
        DR05 checkout_via_pricing_catalog_edge spawns TWO temp instances under
        BARE-SHORT leading tokens (NOT the full base name):
            co_otel = f"checkout_{combo_id}_checkout"
            pr_otel = f"pricing_{combo_id}_pricing"
        toxi_edge / direct toxiproxy (network_delay / dependency_failure) and
        host cpu/mem route through the REAL persistent service (jaeger_service /
        probe_service) — those names already ARE a base name and fold via the
        longest-base-prefix fallback.

  * agentchaos_runner.py (start_temp_instance):
        OTEL_SERVICE_NAME = "recommendation_agent_taskx" (folds via prefix +
        explicit alias).

canonicalize(otel_name) -> (base, matched_by):
  1. exact hit in CANON_MAP                              -> ('<base>', 'canon')
  2. longest BASE_SERVICES prefix on '_' token boundary  -> ('<base>', 'prefix')
  3. unknown                                             -> (None, 'unknown')

Unknown names are SURFACED (return None + matched_by='unknown' + optional
on_unknown callback / module-level UNKNOWN_SEEN log) — never silently
misattributed. raw/*.json keeps BOTH the raw OTEL name and the canonical fold.
"""

from __future__ import annotations

from typing import Callable, Optional, Tuple

# ============================================================
# BASE_SERVICES — the 25 from start_all.py SERVICES (services/<dir> names).
# Order: longest-first NOT required here (canonicalize sorts by length); kept in
# start order for readability. This is the stable per-service column key set.
# ============================================================
BASE_SERVICES = [
    "sasrec_api",
    "backend_api",
    "recommendation_agent",
    "llm_rerank_service",
    "review_service",
    "shop_web",
    "user_service",
    "catalog_service",
    "cart_service",
    "address_service",
    "ai_memory_service",
    "announcement_service",
    "order_service",
    "checkout_service",
    "payment_service",
    "inventory_service",
    "pricing_service",
    "promotion_service",
    "shipping_service",
    "search_service",
    "review_query_service",
    "merchant_service",
    "interaction_service",
    "notification_service",
    "admin_audit_service",
]
BASE_SET = set(BASE_SERVICES)

# ------------------------------------------------------------
# Emitter-derivation inputs (mirror the runners verbatim).
# ------------------------------------------------------------

# chaos6x18_v3_runner.OTEL_SHORT — carrier_service -> short OTEL suffix.
OTEL_SHORT = {
    "catalog_service": "catalog",
    "order_service": "order",
    "announcement_service": "announcement",
    "inventory_service": "inventory",
    "pricing_service": "pricing",
    "checkout_service": "checkout",
}

# chaos6x18_v3 single-position fault "short" codes (the {short} token). Sourced
# from every _proc_site/site dict in chaos6x18_v3_runner.py.
V3_SHORTS = [
    "applat",        # application_latency
    "runtimeexc",    # runtime_exception
    "poolex",        # resource_exhaustion
    "concurlimit",   # concurrency_limit_misconfiguration
    "restart",       # service_restart
    "flap",          # flapping_restart
    "deperr",        # dependency_return_error
    "timeoutcfg",    # timeout_misconfiguration
    "netdelay",      # network_delay
    "depdelay",      # dependency_delay
    "depfail",       # dependency_failure
    "cpustress",     # cpu_stress (host; probe persistent svc, listed for safety)
    "memstress",     # memory_stress (host; probe persistent svc)
]

# Carrier services that actually spawn a temp instance under
# f"{svc}_{short}_{OTEL_SHORT[svc]}" (carrier_service of every v3 site that
# starts a temp instance: proc / dep_5xx / timeout_cfg / toxi_via_carrier).
# = exactly the keys of OTEL_SHORT (the only services with an ALT_PORT + short).
V3_CARRIERS = list(OTEL_SHORT.keys())

# chaos_dualroot_runner combo ids (the {combo_id} token). One per COMBOS entry.
# DR12/DR13 added for the dual-root group-aware gap-fill (TASK-Z22 §新增组合设计审核):
#   DR12 checkout_dual_edge — temp checkout + temp pricing (bare-short leading,
#        handled via DR05_BARE_LEADING below);
#   DR13 dual_restart — temp catalog@15005 + temp inventory@15013 (full-base
#        leading f"{svc}_{combo_id}_{OTEL_SHORT[svc]}", handled via
#        DUALROOT_TEMP_CARRIERS below).
DUALROOT_COMBO_IDS = ["DR01", "DR02", "DR03", "DR05", "DR06", "DR08", "DR09",
                      "DR12", "DR13"]

# Dualroot carrier services that bake a temp instance under
# f"{svc}_{combo_id}_{OTEL_SHORT.get(svc, svc)}". From the carrier dicts:
#   DR01 order_service, DR02 catalog_service, DR06 catalog_service,
#   DR08 order_service, DR09 order_service. (DR03 toxi_edge + DR05 special are
#   handled separately below.)
#   DR13 dual_restart adds catalog_service + inventory_service temp instances
#   under the full-base pattern -> inventory_service added here so
#   f"inventory_service_DR13_inventory" folds via the explicit CANON_MAP.
DUALROOT_TEMP_CARRIERS = ["order_service", "catalog_service", "inventory_service"]

# DR05 + DR12 spawn two temp instances under BARE-SHORT leading tokens:
#   f"checkout_{combo_id}_checkout"  /  f"pricing_{combo_id}_pricing"
# Map (short_leading_token -> base service). DR12 reuses the same naming as DR05
# (checkout dual-edge carrier), so the DUALROOT_COMBO_IDS expansion below covers
# both combo ids.
DR05_BARE_LEADING = {
    "checkout": "checkout_service",
    "pricing": "pricing_service",
}

# agentchaos temp instance OTEL_SERVICE_NAME.
AGENTCHAOS_NAMES = {
    "recommendation_agent_taskx": "recommendation_agent",
}


def _build_canon_map() -> dict:
    """Programmatically derive the EXPLICIT alias table from all emitters.

    Every entry maps an exact emitter OTEL name -> its BASE service. Built so a
    refactor of the runner naming surfaces here (re-run smoke) rather than
    silently drifting.
    """
    m: dict = {}

    # (0) Identity: each base maps to itself (persistent-service emitters).
    for b in BASE_SERVICES:
        m[b] = b

    # (1) chaos6x18_v3 single-position:
    #     full     f"{svc}_{short}_{OTEL_SHORT[svc]}"
    #     truncated f"{svc}_{short}"  (older generation seen live)
    for svc in V3_CARRIERS:
        suffix = OTEL_SHORT[svc]
        for short in V3_SHORTS:
            m[f"{svc}_{short}_{suffix}"] = svc   # full
            m[f"{svc}_{short}"] = svc            # truncated (base + short)

    # (2) chaos_dualroot single_temp / single_temp_pool / dr02 / dual_sasrec_order:
    #     f"{svc}_{combo_id}_{OTEL_SHORT.get(svc, svc)}"
    for svc in DUALROOT_TEMP_CARRIERS:
        suffix = OTEL_SHORT.get(svc, svc)
        for cid in DUALROOT_COMBO_IDS:
            m[f"{svc}_{cid}_{suffix}"] = svc

    # (3) DR05 bare-short leading-token temp instances:
    #     f"checkout_{combo_id}_checkout" / f"pricing_{combo_id}_pricing"
    for short_lead, base in DR05_BARE_LEADING.items():
        suffix = OTEL_SHORT.get(base, base)   # 'checkout' / 'pricing'
        for cid in DUALROOT_COMBO_IDS:
            m[f"{short_lead}_{cid}_{suffix}"] = base

    # (4) agentchaos.
    m.update(AGENTCHAOS_NAMES)

    return m


# EXPLICIT alias table (the mandated CANON_MAP).
CANON_MAP = _build_canon_map()

# Bases sorted longest-first for the prefix fallback (so e.g. review_query_service
# wins over a hypothetical shorter prefix; token-boundary check makes it exact).
_BASES_BY_LEN = sorted(BASE_SERVICES, key=len, reverse=True)

# Module-level accumulator so callers that don't pass on_unknown still get a
# surfaced, inspectable record of unmapped emitters (never silent).
UNKNOWN_SEEN: dict = {}


def canonicalize(
    otel_name: Optional[str],
    on_unknown: Optional[Callable[[str], None]] = None,
) -> Tuple[Optional[str], str]:
    """Fold an emitter OTEL/exported_job/serviceName into its BASE service.

    Returns (base_or_None, matched_by) where matched_by in
    {'canon','prefix','unknown','empty'}.

      * 'canon'   — exact hit in the explicit CANON_MAP.
      * 'prefix'  — longest BASE_SERVICES prefix on a '_' token boundary.
      * 'unknown' — no match; base is None and the name is SURFACED (recorded in
                    UNKNOWN_SEEN and passed to on_unknown) — never misattributed.
      * 'empty'   — name is None/blank.
    """
    if not otel_name:
        return None, "empty"
    name = otel_name.strip()
    if not name:
        return None, "empty"

    # (1) exact explicit alias.
    hit = CANON_MAP.get(name)
    if hit is not None:
        return hit, "canon"

    # (2) longest base prefix on a token boundary (so 'order_service_xyz' folds
    #     to 'order_service' but 'orders_foo' does NOT fold to any base).
    for base in _BASES_BY_LEN:
        if name == base or name.startswith(base + "_"):
            return base, "prefix"

    # (3) unknown — surface, do not guess.
    UNKNOWN_SEEN[name] = UNKNOWN_SEEN.get(name, 0) + 1
    if on_unknown is not None:
        on_unknown(name)
    return None, "unknown"


def reset_unknowns() -> None:
    """Clear the module-level unknown accumulator (per-run housekeeping)."""
    UNKNOWN_SEEN.clear()


__all__ = [
    "BASE_SERVICES",
    "BASE_SET",
    "CANON_MAP",
    "OTEL_SHORT",
    "canonicalize",
    "UNKNOWN_SEEN",
    "reset_unknowns",
]
