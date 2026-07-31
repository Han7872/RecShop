#!/usr/bin/env bash
# Phase1 承重闸 orchestrator (main loop drives this). Sequence:
#   1) bootstrap  -> throwaway venv (scratchpad/phase1_venv) with openinference slice
#   2) launch+probe each mode (normal-first). Fail-fast on normal ENV-GAP so we don't
#      burn DeepSeek calls on delay/garbage/error when the stack isn't up.
#   3) verify     -> offline judge, writes phase1_verdict.json
#
# This orchestrator does NOT touch services/ or the conda env. Launcher + verifier run
# under the main conda python (stdlib only); the venv python is used ONLY for the
# spawned rec_agent instance (so it inherits conda langchain AND has openinference).
#
# Env knobs:
#   CONDA_PY            (default python3)
#   PHASE1_INSTRUMENT_MODE  minimal (default, proven primary) | monkeypatch (fallback)
#   PHASE1_SKIP_BOOTSTRAP  1 = assume venv already built (re-runs after a bootstrap pass)
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
CONDA_PY="${CONDA_PY:-python3}"
SMOKE_DIR="$REPO/(v1)_smoke"
mkdir -p "$SMOKE_DIR"

echo "=========================================================="
echo " Phase1 load-bearing gate (openinference content mounting)"
echo "   CONDA_PY = $CONDA_PY"
echo "   mode     = ${PHASE1_INSTRUMENT_MODE:-minimal}"
echo "=========================================================="

# 1) bootstrap the throwaway venv
if [ "${PHASE1_SKIP_BOOTSTRAP:-0}" != "1" ]; then
  bash "$HERE/phase1_bootstrap.sh"
  BS_RC=$?
  if [ $BS_RC -ne 0 ]; then
    echo "[phase1-run] bootstrap failed (rc=$BS_RC) -> NO-GO at venv layer."
    exit $BS_RC
  fi
fi

# 2) normal-first launch. Fail-fast on env-gap (health/probe failure).
"$CONDA_PY" "$HERE/phase1_launcher.py" normal
N_RC=$?
NORMAL_PROBE="$SMOKE_DIR/phase1_normal_probe.json"
NORMAL_OK=0
if [ -f "$NORMAL_PROBE" ]; then
  # jq-free portable check: python one-liner reads the probe json
  NORMAL_OK=$("$CONDA_PY" - "$NORMAL_PROBE" <<'PY'
import json, sys
try:
    p = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception:
    print(0); sys.exit(0)
# OK to proceed only if NOT env_gap AND http 200 AND response success
ok = (not p.get("env_gap")) and p.get("http_status") == 200 and bool((p.get("resp") or {}).get("success"))
print(1 if ok else 0)
PY
)
fi
if [ "$NORMAL_OK" != "1" ]; then
  echo "[phase1-run] normal mode did not return a healthy 200 response (env-gap)."
  echo "[phase1-run] Check sasrec_api :8200, DeepSeek reachability, and the server log:"
  echo "             $SMOKE_DIR/phase1_normal_server.log"
  echo "[phase1-run] HALTING before delay/garbage/error to avoid wasting DeepSeek calls."
  # still run the verifier so a phase1_verdict.json (INCONCLUSIVE) is emitted
  "$CONDA_PY" "$HERE/phase1_smoke_verify.py" --modes normal || true
  exit 3
fi

# 3) remaining modes
for M in delay garbage error; do
  "$CONDA_PY" "$HERE/phase1_launcher.py" "$M" || echo "[phase1-run] launcher $M returned non-zero (verifier will judge from artifacts)"
done

# 4) offline verify (normal-first fail-fast is handled inside the verifier too)
"$CONDA_PY" "$HERE/phase1_smoke_verify.py"
V_RC=$?
echo "[phase1-run] verify rc=$V_RC (0=PASS, 1=NO-GO, 2=INCONCLUSIVE)"
exit $V_RC
