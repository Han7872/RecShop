#!/usr/bin/env bash
# Phase0 承重闸 bootstrap — builds a THROWAWAY venv, pins the prod stack, installs
# openinference-instrumentation-langchain (constrained so it CANNOT bump langchain),
# asserts the pins survived, then runs the offline content-span smoke.
#
# RED LINES honored:
#   - installs NOTHING into conda env recweb2 (venv lives under scratchpad, isolated).
#   - never upgrades langchain (constraints.txt pins the whole stack; -c on every install).
#   - all pip/network bypasses Clash (NO_PROXY='*' + cleared HTTP(S)_PROXY).
#   - touches zero services / runner / workflow.py.
#
# Overridable via env:
#   CONDA_PY      = python3   (venv base interpreter)
#   PHASE0_VENV   = <scratchpad>/phase0_venv                        (throwaway venv dir)
# Extra args (e.g. --console, --live) are forwarded to the smoke script.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
OUT_DIR="$REPO_ROOT/(v1)_smoke"
mkdir -p "$OUT_DIR"

CONDA_PY="${CONDA_PY:-python3}"
PHASE0_VENV="${PHASE0_VENV:-$OUT_DIR/../../../.phase0_venv_MISSING}"
CONSTRAINTS="$HERE/constraints.txt"
PIP_LOG="$OUT_DIR/phase0_pip_install.log"

# Clash bypass for every network step
export NO_PROXY='*' no_proxy='*'
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy

echo "[phase0] CONDA_PY   = $CONDA_PY"
echo "[phase0] PHASE0_VENV= $PHASE0_VENV"
echo "[phase0] constraints= $CONSTRAINTS"

# 1) build isolated venv (NO --system-site-packages: must NOT see conda's langchain,
#    else -c could skip the reinstall and mask drift)
if [ ! -x "$PHASE0_VENV/Scripts/python.exe" ]; then
  echo "[phase0] creating isolated venv ..."
  "$CONDA_PY" -m venv "$PHASE0_VENV" || { echo "[phase0] FATAL: venv creation failed"; exit 2; }
fi
VPY="$PHASE0_VENV/Scripts/python.exe"

# guard: prove isolation (langchain must NOT already be importable before install)
if "$VPY" -c "import langchain" 2>/dev/null; then
  echo "[phase0] FATAL: venv leaked conda site-packages (langchain visible pre-install)"; exit 2
fi

# 2) install the pinned stack + openinference, CONSTRAINED. A resolution FAILURE here is a
#    legitimate NO-GO (the compat answer) — do NOT loosen the langchain pins to satisfy it.
echo "[phase0] installing (constrained, Clash-bypassed) ... log -> $PIP_LOG"
"$VPY" -m pip install --disable-pip-version-check \
    -c "$CONSTRAINTS" \
    langchain==0.3.24 langchain-core==0.3.56 langchain-openai==0.3.14 langgraph==0.3.24 \
    opentelemetry-api==1.42.1 opentelemetry-sdk==1.42.1 \
    openinference-instrumentation-langchain \
    >"$PIP_LOG" 2>&1
PIP_RC=$?
if [ $PIP_RC -ne 0 ]; then
  echo "[phase0] pip install FAILED (rc=$PIP_RC) — this may itself be the compat NO-GO."
  echo "[phase0] ---- tail of $PIP_LOG ----"
  tail -n 40 "$PIP_LOG"
  exit 3
fi

# 3) post-install pin assert = the 0.3.24-compat proof (drift detector)
echo "[phase0] post-install pin check:"
"$VPY" -m pip list 2>/dev/null | grep -Ei "^(langchain|langchain-core|langchain-openai|langgraph|opentelemetry-api|opentelemetry-sdk|openinference|wrapt) "
LC=$("$VPY" -c "import langchain,langchain_core; print(langchain.__version__, langchain_core.__version__)" 2>/dev/null)
echo "[phase0] langchain / langchain-core = $LC"
if [ "$LC" != "0.3.24 0.3.56" ]; then
  echo "[phase0] FATAL: pins DRIFTED ($LC) — openinference bumped the stack. NO-GO."
  exit 4
fi

# 4) run the offline content-span smoke under the venv python
echo "[phase0] running smoke ..."
"$VPY" "$HERE/phase0_smoke_openinference.py" "$@"
SMOKE_RC=$?
echo "[phase0] smoke rc=$SMOKE_RC  (0=PASS, 1=NO-GO)"
exit $SMOKE_RC
