#!/usr/bin/env bash
# Phase1 承重闸 bootstrap — builds a THROWAWAY venv that INHERITS the conda
# recweb2 stack via --system-site-packages, then installs ONLY the openinference
# instrumentation slice (--no-deps, so it cannot drag a shadow copy of
# otel/wrapt/langchain_core into the venv). Pinned to the Phase0-already-PASS
# openinference-instrumentation-langchain==0.1.67.
#
# Why --system-site-packages + --no-deps (NOT Phase0's isolated-venv+full-stack):
# Phase1 must exercise the SAME langchain/langchain_core/wrapt/otel BYTECODE that
# the real rec_agent runs under. --system-site-packages makes the venv see conda's
# copies; --no-deps confines the install to the 3 openinference wheels only. This
# maximizes same-source-ness and collapses the compatibility unknown to the single
# openinference<->conda-langchain variable. The cost is a SHADOWING risk (a shared
# module silently resolving to a venv copy) which the post-install __file__-origin
# assertion below catches (version-equality alone is NOT sufficient).
#
# RED LINES honored:
#   - installs NOTHING into conda env recweb2 (throwaway venv under scratchpad/).
#   - never upgrades langchain (no langchain* on the install line at all).
#   - all pip/network steps bypass Clash (NO_PROXY='*' + cleared HTTP(S)_PROXY).
#   - touches zero services / runners / workflow.py.
#
# Overridable via env:
#   CONDA_PY     = python3  (venv base interp)
#   PHASE1_VENV  = <repo>/scratchpad/phase1_venv                   (throwaway venv dir)
# Exit codes: 0=venv ready, 2=venv-build fail, 3=pip fail, 4=drift/shadow fail.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
CONDA_PY="${CONDA_PY:-python3}"
PHASE1_VENV="${PHASE1_VENV:-$REPO_ROOT/scratchpad/phase1_venv}"
SMOKE_DIR="$REPO_ROOT/(v1)_smoke"
mkdir -p "$SMOKE_DIR" "$REPO_ROOT/scratchpad"
PIP_LOG="$SMOKE_DIR/phase1_pip_install.log"

# --- expected origin for shared modules (conda base; case-insensitive on win) ---
CONDA_BASE_NORM="$("$CONDA_PY" -c 'import sys,os.path; print(os.path.normpath(sys.prefix).replace(chr(92),"/").lower())')"
echo "[phase1] conda base = $CONDA_BASE_NORM"

# --- Clash bypass for pip/PyPI steps only (instance run step uses a different policy) ---
export NO_PROXY='*' no_proxy='*'
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy

echo "[phase1] CONDA_PY    = $CONDA_PY"
echo "[phase1] PHASE1_VENV = $PHASE1_VENV"

# 1) build venv inheriting conda site-packages
if [ ! -x "$PHASE1_VENV/Scripts/python.exe" ]; then
  echo "[phase1] creating venv (--system-site-packages) ..."
  "$CONDA_PY" -m venv "$PHASE1_VENV" --system-site-packages || {
    echo "[phase1] FATAL: venv creation failed"; exit 2; }
fi
VPY="$PHASE1_VENV/Scripts/python.exe"

# sanity: conda langchain must be visible THROUGH the venv (proves --system-site-packages)
if ! "$VPY" -c "import langchain, langchain_core" 2>/dev/null; then
  echo "[phase1] FATAL: venv does not see conda langchain (--system-site-packages broken)"; exit 2
fi

# 2) install ONLY the openinference slice, NO deps (so nothing shadows conda).
#    -c constraints.txt is belt-and-suspenders: it cannot relax versions, and with
#    --no-deps pip won't even consult deps. The langchain instr is pinned to 0.1.67
#    (Phase0 PASS version); the other two float (base satisfies their >= deps).
echo "[phase1] installing openinference slice (--no-deps) ... log -> $PIP_LOG"
"$VPY" -m pip install --disable-pip-version-check --no-deps \
    -c "$HERE/constraints.txt" \
    openinference-instrumentation-langchain==0.1.67 \
    openinference-instrumentation \
    openinference-semantic-conventions \
    >"$PIP_LOG" 2>&1
PIP_RC=$?
if [ $PIP_RC -ne 0 ]; then
  echo "[phase1] pip install FAILED (rc=$PIP_RC). tail of log:"
  tail -n 40 "$PIP_LOG"
  exit 3
fi

# 3) post-install DRIFT/SHADOW detector (stricter than Phase0):
#    shared modules MUST (a) report exact version AND (b) resolve __file__ to conda.
#    openinference slice MUST resolve to the venv. Any shared module landing in the
#    venv site-packages = shadowing -> NO-GO.
echo "[phase1] post-install drift/shadow check:"
"$VPY" - "$PHASE1_VENV" "$CONDA_BASE_NORM" <<'PYCHECK' || { echo "[phase1] FATAL: drift/shadow detected — NO-GO"; exit 4; }
import sys, os
venv_norm   = os.path.normpath(sys.argv[1]).replace("\\","/").lower()
conda_norm  = os.path.normpath(sys.argv[2]).replace("\\","/").lower()
def n(p): return os.path.normpath(p or "").replace("\\","/").lower()

checks = [
    ("langchain",      "0.3.24"),
    ("langchain_core", "0.3.56"),
    ("wrapt",          "2.1.2"),
]
fails = []
for mod, exp in checks:
    m = __import__(mod)
    ver = getattr(m, "__version__", "?")
    f  = n(getattr(m, "__file__", "") or "")
    origin = "conda" if conda_norm in f else ("venv" if venv_norm in f else "other")
    print(f"  {mod:14s} v={ver:9s} origin={origin:5s} {f}")
    if ver != exp:
        fails.append(f"{mod} version {ver} != {exp}")
    if venv_norm in f:
        fails.append(f"{mod} SHADOWED into venv ({f})")
    elif conda_norm not in f:
        fails.append(f"{mod} NOT in conda base ({f})")

# openinference slice expected in VENV (NOT conda — conda has no openinference)
try:
    import openinference.instrumentation.langchain as oilc
    of = n(getattr(oilc, "__file__", "") or "")
    print(f"  openinference.langchain v={getattr(oilc,'__version__','?')} origin={'venv' if venv_norm in of else 'other'} {of}")
    if venv_norm not in of:
        fails.append(f"openinference.langchain NOT in venv ({of}) — expected venv install")
    # real API surface the loader depends on
    from openinference.instrumentation.langchain import LangChainInstrumentor
    li = LangChainInstrumentor()
    assert callable(getattr(li, "instrument", None)), "LangChainInstrumentor.instrument missing"
    # is_instrumented_by_opentelemetry is a BOOLEAN PROPERTY here (not a method);
    # guard against a future rev flipping it to a method by accepting either form.
    flagged = getattr(li, "is_instrumented_by_opentelemetry", None)
    assert flagged is not None, "is_instrumented_by_opentelemetry missing"
except Exception as e:
    fails.append(f"openinference import/API error: {e!r}")

if fails:
    print("  DRIFT/SHADOW FAIL:")
    for x in fails: print("    -", x)
    sys.exit(1)
print("  OK: shared stack pinned + conda-origin; openinference slice in venv only.")
PYCHECK

echo "[phase1] venv ready: $VPY"
exit 0
