#!/usr/bin/env bash
# =============================================================================
# run_eval_agentfault.sh  —  one-click agent-fault EVAL on an EXISTING dataset
# -----------------------------------------------------------------------------
# Runs pipeline steps 3-10 (see scripts/chaos/agentfault/README.md).
#
# DEFAULT = OFFLINE / FREE steps only:
#   3  compute_context_drift_outcome.py     -> <dir>/context_drift_outcomes.json
#   4  eval/eval_agentfault_tierA.py         -> <dir>/BASELINE_RESULTS.md
#   5  eval/infra_negatives/run_infra_negatives.py (--method all --channel both)
#                                            -> <dir>/infra_negatives/
#   6  eval/content_ctxdrift_track.py        -> <dir>/RESULTS_CONTENT_CTXDRIFT.md
#   7  eval/whowhen/make_whowhen_cases.py    -> <dir>/whowhen/cases/
#   9  eval/whowhen/score_whowhen.py         -> <dir>/whowhen/whowhen_results[_<tag>].json
#      (scores WHATEVER judge outputs already exist; free)
#   10 eval/whowhen/build_whowhen_format_delivery.py -> (upstream whowhen view)
#
# OPTIONAL PAID steps (additive flags; NOT run by default):
#   --with-judge   step 8 run_whowhen with DeepSeek  (COSTS MONEY: judge API)
#   --with-glm     step 8 run_whowhen with GLM-5.2    (COSTS MONEY + slow, hours)
#
# Idempotent for the offline steps (safe to re-run; they overwrite their own
# deterministic outputs). No live collection here — needs an existing tree.
# =============================================================================
set -euo pipefail

# ---- locate repo root, cd there ---------------------------------------------
# ★2026-07-27:与 run_collect_agentfault.sh 同款修复(改一处必查同款,本仓已犯过五次)。
#   裸 `pwd` 给 MSYS 风格 /path/to/repo/...,把它拼成脚本路径传给 **Windows 版 python.exe** 时,
#   若 python 的 cwd 恰好在盘根,`/d/...` 会被当【相对根路径】拼到盘符后 →
#   D:\path\to\repo\... → "can't open file"(2026-07-27 实测复现:cwd=D:\ 时
#   os.path.abspath("/path/to/repo/x.py") == "D:\path\to\repo\x.py")。
#   本脚本此前没炸只是 cwd 恰好合适,属**潜伏**同款风险,一并修。
#   ⚠ 不能写 `cd X && pwd -W || cd X && pwd`(&&/|| 同优先级左结合会两个 pwd 都跑且 fallback cd 失败)。
_abspath() {  # $1 = 目录;Git Bash 下输出 D:/... 正斜杠风格,其它 shell 回退原生绝对路径
  ( cd "$1" || exit 1
    p="$(pwd -W 2>/dev/null)" || p=""
    [ -n "$p" ] || p="$(pwd)"
    printf %s "$p" )
}
SCRIPT_DIR="$(_abspath "$(dirname "${BASH_SOURCE[0]}")")"
REPO_ROOT="$(_abspath "${SCRIPT_DIR}/../../..")"
cd "${REPO_ROOT}"

# ---- environment iron rules --------------------------------------------------
export NO_PROXY='*'
export PYTHONIOENCODING=utf-8
PY="${CONDA_PY:-python3}"

# ---- defaults / args ---------------------------------------------------------
DATASET_DIR="(archived) agentfault_v2"
WITH_JUDGE=0
WITH_GLM=0

# GLM cross-family judge knobs (per SPEC; glm-5.2 thinking model needs a large budget)
GLM_API_BASE="https://open.bigmodel.cn/api/paas/v4"
GLM_API_KEY_ENV="GLM_API_KEY"
GLM_MODEL="glm-5.2"
GLM_MAX_TOKENS=8192
GLM_TAG="glm52"

usage() {
  cat <<'EOF'
run_eval_agentfault.sh — one-click agent-fault EVAL (steps 3-10) on an EXISTING tree

USAGE:
  bash scripts/chaos/agentfault/run_eval_agentfault.sh [options]

OPTIONS:
  --dataset-dir DIR   dataset tree root         (default: (archived) agentfault_v2)
  --with-judge        ALSO run step 8 DeepSeek judge   (COSTS MONEY)
  --with-glm          ALSO run step 8 GLM-5.2 judge    (COSTS MONEY + hours)
  -h, --help          show this help and exit

DEFAULT (no flags) runs only OFFLINE/FREE steps: 3,4,5,6,7,9,10.
  Step 9 scores whatever judge outputs already exist (deepseek and/or glm52) for free.
  Step 10 always builds the delivery view from (archived) agentfault_v2 (it is
  hardcoded to that path and takes no arguments).

⚠️ --with-judge / --with-glm hit paid judge APIs (DeepSeek / GLM). Each is
   additive; you may pass both.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset-dir) DATASET_DIR="${2:?--dataset-dir needs a value}"; shift 2 ;;
    --with-judge)  WITH_JUDGE=1; shift ;;
    --with-glm)    WITH_GLM=1; shift ;;
    -h|--help)     usage; exit 0 ;;
    *) echo "ERROR: unknown arg '$1' (see --help)" >&2; exit 2 ;;
  esac
done

# script paths
S3="${SCRIPT_DIR}/compute_context_drift_outcome.py"
S4="${SCRIPT_DIR}/eval/eval_agentfault_tierA.py"
S5="${SCRIPT_DIR}/eval/infra_negatives/run_infra_negatives.py"
S6="${SCRIPT_DIR}/eval/content_ctxdrift_track.py"
S7="${SCRIPT_DIR}/eval/whowhen/make_whowhen_cases.py"
S8="${SCRIPT_DIR}/eval/whowhen/run_whowhen.py"
S9="${SCRIPT_DIR}/eval/whowhen/score_whowhen.py"
S10="${SCRIPT_DIR}/eval/whowhen/build_whowhen_format_delivery.py"

# delivery view is HARDCODED inside step 10 (no args): always (archived) agentfault_v2
DELIVERY_DIR="(archived) agentfault_v2_whowhen"

echo "=============================================================="
echo " agent-fault EVAL  (steps 3-10)"
echo "   repo root   : ${REPO_ROOT}"
echo "   dataset-dir : ${DATASET_DIR}"
echo "   with-judge  : $([[ ${WITH_JUDGE} -eq 1 ]] && echo yes || echo no)"
echo "   with-glm    : $([[ ${WITH_GLM} -eq 1 ]] && echo yes || echo no)"
echo "=============================================================="

if [[ ! -f "${DATASET_DIR}/dataset_agentfault.csv" ]]; then
  echo "✗ no dataset_agentfault.csv under '${DATASET_DIR}' — collect first (run_collect_agentfault.sh)." >&2
  exit 1
fi

if [[ ${WITH_JUDGE} -eq 1 || ${WITH_GLM} -eq 1 ]]; then
  echo ""
  echo "  ⚠️  PAID JUDGE ENABLED — this run will hit a paid LLM-judge API"
  [[ ${WITH_JUDGE} -eq 1 ]] && echo "       --with-judge : DeepSeek judge over all whowhen cases"
  [[ ${WITH_GLM}   -eq 1 ]] && echo "       --with-glm   : GLM-5.2 cross-family judge (slow; can take hours)"
  echo "     Cost note: each --method all pass = 4 methods x N cases of judge calls."
fi

# ---- helpers -----------------------------------------------------------------
# do_step LABEL EXPECTED_OUTPUT -- CMD...   : run, hard-fail on error or missing output
do_step() {
  local label="$1"; local out="$2"; shift 2
  echo ""
  echo "════════ ${label}"
  echo "  \$ $*"
  if ! "$@"; then
    echo "  ✗ ${label} — command FAILED (see output above)"
    exit 1
  fi
  if [[ -e "${out}" ]]; then
    echo "  ✓ ${label} — output: ${out}"
  else
    echo "  ✗ ${label} — expected output MISSING: ${out}"
    exit 1
  fi
}

# ---- STEP 3: context_drift outcome  [offline] -------------------------------
do_step "STEP 3  context_drift outcome" \
        "${DATASET_DIR}/context_drift_outcomes.json" \
        "${PY}" "${S3}" --dataset-dir "${DATASET_DIR}"

# ---- STEP 4: Tier-A offline baselines  [offline] ----------------------------
do_step "STEP 4  Tier-A baselines" \
        "${DATASET_DIR}/BASELINE_RESULTS.md" \
        "${PY}" "${S4}" --dataset-dir "${DATASET_DIR}"

# ---- STEP 5: BARO/RCD infra negatives  [offline] ----------------------------
do_step "STEP 5  infra negatives (BARO/RCD, corrected+raw)" \
        "${DATASET_DIR}/infra_negatives/infra_negatives_results.json" \
        "${PY}" "${S5}" --method all --channel both --dataset-dir "${DATASET_DIR}"

# ---- STEP 6: content context-drift track  [offline] -------------------------
# depends on STEP 3 (context_drift_outcomes.json) + STEP 4 (BASELINE_RESULTS.md)
do_step "STEP 6  content context-drift track" \
        "${DATASET_DIR}/RESULTS_CONTENT_CTXDRIFT.md" \
        "${PY}" "${S6}" --dataset-dir "${DATASET_DIR}"

# ---- STEP 7: build Who&When cases  [offline] --------------------------------
do_step "STEP 7  Who&When cases" \
        "${DATASET_DIR}/whowhen/cases_index.json" \
        "${PY}" "${S7}" --dataset-dir "${DATASET_DIR}"

# ---- STEP 8 (optional, PAID): run Who&When judges ---------------------------
if [[ ${WITH_JUDGE} -eq 1 ]]; then
  do_step "STEP 8  run_whowhen DeepSeek judge  [PAID]" \
          "${DATASET_DIR}/whowhen/outputs/all_at_once_deepseek.txt" \
          "${PY}" "${S8}" --method all --dataset-dir "${DATASET_DIR}" --tag deepseek
fi
if [[ ${WITH_GLM} -eq 1 ]]; then
  do_step "STEP 8b run_whowhen GLM-5.2 judge  [PAID, slow]" \
          "${DATASET_DIR}/whowhen/outputs/all_at_once_${GLM_TAG}.txt" \
          "${PY}" "${S8}" --method all --dataset-dir "${DATASET_DIR}" \
                  --api-base "${GLM_API_BASE}" --api-key-env "${GLM_API_KEY_ENV}" \
                  --model "${GLM_MODEL}" --max-tokens "${GLM_MAX_TOKENS}" --tag "${GLM_TAG}"
fi

# ---- STEP 9: score Who&When outputs  [offline, free] ------------------------
# Score whatever judge outputs exist. DeepSeek default tag; GLM tag if present.
DS_OUTPUTS_EXIST=0
if ls "${DATASET_DIR}"/whowhen/outputs/*_deepseek.txt >/dev/null 2>&1; then DS_OUTPUTS_EXIST=1; fi
GLM_OUTPUTS_EXIST=0
if ls "${DATASET_DIR}"/whowhen/outputs/*_${GLM_TAG}.txt >/dev/null 2>&1; then GLM_OUTPUTS_EXIST=1; fi

if [[ ${DS_OUTPUTS_EXIST} -eq 1 ]]; then
  do_step "STEP 9  score_whowhen (deepseek)" \
          "${DATASET_DIR}/whowhen/whowhen_results.json" \
          "${PY}" "${S9}" --dataset-dir "${DATASET_DIR}"
else
  echo ""
  echo "════════ STEP 9  score_whowhen (deepseek)"
  echo "  ⚠️  skipped — no whowhen/outputs/*_deepseek.txt yet."
  echo "     Produce them first with:  --with-judge"
fi

if [[ ${GLM_OUTPUTS_EXIST} -eq 1 ]]; then
  do_step "STEP 9b score_whowhen (${GLM_TAG})" \
          "${DATASET_DIR}/whowhen/whowhen_results_${GLM_TAG}.json" \
          "${PY}" "${S9}" --dataset-dir "${DATASET_DIR}" --tag "${GLM_TAG}"
elif [[ ${WITH_GLM} -eq 1 ]]; then
  echo "  ✗ STEP 9b — --with-glm set but no ${GLM_TAG} outputs were produced" >&2
  exit 1
fi

# ---- STEP 10: build Who&When-format delivery view  [offline] ----------------
# NOTE: step 10 takes NO arguments and is HARDCODED to (archived) agentfault_v2.
if [[ "${DATASET_DIR}" == "(archived) agentfault_v2" ]]; then
  do_step "STEP 10 build_whowhen_format_delivery" \
          "${DELIVERY_DIR}/cases_index.json" \
          "${PY}" "${S10}"
else
  echo ""
  echo "════════ STEP 10 build_whowhen_format_delivery"
  echo "  ⚠️  skipped — this step is hardcoded to (archived) agentfault_v2 (no args),"
  echo "     but --dataset-dir='${DATASET_DIR}'. Run against the default tree to build the view."
fi

echo ""
echo "=============================================================="
echo " EVAL DONE."
if [[ ${WITH_JUDGE} -eq 1 || ${WITH_GLM} -eq 1 ]]; then
  echo " NOTE: paid judge API calls were made this run"
  echo "       ($([[ ${WITH_JUDGE} -eq 1 ]] && echo 'DeepSeek ')$([[ ${WITH_GLM} -eq 1 ]] && echo 'GLM-5.2')) — check your API billing."
fi
echo "=============================================================="
