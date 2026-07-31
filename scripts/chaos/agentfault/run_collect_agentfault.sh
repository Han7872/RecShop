#!/usr/bin/env bash
# =============================================================================
# run_collect_agentfault.sh  —  one-click agent-fault dataset COLLECTION
# -----------------------------------------------------------------------------
# Produces (into --out-dir):
#   dataset_agentfault.csv        (APPEND-ONLY main table)
#   journal/  raw/  ledgers/  spans/   run_summary.json
#   assets/carrier_pool.json      (only (re)built here if it is MISSING)
#
# This is pipeline steps 1-2 of the agent-fault pipeline
# (see scripts/chaos/agentfault/README.md for the full 10-step list).
#
# TWO BACKENDS (--backend, default local) ─────────────────────────────────────
#   local  本机隔离 harness:phase1 venv 起一个临时 rec_agent 进程 + 真实 SASRec:8200。
#          = (upstream batch) 那 108 case 的口径。**行为与加 backend 之前逐字节一致**。
#   k8s    25 微服务全栈:kubectl set env 打常驻 rec-agent pod + rollout,探针经
#          kubectl proxy 的 service proxy。用途 = 消掉交付包里"agent 跑在全栈内"的
#          overclaim(v2 其实只跑了 rec-agent + sasrec 两个进程)。
#          combo 矩阵 / GT 逻辑 / CSV schema 与 local **完全同一套**(同一个 runner),
#          四套 eval 一行不用改;K8S 树的 CSV 只在**末尾**多 4 列口径/provenance 标签。
#
#   bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes                # 本机
#   bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes --backend k8s  # 全栈
#
# ⚠️ LIVE + PAID: step 2 drives the real rec_agent, which makes LIVE DeepSeek
#    API calls (the fault injector rewrites completions with a sub-LLM).
#    Collecting a full 9-combo x --runs sweep costs real money.
#
# ⚠️ APPEND-ONLY / NO --force: the runner appends to dataset_agentfault.csv and
#    resumes from journal/ (skips combos already journalled). There is NO
#    --force / no-overwrite mode. To RE-collect rows that already exist you must
#    FIRST manually strip the target rows from dataset_agentfault.csv AND delete
#    the matching journal/ raw/ ledgers/ spans/ files, otherwise you get
#    duplicate CSV rows and/or silently-skipped combos.
#    (The runner also refuses to mix two backends in one tree: it drops a
#     .collect_backend marker in non-local trees and FATALs on mismatch.)
#
# Requires the LIVE stack — local backend:
#   - SASRec on http://127.0.0.1:8200/health   (start: cd services/sasrec_api && python api_server.py)
#   - phase1 throwaway venv  scratchpad/phase1_venv/  (build: bash scripts/chaos/agentfault/phase1_bootstrap.sh)
#     The runner FATALs "run phase1_bootstrap.sh" if this venv is missing;
#     it spawns the rec_agent subprocess with THAT venv's python (VENV_PY),
#     NOT the conda python — do not try to override that.
# Requires the LIVE stack — k8s backend:
#   ★★完整的"从零到开采"有序清单(0..6 步)在 scripts/chaos/agentfault/README.md 的
#     §"K8S 分支:从零到开采",**别只照下面这几行**——它只是提要,四条硬前置里有两条
#     (25 服务全栈已部署 / 两个 secret)不在本脚本能检查的范围内。
#   - 25 服务全栈已 apply 到 ns recweb-chaos(见 k8s/pilot/README.md §全栈 bring-up)
#   - 两个 secret:db-cred(全栈)、deepseek-env(rec-agent 的 LLM 凭据)
#   - 宿主 Docker OTel 栈已起(Prometheus/collector 都在那里,不在 K8S 里):
#       docker compose -f ops/docker-compose.otel.yml up -d
#   - `kubectl proxy --port=8001 --address=0.0.0.0 --accept-hosts='.*'` 常驻(★不能 port-forward)
#   - PVC 里已灌 electronics.item:powershell -File scripts/chaos/agentfault/k8s/load_recagent_data.ps1
#   - rec-agent 已切采集变体:powershell -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1
#   - 载体池 assets/carrier_pool.json(已随仓库提交;若缺失,重建需**宿主** SASRec:8200
#     → 跑 k8s 分支也要求本机备齐 9.2GB pkl + 260MB pth。这是两个分支唯一共享的本机依赖)
#   - 采完还原(★不自动跑,怕清掉还没拉走的 span):
#       powershell -File scripts/chaos/agentfault/k8s/restore_recagent_stock.ps1 -ConfirmedCollected
#     ★不还原的后果:rec-agent 停在 :agentfault-v2 / 1536Mi / Recreate / 挂 PVC 的形态,
#       之后 traditional 线采到的内存上限与 pod 重建语义都与产出 255 的口径不一致(跨批次不可比)。
#   - B 档**不需要**装 Chaos Mesh(相反:ns 里有任何遗留 Chaos CRD 会被 preflight 拦下,
#     因为那会把 agent-only 的 B 档污染成跨层的 C/D 档)。
# =============================================================================
set -euo pipefail

# ---- locate repo root from this script's location, then cd there -------------
# ★2026-07-27 修(首轮 B 档采集被它坑过):必须用 `pwd -W` 而不是 `pwd`。
#   Git Bash 的 `pwd` 给 MSYS 风格 /d/AIProjects/...,把它拼进路径再传给
#   **Windows 版 python.exe**,会被解释成 D:\d\AIProjects\... → "can't open file"。
#   `pwd -W` 直接给 ${REPO_DIR}/...(正斜杠 Windows 风格),bash 与 Windows python 都认。
#   (纯 bash 用途如 `cd`/`[ -f ]` 用哪种都行,所以统一成 -W 最省事。)
#   ★写法坑(2026-07-27 二次修):不能写成 `cd X && pwd -W || cd X && pwd` ——
#   bash 的 && / || 同优先级左结合,会被解析成 `((cd X && pwd -W) || cd X) && pwd`,
#   结果两个 pwd 都执行(正常情形多打一行)、且 fallback 分支里 cd 是相对路径会失败 → 变量变空。
#   正解:**在同一个子 shell 里先 cd,再对 pwd 做判断**(下面这种)。
_abspath() {  # $1 = 目录;输出 Windows 正斜杠风格(Git Bash)或原生绝对路径(其它 shell)
  ( cd "$1" || exit 1
    p="$(pwd -W 2>/dev/null)" || p=""
    [ -n "$p" ] || p="$(pwd)"
    printf %s "$p" )
}
SCRIPT_DIR="$(_abspath "$(dirname "${BASH_SOURCE[0]}")")"
REPO_ROOT="$(_abspath "${SCRIPT_DIR}/../../..")"
cd "${REPO_ROOT}"

# ---- environment iron rules --------------------------------------------------
export NO_PROXY='*'            # bypass Clash / system proxy
export PYTHONIOENCODING=utf-8
PY="${CONDA_PY:-python3}"   # conda env recweb2

# ★注意:绝不能在这里 `export MSYS2_ARG_CONV_EXCL='*'`。
#   本脚本给 python.exe 传的 ${RUNNER} 是 MSYS 风格路径(/d/AIProjects/...),要靠 MSYS 的
#   自动转换才能变成 Windows 路径;全局关掉转换 = 本机分支直接跑不起来。
#   只有"参数里带 Linux 容器内路径"的那几条 kubectl 命令需要**逐条**前缀 MSYS2_ARG_CONV_EXCL='*'
#   (否则 /app/... 会被改写成 Windows 路径)。runner 内部走 subprocess 直调 kubectl、不过 shell,
#   天生没这个问题 —— 只有本脚本里手敲的 kubectl 有。

# phase1 throwaway venv python — mirrors injector_smoke.VENV_PY
# (REPO/scratchpad/phase1_venv/Scripts/python.exe). This is the venv the runner
# uses for the agent subprocess; we only PROBE it here, never override it.
VENV_PY="${REPO_ROOT}/scratchpad/phase1_venv/Scripts/python.exe"

CARRIER_POOL="${SCRIPT_DIR}/assets/carrier_pool.json"
RUNNER="${SCRIPT_DIR}/collect/agentfault_runner.py"
POOL_BUILDER="${SCRIPT_DIR}/build_carrier_pool.py"
SASREC_HEALTH="http://127.0.0.1:8200/health"

# ---- k8s backend knobs -------------------------------------------------------
K8S_NS="${K8S_NS:-recweb-chaos}"
K8S_DEPLOY="${K8S_DEPLOY:-rec-agent}"
K8S_PROXY="${K8S_PROXY:-http://127.0.0.1:8001}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
# ★必须是 agentfault-v2 不是 agentfault:下面/runner 里都是**子串**匹配,写 agentfault 会把
#   G1(single_recagent 15)用的旧 tag `:agentfault` 一起放行,而那个镜像没有 _filter_real_title
#   也没挂 PVC —— 与本机 v2 不同口径(审查 R7)。
IMAGE_HINT="${IMAGE_HINT:-agentfault-v2}"
KUBECTL="${KUBECTL:-$(command -v kubectl 2>/dev/null || echo '/c/Program Files/Docker/Docker/resources/bin/kubectl.exe')}"

# ---- defaults / args ---------------------------------------------------------
BACKEND="local"
OUT_DIR=""            # 空 = 按 backend 取默认值(见下)
RUNS=12               # ★复现 agentfault_v2 / agentfault_k8s 的口径就是 12
                      #   (runner 自己的 argparse default 是 5,直调 runner 时务必显式给 12)
ONLY=""
YES=0
ALLOW_RESIDUE=0
HOST_METRICS="prom"   # prom | none(--host-metrics 覆盖;none 时跳过 Prometheus 前检)
SKIP_CODE_PARITY=0
EXTRA_ARGS=()         # `-- <args>` 之后的一切,原样透传给 runner

usage() {
  cat <<'EOF'
run_collect_agentfault.sh — one-click agent-fault COLLECTION (steps 1-2)

USAGE:
  bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes [options]

OPTIONS:
  --yes                REQUIRED to proceed. Acknowledges LIVE, PAID DeepSeek calls.
  --backend local|k8s  execution backend (default: local)
                         local = 本机隔离 harness  (= (archived) agentfault_v2 口径)
                         k8s   = 25 微服务全栈的常驻 rec-agent pod
  --out-dir DIR        output tree root
                         default local -> (archived) agentfault_v2
                         default k8s   -> datasets/agentfault_k8s
  --runs N             reps per combo (K)          (default: 12)
  --only a,b,c         comma-separated combo ids to run (subset; default all 9)
  --allow-inject-residue
                       k8s only: 放行 pod 上残留的 AGENTFAULT_INJECT(仅"崩溃后 resume"用;
                       首次开采必须从干净态起,否则 normal 臂口径会被 INJECT 顶掉)
  --host-metrics prom|none
                       k8s only: host_cpu_pct/host_mem_pct 取值来源(默认 prom=cadvisor
                       容器级;none=两列留空。Prometheus/cadvisor 起不来时用 none 降级)
  --skip-code-parity   k8s only: 跳过 _filter_real_title / electronics.item 口径校验
                       (明知与本机 v2 不同口径时才用;会让两批数据不可直接对读)
  -- <args...>         其后的一切原样透传给 collect/agentfault_runner.py
                       (例:-- --skip-preflight --allow-mixed-tree --warmup 0)
  -h, --help           show this help and exit

WHAT IT DOES:
  1. build_carrier_pool.py   -> assets/carrier_pool.json  (only if MISSING; needs host SASRec)
  2. collect/agentfault_runner.py --backend B --runs N --out-dir DIR [--only ...]

TREE RULES:
  一棵输出树只能属于一个 backend。默认 out-dir 已按 backend 分开(见 --out-dir),
  别手动指到同一个 —— CSV 是 append-only,混进去会写出 ragged 表且无回退路径。
  runner 有三道闸:CSV 表头比对 / .collect_backend 标记 / 每次 append 再查一次表头。

COST / TIME (量级,便于决定要不要跑):
  全量 9 combo × 12 rep = 108 case。每 case 一次 /recommend = LangGraph 4 agent 各一次
  DeepSeek 调用;faulted 臂的 hallucinate/wrong_pick/format 还各多一次副 LLM 改写。
  → 约 450-500 次 DeepSeek 调用。本机 e2e 实测 max 63.9s/case;
  local  ≈ 2-3 小时;k8s ≈ 3-4 小时(多约 20 次 rollout,format combo 每 rep 一次)。
  ★先试水:--only hallu_Product_Analyzer --runs 2 --out-dir <某个 _smoke 目录>

PRE-FLIGHT (local): SASRec 200 @ 127.0.0.1:8200 · phase1 venv 存在
PRE-FLIGHT (k8s)  : 集群可达 · kubectl proxy:8001 · Prometheus 可达 + cadvisor target up ·
                    rec-agent 已切变体镜像 · 无 AGENTFAULT_INJECT 残留 ·
                    secret deepseek-env · MySQL CHECKSUM 基线可取 · 载体池已存在
                    (runner 自己还会再查一遍更深的:strategy=Recreate / PVC 里的
                     electronics.item / tools.py 的 _filter_real_title / pod 内探 sasrec /
                     /agentfault-data 可写 / pod 时钟偏移 / ns 内无遗留 Chaos CRD)
  ★k8s 分支的完整"从零到开采"清单(全栈部署 / 两个 secret / OTel 栈 / 载体池 / 还原)
    见 scripts/chaos/agentfault/README.md §"K8S 分支:从零到开采(0..6 步)"。

⚠️ COSTS MONEY (live DeepSeek). ⚠️ APPEND-ONLY CSV, no --force — see header /
   README before re-collecting an existing tree.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes)      YES=1; shift ;;
    --backend)  BACKEND="${2:?--backend needs local|k8s}"; shift 2 ;;
    --out-dir)  OUT_DIR="${2:?--out-dir needs a value}"; shift 2 ;;
    --runs)     RUNS="${2:?--runs needs a value}"; shift 2 ;;
    --only)     ONLY="${2:?--only needs a value}"; shift 2 ;;
    --allow-inject-residue) ALLOW_RESIDUE=1; shift ;;
    # ★下面三条修的是"脚本教的操作做不到":原来 EXTRA_ARGS 声明了却**没有任何路径给它赋值**,
    #   于是 fatal 文案里教人用的 --k8s-host-metrics none 根本传不进去(照做得到 unknown arg)。
    --host-metrics)
                HOST_METRICS="${2:?--host-metrics needs prom|none}"; shift 2 ;;
    --skip-code-parity) SKIP_CODE_PARITY=1; shift ;;
    --)         shift; EXTRA_ARGS+=("$@"); break ;;
    -h|--help)  usage; exit 0 ;;
    *) echo "ERROR: unknown arg '$1' (see --help; 任意 runner 参数可用 '-- <args>' 透传)" >&2
       exit 2 ;;
  esac
done

case "${BACKEND}" in
  local) : ;;
  k8s)   : ;;
  *) echo "ERROR: --backend must be 'local' or 'k8s' (got '${BACKEND}')" >&2; exit 2 ;;
esac
case "${HOST_METRICS}" in
  prom|none) : ;;
  *) echo "ERROR: --host-metrics must be 'prom' or 'none' (got '${HOST_METRICS}')" >&2; exit 2 ;;
esac
# k8s 专属旋钮误用在 local 上 -> 早失败(别等到跑完 carrier pool / 前检才发现)
if [[ "${BACKEND}" == "local" && ( "${HOST_METRICS}" != "prom" || "${SKIP_CODE_PARITY}" -eq 1 ) ]]; then
  echo "ERROR: --host-metrics / --skip-code-parity 只对 --backend k8s 有意义" >&2; exit 2
fi

# 默认输出树按 backend 分开 —— ★绝不能让 K8S 的行追进 agentfault_v2(append-only CSV,
# 混进去只能手工剥行 + 删 journal/raw/ledgers/spans;runner 里还有一道 .collect_backend 守卫)
if [[ -z "${OUT_DIR}" ]]; then
  if [[ "${BACKEND}" == "k8s" ]]; then OUT_DIR="datasets/agentfault_k8s"
  else OUT_DIR="(archived) agentfault_v2"; fi
fi

echo "=============================================================="
echo " agent-fault COLLECTION  (steps 1-2)"
echo "   repo root : ${REPO_ROOT}"
echo "   backend   : ${BACKEND}"
echo "   out-dir   : ${OUT_DIR}"
echo "   runs (K)  : ${RUNS}"
echo "   only      : ${ONLY:-<all 9 combos>}"
echo "=============================================================="

# ---- the paid-run warning gate ----------------------------------------------
echo ""
echo "  ⚠️  LIVE + PAID RUN"
echo "  ---------------------------------------------------------------"
echo "  Step 2 drives the real rec_agent which makes LIVE DeepSeek API"
echo "  calls (the injector rewrites completions with a sub-LLM). A full"
echo "  9-combo x ${RUNS}-reps sweep costs REAL money."
echo ""
echo "  ⚠️  APPEND-ONLY: the CSV is appended and resume is journal-based."
echo "  There is NO --force. To re-collect rows that already exist in"
echo "  '${OUT_DIR}', FIRST strip those rows from dataset_agentfault.csv"
echo "  and delete matching journal/ raw/ ledgers/ spans/ files, else you"
echo "  get duplicate rows / skipped combos."
if [[ "${BACKEND}" == "k8s" ]]; then
echo ""
echo "  ⚠️  K8S BACKEND: 每个 combo 会 kubectl set env + rollout 一次 rec-agent"
echo "  (format combo 每 rep 一次,共约 20 次)。整轮期间 rec-agent 上挂着注入器,"
echo "  别同时跑 traditional 线的采集(两条线靠时间窗隔离,Jaeger/Prom 会串)。"
echo "  采完必须手动还原:"
echo "    powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/restore_recagent_stock.ps1 -ConfirmedCollected"
fi
echo "  ---------------------------------------------------------------"
if [[ "${YES}" -ne 1 ]]; then
  echo ""
  echo "  Refusing to start a paid run without --yes. Re-run with --yes to proceed."
  exit 1
fi

fatal() { echo "  ✗ $*" >&2; exit 1; }

if [[ "${BACKEND}" == "local" ]]; then
  # =====================================================================
  # LOCAL 前检 —— ★这一段与加 backend 之前逐字未变
  # =====================================================================
  echo ""
  echo "── pre-flight: SASRec health @ ${SASREC_HEALTH}"
  code="$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' --max-time 5 "${SASREC_HEALTH}" || true)"
  if [[ "${code}" != "200" ]]; then
    echo "  ✗ SASRec not healthy (http_code=${code:-none})."
    echo "    Start it:  cd services/sasrec_api && python api_server.py   (port 8200; wait for model load)"
    exit 1
  fi
  echo "  ✓ SASRec healthy (200)"

  echo ""
  echo "── pre-flight: phase1 venv @ ${VENV_PY}"
  if [[ ! -x "${VENV_PY}" && ! -f "${VENV_PY}" ]]; then
    echo "  ✗ phase1 venv missing. The runner will FATAL without it."
    echo "    Build it:  bash scripts/chaos/agentfault/phase1_bootstrap.sh"
    exit 1
  fi
  echo "  ✓ phase1 venv present"
else
  # =====================================================================
  # K8S 前检
  #   注:runner 自己还有一层更深的 preflight(strategy/PVC 数据/代码口径/pod 内探
  #   sasrec/Chaos CRD)。这里查的是"花钱之前就该拦住"的那几条 + CHECKSUM 基线。
  # =====================================================================
  echo ""
  echo "── pre-flight(k8s): kubectl @ ${KUBECTL}"
  [[ -x "${KUBECTL}" || -f "${KUBECTL}" || -n "$(command -v "${KUBECTL}" 2>/dev/null)" ]] \
    || fatal "kubectl 不可执行:${KUBECTL}(可用 KUBECTL=... 覆盖)"
  "${KUBECTL}" get nodes >/dev/null 2>&1 || fatal "K8S API 不可达(集群没起?)"
  echo "  ✓ 集群可达"

  echo ""
  echo "── pre-flight(k8s): kubectl proxy @ ${K8S_PROXY}"
  # ★必须是 proxy 不是 port-forward:port-forward 绑定具体 pod,rollout 后立刻
  #   'failed to find sandbox' 整个死掉(2026-07-27 dprobe 实证)。
  #   ★Prometheus 的 cadvisor / kube-state-metrics 两个 target 也经 host.docker.internal:8001 抓,
  #     proxy 不起 = 两个 target down = host_cpu_pct/host_mem_pct 全空。
  pcode="$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' --max-time 8 \
           "${K8S_PROXY}/api/v1/namespaces/${K8S_NS}/pods?limit=1" || true)"
  [[ "${pcode}" == "200" ]] || fatal "kubectl proxy 不通(http=${pcode:-none})。起它:
      kubectl proxy --port=8001 --address=0.0.0.0 --accept-hosts='.*'   (放后台常驻)"
  echo "  ✓ kubectl proxy 通"

  echo ""
  echo "── pre-flight(k8s): Prometheus @ ${PROM_URL}(host 水位来源)"
  # ★这两件事必须分开报(审查 R8 / 复现审查③):
  #   ① Prometheus 进程压根没起(= Docker OTel 栈没起)—— 原来报的是"target down?",
  #      把人指去查 target,而真正该做的是 docker compose up。
  #   ② Prometheus 起了但 cadvisor target down。判据也必须换:`count(up{...})` 在 target
  #      down 时序列仍在、值为 0 → count() 恒返 1,是**空闸**(实测)。必须 sum(up)>0。
  if [[ "${HOST_METRICS}" == "none" ]]; then
    echo "  ! --host-metrics none:跳过 Prometheus 检查,host_cpu_pct/host_mem_pct 将留空"
  else
    ready="$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' --max-time 8 \
             "${PROM_URL}/-/ready" || true)"
    [[ "${ready}" == "200" ]] || fatal "Prometheus 不可达(http=${ready:-none})。它不是 K8S 里的,
      是宿主 docker compose 的 OTel 栈。起它:
        docker compose -f ops/docker-compose.otel.yml up -d      (或 python start_all.py)
      确实不要 host 水位则改用:--host-metrics none"
    cad="$(curl -s --noproxy '*' --max-time 10 \
          "${PROM_URL}/api/v1/query?query=sum(up%7Bjob%3D%22cadvisor%22%7D)" || true)"
    upv="$(sed -n 's/.*"value":\[[^,]*,"\([^"]*\)".*/\1/p' <<<"${cad}")"
    if [[ -z "${upv}" || "${upv}" == "0" ]]; then
      fatal "Prometheus 的 cadvisor target 是 down(sum(up)=${upv:-无序列})。
      host_cpu_pct/host_mem_pct 会全空。cadvisor 与 kube-state-metrics 两个 target 都经
      host.docker.internal:8001 抓(ops/prometheus.yml L19-30)→ 先确认 kubectl proxy 常驻,
      再确认 kubectl apply -f k8s/pilot/12-kube-state-metrics.yaml 已生效;
      或明确降级:--host-metrics none"
    fi
    echo "  ✓ Prometheus 可达 且 cadvisor target up (sum(up)=${upv})"
  fi

  echo ""
  echo "── pre-flight(k8s): rec-agent 是否已切变体镜像"
  img="$("${KUBECTL}" get deploy "${K8S_DEPLOY}" -n "${K8S_NS}" \
        -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
  [[ -n "${img}" ]] || fatal "读不到 deploy/${K8S_DEPLOY} 的 image(ns=${K8S_NS} 里没有?)"
  if [[ "${img}" != *"${IMAGE_HINT}"* ]]; then
    fatal "rec-agent 镜像是 '${img}',不含 '${IMAGE_HINT}'。先切采集变体:
      powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1"
  fi
  echo "  ✓ image = ${img}"

  echo ""
  echo "── pre-flight(k8s): pod 内 env(DEEPSEEK key / AGENTFAULT_INJECT 残留)"
  # ★这条 kubectl 的参数里没有容器内路径,不需要 MSYS2_ARG_CONV_EXCL;下面 grep 那条需要。
  podenv="$("${KUBECTL}" exec -n "${K8S_NS}" "deploy/${K8S_DEPLOY}" -- printenv 2>/dev/null || true)"
  [[ -n "${podenv}" ]] || fatal "kubectl exec printenv 失败(pod 没 Running?)"
  grep -q '^DEEPSEEK_API_KEY=' <<<"${podenv}" \
    || fatal "pod 内没有 DEEPSEEK_API_KEY —— envFrom secretRef deepseek-env 没挂上/没建。
      查:${KUBECTL} get secret deepseek-env -n ${K8S_NS}
      建(键名取自 .env.example L23-25;workflow.py L62-66 与注入器副 LLM L132-136 都读这三个):
        ${KUBECTL} create secret generic deepseek-env -n ${K8S_NS} \\
          --from-literal=DEEPSEEK_API_KEY=<你的 key> \\
          --from-literal=DEEPSEEK_API_BASE=https://api.deepseek.com/v1 \\
          --from-literal=DEEPSEEK_MODEL=deepseek-chat
      建完重跑 patch_recagent_collect.ps1(它负责把 envFrom 挂上去)"
  if grep -q '^AGENTFAULT_INJECT=' <<<"${podenv}"; then
    if [[ "${ALLOW_RESIDUE}" -ne 1 ]]; then
      fatal "★pod 上挂着 AGENTFAULT_INJECT 残留(上一轮没收干净)。首次开采必须从干净态起 ——
      残留会把 normal 臂的 observer 顶掉(loader 会打 'AGENTFAULT_OBSERVE ignored'),口径就废了。
      清:powershell -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1(它会全量重置白名单)
      若这是**崩溃后 resume**,加 --allow-inject-residue 放行。"
    fi
    echo "  ! AGENTFAULT_INJECT 残留(--allow-inject-residue 已放行,当作 resume 处理)"
  else
    echo "  ✓ 无 AGENTFAULT_INJECT 残留"
  fi
  echo "  ✓ deepseek-env 已挂"

  echo ""
  echo "── pre-flight(k8s): MySQL CHECKSUM 基线(items/inventory 零漂移闸的起点)"
  # runner 每个 combo 前后都会核 CHECKSUM,漂移即硬停整轮。这里先证明"取得到",
  # 免得跑到第一个 combo 结束才发现 DB 连不上(那时钱已经花了)。
  mkdir -p "${OUT_DIR}"
  if ! "${PY}" - "$OUT_DIR" <<'PYEOF'
import json, os, sys
sys.path.insert(0, os.path.join("scripts", "chaos", "agentfault", "injector"))
import injector_smoke as ISM
cs = ISM.checksum_tables()
print("     checksum =", cs)
if "_error" in cs:
    sys.exit("     取不到 CHECKSUM(MySQL 不通?):" + cs["_error"])
with open(os.path.join(sys.argv[1], ".checksum_baseline.json"), "w", encoding="utf-8") as f:
    json.dump(cs, f, ensure_ascii=False)
PYEOF
  then
    fatal "CHECKSUM 基线取不到 —— 修好 MySQL(shopify2)再开采"
  fi
  echo "  ✓ CHECKSUM 基线已记 -> ${OUT_DIR}/.checksum_baseline.json"
fi

# ---- step 1: carrier pool (only if missing) ---------------------------------
echo ""
echo "── STEP 1: carrier pool"
if [[ -f "${CARRIER_POOL}" ]]; then
  echo "  ✓ already present, skipping build: ${CARRIER_POOL}"
elif [[ "${BACKEND}" == "k8s" ]]; then
  # 载体池必须与本机 v2 **同一份**(所有 combo 的 rep_i 用同一 carrier[i-1] 是设计铁律,
  # 换池子就没法跨批次对读)。而且 build_carrier_pool.py 要打宿主 SASRec:8200,
  # K8S 分支不强制宿主 SASRec 在 → 缺池子直接 FATAL,不在这里现建。
  fatal "载体池缺失:${CARRIER_POOL}(正常情况下它随仓库提交,不该缺)
      K8S 分支不代建(建池要**宿主** SASRec:8200,且必须与 v2 用同一份池子才能跨批次对读)。
      ★这是 local/k8s 两个分支唯一共享的本机依赖 —— 重建它要求本机备齐 9.2GB
        standard_cache.pkl + 260MB SASRec-*.pth。
      先起宿主 SASRec(cd services/sasrec_api && python api_server.py)后跑:
        ${PY} ${POOL_BUILDER}"
else
  echo "  building (needs SASRec): ${CARRIER_POOL}"
  echo "  \$ ${PY} ${POOL_BUILDER}"
  "${PY}" "${POOL_BUILDER}"
  if [[ ! -f "${CARRIER_POOL}" ]]; then
    echo "  ✗ carrier pool not produced: ${CARRIER_POOL}"
    exit 1
  fi
  echo "  ✓ built: ${CARRIER_POOL}"
fi

# ---- step 2: the collection runner ------------------------------------------
echo ""
echo "── STEP 2: agentfault_runner.py  (LIVE DeepSeek)"
RUN_ARGS=(--runs "${RUNS}" --out-dir "${OUT_DIR}" --backend "${BACKEND}")
if [[ -n "${ONLY}" ]]; then
  RUN_ARGS+=(--only "${ONLY}")
fi
if [[ "${BACKEND}" == "k8s" ]]; then
  RUN_ARGS+=(--k8s-ns "${K8S_NS}" --k8s-deploy "${K8S_DEPLOY}"
             --k8s-proxy "${K8S_PROXY}" --prom-url "${PROM_URL}"
             --k8s-image-hint "${IMAGE_HINT}"
             --k8s-host-metrics "${HOST_METRICS}")
  [[ -n "${KUBECTL}" ]] && RUN_ARGS+=(--kubectl "${KUBECTL}")
  [[ "${ALLOW_RESIDUE}" -eq 1 ]] && RUN_ARGS+=(--k8s-allow-inject-residue)
  [[ "${SKIP_CODE_PARITY}" -eq 1 ]] && RUN_ARGS+=(--k8s-skip-code-parity)
fi
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  RUN_ARGS+=("${EXTRA_ARGS[@]}")
fi
echo "  \$ ${PY} ${RUNNER} ${RUN_ARGS[*]}"
if ! "${PY}" "${RUNNER}" "${RUN_ARGS[@]}"; then
  rc=$?
  echo "  ✗ runner exited ${rc} — inspect output above (partial rows may have been appended)"
  if [[ "${BACKEND}" == "k8s" ]]; then
    cat <<EOF

  ★中断/失败后 K8S 侧不是干净态(stop_instance 有意不摘 env,摘 env 会 rollout 清 emptyDir)。
    三条出路(runner 被 Ctrl-C 时也会打同一份说明):
      (a) 干净重来 : powershell -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1
                     然后原样重跑本命令(journal-exists 会跳过已采完的 combo)
      (b) 接着采   : 本命令加 --allow-inject-residue
                     ★normal combo 不能这样 resume(INJECT 残留会把 observer 顶掉)
      (c) 不继续了 : 先核 ${OUT_DIR}/spans/ 与 ledgers/ 非空,再
                     powershell -File scripts/chaos/agentfault/k8s/restore_recagent_stock.ps1 -ConfirmedCollected
EOF
  fi
  exit "${rc}"
fi

CSV="${OUT_DIR}/dataset_agentfault.csv"
if [[ -f "${CSV}" ]]; then
  echo "  ✓ collection complete -> ${CSV}"
else
  echo "  ✗ expected CSV not found: ${CSV}"
  exit 1
fi

echo ""
echo "=============================================================="
echo " DONE. Next: run the eval pipeline on this tree:"
echo "   bash scripts/chaos/agentfault/run_eval_agentfault.sh --dataset-dir ${OUT_DIR}"
if [[ "${BACKEND}" == "k8s" ]]; then
echo ""
echo " ★采后收尾(★不自动跑:还原会重建 pod 并清空 emptyDir,没拉走的 span 永久丢)"
echo "   1) 先确认 ${OUT_DIR}/spans/ 与 ledgers/ 里 9 个 combo 的文件都非空;"
echo "   2) powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/restore_recagent_stock.ps1 -ConfirmedCollected"
fi
echo "=============================================================="
