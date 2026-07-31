# agentfault — agent-fault dataset pipeline (nav doc)

This directory builds and evaluates the **agent-fault RCA dataset**
`(upstream batch)` — MAS-FIRE-style *semantic* agent faults (hallucinate /
wrong-item-pick / format-violation / context-drift) injected into the **real**
`recommendation_agent` (LangGraph 4-agent chain) on the live 25-microservice
OTel stack, captured on **two tracks** (black-box telemetry + content-layer
spans).

> **All commands run FROM REPO ROOT `${REPO_DIR}`**, with env
> `NO_PROXY='*' PYTHONIOENCODING=utf-8`, using conda python
> `python3`. The two `run_*.sh`
> orchestrators set all of this for you.

---

## TL;DR — the two one-click orchestrators

```bash
# COLLECT (steps 1-2) — LIVE stack + PAID DeepSeek. Requires --yes.
bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes \
     [--backend local|k8s] [--out-dir …] [--runs 12] [--only combo_a,combo_b]

# EVAL (steps 3-10) on an EXISTING tree — OFFLINE/FREE by default.
bash scripts/chaos/agentfault/run_eval_agentfault.sh \
     [--dataset-dir (archived) agentfault_v2] [--with-judge] [--with-glm]
```

### `--backend` — 同一个 runner,两个执行环境(2026-07-27,B 档)

| backend | 跑在哪 | 默认 out-dir | 用途 |
|---|---|---|---|
| `local`(默认) | 本机隔离 harness:phase1 venv 起一个临时 `rec_agent` 进程 + 真实 SASRec:8200 | `(archived) agentfault_v2` | 产出既有 108 case 的口径。**行为与加 backend 之前逐字节一致** |
| `k8s` | 25 微服务全栈里的常驻 `rec-agent` pod(`kubectl set env` + rollout,探针经 kubectl proxy) | `datasets/agentfault_k8s` | 消掉交付包里"agent 跑在全栈内"的 overclaim(v2 其实只跑了 rec-agent + sasrec 两个进程) |

combo 矩阵 / GT 逻辑(注入台账)/ CSV schema **完全同一套**(同一个 runner),
四套 eval 一行不用改;K8S 树的 CSV 只在**末尾**多 4 列口径/provenance 标签
(`collect_backend` / `host_metric_source` / `k8s_pod_name` / `k8s_pod_restarts`)。

**TREE RULES —— 一棵输出树只能属于一个 backend。** 默认 out-dir 已按 backend 分开
(`agentfault_v2` / `agentfault_k8s`),别手动指到同一个:CSV 是 append-only,混进去会写出
**ragged 表**且没有回退路径。runner 有三道闸:① 盘上 CSV 表头与当前列集比对(不写文件,
所以冻结树零新增)② `.collect_backend` 标记(**只在非 local 树写**)③ 每次 append 再查一次表头。
明知故犯用 `--allow-mixed-tree`。

**COST / TIME(量级)**:全量 9 combo × 12 rep = 108 case;每 case 一次 `/recommend`
= 4 个 agent 各一次 DeepSeek,faulted 臂再多一次副 LLM 改写 → 约 450-500 次调用。
local ≈ 2-3 小时,k8s ≈ 3-4 小时(多约 20 次 rollout)。**先试水**:
`--only hallu_Product_Analyzer --runs 2 --out-dir <某个 _smoke 目录>`。

---

## K8S 分支:从零到开采(0..6 步)

> 这一节是 `--backend k8s` 的**完整**前置清单。0-4 步只需跑一次(PVC / 镜像 / secret
> 跨批次复用);第 6 步"还原"是**必做**的收尾,不是可选项。
> **本档不需要装 Chaos Mesh** —— 相反,ns 里有任何遗留 Chaos CRD 都会被 preflight 拦下
> (那会把 agent-only 的 B 档污染成跨层的 C/D 档)。

### 步骤 0 —— 25 服务全栈 + 两个 secret + 宿主 OTel 栈

`--backend k8s` 的所有 preflight 都**假定** ns `recweb-chaos` 里已经跑着完整栈。
部署步骤见 **`k8s/pilot/README.md` §"全栈 bring-up"**(镜像 build → apply 顺序 → 验收)。

两个 secret(全仓其它地方只在"缺了"时报错,创建命令只有这里有):

```powershell
# ① db-cred — 全栈各服务的 MySQL 口令(键名固定为 password,见 10-catalog.yaml)
kubectl create secret generic db-cred -n recweb-chaos --from-literal=password='<.env 的 DB_PASSWORD>'

# ② deepseek-env — rec-agent 的 LLM 凭据。三个键名取自 .env.example L23-25;
#    workflow.py L62-66(agent 主 LLM)与 injector L132-136(注入用的副 LLM)都读它们。
#    ★是 envFrom 整包注入,所以键名必须与 .env 一致,不能自创。
kubectl create secret generic deepseek-env -n recweb-chaos `
  --from-literal=DEEPSEEK_API_KEY='<你的 key>' `
  --from-literal=DEEPSEEK_API_BASE='https://api.deepseek.com/v1' `
  --from-literal=DEEPSEEK_MODEL='deepseek-chat'
```

宿主 **Docker OTel 栈**(Prometheus / collector / Jaeger / Loki / Grafana 都在**宿主 compose 里**,
不在 K8S 里)—— `host_cpu_pct` / `host_mem_pct` 两列的唯一来源:

```bash
docker compose -f ops/docker-compose.otel.yml up -d      # 或 python start_all.py
```

> Prometheus 的 `cadvisor` / `kube-state-metrics` 两个 target 都经
> `host.docker.internal:8001` 抓(`ops/prometheus.yml` L19-30)→ **kubectl proxy 不起,
> 这两个 target 就是 down,两列会全空**。确实拿不到时用 `--host-metrics none` 明确降级。

**载体池**:`assets/carrier_pool.json` 已随仓库提交,正常不缺。若缺失,重建需要**宿主**
SASRec:8200(即本机也要备齐 9.2GB `standard_cache.pkl` + 260MB `.pth`)——
**这是 local/k8s 两分支唯一共享的本机依赖**,k8s 分支不代建(必须与 v2 用同一份池子才能跨批次对读)。

### 步骤 1 —— build 变体镜像(★全仓唯一规范命令)

```powershell
docker build -f scripts/chaos/agentfault/k8s/Dockerfile.agentfault `
  --build-context repo=${REPO_DIR} `
  --build-arg SRC_GIT_SHA=$(git rev-parse --short HEAD) `
  -t recweb-rec-agent:agentfault-v2 scripts/chaos/agentfault
```

- `--build-context repo` **必须指仓库根**(不能指 `services/recommendation_agent`,
  那个目录里躺着 2.4GB 的 `electronics.inter` 且目录内没有 `.dockerignore`)。
- tag 用 `:agentfault-v2` 不覆盖 `:agentfault` —— 后者是 G1(`single_recagent` 15)的环境身份。
- **镜像不会自动进节点 containerd**(Docker Desktop 新版 K8S 的 `k8s.io` namespace 与
  `docker build` 的存储不共享)。若 rollout 报 `ErrImageNeverPull` / 跑的还是旧代码:

  ```powershell
  # ★★必须 **cmd.exe** 跑(2026-07-27 实测纠正):Git Bash 管道坏二进制流,
  #   **PowerShell 也不行**(管道传对象 → 3GB 全缓存进内存 → Insufficient memory)。
  #   cmd.exe /c "docker save … | docker exec -i desktop-control-plane ctr -n k8s.io images import -"
  docker save recweb-rec-agent:agentfault-v2 |
    kubectl exec -i -n kube-system <debug-pod> -- chroot /host ctr -n k8s.io images import -
  ```
  `patch_recagent_collect.ps1` 的 verify 段用 `RECWEB_SRC_GIT_SHA` 抓"以为换了其实没换"。

### 步骤 2 —— 灌 `electronics.item` 进 PVC(title cache 数据源)

```powershell
powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/load_recagent_data.ps1
```

- 会校验源文件 **size + SHA256**(权威值来自仓库根 `README.md` 的自备大文件表),
  并把 pod 内 sha256 与宿主**互比** —— 换版/截断的 `.item` 直接 throw,不再是"打印了事"。
- ★该脚本在 PVC 未 Bound 时会**自举调用一次** `patch_recagent_collect.ps1 -SkipDataCheck`
  把卷挂上(storageClass 是 WaitForFirstConsumer,不挂就不 Bound)。所以照本节顺序走时,
  **patch 总共会跑两次,属预期**。
- 期待输出:`entries=1946169 placeholder=506946 real=1439223`。

### 步骤 3 —— 切采集形态

```powershell
powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1
```
镜像 → `:agentfault-v2`;挂 PVC;内存上限 1536Mi / CPU 2core(title cache 实测 +436.8MB,
256Mi 必 OOM);`strategy: Recreate`;注入旋钮全量重置。

### 步骤 4 —— `kubectl proxy` 常驻(★不能用 port-forward)

```bash
kubectl proxy --port=8001 --address=0.0.0.0 --accept-hosts='.*'
```
port-forward 绑定**具体 pod**,rollout 后立刻 `failed to find sandbox` 整个死掉。

### 步骤 5 —— 开采

```bash
bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes --backend k8s
```

### 步骤 6 —— ★还原(必做,不是可选)

```bash
# 先核 datasets/agentfault_k8s/{spans,ledgers}/ 里 9 个 combo 的文件都非空,再还原
powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/restore_recagent_stock.ps1 -ConfirmedCollected
```

- **不还原的后果**:rec-agent 停在 `:agentfault-v2` + 1536Mi + `Recreate` + 挂 PVC 的形态。
  之后任何 traditional 线采集的 `container_spec_memory_limit_bytes` / pod 重建语义都与
  产出 255 的口径不同 —— **跨批次不可比**。脚本会断言环境确实回到了 `:latest` / 256Mi /
  500m / RollingUpdate / 无卷 / env CLEAN。
- 还原**不删 PVC**(`recagent-data` 是独立生命周期对象)→ 重采免重灌 254MiB。
- `-ConfirmedCollected` 是硬守卫:还原会重建 pod、`emptyDir` 随即清空,没拉走的轨迹永久丢。

### 中断 / 崩溃后怎么办

`K8sBackend.stop_instance` **有意不摘 env**(摘 env 会 rollout,清掉还没拉走的 span),
所以任何中断都会把注入旋钮留在**常驻** pod 上。三条出路(runner 被 Ctrl-C 时也会打印这份说明):

| 意图 | 做法 |
|---|---|
| 干净重来 | 重跑 `patch_recagent_collect.ps1`(全量重置旋钮)→ 原样重跑采集命令,journal-exists 会跳过已采完的 combo |
| 接着采 | 采集命令加 `--allow-inject-residue`。★`normal` combo **不能**这样 resume——`AGENTFAULT_INJECT` 残留会把 observer 顶掉(loader 打 `AGENTFAULT_OBSERVE ignored`),口径就废了 |
| 不继续了 | 先核 `spans/` 与 `ledgers/` 非空,再跑步骤 6 的还原 |

> ⚠️ **反方向目前没有闸**:agentfault 侧会检查 ns 里有无 Chaos CRD 防跨层污染,但
> traditional 线的采集脚本(`scripts/chaos/ctk/collect-*.sh`)**不检查 rec-agent 是不是 stock**。
> 忘了还原就去跑 traditional,会带着注入器 + 1536Mi 采数据。跑 traditional 前手动确认:
> `kubectl get deploy rec-agent -n recweb-chaos -o jsonpath='{.spec.template.spec.containers[0].image}'`
> 应为 `recweb-rec-agent:latest`。

`--with-judge` (DeepSeek) and `--with-glm` (GLM-5.2 cross-family) are **additive,
paid** flags — omit them for a fully free offline eval.

---

## Directory map

```
scripts/chaos/agentfault/
│
├── run_collect_agentfault.sh   ★NEW  one-click COLLECTION (steps 1-2)
├── run_eval_agentfault.sh      ★NEW  one-click EVAL (steps 3-10)
├── README.md                   ★NEW  this file
│
├── injector/                   the fault mechanism (runtime monkey-patch; never edits services/**)
│   ├── agentfault_injector.py      patches ChatOpenAI._generate; the 4 fault classes;
│   │                               agent attribution by system-prompt [ROLE] signature
│   ├── contract_validator.py       Synthesizer output-contract check = consumer of
│   │                               format_violation + wrong_item_pick (4-check, mirrors
│   │                               llm_rerank_service/utils/validator.py)
│   ├── injector_smoke.py           LIVE smoke of (agent,kind) combos; also the single source
│   │                               of truth for VENV_PY + env recipe (imported by the runner)
│   ├── loader/
│   │   └── sitecustomize.py     ★CURRENT loader — auto-imported into the phase1 venv;
│   │                               env-gated: AGENTFAULT_INSTRUMENT / _INJECT / _OBSERVE
│   └── INJECTOR_README.md          injector design / rationale / caveats / smoke results
│
├── collect/
│   ├── agentfault_runner.py     the collection runner (combo matrix, GT from the injection
│   │                            ledger, CSV schema, journal-based resume — all env-agnostic)
│   └── backends.py          ★NEW execution backends (2026-07-27): LocalBackend = 原行为逐字
│                                搬迁(phase1 venv 起临时进程);K8sBackend = 全栈 pod
│                                (kubectl set env + rollout + proxy 探针 + span/台账 tail 回收)
├── build_carrier_pool.py        builds assets/carrier_pool.json (history-seq rotation; needs SASRec)
├── assets/carrier_pool.json     committed carrier pool (normally already present)
├── constraints.txt              pinned deps for the phase1 throwaway venv
│
├── eval/                        offline evaluators (+ one paid judge)
│   ├── eval_agentfault_tierA.py     Tier-A offline baselines -> BASELINE_RESULTS.md (MRCBench)
│   ├── content_ctxdrift_track.py    context_drift structural track -> RESULTS_CONTENT_CTXDRIFT.md
│   ├── infra_negatives/
│   │   ├── run_infra_negatives.py   BARO/RCD honest negatives -> infra_negatives/
│   │   └── SPEC_INFRA.md            spec: why BARO/RCD/Eadro degrade on agent faults
│   └── whowhen/                     Who&When + A2P trajectory-attribution baseline
│       ├── make_whowhen_cases.py        raw/ -> whowhen/cases/*.json  (offline adapter)
│       ├── run_whowhen.py               LLM-judge harness  ★PAID (DeepSeek / GLM cross-family)
│       ├── score_whowhen.py             score outputs -> whowhen_results[_<tag>].json (offline)
│       ├── build_whowhen_format_delivery.py  Who&When-schema delivery view (offline; NO args)
│       └── SPEC.md                      spec for the Who&When baseline
│
├── compute_context_drift_outcome.py  context_drift recovered/silent_wrong/unknown (offline)
│
├── judge/                       PROTOTYPE consumers (design spikes; not in the 10-step pipeline)
│   ├── hallucinate_judge.py         MAST-style per-agent LLM judge prototype
│   ├── collect_clean_baseline.py    clean-baseline rates to cancel base-LLM self-hallucination
│   └── JUDGE_README.md
│
├── tests_dev/                  dev-time offline checks (免费,不起服务)
│   ├── test_p0_2_runner.py         combo 矩阵 / build_env / 载体轮换 / ctxdrift outcome
│   ├── test_backend_parity.py  ★NEW backend 改造回归:[A] CSV 表头字节级不变 [B] local seam
│   │                               转调 [C] K8S env 白名单零外泄 [D] **108 行离线 replay 逐字段
│   │                               diff** [E] 审查修正守卫(跨树双向拦截 / append_csv 表头闸 /
│   │                               span 按 combo 累积不被 rollout 覆盖 / apiserver 5xx 判据)
│   └── test_filter_real_title.py
├── k8s/                        K8S 侧环境工具(全部 PowerShell —— Git Bash 的 MSYS 会改写
│   │                           kubectl 参数里的容器内路径)
│   ├── Dockerfile.agentfault       变体镜像(注入器 + openinference + 仓库当前 tools.py)
│   ├── load_recagent_data.ps1  ★NEW 灌 electronics.item 进 PVC recagent-data(title cache 源)
│   ├── patch_recagent_collect.ps1 ★NEW B 档采集形态(镜像/PVC/内存/Recreate/旋钮重置)
│   ├── patch_recagent_observe.ps1  G1+D 档 observe-only 形态(★冻结不改:是那两批数据的环境身份)
│   ├── restore_recagent_stock.ps1  还原 stock(带 -ConfirmedCollected 守卫 + 口径断言)
│   └── dprobe_*.py                 D 档跨层采集器/判官(与 B 档无关)
│
├── COLLECTION_DESIGN.md        (5) dual-track collection design (v1 72-case; ERRATUM at top)
├── TAXONOMY.md                 thin fault taxonomy, mapped to MAS-FIRE (defensible sourcing)
├── REDESIGN_v2_P0_CROSSLAYER.md  v2 redesign: P0 benchmark bar + agent×infra cross-layer roots
│
└── ── historical / phase gates (see next section — DO NOT DELETE) ──
    ├── phase0_setup_and_run.sh, phase0_smoke_openinference.py
    ├── phase1_bootstrap.sh      ★LIVE — builds the throwaway venv the runner uses
    ├── phase1_run.sh, phase1_launcher.py, phase1_smoke_verify.py
    ├── phase1_loader/sitecustomize.py   EARLIER prototype loader (superseded by injector/loader/)
    └── PHASE1_README.md         the content-layer mounting GO/NO-GO gate writeup
```

---

## ★ Current vs historical — do NOT delete "phase*" as if it were dead

`phase0_*` / `phase1_*` **look** like scaffolding but part of it is **live
load-bearing infra**:

| Item | Status | Why it matters |
|---|---|---|
| `phase1_bootstrap.sh` | **LIVE** | Builds `scratchpad/phase1_venv/` — the openinference-instrumented throwaway venv the **runner spawns rec_agent with**. Delete it and collection FATALs `run phase1_bootstrap.sh`. |
| `injector/loader/sitecustomize.py` | **CURRENT** loader | The loader actually used by collection (env-gated instrument/inject/observe). |
| `phase1_loader/sitecustomize.py` | **historical** | The *earlier prototype* loader. Superseded by `injector/loader/`. Kept for provenance. |
| `phase0_*`, `phase1_run.sh`, `phase1_launcher.py`, `phase1_smoke_verify.py`, `PHASE1_README.md` | historical gates | The content-layer mounting GO/NO-GO proof. Reference, not clutter. |

**Rule of thumb:** `injector/loader/` = current; `phase1_loader/` = old prototype;
`phase1_bootstrap.sh` + `scratchpad/phase1_venv/` = live dependency of the runner.

---

## The 10-step pipeline

Labels: **[live]** needs the running stack · **[paid]** hits a paid LLM API ·
**[offline]** pure local/free.

| # | Script | Produces | Label |
|---|---|---|---|
| 1 | `build_carrier_pool.py` | `assets/carrier_pool.json` | **[live]** (SASRec; committed already → normally skip) |
| 2 | `collect/agentfault_runner.py --runs 12` | `(upstream batch)` (csv + journal/raw/ledgers/spans) | **[live] [paid]** DeepSeek |
| 3 | `compute_context_drift_outcome.py --dataset-dir …` | `context_drift_outcomes.json` | **[offline]** |
| 4 | `eval/eval_agentfault_tierA.py --dataset-dir …` | `BASELINE_RESULTS.md` | **[offline]** |
| 5 | `eval/infra_negatives/run_infra_negatives.py --method all --channel both --dataset-dir …` | `infra_negatives/` | **[offline]** |
| 6 | `eval/content_ctxdrift_track.py --dataset-dir …` | `RESULTS_CONTENT_CTXDRIFT.md` | **[offline]** (needs #3 + #4) |
| 7 | `eval/whowhen/make_whowhen_cases.py --dataset-dir …` | `whowhen/cases/` | **[offline]** |
| 8 | `eval/whowhen/run_whowhen.py --method all --dataset-dir … --tag deepseek` | `whowhen/outputs/*.txt` | **[paid]** judge API |
| 8b | …`--api-base https://open.bigmodel.cn/api/paas/v4 --api-key-env GLM_API_KEY --model glm-5.2 --max-tokens 8192 --tag glm52` | `whowhen/outputs/*_glm52.txt` | **[paid] + slow (hours)** GLM-5.2 cross-family |
| 9 | `eval/whowhen/score_whowhen.py --dataset-dir … [--tag glm52]` | `whowhen/whowhen_results[_glm52].json` | **[offline]** |
| 10 | `eval/whowhen/build_whowhen_format_delivery.py` | `(upstream whowhen view)` | **[offline]** (takes NO args; hardcoded to `agentfault_v2`) |

**Orchestrated:**
`run_collect_agentfault.sh --yes` = steps 1-2.
`run_eval_agentfault.sh` = steps 3,4,5,6,7,9,10 (offline default); `+8` with
`--with-judge`; `+8b/9b` with `--with-glm`.

---

## Iron rules / gotchas

- **APPEND-ONLY CSV, NO `--force`.** The runner appends to
  `dataset_agentfault.csv` and resumes from `journal/` (skips journalled combos).
  To **re-collect** rows that already exist you must **first** strip those rows
  from the CSV **and** delete the matching `journal/ raw/ ledgers/ spans/` files —
  otherwise you get duplicate rows and/or silently-skipped combos.
- **一棵树一个 backend**(见上方 TREE RULES)。三道闸都会硬失败:CSV 表头比对 /
  `.collect_backend` 标记 / `append_csv` 每次再查一次表头。★`local` 后端**不往树里写
  任何文件** —— 已冻结的 `agentfault_v2` / `agentfault` 目录零新增。
- **一键脚本挡不住的参数用 `--` 透传**:
  `run_collect_agentfault.sh --yes --backend k8s -- --skip-preflight --warmup 0`。
  常用的两个已有直通开关:`--host-metrics prom|none`、`--skip-code-parity`。
- **`--runs` 两处默认值不同**:一键脚本 `RUNS=12`(= v2 复现口径),runner 的 argparse
  `default=5`。**直调 runner 时务必显式 `--runs 12`**,否则拿到的是 5 rep 的树。
- **Env every time:** `NO_PROXY='*'` (bypass Clash), `PYTHONIOENCODING=utf-8`,
  conda python `python3`, **cwd = repo
  root**. The orchestrators handle this; if you invoke a step by hand, set it.
- **Two pythons.** Everything here runs under **conda** python — *except* the
  rec_agent subprocess the runner spawns, which uses the **phase1 venv**
  (`scratchpad/phase1_venv`, = `injector_smoke.VENV_PY`). Do not override the
  runner's VENV_PY.
- **Corrected, not raw.** hallucinate injection multiplies latency (the sub-LLM
  rewrite adds an extra call), so raw span durations are a latency *artifact*.
  Infra eval uses `span_<A>_duration_corrected_ms` (the `--channel corrected`
  path). The raw channel is kept **only** as a contaminated-baseline contrast
  (e.g. Tier-A "RAW dur — contaminated" ≈ 0.94 is the artifact, not a result).
- **Report per-family.** hallucinate / wrong_item_pick / format_violation /
  context_drift behave differently (structured faults are near-trivial;
  hallucinate is the hard case). Never quote one blended number.
- **Cross-family judge.** The DeepSeek injector rewrites with DeepSeek → the
  judge defaults to a **different model family** (GLM-5.2) to avoid same-source
  bias. That run is slow (thinking model; needs `--max-tokens 8192`).
- **Do NOT move these scripts.** Every script resolves siblings via
  `__file__`-relative paths (`injector/`, `collect/`, `assets/`, the phase1
  venv). Moving/renaming/archiving any of them breaks the pipeline. New files
  only.
- **Step 10 is hardcoded.** `build_whowhen_format_delivery.py` takes no
  arguments and always reads `(archived) agentfault_v2`; the eval orchestrator
  skips it (with a note) if you point `--dataset-dir` elsewhere.

---

## Cross-references

- Dataset docs: `(upstream batch)SUMMARY.md`, `(upstream batch)EVAL_NOTES.md`
- Who&When-format delivery view: `(upstream whowhen view)`
- Publication authority: `(project docs)/TASK-PUB-dataset-paper-2026-07-21.md`
- Build/verify timeline (append-only audit): `(project docs)/archive/TASK-K8S-agentfault.md`
- Baseline specs: `eval/whowhen/SPEC.md`, `eval/infra_negatives/SPEC_INFRA.md`
- Injector design: `injector/INJECTOR_README.md`; judge prototype: `judge/JUDGE_README.md`
