# agentfault_k8s —— agent 语义故障数据集（K8S 全栈批次）

> **一句话**：与 `(upstream batch)` **同一套 9 combo × 12 rep = 108 case**，
> 但跑在**真实的 25 微服务 K8S 集群**里，而不是本机隔离 harness。
>
> **先读这两份**：本文件（是什么 / 怎么采 / 现算的数）+ `EVAL_NOTES.md`（怎么评 / 必读局限）。
> 机器可读的披露清单在 `limitations.json`；验收原始报告在 `(内部验证报告)`。

---

## 0. 定位：它是什么、不是什么

**是**：同一批故障设计在**真集群**里的重采。agent（`rec-agent` pod）跑在 25 服务全在役的
namespace 里，经**集群 DNS** 调真实的 `sasrec` pod，OTLP 导到与 traditional 255 同一个 collector。

**不是**：
- **不是**"一次请求穿过 25 个服务"。`/recommend` 的调用图只经 **rec-agent → sasrec + DeepSeek**
  （业务上本来也不需要别的）。其余 23 个服务是**在役并共享节点 / DB / collector**，不是被调用。
- **不是**多服务三模态数据集。本树只采 **rec-agent 自己的**遥测（86 列，全 agent 中心）
  + rec-agent pod 的容器级 host 水位。25 服务的三模态在 **traditional 255** 那条线。
- **不是** `agentfault_v2` 的替代。两者**同设计、不同环境**，应当**对读**而非二选一。

---

## 1. 规模与构成（全部现算）

| 项 | 值 |
|---|---|
| case 总数 | **108** |
| faulted / zero-root | **96 / 12** |
| combo × rep | **9 × 12** |
| CSV 列数 | **86**（REF 82 + 尾部追加 4 列 provenance） |
| span 合计 | **10,011**（min 77 / p50 97 / max 137 每 case） |
| e2e 时延 | min 31.5s / p50 48.4s / p95 59.8s / max 72.1s |
| `error_span_count` 非零的 case | **0** |

故障族分布（与 v2 设计一致）：

| 族 | case 数 |
|---|---|
| `hallucinate` | 36 |
| `context_drift` | 36 |
| `wrong_item_pick` | 12 |
| `format_violation` | 12 |
| `normal`（零根） | 12 |

---

## 2. 与 `agentfault_v2` 的三处**有意**差异（都是本批次更好的一侧）

### 2.1 候选侧真有商品语义

| | v2（本机） | **本树（K8S）** |
|---|---|---|
| 候选 (id,title) 槽 | 636 | **645** |
| 占位符 `Product_<id>` | 0 | **0** |
| 兜底串「未知商品」 | 0 | **0** |
| 与权威表逐字一致 | ✅ | ✅（47 个不同候选全部命中） |

> **注意这曾经是坏的**：本数据集的**前两轮**采集里，候选标题**全部**退化成「未知商品」
> （46 个 distinct 候选，真标题 0 个）。根因不在 rec-agent 的过滤逻辑，而在
> **K8S 的 sasrec pod 没挂 `electronics.item`** ⇒ `item_info` 空 ⇒ 响应 `title=null` ⇒
> `tools.py` 的 `or "未知商品"` 兜底生效。已修（见 §3 采集环境）。
> 详见 `limitations.json` 与验收规范 §4.6。

**过滤器确实执行过**（可证伪命题，不是形容词）：645 个候选槽里占位符 **0**，
而权威表的占位符率是 **26.05%** ⇒ 无过滤零假设下 p ≈ **2.9e-85**。

### 2.2 候选条数不再有缺额

| | 候选调用数 | 每次候选条数分布 | 不足 5 个 |
|---|---|---|---|
| v2 | 129 | `{4: 9, 5: 120}` | **7.0%** |
| **本树** | 129 | `{5: 129}` | **0%** |

原因：过采倍数 `×3 → ×10`。SASRec top-K 里占位符标题约占**一半**（远高于全表 26.05%），
×3 不够。**这是严格增量改动**：`filtered[:top_k]` 按 SASRec 原序取前 `top_k`，
原本就有 ≥`top_k` 个存活的调用**输出逐字不变**。

### 2.3 零根臂装了观察器（H1，v2 缺的那一半）

| | v2 | **本树** |
|---|---|---|
| `normal` 臂的 `agentfault.resolved_input` span | **0 条** | **98 条** |

⇒ 可以**测量**结构化检测器在无故障运行上的误报率，而不是假设它是 0。

**★零根臂结构基线实测：签名偏离率 = `0.000000`**（98 条样本 / 4 个 agent，set 口径，
非退化断言全过）。**这个数不许被措辞掩盖** —— 它是"结构信号在对照臂上零误报"的直接证据。

> ⚠️ 但**内容型**信号不是 0：见 §5 的零根臂自发幻觉。两者必须分开陈述。

---

## 3. 采集环境 = 25 微服务 K8S 全栈

| 项 | 值 |
|---|---|
| 镜像 | `recweb-rec-agent:agentfault-v2`，`RECWEB_SRC_GIT_SHA=8dfab20` |
| 节点 digest | `sha256:0492dd6dbe06e97fba366b193ae3abae4d275b0c7a5863141df17293db08b7fd` |
| 探测通道 | `kubectl proxy` → `svc/rec-agent:5001`（**不是** port-forward：它绑定单个 pod，rollout 一次就断） |
| 下游 | `sasrec:8200`（**集群 DNS**）+ `api.deepseek.com` |
| OTLP | pod 原生 `OTEL_EXPORTER_OTLP_ENDPOINT` → 与 traditional 255 同一个 collector |
| host 水位 | Prometheus / cadvisor **容器级**（`host_metric_source=prom_container`，与本机口径**不同物**，CSV 里已标） |
| pod 重启 | **108/108 行 `k8s_pod_restarts == 0`** |
| 用到的 pod | 9 个 combo 共 **21 个互不相交的 pod** |

**集群 DNS 指纹**：业务面 **129 条** sasrec 调用 **100%** 走 `sasrec:8200/recommend`，
loopback 占比 **0**，且 `workflow.py:599` 那个硬编码探针 span **0 条**。

> **本批次专有的环境改动（采后已还原）**：给 sasrec pod 以 subPath+readOnly 挂了
> `electronics.item`（`k8s/patch_sasrec_itemfile.ps1`），否则候选侧没有商品语义。
> 实测代价：启动 29s→110s（startupProbe 预算 300s）、RSS 10.02→10.16GB（+140MB，节点 32.8GB）。
> 采完用 `restore_sasrec_stock.ps1` 还原，保后续 traditional 采集的环境可比性。
> 详见 `limitations.json` 的 `ENV-SASREC-ITEMFILE`。

---

## 4. 采集过程（一键脚本 + 一次补采）

```bash
# 主采集(9 combo × 12)
bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes --backend k8s
# 补采(1 个 inject_failed)
bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes --backend k8s \
     --only hallu_Sequence_Recommender
```

- **2 次调用，零逃生开关，`warmup ≥ 1`，combo 并集覆盖全集** —— 留痕在
  `provenance/invocations.json`，原始日志随树同发 `provenance/collect_run.log`（验收会回日志复核）。
- **1 个 `inject_failed`**（`hallu_Sequence_Recommender__r1`，副 LLM 返回空或与原文相同的改写），
  已同载体补采清零。补采前备份了该 combo 的 `spans/` **与 `ledgers/`** 两个文件并在补采后合回
  —— 台账那一半是本轮新加的步骤（前一轮因为漏了它，两个 combo 各丢了 11 / 10 行台账）。
- **CHECKSUM 零漂移**：9 个 combo 的 `items` / `inventory` CHECKSUM 前后逐一相等。
- **幂等**：原样重跑一次，9 个 combo 全部 resume SKIP，CSV sha256 **逐位相同**（零新增行）。

---

## 5. ★发布前必须知道的三条

### 5.1 零根臂会**自发**幻觉（内容信号的底噪）

`normal__r12`（**无任何注入**）产出了 **7 个不存在于权威表的 ASIN**，agent 随后还对它们
调了 `get_product_details`。REF（v2）的 normal 臂 **0/12** 出现该现象。

⇒ **结论必须分开说**：
- **结构信号**（`agentfault.resolved_input` 签名）零根臂偏离率 **0.000000**；
- **内容型「编造 ASIN」检测器**零根臂假阳至少 **1/12 ≈ 8.3%**。

样本量小（n=12），该比例只作**下界提示**，不足以当稳定的假阳率估计。评测里把
"ASIN 查不到"当阳性特征时必须扣掉这个底噪。

### 5.2 GT 事后不可**逐条**复算（C3，BLOCK-RELEASE，本轮不补）

96/96 个 faulted case 的 journal 里没有 `ground_truth.matched`（原始台账条目）。
**GT 本身没丢**（`root_cause_set` / `injected` / `ledger_status` 是采集当时写进 CSV 与 journal 的，
C1 GT 守恒与 C5 交叉表都 PASS），缺的是**事后逐条证伪**的能力。

实际影响面**有数**：全树 **85/96 = 88.54%** 的 faulted case 能从随树同发的 `ledgers/` 复算，
不可复算的 **11** 条全部落在 `format_Recommendation_Synthesizer` 一个 combo 上（见 §5.3）。

**实际影响面（实测，不是估计）**：
- **评测零影响** —— 全 eval 链 grep `ground_truth.matched` **零命中**，四套 harness 的 GT 都从 CSV 列取；
- 不可复算的 11 条**有独立于台账的佐证**：`contract_check_matches_expected = 1`（**11/11**）。
  契约校验器检查真实 tool call 输出，确认观测到的违反恰好是该 `format_subtype` 应造成的那一种。
- ★**但这有前提**：佐证成立是因为丢的恰好是 `format_violation`（确定性注入、契约可检）。
  同样的丢失若落在 `hallucinate` 族，残余不确定性就是真的。⇒ **C3 的严重性依 combo 而定**，
  本批次丢的位置恰好是佐证最强的那个；闸报 FAIL 是对的，它不该假设每次都这么走运。

为什么不补：见 `limitations.json` 的 `C3` 条（一句话：采集期改 runner 对本轮无效却污染 provenance；
采后补写只是把本来可推导的东西再写一遍，反而给人"自包含"的错觉）。

### 5.3 `format_*` 的随包台账只剩 1/12（C2，结构性，**非本轮回归**）

`ledgers/format_Recommendation_Synthesizer.jsonl` 只有 **1** 条 trace（应 12）。
机制：`sync_ledger` 用 `_write_local()` **覆盖写**，而 `format_*` 是 per-rep combo
（每 rep 换实例、每 rep 重新 sync）⇒ 只剩最后一个 rep。
**已发布的 `agentfault_v2` 的 `format_*` 同样是 1/12** —— 这不是本批次的问题。

---

## 6. 验收状态（2026-07-27）

```bash
python scripts/chaos/agentfault/k8s/verify_recollect_acceptance.py \
    --tree datasets/agentfault_k8s --ref (archived) agentfault_v2 \
    --live --jaeger-url http://localhost:16686 --with-item-file --with-eval \
    --rerun-log (collection logs)agentfault_k8s_rerun.log --rerun-post-sha <CSV sha256> \
    --json-out (内部验证报告)
```

规范：`(project docs)/agentfault-k8s-recollect-20260727.md`（43 条闸，
自检含 8 组变异 + 6 组证据等级输入）。

**结果：BLOCK-RECOLLECT 一条都没有**（数据的物理性质全部合格，不需要重采）。
剩余项与处置见 `limitations.json`。

---

## 7. ★评测全景（五条轨，已全部跑完；与 v2 对读复现）

本批次已跑齐 v2 的同一套评测（Who&When / A2P 的 DeepSeek 判官轨于 2026-07-28 补齐）。
**结论：B 档（K8S 全栈）复现 v2（本机）的全部归因结论** —— 把 agent 放进 25 服务的真集群，
五条轨的数字与 v2 同量级、同模式。各轨细节在各自的 `RESULTS_*.md`，本节是总览。

| 轨 | 头条数（Hit@1，分族看） | 详见 |
|---|---|---|
| Tier-A（trivial / RF 上限 / 契约 oracle） | 契约 oracle：wrongpick **1.000** / format **1.000**；hallucinate trivial-corrected 0.278（≈随机）；context_drift 全轨 ≈随机 | `BASELINE_RESULTS.md` |
| infra-负例（BARO / RCD / Eadro） | BARO/RCD 在 corrected 通道 0.219 / 0.250（≈随机地板，预期负例）；raw 是注入伪影对照；Eadro 结构性 N/A | `infra_negatives/RESULTS_INFRA_NEGATIVES.md` |
| context_drift 结构轨 | 观察型检测器（`agentfault.resolved_input`）36/36 = **1.000**；其余输出/infra 轨全盲 | `RESULTS_CONTENT_CTXDRIFT.md` |
| **Who&When / A2P（DeepSeek）** | all_at_once 总体 **0.427**（vs 常量基线 0.375）；hallucinate 0.528（先验 0 ⇒ 真定位）；context_drift 0.194（≈随机，预期盲） | `RESULTS_WHENWHEN.md` |

**三条不重叠的"只有 X 能定位 Y"**（与 v2 一致）：① 结构化故障（wrongpick/format）= 确定性内容
信号可定位，但须对照先验 0.375；② hallucinate = 只有内容感知判官能定位（all_at_once 0.528，
先验 0）；③ context_drift = 对所有输出阅读型方法全盲，只有轨迹结构信号能定位（1.000）。

**与 v2 的对照**：Who&When 四方法总体 Hit@1 全在 v2 的 ±0.05 内（all_at_once 0.427 vs v2 0.406），
per-family 模式一致 —— 把 agent 从本机 harness 搬进真集群，归因结论不变。完整对照表见
`RESULTS_WHENWHEN.md`。

⚠️ **本批次 Who&When 只跑了 DeepSeek，未跑 GLM 跨族复核**（v2 跑了 `whowhen_results_glm52.json`）。
同族偏差风险（judge DeepSeek = 幻觉注入副 LLM 同族）按 SPEC §2 披露；GLM 复核是待办
（`run_whowhen.py --method all --dataset-dir datasets/agentfault_k8s --model glm-5.2 --tag glm52`，需 GLM 额度）。

---

## 8. 附录 A：本文件里每个数怎么复算

```bash
# 候选面语义 / 条数分布 / 历史侧劣化归因 / 零根臂自发幻觉
#   —— 都基于验收脚本自己的抽取口径(V.Tree / V.norm_title / V.RANK_RE),不另写解析
python - <<'EOF'
import sys, csv, io, collections
sys.path.insert(0, 'scripts/chaos/agentfault/k8s')
import verify_recollect_acceptance as V
T = V.Tree('datasets/agentfault_k8s'); t = T.tools()
# 候选条数分布
per = [len([1 for x in (r.get('out') or '').splitlines() if V.RANK_RE.search(x)])
       for sp in T.spans().values() for r in sp['recs'] if r['name'] == V.TOOL_CAND]
print('候选条数分布 =', dict(collections.Counter(per)))
EOF

# 台账完好率(逐 combo)
python scripts/chaos/agentfault/k8s/measure_ledger_completeness.py --tree datasets/agentfault_k8s

# 全部闸(离线部分,不碰集群)
python scripts/chaos/agentfault/k8s/verify_recollect_acceptance.py \
    --tree datasets/agentfault_k8s --ref (archived) agentfault_v2 --with-item-file
```

## 9. 指针

- 怎么评 / 必读局限：`EVAL_NOTES.md`
- 机器可读披露：`limitations.json`
- 验收规范：`(project docs)/agentfault-k8s-recollect-20260727.md`
- 采集编排代码出处：`(project docs)/provenance-map-k8s-backend-20260727.md`
- 时序史：`(project docs)/archive/TASK-K8S-agentfault.md`
- 本机对照批次：`(upstream batch)`
