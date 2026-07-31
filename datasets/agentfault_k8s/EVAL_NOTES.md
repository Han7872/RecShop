# EVAL_NOTES —— `agentfault_k8s` 怎么评，以及四条必读局限

> 配套 `SUMMARY.md`（是什么 / 怎么采）。机器可读披露见 `limitations.json`。
> 本树与 `(upstream batch)` **同设计、不同环境**，设计上就是拿来**对读**的。

---

## 1. 一键跑评测

```bash
bash scripts/chaos/agentfault/run_eval_agentfault.sh --dataset-dir datasets/agentfault_k8s
#   默认免费(不调判官 LLM)。判官藏在 --with-judge(DeepSeek) / --with-glm(GLM，慢，小时级)
```

四套 harness **一行都不用改**，只换 `--dataset-dir`（验收 D4 就是跑这四套来证明这件事）：

| # | 脚本 | 产物 |
|---|---|---|
| 3 | `compute_context_drift_outcome.py` | `context_drift_outcomes.json`（**是第 6 步的硬前置**） |
| 4 | `eval/eval_agentfault_tierA.py` | `BASELINE_RESULTS.md` |
| 5 | `eval/infra_negatives/run_infra_negatives.py` | `infra_negatives/` |
| 6 | `eval/content_ctxdrift_track.py` | `RESULTS_CONTENT_CTXDRIFT.md` |

> ⚠️ 顺序不能乱：第 6 步硬依赖第 3 步的产物，缺了直接 `rc=2`。

---

## 2. ★四条必读局限（不读会得出错的结论）

### 2.1 零根臂**会自发幻觉** —— 内容型检测器有底噪，结构型没有

`normal__r12`（**无任何注入**的对照 case）产出了 **7 个不存在于权威表的 ASIN**。

| 信号类型 | 零根臂表现 |
|---|---|
| **结构信号**（`agentfault.resolved_input` 签名） | 偏离率 **0.000000**（98 条样本 / 4 agent） |
| **内容型「编造 ASIN」** | 假阳 **≥ 1/12 ≈ 8.3%** |

⇒ 报"用 ASIN 查不到当阳性特征"的方法时，**必须扣掉这个底噪**，否则会把对照臂的自发幻觉
算成检出。样本量 n=12，该比例只作**下界提示**，不是稳定的假阳率估计。

（REF `agentfault_v2` 的 normal 臂 0/12 出现该现象 —— 所以这条在两批之间**不可直接平移**。）

### 2.2 时间通道被注入本身污染，**必须用 `*_corrected_ms` 不用 raw**

hallucinate 族靠副 LLM 改写实现，天然多一次 LLM 调用 ⇒ 时延必然变长。
本树给副 LLM 埋了专属 span，CSV 里有 `span_<agent>_subllm_overhead_ms` 与
`span_<agent>_duration_corrected_ms`（= raw − overhead）。

**用 corrected 列**。用 raw 列会得到一个"延迟启发式几乎完美"的假象
（v2 上实测 RAW 1.00 → CORRECTED 0.208）。本树副 LLM overhead 合计 **299,556 ms**。

### 2.3 `format_*` 的随包台账只有 1/12（**v2 也是**）

`ledgers/format_Recommendation_Synthesizer.jsonl` 只有 1 条 trace。
**GT 没丢**（在 CSV 与 journal 里），丢的是**原始台账这个随包工件**。
全树 **85/96 = 88.54%** 的 faulted case 可从 `ledgers/` 复算；不可复算的 11 条**全在这一个 combo**。

⇒ 若你的方法要"逐条回溯注入证据"，`format_*` 这 12 个 case 只能靠 CSV/journal 的结论，
不能靠原始台账。用 `measure_ledger_completeness.py` 可随时复算这个数。

### 2.4 与 `agentfault_v2` 对读时的三处口径差

| 项 | v2 | 本树 | 影响 |
|---|---|---|---|
| 候选过采倍数 | `×3` | **`×10`** | v2 有 7% 的调用只拿到 4 个候选，本树 129/129 都是 5 个。**严格增量**（非缺额调用的输出逐字不变） |
| 零根臂观察器 | 无 | **有**（98 条 span） | v2 测不了结构检测器的误报率，本树能 |
| host 水位口径 | 宿主全局 `virtual_memory().percent` | **容器级** cadvisor | **不同物，勿混比**。CSV 里 `host_metric_source` 已标 |

**不受影响、可直接对读的**：CSV 前 82 列逐字节同序（A1 PASS）、9 个 combo 的
`carrier_seq_id` 集合逐一相等（E1 PASS）、`probe.top_k` 取值集合相等（E3 PASS）、
故障族分布相同（36/36/12/12/12）。

---

## 3. 评测口径提醒

- **常量基线不能省**：`Recommendation_Synthesizer` 是 4 个 agent 里被注入最多的一个，
  "永远猜 Synthesizer"就有相当高的 Hit@1。任何 agent 定位方法都要**与常量基线并列报**，
  否则数字没有意义。
- **契约信号是设计出来的**：`wrong_item_pick` / `format_violation` 两族的确定性内容信号
  （哨兵 ASIN / 结构破坏）是**注入器有意留下的**，检出率高不代表方法强。
  真正的难点在 `hallucinate` 与 `context_drift`。
- **infra 侧方法（BARO / RCD）在本树上是诚实负例**：它们看的是基础设施指标，
  而本树的故障发生在 **agent 语义层**，指标面本来就没有信号。
  `host_cpu_pct` 两臂可分性实测 **p = 0.6214（不可分）** —— 这正是"infra-RCA 对 agent 语义故障盲"
  的直接证据，不是方法没调好。

---

## 4. 验收状态与原始报告

- 规范：`(project docs)/agentfault-k8s-recollect-20260727.md`（43 条闸）
- 报告：`(内部验证报告)`
- **BLOCK-RECOLLECT 零条**（数据物理性质全部合格）；其余项逐条见 `limitations.json`
