# hallucinate 裁判(消费方)原型 —— 纯 MAST 二元 + clean-baseline 增量对照

> 状态:**原型验通 + 采集设计结论**(2026-07-16)。hallucinate 语义故障的"对应消费方"。
> **纯 MAST 二元 judge**(不引入自造 severity 分级),自发幻觉靠 clean-baseline 增量对照消。

## 1. 为什么要它
hallucinate 是**语义**故障,无确定性契约(不同于 format/wrong_pick 走 contract_validator)。
标准消费方 = LLM-as-judge。这是采集 (5) 前的硬前提:每类故障先有消费方,否则重蹈 M13。

## 2. 抄谁(纯贴已有工作,不发明)
- **judge = 纯 MAST 二元**(NeurIPS2025,`third_party/reference/MAST/llm_judge_pipeline.ipynb`):
  trace → prompt(注入 rubric)→ 每 agent **yes/no** → 正则解析 → checkpoint。**一字不加分级**。
  诚实适配:MAST 14-mode 是 MAS 协调层,我们换成认知层 hallucinate rubric + per-agent 定位(黑板纠错③)。
- **消自发幻觉 = clean-baseline 增量对照**(传统故障注入标准做法:注入 vs 基线):
  同输入跑 K 次**不注入** clean run,judge 每条,统计每 agent 自发 yes 率;faulted 里某 agent 判 yes
  且自发率低 → 归因注入。**不靠调 prompt/自造分级**。

## 3. ★原型实测 + 关键结论(如实,不粉饰)
**clean baseline(5 clean run,不注入)自发 yes 率:**
| agent | 自发 yes 率 |
|---|---|
| Sequence_Recommender | 0.0 |
| User_Behavior_Analyzer | 0.0 |
| **Product_Analyzer** | **0.2**(base LLM 本就爱瞎编,~20% run 自发幻觉) |
| Recommendation_Synthesizer | 0.0 |

**这直接解释了**之前 Product_Analyzer 老 FP:不是污染,是它自发率高。

**但增量对照(单 faulted run + 每 agent 率阈值)实测 NToT 干净解决 confound:**
| 阈值 | Product case | UB case | 结论 |
|---|---|---|---|
| 松(<0.5) | 命中,FP=0 | 命中,FP=Seq+Product | Product 自发率 0.2<0.5 仍被归因 |
| 严(<0.01,须自发率=0) | **漏检**(Product 自发率 0.2≥0.01→被压掉)→ **假阴** | 命中,FP=Seq | 严阈值把真注入也压没了 |

**诚实裁决:** 单 faulted 样本 + 聚合率阈值**是类别错配**——拿一条 run 的二元事件去减一个聚合率,
消不掉"该 run 里某干净 agent 恰好自发幻觉"(UB case 里 Sequence 这一 run 自发 yes、baseline 5 run 没采到)。

### → 采集设计结论(比"假装能用"更有价值,进 EVAL_NOTES)
1. **faulted 也要 K 次重复采样**(注入 Product K 次,judge 每条,比**该 agent 的 faulted yes 率 vs baseline yes 率**)——
   这正是传统故障注入的做法(N reps/fault)。单 run 判据噪声太大。
2. **正确的归因口径 = 率增量显著**:agent 的 (faulted_yes_rate − baseline_yes_rate) 显著为正 → 注入根因。
   逐 run 二元 + 静态阈值不行,必须率对率。
3. 采集 runner 设计据此定:每个 (agent, hallucinate) 组合采 **K reps**(与 agentchaos 每故障多 rep 一致),
   judge 出**率**,与同输入 baseline 率做增量。**GT 仍是台账 status==injected 的 agent**(注入端确定),
   judge 是**消费方/评测方**,不是 GT 来源——GT 由注入台账给,judge 用来算"方法能否消费/定位"。

## 4. 成本 / 偏置
- **成本**:每 judge ~4.3k tok / ~2.6s(串行)。DeepSeek ~¥1/百万 tok → 240 case×K reps 成本仍可忽略;
  瓶颈是串行时延(可并发)。baseline K=5 一次性采、多故障 case 复用。
- **同源偏置**(黑板纠错⑤):原型 judge=DeepSeek,与注入副 LLM 同源。**正式采集换模型族**
  (`AGENTFAULT_JUDGE_MODEL/_BASE/_KEY`)并记 threat。

## 5. 文件 / 用法
- `hallucinate_judge.py` — 纯 MAST 二元 judge + clean-baseline 增量对照 + 成本统计
- `collect_clean_baseline.py` — clean baseline 采集器(K 次不注入 run + judge → 自发 yes 率)
- 输出:`(v1)_smoke/judge/{judge_proto_report,clean_baseline_rates}.json`
```bash
# 1) 采 clean baseline(K 次不注入)
python scripts/chaos/agentfault/judge/collect_clean_baseline.py --runs 5
# 2) judge faulted case,带 baseline 增量(阈值 env 可调,默认 0.5)
python scripts/chaos/agentfault/judge/hallucinate_judge.py --all-smoke \
   --baseline (v1)_smoke/judge/clean_baseline_rates.json
# 换模型族避同源偏置
AGENTFAULT_JUDGE_MODEL=<m> AGENTFAULT_JUDGE_BASE=<url> AGENTFAULT_JUDGE_KEY=<k> python ...
```

## 6. 与其它消费方的关系
| 故障类 | 消费方 | 定位口径 | GT 来源 |
|---|---|---|---|
| format_violation | contract_validator(确定性四查) | 哪项 check 失败 | 注入台账 status==injected |
| wrong_item_pick | contract_validator(item∈候选) | item_in_candidates 失败 | 注入台账 |
| **hallucinate** | **MAST 二元 judge + baseline 增量** | **faulted vs baseline yes 率增量** | **注入台账** |

三类消费方齐备 → 采集 (5) 可开工,但 **hallucinate 必须 K reps/组合**(单 run judge 不够),
且 GT 始终由注入台账给(judge 是评测消费方,非 GT 源)。采集侧铁律见 `injector/INJECTOR_README.md §6b`。
