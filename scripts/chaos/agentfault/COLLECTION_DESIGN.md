# (5) 双轨采集设计 —— agentfault 数据集 runner 规格

> **★ERRATUM(2026-07-17,采集+延迟修复后校正,以此为准):** 本文是**采集前设计文档**,几处已被实况覆盖:
> ① 规模 = **64 faulted + 8 normal = 72 case**(全 8/8,无 inject_failed);
> ② **hallucinate 注入并非"延迟正常"** —— 副 LLM 改写多跑一次 LLM,raw span 慢 ~1.3-2.3×(延迟伪影),
>   故 CSV 加 `span_<A>_duration_corrected_ms` + `span_<A>_subllm_overhead_ms`(埋细 span 减 span),**infra eval 用 corrected**;
>   "infra≈Dummy" 仅对 hallucinate 成立(且掺 split 退化),RF-infra 整体 Hit@1≈0.57>随机,结构化故障 infra 可分;
> ③ CSV = **78 列**(Track B 含上述 corrected/overhead 列);
> ④ wrong_item_pick 台账**已写 `status='injected'`**(§9(1) 采前修已应用),§6b 判据用"非 inject_failed"仍对。
> 权威实况见 `(v1){SUMMARY,EVAL_NOTES,BASELINE_RESULTS}.md`。

> 状态:**设计**(2026-07-16,基于 4 路并行 reader 实测复用面盘点)。把注入器三类 + 两消费方 +
> baseline 率对照串成"能跑出数据集的 runner"。**不发明**:60-70% 抄 agentchaos_runner,
> 输出对齐 agentchaos 格式(eval_agentchaos.py 可零改复用),消费方复用已验通的 contract_validator/judge。
>
> 铁律:不改 `services/**` 与 `chaos/ctk/*`;新文件只落 `scripts/chaos/agentfault/`。GT 由台账给。

---

## 0. 设计目标(一句话)
对每个 (agent, kind) 组合采 **K reps**(+ 同输入 clean baseline K reps),每 rep 一条 case,
记录 **infra 轨**(黑盒 + per-agent span)+ **content 轨**(注入指纹/契约违约/judge 可消费)双轨遥测,
GT 由**注入台账**给(判据 = 存在一条 `status != inject_failed` 的记录;**注意** wrong_item_pick
记录不写 status 字段 → 用"非 inject_failed"语义,**不是** "== injected" 严格等值,见 §9(1)),
产出 agentchaos 同构数据集(CSV + features 视图 + journal + spans)。

## 1. Case 矩阵(采什么)
| 故障类 | 目标 agent | reps | 消费方 | 备注 |
|---|---|---|---|---|
| hallucinate | Sequence_Recommender | K | judge(MAST 二元+baseline 率对照) | analyzer 终答改写 |
| hallucinate | User_Behavior_Analyzer | K | judge | |
| hallucinate | Product_Analyzer | K | judge | 自发率 0.2,baseline 对照重点 |
| wrong_item_pick | Recommendation_Synthesizer | K | contract_validator(item∈候选) | 需喂 candidates |
| format_violation × 4 subtype | Recommendation_Synthesizer | K each | contract_validator(对应 check) | missing_field/type/empty/malformed |
| **normal(clean)** | 无(不注入) | **K′≥5** | judge(自发率) + 作 infra 轨负类 | = baseline + 负样本 |

**规模示意(K=8):** 3 hallu×8 + 1 wrongpick×8 + 4 format×8 = **64 faulted** + normal×8 = **72 case**;
K 可调(agentchaos 默认 8)。**faulted 与 clean 同输入 PROBE_SEQ**(baseline 率才可比)。

> **为何 K reps 是硬需求**(judge 原型实测结论):单 faulted run 的二元 judge 噪声太大
> (base LLM 自发幻觉),必须 **faulted yes 率 vs baseline yes 率** 比 → 每组合 K 次。

## 2. 单 case 端到端流程(runner 主循环)
复用 agentchaos_runner 的"一实例 → K probe 窗"结构(摊薄 ~150s 模型加载):

```
对每个 (agent, kind[, subtype]) 组合:
  1. 起临时 rec_agent 实例(VENV_PY=phase1_venv,非 conda python)
     env = agentchaos start_temp_instance 骨架 [COPY]
         + AGENTFAULT_INSTRUMENT=1                 [content 捕获]
         + AGENTFAULT_INJECT=1                     [faulted;clean 组合不设]
         + AGENTFAULT_KIND_<agent>=<kind>          [先 pop 所有 AGENTFAULT_KIND_*/AGENT_FAULT_*]
         + (wrong_item_pick) AGENTFAULT_WRONG_ASIN
         + (format) AGENTFAULT_FORMAT_SUBTYPE/_FIELD
         + AGENTFAULT_LEDGER=<case ledger.jsonl>   [每组合独立台账]
         + PYTHONPATH=injector/loader : ...         [sitecustomize 先行]
         + SPAN_FILE / OTLP→14318 / NACOS=false / Clash bypass  [COPY]
  2. wait_health [COPY]  → warmup 1-2 uncounted probe
     ★[FIX-A 采前审] warmup probe 必须 **AGENTFAULT_INJECT 不设**(或 warmup 后清空/新建台账),
       否则 warmup 的注入记录混进 K reps 台账,若其 trace_id 为空会污染每个 rep(见 §6b-硬化)。
  3. checksum_before(items/inventory) [COPY,标准 standalone 版]
  4. for rep in 1..K:
       a. probe /recommend(固定 PROBE_SEQ)→ 取 trace_id;**断言 trace_id 非空**(空则该 rep 判无效/重试)
       b. read_spans_for_trace(SPAN_FILE, trace_id) [COPY]
       c. read_ledger(trace_id)  → §6b-硬化:**精确非空 trace_id 匹配**该 rep 是否有 injected 记录
       d. 抽 infra 轨特征 [ADAPT: 复用 span-map 骨架,换特征]
       e. 抽 content 轨特征 [NEW: 注入指纹/契约违约/judge 输入]
       f. 判定该 case GT + 消费方判据(见 §3/§4)
       g. append CSV row + journal/<case_id>.json + skip-if-exists checkpoint [ADAPT]
  5. checksum_after + zero-drift 断言 [COPY]
  6. stop_temp_instance [COPY]

单独跑 clean baseline(K′ reps,AGENTFAULT_INJECT 不设):
  复用 collect_clean_baseline.py → clean_baseline_rates.json(judge 率对照用)
```

**§6b 铁律落地(★采前审硬化 FIX-A,critical):** rep 的 faulted 标签**只**在该 rep 的
**非空 trace_id 精确匹配**到一条 `status!=inject_failed` 台账记录时打。**关键**:参考实现
`read_ledger_entries`(injector_smoke L327-339)对 **trace_id 为空**的台账记录会**通配匹配每个 rep**
(`if trace_id and e.get('trace_id') and ...` 中间项为空即跳过不等判断)→ 一条空 trace 的注入记录会把
K 个 rep **全部**误标 faulted(即便某 rep 实际 inject_failed = false-faulted 脏行)。**新 runner 不得
逐字照抄这段**,必须:① 注入记录 trace_id 为空 → **硬拒/丢弃**该记录不参与匹配;② 每 rep 断言 probe 响应
trace_id 非空;③ warmup 不注入。〔实测 4/4 冒烟台账均带合法 32-hex trace_id,此为 K>1 且注入时序正常下
的休眠隐患,但 runner 必须防御性硬化,否则一次空 trace 即污染整组〕。绝不按 env 意图标。

## 3. GT(标准答案,来自台账,非 judge)
| 故障类 | GT 实体 | GT 来源 | fault_type |
|---|---|---|---|
| hallucinate | agent 名(root_cause_agent_set) | 台账 injected 记录的 agent 字段 | hallucinate |
| wrong_item_pick | Recommendation_Synthesizer | 台账 | wrong_item_pick |
| format_violation | Recommendation_Synthesizer | 台账 + violation.subtype | format_violation(+subtype) |
| normal | 空集(无根因) | 无台账 injected 记录 | none |

**照 agentchaos GT 结构**(journal `ground_truth`):`root_cause_agent_set`(agent 名 LIST)+
`n_root_causes` + `fault_type_set` + `per_agent_fault`(4 agent→kind|none)。候选空间 = 4 agent。
**GT 实体是 in-process agent 名,非 service**(与 k8s 交付格式的关键区别,承 output-schema 盘点)。

## 4. 双轨特征(采什么遥测)
**infra 轨(RF Track A/B,复用 agentchaos 口径,eval_agentchaos.py 零改可用):**
- Track A 黑盒 11 列:e2e_latency_ms / http_status / http_success / total_span_count /
  error_span_count / recommendation_confidence / recommended_product_is_unknown /
  degrade_message_present / **format/hallucinate 类需换等价 quality flag** / host_cpu_pct / host_mem_pct
- Track B +per-agent span:span_<A>_{duration_ms,status,present}(httpx/sasrec child count 对 agent 语义故障
  意义弱,可留可删)+ conv_<A>_text_len
- **诚实预期 + ★采前审 FIX-D 混淆警告**:agent 语义故障对 infra 轨**基本不可见**(注入后延迟/状态/
  span 数正常)→ Track A 应接近 Dummy(对照臂)。**但**:3 个 hallucinate agent 各只在**唯一一个组合**
  里当 GT → LOGO/SGKF 留一组即把该 agent 训练正类清零 → 其分类器退化为常量 0 = 结构性随机。∴
  **"Track A≈Dummy" 对 hallucinate 组合是「infra 盲」与「split 退化」两因混淆,不可单独归因 infra 盲**。
  wrong_item_pick+4 format 都在 Synthesizer(5 组合)→ 留一还有正类,infra 可正常训。**如实写进 EVAL_NOTES**
  (§7.8),别把 hallucinate 的随机 Top@1 当"infra 盲"卖点;真正卖点靠 content 轨(下)。

**content 轨(NEW,注入器/消费方已产,runner 只需归集):**
- hallucinate:该 agent ChatOpenAI output content 里 `divergent_needle` 是否出现(注入指纹可机检);
  judge yes/no(用于率对照,非 GT)
- wrong_item_pick:Synthesizer tool_call 的 recommended_product(==哨兵?)—— **主判据 = 响应/span ASIN==哨兵**
  (已验通);contract `item_in_candidates` **仅** `/recommend/from_candidates` 路径可用(候选给定),`/recommend`
  路径候选=SASRec 全词表不在响应里 → 该查退化(见 §9(2))。
- format_violation:Synthesizer tool_call 过 contract_validator → `first_failed_check`(==期望 subtype?)
- 通用:ledger status(injected/inject_failed)、divergent_needle/violation.subtype(GT 溯源)

**content 轨是新颖贡献点**:传统 infra-RCA 盲、content 轨可定位/消费。

## 5. 消费方接线(被对应方法消费,你强调的验收点)
| 轨 | 消费方 | 判据 | 复用状态 |
|---|---|---|---|
| infra | RF Track A/B(eval_agentchaos.py) | Top@1/Recall@root-set,LOGO+SGKF5,SEEDS 0-4 | **需路径参数化**(见下 FIX-C,非零改) |
| content:format_violation | contract_validator | first_failed_check==期望 subtype | 已 LIVE 验通 |
| content:wrong_item_pick | **响应/span ASIN==哨兵**(主) + contract item∈候选(仅 from_candidates) | ASIN 被换 | ASIN 判据已验通;contract 仅 format 验过 |
| content:hallucinate | judge + baseline 率对照 | faulted 率 − baseline 率 显著 | 原型验通(判据待实装,见 §5-统计) |

> **★采前审 FIX-E(§5 措辞校准)**:wrong_item_pick 的**已验通**消费判据是**响应/span 里 recommended_product
> == 哨兵 ASIN**(injector_smoke 已 LIVE 验),**不是** contract_validator——contract 只在 format_violation 路径
> LIVE 验过。contract 的 `item_in_candidates` 查只在 `/recommend/from_candidates`(候选显式给定)成立;
> 采集若走 `/recommend` 则用 ASIN==哨兵 判据(如实,不套 contract 框)。

**★采前审 FIX-C(§8 "零改"纠正,CONFIRMED)**:`eval_agentchaos.py` / `make_agentchaos_features.py`
**硬编码** `(archived) agentchaos/*` 输入输出路径(前者 L42-44,后者 L29-31),**不读 argv/env** → **非零改**。
必须:① 把 dataset 目录参数化(argv/env)或 fork agentfault 副本;② **CSV 列 schema 钉死** —— label 列名
必须是 `root_cause_set`(**分号 `;` 连接** agent 名,非 `root_cause_agent_set` LIST)+ `fault_<Agent>` +
全部 FEATURE_COLS,否则 `make_agentchaos_features` 切分出空 → `y_rc__*` 全 0 → 所有 case 看似"无根因" →
infra eval 静默退化(bad-eval footgun)。**§8 复用清单"零改"改为"路径参数化 + 列名对齐后可复用"**。

**hallucinate 率对照(判据,非 GT;★采前审 FIX-F 统计口径定死):** 每 (agent, target-组合) 统计 K reps 里
judge yes 次数 → `faulted_yes_rate`;与 clean_baseline K′ reps 的 `baseline_yes_rate` 比。**归因判据 = 率增量
显著**:用 **Fisher 精确检验**(2×2:agent 在 faulted vs baseline 的 yes/no 计数)或 **bootstrap CI**,
显著性 α=0.05;**不是**单 run 二分 + 静态阈值(原型 judge_case 的 per-case 阈值法已知会把自发率 0.2 的
Product 误归因,弃用)。**K′ 需 ≥ 10**(K′=5 时 0.2 率的 SE≈0.18,阈值被估计噪声淹没)。GT 始终台账给,
judge 是评测方。

## 6. 输出格式(对齐 agentchaos,eval 脚本可复用)
```
(v1)                     (正式采集区;_smoke/ 是冒烟区)
  dataset_agentfault.csv                 每 rep 一行(infra+content 列 + label/provenance 列)
  features_agentfault.csv                去泄漏 feature 视图(照 make_agentchaos_features 口径)
  clean_baseline_rates.json              judge 率对照
  SUMMARY.md                             数据集权威文档(case 矩阵/语义/局限)
  EVAL_NOTES.md                          诚实须知(见 §7)
  FEATURES_README.md                     feature/leak/split 协议
  BASELINE_RESULTS.md                    eval 结果(infra Track A/B + content 消费方)
  journal/<case_id>.json                 每 rep GT+config+checksum(台账溯源)
  spans/<combo>.jsonl                    每组合原始 span(含 content 属性)
  run_summary.json                       run manifest
```
**REGISTRY.json** 注册新 tree(agentfault,`covered_by_cases:false` 因结构异于 k8s case-dir)。

## 7. EVAL_NOTES 诚实须知(照 gtfix 交付惯例,预先钉)
1. **split**:按 (agent,kind) 组合切(LOGO by group_id=组合),**不按 rep 切**(泄漏);
   K reps/组合。infra 轨 group-aware LOGO+SGKF5。
2. **hallucinate GT 口径**:GT=台账注入 agent;judge 是消费方非 GT 源;**单 run judge 噪声大,须率对照**
   (faulted vs clean baseline)——judge 原型实测结论。
3. **infra 轨对 agent 语义故障近盲**(Track A≈Dummy 是**预期**,证 infra-RCA 够不着,非缺陷);
   content 轨才可定位 = 数据集卖点。如实标,不藏。
4. **同源偏置**:hallucinate 注入副 LLM=DeepSeek;judge 须换模型族(AGENTFAULT_JUDGE_MODEL),记 threat。
5. **非流式 threat**:测试床强制非流式(收口 _generate);baseline 与 faulted 同 regime 采,差异抵消。
6. **base LLM 自发幻觉**:Product_Analyzer 自发率 ~0.2(实测);故 hallucinate 定位须率增量,非单次二分。
7. **candidates 来源**:wrong_item_pick 主判据=响应/span ASIN==哨兵(已验);contract item∈候选查**仅**
   `/recommend/from_candidates`(候选显式给定)成立,`/recommend` 路径候选=SASRec 全词表不在响应里 → 退化。
8. **★采前审 FIX-D:hallucinate 组合 infra Top@1 是 split 退化非纯 infra 盲** —— 3 hallu agent 各只在唯一
   组合当 GT,LOGO 留一组即清零该 agent 训练正类 → 分类器退化常量 0 = 结构随机。**不可**把这随机 Top@1
   单独当"infra 盲"证据(与 split 退化混淆)。要么每 agent 加多组合让 LOGO 留正类,要么 infra 轨只当
   异常检测对照(非定位),要么显式声明此混淆。真正"infra 盲、content 可定位"靠 content 轨消费方(不走 RF split)。
9. **★采前审 FIX-B:每组合 QC 断言 ≥1 faulted rep** —— 若某组合 K reps 全 inject_failed(如 hallucinate 副 LLM
   连续拒答),该组合正类为空,须告警不静默;normal 组合不产 injected 记录属正常。

## 8. 复用清单(抄多少,一览)
| 组件 | 来源 | 动作 |
|---|---|---|
| 临时实例生命周期(起/健康/probe/停) | agentchaos_runner L251-342 | **COPY**,只换 fault-env 块 |
| trace_id 归窗 read_spans_for_trace | agentchaos_runner L348-377 | **COPY**(机制无关) |
| SPAN_FILE JSONL 接线 | 服务侧 exporter + env | **COPY** |
| CHECKSUM 守卫 | injector_smoke standalone L138-161 | **COPY**(避 toxiproxy 耦合) |
| K reps 循环(一实例 K 窗) | agentchaos_runner L731-775 | **COPY** |
| 增量写 CSV+journal | agentchaos_runner L612-646 | **ADAPT** + 加 skip-if-exists checkpoint |
| span→record 聚合 | agentchaos L380-470 骨架 | **ADAPT**:换 content 轨特征(injector_smoke 提取器) |
| content 提取器(ChatOpenAI content/tool_call/ledger) | injector_smoke L246-352 | **COPY** |
| 消费方 contract_validator / judge | 已验通 | **COPY** |
| infra 轨 eval + feature 视图 | eval_agentchaos.py / make_agentchaos_features.py | **路径参数化 + 列名对齐后可用**(★FIX-C:非零改,硬编码 (archived) agentchaos 路径;label 列须 `root_cause_set` 分号连接 + `fault_<Agent>`) |
| **不抄**:cross_layer/toxiproxy 子系统(~40%)、delay/error/garbage 特征、AGENT_FAULT_* env-hook | agentchaos L120-210/803-1461 | **DROP** |

## 9. 已知缺口 / 采前须补(reader 盘点 + ★采前审补充)
1. **wrong_item_pick 台账无 status 字段** → **采前给注入器 wrong_item_pick 分支补 `status='injected'`**
   (与 hallucinate/format 统一),同时 §6b/§0 判据用"非 inject_failed"语义(不用严格 ==injected 等值),两头都对齐。
2. **candidates 不在 /recommend 响应里**(候选=SASRec 全词表)→ wrong_item_pick 主判据用响应/span ASIN==哨兵
   (已验);若要走 contract item∈候选查须用 `/recommend/from_candidates` 路径显式喂 SASRec top-100。
3. **content-layer OTel 属性 schema 未固化** → 现靠 openinference `llm.output_messages.*.content` /
   `tool_call.function.arguments`;采集直接读这些(已在 injector_smoke 验通),无需新埋点。
4. **VENV_PY 依赖 phase1_venv 存在**(--system-site-packages 继承 conda langchain+openinference)。
5. resume/checkpoint agentchaos 没有 → 新增 skip-if-exists(journal/<case_id> 存在则跳)。
6. **★采前审 FIX(normal/infra 负类 CSV):** `collect_clean_baseline.py` **只产 rates JSON,不产 CSV 行**。
   infra 轨 RF 需要 normal 组合的**负样本 CSV 行**(y_rc__* 全 0)才能训分类器。**采集 runner 必须让 normal 组合
   也走 §2 主流程产 CSV+journal 行**(AGENTFAULT_INJECT 不设,GT=空根因),**不是**只调 collect_clean_baseline。
   clean baseline(judge 率对照用)与 normal CSV 负类行是**两个产物**,前者喂 judge、后者喂 infra RF。
7. **★采前审 FIX(baseline/负类可能双用泄漏):** clean run 若**同一批**既估 baseline_yes_rate 又当 infra 训练负类,
   两处共用同批数据。infra 轨是 group-aware LOGO(按组合分组),normal 自成一组,与 hallucinate 组合不同组 → 不泄漏
   到 hallucinate 定位;但**如实在 EVAL_NOTES 声明** clean run 双用,或干脆 baseline 与 normal-负类分开采两批。

## 10. 实现编排(下一步,不在本设计内)
主循环调度:coder 实装 runner(照 §8 复用清单)→ reviewer 对抗审(无数据突变/§6b 铁律/GT 台账/幂等只读)
→ 主循环亲驱小规模冒烟(1 组合×2 rep,核 CSV/journal/checksum/消费方)→ 扩全量 K reps。
**长采集主循环 nohup 亲驱 + Monitor 盯 per-case,不外包 subagent 撒手**(黑板铁律)。
