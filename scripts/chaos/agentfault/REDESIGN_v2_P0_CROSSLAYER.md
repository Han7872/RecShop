# agentfault v2 重采设计稿 —— P0 benchmark 及格线 + 跨层双根(agent×infra)

> 状态:**设计稿待用户拍板**(2026-07-19)。基于三路侦查(TAXONOMY + runner 多样化面 + Chaos Mesh combo 原语)
> 与 injector-进-K8S spike(5/5 全绿)的事实。母本承 `COLLECTION_DESIGN.md`(v1,72 case)。
> 决策点见文末"★待拍板";拍板后再拆实现工单。

## Context —— 为什么重采

v1(72 case)已闭环并跨族验证(DeepSeek + GLM-5.2 双 judge),核心叙事成立,但离"标准 benchmark 及格线"
还差四项结构缺陷(judge panel + 上一轮实测暴露):

1. **GT 先验重偏**:40/64(62.5%)GT = Synthesizer(format 4 subtype + wrongpick 全压在它身上),
   常量基线靠先验就拿 0.625 —— 任何 overall 数字都要先减这个先验。
2. **case 同质**:72 个 case 几乎是同一段对话(同用户/同历史/同请求),对需训练或统计的方法≈1 场景×72 重复。
3. **占位符污染候选**:SASRec top-K 候选含 32.95% `Product_<id>` 占位符,经 agent 转述进对话
   (GLM 推理原文实证)→ 幻觉检测在零语义商品上 ill-posed,且 base agent 面对 unknown 更易自发编造。
4. **规模**:72 case、单拓扑,报不了紧置信区间。

上游 2026-07-16 另点名方向:**agent 故障 × 传统 infra 故障"结合"(跨层双根)** —— 全场独有卖点,
v1 是纯 agent。injector-进-K8S spike 已证跨层技术前提就绪(变体镜像/注入/落盘/出网全通)。

**目标**:一次重设计,同时消解四缺陷(Tier 1 纯 agent 扩展)+ 立起跨层能力(Tier 2)。

---

## 分层(基于难度侦查,不混采)

侦查结论把工作清晰劈成两层——**Tier 1 全是低-中难度已验证曲柄;Tier 2 有两个 high 难度未知数**。
故建议**分两个里程碑**,Tier 1 先落地(数据集及格线),Tier 2 作独立 spike→采集(上游的跨层卖点)。

### Tier 1 —— 纯 agent 扩展 benchmark(P0,先做)

消解四缺陷,全部复用 v1 的 injector/runner/eval/QC,改的是**配置矩阵 + 载体多样化 + 词表过滤**。

**(A) 故障矩阵铺满 + 先验去退化**
承 TAXONOMY 的 MAS-FIRE intra-cognitive 子集。物理约束(injector 实证):hallucinate 只对 analyzer
(Synthesizer 走 tool_call 无自由文本终答);wrong_item_pick/format 只对 Synthesizer。要平衡先验,靠**副 LLM 类故障铺到全 agent**:

| kind | 机制 | 可注 agent | 现状 |
|---|---|---|---|
| hallucinate | 副 LLM 整段改写终答(带 subllm span) | Seq/UB/Product | ✅v1 |
| **context_drift** | **pre-call 篡改 messages 删/截断上游 agent 结论**(MAS-FIRE prompt-mod 机制) | Product/Synth(丢上游) | ★**照抄零件拼装**(见下"照抄映射");注入自研但定义/骨架/canary 信号/GT 全有出处 |
| **instruction_ignore** | 副 LLM 改写使其违反"中文/从候选选"约束 | Seq/UB/Product/Synth | 拟(副 LLM,照 hallucinate 模板新增分支) |
| wrong_item_pick | 确定性换 tool_call 推荐 ASIN 为哨兵 | Synth | ✅v1(哨兵改**不含"FAULT"字面**,消上界脚注 [b]) |
| format_violation | 确定性破坏 tool_call 结构 | Synth | ✅v1(4 subtype 收敛为 **1 combo 内轮换**,不再 4 独立 combo → Synth 先验降) |
| **overconfidence** | 确定性把 confidence 拉 0.99 与质量脱钩 | Synth | 拟(零成本确定性,TAXONOMY 候选表已列) |

铺开后每 agent 的 combo 数(目标≈均匀,消 62.5% 退化):
- Seq / UB / Product 各:hallucinate + context_drift + instruction_ignore = **3 kind/agent**
- Synth:context_drift + instruction_ignore + wrong_item_pick + format(1) + overconfidence = **5 kind**
先验从 62.5%→约 **Synth 5/(3×3+5)=35.7%**,四 agent 更接近均匀(理想 25%)。format 收敛 + 新 kind 是关键杠杆。

**★照抄映射(2026-07-19 侦查 third_party/reference,不重复造轮子)**
context_drift 注入逻辑需自研(无现成"顺序链丢弃上游结论"代码),但零件全有出处:
- **定义/命名** ← MAST taxonomy:1.4 Loss of Conversation History / 2.5 Ignored Other Agent's Input /
  2.4 Information Withholding(对标已发表分类学)。
- **注入骨架** ← chaosgraph `Fault` 抽象基类 + monkeypatch interceptor(我们 patch `_generate` 同款)。
- **确定性可检信号** ← agentdojo canary 探针:上游结论埋 canary token,检下游可见上下文是否保留
  (drift 生效则丢失)。**注意**:canary 查"下游 messages 是否含结论",性质与 hallucinate 的 needle
  (从最终产物文本检出)不同,EVAL_NOTES 须如实区分两种 content 信号。
- **注入生效判定 + GT** ← chaosgraph FaultEvent 结构化台账 + Who&When `mistake_agent/step/reason` schema
  (whowhen adapter 已在用同 schema)。
- 我们已有两机制的同源:hallucinate post-rewrite = mas-resilience `AutoInject.modify`;
  B 变体 pre-context = agentdojo prompt injection。

**(B) case 多样化(载体轮换)**
v1 全用固定 `PROBE_SEQ`(3 商品)。改:每 rep 轮换 `item_sequence`(不同用户历史)。
- 改造点(侦查 Q1):`ISM.probe(port)` 加 `seq/top_k` 入参;`run_combo`/`run_one_rep` 穿一组 per-rep 序列;
  journal 记录点(`agentfault_runner.py:643`)改记该 case 实际序列。约 3-4 处。
- **baseline 口径连带**:clean_baseline_rates.json 是"单序列基线率",多序列后须按序列分组或改口径 —— 这是设计连带,非代码难点(见开放问题)。

**(C) 真标题过滤(消占位符)—— 双侧(★2026-07-19 用户拍板:服务侧过滤获批)**
- **候选侧 = 服务内过滤(v2 pipeline 变更,用户批准的 services/** 例外)**:
  `services/recommendation_agent/agents/tools.py::get_sequence_recommendations` 过采
  (向 SASRec 请求 top_k×3)→ 用既有 `_load_title_cache()` 滤掉 `title==Product_<id>`/缺失
  → 截断回 top_k。**定性 = 合理产品行为**(真实推荐系统本就滤元数据残缺商品),非 hack;
  爆炸半径 = 仅 rec_agent(买家站推荐走 backend_api 不受影响);候选数不变、全部有语义;
  契约 `item_in_candidates` 用过滤后候选集,自洽。**管住三件事**:Datasheet+EVAL_NOTES 声明
  (过采×3+过滤,均匀作用全部 case)/ 单独 commit 版本界线(v1 已归档不受影响)/
  两个镜像(latest+agentfault)重 build。
- **历史侧 = 载体池过滤(纯本地)**:`/dataset/test_sequences` 取真实用户序列(天然在词表内),
  历史商品对 `electronics.item` 查真标题,全真才入池 → `assets/carrier_pool.json`。
  **不再需要每序列预跑 SASRec 筛候选**(服务侧已根治)→ P0-0 大幅缩水,不碰 9.2GB pickle。
- 净效果:v1 的 L1-L4 产出率赌局作废;历史+候选双侧零占位符。

**(D) 规模**
每 kind×agent combo × R rep + normal × R。矩阵约 **14 faulted combo**(Seq/UB/Product 各 3 + Synth 5)。
R=12 → 168 faulted + 12 normal = **180 case**;R=16 → 224+16 = **240**;R=20 → 280+20 = **300**。
载体多样化让 rep 不再是纯重复(每 rep 不同序列)→ R 同时买到"重复稳定性"与"场景多样性"。

**Tier 1 复用面(侦查确认)**:CHECKSUM 守卫免疫多样化;normal 分支透明;QC 检查项(trace/span/isolation/faulted-rep)与序列无关;eval 三套 harness(tierA/whowhen/infra_neg)+ 双 judge 全复用,改路径参数即可。

### Tier 2 —— 跨层双根(agent×infra,上游点名,独立里程碑)

同窗叠加 rec_agent 语义故障(env 开关)+ Chaos Mesh infra 故障(如 catalog CPU / net delay)。

**已就绪(侦查 + spike)**:
- infra CRD 注入原语 `apply_chaos_crd`(L1353)+ `inject_stress_catalog`(L1576)等,现成。
- 同窗叠加接缝 `mid_actions`(`capture_stage` L2507)+ 现成跨类型双根闭包(DK15/DK11/DK17)可照抄。
- env-on-pod 注入先例 `inject_runtime_exception`(L1495,`kubectl set env`+rollout+窗对齐)——agent 注入镜像它。
- rec-agent 已在 `FIXED_25` 采集集(`--full-telemetry` 下三模态免费捕获)。
- GT 结构 `_build_fault_profile`(L8064)可扩:已有 `app_env_hook`/`crd:None` 先例,加 `root_cause_agents` + `cross_layer` composition 取值。
- 变体镜像 `recweb-rec-agent:agentfault`(spike 已 build 验通,loader 装配已解决)。

**两个 high 难度未知数(必须先 spike 再采)**:
1. **窗对齐**:agent 语义故障无 HTTP 500/slow 翻转,`_wait_carrier_code/slow`(L1431/1459)对不上窗。
   候选解:driver 探针发 /recommend + 检测 needle/台账时戳作注入 marker;或 loader 常 armed、只靠 ledger 时戳定窗。
2. **ledger 跨 pod 回收**:agentfault GT 是 pod 内进程写的 per-request ledger(trace_id 键)。
   候选解:挂共享卷(hostPath/emptyDir)+ 采后 `kubectl cp`;或 pod 内 ledger 走 stdout 由 `k_logs` 收。
   (spike 已证台账落盘 + span 落盘在 pod 内工作,只差回收管道。)

**Tier 2 规模**:小而精,agent(3-4 类)× infra(2-3 类,catalog CPU / net delay / pod fail)× R(5-8)
≈ 40-80 跨层双根 case + 对照的单层 case。作"独有卖点"演示集,不追求大规模。

---

## 分阶段计划(拍板后拆工单)

- **P0-0 资产**:离线求 真标题∩词表 交集 → ASIN 白名单文件(碰一次 9.2GB pickle 或起 SASRec 枚举)。
- **P0-1 injector**:新增 context_drift(挂 B 变体 env)/ instruction_ignore(副 LLM 新分支)/ overconfidence(确定性);
  wrong_item_pick 哨兵去 "FAULT" 字面;各出 LIVE 冒烟 + **每类过延迟伪影关(corrected 通道 vs normal 抖动带)**。
- **P0-2 runner**:build_combos 扩矩阵;probe 加序列参 + per-rep 轮换;journal 记序列;format 4 subtype 收敛 1 combo。
- **P0-3 重采**:R=? nohup 亲驱 + Monitor + per-combo QC/CHECKSUM(老套路)。
- **P0-4 eval**:三套 harness + 双 judge 重跑(改路径);更新 SUMMARY/EVAL_NOTES/BASELINE_RESULTS + Datasheet 占位符声明。
- **P1(Tier 2)**:跨层 spike(窗对齐 + ledger 回收两未知数)→ 通了再采 40-80 跨层 case。

## 铁律(全程,不变)
不改 `services/**`、`third_party/**`;Chaos Mesh/K8S runner(`scripts/chaos/ctk/*`)Tier 2 才碰且只加不改核心;
不 pip 装主 env;不升 langchain;GT 由台账给;长采集主循环 nohup 亲驱;items/inventory 只读 CHECKSUM 零漂移;
新故障类每类必过延迟伪影关(v1 血泪:语义故障注入固有扰时间通道)。

## 开放问题(拍板时一并定)
- **多序列下的 clean baseline 口径**:每序列各采 K' 干净基线?还是接受"注入 vs 干净不再严格同输入"、改用 per-agent 分布检验?
- **instruction_ignore 的确定性信号**:hallucinate 有 needle、format 有契约、wrongpick 有哨兵;
  instruction_ignore/context_drift 的确定性可检信号是什么(否则只能靠 LLM-judge,削弱"content 轨完美分离"卖点)?
- **占位符是排除还是做受控 stratum**:全排除(干净)vs 留一个占位符 stratum 测方法在零语义下的退化曲线(多一个卖点,多一分复杂)。

---

## ★已拍板(2026-07-19,用户)
1. **范围 = 先 Tier 1**(跨层 Tier 2 作独立里程碑,先 spike 窗对齐/ledger 回收再采)。
2. **规模 R=16 → 约 240 case**(14 faulted combo×16 + normal×16)。
3. **新增 kind = 真 context_drift**(★2026-07-19 更新:非 B 变体挂 env。侦查推翻"无确定性信号"顾虑——
   agentdojo canary 可抄。走 MAST 定义 + canary 信号 + chaosgraph 骨架,照抄不造轮子。
   instruction_ignore/overconfidence 留待下版)。先做实现 spike 验注入生效 + canary 传递,再入矩阵。
4. **占位符 = 全排除**,实现 = 服务侧候选过滤(用户批准的 services/** 例外)+ 载体池历史过滤(见 (C))。
5. 执行纪律:大规模 API 调用(全量采集/judge 复跑)前须先告知用户;小规模冒烟自行执行。
6. 多序列 clean baseline 口径(开放问题落定):normal combo 与 faulted 同池轮换序列;
   MAST judge 率基线在轮换后的 normal 集上重算,EVAL_NOTES 声明口径变更(不再"单序列同输入")。
