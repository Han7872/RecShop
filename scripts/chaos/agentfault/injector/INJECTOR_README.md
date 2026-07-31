# agentfault 注入器 —— 设计 / 依据 / caveats / 冒烟结果

> 状态:**LIVE 冒烟 3 组合全 PASS**(2026-07-16)。这是 agent 语义故障注入器的可用地基。
> 铁律:不改 `services/**`,只运行时 monkey-patch;新文件只落 `scripts/chaos/agentfault/`。

## 1. 它做什么
在真实 rec_agent(`services/recommendation_agent`,LangGraph 4-agent 顺序链)上,**运行时**
按 agent 选择性注入语义故障,并让注入内容在 openinference 内容层可见 + 落 GT 溯源台账。
**不改服务码**,靠 throwaway venv 里的 sitecustomize monkey-patch。

三类故障(v1,LIVE 冒烟全 PASS;上游点名"格式+幻觉"两类均覆盖):
| kind | 目标 agent | 机制 | 参考(已发表工作) |
|---|---|---|---|
| `hallucinate` | 任一 analyzer(Seq/UB/Product) | 副 LLM 整段改写**最终自由文本答案**为"流畅但事实错误" | mas-resilience/AutoInject.modify(ICML2025)= MAS-FIRE §3.2 response-rewriting 前作 |
| `wrong_item_pick` | Recommendation_Synthesizer | 确定性把 tool_call 的 `recommended_product` 换成哨兵错 ASIN | chaosgraph ToolMalformedFault 静态 payload 姿势 |
| `format_violation` | Recommendation_Synthesizer | 确定性破坏 tool_call 结构(4 子类型:missing_field/type_violation/empty_required/malformed_json) | chaosgraph ToolMalformedFault + 消费方照 `llm_rerank_service/utils/validator.py` 四查 |

**★消费方(你强调的"被对应方法消费"):** `contract_validator.py` 的 `validate_synthesizer_contract`
= format_violation **与** wrong_item_pick 的**共同**消费方(照 llm_rerank validator 四查:
JSON 可解析/必需字段/字段类型/item∈候选)。故障表现为**哪项 check 失败**不同 → 采集据此标 GT。
format_violation 的价值点:rec_agent `.get(默认值)` 愈合响应(confidence 缺失→响应显示 0.5),
**黑盒看正常但契约层从 tool_call span 抓到违约** = infra-RCA 盲、契约层可见的活证。

## 2. 关键设计依据(读 workflow.py 源码后钉死)
- **3 个 analyzer agent(Seq/UB/Product)共享 1 个 `ChatOpenAI(temp=0.7)`;Synthesizer 独立 `ChatOpenAI(temp=0)` + 强制 tool_choice**
  → 在**类级** patch `ChatOpenAI._generate` 一次即拦截全部。
- **agent 归属靠系统提示签名**(每 agent 的 `[ROLE]` 短语唯一),不能靠 LLM 实例(共享)。
  见 `AGENT_SIGNATURES`。
- **两种输出形状 → 两种机制,天然规避"改写撕碎结构化输出"的坑**(黑板 Reviewer 深审警告):
  - analyzer 终答 = 自由文本(该轮 `tool_calls` 为空)→ 副 LLM 整段改 prose,**不按句/逗号切**;
  - ReAct 中间的 tool-call 轮(`tool_calls` 非空)**绝不碰**;
  - Synthesizer 的结构化 tool_call → 只换 `recommended_product` 字段值,**不走副 LLM**,确定性、可复现。

## 3. ★关键坑与修复:analyzer 走 `_stream` 不走 `_generate`
第一次冒烟 FAIL(ledger 空、零注入)。调试发现:**只有 Synthesizer 命中 `_generate`**。
根因(读 langchain 源码确认):AgentExecutor 经 `.stream()` 驱动 analyzer LLM → `_stream`
(token 生成器,`_should_stream()` 见有 streaming callback 即走流式);Synthesizer 的裸
`prompt | model.bind_tools | parse` 链走 `.invoke()` → `_generate`。
**修复**:patch `ChatOpenAI._should_stream` 恒返 `False` → 所有 agent LLM 调用回退到
`invoke()` → `_generate`,注入点统一。openinference 内容捕获经 `on_llm_end`(流/非流都触发),
不受影响。冒烟证实:修复后 4 agent 全部命中 `_generate`,每个 analyzer 2 次调用
(tool-call 轮 skip + 终答轮注入),Product_Analyzer 正确注入、其余不动。

## 4. 内容层可见性(on-thesis 卖点)
调用栈 = `generate → _generate(patched:orig 后注入) → on_llm_end(见注入后 result)`。
∴ openinference 捕到的是**注入后**内容。冒烟(加固后)证实:hallucinate 的**注入指纹**
(分叉 needle,取改写文本与原文分叉处的一段,**不含两文共有的开头 boilerplate**)出现在
**该 target agent 子树** 的 ChatOpenAI output content span(agent-scoped,非全局);
wrong_item_pick 的哨兵 ASIN 出现在 Synthesizer tool_call 的 `recommended_product` span。
→ **注入的语义内容在内容层可机检地看得见**,而传统 infra 指标(延迟/状态/span 数)对它盲。

> 措辞校准(承 Reviewer 终审):本 harness **自动核**的是"链存活 + 注入被记 + **注入指纹**
> 出现在 **target agent** 的 content span + 无非 target agent 被注入"。改写内容**确为事实错**
> 由人工抽检佐证(见 §5b 引文),harness 不自动判"事实真假"(那需语义 judge,= DAG (4) 阶段)。

## 5. 判定信号(加固后,修 Reviewer 终审的空证/隔离缺口)
**hallucinate PASS = 四签全过:**
1. **台账**(status=='injected'):注入器落 GT 一条(agent/kind/trace_id/orig→inj + **divergent_needle**);
   `inject_failed`(副 LLM 异常/空/退化/**拒答**)显式落盘、**不算注入**;
2. **链存活**:HTTP 200 + success(analyzer 幻觉优雅降级不炸链);
3. **target-scoped 内容层可见**:`divergent_needle`(分叉指纹)出现在 **target agent 子树**的
   ChatOpenAI output content(靠 LangChain Run 树祖先的 LangGraph 节点名归属,非全局扫描);
4. **隔离负检**:除 target 外无其它 agent 落 injected 台账。

**wrong_item_pick PASS = 四签全过:** 台账 + 响应 `recommended_product==哨兵` + tool_call span
ASIN 被换 + 隔离负检。
+ **CHECKSUM(items/inventory)前后不变** = 不污染持久栈(rec_agent 无 DB 写,恒等)。

### 5a. 冒烟结果(`(v1)_smoke/injector/*_verdict.json`,加固版)
| 组合 | verdict | 关键证据 |
|---|---|---|
| hallu_product(Product_Analyzer) | PASS | divergent_needle=`科幻/奇幻类书籍…`(**不在原文**)、target span 命中、isolation []、链 200 |
| hallu_userbeh(User_Behavior_Analyzer) | PASS | divergent_needle=`哲学类…`(**不在原文**)、target span 命中、isolation [] |
| wrongpick_synth(Synthesizer) | PASS | B0051VVOB2→B00000FAULT 三层齐换、isolation []、CHECKSUM 零漂移 |

### 5b. 人工抽检佐证(注入内容确为流畅但事实错,非 harness 自动判)
User_Behavior_Analyzer 注入 case 手检:注入器臆造"**明显偏好迪士尼品牌**"、"**历史商品均为高端
定价**...倾向**收藏版或绝版**"等假事实(该 3 商品无一迪士尼/无高端),而**同 trace 其余 3 agent
输出保持干净**(靠隔离负检自动保证不被注入,内容为真则由此抽检佐证)。= MAS-FIRE 静默语义
故障签名:流畅+结构正常+事实错+不炸链。

## 6. Known caveats(如实记,进数据集 threats-to-validity)
- **强制非流式(时序 threat,非纯 cosmetic)**:本测试床把临时实例的 LLM 调用改为非流式
  (收口 `_generate`),类级永久生效于每个 case。文本**内容**同质(非流式只是不逐 token 推,
  完成体一致),但**时序特征通道**(端到端/逐 agent 延迟形状)与生产流式路径有别。
  **∵ RF Track A 黑盒 baseline 可能吃延迟特征 → clean 与 faulted 必须在同一非流式 regime 下采**
  (差异抵消),或核实 baseline 不用延迟特征。**勿**只把它当"仅测试床零影响"(Reviewer 终审纠)。
- **★★注入延迟伪影 + 埋细修复(2026-07-17,eval 实测 + 用户思路)**:hallucinate 副 LLM 改写多跑一次
  LLM,耗时被计入 `agent.<Name>` 边界 span → 注入 analyzer span 慢 ~1.3-2.3× → 平凡延迟启发式 raw Hit@1=1.00
  (纯伪影)。**根因是 LLM 语义故障注入固有**(时间∝token+额外调用),A(改写)/B(上下文注入,输出变长)
  皆有,掐表校正死在嵌套。**正解=埋细**:`_hallucinate_text` 给副 LLM 调用包专属 span `agentfault.subllm_rewrite`
  (落 SPAN_FILE)→ runner span 减 span 得 `span_<A>_duration_corrected_ms`。效果:延迟启发式 RAW 1.00→
  CORRECTED 0.208(≈随机)。辅助:`_HALLU_SYSTEM` 加**等长硬约束**(±10%,不扩写)控生成 token 大头。
  **eval infra 轨必须用 corrected 列**。详见数据集 EVAL_NOTES §4a + memory [[recweb2-agentfault-latency-artifact]]。
- **B 变体(上下文注入,env-gated 默认关)**:`AGENTFAULT_HALLU_MODE=context` = MAS-FIRE prompt-modification
  机制,pre-call 塞误导事实让 agent 自己编。冒烟证:agent 听(markers 落输出),但**不比 A 干净**(输出变长
  2-3×→时长仍 1.5×)→ 弃用为主注入,保留作记录在案的备选/未来第二变体。默认 `rewrite`(A)不受影响。
- **同源 judge 偏置**:hallucinate 用 DeepSeek 副 LLM 改写;若 judge 也用 DeepSeek,继承同款盲点
  → 换模型族或记 threat(黑板 Reviewer 纠错⑤)。
- **hallucinate 非确定性**(副 LLM 每次改写不同);wrong_item_pick 确定性可复现。
  采集时 hallucinate 靠台账固化每 case 实际注入内容(GT 溯源),不依赖可复现性。
- **副 LLM 拒答**:DeepSeek 内容策略可能拒改 → 注入器判 `_looks_like_refusal` → 落
  `status=inject_failed`、**不改宿主内容、不当 hallucinate case**。采集侧必须据此丢弃该 case。
- **副 LLM 成本/时延**:每次 1-3s 串行,单 case 拉长;wrong_item_pick 无此成本。
- **needle 判据**:早期用前 240 字 excerpt 前缀作 needle → 命中的是两文共有 boilerplate = 空证
  (Reviewer 高危发现)。**已修**:台账存 `divergent_needle`(改写与原文分叉处的段),
  smoke 在 **target agent 子树** span 里匹配它,保证证到的是**注入内容**且在**对的 agent**。

## 6b. ★采集 runner 硬约束(把 Reviewer 终审的教训钉进未来实现)
注入器的所有失败路径都是**静默 no-op**(异常/空/未匹配签名/无终答轮 → 原样返回干净宿主内容,
这是"永不阻断宿主"的正确安全设计)。**∴ 采集 runner 给 case 打 faulted 标签时,唯一合法依据 =
该 case 的 trace_id 有一条 `status==injected` 的新台账记录**,**绝不**能只按 env 意图
(`AGENTFAULT_KIND_*` 设了)就标 faulted —— 否则副 LLM 一次拒答/超时/未命中,就产出**内容干净却标
faulted** 的脏 case(数据集最坏失效模式 false-faulted)。这条是采集阶段(DAG (5))的验收铁律。

## 7. 文件
- `agentfault_injector.py` — 注入器核心(patch + 三类故障 hallucinate/wrong_item_pick/format_violation + 台账)
- `contract_validator.py` — Synthesizer 契约校验器(format_violation + wrong_item_pick 的**消费方**,照 llm_rerank validator 四查)
- `loader/sitecustomize.py` — arm openinference(内容捕获)+ install 注入器(env-gated)
- `injector_smoke.py` — LIVE 冒烟驱动(主循环亲驱,多路判定 + CHECKSUM)
- 复用 Phase1:`../phase1_bootstrap.sh`(throwaway venv)、`../phase1_loader`(openinference 姿势原型)

## 8. 下一步(DAG)
(0) 薄 taxonomy(已附 `TAXONOMY.md`)→ (3) 端到端冒烟✓(本次即是)→ (4) judge 原型 →
(5) 双轨采集(新 runner 复用 agentchaos_runner 60-70% 模式)→ (6) eval(RF Track A vs 内容轨)。
