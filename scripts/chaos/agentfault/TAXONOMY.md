# agentfault 故障类目表(薄 taxonomy)—— 贴 MAS-FIRE,可辩护

> 目的:每个注入的故障类都能指回一篇**已发表** taxonomy(MAS-FIRE, arxiv 2602.19843,
> 2026-02),避免"自己随手想的类"审稿人问不出处。**求稳、贴已有工作**,不 overclaim 方法新颖。
> 定位:数据集贡献(把 MAS-FIRE 式语义故障搬进真实 25-微服务 OTel 栈 + 内容/黑盒双轨),
> **非**故障 taxonomy 发明。MAS-FIRE 无开源码,注入实现自研(见 injector/INJECTOR_README.md)。

## 本项目故障范围:intra-agent 认知层(agent 内),非 inter-agent 协调层
MAS-FIRE 15 类分两大族:intra-agent 认知(4 类)+ inter-agent 协调(3 类)。
rec_agent 是**确定性顺序链**(无动态路由/无 agent 间协商),协调层故障不适用 →
只取 **intra-agent 认知子集**。这与 MAST 14-mode(偏 MAS 协调)刻意区分(黑板 Reviewer 纠错③)。

## v1 已实装(LIVE 冒烟 PASS)—— 上游点名"格式+幻觉"两类均已通
| 本项目 kind | MAS-FIRE 认知类(映射) | 注入机制 | 目标 agent | GT 标签 | 对应消费方 / 可检信号 |
|---|---|---|---|---|---|
| `hallucinate` | 知识幻觉 / 事实错误(intra-cognitive) | 副 LLM 整段改写终答为流畅但事实错 | Seq/UB/Product | agent=注入者, kind, divergent_needle | 内容层:分叉指纹入 target agent span;黑盒:推荐质量降但链 200 |
| `wrong_item_pick` | 决策错误 / 目标偏移(intra-cognitive) | 确定性换 tool_call 的 recommended_product | Synthesizer | agent=Synth, orig→哨兵 | 契约:`item_in_candidates` 查失败;响应+span+台账三层齐换 |
| `format_violation` | 输出格式违背(intra-cognitive) | 确定性破坏 tool_call 结构(4 子类型) | Synthesizer | agent=Synth, violation.subtype | **契约校验器**(contract_validator)对应检失败;黑盒常被 `.get` 默认值愈合看不见 |

**format_violation 4 子类型 → 契约校验器对应失败项**(offline+live 双验):
| subtype | 破坏 | 契约 check 失败项 | 成本 |
|---|---|---|---|
| `missing_field`(默认) | 删 confidence 字段 | `required_fields` | 零 |
| `type_violation` | confidence 改中文串 | `field_types` | 零 |
| `empty_required` | recommended_product 置空 | `field_types` | 零 |
| `malformed_json` | 截断 raw arguments JSON 串 | `json_parsable` | 零 |

> **★"被对应方法消费"的实证**(你强调的验收点):format_violation 的消费方 =
> `injector/contract_validator.py` 的 `validate_synthesizer_contract`(照
> `services/llm_rerank_service/utils/validator.py` 四查结构写)。**同一个校验器同时消费
> wrong_item_pick**(candidates 给定时 `item_in_candidates` 查失败)——一个契约,故障表现
> 为**哪项 check 失败**不同 → 采集/评测据此标 GT、算契约有效率。format_violation 的杀手锏:
> rec_agent 用 `.get(默认值)` 愈合响应(实测 confidence 缺失→响应显示 0.5),**黑盒看正常
> 但契约层从 tool_call span 抓到违约** = "infra-RCA 对 agent 语义盲、契约/内容层看得见"活证。

## v2 候选(设计,未实装 —— 采集前按需选,优先零成本/确定性)
| 候选 kind | MAS-FIRE 认知类 | 注入机制(拟) | 目标 | 成本 |
|---|---|---|---|---|
| `instruction_ignore` | 指令遵循失败 | 副 LLM 改写终答使其忽略"必须中文/必须从候选选"约束 | analyzer/Synth | 副 LLM |
| `context_drift` | 上下文遗忘 | 副 LLM 改写终答使其丢弃上游 agent 关键结论 | Product/Synth | 副 LLM |
| `overconfidence` | 置信度失真 | 确定性把 confidence 拉到 0.99(与实际质量脱钩) | Synthesizer | 零(确定性) |

> 上游 2026-07-16 点名 Bill 在做**"格式 + 幻觉"两类**(见 [[single-spread-55-delivery]] 定调)。
> → **hallucinate=幻觉、format_violation=格式,二者 v1 均已 LIVE 冒烟 PASS**,是采集主力。

## 采集设计原则(承 agentchaos 经验 + 黑板铁律)
- **每 case 台账固化实际注入内容**(hallucinate 非确定性,GT 靠台账不靠可复现);
- **双轨对齐**靠 `trace_id + agent 名 + 时间窗`(两套并行 OTel 树,非 OTel parent chain);
- **infra 轨 baseline = RF Track A**(agentchaos 同款监督式分类器),**非** BARO/RCD/Eadro
  (范畴错置,黑板 Reviewer 硬错①:三法根因实体=服务,agent 故障根=in-process agent,候选空间无 agent 通道);
- **CHECKSUM(items/inventory)** 每 case 零漂移守卫(rec_agent 无 DB 写,恒等)。

相关:[[recweb2-agent-fault-novelty]](证伪+四坑)· [[recweb2-methods-baro-rcd]](baseline 口径)·
[[single-spread-55-delivery]](上游定调:纯数据集/方法下一篇、agent 先纯后叠加)。
