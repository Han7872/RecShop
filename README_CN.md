# RecShop — 微服务 RCA 数据集平台

[English](README.md) | [中文版](README_CN.md)

![License](https://img.shields.io/badge/license-MIT%20code%20%7C%20CC--BY--4.0%20data-blue) ![Dataset](https://img.shields.io/badge/dataset-351%20cases-orange)

一个真实的 25 微服务电商推荐系统（SASRec + 4-Agent LangGraph + DeepSeek LLM），带故障注入、三模态可观测性（OpenTelemetry）与机器可校验真值——用于根因分析（RCA）研究。

两条互补故障线：**传统基础设施故障**（255 case，根因 = 服务）与 **Agent 语义故障**（96 faulted case，根因 = LLM agent）。两者评测口径不同，分数不可混报。

## 仓里有什么

| 组件 | 说明 |
|---|---|
| `services/` | 25 个微服务（Flask/FastAPI）——可运行的应用本体 |
| `datasets/agentfault_k8s/` | Agent 语义故障数据集（96 faulted case，4 个族）+ 预计算评测结果 |
| `datasets/traditional_k8s/` | 传统基础设施故障数据集（255 case；全量遥测托管在 Google Drive） |
| `scripts/chaos/` | 故障注入 + 采集 + 评测工具链 |
| `docker/` `ops/` `k8s/` | Docker / OTel 栈 / K8S 部署配置 |
| `scripts/database_schema.sql` | 数据库 schema（幂等，`CREATE TABLE IF NOT EXISTS`） |

预计算评测结果随 `datasets/agentfault_k8s/` 提供（`BASELINE_RESULTS.md`、`RESULTS_WHENWHEN.md`、`RESULTS_CONTENT_CTXDRIFT.md`、`infra_negatives/`），无需安装任何评测依赖即可阅读。

## 快速开始

**前置**：Python 3.10+（`requirements.txt`）、MySQL 8.0+，以及大模型资产（体积过大，需自备）：`services/sasrec_api/standard_cache.pkl`（~9.2 GB）、`SASRec-*.pth`（~260 MB）、`services/recommendation_agent/electronics.inter`（~2.4 GB）、`shared/data/electronics.item`（~1.2 GB）。

```bash
mysql -u root -p < scripts/build_database.sql          # 初始化数据库
NACOS_ENABLED=false python start_all.py                 # 启动（离线模式，不起 Nacos）
python start_all.py --stop                              # 停止
```

访问 —— 买家端：http://localhost:3000 · 商家端：`/merchant` · 管理端：`/admin` · Jaeger：http://localhost:16686 · Grafana：http://localhost:3001

## 数据集

**Agent 语义故障**（`datasets/agentfault_k8s/`）——96 faulted case，4 个族，每 case 附机器可校验真值。详见 `SUMMARY.md`（是什么 / 怎么采）+ `EVAL_NOTES.md`（评测协议）。

| 故障族 | case 数 | 机制 |
|---|---|---|
| `hallucinate` | 36 | 副 LLM 把终答改写成流畅但事实错误 |
| `context_drift` | 36 | 删掉送进下游 agent 的上游消息 |
| `wrong_item_pick` | 12 | 推荐的 ASIN 换成哨兵值 |
| `format_violation` | 12 | 4 个子类型（缺失 / 类型错 / 空 / 畸形） |

**传统基础设施故障**（`datasets/traditional_k8s/`）——255 case（single 130 / dual 100 / triple 25），Chaos Mesh 注入。全量遥测（~16 GB）托管在 Google Drive，见 `traditional_k8s/README.md`。

## 数据采集（一键脚本）

故障注入 + 采集工具链在 `scripts/chaos/` 下。一键采集脚本（每个都是自带前置检查、逐 case 闸、可断点续采的 orchestrator）：

| 脚本 | 采集内容 | 输出树 |
|---|---|---|
| `scripts/chaos/ctk/collect-{single,dual,triple}-dense.sh` | single 40 / dual 80 / triple 20（v19 密度） | `datasets/k8s_pilot/{single,dual,triple}_dense/` |
| `scripts/chaos/ctk/collect-single-spread.sh` | single 55（pod 崩 & 服务 CPU，6 个服务） | `datasets/k8s_pilot/single_spread/` |
| `scripts/chaos/ctk/collect-single-recagent.sh` | single 15（rec-agent，附带 agent-span 阴性采集） | `datasets/k8s_pilot/single_recagent/` |
| `scripts/chaos/ctk/collect-g2ext.sh` | G2ext 批次——双 25 + 三 20（全异服务多根） | `datasets/k8s_pilot/{dual,triple}_ext/` |
| `scripts/chaos/agentfault/run_collect_agentfault.sh` | Agent 语义故障——9 combo × 12 rep（108） | `datasets/agentfault_k8s/` |

> `collect-{single,dual,triple}.sh`（不带 `-dense`）是 v18 前身——仅为溯源保留，**勿照跑**（请用 `-dense` 版）。

> 重新采集需要完整 25 服务 K8S 栈 + Chaos Mesh + 大模型资产都在位。仓内 `datasets/` 是开箱即用的产物；采集脚本仅供复现 / 扩充。

## 评测

```bash
# Agent 故障一键评测（offline 步骤免费；--with-judge 需 DeepSeek API key）：
bash scripts/chaos/agentfault/run_eval_agentfault.sh --dataset-dir datasets/agentfault_k8s
```

BARO / RCD 需要 `third_party/RCAEval` + 打过补丁的 `causallearn==0.1.2.3`（见 `scripts/chaos/ctk/m9_score.py` 头注）；缺失时这两步跳过，其余步骤照常。

## 架构

```
shop_web:3000 (BFF / 三端 UI)
├── 推荐链：backend_api:5000 → sasrec_api:8200
│          recommendation_agent:5001 → sasrec + DeepSeek
│          llm_rerank_service:5002 → DeepSeek
├── 交易链：checkout:5011 (扇出) → cart/pricing/inventory
│          order:5010 · payment:5012 · shipping:5016 → notification:5021
├── 内容链：catalog:5005 (高 fan-in) · search:5017 · review:5003/5018
├── 资料链：user:5004 · address:5007 · merchant:5019 · announcement:5009
└── 埋点链：interaction:5020 · admin_audit:5022 · ai_memory:5008
共享：MySQL shopify2 · Nacos (可选) · DeepSeek API · OTel collector
```

## 许可与引用

- 源代码：MIT · 数据集（`datasets/`）：CC-BY-4.0 · 第三方 vendored 库：遵循各自原始许可
- 详见 `LICENSE.md`；引用见 `CITATION.cff`。
