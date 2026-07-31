# traditional_k8s — 传统基础设施故障数据集（K8S）

> **完整数据集（全量三模态遥测 + GT + 评测结果，~16 GB）托管在 Google Drive：**
> 🔗 **`https://drive.google.com/<TODO_REPLACE_WITH_SHARED_LINK>`**
> 下载后解压到本目录（`datasets/traditional_k8s/`）即可使用。

## 这是什么

在真实 25 微服务 K8S 集群（docker-desktop 单节点）上，用 **Chaos Mesh** 注入传统基础设施故障，
同步采集三模态遥测（OpenTelemetry trace + metric + log），每个 case 附机器可读真值（GT）。
用于微服务根因分析（RCA）方法的评测与对比。

## 规模（255 case，按注入 arity 分档）

| 档 | case 数 | 注入 arity |
|---|---|---|
| single | 130 | 1 个故障点 |
| dual | 100 | 2 个故障点组合 |
| triple | 25 | 3 个故障点组合 |
| **合计** | **255** | |

> 评测口径按**去重根因服务数 G** 分档（G=1/2/3）；同一服务的多点注入会坍缩为更低 G。
> 详见完整数据集内的 `DATASHEET.md`。

## 故障类型（Chaos Mesh 原语）

CPU 负载、Pod 故障、网络（延迟 / 丢包）、配置超时、DB 竞争等。完整机制清单与
single/dual/triple 组合设计见完整数据集内的 `FAULT_DESIGN.md`。

## 每个 case 包含

- `raw/`：三模态遥测 —— trace（含跨服务调用树）+ metric 时序 + log
- `eval/`：特征视图 + 机读 GT（根因服务 / 容器 / 节点）
- 随树 `per_case_scores_255.csv`：255 case × 多方法 × 多特征集的评测打分

## 评测结果（随数据集提供）

`per_case_scores_255.csv` 列含 BARO / RCD 等已发表方法在 single/dual/triple 三档的
Hit@k、Recall@R、FullHit@k、NDCG@k、MRR。列 schema：

```
case_id, type, arity, n_distinct_roots, gt_roots, method, feature_set,
top1, top5, hit@1, hit@3, hit@5, recall@R,
fullhit@1, fullhit@3, fullhit@5, ndcg@1, ndcg@3, ndcg@5, avg@1, avg@3, avg@5, mrr
```

## 采集与复现

采集 harness（runner + Chaos Mesh manifest + K8S 部署配置）随本仓 `scripts/chaos/ctk/` 提供；
采集入口与一键脚本见 `scripts/chaos/ctk/` 目录说明。
