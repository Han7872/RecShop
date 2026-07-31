# datasets

[English](README.md) | [中文版](README_CN.md)

微服务 RCA 数据集：Agent 语义故障 + 传统基础设施故障。

## 目录内容

| 目录 | case 数 | 说明 |
|---|---|---|
| `agentfault_k8s/` | 108（96 faulted + 12 零注入对照） | 在完整 25 微服务 K8S 集群上注入的 Agent 语义故障 |
| `traditional_k8s/` | 255（single 130 / dual 100 / triple 25） | 传统基础设施故障（Chaos Mesh）。**全量遥测托管在 Google Drive** —— 见 `traditional_k8s/README.md` |

## 快速链接

- `agentfault_k8s/SUMMARY.md` —— 是什么、怎么采的
- `agentfault_k8s/EVAL_NOTES.md` —— 评测协议 + 关键局限
- `agentfault_k8s/dataset_agentfault.csv` —— 每 case 一行
- `agentfault_k8s/RESULTS_WHENWHEN.md` —— Who&When 结果（DeepSeek + GLM 跨族复核）
- `agentfault_k8s/BASELINE_RESULTS.md` —— Tier-A 基线（契约 oracle、trivial、RF）
- `traditional_k8s/README.md` —— 传统数据集概览 + Google Drive 下载链接
- `REGISTRY.json` —— 机器可读目录
