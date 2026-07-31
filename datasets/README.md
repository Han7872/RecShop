# datasets

[English](README.md) | [中文版](README_CN.md)

Microservice RCA datasets: agent semantic faults + traditional infrastructure faults.

## What's here

| Directory | Cases | Description |
|---|---|---|
| `agentfault_k8s/` | 108 (96 faulted + 12 zero-injection controls) | Agent semantic faults injected on a full 25-microservice K8S cluster |
| `traditional_k8s/` | 255 (single 130 / dual 100 / triple 25) | Traditional infra faults (Chaos Mesh). **Full telemetry hosted on Google Drive** — see `traditional_k8s/README.md` |

## Quick links

- `agentfault_k8s/SUMMARY.md` — what it is, how it was collected
- `agentfault_k8s/EVAL_NOTES.md` — evaluation protocol + key limitations
- `agentfault_k8s/dataset_agentfault.csv` — one row per case
- `agentfault_k8s/RESULTS_WHENWHEN.md` — Who&When results (DeepSeek + GLM cross-family)
- `agentfault_k8s/BASELINE_RESULTS.md` — Tier-A baselines (contract oracle, trivial, RF)
- `traditional_k8s/README.md` — traditional dataset overview + Google Drive download link
- `REGISTRY.json` — machine-readable catalog
