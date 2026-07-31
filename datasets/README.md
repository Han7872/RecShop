# datasets

Agent semantic fault dataset for microservice RCA research.

## What's here

| Directory | Cases | Description |
|---|---|---|
| `agentfault_k8s/` | 108 (96 faulted + 12 zero-injection controls) | B-tier dataset: agent semantic faults injected on a full 25-microservice K8S cluster |
| `agentfault_k8s_whowhen/` | 96 | Who&When-format delivery view (drop-in for Who&When / A2P) |

## Quick links

- `agentfault_k8s/SUMMARY.md` — what it is, how it was collected
- `agentfault_k8s/EVAL_NOTES.md` — evaluation protocol + key limitations
- `agentfault_k8s/dataset_agentfault.csv` — one row per case
- `agentfault_k8s/RESULTS_WHENWHEN.md` — Who&When results (DeepSeek + GLM cross-family)
- `agentfault_k8s/BASELINE_RESULTS.md` — Tier-A baselines (contract oracle, trivial, RF)
- `REGISTRY.json` — machine-readable catalog
