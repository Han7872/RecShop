# RecShop — Microservice RCA Dataset Platform

[English](README.md) | [中文版](README_CN.md)

![License](https://img.shields.io/badge/license-MIT%20code%20%7C%20CC--BY--4.0%20data-blue) ![Dataset](https://img.shields.io/badge/dataset-351%20cases-orange)

A real 25-microservice e-commerce recommendation system (SASRec + 4-Agent LangGraph + DeepSeek LLM) with fault injection, three-modal observability (OpenTelemetry), and machine-verifiable ground truth — for root cause analysis (RCA) research.

Two complementary fault lines: **traditional infrastructure faults** (255 cases, root cause = service) and **agent semantic faults** (96 faulted cases, root cause = LLM agent). Their scores use different units — do not mix.

## What's in this repo

| Component | Description |
|---|---|
| `services/` | 25 microservices (Flask/FastAPI) — the running application |
| `datasets/agentfault_k8s/` | Agent semantic fault dataset (96 faulted cases, 4 families) + pre-computed eval results |
| `datasets/traditional_k8s/` | Traditional infra fault dataset (255 cases; full telemetry on Google Drive) |
| `scripts/chaos/` | Fault injection + collection + evaluation toolkit |
| `docker/` `ops/` `k8s/` | Docker / OTel stack / K8S deployment configs |

Pre-computed evaluation results ship inside `datasets/agentfault_k8s/` (`BASELINE_RESULTS.md`, `RESULTS_WHENWHEN.md`, `RESULTS_CONTENT_CTXDRIFT.md`, `infra_negatives/`) — readable without installing any evaluation dependencies.

## Quick start

**Prerequisites**: Python 3.10+ (`requirements.txt`), MySQL 8.0+, and the large model assets (not included due to size): `services/sasrec_api/standard_cache.pkl` (~9.2 GB), `SASRec-*.pth` (~260 MB), `services/recommendation_agent/electronics.inter` (~2.4 GB), `shared/data/electronics.item` (~1.2 GB).

```bash
bash install.sh        # install Python deps (large model assets must be supplied — see below)
bash init_db.sh        # build DB + load items + demo seed  (set DB_PASSWORD env; needs electronics.item)
bash start.sh          # start all 25 services (offline mode by default)
bash start.sh --stop   # stop
# optional, full re-collection (overnight, K8S stack required; datasets/ ships pre-collected):
#   bash collect_all.sh
```

Access — Buyer: http://localhost:3000 · Merchant: `/merchant` · Admin: `/admin` · Jaeger: http://localhost:16686 · Grafana: http://localhost:3001

## Datasets

**Agent semantic faults** (`datasets/agentfault_k8s/`) — 96 faulted cases across 4 families (hallucinate / context_drift / wrong_item_pick / format_violation), each with machine-verifiable ground truth. See `SUMMARY.md` (what / how) + `EVAL_NOTES.md` (evaluation protocol).

**Traditional infra faults** (`datasets/traditional_k8s/`) — 255 cases (single 130 / dual 100 / triple 25), Chaos Mesh. Full telemetry (~16 GB) on Google Drive; see `traditional_k8s/README.md`.

## Data collection

The fault-injection + collection toolkit lives under `scripts/chaos/`. One-click collection scripts (each a self-contained orchestrator with pre-flight checks, per-case gates, and resume):

| Script | Collects | Output tree |
|---|---|---|
| `scripts/chaos/ctk/collect-{single,dual,triple}-dense.sh` | single 40 / dual 80 / triple 20 (v19 dense) | `datasets/k8s_pilot/{single,dual,triple}_dense/` |
| `scripts/chaos/ctk/collect-single-spread.sh` | single 55 (pod-failure & service-CPU, 6 services) | `datasets/k8s_pilot/single_spread/` |
| `scripts/chaos/ctk/collect-single-recagent.sh` | single 15 (rec-agent, + agent-span side capture) | `datasets/k8s_pilot/single_recagent/` |
| `scripts/chaos/ctk/collect-g2ext.sh` | G2ext batch — dual 25 + triple 20 (all-distinct multi-root) | `datasets/k8s_pilot/{dual,triple}_ext/` |
| `scripts/chaos/agentfault/run_collect_agentfault.sh` | agent semantic faults — 9 combos × 12 reps (108) | `datasets/agentfault_k8s/` |

> `collect-{single,dual,triple}.sh` (without `-dense`) are the v18 predecessors — kept for provenance, **do not run** (use the `-dense` versions).

> Re-collection requires the full 25-service K8S stack + Chaos Mesh + the large model assets. The shipped `datasets/` are the ready-to-use outputs; collection is for reproducing / extending only.

## Evaluation

```bash
# One-click agent-fault eval (offline steps are free; --with-judge needs a DeepSeek API key):
bash scripts/chaos/agentfault/run_eval_agentfault.sh --dataset-dir datasets/agentfault_k8s
```

BARO / RCD need `third_party/RCAEval` + patched `causallearn==0.1.2.3` (see `scripts/chaos/ctk/m9_score.py` header); without them those steps skip and the rest still runs.

## License & citation

- Source code: MIT · Dataset (`datasets/`): CC-BY-4.0 · Vendored libraries: original licenses
- See `LICENSE.md`; cite via `CITATION.cff`.
