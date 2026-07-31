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
| `scripts/database_schema.sql` | Database schema (idempotent, `CREATE TABLE IF NOT EXISTS`) |

Pre-computed evaluation results ship inside `datasets/agentfault_k8s/` (`BASELINE_RESULTS.md`, `RESULTS_WHENWHEN.md`, `RESULTS_CONTENT_CTXDRIFT.md`, `infra_negatives/`) — readable without installing any evaluation dependencies.

## Quick start

**Prerequisites**: Python 3.10+ (`requirements.txt`), MySQL 8.0+, and the large model assets (not included due to size): `services/sasrec_api/standard_cache.pkl` (~9.2 GB), `SASRec-*.pth` (~260 MB), `services/recommendation_agent/electronics.inter` (~2.4 GB), `shared/data/electronics.item` (~1.2 GB).

```bash
mysql -u root -p < scripts/build_database.sql          # init DB
NACOS_ENABLED=false python start_all.py                 # start (offline mode, no Nacos)
python start_all.py --stop                              # stop
```

Access — Buyer: http://localhost:3000 · Merchant: `/merchant` · Admin: `/admin` · Jaeger: http://localhost:16686 · Grafana: http://localhost:3001

## Datasets

**Agent semantic faults** (`datasets/agentfault_k8s/`) — 96 faulted cases across 4 families, each with machine-verifiable ground truth. See `SUMMARY.md` (what / how) + `EVAL_NOTES.md` (evaluation protocol).

| Family | Cases | Mechanism |
|---|---|---|
| `hallucinate` | 36 | Sub-LLM rewrites the answer to be fluent but factually wrong |
| `context_drift` | 36 | Upstream message deleted from the downstream agent's input |
| `wrong_item_pick` | 12 | Recommended ASIN swapped to a sentinel |
| `format_violation` | 12 | 4 subtypes (missing / type / empty / malformed) |

**Traditional infra faults** (`datasets/traditional_k8s/`) — 255 cases (single 130 / dual 100 / triple 25), Chaos Mesh. Full telemetry (~16 GB) hosted on Google Drive; see `traditional_k8s/README.md`.

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

## Architecture

```
shop_web:3000 (BFF / three-endpoint UI)
├── Recommendation: backend_api:5000 → sasrec_api:8200
│                  recommendation_agent:5001 → sasrec + DeepSeek
│                  llm_rerank_service:5002 → DeepSeek
├── Transaction: checkout:5011 (fan-out) → cart/pricing/inventory
│                order:5010 · payment:5012 · shipping:5016 → notification:5021
├── Content: catalog:5005 (high fan-in) · search:5017 · review:5003/5018
├── Profile: user:5004 · address:5007 · merchant:5019 · announcement:5009
└── Telemetry: interaction:5020 · admin_audit:5022 · ai_memory:5008
Shared: MySQL shopify2 · Nacos (optional) · DeepSeek API · OTel collector
```

## License & citation

- Source code: MIT · Dataset (`datasets/`): CC-BY-4.0 · Vendored libraries: original licenses
- See `LICENSE.md`; cite via `CITATION.cff`.
