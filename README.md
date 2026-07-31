# RecShop — Microservice RCA Dataset Platform

A real 25-microservice e-commerce recommendation system (SASRec + 4-Agent LangGraph + DeepSeek LLM) with fault injection, three-modal observability (OpenTelemetry), and machine-verifiable ground truth — for root cause analysis (RCA) research.

## What's in this repo

| Component | Description |
|---|---|
| `services/` | 25 microservices (Flask/FastAPI) — the running application |
| `datasets/agentfault_k8s/` | Agent semantic fault dataset (96 faulted cases, 4 families) |
| `datasets/traditional_k8s/` | Traditional infra fault dataset (255 cases; full telemetry on Google Drive) |
| `scripts/chaos/` | Fault injection + collection + evaluation toolkit |
| `docker/` `ops/` `k8s/` | Docker / OTel stack / K8S deployment configs |
| `scripts/database_schema.sql` | Database schema (idempotent, `CREATE TABLE IF NOT EXISTS`) |

**Pre-computed evaluation results** ship inside `datasets/agentfault_k8s/` (`BASELINE_RESULTS.md`, `RESULTS_WHENWHEN.md`, `RESULTS_CONTENT_CTXDRIFT.md`, `infra_negatives/`). You can read these without installing any evaluation dependencies.

## Quick start

### Prerequisites

- Python 3.10+ (`requirements.txt`)
- MySQL 8.0+
- **Large model assets** (not included due to size — download separately):

| File | Size | Description |
|---|---|---|
| `services/sasrec_api/standard_cache.pkl` | ~9.2 GB | SASRec model cache |
| `services/sasrec_api/SASRec-*.pth` | ~260 MB | SASRec checkpoint |
| `services/recommendation_agent/electronics.inter` | ~2.4 GB | SASRec interaction data |
| `shared/data/electronics.item` | ~1.2 GB | Item metadata |

Place them at the paths shown above.

### Database

```bash
mysql -u root -p < scripts/build_database.sql
```

### Start the system

```bash
# Without Nacos (offline mode):
NACOS_ENABLED=false python start_all.py

# With Docker OTel stack:
python start_all.py    # auto-starts Grafana/Jaeger/Prometheus/Loki

# Stop:
python start_all.py --stop
```

Set `NACOS_ENABLED=false` in `.env` (copy from `.env.example`) if you don't have Nacos installed. All services fall back to `127.0.0.1:<port>` direct connections.

### Access

- Buyer storefront: http://localhost:3000
- Merchant console: http://localhost:3000/merchant
- Admin backend: http://localhost:3000/admin
- Jaeger UI: http://localhost:16686
- Grafana: http://localhost:3001

## Agent semantic fault dataset

96 faulted cases across 4 families, each with machine-verifiable ground truth:

| Family | Cases | Target agents | Mechanism |
|---|---|---|---|
| `hallucinate` | 36 | 3 analyzers (×12 each) | Sub-LLM rewrites answer to be fluent but factually wrong |
| `context_drift` | 36 | 3 downstream agents (×12 each) | Upstream message deleted from downstream input |
| `wrong_item_pick` | 12 | Synthesizer | Recommended ASIN swapped to sentinel |
| `format_violation` | 12 | Synthesizer | 4 subtypes (missing/type/empty/malformed) |

See `datasets/agentfault_k8s/SUMMARY.md` for details, `datasets/agentfault_k8s/EVAL_NOTES.md` for evaluation protocol.

## Re-running evaluation

The evaluation toolkit (`scripts/chaos/agentfault/`) supports BARO, RCD, Who&When, and A2P. Pre-computed results ship with the dataset.

**To re-run evaluation from scratch**, you need two additional dependencies:

1. **RCAEval** — BARO/RCD implementations:
   ```bash
   git clone https://github.com/q7kcc/RCAEval.git third_party/RCAEval
   ```

2. **Patched causal-learn** — RCD requires `causal-learn==0.1.2.3` with 4 vendored patch files. Install causal-learn, then apply the patches:
   ```bash
   pip install causal-learn==0.1.2.3
   # Create third_party/_cl_patched/ with the patched causallearn package
   # (see scripts/chaos/ctk/m9_score.py:42-44 for sys.path setup)
   ```

Without these, Steps 4/5/9 of `run_eval_agentfault.sh` will fail with ImportError. All other steps (make cases, run Who&When judge, score) work without them.

```bash
# One-click evaluation (requires DeepSeek API key for Who&When judge):
bash scripts/chaos/agentfault/run_eval_agentfault.sh --dataset-dir datasets/agentfault_k8s
```

## Architecture

25 microservices on K8S (docker-desktop single node):

```
shop_web:3000 (BFF/three-endpoint UI)
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

## License

- **Source code**: MIT
- **Dataset** (`datasets/`): CC-BY-4.0
- **Vendored libraries** (`services/sasrec_api/vendor/`): original licenses apply

See `LICENSE.md` for details. Cite using `CITATION.cff`.
