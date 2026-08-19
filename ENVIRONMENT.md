# Environment & Version Provenance

Versions of the platform the datasets were collected on (authoritative, captured live
from the collection cluster). Pin your environment to these for closest reproduction.

| Component | Version | Where it runs |
|---|---|---|
| Docker Desktop | 4.82 (engine 29.6.1) | Windows host |
| Kubernetes | v1.36.1 | docker-desktop, single node |
| Chaos Mesh | v2.8.3 (chaos-dns v0.2.8) | K8s, `chaos-mesh` namespace |
| MySQL | 8.0.46 Community | Windows host service (`DB_HOST`; not a pod) |
| Python | 3.10.20 | conda env |
| OTel collector | 0.128.0 | docker compose (`ops/docker-compose.otel.yml`) |
| Jaeger | 1.76.0 | docker compose |
| Prometheus | v3.5.0 | docker compose |
| Loki | 3.5.3 | docker compose |
| Grafana | 11.6.1 | docker compose |

Python package pins: `requirements.txt` (fully pinned except `torch`, which is
platform-variant — install per your CPU/CUDA target). Service images are built
locally (`recweb-*:latest`, see `k8s/`) on top of `nginx:alpine` for the gateway.

## Dataset provenance

- The **strict51 (traditional v2)** campaign (2026-08-17 → 08-19) ran on exactly the
  versions above. Code identity for that campaign is frozen in
  `docs/acceptance/contracts/traditional-v2-lite-strict51-20260816/strict51-freeze-report.json`
  (SHA256 pins; component chain verified at runtime by the harness).
- Database seed checksums enforced per-case by the collection gates (fail-closed):
  `items = 3849590678`, `inventory = 3935678504`.

## Known version sensitivities

- **Docker Desktop 4.82**: the host-port kubeconfig proxy started returning 404 for
  non-resource paths (`/readyz`, `:8001/api`). `run-traditional-v2-lite.ps1` probes
  typed paths (`/api/v1/namespaces/kube-system`) to stay compatible; older Docker
  versions work the same way.
- MySQL must be reachable from inside the cluster (host service). With
  `NACOS_ENABLED=false` the services fall back to fixed `DB_HOST` from `.env`.
