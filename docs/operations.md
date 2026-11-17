# Operations Runbook

- **Start full stack**: `docker compose up --build -d`
- **Gateway Health**: `GET http://localhost:8000/health`
- **Prometheus Metrics**: `GET http://localhost:8000/metrics`
- **Prometheus UI**: `http://localhost:9090`
- **Grafana UI**: `http://localhost:3000` (default credentials `admin` / `admin`)
- **Inspect Circuit States**: `GET /admin/circuits` with header `X-Admin-Token: <ADMIN_API_KEY>`
- **Trigger Manual Canary Probe**: `POST /admin/circuits/{provider}/probe`
- **Inspect Queue & DLQ Depth**: `GET /admin/queue`
