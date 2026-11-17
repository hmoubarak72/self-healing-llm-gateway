# Chaos Engineering Guide

Inject fault rules dynamically via `POST /admin/chaos`:

```bash
curl -X POST http://localhost:8000/admin/chaos \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: change-me-super-secret-admin-key" \
  -d '{"provider": "openai", "error_rate": 1.0, "status_code": 500, "duration_seconds": 30}'
```

Clear all active chaos rules:

```bash
curl -X DELETE http://localhost:8000/admin/chaos \
  -H "X-Admin-Token: change-me-super-secret-admin-key"
```
