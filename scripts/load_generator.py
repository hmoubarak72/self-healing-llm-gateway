from __future__ import annotations

import argparse
import asyncio
import os
import random
import time
import httpx

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8000")

TENANTS = ["tenant_alpha", "tenant_beta", "tenant_enterprise"]
FEATURES = ["support_triage", "doc_summarizer", "lead_enrichment"]
CLASSES = ["low_latency_classification", "long_form_generation", "batch_enrichment"]


async def worker(client: httpx.AsyncClient, stop_at: float, worker_id: int) -> None:
    seq = 0
    while time.time() < stop_at:
        seq += 1
        req_class = random.choice(CLASSES)
        workload = "deferrable" if req_class == "batch_enrichment" else "interactive"
        try:
            await client.post(
                "/v1/chat/completions",
                headers={
                    "X-Tenant-ID": random.choice(TENANTS),
                    "X-Feature-Name": random.choice(FEATURES),
                    "X-Request-ID": f"load-w{worker_id}-{seq}",
                    "Idempotency-Key": f"idem-w{worker_id}-{seq}",
                },
                json={
                    "model": "gpt-4o-mini",
                    "request_class": req_class,
                    "workload_type": workload,
                    "messages": [{"role": "user", "content": f"Sample workload payload {seq}"}],
                },
            )
        except Exception:
            pass
        await asyncio.sleep(random.uniform(0.05, 0.25))


async def main(duration: int, concurrency: int) -> None:
    stop_at = time.time() + duration
    async with httpx.AsyncClient(base_url=GATEWAY_URL, timeout=15.0) as client:
        await asyncio.gather(*(worker(client, stop_at, i) for i in range(concurrency)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=90)
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    asyncio.run(main(args.duration, args.concurrency))
