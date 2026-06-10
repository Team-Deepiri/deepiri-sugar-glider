#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

export PATH="/usr/local/go/bin:/snap/bin:${PATH}"

echo "==> unit tests"
go test ./... -count=1

echo "==> go benchmarks (wal depth)"
go test -bench=BenchmarkDepthIncremental -benchmem ./internal/wal/

echo "==> starting bench stack"
docker compose -f docker-compose.bench.yml up -d --build

cleanup() {
  docker compose -f docker-compose.bench.yml down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

HTTP_URL="${BENCH_HTTP_URL:-http://localhost:18081}"
GRPC_ADDR="${BENCH_GRPC_ADDR:-localhost:15051}"

echo "==> waiting for sugar-glider health (${HTTP_URL})"
for _ in $(seq 1 60); do
  if curl -sf "${HTTP_URL}/healthz" >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf "${HTTP_URL}/healthz" >/dev/null

echo "==> gRPC smoke"
go run ./cmd/grpc-smoke --addr "${GRPC_ADDR}"

echo "==> publish pipeline load (1000 sequential publishes)"
export HTTP_URL
python3 - <<'PY'
import json, os, urllib.request, time

url = os.environ["HTTP_URL"] + "/v1/publish"
payload = {"event_type": "bench.load", "sender": "bench", "payload": {"n": 1}}
body = json.dumps(payload).encode()
start = time.perf_counter()
for i in range(1000):
    payload["payload"]["n"] = i
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=5) as resp:
        if resp.status != 200:
            raise SystemExit(f"publish failed status={resp.status}")
elapsed = time.perf_counter() - start
print(f"published 1000 events in {elapsed:.3f}s ({1000/elapsed:.1f} ops/s)")
PY

echo "✅ bench gate passed"
