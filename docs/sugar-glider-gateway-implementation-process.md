# Sugar Glider Gateway Implementation Process

## Executive Summary

This document records the implementation process for making Sugar Glider viable as the main Realtime Gateway transport path.

On April 21, 2026, the reported status was: Plan #5 core work was complete, including the gRPC path, dispatcher mode, ACK batching/adaptive flush, and shadow-mode safety. The remaining work was targeted `32KB` and low-concurrency tuning plus final benchmark reruns before defaulting Glider fully.

The current implementation result is stronger than that April 21 status. The fixed v3 candidate closes the previous low-concurrency heavy-path gap in the local benchmark matrix while keeping reliability clean.

Recommendation: Sugar Glider should be kept and treated as the current best main gateway candidate for the RTG/Synapse transport lane. Redis remains the underlying stream substrate and comparison/rollback baseline; this work does not require event schema changes or LIS/Cyrex/Helox write-path rewrites.

## Scope

In scope:

- Realtime Gateway stream consumption path.
- Sugar Glider HTTP/gRPC transport service.
- Dispatcher mode for Redis Streams consumption behind Sugar Glider.
- ACK batching, adaptive flush, low-traffic flush behavior, and gRPC streaming stability.
- Shadow-mode and transport fallback safety flags.
- End-to-end publish-to-socket benchmark reproducibility.

Out of scope:

- Replacing Redis as the durable stream substrate.
- Changing event schemas.
- Rewriting LIS, Cyrex, or Helox document-routing write paths.
- Treating benchmark-only local Docker settings as final deployment topology.

## Architecture

The intended lane is:

```text
Publisher/Synapse API -> Redis Streams -> Sugar Glider -> Realtime Gateway -> Socket.IO clients
```

The important ownership boundary is that Realtime Gateway no longer owns Redis Streams implementation details directly for this lane. It consumes through Sugar Glider over HTTP or gRPC, while Sugar Glider owns Redis read, dispatch, ACK, WAL replay, and DLQ behavior.

## Implementation Timeline

April 21, 2026 status:

- Plan #5 core work complete.
- gRPC path implemented for RTG consumption.
- Sugar Glider dispatcher mode implemented.
- ACK batching/adaptive flush implemented.
- Shadow-mode safety implemented.
- Remaining risk: `32KB` heavy payloads and low-concurrency behavior needed more tuning and reruns.

Post-April 21 tuning:

- Tuned heavy path and ACK/dispatcher behavior.
- Added lazy payload parsing and optional broadcast-heavy mode by disabling payload user extraction.
- Added production-facing transport flags and startup logging.
- Validated `heavy-path-boost-v2` as the previous best proven checkpoint.
- Fixed v3 gRPC keepalive rejection.
- Fixed v3 Sugar Glider publish hot-path marshaling regression.
- Rebuilt and reran the full benchmark matrix twice.

## Key Implementation Changes

Realtime Gateway changes:

- Added `STREAM_TRANSPORT` selection between `sugar-glider-grpc` and `sugar-glider-http`.
- Added `STREAM_SHADOW_MODE` so RTG can consume and ACK without emitting Socket.IO events.
- Added gRPC subscribe path for Sugar Glider.
- Added ACK batching and flush controls.
- Added low-traffic ACK flush controls to protect sparse traffic.
- Added in-flight pause/resume controls for gRPC event processing.
- Added lazy payload parsing to avoid unnecessary JSON work on broadcast lanes.
- Added `STREAM_EXTRACT_USER_FROM_PAYLOAD=false` option for pure broadcast-heavy benchmark lanes.
- Added conservative gRPC keepalive configuration after discovering server-side excess-ping rejection.
- Added startup logs for transport, shadow mode, keepalive, ACK, in-flight, and payload parsing settings.

Sugar Glider changes:

- Added dispatcher consume mode for stream fanout.
- Added gRPC service path for subscribe, ACK, and health checks.
- Added dispatcher-side ACK batching and pipeline flushing.
- Added WAL replay and DLQ validation path.
- Tuned dispatcher read, block, subscriber buffer, ACK batch, ACK concurrency, and ACK queue settings.
- Optimized the Redis `XADD` publish hot path by using a positional value slice instead of a map.
- Fixed the v3 publish hot-path regression by converting `json.RawMessage` payloads to string values before passing them to go-redis.

Configuration changes:

- Local compose now sets transport explicitly.
- Kubernetes ConfigMap now includes transport and tuning flags.
- RTG README documents canary, rollback, gRPC keepalive, ACK, in-flight, and payload parsing flags.

## Important Environment Flags

Realtime Gateway:

```bash
STREAM_TRANSPORT=sugar-glider-grpc
STREAM_SHADOW_MODE=false
STREAM_CONSUMER_GROUP=realtime-gateway
STREAM_CONSUMER_NAME=realtime-1
STREAM_SUBSCRIBE_BATCH_SIZE=128
STREAM_EVENT_MAX_IN_FLIGHT=1024
STREAM_EVENT_RESUME_IN_FLIGHT=768
STREAM_ACK_BATCH_SIZE=256
STREAM_ACK_FLUSH_MS=6
STREAM_ACK_FLUSH_CONCURRENCY=8
STREAM_ACK_RETRY_MAX_ATTEMPTS=3
STREAM_ACK_RETRY_BASE_MS=25
STREAM_ACK_LOW_TRAFFIC_FLUSH_MS=1
STREAM_ACK_LOW_TRAFFIC_GAP_MS=16
STREAM_ACK_LOW_TRAFFIC_MAX_PENDING=32
STREAM_LAZY_PAYLOAD_PARSE=true
STREAM_EXTRACT_USER_FROM_PAYLOAD=false
STREAM_GRPC_KEEPALIVE_MS=300000
STREAM_GRPC_KEEPALIVE_TIMEOUT_MS=20000
STREAM_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS=false
```

Sugar Glider:

```bash
SIDECAR_CONSUME_MODE=dispatcher
SIDECAR_WAL_REPLAY_MODE=background
SIDECAR_DISPATCHER_CONSUMER_NAME=sugar-glider-dispatcher
SIDECAR_DISPATCHER_READ_COUNT=512
SIDECAR_DISPATCHER_BLOCK_MS=200
SIDECAR_DISPATCHER_SUBSCRIBER_BUFFER=1024
SIDECAR_DISPATCHER_ACK_BATCH_SIZE=256
SIDECAR_DISPATCHER_ACK_FLUSH_CONCURRENCY=8
SIDECAR_DISPATCHER_ACK_FLUSH_MS=6
SIDECAR_DISPATCHER_ACK_QUEUE_SIZE=16384
```

## Reproduce The Implementation Locally

Start from the active worktree:

```bash
cd /Users/Kyle/Developer/Deepiri/deepiri-platform-extract-hard
```

Run Sugar Glider tests:

```bash
cd /Users/Kyle/Developer/Deepiri/deepiri-platform-extract-hard/platform-services/shared/deepiri-sugar-glider
go test ./...
```

Build the Realtime Gateway Docker image:

```bash
cd /Users/Kyle/Developer/Deepiri/deepiri-platform-extract-hard
docker compose -f docker-compose.rtg-sugar-glider.local.yml build realtime-gateway
```

Build the Sugar Glider Docker image:

```bash
cd /Users/Kyle/Developer/Deepiri/deepiri-platform-extract-hard
docker compose -f docker-compose.rtg-sugar-glider.local.yml build synapse-sugar-glider
```

Start the local RTG/Sugar Glider stack:

```bash
docker compose -f docker-compose.rtg-sugar-glider.local.yml up -d
```

Verify health:

```bash
make rtg-health
```

Run clean-stream smoke and failure-path checks:

```bash
docker exec deepiri-redis-rtg-local redis-cli -a redispassword FLUSHALL
make rtg-smoke
make rtg-grpc-smoke
make rtg-failure
```

## Validation Notes

Validated successfully:

- `go test ./...` in Sugar Glider.
- Docker build for `realtime-gateway`.
- Docker build for `synapse-sugar-glider`.
- `make rtg-health`.
- `make rtg-smoke` after clean Redis flush.
- `make rtg-grpc-smoke` after clean Redis flush.
- `make rtg-failure` after clean Redis flush.
- Two full fixed v3 benchmark matrices.

Known local caveat:

- `make rtg-gate` can fail at preflight on the local laptop when available memory is `3GB (< 4GB required)`. In that case, record the memory preflight failure and proceed only if `make rtg-health`, HTTP smoke, gRPC smoke, and failure-path checks pass.
- Local `npm run build` for RTG can fail before patch validation because the workspace cannot resolve `@deepiri/shared-utils` from that package alone. Docker build was used as the authoritative build path because it has the correct package context.

## Rollback Path

Rollback is environment-only for this lane:

```bash
STREAM_TRANSPORT=sugar-glider-http
STREAM_SHADOW_MODE=false
```

Then restart the RTG workload. This does not require schema changes.

## Current Decision

Previous status was hybrid because v2 still lagged on low-concurrency heavy traffic. Fixed v3 closes that gap in the local matrix.

The current engineering recommendation is:

```text
Sugar Glider is viable as the main Realtime Gateway transport candidate. Keep Redis underneath as the durable stream substrate and fallback comparison point, but continue forward with Sugar Glider as the gateway abstraction path.
```
