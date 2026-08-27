# Sugar Glider Next Steps

This document tracks follow-up work after the transport hardening and beef-up passes.

## Completed

- Incremental WAL depth tracking and optional `SIDECAR_WAL_MAX_ENTRIES` / `SUGAR_GLIDER_WAL_MAX_ENTRIES` cap
- Optional WAL disk byte quota via `*_WAL_MAX_BYTES`
- Streaming WAL replay to reduce memory use on large backlogs
- gRPC `PublishBatch` pipelined enqueue path
- Dispatcher read-loop backpressure and tolerant subscriber eviction
- Per-stream DLQ overrides via `*_DLQ_STREAM_POLICIES`
- Paginated DLQ pending scans via `*_DLQ_SCAN_BATCH`
- DLQ replay API (`POST /v1/dlq/replay` + gRPC `ReplayDLQ`)
- Richer `/readyz` (WAL depth/bytes, publish queue depth, dispatcher counts, reasons)
- Optional readiness thresholds: `*_READY_MAX_WAL_DEPTH`, `*_READY_MAX_PUBLISH_QUEUE_DEPTH`
- Dual env namespace: prefer `SUGAR_GLIDER_*`, keep `SIDECAR_*` aliases
- Dual Prometheus metric names (`synapse_sidecar_*` + `sugar_glider_*`)
- Explicit `PublishResponse.queued` proto field
- CI workflow, Docker `HEALTHCHECK`, and local `./scripts/run_bench_gate.sh`

## P0 — Production validation

1. Run the platform RTG gate after submodule bump:
   ```bash
   cd deepiri-platform
   make rtg-sugar-gate
   ```
2. Re-run the full end-to-end benchmark matrix and compare against the April baseline / v3 checkpoint.
3. Add Grafana dashboards for `sugar_glider_*` (and legacy `synapse_sidecar_*`) metrics.

## P1 — Reliability and operability

1. Integration tests — Redis testcontainer coverage for DLQ policy overrides, WAL caps, and publish pipeline batching.
2. Auth / ACL for `/v1/dlq/replay` in production networks.

## P2 — Performance

1. ACK span experiments — use `dispatcher_ack_contiguous_*` metrics to validate larger ACK batches.
2. Histogram metrics — add Prometheus histograms for publish/read/fan-out/ack latency.

## P3 — Naming cleanup

1. Rename binary/container artifact from `sidecar` to `sugar-glider` while keeping compatibility entrypoints.
2. After dual-metric adoption, plan deprecation of `synapse_sidecar_*` names.

## Suggested promotion sequence

```text
1. Merge sugar-glider PR → dev
2. Merge platform submodule bump + compose wiring PR
3. make rtg-sugar-gate
4. Run e2e benchmark matrix (2x)
5. Promote in RTG transport config after gates pass
```
