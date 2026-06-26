# Sugar Glider Next Steps

This document tracks the recommended follow-up work after the performance and reliability hardening pass (WAL backpressure, publish batching, dispatcher backpressure, per-stream DLQ, and Prometheus metrics migration).

## Completed in this pass

- Incremental WAL depth tracking and optional `SIDECAR_WAL_MAX_ENTRIES` cap
- Streaming WAL replay to reduce memory use on large backlogs
- gRPC `PublishBatch` pipelined enqueue path
- Dispatcher read-loop backpressure and tolerant subscriber eviction
- Per-stream DLQ overrides via `SIDECAR_DLQ_STREAM_POLICIES`
- Paginated DLQ pending scans via `SIDECAR_DLQ_SCAN_BATCH`
- Prometheus client migration for `/metrics` (legacy `synapse_sidecar_*` names preserved)
- CI workflow, Docker `HEALTHCHECK`, and local `./scripts/run_bench_gate.sh`

## P0 — Production validation

1. Run the platform RTG gate after submodule bump:
   ```bash
   cd deepiri-platform
   make rtg-sugar-gate
   ```
2. Re-run the full 27-cell end-to-end benchmark matrix twice and compare against the April 12 baseline and fixed v3 checkpoint.
3. Add Grafana dashboards for the Prometheus metrics now emitted by the official client.

## P1 — Reliability and operability

1. **DLQ replay API** — add admin/gRPC endpoint to requeue entries from per-stream DLQ destinations.
2. **Richer readiness** — include WAL depth, publish pipeline queue depth, and dispatcher active count in `/readyz` failure reasons.
3. **WAL disk quota** — enforce max bytes on disk in addition to max entry count.
4. **Integration tests** — Redis testcontainer coverage for DLQ policy overrides, WAL cap behavior, and publish pipeline batching.

## P2 — Performance

1. **ACK span experiments** — use `dispatcher_ack_contiguous_*` metrics to validate whether larger ACK batches or alternate flush windows reduce Redis pressure.
2. **Histogram metrics** — add Prometheus histograms for publish, read, fan-out, and ack latency (summaries currently use counter + max gauge only).
3. **Proto queued flag** — extend `PublishResponse` with explicit `queued` semantics instead of empty `entry_id`.

## P3 — Naming and platform cleanup

1. Migrate env vars from `SIDECAR_*` to `SUGAR_GLIDER_*` with backward-compatible aliases.
2. Rename binary/container artifact from `sidecar` to `sugar-glider` while keeping compatibility entrypoints.
3. Add dual metric names (`sugar_glider_*` aliases) before deprecating `synapse_sidecar_*`.
4. Ensure `deepiri-platform` submodules for `deepiri-sugar-glider` and `deepiri-synapse` are initialized in onboarding docs — local RTG compose depends on both.

## Suggested promotion sequence

```text
1. Merge sugar-glider PR
2. Merge platform submodule bump PR
3. make rtg-sugar-gate
4. Run e2e benchmark matrix (2x)
5. Promote in RTG transport config after gates pass
```

## Owners / dependencies

- **Sugar Glider service:** this repository
- **RTG transport integration:** `deepiri-platform` → `deepiri-realtime-gateway`
- **Benchmark harness:** `deepiri-platform/scripts/dev/sugarglider/`
- **Blocked locally if:** `platform-services/shared/deepiri-synapse` submodule is not checked out
