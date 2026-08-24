# The async-engine arms cannot run under controlled arrival - the warmup barrier never sees them

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

Found 2026-08-24 by the first attempt to extend the latency matrix to the `pc`/`vertx`, `reactor`,
`mutiny` and `proxy` arms: **all 84 of their arrival rows timed out** (recorded as
`RUN_TIMEOUT_60s` in `bench/results/arrival-tail-skew-matrix-2.csv`) while the same arms' saturated
capacity runs passed in the same sweep.

## Mechanism, diagnosed

The arrival feeder's warmup barrier waits for the arm to finish `BENCH_ARRIVAL_WARMUP` records
before starting the measured schedule, and it watches `Bench.completedRecords` - which increments
only through `Bench.recordCompletion`. The `core`, `pool`, `vanilla` and `streams` paths call it;
the `ShareArm` was given support when the share matrix ran; **the `ExternalEngine` arm classes keep
their own completion counters and never touch it**, so the barrier waits out its 60s
(`BENCH_ARRIVAL_WARMUP_TIMEOUT_MS`) and the row is recorded as a timeout. The template's own
comment ("it needs no per-arm support") is true only of arms implemented inside `Bench` itself.

## Fixed 2026-08-24, same day

Each arm's completion callback now calls `Bench.recordCompletion(-1, ...)` - Reactor and Mutiny
pass `Bench.contextValue(ctx)` so e2e is measured; the proxy passes the dispatched record's value;
**Vert.x passes null**, because its only hook back is the HTTP response and no record context
reaches it - its e2e column stays honestly blank (residence still comes from the engine's own
meter). Verified by a reactor smoke under controlled arrival: barrier passed, e2e populated at the
handler's own tail. The full async sweep runs with `BENCH_RUN_TIMEOUT=420`, which also covers the
`proxy`/KEY/zipf capacity run the default deadline killed.

What remains open here: **Vert.x has no e2e measure** until its engine exposes the record context
on the response hook (or the arm correlates request to record itself). Everything else this note
described is closed by the sweep whose results land in `arrival-tail-skew-matrix-2-async.csv`.
