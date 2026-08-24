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

## Fix shape

Have each arm class report completions into the shared counter (or expose its counter for `Bench`
to poll) - the same one-line wiring per arm that `recordCompletion` gives the built-in paths. Until
then, **no latency figure exists for any async engine or for the proxy path**, which matters
because the proxy is what every non-JVM client runs on (astubbs#242) and its throughput already
collapses under skew - its latency under skew is unmeasured.

Also open from the same sweep: `proxy`/KEY/zipf has no capacity number (its ~78 msg/s saturated
rate needs `BENCH_RUN_TIMEOUT` raised above the capacity run's deadline).
