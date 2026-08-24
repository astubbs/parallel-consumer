# Register: the performance work still open, ranked by what it blocks

<!-- inflight-type: register -->

The engine-performance campaign (2026-08-20 → 24) left these open. Consulted, never completed:
strike items here as their notes close. Full findings live in the per-item notes; this register
exists so no session has to re-derive the list (it was re-derived from grep on 2026-08-24, which is
what prompted it).

## Open measurement and investigation

1. **The ~30% throughput regression since 0.3.0.2 - A RELEASE GATE THE OWNER SET (2026-08-20).**
   ANSWERED WITH THE GOOD ANSWER 2026-08-24: the fix is built on `fix/external-engine-pipeline-buffer`
   (local, awaiting the owner's word to push/PR) - ExternalEngine's shortfall-only request target
   deleted so every async engine inherits core's pipelined target. Proven-red unit test, and a
   one-term controlled bench: +23% on the Vert.x arm at 100k records (the note's 350k experiment
   read +34%), peak in-flight exactly 100 in both arms. What remains is review and merge.
   [`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md).
2. **Async-engine latency sweep** - harness fixed and smoke-verified on `perf/engine-concurrency`
   (the arms now feed the arrival barrier's counter); the full sweep was interrupted by the operator
   and is one re-run of `bench/run-arrival-matrix.sh`. Residual: Vert.x has no e2e measure (no
   record context on its response hook).
   [`perf-async-arms-cannot-run-controlled-arrival.md`](perf-async-arms-cannot-run-controlled-arrival.md).
3. **The 5.9x share-broker variance** - unexplained; blocks any share-groups figure in either
   direction. [`perf-share-groups-versus-pc-2026-08-22.md`](perf-share-groups-versus-pc-2026-08-22.md).
4. **Why `proxy` collapses under key skew** (~78 msg/s against `core`'s 371) - unattributed, and it
   is every non-JVM client's path. Sightings in
   [`perf-the-tail-experiment-ran-2026-08-22.md`](perf-the-tail-experiment-ran-2026-08-22.md).
5. **Why `core-dpvt` is the most failure-sensitive arm** (loses half its throughput to a 1% failure
   rate) - unattributed. Same source note.
6. **`PARTITION` starves on the default buffer** - workaround known (`messageBufferSize` 20,000
   recovers 14.8x), the real fix (a coverage-based prefetch target) unbuilt.
   [`bug-partition-ordering-starves-on-a-narrow-buffer.md`](bug-partition-ordering-starves-on-a-narrow-buffer.md).
7. **The in-flight ceiling above 2,000 configured concurrency.**
   [`bug-in-flight-ceiling-above-2000-concurrency.md`](bug-in-flight-ceiling-above-2000-concurrency.md).
8. **The retry-delay axis was never varied** - PC's 1s retry makes failure-mode comparisons (Share
   Groups' immediate re-acquire; the `tailf` cells) not like-for-like, and `tailf` latency needs
   more repeats before any figure is quotable (heavy-tailed at two).

## Done on a branch, waiting only on merges

- Virtual threads, direct pull + `ShardOccupancy`, residence-time latency, the eleven-arm harness
  with controlled arrival and three latency families: all on `perf/engine-concurrency`, landing via
  the stack in [`branch-engine-concurrency-pr-stack.md`](branch-engine-concurrency-pr-stack.md).
- The K4 head-of-line measurement (partition-serial vs `KEY`): `perf/engine-concurrency`,
  `bench/results/streams-model-head-of-line.csv`.
- The broker-poller load-gate drift fix: astubbs#336, cut from master.
- The dispatch-scan fix's counting test and the exactly-once claim fix: astubbs#335.

## Promoted engineering ideas - speculative, none blocking v6

Async engines on virtual threads ([`next-async-engines-on-virtual-threads.md`](next-async-engines-on-virtual-threads.md)) ·
GraalVM native + proxy ([`next-virtual-threads-under-graalvm-native.md`](next-virtual-threads-under-graalvm-native.md)) ·
work manager as a thread ([`next-work-manager-as-a-thread.md`](next-work-manager-as-a-thread.md)) ·
pre-rendered work order ([`next-pre-rendered-work-order-list.md`](next-pre-rendered-work-order-list.md)) ·
retry-queue selection ([`next-select-retries-from-the-retry-queue.md`](next-select-retries-from-the-retry-queue.md)) ·
selectable shard queue ([`next-selectable-shard-queue.md`](next-selectable-shard-queue.md)).

## Deferred - after v6, by state tag

The comparison matrix, the astubbs#192 offset-density follow-ups, and the batch quantity
over-request carry `deferred - after v6` in their own notes.
