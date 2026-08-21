# Web GUI / demo: retry-queue visibility and making offset encoding's advantage obvious

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - after v6, direction not yet chosen -->


> Extracted from `origin/perf/192-offset-encoding-density` @2a31b0a74, `docs/inflight/web-gui-observability-ideas.md`.

Two user-raised ideas for the demo/GUI surface, parked 2026-08-18. The web GUI lives on the
unmerged `feats/web-gui` branch; the demonstration-app idea is likewise on an unmerged
experimental branch.

## 1. Expose retry-queue information in the web GUI

Question asked: is the retry queue already exposed in metrics? **Partially, not distinctly.**
`PCMetricsDef` today has:

- `inflight.records` - "processing OR waiting for retry" **mixed into one gauge**; retries are not
  separable
- `failed.records` - a counter of failures (per topic/partition), not a queue depth
- `waiting.records`, `slow.records`, `incomplete.offsets.total`, `partition.incomplete.offsets` -
  adjacent, but none says "N records are currently backed off awaiting retry, next due at T"

So a GUI retry view needs either a new dedicated gauge (retry-queue depth, ideally with
next-retry-due timing) or direct WorkManager state reads. Adding the metric first would benefit
all operators, not just the GUI.

## 2. Demonstrate what offset encoding actually buys (range beyond the committed offset)

The project makes little attempt to explain or demonstrate its headline trick: PC can keep
processing and acking records far beyond the committed base offset, because the incomplete map is
encoded into commit metadata. With run-length encoding and a single unacked offset the
continuable range is enormous - and with astubbs#306's density work (delta-list + Z85, plus the
committed benchmark report's engagement-point table) we now have real numbers for how far each
encoding stretches before back-pressure.

Idea: make this a centerpiece of the live demo app - visualize the incomplete map, the chosen
encoder, the encoded payload size vs the 4096 cap, and how far ahead of the committed offset
processing has run. `docs/offset-encoding-density-benchmark.md` already carries the numbers to
script such a demo. Related: `docs/inflight/perf-192-followups.md`.


## Feature targets taken from a competitor's telemetry surface (2026-08-21)

llingr ([`market-analysis-llingr.md`](market-analysis-llingr.md)) exposes a JSON snapshot endpoint
whose *fields* are a ready-made target list for this GUI - not because llingr is novel, but because
each answers a question an operator actually asks. Recorded as feature targets:

- **Per-partition `committedOffset` vs `highestReadyOffset`, and the gap between them.** Their
  snapshot shows `committedOffset: 361488, highestReadyOffset: 361515, gapBufferDepth: 27`. The
  equivalent for PC is richer and more interesting: we commit *past* gaps, so the natural display is
  **the incomplete-offset set itself** - how many, how far behind the frontier, and how long the
  oldest has been outstanding. That single view would make PC's central differentiator *visible*,
  which no amount of prose does.
- **Sliding 15-second throughput windows with latency figures**, per topic - rate over a short recent
  window rather than a cumulative average, which is what you want when watching a system move.
- **Per-shard / per-worker-pool occupancy**, and **guard-channel utilisation** - i.e. "how full is the
  in-flight budget right now". For PC that is in-flight versus `maxConcurrency`, and it is exactly the
  number [`next-auto-scaling.md`](next-auto-scaling.md) wants to control - so the GUI and the
  auto-scaler want the same series.
- **A single snapshot endpoint** returning all of it as JSON, separate from the metrics sink. Cheap,
  scriptable, and it makes the GUI one consumer of a documented API rather than the only way to see
  the data.
- **A live chaos/scaling widget** counting messages, scaling events, reassignments, dropped,
  duplicates and out-of-order - on their *marketing* site. Worth noting for
  [`next-landing-page.md`](next-landing-page.md): the same component serves operations and
  promotion.

We already have Micrometer metrics; the gap is presentation and the offset-set view.

## A full observable inventory from the only comparable product (2026-08-21)

llingr is the only other product solving this problem, so it has had to answer "what does an operator
need to see?" from scratch. Its answers are enumerated below - **not to copy, but because the
questions are the same ones our GUI must answer.** Source: its `/observability/` page, its Prometheus
module, its snapshot endpoint, and a terminal dashboard screenshot.

### What it exposes, grouped by the operator question

**"Am I saturated? Should I turn the dial up?"**
- `guardActive` / `guardCapacity` - in-flight now, versus the configured ceiling. **The single most
  useful number on the list**, and the direct analogue of PC's in-flight versus `maxConcurrency`.
- `overflowActive` / `overflowCapacity`, and a `used_overflow_total` counter - how often burst
  capacity was needed.
- `commitIngestActive` / `commitIngestCapacity` - commit-side backpressure.

**"Where am I, and how far behind?"**
- Per partition: `committedOffset`, `highestReadyOffset`, `maxOffsetSeen`, `assigned`.
- `gapBufferDepth` per partition - their stuck-handler alarm.
- A `current_offset` gauge per partition, for lag joins against the producer's end offset.

**"Is it fast, and where does the time go?"**
- A sliding window of 15 x 1-second buckets, each with `processedCount` and `deadLetterCount` -
  rate over a *recent* window rather than a cumulative average.
- `avgProcessDuration` / `maxProcessDuration` - time inside the user's function.
- `avgEndToEndDuration` / `maxEndToEndDuration` / `minEndToEndDuration` - poll to committable,
  inclusive of queue wait.
- Histograms for process duration, dead-letter write duration, and **queue wait** (read to
  process-start) - the last separates "my handler is slow" from "the engine made it wait".

**"Is the work spread evenly?"**
- Per shard: `activeWorkers`, `pooledWorkers` (16 tiles in the dashboard).
- `QueueDepth` per message - how many records for the same key are queued ahead of this one, which is
  a **direct measure of head-of-line pressure on a hot key**.

**"What is going wrong?"**
- Counters: `processed`, `errored`, `panicked`, `dead_lettered`, `duplicate`, `used_overflow`.
- Per-message outcome flags including `CommitBuffered`, `Orphaned` (completed after reassignment) and
  `FirstAfterRebalance`.

**"What is this costing me?"**
- Bandwidth per partition: received/transmitted bytes, message counts, compressed versus
  uncompressed, codec name.
- Broker topology with **rack** labels - so rack affinity, and therefore cross-AZ transfer cost, is
  visible.

**Design choices worth adopting regardless of the feature list**
- **Three channels, different audiences**: a push sink per message (fine-grained, for your own
  pipeline), a push sink per interval (bandwidth), and a pull JSON endpoint (dashboards). One
  endpoint returning everything is not a substitute for a metrics sink, and vice versa.
- **A single JSON snapshot endpoint** that a dashboard renders - the terminal dashboard is a
  faithful renderer of it, nothing more. That is the right layering for us too: the GUI becomes one
  consumer of a documented API rather than the only way to see the data.
- **Cardinality stated as a guarantee**: labels scale with partition count, not message volume or key
  count. Worth stating for ours.
- **Partition keys deliberately excluded** from telemetry, to avoid leaking user data. We should make
  the same commitment explicitly.

### What to ship first - ranked by operator value per unit of work

**Tier 1 - the minimum that makes a GUI worth opening.** These answer the three questions an operator
actually has when something is wrong, and PC already has the underlying data.

1. **In-flight versus the ceiling.** A single gauge: records in flight now, `maxConcurrency`, and the
   ratio. Answers "am I saturated" and is the input the auto-scaler will need anyway.
2. **Per-partition committed offset versus highest succeeded, and the size of the incomplete set.**
   This is **PC's differentiator made visible** - we commit *past* gaps, so the natural display is
   the incomplete set itself: how many, how far back the oldest is, how long it has been outstanding.
   No competitor can draw this picture because no competitor has the data.
3. **A recent-window throughput rate** - a sliding window of per-second buckets, not a cumulative
   average. Cumulative averages hide exactly the cliff you are looking for.

**Tier 2 - the diagnostic layer.**

4. **Queue-wait versus process-duration**, side by side. Separates "my code is slow" from "the engine
   made it wait" - the first question in every performance conversation.
5. **Per-shard occupancy**, to show key skew.
6. **Outcome counters** - processed, retried, failed, dead-lettered - with retry counts, which we have
   and llingr structurally cannot show because it has no retries.

**Tier 3 - later.**

7. Per-key queue depth (hot-key detection).
8. Bandwidth and cost telemetry, including rack affinity.
9. End-to-end latency percentiles.

**Correction, 2026-08-21: the JSON-first layering is already built.** `origin/feats/web-gui` carries
`parallel-consumer-dashboard` with `PcSnapshot`, `SnapshotJson`, `SnapshotPublisher`, `StateSampler`,
`PartitionSnapshot`, `WorkSnapshot`, `LifecycleSnapshot`, `MeterSource`/`MeterIndex` - and, notably,
**`EncodingSnapshot`**, which is the offset-encoding view no competitor can produce. So the
recommendation below is not new work; it is **confirmation that the existing design is the right one**,
and the useful output of this comparison is the *field list* rather than the architecture.

**What to check against that branch**, rather than rebuild:

- Does `PcSnapshot` carry **in-flight versus `maxConcurrency`** as an explicit pair? That is Tier 1
  item 1 and the series the auto-scaler needs.
- Does `PartitionSnapshot` expose **the incomplete set** - count, oldest outstanding offset, and its
  age - not merely committed and highest-seen? That is the differentiator view.
- Is throughput a **recent sliding window** or a cumulative average? See the note on rates below.
- Does `EncodingSnapshot` expose **encoded payload size against the broker's metadata limit**? That
  is a genuinely PC-only operational number: how close the offset map is to the 4 KiB ceiling, which
  is the thing that would actually hurt a user and which nothing else can measure.

**"Recent-window rate" means:** throughput computed over a short trailing window - llingr uses 15
buckets of 1 second - rather than total-processed divided by uptime. A cumulative average moves so
slowly that a consumer which stopped five minutes ago still shows a healthy number. A windowed rate
shows the cliff. This is the difference between a metric that reassures and one that informs.

**Data-privacy commitment, and PC's should be stronger.** llingr excludes partition keys from
telemetry deliberately. **Ours should go further and say so: no keys and no record data leave the API
at all - only statistics.** The one exception worth building is visibility into the *encoded offset
payload* - its size, run count and encoding chosen - which is metadata about our own commit metadata,
not user data.

**The recommendation below stands, but as a checklist against existing work rather than a build
order.**
Three numbers, honestly presented, beat a dashboard of twenty that nobody has decided the meaning of.
And item 2 is the one to lead on - it is the only view here that is *ours*.

### Related

- [`market-analysis-llingr.md`](market-analysis-llingr.md) - the full teardown.
- [`next-auto-scaling.md`](next-auto-scaling.md) - wants the same in-flight series as item 1.
- [`next-landing-page.md`](next-landing-page.md) - item 2 rendered as an animation is the
  clearest way to explain the offset map to someone who will not read prose.
