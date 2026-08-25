# What survives Share Groups - and the honest answer to "why bother with PC now"

<!-- inflight-type: next -->
<!-- inflight-impact: architecture -->

**Antony, 2026-08-22, on seeing the measurement: "I wonder why anyone would bother with pc after v6
is out?"** Taken seriously rather than defended against, because the measurement is real and the
question is the right one.

## The concession, stated first

**If you do not need per-key ordering and you are on Kafka 4.2+, Share Groups are probably the better
choice.** No library, no sidecar, not JVM-only, maintained by the Kafka project itself, and **2.5x
faster** than PC's best arm at a near-zero handler
([`perf-share-groups-versus-pc-2026-08-22.md`](perf-share-groups-versus-pc-2026-08-22.md)).

Any positioning that does not begin there is not credible, and a reader who has seen the number will
stop reading at the first sentence that dodges it.

## What actually survives

| | Why Share Groups cannot take it |
|---|---|
| **Per-key ordering with concurrency** | They have no ordering guarantee and no equivalent of one. **This is the moat** |
| **Getting ahead of your own batch** | Neither acknowledgement mode permits polling with records unacknowledged, so outstanding work is capped at one batch. PC keeps records from many polls in flight - measured at 5,000 against 2,606 |
| **Old brokers** | Share Groups need 4.2+. Anyone on 3.x cannot use them at all, and that is most installations for years yet |
| **Retry and DLQ policy in your own code** | Delivery-count archival is the broker's policy, not yours |

## One slow record: Share Groups have head-of-line blocking too, at BATCH granularity

Not partition granularity as Kafka Streams does - but not per-record as PC does either.

A share consumer cannot poll until its batch is acknowledged, so **the slowest record in a batch sets
the poll cadence**. Processing the batch concurrently in your own pool does not fix it: throughput
becomes bounded by each batch's **maximum**, not its mean. One 10-second record in a 2,606-record
batch holds all of them. Past `share.record.lock.duration.ms` the record is released and redelivered
elsewhere, delivery count incremented, archived after the attempt limit.

**This is measurable now and has not been measured.** The tailed work model landed 2026-08-22
(`BENCH_DELAY_P99`, `BENCH_DELAY_STDDEV`, `BENCH_FAILURE_RATE`). Share Groups against PC with a p99 at
50-100x the median is the experiment that would show it, and it is the same experiment
[`next-the-harness-cannot-measure-latency-yet.md`](next-the-harness-cannot-measure-latency-yet.md)
already wants for `PARTITION` versus `KEY`. **One harness change serves both.**

## Could they add key ordering? Plausibly - and the state cost is why they have not

KIP-932 already tracks per-record state, so "do not acquire a record for key K while another K is
acquired" is conceptually a filter on acquisition. The obstacle is not the idea, it is where the state
lives:

- The share coordinator tracks **offsets and ranges**. Key-aware acquisition needs per-**key**
  in-flight state, and key cardinality is unbounded - potentially millions per partition.
- PC holds that in a client: cheap, per-instance, and losing it on restart costs nothing.
  Broker-side it is **replicated, durable state in `__share_group_state`**.

**That is the moat, and it is a cost asymmetry rather than a capability gap.** Which means it is not
safe: if Kafka decides the cost is worth paying, they can, and KIP-932's per-record design is a large
step toward it. **Nothing here should be written as though the moat is permanent.**

## The strategic consequence: this raises the value of the Streams work

**Share Groups eat the unordered use case. They cannot touch the ordered one.**

Stateful Kafka Streams operations require per-key ordering *by definition* - an aggregation, a join, a
windowed count. So Share Groups can never serve a stateful topology, whatever Kafka adds next, unless
they solve the state-cost problem above.

So the direction that survives this measurement is the one
[`next-what-kafka-streams-on-pc-is-worth.md`](next-what-kafka-streams-on-pc-is-worth.md) describes:
**ordered concurrency, and Streams**. The unordered-throughput story is the one under threat, and it
was never the differentiator - it was the easiest thing to benchmark.

**What this changes in practice:** the v6 announcement should not lead on unordered throughput; the
Streams work moves up the priority list rather than down; and the ordering guarantee stops being a
feature bullet and becomes the reason the project exists.
