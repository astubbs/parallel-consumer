# Pre-v6: the clients must expose core's batching modes

Owner's call, 2026-08-15, and the reasoning is defensive as much as technical:

> "One of the first things people are gonna say is, well, it'll be slower because it's one record at
> a time over a wire protocol. At least having the batching as it stands usable will help
> counter-argue that."

The objection is correct, and it will be the first one asked. Core already has the answer — batching
is implemented and shipped there — so the multi-language clients not exposing it is the gap, not the
architecture.

## The framing that matters: the API *is* batch, and single-record is the degenerate case

Owner's correction, and it reframes the work: **core's API is already batch-shaped.** `poll` hands
the user function a poll *context*, which is a container of records; a batch size of one simply
yields a context holding one. There is no separate single-record API in core to add batching to.

The clients modelled the degenerate case as though it were the API — one record in, one outcome out.
That is why adding batching later changes the user-facing signature in every language: a cost that
would not exist had they mirrored core's shape from the beginning, where batching is not a feature to
add but a size to configure.

The consequence for whoever picks this up: **do not design a second, parallel batch API.** Widen the
existing one so the record-shaped call becomes the convenience over a batch-shaped one, exactly as
core has it. And the general lesson for the remaining language waves — mirror the shape core already
chose rather than the shape the first client happened to need.

## Where this stands today

The proxy pins the batch size to **1** and the engine actively rejects anything larger, a deliberate
decision taken when the interaction model was settled (KTD10 in the language-proxy plan). So this is
a decision to revisit with its own reasoning in hand, not an oversight to patch: read why it was
pinned before undoing it, and record what changed.

Note the distinction that matters, because the words collide: the wire already carries a **wave** — a
`Dispatch` holding several records — but a wave is several records dispatched, each processed and
reported *individually*. **Batching** in core's sense is different: the user's function receives N
records in one call. The wire form may already suffice; the API shape does not.

## Two words, because one of them is doing two jobs

**"Batching" in this note means the PC client API shape and nothing else** - how many records the
user's function is handed per call. That is the meaning the rest of this file uses, it is core's
existing meaning, and it should not drift.

**The other thing is BUNDLING: how many records cross the language boundary per hop.** Borrowed
deliberately from Apache Beam, which calls exactly this unit a bundle, so the word arrives with
provenance and cannot be confused with the API question. Bundling is a transport concern and is
invisible in the user's signature; batching is a signature concern and says nothing about hops. A
client could bundle a hundred records per hop and still hand the user one at a time, or the reverse.

Whoever picks up either must say which word they mean in the first sentence. The two have different
owners, different difficulty, and - as below - different answers for Parallel Consumer and for
Kafka Streams.

## What a crossing actually costs, at sizes worth caring about

Measured 2026-08-24:
[`perf-streams-crossing-attribution.md`](perf-streams-crossing-attribution.md). **A boundary
crossing costs about 150us per record at steady state**, and Kafka's and Kafka Streams' own marginal
per-record work measured at statistically zero beside it. One stream thread therefore tops out
around 6,700 records/sec.

150us is easy to wave away. Scaled up it is not:

| Sustained rate | Stream threads consumed by the crossing ALONE | Continuous CPU |
|---|---|---|
| 1,000 rec/s | 0.15 | 15% of one core |
| 10,000 rec/s | 1.5 | ~1.5 cores, or **36 core-hours a day** |
| 100,000 rec/s | 15 | ~15 cores, or **360 core-hours a day** |
| 1,000,000 rec/s | 150 | ~150 cores |

Two consequences that are easy to miss from the per-record figure:

- **It sets a partition floor.** Kafka Streams cannot run more stream threads than partitions, so
  100,000 rec/s needs at least fifteen partitions purely to have somewhere to put the boundary work
  - before a single partition is allocated to the actual business logic.
- **The cost is per record, so it is invisible until it is dominant.** Below about 1,000 rec/s
  nobody will notice. Between 10,000 and 100,000 it becomes a line item you provision cores for.
  Above that the boundary IS the architecture, and bundling stops being an optimisation and becomes
  a precondition.

**Read the comparison honestly in both directions.** Against native JVM Kafka Streams this is
enormous: the operator call it replaces is a Java lambda invocation costing nanoseconds. Against
what the target user actually has, it is not - a non-JVM team has no Kafka Streams at all, and the
JVM team's status quo for reaching a Python library is an HTTP call to a service from inside the
topology, which blocks the same stream thread for a typical local round trip of roughly 0.5-2ms.
150us is several times better than that AND removes a deployment. Neither framing is the whole
truth and the note should keep both.

**Caveats, because the number was measured on a deliberately thin workload.** The engine arm ran an
in-memory store and a trivial transform, so real workloads doing RocksDB writes, joins or larger
payloads pay more per record on the engine side - the crossing's absolute cost stays put while its
*share* falls. Records were small, so 150us is a floor for payload size, not a ceiling. One machine,
loopback gRPC, one thread.

## What bundling would actually recover, and the number nobody has yet

The obvious arithmetic - a hundred records per hop turns 150us into 1.5us - **assumes the crossing
is all fixed cost, and that is unmeasured.** A crossing is a serialise, a syscall, a thread handoff
and a wake, plus a per-byte copy that bundling does not amortise at all. For the small records
measured here the fixed part almost certainly dominates, but "almost certainly" is not a
measurement.

**MEASURED 2026-08-24, and the answer is "both, depending on payload":**
[`perf-crossing-fixed-versus-per-byte.md`](perf-crossing-fixed-versus-per-byte.md). The crossing is
**about 120us fixed plus ~6.5us per KB**. Below 1 KB there is no size dependence at all - the fitted
slope is -0.09us/KB with an r2 of 0.00 - so a small record's crossing is entirely fixed cost, which
is entirely what bundling amortises.

| Record size | Crossing | Fixed share | Bundle of 100 | Gain |
|---|---|---|---|---|
| 16 B | 120us | 100% | 1.3us | 92x |
| 1 KB | 126us | 95% | 7.7us | 16x |
| 4 KB | 146us | 82% | 27.2us | 5x |
| 16 KB | 224us | 54% | 105.2us | 2x |
| 64 KB | 536us | 22% | 417.2us | 1x |

So **bundling is transformative under a few KB and marginal above 16 KB**, where the per-byte copy
dominates and only a zero-copy transport can remove it. A single headline figure would have been
wrong for half the range, which is why the question was worth measuring rather than assuming.

**The gains are upper bounds: bundle-assembly cost is not measured.** Grouping N records into one
frame is not free, and whoever builds this should measure that before quoting these numbers.

## The cross-cutting view

[`../language-bindings.md`](../language-bindings.md) places this decision among the other four a
binding has to make, and records a collision worth reading before designing anything here: the
per-record-outcome requirement below is exactly what Beam gave up to get its batching, deliberately.
The options for keeping both are set out there.

## What it costs, honestly

- **Every client's user-facing surface changes**, in all the languages. That is the horizontal cost
  this project pays for anything, so it should land while the clients are young rather than after
  they are published and their examples are copied.
- **Per-record outcomes must survive.** The engine completes each record's container independently,
  which is what makes partial failure expressible. A batch API that returns one outcome for N records
  would throw that away — one bad record would poison its whole batch, turning a per-record retry
  into a batch-wide one. Whatever the shape, N records in must be able to produce N outcomes out.
- **Ordering guarantees must not weaken.** A batch must not be allowed to span shards in a way that
  breaks the ordering the product exists to provide; the engine's existing distinct-shard rule for
  waves is the precedent to follow.

## Why it is worth the cost

It is the honest answer to the throughput question rather than a rhetorical one, and it is already
built on the engine side. Measuring it is also the point — the performance work
([`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md))
should report the sidecar hop's cost *with batching in use*, since that is the configuration a
throughput-sensitive user would actually run. A benchmark that only measures the unbatched path
answers the objection in the worst possible way: by confirming it.
