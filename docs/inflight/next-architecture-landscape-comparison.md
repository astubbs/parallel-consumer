# Next: a standing comparison of PC against the systems that solved the same problems

A document comparing Parallel Consumer with the projects that have already crossed the same
boundaries, so this project can check itself against them instead of rediscovering their answers.
Wanted as a permanent reference, not a one-off: the flow-control / in-flight-accounting seam has
produced four consecutive review rounds where each fix caused the next round's defect, and every one
of those rounds would have been shortened by knowing what Beam or Temporal already decided.

## Why it is worth writing down

Research during the language-proxy ideation (astubbs#242) surfaced that the hardest questions on that
branch are all solved problems elsewhere, with published rationale:

- **Apache Beam's portability framework / Fn API** — the closest analogue in existence. Runner owns
  scheduling and state; a per-language SDK harness runs user code; gRPC between them on split
  control/data/state/logging planes. The runner is the *server* and the harness *dials out*,
  deliberately, because runners often sit where they cannot accept inbound connections. One harness
  fans out internally — threads for Java and Go, one subprocess per core for Python because of the
  GIL. Work travels as bundles, and **any element failing discards and retries the whole bundle**;
  per-item outcomes were considered and deliberately rejected as not worth the complexity.
- **Temporal** — replaced N full-logic SDKs with a shared Rust core plus thin bindings, explicitly to
  stop reimplementing hard protocol logic per language. Note the limit, which is easy to overstate:
  the **Go and Java SDKs are independent implementations**, not core bindings; only Python,
  TypeScript, .NET and Ruby wrap the core.
- **Ray** — worker pool lifecycle owned entirely by a node-local daemon; the language driver never
  spawns workers, because spawn/reap/orphan semantics are where the per-language bugs live.
- **Envoy `ext_proc`** — one bidirectional stream per request, with the *external processor*
  controlling how much it receives. Inverts the usual assumption that the host dictates granularity.
- **Bytewax** (Rust core, Python over in-process FFI, no IPC at all) and **Quix Streams** (pure
  Python, no polyglot split) — the two baselines either side of a protocol boundary.
- **Dask** — batches assigned to the least-busy worker, batch size an explicit tunable trading
  submission overhead against idle-start latency.
- **Kafka Share Groups (KIP-932)** — the nearest competitor for the same use case, and the reason
  PC's "no processing clock" property matters: Share Groups' acquisition-lock timeout genuinely is a
  redelivery clock, and PC's absence of one is a stated differentiator.

## What the document should answer, per system

Who owns scheduling; what crosses the language boundary and what never does; the unit of work;
how worker death is detected and what it costs; how thin the per-language client is; how ordering is
preserved; and where PC deliberately differs rather than accidentally.

## The second half: competitive positioning

Three landscapes, and PC sits differently in each:

- **Kafka clients generally** — key-based ordering with concurrency beyond partition count, breadth
  of client languages, latency, retry and offset semantics.
- **Message-processing systems generally** — output modelling, batching, delivery guarantees, and
  what a user has to give up to get each.
- **The "faster / cheaper / more flexible Kafka" vendors** — added 2026-08-15 in answer to a direct
  question: *is somebody already selling what this is becoming?* Provisional answer below; it is the
  axis most likely to be out of date, so **verify before publishing anything that depends on it**.

### Is anyone already selling this? — provisional map, needs verification

The question arose while writing
[`next-work-server-pitch-and-buyer.md`](next-work-server-pitch-and-buyer.md), whose partition-cost
argument would be undercut if a vendor already made it. The provisional finding is that **the
"faster Kafka" vendors compete on storage and operational economics and leave the consumption model
untouched** — which is why none of them scores what this does, and why llingr is complements rather
than rivals.

- **Kafka-protocol reimplementations and re-engines** — Redpanda, WarpStream, AutoMQ, Bufstream,
  Confluent's own newer engine work. They compete on cost per byte, cross-AZ traffic, object-store
  backing, no-ZooKeeper operation, per-core efficiency. **None of them changes the consumer group
  model**: a topic still delivers partition-bounded ordered consumption, so the partition↔concurrency
  coupling survives every one of them. PC runs on top of any of them, and their per-partition
  efficiency claims reduce the *cost* of over-partitioning without removing the *reason* for it.
- **Kafka Share Groups (KIP-932)** — the genuinely overlapping feature, and the one that concedes
  ordering. `STRATEGY.md` owns this comparison; do not restate it here.
- **Pulsar's `Key_Shared` subscription** — the closest thing to prior art for the actual combination:
  per-key ordering with more consumers than partitions, shipped and used. It is worth treating as
  **validation that the demand is real rather than as evidence this is redundant**, because obtaining
  it costs a broker migration. If the "the combination appears to be new" claim is ever published,
  this is the counter-example a reader will raise, so establish exactly how its guarantees differ
  before making the claim.

  **The hypothesis to test first, because the whole comparison turns on it: the unit of parallelism
  differs.** `Key_Shared` routes a hash range of keys to a *consumer*, so N consumers give N-way
  parallelism and the application must still serialise per key inside each one — the intra-consumer
  head-of-line problem appears untouched. PC's unit is the *key*, so one process can hold as many
  records in flight as its pool allows. If that holds, the difference is orders of magnitude of
  concurrency per process, not a feature checkbox — and it is the single most important thing to
  verify in this whole document.

  Two things to check in the other direction, since they look like real Pulsar advantages: the broker
  tracks **individual acknowledgements natively**, which is precisely the capability Kafka lacks and
  that PC's offset-map encoding exists to synthesise client-side; and consequently Pulsar has no
  analogue of PC's commit-metadata size ceiling. Also establish how `Key_Shared` behaves when one
  consumer is slow, and what `allowOutOfOrderDelivery` concedes.

  **"Is PC on Kafka faster than Pulsar?" is the wrong question and should be refused rather than
  answered.** As posed it collapses into Kafka-versus-Pulsar throughput, which is contested vendor
  benchmark territory and unwinnable. The answerable question is **how much concurrency is achievable
  per unit of infrastructure at a given ordering guarantee** — and nobody here has measured it against
  Pulsar. Say unmeasured until it is measured.
- **Durable-execution platforms** — Temporal, Restate, Inngest. A different axis again (workflow
  orchestration and state, not stream consumption), and already partly covered above.

**Freshness caveat, and it is the point of writing this down rather than asserting it in a post:**
this space moves faster than any other on the list, the above is from memory, and
`docs/solutions/documentation-gaps/competitor-comparison-docs-must-cite-the-primary-spec.md` already
records what happens when a comparison is written without citing the primary source.

## Not started

No file written yet. This note exists so the research is not lost — it was gathered during the
astubbs#242 ideation and is currently only in that session's artifact
(`docs/ideation/2026-08-14-language-proxy-interaction-model-ideation.html`), which is scoped to the
interaction model and will not carry the comparison as it grows.


## Add llingr - the closest analogue found so far (2026-08-21)

[`market-analysis-llingr.md`](market-analysis-llingr.md) is a full teardown; this is the pointer, and
the reason it belongs in *this* document rather than only in a competitor file.

It answers this note's own per-system questions unusually directly, and it converged on the same
answers as the language-proxy work **independently and at the same time** - which is evidence about
the architecture rather than about either project:

- **Who owns scheduling:** the engine, entirely. Same as PC.
- **What crosses the language boundary:** five gRPC methods - `ProcessMessage`, `WriteDeadLetter`,
  `SendMetrics`, `NotifyShutdown`, `Heartbeat`. Compare astubbs#242's protocol, which has `dispatch`
  negotiated today and leases/heartbeat designed but unimplemented by any client.
- **The unit of work:** one message, not a bundle - the opposite of Beam's bundle-retry model recorded
  above, and the same choice PC made.
- **The FFI-versus-sidecar split:** Rust and C/C++ via FFI, everything else via a gRPC sidecar
  container. **That is the exact conclusion `branch-language-proxy.md` reached** in its
  native-bindings section.
- **Engine duplication:** Go original, a **separate native JVM implementation**, and Rust as an FFI
  binding over the Go engine. That is the Temporal pattern this document already records - shared core
  plus bindings, with specific languages reimplemented natively - arrived at from the other direction.
- **The broker is pluggable:** Kafka via two client adapters, and NATS JetStream. PC is Kafka-only by
  construction, which is a scope decision worth making consciously rather than by default.


## Add KPipe - the embedded competitor, and the first to benchmark against PC (2026-09-08)

<https://github.com/eschizoid/kpipe> - Apache-2.0, on Maven Central as `io.github.eschizoid:kpipe-*`,
Java 25 floor. One author; first commit 2025-04-09, then near-dormant until v1.0.0 on 2026-03-09,
forty releases since, still committing on 2026-09-08. **Surveyed from source, README, benchmark
harness and git history; not run.** The full entry, against the Hasten register's eight questions,
is in `core-hasten-adjacent-systems-register.md` on astubbs/parallel-consumer#367; this is the pointer
and the part that answers *this* note's questions.

It sits where llingr does not: **embedded, JVM-only, same side of the position axis as PC**, and it
is the first system found anywhere in this corpus that names Parallel Consumer as its comparator.

- **Who owns scheduling:** the library, but there is no scheduler. `PARALLEL` starts one virtual
  thread per record as it arrives; `KEY_ORDERED` keeps a map of per-key queues, each drained by a
  virtual thread that exits when its queue empties, capped at 10,000 distinct keys with an
  evict-empty-idle policy and a stall when nothing is evictable. Nothing chooses *which* work goes
  next - arrival order is the order. PC's controller loop selecting across shards under per-partition
  in-flight limits, retry delays and commit-metadata backpressure is the thing KPipe does not have.
- **What crosses the language boundary:** nothing. No sidecar, no FFI, no polyglot story at all.
- **The unit of work:** one record, same as PC and llingr - not Beam's bundle.
- **Worker death:** a virtual thread per record makes it free to start and nothing to detect; the
  cost moved to allocation, which its own captures put at about 1.7 KB per record against about 35 B
  for PC.
- **Ordering:** per-key serial queues, plus a whole-consumer `SEQUENTIAL` mode. No partition mode.
- **Commit:** lowest still-pending offset per partition, in memory, committed as the contiguous
  prefix - the llingr shape, and the same restart cost `market-analysis-llingr.md` measured: a crash
  replays everything above the frontier. Nothing is encoded into commit metadata.
- **Where PC deliberately differs:** the encoded frontier, the shard scheduler, `PARTITION` order,
  transactional produce (KPipe's README says *not supported*, and its source has no
  `initTransactions`), and every adaptive or global half proposed in this corpus - KPipe's only
  capacity control is a fixed in-flight watermark that pauses polling at 10,000.
- **Where KPipe is ahead, stated plainly:** a built-in dead-letter topic with a specified failure
  matrix (a failed DLQ send blocks the frontier rather than dropping the record); a single
  guarantees page stating the at-least-once boundary case by case, which PC does not have in one
  place; a typed pipeline with JSON, Avro, Protobuf and Schema Registry modules; jqwik property
  suites over the offset lifecycle; and a 21-class jcstress suite being ported to Fray under a
  written ADR, which is a tool decision this project should read before making its own.

**Its benchmark is a configuration ceiling, and the same lesson as llingr's.** The README claims
6.6x PC's throughput at 10 ms of work per record and 41x at 100 ms. The JMH harness pins PC 0.5.3.3
(the abandoned upstream artifact) at `maxConcurrency(100)`, `UNORDERED`, so PC lands at exactly
`workers / work-time` - its own capture predicts 1,000 records per second at 100 ms and measures
968. The setting dominates the engine, which is the finding
[`market-analysis-llingr.md`](market-analysis-llingr.md) section 5a already records from the other
direction. The same capture has `KEY_ORDERED` losing to PC's `KEY` mode at 1 ms on one of two
machines. Unlike llingr's harness it keeps a real broker in the path, which is why it was cheap to rerun.

**Rerun 2026-09-09 with the constant at 2000, and the claim inverts at 10 ms.** Same harness, same
box: PC 0.5.3.3 delivered 1.7x KPipe unordered and 1.6x key-ordered at 10 ms; at 100 ms PC sat at
91 percent of its new ceiling, 2.6x behind KPipe unordered and 1.8x ahead key-ordered. The
100-worker control reproduced KPipe's published numbers within 15 percent. The dated record with
method and raw JMH output is `docs/plans/2026-09-09-001-investigate-kpipe-benchmark-rerun.md` on
astubbs/parallel-consumer#367. Not run: the fork's artifact, and anything above 2000 workers.
