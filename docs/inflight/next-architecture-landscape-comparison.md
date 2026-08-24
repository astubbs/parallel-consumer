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
  **Correction, from building the Kafka Streams PoC (2026-08-23): Beam does not pass function
  references, it passes the function.** A `ParDoPayload` carries a `FunctionSpec` whose URN is
  `beam:dofn:pythonsdk:0.1` and whose payload is a **pickled DoFn**; the harness deserializes and
  runs it. That is materially different from the token-and-local-lookup this project uses, and the
  difference is forced by distribution: Beam's harness may run on another machine, so the code has
  to travel, which is where Python's pickling constraints on closures and unpicklable resources
  come from. Our harness *is* the host process, so the function never moves and no serialization
  constraint exists. We are strictly less capable (cannot distribute the harness) and strictly more
  ergonomic (capture anything). Do not describe the two as the same mechanism.

- **PyFlink and PySpark** — the closest *shipped* analogues to wrapping a JVM engine from another
  language, and worth more attention than they have had here, because they are the same shape as
  the language-proxy work rather than merely adjacent to it. Both split control plane from data
  plane and answer them differently:
  - **Control plane: Py4J in both.** A generic reflective Java-object-proxy bridge over a local
    socket - the Python side holds proxies and calls arbitrary Java methods to build a DAG or query
    plan. This is a handle model, so it is the same *family* as our builder-call replay, but it is
    reflective and untyped where ours is an explicit versioned schema. The consequence that matters:
    **Py4J requires a JVM at the far end forever and can never become a C ABI**, because it is
    Java-reflection-shaped. Our messages are transport-shaped, which is the whole reason the FFI
    path is even possible.
  - **Data plane, and this is the finding worth acting on: both moved off per-record IPC.** PySpark
    pipes rows to forked Python workers and its answer to the cost was **Arrow-batched vectorized
    UDFs**. PyFlink's process mode uses Beam's Fn API over gRPC, and
    [FLIP-206](https://cwiki.apache.org/confluence/display/FLINK/FLIP-206:+Support+PyFlink+Runtime+Execution+in+Thread+Mode)
    added a **thread mode** where PEMJA embeds the Python interpreter in the JVM process and they
    call each other through C - no IPC at all.
  - **Both escape routes we are considering have already been taken by someone bigger, which is
    validation rather than a warning.** PEMJA's thread mode is our FFI/embedded path; Arrow
    batching is the bundling question. Neither project chose one and stopped - Flink kept process
    mode for isolation. That is the shape of the decision, and it says the answer is probably
    "both, selectable" rather than "pick one".

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
untouched** — which is why none of them scores what this does, and why they are complements rather
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
