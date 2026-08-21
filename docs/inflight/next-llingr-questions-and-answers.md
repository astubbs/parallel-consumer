# llingr: questions asked, and the answers

<!-- inflight-type: register -->

Questions raised while reviewing [`market-analysis-llingr.md`](market-analysis-llingr.md), with the
answers, so neither is lost. **Refer to the product as `llingr`, never as a person** - see the
convention note in the competitor file.

---

## "Relay is announced, not shipped - the polyglot race is level, not lost." Explain.

`llingr-relay` is the polyglot story: a container holding the broker adapter, the engine and a gRPC
bridge, so a handler can be written in any gRPC language. The website documents it in detail - five
service methods, guarantees preserved across the boundary, a language table, a Helm chart in the page
title.

**But it does not exist yet as an artifact.** The docs list `llingr-grpc-kafka-bridge (coming soon)`,
and the GitHub org has twelve repositories, none of them the relay or bridge. No proto file is
published either.

So the comparison to astubbs#242 is: **both are designed and documented, neither is released.** An
earlier revision of this note said llingr's was "shipping" and PC's was "unreleased", which framed a
level race as a lost one. What llingr does have shipped is the *native* language coverage - Go, a JVM
build and a Rust FFI binding - which is a different axis from the sidecar.

---

## Where does this project actually stand? (opinion, asked for)

**Much stronger than it felt before the analysis - and the analysis is *why*, not despite it.**
Avoiding the competitor would not have saved the stress; it would have preserved it. What hurt was an
unmeasured assumption ("he is successfully commercialising it") that the evidence does not support.
Looking is what dissolved it. Worth remembering the next time something looks threatening from a
distance.

**Where PC is strong, in rough order of how hard the advantage is to copy:**

1. **The offset map.** Committing past gaps, encoded into commit metadata, surviving restart. It is
   the hardest thing in this problem space and llingr has not attempted it. Everything downstream -
   retries, a slow record not blocking a partition, a rebalance not redoing completed work - follows
   from it.
2. **Scheduling machinery.** Fair traversal, per-shard state, retry scheduling. This is what makes
   ordering modes, retry delays and future per-topic fair shares *expressible*.
3. **Years of production contact.** Sixteen substantive feature advantages, and they are not
   arbitrary: retries, produce-and-commit, transactions, batching, multi-topic, ordering modes. That
   list is what real failure reports turn into over five years.
4. **Apache-2.0, no patent claim, no licence key.** For a large share of enterprise evaluators this
   settles the question before any benchmark.
5. **JVM ecosystem depth** - Streams and Connect previews, native reactive integrations.

**Where PC is weak, and it is a different axis:**

1. **Nothing above is *stated* anywhere a prospective user reads.** The offset map is the biggest
   differentiator in the whole comparison and it is explained nowhere. That is the single largest
   gap.
2. **No evidence practices.** No formal verification, no published per-message overhead, no asserted
   performance budget. A five-year 35% regression shipped unnoticed.
3. **Per-record overhead** - measurably behind, though it shrinks to ~4% at realistic latency.
4. **Missing features that are on the roadmap** - DLQ most visibly.

**The honest summary: PC leads on capability and trails on credibility, and only one of those is
being worked on.** The roadmap is well aimed at capability gaps and aimed at nothing else. A
competitor three months old with 19 commits has better published evidence than a library with years
in production - not because the engineering is better, but because it was written down.

**On the competitive threat specifically: small, early, and not the thing to organise around.** Five
stars, one author, no coverage, no published prices, no live purchase channel, an unverifiable JVM
build and an unshipped relay. It is a serious, well-engineered project by someone who clearly knows
what they are doing, and it is three months old. The useful output of this analysis was never a
threat assessment - it was a list of things PC does that nobody has been told about, and a set of
evidence practices worth adopting.

## Why not benchmark llingr's Java client instead of the Go one?

**Because it is not obtainable.** The JVM build is `com.llingr:llingr-demux:0.1.0`, distributed only
through a private Google Artifact Registry that returns **401 to anonymous requests**; there is
nothing under `com.llingr` on Maven Central; and **no JVM repository exists in the GitHub org.** The
purchase route is a "Marketplace (coming soon)" button.

So the only inspectable, runnable llingr is the Go engine. That is also why every claim about the JVM
edition - native Kotlin implementation, full feature parity, shares the formal verification - is
recorded in these notes as *claimed* rather than *verified*.

**And that is why the `franz` control arm matters.** Comparing PC (Java client) against llingr (Go +
franz-go) mixes two variables. A franz-go-only arm isolates how much of the difference is the Kafka
client rather than the engine - the Go-side equivalent of the `vanilla` KafkaConsumer arm.

## Are the announced dates slipping?

Commit activity in `llingr-demux`, from the GitHub API on 2026-08-21:

| Month | Commits |
|---|---|
| 2026-05 | 6 |
| 2026-06 | 4 |
| 2026-07 | 9 |
| 2026-08 | **0** |

Last commit to the engine: **2026-07-22**. To `llingr-rs-kafka`: 2026-07-29. To `llingr-nexus`:
2026-07-15. (The August `pushed_at` timestamps move on any ref activity, including a tag, and are not
commits.)

The July commitments - **Python FFI (Jul 2026)** and the **Java relay adapter (Jul 2026)** - have not
appeared, and neither has a repository for them.

**Do not over-read this.** A month of quiet for a solo founder in August is as likely to be a holiday
as a stall, and **JVM work would be invisible anyway** because that build has no public repository. The
factual statement is: no public commits for about a month, and two dated deliverables have passed
without shipping.

## Which language clients actually exist, and which are announced?

"Relay" is llingr's name for the gRPC bridge - a container packaging the broker adapter, the engine
and a gRPC server-side bridge, so a handler in any gRPC language can be driven by the Go engine. It is
the same idea as astubbs#242's sidecar.

**Shipped, verifiable:**

| | What it is | Evidence |
|---|---|---|
| **Go** | The engine itself | Public repo, on the Go module proxy, v0.12.2 |
| **Rust** | **FFI binding over the Go engine**, not a second engine | `llingr-rs-kafka` on crates.io (0.10.6); its `bridge/go.mod` requires `llingr-demux` |

**Claimed but not inspectable:**

| | Status |
|---|---|
| **JVM** (Java, Kotlin, Scala) | A *native reimplementation*, not a binding. **v0.1.0**, behind a licence-key Maven registry (anonymous fetch returns 401), nothing on Maven Central, **no repository in the GitHub org.** Every claim about it is self-reported |

**Announced, with dates, not shipped:**

| | Announced for | Repo exists? |
|---|---|---|
| **Relay / gRPC bridge** | "coming soon" | **No** |
| **Python** (in-process FFI) | Jul 2026 | No - nothing on PyPI |
| **C/C++** (in-process FFI) | unstated | No |
| **Java relay adapter** | Jul 2026 | No |
| **C# adapter** | Sep 2026 | No |
| **NATS adapter** | "planned" | No |
| **Pulsar adapter** | "planned" | No |

**So the polyglot story today is: Go, plus Rust over the Go engine, plus an unverifiable JVM build.**
Everything that would make it genuinely polyglot - the relay, and the Python/C/C++/C# routes - is
announced. Several dates have already passed without a repository appearing.

For comparison, astubbs#242 has **eleven** client implementations driven through a shared conformance
suite, unreleased. Different bet: llingr shipped two native paths and announced the rest; PC built the
breadth first and has not shipped it.

## What does dialling out buy llingr?

Worth answering fairly, because it is a real trade rather than a mistake.

- **llingr:** the relay holds the consumer and **dials out** to the user's application, which runs a
  gRPC **server**.
- **PC (astubbs#242):** the sidecar **is** the server; the user's application dials in as a client.

**What llingr's direction buys:**

1. **The user writes a server, which is the better-supported shape.** Every language has a mature gRPC
   server framework with codegen, interceptors and testing tools. Writing a *client* that must
   reconnect, back off and manage a long-lived bidirectional stream is more work and more places to
   get it wrong.
2. **Connection lifecycle lives in the engine.** The relay owns retries and backoff - documented as
   4 attempts, 25ms to 1s exponential - so no user-language code implements reconnection. In PC's
   direction that logic is duplicated in all eleven clients, which is real surface area.
3. **It matches Envoy `ext_proc`**, where Envoy is the client and the external processor is the
   server - a well-trodden sidecar pattern.
4. **Liveness falls out naturally**: the relay heartbeats and the handler detects a dead relay by
   missed heartbeats.

**What PC's direction buys:**

1. **It works when the application cannot accept inbound connections** - the reason Beam chose it, and
   the strongest argument. Restrictive egress-only environments, no listener, no port to expose.
2. **The application need not be a server at all.** A short Python script can connect, process and
   exit. Under llingr's shape that script must stand up a gRPC server and stay listening.
3. **The application owns its own lifecycle**, which matches astubbs#242's settled model - the app
   starts the sidecar and the client library spawns the workers, so the engine never has to know how
   to start the user's program.
4. **No inbound port to secure.** The handler exposes nothing.

**Verdict: neither is wrong, and the trade is deployment topology.** llingr's is more convenient in a
Kubernetes sidecar on localhost; PC's is more deployable when the two sides are separated or the app
cannot listen. Worth stating explicitly in astubbs#242 as a considered choice, with Beam cited, rather
than leaving it to look accidental.

## Other brokers: "NATS and Pulsar are planned, no adapter repos."

Same shape. The adapter table marks `llingr-adapter-nats` and `llingr-adapter-pulsar` **planned**, and
neither repository exists. llingr ships Kafka only, via two client adapters.

Analysis of whether PC should ever support other brokers now lives in
[`next-other-brokers.md`](next-other-brokers.md), including the NATS JetStream research.

---

## "No retry policy at all - an error goes straight to dead-letter." Are you sure?

**Yes, and it is confirmed from source rather than from documentation.**

- The handler contract is `func(ctx, msg) error`. The worker call site is a single
  `err = w.processMessage(workItem.Ctx, workItem.Message)`.
- A non-nil error routes the message to the dead-letter handler. There is no retry counter, no
  backoff, no delay, no attempt cap anywhere in the engine.
- Both required callbacks are `processMessage` and `writeDeadLetter`, and the Rust docs state the
  reason the dead-letter handler is mandatory: *"Required so a failed message always has somewhere to
  go."*
- The circuit breakers are shutdown valves on different conditions entirely - failure to acquire a
  worker within a timeout, failure to complete a rebalance drain, and failure of the *dead-letter
  write itself*. None of them retries the user's function.

**Why this is a coherent design rather than an oversight:** with a contiguous-commit committer, a
message that keeps failing would hold the commit point forever. Dead-lettering immediately is what
keeps the commit point moving. PC can afford retries precisely because it commits past the gap - the
failing record sits in the incomplete set while everything else commits.

**So the user must implement retry inside the handler**, and any in-handler retry blocks that key's
worker and holds a concurrency token for its duration.

---

## "The relay inverts PC's proxy direction - that's the shape Beam rejected." Explain.

Two ways to arrange a sidecar:

- **Engine as server, handler dials in** (PC's astubbs#242 design): the sidecar listens; the user's
  application connects to it.
- **Engine as client, handler listens** (llingr's relay): the relay holds the consumer and **dials
  out** to the user's application, which implements the gRPC service. Their Python example implements
  a `KafkaBridgeServicer` - servicer being gRPC's word for server - and their liveness text says
  *"The client sends periodic heartbeats. The server can detect consumer disconnects"*, where client
  = relay and server = the user's handler.

**Apache Beam's portability framework faced the same choice and went the other way**, for a stated
reason recorded in `next-architecture-landscape-comparison.md`: the runner is the server and the SDK
harness dials out, *"because runners often sit where they cannot accept inbound connections"*.

The practical difference is deployment topology. If the engine dials out, the user's application must
be reachable and accept inbound connections - fine as a Kubernetes sidecar on localhost, harder when
the two are separated by a network boundary or a restrictive egress policy. Neither is wrong; PC's
choice matches Beam's and llingr's does not, and the trade is worth stating deliberately in astubbs#242
rather than being discovered later.

---

## "A published per-message overhead" - published where?

On llingr's Technology page: **~1.26µs per message**, described as *"Coordination cost of ~1.26µs on
x86_64 consumer grade CPU subject to thermal throttling. No simulated latency: shows worst-case, fully
contended coordination costs when workers and pipelines are saturated with messages."* There is a
chart beside it, and the benchmarks page points at a public benchmarks directory.

"Published" means stated with its measurement conditions on a public page - not peer-reviewed and not
verified by us. **The point for PC is not the number, it is the practice**: PC has never measured its
own per-message coordination overhead as a figure it could quote. See
[`next-performance-regression-testing.md`](next-performance-regression-testing.md).

---

## NATS: unordered parallelism without a partition ceiling - but what about key ordering?

Correct on both halves.

- **Unordered:** any number of workers can bind to one JetStream pull consumer and share its cursor,
  each acknowledgement advancing it. No partition-count ceiling. So the "I need more concurrency than
  partitions allow, order does not matter" case - a large part of PC's `UNORDERED` value - **is
  already solved natively on NATS.**
- **Key-ordered:** the native mechanism for serial processing is `MaxAckPending=1`, which serialises
  *everything* matching the consumer's filter rather than per key. That is worse than Kafka, where
  partition count at least buys some ordered parallelism.

So the PC-shaped problem exists on NATS only for the key-ordered case. Full analysis in
[`next-other-brokers.md`](next-other-brokers.md).

---

## What is Synadia, what is `pcgroups`, and why does the Java community size matter?

**Synadia** is the company that created and maintains NATS - the commercial entity behind the project,
the way Confluent stands behind Kafka. Relevant beyond trivia: in April 2025 Synadia attempted to
withdraw NATS from the CNCF, reclaim the donated trademark and relicense the server under the Business
Source Licence; CNCF resisted and prepared to fork, and it settled on 1 May 2025 with the trademarks
assigned to the Linux Foundation and NATS staying Apache-2.0. A single-vendor-capture risk that
materialised once already.

**`pcgroups`** is Synadia's "partitioned consumer groups" library, in their Orbit collection. It
combines two server features - deterministic subject partitioning by hash, and `pinned_client`
priority groups that designate one active worker per partition with failover - to give per-key ordered
parallelism. Its own documentation states the ceiling: *"effective parallelism is
`min(partitions, members)`"*, and that pinning is *"affinity plus failover, not a distributed lock"*,
so handlers must be idempotent.

**So the gap PC would fill on NATS is already occupied** - by the vendor, in Go, Rust, JavaScript
**and Java**.

**Why the Java community size matters:** it sizes the addressable market for a *JVM* library. PC is a
JVM library; a NATS port would be a JVM NATS library. `nats.java` has ~675 GitHub stars against
~6,727 for `nats.go`, and Synadia's own `orbit.java` has 8 stars against 91 for `orbit.go`. So NATS
users are overwhelmingly not JVM users, which means a JVM parallel-consumer for NATS would serve the
small overlap of two already-small sets: NATS users who are also JVM users who also need *key-ordered*
concurrency and are not served by the vendor's own library. That is the argument for "not any time
soon", and it is about market size rather than difficulty.

---

## llingr is very new

Confirmed: company incorporated 2026-05-08, GitHub org 2026-05-16, engine repo 2026-05-18. Five stars
on the engine, 19 commits, one author, no third-party coverage, no published prices, purchase channel
"coming soon". **Roughly three months old in public.**

Two consequences worth holding together, because they point in opposite directions and both are true:

- **The roadmap surface is large and mostly unbuilt.** The relay, NATS, Pulsar, Python/C++ FFI, the
  marketplace, a C# adapter - all announced, none shipped. A great deal is intended.
- **PC's advantages are not luck.** Multi-topic, retries, ordering modes, poll-and-produce,
  transactions, reactive integrations and the offset map are the accumulated result of years of
  production contact with real failure modes. Those are the parts that are expensive to acquire
  quickly, whatever the tooling.

---

## "Steal" was the wrong word

Agreed, and the notes have been corrected. **Inspired by**, or **learn from**. The techniques being
looked at - formal verification, per-message hashes with an end-to-end validator, budgets asserted as
tests - are general engineering practice, not anyone's property. The framing matters because these
notes should stay usable if anyone else reads them.

---

## Positioning: state our strengths, do not attack llingr

**Recorded as a rule.** Every advantage found here should be marketed as *what PC does*, never as
*what a competitor lacks*:

- Not "llingr has no unordered mode" -> **"Three ordering modes: per partition, per key, or
  unordered. Pick the guarantee your workload actually needs."**
- Not "llingr has no retries" -> **"Retries with configurable delays, custom backoff and a
  max-retry escape - a transient downstream failure does not become a dead letter."**
- Not "llingr cannot produce" -> **"Read, process and produce in one step, with the produce tied to
  the offset commit - including transactional exactly-once."**
- Not "llingr is single-topic" -> **"Subscribe to many topics, or a pattern."**
- Not "llingr's commits stall behind one slow key" -> **"Commits move past in-flight work: one slow
  record never holds up everything behind it, and a rebalance does not redo the work that finished."**

The last one is the strongest and it is currently invisible in our documentation. See
[`next-landing-page.md`](next-landing-page.md).

**No public comparison against llingr**, per the standing decision. These notes are internal.

---

## Is the pre-commit cache bounded or not?

**Not bounded by configuration** - which is the correction. The gap buffer is a plain Go slice with a
bare `append` and no length check: no blocking, no drop, no backpressure, no circuit-breaker trip.

**Bounded by memory, obviously** - as you say, and that is a large bound. Each buffered entry is a
`*WorkItem` pointer, so a partition sitting far behind its commit point costs pointers plus the retained
messages, not offsets alone. With tens of gigabytes available this absorbs a great deal before it
matters, which is exactly why the failure is *silent*: nothing complains until either the operator
notices the gauge or the process dies.

The practical difference from PC: PC's incomplete set is **encoded into commit metadata**, so it is
bounded by design, survives restart, and does not retain the messages.

---

## `CommitPartitionSliceLen` is documented, validated, and inert. Explain.

A config field that does nothing:

- `CommitPartitionSliceLen` is declared in the config struct, defaulted to 400, and **validated** -
  values below 50 or above 2,000 panic at startup.
- Their config README gives tuning advice for it: *"pre-allocates space for tracking message gaps
  within each partition... consider increasing this to 800-1000"*.
- **The committer never reads it.** It sizes its pre-allocation from `ConcurrentKeys * 3 / 2` instead
  - 375 at defaults, not 400. Repo-wide, the only non-test references to the field are its
  declaration and its validator.

So a user can set it, be rejected for setting it wrongly, follow published advice about it, and change
nothing at runtime. It is a hint that is validated but disconnected.

**The transferable lesson for PC** is not that llingr has a bug - it is the shape of the bug. A
configuration option whose *only* references are its declaration and its validation is exactly the
defect a "does every option reach a consumer?" check would catch. PC has 30+ options and no such
check.

---

## What is Envoy, and what is `ext_proc`?

**Envoy** is a network proxy - the one most service meshes are built on (Istio, Consul, AWS App
Mesh). It sits beside your service and handles inbound and outbound traffic.

**`ext_proc`** is Envoy's External Processing filter. It lets you inspect and modify requests and
responses in **your own** code, in any language, without writing an Envoy plugin in C++. Envoy opens a
**bidirectional gRPC stream to your service** and streams request/response pieces across it; your
service replies with instructions.

**Why it is relevant here:** in `ext_proc`, **Envoy is the gRPC client and your processor is the
server.** That is the same direction llingr's relay chose. So llingr is not doing something unusual -
it matches a widely deployed sidecar pattern - while PC's astubbs#242 matches Beam's, which chose the
opposite for its own stated reason. Both patterns are load-bearing in production systems; they differ
on who must accept a connection.

## How does a gRPC server inside the foreign client actually work, and what would PC trade?

**How llingr's arrangement works.** The relay container holds everything stateful: the Kafka client,
the engine, the worker pool, the offset committer. The user's application holds only handler code and
runs a gRPC server implementing five methods (`ProcessMessage`, `WriteDeadLetter`, `SendMetrics`,
`NotifyShutdown`, `Heartbeat`). The relay dials that server and calls `ProcessMessage` per record; the
handler's **return** is the completion signal, and the offset commits only after a successful
response. Concurrency comes from the relay making many concurrent gRPC calls - the handler's gRPC
server naturally serves them in parallel, so the engine's `ConcurrentKeys` becomes "how many calls are
outstanding".

**The engine keeps all work management.** Nothing about the direction changes that - ordering,
retries (such as they are), offset tracking and rebalance drain stay engine-side in both designs.
The direction only decides **who opens the socket.**

**What PC would trade by switching to server-in-the-client:**

| | Gained | Lost |
|---|---|---|
| Client authors | A gRPC **server** per language - better tooling, codegen, familiar testing | Every client must now *run a server*, including short scripts |
| Reconnection | Owned once, in the sidecar | - |
| Lifecycle | - | **The app-owns-lifecycle model.** astubbs#242's settled design is that the app starts the sidecar and the client library spawns workers using its own language's mechanism. If the engine dials in, the sidecar must know the handler's address before the handler exists - an ordering problem the current design does not have |
| Deployment | Matches Envoy `ext_proc` | Needs a listening port and a reachable address |

**The point you make about the socket shortcut is the decisive one.** astubbs#242 already plans a
loopback/UDS path with a single-connection guard and an authority allowlist - the sidecar accepts
exactly one connection from the process that spawned it. In that arrangement *"the app cannot accept
inbound connections"* is indeed not the interesting argument, because both sides are on the same host
by construction.

**So the real trade is lifecycle, not networking.** PC's direction follows from "the app spawns the
sidecar, so the app connects to it" - which is what removes the bind-race election and the detached
process group from the design. llingr's direction follows from the relay being an independently
deployed container that must find its handler. **Both are internally consistent; each falls out of who
owns process startup.** That is the sentence that belongs in astubbs#242.

## Is 400 a limit on how far ahead of the committed offset you can be?

**No - and the distinction matters.** `CommitPartitionSliceLen` (default 400) is a **pre-allocation
capacity hint for a Go slice**, not a cap. Even if it were read, exceeding it would grow the slice
rather than reject work. And it is not read: the committer pre-allocates `ConcurrentKeys * 3 / 2`
instead.

So llingr has no configured ceiling on how far the commit point may lag - just as you say, memory is the
only bound. What the validated 50-2,000 range does is constrain a number that reaches nothing.

**How far ahead can PC be?** Structurally, very far - and the binding constraint is completely
different. PC does not buffer completed work at all; it **encodes the incomplete set into commit
metadata**, so the limit is the size of that encoded payload against the broker's metadata limit
(`offset.metadata.max.bytes`, 4 KiB by default). Because the encodings are run-length and bitset
based, **one stuck record with a clean contiguous block behind it compresses to a handful of bytes**.
In exactly the case you describe - a single stuck offset, everything after it complete - PC can be
hundreds of millions of offsets ahead of its committed offset, and the payload barely moves.

The payload grows with the **number of discontiguous runs**, not the distance. Many scattered
incompletes is the expensive shape, which is what the offset-encoding density work
(`astubbs#192`) exists to manage.

**That is the sharpest form of the architectural difference:** llingr's cost scales with *how far
ahead* it is, in retained messages; PC's scales with *how fragmented* the incomplete set is, in
compressed metadata. And PC's survives a restart, because it is on the broker.

## `DESIGN.md` says the buffer "grows without bound". Explain.

llingr's own design document, describing the worst case when a handler is stuck:

> *"Commits stop, every completion joins the buffer, and the buffer grows without bound - a silent
> stall that only a rebalance would clear."*

and the README's operator guidance:

> *"One important alert worth considering could be on a partition's `gapBufferDepth` growing without
> bound: it is the sign of a stuck handler invocation."*

The mechanism: one key's handler blocks forever. Its offset never completes, so the contiguous
commit point cannot pass it, so commits for that partition freeze. Meanwhile every *other* key on that
partition keeps completing - and because each of those completions is ahead of the stuck offset, each
one joins the gap buffer instead of advancing the pointer. Work continues, memory grows, and the
committed offset stays still.

**Worth crediting: this is documented candidly in the repository**, with an operator alert
recommended. It is the marketing that omits it, not the engineering.

---

## Fairness and head-of-line blocking at admission. Explain.

The one to understand properly, because it is the product's own core claim reappearing one level up.

**The design intent** is that a slow key blocks only itself: keys hash to workers, each worker handles
its own key serially, other keys proceed. That works *once a message has been routed to a worker*.

**The problem is what happens before routing.** From `Processor.Process`, simplified:

```go
select {
case p.guard <- struct{}{}:               // acquire a concurrency token FIRST
    p.demux.SendToWorkerForProcessing(key, workItem)   // only then look at the key
default:
    // wait up to AcquireWorkerTimeoutCircuitBreaker (default 1 minute)
    // then EMERGENCY SHUTDOWN
}
```

The token is acquired **before** the key is examined. There are `ConcurrentKeys` tokens (250 by
default), and a token is held for as long as its handler runs.

So: 250 slow handlers are in flight, each holding a token. The next polled message - **whatever key it
belongs to**, including a key with an idle worker sitting ready - cannot get a token. And the polling
loop is a single goroutine calling a blocking `Process`, so **the entire consumer stops** behind that
one message. If no token frees within a minute, the circuit breaker fires and the process shuts down.

**That is head-of-line blocking**, on the admission path rather than the partition. The product exists
to remove exactly this, and the guard-before-route ordering reintroduces it at a different layer.

### Terms: polling goroutine, broker order, one queue, no scheduler

**"One polling goroutine"** - llingr has a single Go thread whose whole job is: call `Poll`, take the
records, hand each one onward, repeat. PC's `BrokerPollSystem` is the direct analogue and is also a
single thread, so **this part is the same in both**. The difference is what happens next.

**"Broker order"** - the polling loop processes records in exactly the order the broker returned
them, one at a time, with no reordering and no buffering into per-key queues *before* dispatch. The
order records arrive is the order they are admitted.

**"One queue"** - there is only one place work waits: the poll loop itself. A record is either
already handed to a worker, or it is the one record currently being admitted, or it has not been
polled yet. **There is no pool of eligible work sitting in the engine waiting to be chosen from.**

**"No scheduler to ask"** - a scheduler is only needed when several things are eligible and one must
be picked. In llingr nothing is ever eligible-but-waiting: the next record is simply the next record.
So there is no point in the code where a policy could be applied - no place to say "serve this topic
first", "this key is behind, catch it up", or "this shard has waited long enough". **Not a missing
feature so much as a missing seam.**

**"astubbs#236 has nowhere to live"** - that issue asks to prioritise some topics over others when
subscribed to several. Implementing it requires a moment where the engine holds work from topic A and
work from topic B and chooses. llingr has no such moment: one consumer serves one topic, and if you
run several consumers the only shared thing is a burst-capacity channel whose own FAQ calls it *"an
overflow channel, not a scheduler"*. So llingr would have to build the seam first.

### Why does llingr avoid needing this, and PC does not?

**Because PC buffers eligible work and llingr does not.** That single difference produces everything
else.

- **PC pulls work forward.** `WorkManager` holds many shards, all with records ready, and each control
  loop iteration asks for the next N units across them. Many things are eligible at once, so PC
  *must* choose, and choosing badly starves someone. Hence `LoopingResumingIterator`, per-shard
  state, fair traversal as a published guarantee, and a place to hang retry scheduling.
- **llingr pushes one at a time.** The poll loop takes a record, blocks until a concurrency token is
  free, dispatches it, takes the next. Nothing accumulates engine-side, so nothing competes, so
  nothing needs choosing.

**Both designs pay for their choice, and the bills come due in different places.**

llingr's bill is the head-of-line problem: because admission is one-at-a-time and takes a token
*before* looking at the key, one message that cannot get a token stops **everything**, including keys
with idle workers. And it cannot express any policy.

PC's bill is complexity, and the buffer it maintains has to be bounded and fair - which is exactly
what `messageBufferSize`, the load factor and the fair iterator are for. **This session found PC
paying that bill badly in one place:** `ExternalEngine` pins the load factor at 1, so Vert.x, Reactor,
Mutiny and the proxy get the complexity of the buffered design with none of the buffer.

**The trade in one line: llingr avoided the scheduling problem by never having eligible work waiting,
and inherited head-of-line blocking at admission plus no way to express priority. PC took the problem
on, solved it, and can therefore offer ordering modes, retry delays, and per-topic fair shares when
someone builds them.**

### "Fairness is incidental" versus deliberate machinery - the difference in full

**llingr's fairness is a side effect of having only one waiter.** A single polling goroutine takes
messages in broker-delivery order and hands each to a worker. Nothing chooses between contending keys
because nothing ever contends - there is one queue and one consumer of it. So no key is *starved* in
the strict sense, and equally no key can be *favoured*, deferred, or caught up. There is no scheduler
to ask.

That has two consequences. **It cannot express a policy** - no priority, no weighting, no aging, so
astubbs#236's "prioritise some topics over others" has no place to live. And **it degrades badly under
contention**, because the only thing standing between a full worker pool and a stalled consumer is a
timeout that ends in shutdown.

**PC's fairness is machinery, because PC has a real scheduling problem.** The work manager holds many
shards - per key or per partition - all eligible at once, and the control loop must decide *which to
take from* each time it fetches work. That decision is where starvation lives, and PC has explicit
answers:

- **`LoopingResumingIterator`** - the shard traversal resumes from where the last pass stopped rather
  than restarting at the beginning, so a shard near the front cannot be served repeatedly while one
  near the back waits. It is the classic round-robin-with-resume-point, and it exists because the
  naive version starved real users.
- **Fair shard and partition traversal** is a **published feature** with its own record
  (`docs/features/fair-partition-traversal.yaml`, `production-use`) - it is a stated guarantee, not an
  implementation detail.
- Retry scheduling, the retry queue's ordering, and per-shard state all hang off the same structure,
  so delay and backoff are expressible.

**The asymmetry to hold on to:** llingr avoided needing a scheduler by routing at admission, and paid
for it with the head-of-line problem above. PC took on the scheduling problem and solved it, which is
why it can offer ordering modes, retries with delays, and - when someone builds it - per-topic fair
shares. **The machinery that looks like complexity is what makes the feature list possible.**

---

## Offset gaps: what does PC do? Do we rely on contiguous offsets?

**Your intuition was right, and it is better than "not a problem" - it is a documented design
decision with the reasoning written into the code.** From `PartitionState.incompleteOffsets`:

> *"**How does this handle gaps in the offsets in the source partitions?** We track per record
> acknowledgement, by only storing the offsets of records **OF WHICH WE'VE RECEIVED** through
> `KafkaConsumer#poll` calls. This is as explicitly opposed to looking at the lowest offset we've
> polled, and synthetically creating a list of EXPECTED offsets from the range from it to the highest
> polled... usually due to compaction. Instead, the offsets tracked are only determined from the
> records we've given to process from the broker - we make no assumptions about which offsets exist.
> This way we don't have to worry about gaps in the offsets. Also, a nice outcome of this is that **a
> gap in the offsets is effectively the same as an offset which has succeeded** - because either way
> we have no action to take."*

The mechanism, in one line: `getOffsetHighestSequentialSucceeded()` returns
**lowest-incomplete-minus-one**, not "scan upward from committed until a hole appears". A missing
offset is not in the set, so it is invisible to the calculation. `incompleteOffsets` is a sparse
`ConcurrentSkipListMap` keyed by real offsets, and the only way in is a genuinely polled record.

**The encoders agree.** `OffsetSimultaneousEncoder` walks a range but the per-offset decision is a
set-membership test, so a compacted-away offset is encoded as **completed**. From `BitSetEncoder`'s
javadoc: *"if offset 8 is missing from the partition, we will encode it as having been completed (when
in fact it doesn't exist)... we are assured we will never see record 8."*

**All three gap causes are handled, and two deliberately rather than incidentally:**

| Cause | Handling |
|---|---|
| **Log compaction** | `maybeTruncateOrPruneTrackedOffsets`, run on every poll batch - `confluentinc#409`, shipped 0.5.2.4, with a feature record and a real-broker integration test that compacts mid-run |
| **Control records / transaction markers** | `OffsetSimultaneousEncoder.maybeRaiseOffsetHighestSucceeded` - `confluentinc#329`. Without it the encoder computes a **negative** length and throws, because markers push the next-expected offset more than one above the highest succeeded. Covered by `TransactionMarkersTest`, six methods |
| **Aborted transactions under `read_committed`** | Nothing needed - the client filters them before `poll()` returns, so they never enter tracking |

So **open item 10 on the register is resolved**: all three are handled, and the two we were unsure
about are deliberate.

### But the investigation found one real edge case, previously unknown

The per-batch prune that removes phantom incompletes is bounded by **the current batch's own range**:

```java
incompleteOffsets.keySet().subSet(offsetOfLowestRecord, true, offsetOfHighestRecord, true)
```

An offset that is tracked as incomplete from decoded metadata, has since been compacted away, is
**not the lowest** incomplete, and sits **exactly between the last record of one poll batch and the
first record of the next**, falls outside both windows and is never pruned.

Worked example: incompletes `{100, 500}`, committed 100, offset 500 compacted away. Bootstrap poll
returns `100..499`, so the bootstrap truncate does not fire. Next poll returns `501..600`.
`subSet(100,499)` misses 500; `subSet(501,600)` misses 500. **Offset 500 is a permanent phantom**, and
once 100 completes the committed offset pins at 500 for as long as the partition stays assigned.

It is a **commit stall, not redelivery or loss**; the encoded payload stays small, so there is no
back-pressure; and it **self-heals on the next rebalance or restart**, because the bootstrap poll then
starts at 500 and prunes it. Low probability - it needs the hole to align with a fetch-response
boundary - and untested. Recorded as
[`bug-compacted-phantom-on-poll-batch-boundary.md`](bug-compacted-phantom-on-poll-batch-boundary.md).

**Comparison note, stated fairly.** llingr's approach to the same problem is different and also good:
each message carries the actual previously-delivered offset as a stamp, so contiguity is proved
against delivery rather than arithmetic. PC's approach - track only what was received, treat a gap as
success - reaches the same place from the other direction and has been in production for years.
Neither is a differentiator; both are correct.

## The memory-growth failure mode, in detail

Both projects have a "one stuck handler" story. The shapes differ, and the difference is the
positioning.

**llingr, with one permanently stuck key on a partition:**

1. Commits for that partition freeze at the offset before the stuck record. Correct behaviour - the
   commit point cannot pass an incomplete offset.
2. Every other key on that partition keeps completing, and each completion lands in the gap buffer
   because it is ahead of the frozen commit point. **Unbounded slice growth**, holding a `*WorkItem` per
   entry.
3. The stuck handler holds one concurrency token permanently.
4. When enough handlers are stuck to exhaust the 250 tokens, the polling loop blocks on admission -
   the head-of-line case above.
5. If no token frees within `AcquireWorkerTimeoutCircuitBreaker`, **emergency shutdown**.
6. On restart or rebalance, everything after the frozen commit point is redelivered and reprocessed,
   including all the completed work that was sitting in the buffer.

**PC, same scenario:** the stuck record stays in the incomplete set; every other record commits past
it because the incomplete set is encoded into commit metadata; the set is bounded and does not retain
messages; on rebalance the new owner reads the metadata and redelivers **only** the genuinely
incomplete records.

**Why the memory framing is stronger than the stall framing I first used.** A stall is loud - throughput
goes to zero and someone investigates. This is quiet: **throughput looks normal**, because every
unstuck key is still being processed at full speed. The only symptoms are a committed offset that has
stopped moving and a gauge climbing. Consumer lag looks *fine* if you measure it by processing rate;
it looks terrible if you measure it by committed offset, and those two disagree only in this failure.
The outcome is either an eventual restart that redoes hours of work, or an out-of-memory kill that
does the same thing less politely.

**The honest caveat for any comparison:** PC is not immune to a stuck handler - a stuck record blocks
its own key, holds a thread, and if the incomplete set grows large the encoded metadata can approach
the broker's limit, which is what the offset-encoding density work exists to manage. The difference is
that PC's tracking is bounded, encoded and durable, so completed work is not lost and the failure is
visible in the committed offset rather than in memory.

---

## Terminology and housekeeping answers

### "Confound"

A **confounding variable**: a second thing that changed at the same time as the thing you meant to
measure, so the result cannot be attributed to either. Used three times in these notes, each a real
one:

1. **Kafka client version.** The first regression sweep compared PC 0.3.0.2 (kafka-clients 2.5.1)
   against 0.6.0.0 (3.9.2). A throughput change could have been PC or the client. The `vanilla`
   `KafkaConsumer` arm - which touches no PC code - is the **control** that separated them: it moved
   4.2% while PC moved 31%, so the client was not the explanation.
2. **Logging configuration.** The unpinned harness compared arms that happened to have different
   logback configs. This one was *not* controlled at first and it manufactured a whole cliff.
3. **Kafka client, again, across languages.** PC uses the Java client; llingr uses franz-go. Nothing
   currently separates "llingr's engine is faster" from "franz-go is faster". The `franz` control arm
   now being added is exactly that control.

### How is the "200x faster" claim derived? (It is a tautology.)

**Answered from llingr's own FAQ, its benchmark harness and its raw CSV.** Two separable things are
being conflated, and only one of them is a measurement.

**The claim:** *"under default settings llingr-demux is 200x+ faster than conventional Kafka
consumers... adjustable beyond 2000x."*

**Their own derivation, from the FAQ:** *"Default settings provide 250x concurrency (the maximum is
5,000x concurrent workers), reliably (and conservatively) delivering 200x+ the throughput of a
conventional, sequential Kafka consumer."*

**So "200x" is `ConcurrentKeys` (default 250), rounded down for conservatism.** And "beyond 2000x" is
the 5,000 maximum. Three findings make this a restatement of a config default rather than a result:

1. **The latency term cancels algebraically.** Their efficiency formula is
   `theoretical = concurrent_keys x (1000 / latency_ms)`. A serial consumer is `1000 / latency_ms`.
   Divide one by the other and **the ratio is `concurrent_keys`, whatever the latency**. Choosing
   35ms or 100ms changes absolute throughput and cannot change the multiple.
2. **The serial comparator was never run.** Their published `benchmark_results.csv` has 540 rows.
   Exactly one has `concurrent_keys=1`, and its latency is 0 - so its theoretical and efficiency
   columns are the "N/A" sentinel. **There is no serial data point anywhere in the dataset.**
3. **There is no Kafka in the benchmark.** The harness constructs a `MockBroker` - an in-memory slice
   with an atomic index. No network, no fetch, no commit round-trip, no consumer group.

The one before/after pair that does exist - the "200x" demo screenshots - is **llingr at low
concurrency versus llingr at default concurrency**, not llingr versus a conventional consumer. The
giveaway is in the screenshots themselves: the "before" pane displays llingr's own engine internals
(gap buffers, commit ingest, overflow), and its commit-ingest capacity of 5,000 versus the after
pane's 25,000 decodes to `ConcurrentKeys` below 50 versus the default 250.

**But give credit where it is due, because this matters for our own honesty.** What the harness
*actually* measures is legitimate and unusually well documented: **coordination overhead, and
efficiency against ideal fan-out** - 93-99% of theoretical across 200 to 5,000 keys, with a stated
~1.26µs per message. The harness is public and re-runnable, the raw CSV is committed, the hardware is
specified, there is a 520-line methodology document, and it is explicit that the handler is a
`time.Sleep` and that broker and database costs are excluded. That is better evidence practice than
this project currently has.

**The honest form of their claim** would be: *"a serial consumer processes one message at a time; we
process 250 concurrently at 99% efficiency, so on an I/O-bound workload whose downstream can absorb
250x the load, you get about 250x."* Every conditional there is load-bearing and none is measured.

**Why this matters to us beyond scoring a point.** Your instinct - *"PC is 1,000,000 times faster in
the right circumstance; it is meaningless and comes off disingenuous"* - is exactly right, and it is a
trap we can fall into just as easily. **Any headline ratio PC publishes must name the baseline, the
workload and the concurrency**, or it is the same sentence. The fairness charter in
[`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md)
already says this; this is the worked example of what happens when it is not followed.

### What is llingr actually patenting?

**Now answered from the source, and the short version is: probably nothing patentable, and the
posture reads as deterrence.**

**What the repository says:** three words. `README.md` under "License & Copyright" reads
`Patent pending.` and that is the entire mention across 161 files. No number, no application number,
no jurisdiction, no filing date, no scope, no `PATENTS` file. Every licence file - the AGPL text, the
commercial licence reference, `COPYRIGHT`, `REUSE.toml`, the SPDX document - contains **no custom
patent language at all**; every "patent" hit is stock AGPL §11/§12.

**That asymmetry is the tell.** This is a repository that is otherwise meticulous about licensing
metadata: SPDX headers on every file, a REUSE manifest, a separate SPDX package document. Everything
legally load-bearing is documented to a standard. The patent claim is documented not at all.

**A structural point worth knowing:** AGPL §11 already obliges each contributor to grant recipients a
patent licence covering the version they convey. llingr is the sole copyright holder conveying this
code under AGPL - so AGPL users arguably already hold a patent licence for whatever the claims cover.
The patent's remaining function is aimed at commercial licensees and at clean-room reimplementers who
take the idea without the code.

**The strongest candidate, having read the engine:** the `previousOffset` **delivery stamp**. Each
record is stamped with the offset of the record *previously delivered on that partition*, and the
commit watermark advances only on stamp match, never on `n+1` arithmetic. It is a genuinely tidy idea
that addresses a real problem - Kafka's offset space is not dense - and it survives out-of-order
concurrent completion.

**And the prior art that answers it is ours.** PC solves the identical problem earlier by a different
route with the same effect: `incompleteOffsets` is populated only from records *actually delivered*,
so broker-side gaps never enter the set at all. Dated, public, and six years before llingr's
copyright:

| PC capability | Date |
|---|---|
| Initial commit | **2020-05-20** |
| Offset map | 2020-08-27 |
| Partition state tracking (`confluentinc#218`) | 2022-03-25 |
| **Transaction-marker gaps** (`confluentinc#328`/`#329`) | 2022-07-12 |
| **Compacted-topic support** (`confluentinc#425`, issue `#409`) | 2022-10-12 |

Plus ARIES write-ahead logging with `PrevLSN` (1992) and TCP with SACK (1981/1996), both of which
acknowledge a contiguous prefix while out-of-order pieces wait in a reassembly queue - structurally
the same idea, decades earlier.

**Everything else in the engine is textbook**, and their own Technology page lists most of it as
such: false-sharing padding (Disruptor, 2011), lock striping (`ConcurrentHashMap`, 2004), bitmask
shard selection, FNV-1a (public domain by its own file's admission), object pools (Netty `Recycler`),
channel-as-pool, token-bucket rate limiting (1980s), batched mutex acquisition (group commit, 1980s).
Their "circuit breaker" is not even one - no half-open state, no failure-rate window; it is a one-shot
trip latch.

**Verdict, as engineering analysis and explicitly not legal advice:** there is one idea here that is
not nothing, and it is not novel enough to survive being shown the right prior art - the difficulty
being that PC is not art an examiner finds by keyword search, which is how unexamined "patent
pending" claims survive. The real commercial moat is the AGPL/commercial dual licence, which is what
actually stops a SaaS competitor. The patent line reads as a second deterrent aimed at clean-room
reimplementation.

**The single most useful thing to preserve is the dated prior-art record above.** If this ever matters
commercially, that table is the fact to hand a solicitor.

### Historical note on the earlier answer

What is known: every page footer says "Patent pending", the JVM page ties it to *"the performance this
patent-pending technology makes possible"*, and copyright is assigned to Ordologica Ltd. **No patent
number, no application number, no claims text is published**, and pending applications are typically
unpublished for 18 months - so from May 2026 there may be nothing public to read until late 2027.

What is certainly *not* novel: hash-key-to-worker fan-out, a contiguous-commit frontier, and
key-ordered concurrency past partition count. **Parallel Consumer is public prior art from 2020**, and
the general technique predates both projects.

What a filing in this position usually claims is a specific *mechanism* rather than the architecture -
the shard-padding and cold/hot worker-parking scheme, or the pre-commit resolution algorithm. Whether
such a claim survives examination is a separate question from whether it is filed; filings are cheap
relative to their deterrent effect, and "patent pending" can be written the day an application is
submitted.

**Practical effect on PC: minimal.** PC's approach is independently developed, publicly documented,
and predates the filing by years. **This is not legal advice and nothing here is a
freedom-to-operate opinion** - if it ever matters commercially, that is a question for a solicitor,
and the prior-art record is the thing to preserve.

### Is `bench/llingr/NOTICE.md` ours or llingr's?

**Ours.** It is a file this project wrote, in this repository, recording the licensing constraints
that come with benchmarking an AGPL dependency. Nothing of llingr's was copied into this repo.

### The AGPL question - which project, explicitly

You asked about **llingr's** licensing; I answered about **ours**. Both matter, so here is each,
labelled.

**llingr's own licensing** (the thing you asked):

| Component | Licence | Freely obtainable? |
|---|---|---|
| `llingr-demux` - the Go engine | **AGPL-3.0-only OR a commercial licence** | **Yes** - public repo, `go get` works, no key needed |
| `llingr-nexus` - the shared contracts | Apache-2.0 | Yes |
| The Kafka adapters, metrics and logger modules | Apache-2.0 | Yes |
| `llingr-kafka` - the Rust crate | AGPL-3.0-only OR commercial | Yes, crates.io |
| **The JVM build** | not stated in SPDX terms anywhere | **No - licence key required** |
| The relay container | no licence statement found | No repo exists |

So: **the Go engine is genuinely free and open source under AGPL**, and a hobbyist or a retired
individual can use it at no cost. The commercial licence exists for anyone who cannot accept AGPL's
network clause - which in practice means most companies shipping closed-source products. The JVM
build is the paywalled one.

**Our repository's exposure** (what I answered instead, still worth knowing):

"This repository" means **Parallel Consumer's**, which is Apache-2.0. Adding the benchmark arm put a
`go.mod` file into it. A `go.mod` is Go's dependency manifest - the equivalent of a `pom.xml` - and it
now lists `github.com/llingr/llingr-demux` as a dependency of the *benchmark*.

Why that is almost certainly fine: **nothing of llingr's source is copied into our repository**; the
benchmark is its own isolated Go module, so nothing PC publishes can pick it up; the compiled binary
is written outside the repository; and it is never distributed. AGPL obligations attach to
distributing or network-serving a derived work, none of which happens for a local benchmark.

The only real consequence is cosmetic: a licence scanner pointed at our repo will now report an AGPL
dependency, and someone will eventually ask why. `bench/llingr/NOTICE.md` - **a file we wrote** -
exists so the answer is already written down. If you would rather avoid the question entirely, moving
`bench/llingr/` outside the repository is a one-directory change with no functional effect.

### The AGPL point, restated plainly

The concern is only this: **this repository is Apache-2.0, and it now contains a `go.mod` file that
names an AGPL-licensed, patent-pending library as a dependency.**

Why that is *probably* fine: nothing is vendored (no llingr source is copied in); the benchmark lives
in its own Go module so nothing PC ships can pick it up transitively; the compiled binary is written
outside the repository; and it is a private benchmarking tool, never distributed. AGPL obligations
attach to distributing or network-serving a derived work - none of which happens here.

Why it was flagged at all: a licence scanner pointed at this repo will now report an AGPL dependency,
and someone will eventually ask. `NOTICE.md` exists so the answer is already written.

**If that is unwelcome, the fix is trivial** - move `bench/llingr/` outside the repository and have
the harness look for it at a configured path. One directory move, no functional change.

### The `ConcurrentKeys` panic and the quiet logger

Two small implementation notes from building the arm:

- **llingr panics if `ConcurrentKeys` exceeds 5,000** - a hard validated maximum. A sweep that tried
  10,000, as our PC sweeps did, would crash mid-run. The harness now clamps to 5,000 and warns, so a
  sweep completes and the clamp is visible rather than silent.
- **A quiet logger was set on the Go side deliberately.** llingr's engine logs through a pluggable
  logger and would otherwise log at its default level. This harness has already been caught measuring
  its own logging once - the phantom "second cliff" at 0.5.2.8 was entirely logback defaulting to
  DEBUG - so pinning log level on **every** arm is now a standing rule, not a nicety.

---

## Does llingr use Envoy? Should PC? Should Envoy embed a PC engine?

**No, llingr does not use Envoy.** The comparison was about a *pattern*, not a dependency. Envoy's
`ext_proc` filter and llingr's relay happen to make the same choice about who dials whom - the engine
is the gRPC client, the user's handler is the server. That is all the resemblance amounts to. llingr
ships its own Go gRPC bridge.

**Should PC use Envoy? No, and the reason is that Envoy does not speak Kafka.** Envoy is an HTTP,
gRPC and TCP proxy. It has no Kafka consumer, no consumer-group membership, no offset management. It
does have a Kafka *broker* filter that can parse the Kafka wire protocol for observability and
routing, but that is a proxy watching traffic, not a consumer participating in a group. **The part PC
does - group membership, offset commits, ordering, retries - Envoy has no notion of.** There is
nothing to delegate.

**Should Envoy embed a PC engine?** It is an interesting thought and the answer is no, for a
structural reason: Envoy filters sit on a *request* path, and Kafka consumption is not request-shaped.
A consumer is a long-lived participant in a group with its own poll loop and rebalance lifecycle. You
would be embedding a stateful daemon inside a stateless filter chain, and everything hard about PC -
partition assignment, offset state, drain-on-revoke - would have to live somewhere Envoy has no
lifecycle for. The sidecar model PC already plans achieves the deployment benefit without that
mismatch.

**What *is* worth taking from Envoy:** `ext_proc`'s existence is evidence that "let the user process
this in their own language over gRPC" is a mainstream, production-proven pattern rather than an
exotic one. That is a point in favour of astubbs#242's whole premise.

## Does llingr's relay have to be managed separately from the application?

**Yes - and you are right that it is a genuine advantage for PC's design.**

llingr's relay is a **container image** holding the broker adapter, the engine and the gRPC bridge. It
is deployed as its own artifact - *"either as a standalone service, or in a sidecar"* - and it dials
out to the handler. So a user adopting the polyglot path operates **two deployable units**: their
application, and a relay container that must be configured, versioned, upgraded, monitored and
network-reachable from the relay to the app.

PC's design has the application **spawn** the sidecar as a child process. One deployable unit; the
sidecar's lifetime is the app's lifetime; no separate image to version; no service discovery; no
inbound listener on the app.

**Why would llingr choose the harder-to-operate option?** Two plausible reasons, both defensible:

1. **It falls out of what was already built.** The Go engine is a library. The quickest way to reach
   other languages is to wrap the existing library in a server and ship it as a container - no
   process-spawning, no per-language packaging, no vendored binaries. PC's model requires each client
   package to ship and spawn a platform-matched binary, which is the harder engineering (and is
   exactly what astubbs#242's KTD41 wrestles with).
2. **It fits a Kubernetes-native audience.** For a team already running sidecars, "add a container to
   the pod" is routine and arguably *preferred* to a library spawning child processes - some
   environments actively dislike that.

**So the honest framing: PC's is better for a developer** (one dependency, `new PC(...)`, nothing to
deploy) **and llingr's is more conventional for a platform team.** PC's is materially harder to build,
which is why the plan has KTDs about vendoring binaries and parent-death watching. That difficulty is
the price of the simpler user experience, and it is worth saying so rather than treating the choice as
obvious.

## If nothing waits in llingr's engine, where do records wait?

**In the broker, and in the client's fetch buffer.** That is the whole answer, and it is the
structural difference.

llingr's poll loop takes a record, blocks until it can hand it to a worker, then asks for the next.
When workers are busy, the loop simply **stops polling** - so unfetched records stay on the broker,
and whatever the Kafka client has already fetched sits in the client's own buffer. The engine holds
only what is in flight.

PC deliberately does the opposite: it **pulls work forward** into the `WorkManager` so the engine has
a buffer of eligible records to dispatch from. That is what the load factor and `messageBufferSize`
size, and it is why PC keeps the dispatch thread fed rather than waiting on the next poll.

**This session measured what happens when PC accidentally behaves like llingr:** `ExternalEngine` pins
the load factor at 1, giving Vert.x, Reactor, Mutiny and the proxy no buffer behind their in-flight
ceiling - and that alone cost 35% throughput. So the two designs' costs are now measured rather than
argued: no buffer is cheaper in memory and slower in throughput.

## Doesn't llingr have a shard system like PC's?

**It has shards, but they are a lock-striping device, not a scheduling structure - and that is the
distinction.**

llingr's `WorkerShard`s exist to spread mutex contention: shard count is a power of two, keys hash to
a shard with FNV-1a and a bitwise mask, and each shard has its own lock and worker map. That is
*concurrency hygiene* - the same reason a `ConcurrentHashMap` has bins.

**What it is not is a place where work queues up and something chooses between shards.** A record's
route through llingr is: acquire a concurrency token, hash the key, find or create that key's worker,
push to that worker's channel. The shard is looked up **for one record at a time**, on the polling
thread, with nothing accumulated.

PC's shards are the opposite: they are **queues of eligible work**, many non-empty at once, and each
control-loop iteration must decide which to draw from and how much. That is a scheduling decision, and
`LoopingResumingIterator` is the policy.

**Same word, different job.** llingr's shards make locking cheap; PC's shards make ordering and
fairness expressible. It is a good example of why comparing architectures by vocabulary is
misleading.

## Head-of-line blocking at admission - the mechanism, once more

Head-of-line blocking is when the item at the front of a queue prevents everything behind it from
proceeding, even though those items could have been served. It is the whole reason parallel consumers
exist: on a plain Kafka consumer, one slow record blocks its entire partition.

llingr removes it at the *processing* layer - keys go to independent workers, so a slow key blocks
only itself. **But it reintroduces it at the admission layer:**

```
poll a record
  -> acquire a concurrency token        <-- happens FIRST, before the key is examined
  -> hash the key, dispatch to a worker
  -> repeat
```

There are 250 tokens by default, and a token is held for as long as its handler runs. So when 250
handlers are slow, all tokens are held. The **next** record polled cannot get a token - **whatever key
it belongs to, including a key whose worker is sitting idle.** And because the poll loop is a single
thread calling a blocking dispatch, the entire consumer stops behind that one record. If no token
frees within `AcquireWorkerTimeoutCircuitBreaker` (default one minute), the circuit breaker fires and
the process shuts down.

**So the queue is the poll loop itself, and the head of it is one record that cannot get a token.**
Slow keys block fast keys - the original problem, one layer up.

PC does not have this shape because it acquires work *for* shards rather than acquiring capacity
before looking at the record, and because it holds a buffer of eligible records rather than admitting
one at a time.

## Is `ConcurrentKeys` the same as PC's `maxConcurrency`?

**Effectively yes, with two differences worth knowing.**

Both name the maximum number of records being processed at once, and both are the primary throughput
dial. PC's `maxConcurrency` (× `batchSize`) sets the in-flight target; llingr's `ConcurrentKeys` sets
the number of concurrency tokens.

- **llingr caps it at 5,000 and panics above that** - a validated hard maximum. PC has no such cap;
  this session ran it at 10,000 (slower than 1,000, but it ran).
- **The name reflects the model.** llingr's unit is a *key*, because one key means one worker; PC's is
  a *record*, because records from many keys can be in flight independently. In practice both bound
  concurrent work.

For benchmarking they are the dial to match on both sides - which this harness does.

---

## Corrections and follow-ups, 2026-08-21 (second round)

### "Isn't wrapping Java in a server and spawning binaries exactly what WE do?" - yes, and my framing was wrong

**You are right and the earlier answer was sloppy.** astubbs#242 does wrap the JVM engine in a gRPC
server and vendor a platform-matched binary into each language package to be spawned. So "wrap the
library in a server" is not the difference.

**The actual differences are two:**

1. **Container versus vendored binary.** llingr ships *one* container image. PC ships a
   platform-matched native binary inside *each* language package - eleven packages, several platforms
   each. That is the packaging burden KTD41 wrestles with, and it is genuinely more work than
   publishing one image. But it buys the single-deployable-unit experience.
2. **llingr has native engines where PC has a sidecar.** Go, the JVM build and Rust (via FFI) run
   **in-process with no sidecar at all**. The relay is only for everything else.

**And on Swift specifically: llingr has nothing in Swift.** Its coverage is Go, a JVM build and a
Rust FFI binding; the relay is advertised for *"Java, Python, Rust, .NET, TypeScript - or any language
with gRPC support"* and **the relay does not exist yet**. Swift is not named anywhere. PC has eleven
clients including Swift, driven through a shared conformance suite. That comparison runs the other
way from how the earlier note framed it.

### "Aren't PC's shards just the client's fetch buffer, organised?" - close, and the difference is the point

**Largely yes**, and it is a fair way to see it. Both designs bound how much is outstanding and both
apply backpressure - llingr by stopping its poll loop, PC by pausing partitions when its buffer fills.

**The difference is that PC's copy is *organised and selectable*.** llingr's records sit in the Kafka
client's fetch buffer as an undifferentiated queue in broker order. PC pulls them into per-key or
per-partition shards, which is what makes it possible to *choose* what to dispatch next - and choice
is what ordering modes, retry scheduling, fair traversal and future per-topic shares are all built on.

So: same records, same backpressure, different structure - and the structure is the feature. The cost
of that structure is the open question in
[`next-performance-regression-testing.md`](next-performance-regression-testing.md).

### How does llingr avoid dispatching to a key that is already busy?

**It does not need to check, because a key has exactly one worker and records queue behind it.**

The route is: acquire a token, hash the key, look up the key's worker in that shard's map. If a worker
already exists, **push onto that worker's channel** (`PerKeyBufferLen`, default 16) and **release the
token immediately**. If no worker exists, borrow one from the pool and hold the token until it drains
and returns itself.

So there is no scan and no eligibility check. The semaphore bounds **concurrently active distinct
keys**, not records - which is why the setting is called `ConcurrentKeys` rather than max concurrency.
Ordering per key falls out of the worker being single-threaded over its own channel.

**This does not soften the head-of-line problem**, because the token is acquired in `Process` *before*
the key is examined. A record for an already-active key still needs a token to get far enough to
discover that it can release it again.

### Head-of-line at admission - is 250 his max, and can PC have both?

**Yes: `ConcurrentKeys` defaults to 250 and is the token count. 5,000 is the maximum you may
configure** - the same dial, not a second one. Above 5,000 the library panics.

**The mechanism, precisely.** 250 distinct keys are active, each holding a token. A record arrives for
a 251st key - or even for one of the 250 already active. It must take a token to proceed at all. None
is free. The poll loop is single-threaded and blocks. **Everything stops**, including work for keys
whose workers are idle. After `AcquireWorkerTimeoutCircuitBreaker` (default one minute), the process
shuts down.

**PC does not have this shape**, and the reason is the buffer. PC pulls records into shards and the
control loop dispatches from them up to the in-flight target. A record whose key is busy simply waits
*in its shard* - it never blocks the poll loop, and other shards keep being served.

**So PC already has both properties**: bounded concurrency *and* no admission-time head-of-line
blocking. The trade is the one this session has been measuring - PC maintains an organised buffer, and
that buffer is where the 118µs per record is being spent. **The question is not whether to adopt
llingr's structure; it is how much cheaper PC's can be made.** See the lock-free hypothesis below.

### go.mod, SPDX headers, REUSE

- **`go.mod`** is Go's dependency manifest - **yes, the direct equivalent of `pom.xml`**. It names the
  module, the Go version and the dependencies with versions. `go.sum` is the checksum lockfile.
- **SPDX headers** are one-line machine-readable licence declarations at the top of each source file -
  `// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial`. They let automated
  scanners determine a file's licence without heuristics.
- **REUSE** is an FSFE specification for doing that consistently - a `REUSE.toml` plus per-file
  headers plus a `LICENSES/` directory, verifiable by a linter. It is the tidy end of licence hygiene.

Worth noting for us: PC's copyright headers already serve the human purpose;
`bin/check-copyright-headers.sh` enforces them. Adding SPDX identifiers would make them
machine-readable too, which is a small change with real value for enterprise adopters whose scanners
must classify every file.

### Why charge for the JVM build but give the Go engine away?

Not stated anywhere, so this is reasoning rather than fact - but the shape is legible:

- **The Go engine is AGPL.** Any company shipping closed-source software cannot use it without a
  commercial licence anyway, so publishing it costs little and buys adoption, credibility and
  inspectable source.
- **The JVM is where Kafka's enterprise money is**, and JVM shops are exactly the population most
  likely to need a commercial licence. Gating the build behind a licence key converts the AGPL
  obligation into a purchase conversation earlier.
- **It also conveniently makes the JVM claims unverifiable** - no repo, nothing on Maven Central, a
  401 registry. Whether that is deliberate or incidental, the effect is that the parity, performance
  and formal-verification claims about the JVM edition cannot be checked by anyone.

### The 200x claim also assumes a key distribution nobody mentions

**A sharper objection than the ones already recorded, and it is yours.** llingr has **no unordered
mode** - concurrency comes exclusively from distinct keys hashing to distinct workers. So reaching
250x requires **at least 250 distinct keys among the records currently at the head of the partition**,
continuously.

Real data frequently does not look like that: a handful of hot keys, keys clustered in time, or a
producer keying by a low-cardinality field. In those shapes llingr's achievable concurrency is bounded
by key cardinality *at the head of the log*, not by `ConcurrentKeys`, and the 250x collapses toward
the number of distinct keys actually available.

**PC's `UNORDERED` mode has no such dependency** - it will use the full in-flight budget regardless of
key distribution. That is a real and previously unstated advantage, and it belongs in the positioning:
*"Choose per-key ordering when you need it, or unordered when you do not - and get full concurrency
either way."*

Their own benchmark sidesteps this entirely - and now confirmed from the generator source, not
inferred. **Every message in every published run gets a globally unique key**
(`tests/testkit/scenario/generate_messages.go`: `Key: fmt.Sprintf("key-%d-%s", i, randomString(30))`,
where `i` is the global index). 100,000 distinct keys per run against a maximum `ConcurrentKeys` of
5,000 - a 20x surplus at the extreme and 400x at the default 250. The generator takes **no key-count
parameter**: cardinality is not a benchmark dimension, it is a constant pinned at its maximum. Full
analysis in [`market-analysis-llingr.md`](market-analysis-llingr.md) section 5c.

**So the objection above is not hypothetical - it is exactly what the harness is arranged to avoid.**
The worker map never finds an existing worker for a key, no record ever queues behind a sibling, and
the serial-per-key mechanism the library exists to provide is not exercised in a single published run.

**Two things worth separating, in fairness.** The claim on the site is a ratio, and the benchmark fixes
that ratio at its maximum. But the benchmark's *stated* purpose is measuring framework coordination
overhead, and unique keys make that measurement genuinely harder rather than easier - every record pays
the cold path with no amortisation. The overhead numbers are honest; it is the headline ratio that the
distribution flatters. And note the ratio claim **does not appear in the repository at all** - searched
the README, the deep dive, the chart HTML and the scripts: zero hits. It lives only on the site, one
step further from the data that would qualify it.

### What is "the CSV" and what is published?

`benchmark_results.csv` in `llingr-demux/benchmarks/` - 540 rows of raw per-run measurements
committed to the repository, alongside the harness (`run_benchmarks.sh`, `runner/main.go`), the
configs, a chart generator and a 520-line `TECHNICAL_DEEP_DIVE.md`. That is what "published" means
here: the raw data and the tooling to regenerate it, not merely a chart.

**It is also what makes the tautology checkable** - the absence of a serial data point is visible in
their own committed data.

### Our demo is a stronger artifact, and that should be said

llingr's benchmark runs against an **in-memory mock broker**. PC's comparison demo
([`branch-classic-comparison-demo.md`](branch-classic-comparison-demo.md)) is designed to run against
**a real broker, and against the user's own topic and data** - and to generate synthetic data whose
shape the user dials: key-set size as a percentage of records, failure percentage, per-record delay,
concurrency, partitions, instances.

**That is a categorically better demonstration**, and for the reason that matters: it answers *"will
this help me?"* rather than *"is this fast in the author's harness?"*. The key-distribution point
above is exactly what a user can discover for themselves by turning the key-count dial - including the
region where a competing design would degrade and PC's unordered mode would not.

### Hypothesis for the performance work: shared state and lock contention

**Recorded 2026-08-21 as the owner's hypothesis for where PC's 118µs per record goes**, to be tested
rather than assumed:

> At a 0ms function we probably pay a large cost for shared state - concurrency-safe collections
> guarded by locks that are not really needed. Lock-free structures, and passing single messages
> between components instead of sharing collections, would be substantially faster.

**Related prior work in this repository:** the actor/IPC branches - `improvements/lambda-actor-bus`,
`improvements/commit-command-actor`, `improvements/poller-bus-actor`, `improvements/actor-scheduled` -
were all aimed at exactly this: isolating threads and state, and replacing shared mutation with
message passing. All are unmerged and, per
[`next-fork-branch-archaeology.md`](next-fork-branch-archaeology.md), referenced by no document.

**How to test it rather than believe it:** the instrumentation in
[`next-performance-regression-testing.md`](next-performance-regression-testing.md) - per-stage timing
and allocation attribution - is the prerequisite. A lock-contention profile at concurrency 1 versus
concurrency 100 would separate "lock contention" from "fixed per-record cost", since contention should
grow with concurrency while a fixed cost should not. **Note the earlier version of this hypothesis was
tested at one boundary and not supported**, so it needs testing where the overhead actually is.

### Is llingr's fan-out adaptive? No - and PC's being fixed is a real difference

**llingr's fan-out is not adaptive.** It fans out to whatever the work requires and no further, because
a worker is created only when a record for a *new* key arrives and no worker exists. At 0ms the handler
returns before the next record arrives, so one worker is usually enough and peak in-flight stays at
2-3. At 2ms, records arrive faster than they complete, workers accumulate, and it reaches exactly
1,000. **Demand-driven, not measured or controlled** - there is no feedback loop, no target, nothing
observing performance.

**PC's is different: PC pre-fetches to its in-flight target regardless of need.** At 0ms PC held 100
in flight where llingr needed 2. That looked wasteful - and the concurrency sweep showed it is not:
more fan-out made PC monotonically faster, all the way to 1,000.

**So the honest comparison is:** llingr's shape is emergent and self-limiting; PC's is deliberate and
targeted. Neither is adaptive in the sense
[`next-auto-scaling.md`](next-auto-scaling.md) means - **nobody is measuring outcomes and adjusting.**
That remains unbuilt by both, and it is still the differentiator.
