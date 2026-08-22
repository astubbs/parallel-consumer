# v6 release: the "Phoenix — Spreading the Love" theme, and the announcement

Owner's idea, 2026-08-15, for the v6 release. Recorded now because a theme is worth deciding while
the work that justifies it is fresh, and forgotten otherwise.

## The theme

**Phoenix — Spreading the Love.**

- **Phoenix** — the project rising from the ashes in a new form, better than before: a fork of a
  no-longer-maintained upstream, brought back with new features rather than merely kept alive. In
  the owner's framing it is personal as well as technical, about himself as much as the codebase.
- **Spreading the Love** — the multi-language client libraries. What was a JVM-only library reaching
  other runtimes.

The two halves work together: the first says the project is alive, the second says who it is now for.

## The announcement, and its order

**This is the last task of the v6 release, not an early one.** It describes what shipped, so it
cannot be written honestly before the shipping is done — and it is the fun part, which is a reason
to protect it from being rushed rather than to skip it.

A funnel, in three pieces:

1. **The LinkedIn post — short, a teaser.** Names the themes, **apologises for the release taking
   longer than promised**, and says he comes bearing gifts. Its only job is the click through to the
   blog post. Keep it short: it is not the announcement, it is the invitation to read it.
2. **The blog post — the actual announcement.** What is in v6, what the multi-language work means for
   someone who is not on the JVM, and why the project is back. Links onward to the release notes.
3. **The release notes** — wherever they end up published. The blog links to them rather than
   restating them.

## One paragraph the blog post should carry, close to verbatim

Singled out by the owner as the clearest statement of what these libraries are, and why they are not
ten reimplementations:

> The one thing the client deliberately doesn't mirror is ordering. All shard selection, retry
> scheduling and offset tracking stay in the engine. The client gets records and gives back verdicts -
> which is what keeps it a facade rather than a second implementation of Parallel Consumer in every
> language.

The frame it sits in, if the post has room: **each client is a miniature of Parallel Consumer's own
controller.** The transport thread mirrors the broker poller, the dispatch queue mirrors the work
manager's in-flight set, the executors mirror the worker pool, and the queue's depth mirrors the
in-flight ceiling. Both ends share one invariant - **the thread that moves work must never be the
thread that waits on work** - which is why core separates the poller from the control loop, and why a
client whose processor blocks instead of awaiting deadlocks its entire session.

That symmetry is the honest answer to the question the announcement will provoke: *how can ten
libraries written this quickly be trustworthy?* Because nine of them implement almost nothing. The
hard parts - ordering, retries, offset encoding, commit decisions - live in one place that has been
tested for years, and the same shared conformance suite proves every client behaves identically at
the boundary.

## The rule that governs all of it: claim nothing while it is experimental

Owner's call, 2026-08-15, and it binds this document: **no claim is made publicly until the code it
describes is out of experimental status.** Everything below is material for when that is true, not a
licence to say it sooner.

The positioning is strong enough that the temptation will be to lead with it while the clients still
carry an experimental warning on every README, nothing is published to any registry, and no client
has carried production traffic. Saying it early spends the credibility the evidence is supposed to
buy — and this project's whole argument is that its claims are testable. A claim that outruns its
code is the one thing that would undermine it.

So: write the post, keep it ready, and publish the claims **as each part earns them**. The
conformance suite makes "has it earned it" a test result rather than a judgement, which is exactly
the discipline to lean on here.

## The positioning, worked out 2026-08-15 — the strongest material for the post

**Lead with the architecture the reader already has, not with the feature.** The common shape is
Kafka as the backbone, bridged into RabbitMQ, SQS or Celery whenever work needs per-key ordering,
retries or real concurrency. That bridge costs a second system to operate, two sets of delivery
semantics, and data leaving Kafka — forfeiting replay, retention and ordering — plus a bridge that
can lose or duplicate. The claim, in its narrow and defensible form:

> If you bridged Kafka into a queue broker to get concurrency, ordering and retries — you do not need
> to.

Keep it narrow. The wide version ("you don't need RabbitMQ") is false: no routing or exchanges, no
priority queues, no arbitrary delayed delivery, no per-message time-to-live.

**Why more concurrency is not just a bigger number.** Every other way to exceed partition-count
concurrency means adding partitions, dropping ordering, or leaving Kafka. This gives all three at
once — and now in any language. What makes it possible is not the sidecar or the protocol but the
offset-map encoding that lets out-of-order completion commit safely; that is the years of work
nothing on the wire reveals.

**Address Dapr directly rather than hoping nobody asks.** Dapr is the architectural neighbour every
informed reader will reach for: also a sidecar, also any language, also Kafka. The distinction is
purpose — Dapr moves *messages* and exists for portability, its Kafka component being an ordinary
consumer group bounded by partitions. This redistributes *work*. Same shape, different problem. See
[`next-study-dapr-and-kafka-proxies.md`](next-study-dapr-and-kafka-proxies.md), and **do the study
before publishing** rather than asserting the comparison from memory.

**"Is it a broker?" is a good question to answer in the post**, because readers will ask it.
Behaviourally it is broker-shaped: ordered dispatch, acknowledgements, redelivery with attempt
counts, leases, worker liveness, a dead worker's records reclaimed. What keeps it from being one is
durability — it owns only the in-flight view, Kafka stays the sole source of truth, and nothing is
lost when the sidecar dies because nothing was acknowledged. The 2021 issue this all descends from
already called it *"a server side queue implementation"*, which is a good line to quote against
itself.

**One caution about novelty.** Every ingredient exists separately — sidecars over Kafka (Dapr,
kafka-pixy, Confluent REST Proxy), key-ordered concurrency (this project), queue semantics with acks
and redelivery (every queue broker). The *combination* is what nobody seems to have. "Never seen
before" is exactly the claim that attracts one embarrassing correction, so make the specific claim
rather than the sweeping one, and let a survey back it.

## Two practical notes for whoever writes it

- **The release notes are generated from the commit log** (`docs/releasing.md` owns how). That makes
  the blog post cheaper to write than it looks: the *why* behind each change is already in the commit
  bodies, by design, rather than needing to be reconstructed months later.
- **The strongest material is evidence, not adjectives.** The most persuasive things this release can
  say are testable: clients in N languages that all pass one shared conformance suite, a protocol
  frozen against a breaking-change gate, and a specification a fresh author implemented from the
  documents alone. See [`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md) —
  that note exists because "these are not generated slop" is a claim the announcement will have to
  make, and it is only worth making if it is backed.

## The points inventory - what has to get across, and where

**Built 2026-08-22, language deliberately not settled.** This is the checklist, not the copy. The
owner reviews it for gaps first; wording comes after.

**L = the LinkedIn reply. B = the blog post.** L is a strict subset of B - the reply's only job is the
click through, so anything that needs a sentence of setup belongs in B.

Ordered by how much a reader who already knows PC would care.

### Performance

| # | Point | L | B | Notes / evidence |
|---|---|---|---|---|
| P1 | **Nearly twice the throughput of the last public release** | **yes** | yes | ~1.75x vs 0.5.3.3, same hardware, same workload. **NOT YET PUBLISHABLE** - measured at 50,000 records on one partition, one repeat pair. Re-take at 100k across several partitions before this goes out |
| P2 | The engine now reaches the concurrency you configure - 40,000 records in flight | yes | yes | Frame as virtual threads lifting a platform-thread ceiling. **Do not** frame as the old version being broken |
| P3 | Virtual threads, opt-in, JDK 21+ | no | yes | Needs the Java-baseline caveat alongside it |
| P4 | A direct-pull engine, opt-in - fastest configuration measured when paired with virtual threads | no | yes | Preview-grade. Say so |
| P5 | PC now sits within ~13% of a bare Java consumer that does no ordering at all | no | yes | The honest framing of overhead, and it invites the "what does it cost me" question rather than dodging it |

### Self-tuning - the strategy doc calls this the capability nothing else offers

| # | Point | L | B | Notes |
|---|---|---|---|---|
| S1 | **The engine discovers its own concurrency from runtime measurement** | **yes** | yes | Lead item |
| S2 | **It recommends its own instance count to your infrastructure** | **yes** | yes | The part people will not expect |
| S3 | Per-record ground truth no external autoscaler can see | yes | yes | Why it can do this and a sidecar HPA cannot |
| S4 | The non-JVM clients inherit it | yes | yes | A Python consumer getting runtime-discovered concurrency is something no Python Kafka library can offer |
| S5 | It needs the signal-integrity work underneath - accurate timing under every engine | no | yes | Answers "how do you know your own numbers" |

### Kafka Streams (preview)

| # | Point | L | B | Notes |
|---|---|---|---|---|
| K1 | **A topology's parallelism has always been its partition count. Not on PC's engine** | **yes** | yes | The headline capability |
| K2 | **12 partitions, 100ms of I/O per record: ~120 rec/s becomes thousands** | **yes** | yes | Arithmetic plus measurement. Under-claims deliberately |
| K3 | Same topology - no rewrite, no new API | yes | yes | The reason it is adoptable |
| K4 | **Head-of-line blocking goes away**: one slow record no longer delays every record behind it in its partition | no | yes | The deeper argument, and it needs a paragraph. **The latency measurement that proves it does not exist yet** |
| K5 | It is a preview, and what that means | yes | yes | State the limits: state stores, stream time, EOS |

### Kafka Connect (preview)

| # | Point | L | B | Notes |
|---|---|---|---|---|
| C1 | **A sink's throughput is bounded by task count, and tasks by partitions. Not here** | **yes** | yes | On L this is one clause riding on K1's setup |
| C2 | Paired with self-tuning, a sink stops being a sizing exercise | no | yes | Needs S1-S2 established first. Vague if it comes early |

### Other runtimes

| # | Point | L | B | Notes |
|---|---|---|---|---|
| R1 | **Eight new languages: Python, Go, Rust, Ruby, .NET, TypeScript, Swift, C++** | **yes** | yes | Java uses core directly - no binding, do not imply one |
| R2 | Ordering, retries and offset encoding stay in the engine; clients receive records and return verdicts | yes | yes | The facade claim |
| R3 | Nine facades, not nine reimplementations - one shared conformance suite proves identical behaviour at the boundary | no | yes | The answer to "how can these be trustworthy" |
| R4 | Each client is a miniature of PC's own controller - the near-verbatim paragraph already chosen for the blog | no | yes | Already recorded above in this note |
| R5 | Kotlin and Scala bindings exist, for people who want idiomatic surfaces | no | yes | Honest, and unimpressive on L where it reads as padding |

### Reliability and correctness

| # | Point | L | B | Notes |
|---|---|---|---|---|
| X1 | **Exactly-once still holds across all of it** | **yes** | yes | The first question anyone asks about Streams-on-something-else |
| X2 | **Every known defect cleared** | yes | yes | A roadmap gate for 0.6.0.0. Strong claim - make sure it is still true at ship |
| X3 | The classes fixed: stalls, rebalance handling, offset tracking | no | yes | Name them. This is the account STRATEGY.md says every lost-record bug withdraws from |
| X4 | Faults found and fixed during the work itself - a silent Reactor stall on an empty publisher, a check-then-act claim that could deliver a record twice | no | yes | Credibility through specificity, and it shows the testing found them rather than the users |

### Observability

| # | Point | L | B | Notes |
|---|---|---|---|---|
| O1 | **A metrics dashboard showing offset encoding and in-flight state** | **yes** | yes | Not "a GUI". Name the quantities |
| O2 | Because moving the queue into your client moves it out of the cluster's view | yes | yes | Straight from STRATEGY.md, and the strongest sentence available |
| O3 | Record residence time - how long a record spends inside PC, end to end, including retries | no | yes | New. The only metric that says whether your configured concurrency is actually being used |
| O4 | Metrics that exist end to end, under every engine | no | yes | |

### Features

| # | Point | L | B | Notes |
|---|---|---|---|---|
| F1 | **Dead letter queues** | **yes** | yes | Most-asked. Its own line, and acknowledge the wait |
| F2 | Richer batch modes / micro-batching | no | yes | |
| F3 | Health check surface | no | yes | |
| F4 | Bounded buffers | no | yes | |
| F5 | Distributed throttling | no | yes | Genuinely unusual - may deserve promotion to L |
| F6 | An HTTP endpoint server, as a candidate | no | maybe | Only if it actually ships |

### The project itself

| # | Point | L | B | Notes |
|---|---|---|---|---|
| J1 | **Not dead - forked, and shipping** | **yes** | yes | The reply's opening line, because it answers the thread's premise directly |
| J2 | **Apache 2.0** | **yes** | yes | |
| J3 | **An apology for the delay** | **yes** | yes | The owner's own instruction, and it buys more goodwill than any feature |
| J4 | New Maven coordinates | no | yes | The first thing an adopter needs |
| J5 | The "Phoenix - Spreading the Love" theme | no | yes | The blog's frame. Too much for a reply |
| J6 | How it is maintained, and the bet that every failure becomes a mechanism | no | yes | Answers "will this be abandoned too", which is the thread's actual anxiety |
| J7 | Links: the blog, then the release notes | **yes** | n/a | The reply's whole purpose |

### Caveats that belong in the blog, unburied

| # | Point | L | B | Notes |
|---|---|---|---|---|
| Q1 | **PC is not current with Kafka today** | no | **yes** | STRATEGY.md states it plainly. Omitting it is the kind of thing that gets found and quoted back |
| Q2 | The Java baseline - 0.6.x stays on Java 8; virtual threads need 21+ | no | yes | |
| Q3 | Streams and Connect are previews, and what does not work yet | yes | yes | On L as the word "preview"; in B with the actual limits |

## Known gaps in this list

- **The head-of-line latency number does not exist** (K4). It is the strongest argument available and
  is currently an argument rather than a measurement.
- **P1's ~1.75x needs re-taking** at 100,000 records across several partitions before publication.
- **Nothing here says what it is like to USE.** Every point is a capability or a number. A release post
  with no code in it is a specification, not an announcement.
