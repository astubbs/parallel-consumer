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
