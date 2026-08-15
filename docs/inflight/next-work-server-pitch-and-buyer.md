# The work-server pitch: who buys it, what it replaces, what it enables

Synthesis of the 2026-08-15 conversation that ran from "wouldn't an HTTP server be just another
foreign language?" to "what *is* this thing". The dialects, adapters and the sidecar-or-server fork
are owned by [`next-http-strategy-ideas.md`](next-http-strategy-ideas.md) — **§4h owns the fork and
§4d owns the marketing lead**; the comparison to the neighbours is owned by
[`next-study-dapr-and-kafka-proxies.md`](next-study-dapr-and-kafka-proxies.md). What is here is the
part neither holds: **the pitch, the buyer, and the architecture of the shared interfaces as a
product rather than as a feature.**

Nothing here is decided. It is written down because the framing arrived all at once and would not
survive the week otherwise.

## 1. The pitch, in one sentence

> **You already run Kafka. This turns it into a work-distribution platform — so your teams stop
> asking for a queue broker, and stop asking you for more partitions.**

The application-team version of the same sentence is different, and that difference is the whole
go-to-market shape (§4):

> **Process Kafka records concurrently, in order, in any language — without adding partitions and
> without adding a broker.**

Both are narrow on purpose. The wide version ("replace RabbitMQ") is false and
[`next-study-dapr-and-kafka-proxies.md`](next-study-dapr-and-kafka-proxies.md) says why: no routing
or exchanges, no priority queues, no arbitrary delayed delivery, no per-message time-to-live.

## 2. What it replaces

Ranked by how much it is actually costing someone today, not by how impressive it sounds.

1. **The bespoke concurrency layer every team writes and nobody counts.** Consume, hand to a thread
   pool, and then discover offsets commit ahead of completed work. This is the largest line item and
   it is invisible on any org chart, because it is spread across a dozen repositories as fifty lines
   each. Most of those implementations are quietly wrong on failure.
2. **The Kafka→queue-broker bridge.** Kafka as the backbone, bridged into RabbitMQ, SQS, Celery or
   Sidekiq the moment work needs per-key ordering, retries or real concurrency. Costs a second system
   to operate, two sets of delivery semantics, and data leaving Kafka — forfeiting replay, retention
   and ordering — plus a bridge that can lose or duplicate.
3. **Partition inflation used as a concurrency dial.** Partitions are the wrong lever: they cost
   rebalance time, memory, file handles and open connections; they are effectively one-way; and the
   request lands on the platform team as a ticket. Concurrency stops being a topology decision.
4. **"That language has no good Kafka client, so we will bridge."** The proxy answers this as a side
   effect rather than as its purpose — which is exactly why it answers it differently from the
   neighbours, who stop at partition-bounded delivery because reaching Kafka *was* their goal.

## 2b. Partition economics — the deflation argument, and its honest limits

The strongest *money* version of the pitch, and it needs stating carefully because the enthusiastic
version overreaches.

**The mechanism.** Partition count is normally sized by the largest of three demands: broker
parallelism, Kafka Streams' task parallelism, and **consumer concurrency**. Only the third is a
Parallel Consumer problem, but it is very often the one setting the number — and once the source
topic is inflated, everything downstream inherits it. Kafka Streams' repartition and changelog topics
mirror the source count, multiplied by topology stages, so a source sized for consumer throughput
becomes thousands of partitions across internal topics that nobody chose deliberately. Connect sink
task parallelism is bounded by partitions too, which is a third reason to over-provision.

**What that costs**: broker memory and open file handles, replication fetch fan-out, controller
metadata, longer and more disruptive rebalances, producer-side batching memory — and, on managed
platforms, partitions are a directly billed unit.

**All three demands are addressable, and two of them by work already on the fork.** Written down
2026-08-15 as a correction: an earlier draft here recorded "does not fix Streams" and "only helps
Connect if the connector is replaced" as permanent limits. Both describe *stock* Streams and Connect,
and both are contradicted by open work:

- **Kafka Streams — astubbs/parallel-consumer#271** (`feats/ks-on-pc-spike`, tracking astubbs#255)
  replaces the point where `StreamTask` selects the next record with PC's `WorkManager`, so a
  topology gets per-key concurrency *inside* a partition. Stock Streams serialises there with no
  semantic justification when records carry different keys. Its own control arm is the thing to quote:
  **57x on the quickest record, 8x median, and 0.69x when every record shares one key** — the last
  figure being what makes the first believable.
- **Connect sinks — astubbs/parallel-consumer#269** (`feats/connect-on-pc-spike`, tracking astubbs#240,
  upstream confluentinc#119) patches `WorkerSinkTask` rather than reimplementing it, precisely so
  single-message transforms, dead-letter queues, `ConfigProvider` and plugin isolation are inherited
  instead of deferred. **The connector is not replaced**, which is what the earlier draft got wrong.
  Its own review killed the reimplementation direction for the reason that matters here: one
  `SinkTask` per partition caps concurrency at the partition count — the same ceiling being argued
  against.

So the honest shape is that partition count is set by the largest of *broker storage and replication
spread*, *Streams task parallelism*, *Connect sink task parallelism* and *consumer concurrency* — and
PC has an answer to the last three, leaving only the first, which is the number a broker actually
needs. **That is the mechanism behind 5000→50**, and it is a roadmap claim rather than a shipped one.

**What is genuinely still limiting, stated so the claim does not outrun the evidence:**

- **Partitions cannot be reduced in place.** Kafka increases only. Deflating an existing topic means
  a new topic, a migration, and a changed key→partition mapping. **The durable, easy win is not
  inflating the next one** — each new system is sized to the broker count rather than to the desired
  concurrency, so the saving arrives as a falling budget over time rather than as a migration project.
- **The evidence behind two of the three is thin.** astubbs#271 is an alpha proven on one partition,
  one task, one instance, with windowing, joins, suppression, exactly-once and stream-time punctuation
  out of scope; astubbs#269 is U1 of a feasibility spike — a shadowing proof that is deliberately
  inert. The architecture is proven; the product is not. Do not publish the collapsed-partition-count
  number as a present-tense capability.

**Fewer clients, less broker load, fewer moving parts** is the same argument from the operational
side and compounds with the above, because the two effects multiply rather than merely coexist:
higher per-instance throughput means **fewer application instances**, therefore fewer consumer clients
— fewer connections, fetch requests, heartbeats and metadata refreshes, fewer group members, and so
faster and rarer rebalances. Fewer partitions then means less broker CPU, memory, file-handle and
replication-fan-out work per byte carried. **Both sides of the broker's ledger fall at once**: fewer
things connecting to it, and less structure inside it for them to connect to. The sidecar shape holds
this; the shared-server shape compounds it further, since many workers then share one engine's
connections rather than one each.

## 2c. The smallest pitch, and the one everybody recognises

Below the platform pitch and the application-team pitch there is a third altitude, and it is the one
that gets nods in a room:

> **"You just polled 40 records. How do you process them concurrently, with no head-of-line blocking,
> and still persist offsets correctly when three of them fail?"**

Everyone who has written a consumer has written a half-answer to this, and left throughput on the
table doing it. This is the *recognition* pitch — it needs no architecture diagram, sells nothing by
itself, and is the fastest way to establish that the problem is real before any larger claim is made.
Naming it as a distinct altitude matters because §4d of
[`next-http-strategy-ideas.md`](next-http-strategy-ideas.md) still binds: **lead with one story.**
This is a way in, not a second front-page message.

## 3. What it enables that nothing replaces

Replacement is the sale; these are the reason it is interesting.

- **Slow work is now normal work.** An inference call, an image or video transform, an external HTTP
  fan-out: seconds to tens of seconds per record. That is precisely the regime where one-record-at-a-
  time-per-partition consumption collapses, and precisely the workload that has become ordinary since
  the library was written. **The value of lifting the partition ceiling scales with how slow the
  work is**, and the work got slower.
- **Per-key ordering over a keyspace far larger than the partition count.** Per-conversation,
  per-customer, per-account, per-primary-key. The canonical hard case is applying change-data-capture
  downstream in parallel while staying ordered per row — a wall Debezium users hit directly.
- **One topic, many languages, identical semantics.** A Rust worker and a Python worker on the same
  topic with the same ordering and retry guarantees, because neither implements any of it. Today that
  is a rewrite decision; it should be a deployment decision.
- **Workers that come and go.** Leases, heartbeats and epoch fencing mean a dead worker's records are
  reclaimed rather than stranded. That is what makes spot instances, autoscaled pods, short-lived
  functions and — at the far end — browser or edge workers plausible consumers of a Kafka topic.
- **A worker restart stops being a consumer group rebalance — and this is already built.** Owner's
  observation, 2026-08-15: *far* lower-hanging fruit than the Connect work, with a larger impact. The
  sidecar holds the group membership; the application is a client of it. `ReconnectWindow` **holds a
  dropped session's in-flight records** rather than returning them — `CONNECTED`, `HOLDING`, `SPENT`,
  with attempt counts unchanged because no verdict was ever reached. So an application redeploy
  inside the window costs no reassignment, no pause, no re-fetch.

  **It arrived as a side effect.** The window exists to stop two workers holding one key after a
  disconnect; nobody set out to remove rebalances. It is written down here because a capability
  nobody aimed at is the one nobody thinks to claim.

  **Against the existing answer**: static membership (`group.instance.id`) already survives a planned
  restart inside the session timeout — but it needs the application to hold the Kafka client and be
  configured right, and **it loses in-flight work**, since records are simply re-fetched and
  reprocessed. Holding the in-flight set with its attempt counts is the part that is genuinely new,
  and it should be verified against static membership before being claimed.

  **The condition that decides whether any of it is real: the sidecar must outlive the worker.** A
  sidecar container in the same pod restarts with it and delivers exactly nothing. The advantage
  needs an independent lifecycle — a node-local daemon, a separate pod, a shared tier. That is a
  **third independent argument arriving at the same place as §4 and §2b**, which is worth noticing:
  the deployment shape that makes this work is the one the buyer question already pointed to.

  It also cuts across the framing in `STRATEGY.md`: this is a **JVM-user** advantage, so the proxy is
  not only "Parallel Consumer for other languages". Its earmarked question — whether this is that, or
  "the Kafka client for other languages" — gains a third answer: *a deployment shape*.

- **Kafka credentials stop leaving the platform boundary.** A worker needs the sidecar, not the
  brokers: no Kafka client, no broker reachability, no credential distribution. This is worth almost
  nothing to an application team and a great deal to whoever is accountable for the cluster — see §4.

## 4. Why the platform team is the buyer, and the application team is the demand

The observation that this is sold to platform teams is right, and the reason is not that they are
more important. It is that **the value is per-organisation while the cost is per-organisation, and
neither is per-application.**

- **It has an operator.** Someone runs it, gives it credentials, monitors it, upgrades it. A library
  has an importer; infrastructure has an owner, and that owner is the buyer.
- **The benefits compound across teams, not within one.** One team adopting it gets concurrency. The
  platform team adopting it gets: the queue-broker requests stop; retry, dead-letter and observability
  policy live in one place; the "we need more partitions" queue empties; and a uniform answer to
  "can we use Kafka from <language>".
- **Governance is the honest reason.** Brokers stay behind the fence, credentials stay with the
  platform, and every consumer is reachable through one surface that can be instrumented and
  rate-limited. Application teams do not ask for this. Platform teams are measured on it.
- **But the application team feels the pain**, which makes this a bottom-up-demand, top-down-purchase
  shape. The evidence for the pitch is the platform team's own backlog: the bridge somebody built, the
  partition-increase tickets, the three teams that each wrote their own thread-pool consumer.

**The uncomfortable half**: selling to the platform team means the shared-server shape, and §4h is
explicit that a shared server discards all four constraints the sidecar relies on — loopback-only, one
tenant, connection-as-session, credentials-safe-on-the-wire. **The sidecar's operator is the
application team, invisibly, and that is a large part of why it is adoptable at all.** So the buyer
question and the sidecar-or-server question are the same question, and answering "platform team"
answers "server" whether or not anyone meant to.

## 5. The architecture of the shared interfaces

The claim that makes many interfaces defensible instead of reckless: **the semantics live in exactly
one place, and everything else is an encoding of them.**

```
Kafka  ──►  Engine (JVM, core)              ordering, sharding, retry scheduling,
                │                            offset-map encoding, commit decisions
                │
                ▼
           Work seam                        "this record needs processing" →
           (ExternalEngine-shaped)          "someone else will return a verdict"
                │
     ┌──────────┼───────────┬────────────┬──────────────┐
     ▼          ▼           ▼            ▼              ▼
  in-process  gRPC       HTTP/SSE    Dapr component  compatibility
  (JVM)       (built)    (designed)  (adapter)       surface (pixy-shaped)
     │          │           │            │              │
     └──────────┴─────┬─────┴────────────┴──────────────┘
                      ▼
              Client facades — dispatch queue, executor pool,
              verdict channel. Deliberately no ordering logic.
                      │
                      ▼
              Conformance suite — binding = (language, dialect),
              one set of scenarios, one definition of correct
```

Four properties carry the whole design, and each is load-bearing:

1. **One seam.** Every interface hangs off the same internal boundary, so identical semantics are a
   consequence of the structure rather than of anyone's discipline.
2. **Clients are facades.** No shard selection, no retry scheduling, no offset tracking. A client that
   grew any of those would be a second implementation of Parallel Consumer, and there would eventually
   be eleven of them, disagreeing.
3. **Dialects are encodings, not implementations.** gRPC and HTTP differ in framing and nothing else.
   That is what makes a new dialect cheap enough to be worth having.
4. **The conformance suite is the load-bearing part, not the clients.** Its binding key is already
   *(language, dialect)*, so a new dialect is more rows and no new scenarios (§5 of the strategy doc).
   It is what turns "these eleven libraries agree" from a claim into a test result — and therefore
   what makes the architecture above honest rather than aspirational.

## 6. What you would actually build on it

Asked directly, and worth recording because each one is a demo that would land better than a feature
list:

- **An inference queue keyed by conversation** — dozens of concurrent model calls, strictly ordered
  within each conversation, from one topic. Hard today; nearly free here.
- **Ordered per-customer webhook delivery** with retry and backoff, driven from an event stream.
- **Parallel CDC application** to a downstream store, ordered per primary key.
- **A build or test distributor** where the work queue is a Kafka topic and workers are ephemeral.
- **Browser-side work dispatch** — the far end of the reachability argument, and the one that would
  make people look.

## 6b. What else could be layered on the engine

Asked directly 2026-08-15. The pattern behind every item already in flight is the same: **PC owns a
dispatch decision, so anything that wants to influence *which record runs next, where, and how fast*
can be expressed at that seam** without touching the thing above it.

Already in flight, and the proof the pattern generalises: Kafka Streams (astubbs#271), Connect sinks
(astubbs#269), the language proxy (astubbs#242).

Candidates, unranked and none started:

- **Per-key or per-destination rate limiting — and this one is already asked for, so it is not a
  candidate in the same sense as the rest.** `astubbs/parallel-consumer#228` (mirror of
  confluentinc#24, *Add distributed rate limiting support*) is open on the fork, and
  confluentinc#766 is a user arriving with the same need phrased as a question — *how to limit
  message consumption at a certain RPS*. Under §4c's **demand decides** filter that is the strongest
  evidence any item on this page has. The technical case matches: PC already decides when a record is
  dispatched, so a token bucket per shard is an extension rather than a new subsystem, it is what
  every webhook and API-fan-out system builds by hand, and it **composes with retry**, which a
  bolt-on limiter cannot.
- **Generalised scheduled delivery — refuted in the narrow form, 2026-08-15, and demoted.** It was
  written here as the highest-value gap against a queue broker, on the grounds that retry backoff is
  already "run this no earlier than T". **The owner's objection kills it and the objection is
  semantic, not a memory-tuning problem**: PC commits to the *frontier*, the lowest incomplete
  offset, so a record scheduled for next Tuesday pins the commit point until Tuesday. Everything
  completing behind it becomes a gap in the offset map, which is encoded into Kafka commit metadata
  with a hard size ceiling, and the encoded gap grows with throughput for as long as the wait lasts.
  Topic retention bites independently — an eight-day delay on a seven-day topic loses the record.

  So **the cheap half already exists** (short delays, where the frontier moves again soon) and **the
  valuable half requires leaving the frontier model**: commit the record, persist it in a scheduler
  topic or store, re-inject later — at which point it is a durable timer service and the record is no
  longer PC's in-flight work. This is why SQS caps delayed delivery at fifteen minutes and why
  RabbitMQ's delayed messages are a plugin with its own store rather than a queue feature. Keep the
  gap recorded against the queue-broker comparison; drop the claim that PC closes it cheaply.
- **Batch dispatch per key — cheaper than first written.** Core already batches (`batchSize` hands a
  batch to the user function), so the work is **exposing it across the proxy protocol** to the other
  languages rather than inventing it. Verify the core surface before planning it; on that reading it
  is the cheapest item here.
- **A durable-execution layer — and the route runs through Kafka Streams, not through core.** PC
  supplies ordered, retried, at-least-once dispatch; the durable-execution products (Temporal,
  Restate, Inngest, Durable Functions) sell a programming model where workflow code is *replayed*
  from an append-only history after a crash. **That substrate is Kafka already** — append-only log,
  replay, per-key ordering, with the key as the workflow identity — which is the thing Temporal had
  to build on Cassandra and this would not.

  **Kafka Streams supplies three of the four primitives today**: changelog-backed durable state,
  exactly-once as an atomic commit of state, output and consumed offset, and replay from the
  changelog. What it does not supply is the **programming model** — *side-effect memoisation* (Streams
  replay re-runs the processor, so an external HTTP call fires again; only Kafka-side effects are
  covered by the transaction) and *suspension* (a workflow waiting three days for approval has to be
  hand-rolled as a state machine across records, which is exactly the pain the products remove). PC
  adds the fourth thing Streams lacks — concurrency — and astubbs/parallel-consumer#271 already puts
  it inside Streams, so the state store is already where it would be needed.

  **Correction to an earlier claim here**: this was recorded as gated on exactly-once through the
  proxy. That gates the **polyglot** version only, because `ExternalEngine` throws on transactional
  commit mode. The JVM version's real gate is **Streams-side exactly-once**, which
  astubbs/parallel-consumer#271 puts out of scope.

  **Do not build this. Owner's instinct, 2026-08-15, and it is right** — Temporal is a company, not a
  feature, and reimplementing it is not available to this project. Recorded as considered-and-not-
  taken so the idea is not rediscovered as novel. Three findings are worth keeping from the
  consideration:

  - **Temporal does not use Kafka.** Its persistence is Cassandra, MySQL or PostgreSQL, with
    Elasticsearch for visibility. So a team whose events are already in Kafka must *bridge* — consume
    Kafka, start a workflow — which is a second system, a second set of delivery semantics, and a
    join that can lose or double-start.
  - **That bridge is the one structural advantage, and it is the only one worth defending.** On
    Streams the event *is* the workflow start, atomically: the offset commit and the first state
    write are one transaction, so a start cannot be lost or duplicated. Everything else — no new
    datastore, workflow history and event history being the same log — is real but merely convenient.
    Against it sits everything Temporal has that this would not: the UI, visibility and search,
    workflow versioning, signals and queries, child workflows, and SDK maturity across languages.
  - **The smallest useful thing is much smaller than a workflow engine, and should be judged on its
    own**: **memoised steps, and nothing else.** A `ctx.once("charge-customer", () -> …)` backed by a
    state store keyed by record key plus step name, covered by the existing transaction. No
    suspension, no timers, no continuations, no versioning. **PC already retries records, and a retry
    re-runs the whole function including the side effects that already succeeded** — which is the
    commonest practical complaint about at-least-once processing and is hand-rolled per project
    today. Framed honestly it is not durable execution at all: it is **retry that does not redo the
    parts that worked**, it helps every existing user rather than only workflow users, it competes
    with nobody, and it is the foundation the larger idea would need anyway. If anything here is ever
    built, it is this, and only on demand.
- **A first-class dead-letter and retry-topic policy**, owned once rather than reimplemented per
  application.

The filter is unchanged and applies to every one of them: §4c's rule that **demand decides**, and
§4d's that breadth is itself a risk.

## 7. What this synthesis does not settle

Stated plainly so it is not mistaken for a decision:

- **Whether anyone buys it.** This is a customer-discovery question and no amount of architecture
  answers it. §4c's rule stands: **demand decides** — build the thing somebody asked for.
- **Whether somebody already sells it.** Asked directly, and the provisional answer — that the
  "faster Kafka" vendors compete on storage economics and leave the consumption model alone — is
  recorded in
  [`next-architecture-landscape-comparison.md`](next-architecture-landscape-comparison.md), which
  **owns that survey**. It is from memory and unverified; §2b's cost argument depends on it, so it
  needs establishing before either is used publicly.
- **Sidecar or server** (§4h). §4 above pushes toward server; the adoptability argument pushes back.
- **Exactly-once through the proxy**, which any durable-work-queue story depends on, and which is
  post-v6 with core changes sanctioned (§4h).
- **Multi-tenancy in full** — authentication, isolation, quotas, high availability, upgrade paths.
  Not a bigger sidecar; a different product on the same engine.

Anything here that survives scrutiny belongs in `STRATEGY.md`, which owns claims the work must keep
true, and in the announcement
([`release-v6-phoenix-theme-and-announcement.md`](release-v6-phoenix-theme-and-announcement.md)) —
subject to that document's rule that **no claim is made publicly while the code is experimental.**
