# Standalone deployment: what the product is when it is not inside the application

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - ideas only; it inherits the grant-semantics question and adds a positioning one, and the AWS test constrains it before anything is built -->

From the continuation of the ce-brainstorm session, 2026-09-01, on the owner's idea that if the
scheduler can ingest Prometheus it does not need to live inside every application to do its job.
Sibling of [`core-non-kafka-participants.md`](core-non-kafka-participants.md), which asks how
outsiders enter the resource graph; this note asks the inverse - **whether the runtime itself has
to be in anyone's process.**

## The move

Two halves make the engine work: it has to *see* what is happening, and it has to *change* what
happens next. The corpus assumes both arrive through the client library. The owner's observation is
that neither has to:

- **Seeing** can come from the telemetry a company already runs - mechanism 2 in
  [`core-non-kafka-participants.md`](core-non-kafka-participants.md), the firehose.
- **Changing** can come from the credit-vending surface - mechanism 1 in the same note, which stops
  being a side door for non-Kafka callers and becomes **the actuation surface of the whole product**
  when there is no in-process engine to actuate through.

So a Hasten deployment that hosts nobody's application becomes possible: point it at a Prometheus,
give it an endpoint, and it observes, solves, recommends, and vends. And that reframes the token
endpoint's importance - it is not a market-widener bolted onto the side, it is half of the minimum
viable product for anyone who has not adopted a client.

## Why it matters: it moves the adoption floor, not the feature list

Every rung of the falsification staircase in
[`core-lighthouse-mvp.md`](core-lighthouse-mvp.md), and the lighthouse itself, presumes somebody
adopts a client library and lets it run their consumer. That is the most expensive thing on the
list to ask for and the last thing a stranger grants. **A standalone deployment asks for a socket.**
Whatever it can honestly say from there, it can say to people who have agreed to nothing.

That is a different rung *below* the staircase, not a competing product - and it is the only one
that can be reached without a design partner.

## The tension with embedded-not-cluster, and where the line actually falls

[`core-engine-thesis.md`](core-engine-thesis.md) proposes *embedded, not cluster* as positioning,
and [`core-fleet-capacity-coordination.md`](core-fleet-capacity-coordination.md) claim 7 extends it
to the fleet - "application records never transit it, every runtime decides locally, collectively
one scheduler, no cluster to operate". A standalone Hasten is, plainly, a process to operate. It
needs an answer, and the answer is that the positioning was never about process count:

- **Advising and vending do not violate it.** Records still never transit. A vended credit is
  delegation - the caller spends it locally, the way the Envoy shape requires and the way the
  in-process navigator already works. The standalone deployment is then a *slow-cadence agreement
  point*, which is what the coordination topic already is.
- **Forcing a per-call decision on a caller who wanted delegation does violate it.** An endpoint
  that makes every caller ask "may I proceed?" once per request puts a cluster in a hot path the
  caller never offered, pays the round trip the competitive line refuses ("the acquire never waits"),
  and makes the deployment's availability a liveness dependency nobody signed up for.
- **Offering it to a caller who asks to be paced does not.** Refined 2026-09-02: the paced persona in
  [`core-non-kafka-participants.md`](core-non-kafka-participants.md) registers work and is told *go*,
  per unit, by push. That is per-call decision, and the liveness dependency is *the product* - a
  pacemaker you do not depend on is a paperweight. The caller handed over the hot path on purpose,
  the same way a consumer depends on its broker; the round trip is pipelined away; and the shard
  owner it depends on is embedded and fails over with Kafka ownership, so at worst it is the single
  hop any remote call already costs.

So the line is **forcing versus offering**, not one process versus two and not per-call versus
delegated. The batch-of-one/batch-of-N dial that the participants note leaves undecided is then a
choice between two customers rather than a positioning hazard: delegation for the caller who wants
to own its spend, pacing for the one who wants to own nothing. Holding the *default* at delegation
still stands, because it is the shape that costs the caller no dependency.

[`web-control-plane.md`](web-control-plane.md) already drew the compatible shape one notch back: the
control plane is a separate application that *may* run embedded in the PC instances. This is the same
question asked where there are no PC instances to embed in.

## Vantage is what you are buying, and the tiers are not nested

The tempting reading - standalone is the embedded product minus some features - is wrong in both
directions.

**It is weaker where the corpus says the vantage is the whole point.**
[`core-auto-scaling.md`](core-auto-scaling.md) is explicit that dimension 2's vote beats lag-based
autoscaling because an engine inside the loop can distinguish local capacity exhausted / downstream
saturated / no exploitable key parallelism left - "the signal no external autoscaler can construct".
Scraped metrics reconstruct that only as far as the application already exports it, which for most
is not at all. A standalone recommender that does not say so is selling the state of the art it
exists to beat.

**It is stronger where no in-process engine can see either.** One embedded instance knows its own
partitions; it does not know that the database is the binding constraint across three applications,
two of which are not its own. That is the claim-6 argument in
[`core-fleet-capacity-coordination.md`](core-fleet-capacity-coordination.md) - moving spend toward
the current *global* constraint - and it is a **breadth** signal, not a depth one. Scraping is a
perfectly good way to acquire it.

So the honest shape is **wide-and-shallow versus narrow-and-deep**, and a claim made from either
tier has to name which one it came from. The owner's standing ruling applies -
partial coverage beats none - but a recommendation has to carry its provenance or it will be
believed at the wrong strength.

## The token endpoint is how the shallow tier climbs

The nicest part, and the reason the two halves are not independent. A vended credit is not just
actuation; it is **observation of a kind scraping cannot buy**:

- The ask is a *declared intention*, timestamped, attributed, before the work happens.
- The wait is *demonstrated convertible demand* - already the load-bearing argument in
  [`core-non-kafka-participants.md`](core-non-kafka-participants.md).
- The completion report is a *measured cost* against a named resource.

That is per-call ground truth, volunteered from outside the application, in the same units the
in-process engine uses. **The broker is the mechanism by which an agentless deployment earns back
the fidelity it gave up** - and it earns it one integration at a time, which is exactly the
incremental shape the adoption ramp below wants.

## The third topology: bundled - and the broker inside the library

Added 2026-09-03, from the owner. The record so far has two topologies: **embedded** (in your
application) and **standalone** (beside your stack). This adds a third - **bundled**: a Hasten
distribution with Kafka inside it, run by people who want the runtime and do not want to run Kafka.
Prior art: nothing on any ref proposes it, and the handoff's product shape says the opposite in as
many words - *"a globally coordinated runtime with no runtime cluster"*. So this is an inversion of
the record, taken deliberately, and it does not disturb the other two: nobody who has Kafka is asked
to run anything new, which is the forcing-versus-offering line above applied once more.

**The persona, settled with the owner: the appliance.** Not the durable-execution buyer (Temporal,
Restate), not a Kafka-distribution buyer, but the standalone persona above who will not stand up
Kafka to get a scheduler and a token broker. That collapses standalone and bundled for them - the
standalone process *is* the bundle - and it bounds what the bundled broker has to carry: **Hasten's
own topics only** (coordination, catalog, resources, skipped state), which are small and
low-throughput. Application records never transit it, per
[`core-fleet-capacity-coordination.md`](core-fleet-capacity-coordination.md) claim 7. Embedding a
broker to carry control state is a different proposition from embedding one to carry traffic.

**Two things the bundle must be, from rules already on the record.** The Kafka inside is stock and
replaceable ([`core-nile-boundary.md`](core-nile-boundary.md): never replace the specialist), so
bring-your-own-Kafka stays a configuration switch and the bundle is packaging, never a fork. And it
passes [`core-internal-machinery-as-features.md`](core-internal-machinery-as-features.md)'s AWS
test only through the dev loop: the lighthouse and every quickstart need a Kafka that exists with
zero setup, so a single-process Hasten-with-a-KRaft-node is the artefact built regardless, and
production-hardening it is the marginal step. KRaft combined mode - broker and controller in one
JVM - makes the single process real today.

**Self-hosted bundling hides Kafka's name, not Kafka's operations.** Storage, replication,
retention and upgrades are all still the operator's; the tin says Hasten. The persona's actual
sentence - *does not want to run Kafka* - is delivered only by a **hosted offering**, which makes
"hosted, perhaps?" the load-bearing half of the idea rather than an afterthought, and a business
rather than a feature. It is recorded as the second commercial-shape candidate beside the
OSS/enterprise split in [`core-engine-thesis.md`](core-engine-thesis.md).

### The broker inside the client library - an open question, not a verdict

The owner's follow-on: what happens if the broker is embedded in the client library itself, so
three instances of your application *are* a three-node cluster and there is no separate thing to
run at all? That is "Kafka as the only cluster" read to its limit - **no cluster at all**, the
strongest possible form of embedded-not-cluster, and a genuinely new answer to the line above.
Recorded as a question with its considerations, at the owner's request:

- **With a stateful broker, application lifecycle and storage lifecycle collide.** A broker expects
  stable identity, stable storage and a lifetime of months; an application is redeployed,
  autoscaled and killed. A rolling deploy becomes a partition reassignment, a scale-in a
  decommission, the pod needs a volume and a stable name, and a pause or crash in *your* code takes
  a broker with it. Hazelcast's embedded mode is the precedent to study: "your app is the cluster"
  was its original shape, and client/server mode was added because of rolling deploys. It also
  puts a floor under scale-in - instance count cannot drop below replication factor - noted in
  [`core-scale-in-proof.md`](core-scale-in-proof.md).
- **With a diskless broker, most of that dissolves.** If the log lives in object storage and the
  in-process broker is a stateless cache and coordinator, it can live and die with the application.
  The cost is produce latency in the hundreds of milliseconds, which is the wrong trade for
  streaming and may be the right one for the appliance persona, who is doing grants and
  coordination. The upside nobody stateful gets: the instance that owns a partition for scheduling
  also holds its data locally - scheduler, ownership and log co-locate. **Not available in Apache
  Kafka yet**: KIP-1150 (diskless topics) was accepted 2026-03-02 as an umbrella; its implementation
  KIPs are still under discussion and the only running implementation is a vendor fork. The
  direction is voted, the code is not.
- **The alternative to weigh against both: embed Raft, not Kafka**, for coordination topics only -
  far less baggage. Against it, law 4 in [`../w2-vision.md`](../w2-vision.md): one implementation
  of intelligence. The coordination plane is written against Kafka topics; an embedded Kafka keeps
  one substrate, an embedded Raft is a second.

What is not decided is which of these the appliance persona actually needs, and whether the
question is worth answering before diskless Kafka exists to answer it with.

## Which features survive the boundary - a test, not a list

The owner's open question is what else works without being inside. A list would rot; the test is:
**does the feature need to see a record, or to change what a record does?**

A first pass at sorting the corpus by it, offered to be corrected rather than relied on:

- **Survives outside, at named fidelity.** Capacity fingerprinting
  ([`core-capacity-fingerprinting.md`](core-capacity-fingerprinting.md)) - resource behaviour over
  time is what a metrics store already holds, so this one may survive nearly intact. Bottleneck
  attribution ([`core-bottleneck-attribution.md`](core-bottleneck-attribution.md)) - *across*
  applications yes, *inside* one no. Retry economics
  ([`core-retry-economics.md`](core-retry-economics.md)) where retry counters are exported.
  Cost-to-SLO projection. The partition advisor
  ([`core-partition-advisor.md`](core-partition-advisor.md)) partially - lag, partition count and
  member count scrape fine, but "exploitable key parallelism remains" is the input that makes it
  more than a lag heuristic, and that input is in-process.
- **Needs the handshake, so the token surface is the prerequisite.** Distributed throttling,
  shared execution resources, per-function arbitration, tenant quotas and priorities - everything
  that has to *withhold* something.
- **In-process only, and probably permanently.** Prescience, decision lineage, the
  execution-opportunity model, frontier handover, queue disciplines - anything phrased in terms of
  records, ordering domains or the commit frontier. Scale-in proof too: it works by *constraining*
  the fleet, which nothing outside can do.

The first bullet is the standalone product. The second is what the endpoint unlocks. The third is
what the client library is still for, and it is worth being able to say that sentence to a
prospect.

## The persona this actually serves

Not only "does not use Kafka". Equally: **uses Kafka, and will not hand over the consumer.** That
is a reasonable position from a stranger and there is currently no answer to it. The four mechanisms
in [`core-non-kafka-participants.md`](core-non-kafka-participants.md) turn out to be an ordered
ramp, each rung buying more fidelity for more trust:

```
observe (scrape)  ->  recommend  ->  vend credits  ->  intercept  ->  embed
```

Nobody has to start at the end, and every rung is useful on its own. That is a better adoption story
than the library has ever had, and it costs nothing to tell because all four mechanisms were already
on the list.

## Positioning: a fourth disjoint set, and the trap in it

The sibling note names three disjoint product sets - rate limiters, resource optimisers, Kafka
runtimes. Observe-and-recommend adds a fourth: the AIOps / cloud-cost-recommendation category. The
owner's little-foot ruling covers entering it, and the reasoning is unchanged.

**The caution is that advisory-only is the weakest and most crowded form of the product.**
Recommendation engines are a commodity; what is not is being able to *act* - exactly, locally,
through a credit nobody else can vend. So the standalone tier should be framed as the on-ramp that
ends at actuation, never as a product that stops at advice. Recommending is how it earns the right
to be believed; vending is what it is for.

## The guard, from a test already on the record

[`core-internal-machinery-as-features.md`](core-internal-machinery-as-features.md)'s AWS test says
the machinery must be needed internally regardless, so exposing it is marginal cost and never a
build-to-sell. Standalone passes **only while it is assembled from parts that exist for other
reasons** - telemetry ingest (needed to cover the non-Kafka half), the solver, the recommendation
logic (auto-scaling dimension 2), the credit ledger. The packaging is new; nothing under it is.

**The moment a standalone-only feature is proposed, the test has fired** - that is a separate
product being built to sell, and it should be recognised as one rather than arrived at by drift.

## What is deliberately not decided

- Whether the standalone deployment ever *decides*, or only advises and vends. The batch dial above
  is the same question wearing different clothes.
- Which telemetry systems, what the ingest looks like, and whether recommendations are published
  back into the same system (a Prometheus that scrapes us as well) or somewhere else.
- Whether this is a deployment mode of one artefact or a second artefact. Recording it as a mode is
  the cheaper assumption and nothing yet argues against it.
