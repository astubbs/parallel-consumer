# The engine thesis: STRATEGY.md explains v6, and the work has overtaken it

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

Source: an external-model strategy review (Codex) over the weekend of 2026-08-22/23, run against
this repo with the `research/market-analysis-recut` STRATEGY.md in view. This note is the breakdown
of that conversation's central claim; its satellite ideas have their own notes
([`core-alternate-api-facades.md`](core-alternate-api-facades.md),
[`core-spring-kafka-integration.md`](core-spring-kafka-integration.md),
[`docs-research-program.md`](docs-research-program.md),
[`docs-content-series.md`](docs-content-series.md),
[`web-three-reveal-demo.md`](web-three-reveal-demo.md)).

## The claim

STRATEGY.md describes tracks well but never states what the tracks are consequences OF, so the
repository reads as feature sprawl - Streams, Connect, polyglot, GUI, self-tuning each look like an
independent bet - when they are one thesis applied at different seams. The proposed fix is a short
section immediately above `## Tracks`: what PC is becoming, deliberately written so it claims no
experiment's outcome.

**The four-decision model, which is the candidate centre of the whole document:**

```
partitions     -> where work is owned
keys           -> what work may execute independently
engine         -> how much of that parallelism to use
infrastructure -> how many engine instances exist
```

The first is Kafka's job. The other three do not inherently belong to the partition. Kafka's
historical accident is letting one number (partition count) answer all four, and every track is the
removal of that coupling at one seam. The compact form: **ownership and execution are independent.**
A consequence worth stating in the doc: if the Streams work succeeds, partition count is demoted
from an application-performance parameter to a data-distribution parameter.

**The taxonomy that makes the repo cohere** (front ends are views onto one engine):

```
FRONT ENDS   PC API · KafkaConsumer-shaped · ShareConsumer-shaped · Streams · Connect · Dapr · language SDKs
ENGINE       key-ordered scheduling · virtual threads · retries/batching · offset frontier · EOS
CONTROL      adaptive internal concurrency · throttling · external scaling recommendation
BOUNDARIES   JVM direct · native FFI · sidecar RPC - three ways to cross ONE boundary, not three products
OPERATIONS   metrics · web GUI · health
```

**The acceptance test for any proposed integration**, which tells you what NOT to build: *what
unnecessary coupling does this remove?* If the target already exposes concurrency independently of
ownership (RabbitMQ, Pulsar), PC has nothing profound to add there; if it forces partition-shaped
execution (Spring Kafka listeners, Streams tasks, Connect tasks), it is a legitimate seam. With the
current agent throughput, this filter is a merge gate as much as a strategy point.

**Positioning line to add:** embedded, not cluster. The project is not trying to become
Flink/Dataflow; its advantage is that sophisticated processing remains the application's own
deployment, with Kafka as the only cluster.

## The specific gaps named in the current STRATEGY.md

1. **No Kafka Streams presence at all** - while
   [`next-what-survives-share-groups.md`](next-what-survives-share-groups.md) concludes the Streams
   work "moves up the priority list" and
   [`pr-strategy-doc-merge-triggers.md`](pr-strategy-doc-merge-triggers.md) already lists the
   Streams branches as strategy-touching. The doc's thesis is still "a smarter consumer".
2. **Polyglot Streams absent** - astubbs#334 (Streams described from Python, run in a JVM engine) is
   the most differentiated composition of the two experiments and the "Other runtimes" section
   predates it.
3. **Self-tuning track blurb does not carry the two-loop shape** - fast loop: instance discovers
   sustainable internal concurrency; slow loop: fleet decides instance count.
   [`core-auto-scaling.md`](core-auto-scaling.md) has the full design; the strategy summary should
   compress it, not omit it.
4. **No embedded-vs-cluster positioning.**
5. **"The v1 framing is..." conflates release framing with strategy.** Keep the conservative
   release scope, but say so: *the v1 release framing is deliberately narrower than the
   architectural possibility.* Otherwise the doc contradicts itself the moment the thesis section
   exists.

## What the review got wrong, so nobody inherits it

- It believed no inflight note existed for the `features/consumer-interface` branch. One does:
  [`next-expose-consumer-and-admin-apis.md`](next-expose-consumer-and-admin-apis.md), including the
  producer-facade rejection record.
- It presented the two-loop controller as "not yet articulated". It is - in
  [`core-auto-scaling.md`](core-auto-scaling.md), down to delta votes and hysteresis. The gap is
  only STRATEGY.md's summary of it.
- Its proposed near-term order (remote platform, then adaptive concurrency, then cheap facades,
  then Streams) is a re-derivation of the sequencing the open PRs already imply, not a correction
  to it. Ranking stays owned by [`process-candidate-ranking.md`](process-candidate-ranking.md).

## Delete when

STRATEGY.md carries the thesis section (or the owner rejects it) and the five gaps are either
reflected or explicitly declined. The satellite notes have their own lifecycles.
