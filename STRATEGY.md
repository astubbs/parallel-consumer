---
name: Parallel Consumer
last_updated: 2026-08-18
---

# Parallel Consumer Strategy

## Target problem

Teams processing Kafka records where concurrency is welded to partition count, and one slow
record blocks everything behind it in its partition. Adding partitions is often prohibitive and
still doesn't remove the head-of-line block - and a single-partition topic can't be sped up at
all. Share Groups decouple scaling from partitions but deliver out of order, *and* cannot
acknowledge inside a transaction - so nothing else gives low latency, guaranteed per-key
ordering, and exactly-once at the same time.

And even where parallelism is unlocked, teams must fix concurrency and instance counts at
deploy time - quantities only the runtime data can answer. The guess is wrong in both
directions: too low silently leaves throughput on the table, too high floods downstream
systems - and it goes stale as the workload shifts.

## Our approach

We win by doing it in the client. Modifying the broker is extremely difficult, politically and
engineering-wise, and no broker-side answer to high-performance key ordering exists or may ever.
Parallel Consumer works like a client-side sub-broker: a library you add to a pom, invisible to the
cluster, needing no broker version, no feature flag, and nobody's permission to deploy.

The client is also where the ground truth lives: per-record timings, failures, key-level
ordering state. So the second half of the bet: the engine measures and decides at runtime what
configuration used to guess. External autoscalers see a black box and scale on consumption lag;
an engine inside the processing loop can tune itself - and tell infrastructure when more of it
would actually help.

## How it is maintained

A claim about the fork, not the library - and one that decides whether anyone should adopt it.
Upstream is abandoned. A revived fork is only worth depending on if the revival is more durable than
the thing it replaced, and "an AI wrote a lot of code quickly" is a reason to trust it *less*, not
more.

**The bet: every failure is converted into a mechanism, so the fork's reliability compounds instead
of depending on whoever is paying attention.** A fixed bug that leaves no gate behind is a bug the
next person re-introduces. So the work is not finished when the tests pass; it is finished when
someone can name how it would have gone red had it been wrong.
[`docs/compound-engineering.md`](docs/compound-engineering.md) owns the loop and its worked chain;
[`docs/agent-harness.md`](docs/agent-harness.md) owns the layer that gives a rule teeth.

**The second half of the bet is about working memory.** Every codebase carries knowledge that is not
in the code - what was tried and abandoned, which test lies, which branch must not merge before
something else happens. A team absorbs that by osmosis; an agent arrives with the code and nothing
else, every session. So the fork treats that knowledge as an artefact rather than a culture:
`docs/inflight/` is a structured wiki of what is true about the code right now, delivered into an
agent's context at session start rather than waiting to be searched for. Whether that generalises
beyond this repo is untested - but a revived fork with no team is the case that most needs it.

What that looks like in practice, and how it would be falsified:

- **Defects arrive with a reproduction that is proven to fail without the fix**, not merely a test
  that passes with it. Falsified by a regression test that stays green when its fix is reverted -
  which has happened here and was caught by control arm.
- **A green check that asserts nothing is treated as an outage.** A mutation lane scoring zero
  mutants and a self-test suite printing `FAIL` while exiting `0` were both found and fixed;
  `misdirection` is the highest-ranked class of open work, above data loss, because everything else
  is measured through the instruments.
- **What was learned is written where the next agent will meet it**, not where someone would have to
  know to look. Falsified by a rediscovery - a problem solved twice because its write-up existed and
  was never opened.

The risk this carries, stated plainly: **volume is not evidence.** The same mechanisation that
produces a fix, its reproduction, its guard and its write-up in one sitting can produce four
plausible artefacts built on one wrong premise. The mitigations are the control arm, the negative
control, and a human who refuses the first confident answer - all three earned their place by
catching real errors, repeatedly.

## Who it's for

**Primary:** Teams whose downstream - a service, or just a processing step - scales further
horizontally than their broker partitions do, and who need per-key ordering while it happens.
They're hiring Parallel Consumer to decouple how fast they process from how many partitions they
have, without giving up the ordering guarantee Kafka gave them.

## Key metrics

- **Head-of-line blocking avoided** - per partition, highest completed offset minus highest
  sequential succeeded offset: the records processed that vanilla Kafka would still be waiting
  on. Derivable today from two existing gauges; not emitted as its own meter.
- **End-to-end record latency, median and p99** - poll to completion, not just user function
  time. Not measured today - `pc.user.function.processing.time` covers only part of it.
- **Discovered concurrency vs sustainable ceiling** - does the engine find and hold its
  plateau? Regresses if the controller hunts, oscillates, or undershoots. Until the
  self-tuning controller ships, read as achieved fan-out vs configured max (partly derivable
  from `pc.shards` and `pc.inflight.records`).
- **Production deployments with a public story** - the lagging signal that the library is trusted
  in anger. Counted by hand.

## Tracks

### Performance

The main track. Minimum per-record latency and maximum concurrency, including the offset encoding
and buffering work that sets the ceiling.

_Why it serves the approach:_ The client-side bet only pays if the client is fast - a sub-broker
that adds latency has no reason to exist.

### Self-tuning

Priority raised 2026-08-18. The engine discovers its own concurrency from runtime measurement
and recommends its own instance count to infrastructure - including the signal integrity work
(accurate timing under every engine) that the controller stands on.

_Why it serves the approach:_ The client-side vantage is the moat here - per-record ground
truth no external controller can see. Paired with key-ordered concurrency, runtime-discovered
scaling is the capability nothing else in the ecosystem offers.

### Reliability

Bug squashing, with a bias to the correctness bugs: stalls, rebalance handling, offset tracking.

_Why it serves the approach:_ This bet asks users to trust a library with delivery semantics the
broker normally owns, and every lost-record bug is a withdrawal from that one account.

### Observability

Metrics that actually exist end-to-end, plus a web GUI to see inside a running PC.

_Why it serves the approach:_ Moving the queue into the client moves it out of the cluster's view
- PC's state lives in a JVM where standard Kafka tooling cannot reach it, so visibility is the
bill that comes with the choice.

### Flexibility

Let users process records how they want: richer batch modes, and candidates like an HTTP endpoint
server.

_Why it serves the approach:_ A broker has to be generic; a library living inside your
application does not. This is where the backflips are.

## Marketing

**One-liner:** Like a client-side sub-broker that can do backflips.

**Lead with the combination nothing else has: exactly-once, massively parallel, and optionally
key-ordered.**

Each half is unremarkable alone. Kafka has had exactly-once since KIP-98, and KIP-932 Share Groups
now scale consumers past the partition count. Having both at the same time is not available
anywhere else, and that is the line to put in talks, posts and the README's opening rather than
leaving it as a row two screens down a comparison table.

It holds because the broker-native answer to parallelism gives up exactly-once **by protocol, not by
omission**. [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka):

> "Although it is possible to read transactionally written records, the current protocol does not
> include the ability to acknowledge message delivery within an atomic transaction."

> "This means that the delivery behavior is at-least-once."

The mechanism: exactly-once processing needs the consumer's *offset* commit to join the producer's
transaction, and a share group has no offset to contribute - its state is per-record acknowledgement
state held broker-side, which nothing can enlist in a transaction. The KIP lists exactly-once only as
possible future work. Two details worth keeping straight when writing about this: isolation level is
a **group-level** setting (`share.isolation.level`), not per-consumer; and the delivery counts behind
poison-message protection are themselves not exactly-once, so the KIP says they "cannot be relied
upon to be precise".

### Verified - and the verification found two real defects on the way

**Say it exactly as loudly as it is verified.** This is a promise about delivery semantics, and the
README already warns that EoS does not prevent duplicate *replay*. An overstated headline here is the
kind of claim that costs trust rather than winning it.

The validation is `docs/plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md`, which
enumerates every documented transactional guarantee and proves or refutes each one, with a negative
control required before any claim counts as proved. That gate has now fired against us, so this
section is written down as the finding rather than as an aspiration:

**Crash and replay, both batch sizes: the guarantee holds.** An abandoned transaction is invisible,
the replay commits results and their source offset as one set, and the output topic holds each result
exactly once. Proved with observed controls (`TransactionalCrashReplayIT`).

That took a real defect out of the path first, which is the part worth telling honestly. At
`batchSize >= 2` the consumer used to **stall outright** - the produce lock was taken once per poll
context but released per record, the failed release failed the whole batch, and because only a
*success* marks a partition dirty, no commit was ever attempted. The source offset froze at 3 of 201.
That was found by this suite before the fix landed, so astubbs#257 is not a fix we assumed works: the
same test went from RED 5/5 to GREEN 5/5 across it.

A second defect was found the same way and fixed in astubbs#261. When one send in a
`pollAndProduceMany` result set failed terminally, the records already accepted stayed in the
transaction and the next commit published them, so a `read_committed` consumer saw a **partial**
result set for one source offset - 2 of 5. `ProducerManager` installed a producer `Callback` that
throws from `onCompletion`, which pre-empted Kafka's own `maybeTransitionToErrorState` and left the
transaction un-abortable. Both affected claims - C7 `PRODUCE_MANY_ALL_OR_NONE` and C2
`ALL_OR_NONE_PER_SOURCE_OFFSET` - were `REFUTED` and now read `PROVED`.

**So the headline is defensible unqualified: exactly-once, massively parallel, optionally
key-ordered.** Every documented guarantee in the register is proved or attributed, none refuted.

Two things to keep honest when using it. The claim is about Kafka's own topics: the README's existing
warning that EoS does not prevent duplicate *replay* into external systems still stands, and this
work does not touch it. And the register - not this section - is the gate. If a claim is ever refuted
again, this section is the first thing to revisit, exactly as it was the first thing revisited when
one was.
