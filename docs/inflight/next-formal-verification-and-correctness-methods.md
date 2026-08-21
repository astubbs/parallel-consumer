# Next: formal verification, and correctness methods worth stealing

<!-- inflight-type: feature -->
<!-- inflight-impact: reliability -->

Prompted 2026-08-21 by reading how llingr proves its engine correct
([`market-analysis-llingr.md`](market-analysis-llingr.md)). llingr's published method is unusually
specific, and most of it is applicable here directly. **Everything attributed to llingr below is llingr's
published claim, not something verified by us** - but the *techniques* stand on llingr's own merits
regardless of whether llingr's numbers do.

## The finding that should decide this

Their TLA+ run reports discovering **a real race between offset commit and partition revocation** -
`CommitTick` executing during a revoked window before `MarkPartitionRevoked` completed, producing
silent duplicate processing under specific rebalance interleavings.

**That is the shape of the `confluentinc#857` family** ([`bug-857-family.md`](bug-857-family.md)):
commit and revocation interleaving badly, found here empirically, over months, through chaos runs and
seeded replays. Exhaustive state exploration found the same class of defect by construction rather
than by luck.

That is the argument for doing this, and it is not "because a competitor does it". It is that this
project has an open, expensive, long-running bug family of precisely the kind the technique is good
at, and the empirical route has not closed it.

## Their models are not published - but one write-up is, and it is the useful artifact

Verified 2026-08-21: **no `.tla` or `.cfg` files exist in any of llingr's public repositories**, and no
paper, blog post or specification repository was found. The headline state counts on llingr's site are
unbacked by anything inspectable.

**But `demux/offset/COMMIT_GUARD_ANALYSIS.md` in `llingr-demux` is 351 lines of genuine methodology**,
and it is a better model for what *we* would want to produce than the marketing numbers are. It
names the model files (`OffsetCommitterP3.tla`, `P3r`, `P4`, `P5`), the tool, the runs, and the
counterexample:

> *"**Counterexample trace (Run 1)**: CommitTick fires while `assignment="Revoked"` but
> `committerAssigned=TRUE`, committing zombie offset 0+1=1 when broker was at 2."*
>
> Run 1, commit-after-ack: **FAIL**, 1,123 states, 1.5s.
> Run 2, atomic-before-ack: **PASS**, 285,994,148 states, 5m 24s.

And the engine source cross-references the models inline, e.g. in
`demux/subscription/subscription_rebalance.go`:

> *"TLA+ verification (OffsetCommitterP3r.tla) proved that doing this AFTER ack leaves a window where
> CommitTick can fire with committerAssigned=TRUE, allowing zombie commits that violate
> monotonicity."*

**Three lessons, and the third is the one that transfers even if we never write a model:**

1. **The counterexample is the deliverable.** A failing run of 1,123 states found in 1.5 seconds is
   more persuasive than a passing run of 286 million - it shows the checker doing work.
2. **The models are cited from the code they constrain.** A comment saying *why* an ordering is
   required, naming the artifact that proved it, is durable in a way a design document is not.
3. **Publishing the write-up matters more than publishing the model.** They published the analysis and
   withheld the `.tla`, and the analysis is what a reader can learn from. If we do this, the write-up
   is the product.

## What llingr does, ranked by what it would cost us to adopt

**1. TLA+ on the coordination algorithms only.** Two models - offset committer and worker
coordination - each with safety and liveness, reported at 1.1B distinct states / 4.5B+ transitions /
25 properties. Note the scoping: **llingr did not model the whole system**, llingr modelled the two
places where concurrent interleavings decide correctness. For PC the equivalents are obvious:
`WorkManager`/`PartitionState` commit advancement, and the rebalance drain/revoke path.

Verified properties, which read as a ready-made specification of what PC also needs to be true:
offsets always committed in contiguous ranges; the ready pointer never skips; every processed
message eventually contributes to a commit; worker tokens always acquired and released; no deadlock
in worker creation/destruction; all in-flight messages complete before partition handoff.

**2. Assert the TLA+ invariants directly in unit tests.** Named ones: `GapBufferAhead`,
`ReadyMonotonic`, contiguous advancement. **This is the cheap half and it is worth doing even if the
model is never written**, because it forces the invariants to be *stated* - and a stated invariant is
testable, greppable and reviewable. Today PC's equivalents live in people's heads and in test names.

**Owner's corrections, 2026-08-21 - read before actioning the list below:**

- **We already do large-scale reconciliation, and have from the beginning.** Item 3 below is not a new
  capability for us; what is worth taking is the *per-message hash* for truncation and corruption
  detection, if we do not already have it.
- **We do not need Kafka-client fault injection, and should not add it.** Apache Kafka tests its own
  client. Injecting network faults between our consumer and the broker mostly re-tests someone else's
  code. **What we test is PC** - rebalance storms today, and killing processors mid-flight in the
  upcoming chaos work. That is the layer where our defects live. Item 5's failure matrix is therefore
  a checklist for *ideas*, not a gap to close wholesale, and its broker/network categories are
  largely out of scope by choice rather than by omission.
- **We already had performance regression tests**; the gap is that they were not formal enough to
  catch this. Split out to
  [`next-performance-regression-testing.md`](next-performance-regression-testing.md).

**3. A hash on every message, and a reconciling validator.** Every message carries a SHA-512 so
truncation and corruption are detected automatically, and a validator confirms **every produced
message reached either the primary store or the dead-letter store, with no gaps**. That end-to-end
"nothing was silently lost" check is a different assertion from anything in the chaos suite today,
and it is the one that makes long soak runs meaningful rather than decorative.

**4. Allocation budgets as tests, not benchmarks.** A zero-allocation invariant on the work-item pool
cycle, where a regression **fails a test**. Compare this session's finding that a 35% throughput
regression shipped for five years unnoticed - a performance property that nothing asserted.

**5. Chaos breadth - as an idea list, heavily filtered.** Per the correction above, the broker and
network categories are mostly Apache Kafka's own responsibility and deliberately not ours. What is
worth mining is the *consumer-side* column, which is our layer: Broker: disconnect, quorum loss,
permanent death, rolling restart, leader reassignment, repartitioning, latency spikes, flapping,
split-brain. Network: packet loss, latency jitter, payload truncation, consumer partition, asymmetric
partition. Persistence and dead-letter outages including simultaneous. Consumer pods: hard kills,
graceful restarts, scale up/down, yo-yo, OOM, random chaos agent. Plus combined multi-category
"kitchen sink". Substrate: 3-node broker, dual persistence, Kubernetes (Kind), Toxiproxy.

Of that list the parts on our side of the line are **hard kills, OOM, scale up/down, yo-yo scaling and
a random chaos agent**, plus the combined scenarios. Compare
[`test-chaos-phase2.md`](test-chaos-phase2.md) and `docs/chaos-pain-suite-design`: we do rebalance
storms, and killing processors mid-flight is the upcoming feature. The genuinely new idea is
**continuous yo-yo scaling as a sustained soak** (llingr's cycles consumer count every 6 seconds and
claims ~20bn messages and ~100k scaling events) rather than as discrete scenarios.

**6. Kafka edge cases llingr calls out that most libraries ignore**, and worth checking we handle:
**control record gaps, transaction boundary gaps, and log compaction gaps.** PC has compaction
support (`confluentinc#409`) - the other two deserve a deliberate answer.

**7. Mutation testing.** We already have this - [`ci-mutation-testing.md`](ci-mutation-testing.md) -
so the note here is only that llingr reaches the same conclusion about why coverage is not evidence.

## How to state our own guarantee, which is a documentation problem more than a testing one

llingr's framing is worth copying because it is honest and it is *specific*: graceful operations produce
**zero** duplicates because the drain coordinator commits before releasing partitions; catastrophic
failures produce duplicates **bounded by the in-flight count at the moment of failure**; both match
at-least-once exactly. PC's guarantees are true but are not stated anywhere in that shape.

## Suggested order

The value is front-loaded and the expensive part is last:

1. **Write the invariants down** (item 2) - days, no new tooling, immediately useful to reviewers.
2. **Add the end-to-end reconciliation validator with per-message hashes** (item 3) to the existing
   chaos suite - makes soak runs assert something.
3. **Continuous yo-yo scaling as a soak** (item 5) - the one chaos idea here that is on our side of
   the line and that we do not already do.
4. **Allocation/throughput budgets as tests** (item 4) - see
   [`next-performance-regression-testing.md`](next-performance-regression-testing.md).
5. **TLA+ the two coordination algorithms** (item 1) - the largest investment, and the one with a
   named target: the astubbs#857 family.
