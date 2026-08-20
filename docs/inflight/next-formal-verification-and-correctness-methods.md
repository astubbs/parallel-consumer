# Next: formal verification, and correctness methods worth stealing

<!-- inflight-type: feature -->
<!-- inflight-impact: reliability -->

Prompted 2026-08-21 by reading how llingr proves its engine correct
(the llingr market-analysis note). Their published method is unusually
specific, and most of it is applicable here directly. **Everything attributed to them below is their
published claim, not something verified by us** - but the *techniques* stand on their own merits
regardless of whether their numbers do.

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

## What they do, ranked by what it would cost us to adopt

**1. TLA+ on the coordination algorithms only.** Two models - offset committer and worker
coordination - each with safety and liveness, reported at 1.1B distinct states / 4.5B+ transitions /
25 properties. Note the scoping: **they did not model the whole system**, they modelled the two
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

**3. A hash on every message, and a reconciling validator.** Every message carries a SHA-512 so
truncation and corruption are detected automatically, and a validator confirms **every produced
message reached either the primary store or the dead-letter store, with no gaps**. That end-to-end
"nothing was silently lost" check is a different assertion from anything in the chaos suite today,
and it is the one that makes long soak runs meaningful rather than decorative.

**4. Allocation budgets as tests, not benchmarks.** A zero-allocation invariant on the work-item pool
cycle, where a regression **fails a test**. Compare this session's finding that a 35% throughput
regression shipped for five years unnoticed - a performance property that nothing asserted.

**5. Chaos breadth, as a checklist to audit ours against.** Broker: disconnect, quorum loss,
permanent death, rolling restart, leader reassignment, repartitioning, latency spikes, flapping,
split-brain. Network: packet loss, latency jitter, payload truncation, consumer partition, asymmetric
partition. Persistence and dead-letter outages including simultaneous. Consumer pods: hard kills,
graceful restarts, scale up/down, yo-yo, OOM, random chaos agent. Plus combined multi-category
"kitchen sink". Substrate: 3-node broker, dual persistence, Kubernetes (Kind), **Toxiproxy** for
network fault injection. Compare [`test-chaos-phase2.md`](test-chaos-phase2.md) and
`docs/chaos-pain-suite-design` - the gap to look at first is **network-level fault injection**, which
we do not do.

**6. Kafka edge cases they call out that most libraries ignore**, and worth checking we handle:
**control record gaps, transaction boundary gaps, and log compaction gaps.** PC has compaction
support (`confluentinc#409`) - the other two deserve a deliberate answer.

**7. Mutation testing.** We already have this - [`ci-mutation-testing.md`](ci-mutation-testing.md) -
so the note here is only that they reach the same conclusion about why coverage is not evidence.

## How to state our own guarantee, which is a documentation problem more than a testing one

Their framing is worth copying because it is honest and it is *specific*: graceful operations produce
**zero** duplicates because the drain coordinator commits before releasing partitions; catastrophic
failures produce duplicates **bounded by the in-flight count at the moment of failure**; both match
at-least-once exactly. PC's guarantees are true but are not stated anywhere in that shape.

## Suggested order

The value is front-loaded and the expensive part is last:

1. **Write the invariants down** (item 2) - days, no new tooling, immediately useful to reviewers.
2. **Add the end-to-end reconciliation validator with per-message hashes** (item 3) to the existing
   chaos suite - makes soak runs assert something.
3. **Add network fault injection** (item 5) - the biggest gap in our chaos coverage.
4. **Allocation/throughput budgets as tests** (item 4) - would have caught this session's regression.
5. **TLA+ the two coordination algorithms** (item 1) - the largest investment, and the one with a
   named target: the astubbs#857 family.
