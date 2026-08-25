# Next: exactly-once for the async engines

<!-- inflight-type: next -->
<!-- inflight-impact: architecture -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21. **`ExternalEngine` rejects transactional commit mode outright**, so Vert.x, Reactor,
Mutiny and the proxy cannot do exactly-once at all:

```java
if (options.isUsingTransactionCommitMode()) {
    throw new IllegalStateException(msg("External engines (such as Vert.x and Reactor) do not support transactions / EoS ({})", ...));
}
```

**That restriction is about to get more expensive.** The measurements say the async engines are the
only ones that escape the platform-thread ceiling
([`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md)), and the
documentation is being rewritten to steer people towards them
([`next-docs-publish-the-engine-comparison.md`](next-docs-publish-the-engine-comparison.md)). **So the
faster path and the exactly-once path are currently disjoint, and users are about to be pushed onto the
one that cannot do EoS.**

## Why it is blocked, stated as a cause rather than a module fact

**You cannot hold a producer transaction open across a completion you do not control, for an unbounded
time.** The transaction spans a batch; the commit happens on the control loop. If a record completes
asynchronously, the transaction has to stay open until it does - and nothing bounds when that is.

Stated that way it stops being surprising that Vert.x specifically is excluded, and it points at what a
solution has to do: **bound the window.**

## The shape of a solution

**Latch groups of work together.** Rather than a transaction spanning whatever the control loop happens
to have in flight, form an explicit group, hold the transaction for that group only, and commit when
every future in it has completed - or when a deadline expires, whichever comes first.

That converts an unbounded wait into a bounded one, which is the whole difficulty. The cost is
latency: a group commits at the speed of its slowest member, so group size trades throughput against
commit latency in a way the synchronous path never had to.

**Open questions this note does not answer:**

- **What forms a group** - a poll batch, a time window, a count, or a per-partition boundary?
- **What happens to a future that misses the deadline.** Abort the whole group and redeliver, or commit
  without it and let it redeliver alone? The first is simpler and wastes work; the second may not be
  expressible in one transaction.
- **How this interacts with the retry system**, where a failed record is deliberately deferred past any
  reasonable group deadline.
- **Whether the group boundary can be per-partition**, since Kafka transactions are per-producer, not
  per-partition.

## Why it may be worth doing anyway

**Nothing here makes transactions impossible under async completion** - it makes holding one open
across an *uncontrolled* callback impossible, which is much narrower. An engine offering both the
concurrency and the guarantee would have no equivalent, and the two-axis trade in the engine comparison
is currently the honest answer precisely because nobody has built one.

**It is also the natural home for a liveness policy.** A future that never completes pins a record
forever, and neither core nor the Vert.x path has a timeout today. A group deadline is that policy,
arriving for a different reason.
