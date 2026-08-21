# Open threads from the performance + market session (2026-08-20/21)

<!-- inflight-type: register -->

A register, not a backlog: the session raised more questions than it closed, and they were raised
faster than they were answered. **This is the list to work down in order.** Each item says where the
real content lives - this file holds no analysis of its own, so it cannot go stale in the way a second
tracker does. **Delete an item when it is done; delete the file when the list is empty.**

Branches in play, all stacked: `feats/classic-vertx-demo` -> `perf/throughput-regression-since-0-3`
-> `research/market-analysis-llingr`.

## Open

1. **Show the conformance/feature comparison.** Owner asked to see it, current master only, versus
   llingr. Written in [`market-analysis-llingr.md`](market-analysis-llingr.md) - needs rendering or
   confirming that a table in the note is what was wanted.
2. **Benchmark llingr privately.** Owner's decision: include it in our benchmarking for research, not
   for publication. llingr's benchmarks are published and re-runnable
   (`github.com/llingr/llingr-demux/benchmarks`), and `bench/run-bisect.sh` is the local half. Note
   the licence-key gate on their JVM artifacts; the Go engine is AGPL and public.
3. **Which is used more, Kafka Connect or Kafka Streams?** Owner's question, and it ranks the two
   0.6.0.0 previews against each other - `connect-integration-preview` and
   `streams-parallelism-preview`. Unanswered.
3a. **Settle the licensing question** - [`next-licensing-strategy.md`](next-licensing-strategy.md).
    The first question in it is whether anything is actually being protected, since PC is a library
    rather than a service and BSL's core use case may not apply.
3b. **Add llingr as a benchmark arm** -
    [`next-benchmark-llingr-as-a-baseline.md`](next-benchmark-llingr-as-a-baseline.md). Sweeps the
    per-record delay axis to turn "Go is faster" into a measured constant.
4. **Should auto-scaling be a roadmap entry?** `roadmap.yaml` has none, while it is named as the
   differentiator. Raised in the competitor note's roadmap section; the decision is the owner's.
5. **Narrow the 0.3.2.0 -> 0.4.0.0 cliff to a single commit.** 19 commits; the cause is understood
   (`ExternalEngine`'s two overrides) but not attributed to a commit. `mvn install` per commit makes
   this bisectable now.
6. **Diagnose core's own ~10% decline**, and its 0.5.2.4 -> 0.5.3.2 drop. Core has the same
   trough-and-recovery shape as Vert.x at a third the amplitude, so the cause is in shared code.
   Untouched.
7. **Attribute the 0.5.2.0 -> 0.5.2.4 recovery.** A stated prediction (the `Instant.now()` comparator
   fix, `202efcaac`) was **refuted** - the recovery lands at 0.5.2.4, not 0.5.2.3. Leading candidates
   are the two collection refactors `f06c26fc8` and `b74314d0f`.
8. **Finish the `kafka-clients` pinned dimension.** The sweep that separates client version from PC
   version was started and never completed; the vanilla control arm makes it lower priority, not
   unnecessary.
9. **Is the removal of the `ExternalEngine` overrides SAFE?** The v6 release gate. It is worth 35% and
   does not breach `maxConcurrency`, but a deep pipeline queued ahead of an external engine may
   interact with rebalance revocation, the astubbs#857 family, or shutdown draining. **Correctness
   question, not a performance one.**
10. ~~Confirm PC handles control-record and transaction-boundary gaps deliberately.~~ **RESOLVED
    2026-08-21** - all three gap causes are handled, two of them deliberately. See
    [`next-llingr-questions-and-answers.md`](next-llingr-questions-and-answers.md). The investigation
    also found a new narrow defect:
    [`bug-compacted-phantom-on-poll-batch-boundary.md`](bug-compacted-phantom-on-poll-batch-boundary.md).
11. **The demo work itself** - the reason the session started.
    [`branch-classic-comparison-demo.md`](branch-classic-comparison-demo.md) has sixteen decisions
    recorded and almost none built. Blocked proxy-side on U10.
12. **Archive and delete `origin/presentation`.** Decided: tag under `archive/`, then delete the
    branch. The rescue is pushed, so this is unblocked.
13. **Remove the untracked `bench/` directory from the `classic-vertx-demo` worktree.** It was left
    there so a running sweep would not lose its script mid-run. Housekeeping only.
14. **Build the branch-accounting check.** 109 pre-2026 branches are accounted for by nothing; see
    `next-fork-branch-archaeology.md` on `docs/fork-branch-archaeology` and its handoff.

## Closed since opening

- **Which language clients llingr actually has** - Go and a Rust FFI binding are shipped; the JVM
  build is claimed but unverifiable; the relay and Python/C/C++/C# are announced, several past their
  dates. Recorded in [`next-llingr-questions-and-answers.md`](next-llingr-questions-and-answers.md).
- **Whether llingr belongs in the user-facing demo** - no. Internal benchmarking only.
- **The AdminClient question** - split into
  [`next-expose-admin-client-api.md`](next-expose-admin-client-api.md); no prior art exists, and the
  recommendation is to do nothing yet.
- **Prior art for the user-selectable concurrency key** - none; the subset rule appears to be new.
  Recorded in [`next-multi-topic-multi-function.md`](next-multi-topic-multi-function.md).
- **The multi-topic/multi-function issue** - astubbs#254 (`confluentinc#372`), and its upstream
  "implemented in #390" claim is false.

- **Offset gaps / compaction (item 10)** - PC tracks only records it actually polled, so a gap is
  indistinguishable from a success and nothing enumerates a numeric range. Deliberate, documented in
  `PartitionState`'s javadoc, and tested. Turned up one previously-unknown edge case, now filed.

- **NATS JetStream** - researched, and "not any time soon" is well supported. Its consumer model
  already gives *unordered* parallelism without a partition ceiling, so the largest part of PC's
  value does not transfer; key-ordered parallelism does hit the same `min(partitions, members)`
  ceiling, but Synadia's `pcgroups` already fills that gap **including in Java**, and the NATS Java
  community is roughly a tenth the size of its Go one. Also worth knowing: Synadia tried to move NATS
  to BSL and out of CNCF in April 2025, settled in May, and Jepsen found real safety defects in
  JetStream 2.12.1 in December 2025.

## Strategic context, recorded so it is not lost

- **He has the same tools we do.** The owner's read: Streams and Connect integrations are not
  protected by difficulty - anyone can attempt them now. What may protect them is *interest*: it is
  not obvious he cares about Kafka Streams, and there is a courtesy dynamic in play given he asked
  for feedback and has not had it. **This is a reason to move, not a reason to relax** - the two
  0.6.0.0 previews are the strongest near-term differentiators in the whole comparison.
- **Engine speed is the smallest term.** Owner's framing, and the measurements support it: one
  configuration setting moved the same build 1.9x while five years of engine change moved it 35%, and
  any realistic per-record delay dwarfs both.
