# Upstream Discussions - a content type our tooling never looked at

<!-- inflight-priority: low -->

Discussions were a blind spot until 2026-08-07: not in
[`upstream-map.yaml`](../../src/docs/development/upstream-map.yaml), not in the mirror set, not in
any sweep mode. `scripts/upstream-sweep.sh --audit` now lists the zero-reply ones, so run that rather
than trusting any list written here.

**They were not swept.** 74 discussions, no bulk closure - the largest single day is 2024-04-02 with
6, and all six are `answered=true`, i.e. housekeeping of resolved threads. So this is a *neglect*
problem, not an administrative-closure problem, and it needs different handling from the
`sweep-2023-*` cohort: these want answering or converting, not mirroring wholesale.

Shape of it: 41 of 74 are Q&A, 29 have no accepted answer, 15 have no reply at all - of which 9 are
release announcements that legitimately need none.

## Two are worth acting on now, because they touch live work

**confluentinc discussion 542** - *Transactional Parallel Consumer stuck while rebalancing*
(mikhelef, 2023-01-27, **zero replies**). Multiple instances, full input topic; the first instance
gets stuck during rebalance because it cannot acquire the **produce lock** - the reporter's own guess
is that the revoke-time flush holds it - and after `max.poll.interval.ms` the member is evicted from
the group. On 0.5.2.4, `PERIODIC_TRANSACTIONAL_PRODUCER`, 4 instances / 4 partitions, 16 threads
each. They raised the produce-lock timeout from 2 to 4 minutes with no change.

This is a user-observed instance of a lock lifecycle we already have open questions about - see
[`bug-producing-lock-double-release.md`](bug-producing-lock-double-release.md), and the
`bug-857-stall-after-rebalance` entry in the manifest. `produceLockAcquisitionTimeout` still defaults
to 1 minute. Worth reading before more produce-lock work: it is the only field report of this
failure mode we have, and raising the timeout demonstrably did not fix it, which is evidence about
the mechanism rather than the duration.

**confluentinc discussion 883** - *Parallel Consumer is extremely slow vs Normal Consumer* (gmreads,
2025-08-02, **zero replies**). 30-40k/sec raw vs 1000/sec PC, 4.5M lz4 messages, 1 topic / 1
partition, with a minimal reproducer and the lag-measurement script inline. Same complaint as fork
mirror astubbs#187 (confluentinc#884, "30 times slower"), from a different reporter with a
*runnable* case - which astubbs#187 does not have. Attach it there.

## The rest

Four more zero-reply questions (confluentinc discussions 577, 601, 815, 910) and seven Q&A threads
that have replies but no accepted answer (403, 512, 643, 671, 677, 778, 849). Unread. Several look
like documentation gaps rather than defects - "What does maxConcurrency actually do?" being the
clearest - so the likely outcome is doc improvements plus a few answers, not new issues.

The policy for what to do with a discussion worth acting on (we do NOT mirror them - raise a
normal fork issue on its merits) is permanent, so it lives in
[`docs/upstream.md`](../upstream.md), "Discussions are not mirrored" - not here.
