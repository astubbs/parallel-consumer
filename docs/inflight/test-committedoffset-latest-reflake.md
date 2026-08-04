# `committedOffsetRemoved[1] latest` is failing again, and cannot be re-quarantined

`PartitionStateCommittedOffsetIT.committedOffsetRemoved[1] latest` failed on #111 (which changes no
main code). It was the quarantine lane's first occupant; **#80 un-quarantined it on merge**, so there
is no open owning fix PR and `bin/check-quarantine-owners.sh` will reject re-quarantining it. It needs
a fix, or a new owning PR.

**Branch:** `test/committedoffset-latest-compaction-reflake`, seeded with the evidence and a plan:
[`docs/plans/2026-08-05-001-investigate-committedoffset-latest-reflake.md`](../plans/2026-08-05-001-investigate-committedoffset-latest-reflake.md).

Working hypothesis, from the failing run's autopsy: not a race but **arithmetic**. The check searches up
to `TO_PRODUCE + 2`, assuming the compaction records sit at offsets 200/201 - but `awaitWithTopicNudge`
injects nudge records into the same topic, pushing them past the window. Two nudges are visible in the
log immediately before the failure. It is load-sensitive because nudges only fire when the await has not
progressed: 3/3 pass locally, fails on the contended 2-core hosted runner.

Note `debug/committedoffset-firstpoll-stall` is a **different** fault in the same test (the #80-era
first-poll stall, where the await hung; here the await completed and the later assertion failed). Not a
duplicate - but its kafka-client DEBUG logging config is worth cherry-picking rather than re-deriving.

Until it is fixed, **any PR can go red on this without having caused it** - check whether the diff could
reach an integration test before investigating your own change.
