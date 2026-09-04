# PartitionStateCommittedOffsetIT sleeps 20s for compaction and nothing confirms it happened

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

`PartitionStateCommittedOffsetIT.triggerCompactionProcessing()` produces records and then sleeps a
flat 20 seconds, with the author's own `// or wait?` beside it. It is called from two sites in a
seven-test class - four calls per run - and is over 60s of that class's ~160s.

**The sleep asserts nothing.** A run in which compaction never happened is indistinguishable from
one in which it did. Everything downstream of it is therefore resting on a timing guess, and no
signal exists to say the guess still holds on a slower runner, a different broker image, or a
changed `min.cleanable.dirty.ratio`.

## What was tried, and why it did not work

Replacing the sleep with a poll on the partition's **log start offset** advancing, using the same
`listOffsets(OffsetSpec.earliest())` call this class already makes elsewhere - the 20s becoming a
deadline rather than a duration. Measured on CI: the poll logged **"Compaction did NOT advance"
four times out of four**, so it never fired and every call paid the full 20s anyway.

**The detector was wrong, and this is NOT evidence that compaction fails to happen.** Log
compaction retains the latest value per key; it rewrites segments without necessarily removing
records at the head, and the log start offset only advances when the head is reclaimed. So the
observable was badly chosen, not the behaviour absent.

That change was dropped rather than merged - it saved nothing and would have added a misleading
warning. What survives is the question it exposed.

## What would actually settle it

Something that observes compaction directly rather than inferring it from an offset:

- read the topic back and assert superseded keys are gone, which is what the tests care about
- or poll a broker metric for the log cleaner having run over that partition
- or assert on the segment layout via `Admin.describeLogDirs`

Any of those turns the sleep into a wait-for-condition, which is both faster in the normal case and
the first thing that would ever say "compaction is not happening here any more".

## Do not reach for this as a speed fix

The wall-clock case is weak and was measured. Four forks already overlap this lane's waiting at
about 91% efficiency, so removing 60s of sleep buys roughly 17s of job time - and the lane's own
noise floor is 119s across three concurrent samples of identical code
([`docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md`](../plans/2026-09-03-001-investigate-integration-gate-wall-time.md)).
The reason to do this is the blind spot, not the seconds.
