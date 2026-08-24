# confluentinc#894 - the one case the reproduction does not cover: a partition that keeps producing

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

astubbs#57 carries `confluentinc#893` as `fix(core) astubbs#121`, and now carries a behavioural
reproduction of the defect it fixes (`PartitionStateCommitEncodeShift894Test`,
`PartitionStateCommitShiftCompounding894Test` - both green on the fix, both red against the unfixed
code on `test/894-reproduce-offset-encode-shift`). What follows is the part that is still open.

**Settled, so nobody re-opens it.** The dirty read between the two `getOffsetToCommit()` calls is
reproduced at three altitudes, up to and including the reporter's own step 8 - a next commit past the
log end offset, which is what fires `auto.offset.reset`. The shift magnitude is measured as a
property of the state at commit time (+2 with one incomplete outstanding, +1 with two), which
accounts for the upstream reviewer's "committing 11 not 10" without needing a second defect. And his
"after multiple rebalances" is refuted **as framed**: repeating the race on a *static* partition
compounds the fabrication (decoded highest-seen 4, 5, 6 against a real maximum of 3) while the
committed offset only reaches the log end offset and stops there, parking the consumer below a
phantom rather than driving it out of range.

## What is untested

**A partition that keeps producing while the race repeats.** New records make the previously
fabricated offsets real, which lets the incomplete set empty again - and that is the exact condition
the single-hop path needs to fall through to a fabricated `offsetHighestSucceeded + 1`. So on a live
partition the out-of-range commit may be reachable on *every* cycle, with the accumulated fabrication
setting how far past the end it lands, rather than only on the one-hop path the tests pin.

This is the reporter's own case. Their walkthrough has offset `601266893` arriving, and
`confluentinc#894`'s stated precondition is "no new offset in this partition" - low traffic, not
*zero* traffic. The static-partition model in the tests is the limiting case, not the reported one.

**Why it was not settled with the rest.** Answering it needs a chosen model of partition growth rate
against rebalance rate, and several are defensible. That is a modelling decision rather than a
measurement, so picking one silently would produce a number that looks like evidence and is not.

## What this does and does not mean for the fix

Nothing here argues against shipping. The fix removes the second read entirely, so every path
described above - reachable or not - is closed by construction, and the residual behaviour is
conservative: an offset completing mid-cycle is committed as still incomplete and replays.

It matters for what is *claimed*. The tests demonstrate the mechanism and the single-hop symptom;
they do not establish the failure rate on a live partition, which is what a user would want to know.

Closing this note needs either that growth model and a test built on it, or a decision that the
mechanism being closed by construction makes the reachability question moot.
