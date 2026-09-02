# Emptying the quarantine lane leaves its last report live, saying ACTION REQUIRED

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

The lane report tells a reader that a deterministic quarantined test which started passing means its
fix landed, so its `@Quarantined` annotation and its registry entry must both be deleted. A PR that
does exactly that - removing the LAST entry - leaves that comment on the PR forever, still demanding
an action that has already been taken.

**Why nothing corrects it.** `.github/workflows/quarantine-lane.yml`'s `Check the lane is not empty`
step sets `found=false` when the registry is empty, and every step after it is gated on
`steps.any.outputs.found == 'true'` - including the one that posts. `bin/quarantine-lane-report.sh`
would stop before writing the report file anyway (`Quarantine lane empty - nothing to report`). So
the run that SHOULD retract the report is the one run guaranteed not to speak.

**The intent is already in the code, which is how this was found.**
`.github/scripts/quarantine-report-comment.js`'s `renderDelta` has a branch for a test that has
`left the lane`, and for the last entry that branch is unreachable. A comment describing behaviour
the wiring cannot reach is the tell.

**Not a stale-comment nuisance - a wrong instruction.** The other stale comments recorded in this
directory are inert (a PIT tombstone, an old Codecov reading). This one actively instructs a reader
to go and delete something that is no longer there, on the PR that removed it.

## What a fix has to do

Run the report on the emptying run and let it say the lane is now empty, rather than gating the post
step on the lane being non-empty. That means:

- a report path for "the lane emptied", distinct from "the lane has entries and here they are";
- the post step ungated (or gated on something that is still true when the registry is empty);
- a test, because this is the one transition no fixture currently covers - the suites all assume at
  least one entry.

<!-- post-merge: checked-begin -->
Deliberately left out of the change that surfaced it (astubbs/parallel-consumer#409, which made a
status change post a FRESH comment rather than edit one in place). That is what made this
transition consequential, but the empty-lane gate predates it, and fixing it properly is a
behaviour change needing its own fixtures rather than a line added during a review pass.
<!-- post-merge: checked-end -->

## How it was found

<!-- post-merge: checked-begin -->
An independent correctness review cross-checked `renderDelta`'s branches against the workflow's
step gating.
<!-- post-merge: checked-end --> Neither side is wrong on its own; the gap is only
visible when you read them together, which is why no test on either side catches it.
