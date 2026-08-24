# astubbs#121 - the carried offset-accuracy fix closes a real window, but nobody has reproduced the bug

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

astubbs#57 carries `confluentinc#893` (author `sangreal`, approved by `rkolesnev`, still **unmerged**
upstream) as `fix(core) astubbs#121`. The carry is faithful - the fork's diff is substantively
identical to upstream's, down to the comment text. What is *not* established is that it fixes the
whole of `confluentinc#894`.

**Do not read the upstream PR body to judge this fix.** It argues a root cause the diff abandoned:
`sangreal` first proposed changing `offsetHighestSucceeded` seeding, `rkolesnev` rejected that
reading, and on 2025-10-29 the author replaced it with the dirty-read fix that actually shipped. The
body was never updated, so it describes a change to `offsetHighestSucceeded` that the diff never
makes.

The mechanism that *did* ship is sound and its evidence is a PR comment dated 2025-11-05: the
incomplete-offsets payload is encoded against one `getOffsetToCommit()` reading, a completion lands
before the second reading, the higher offset is committed, and every decoded incomplete shifts by the
difference - compounding across rebalances until a poll goes out of range. The fix samples once and
threads the value through a tuple, so the committed offset and the payload's decode base are the same
number by construction. The residual race is conservative: replay, never skip.

## Why this stays open after astubbs#57 merges

- **No behavioural reproduction exists anywhere.** `rkolesnev` asked for one on 2025-10-26; none was
  produced. Upstream ships no test at all. The test carried here asserts `getOffsetToCommit()` is
  called exactly once and fails against the old two-read code - it pins the fix's *shape*, and being
  single-threaded it cannot fail for the reason the bug occurs.
- **The approving reviewer approved while still suspecting more.** "i still think there might be
  another edge case here but i havent fleshed it out yet", and separately that "the Parallel Consumer
  has a bug somewhere in marking state dirty and advancing offset to commit by 1 - so after multiple
  rebalances it ends up committing not offset 10 - but offset 11". Neither was ever chased.
- **Field evidence is one datapoint.** The reporter ran it privately for "more than a week" against a
  fault recurring "once every several days" - suggestive, not decisive.
- **Unread:** `Parallel Consumer Offset reset Issue flow.pdf`, attached to `confluentinc#893` on
  2025-10-31, which may hold the reproduction nobody wrote down. Reading it is the cheapest next step.

## What this asks of whoever merges astubbs#57

Word the changelog as closing the dirty-read window rather than as fixing offset reset, and do not
let the merge auto-close astubbs#121 - the issue outlives the fix that is shipping against it.

Closing this note needs one of: a test that reproduces the shift across a rebalance, or a decision
that the upstream reviewer's suspected edge case is not worth chasing.
