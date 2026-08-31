# A compounding pass is owed for the confluentinc#857 diagnosis work

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->
<!-- inflight-state: deferred - the diagnosis is still moving; run this once it settles -->

**Deliberately deferred, recorded so it is not lost.** The operator's call, 2026-09-01: the material
is real but the investigation is still live, and compounding a moving target produces write-ups that
are wrong by the time they land.

This note exists because that reminder would otherwise live only in a conversation. The lessons below
are currently spread across commit messages, which is the one place this repo says knowledge goes to
die - release notes are generated from them, but nobody greps them for method.

## What the pass has to work from

Several distinct lessons, each with a worked incident already in the log:

- **An empty search result is not a finding.** A heading-anchored grep in one vocabulary "proved" no
  suppression register existed; two did, both with ranked re-enable lists. Written up in
  `../solutions/workflow-issues/a-title-grep-is-not-a-search-2026-08-31.md`, which is the shape the
  rest should follow.
- **A detector whose bound exceeds its enclosing timeout cannot fire, and its silence reads as
  health.** The ambient probe said "probe clean" for weeks about a consumer that had stopped
  ingesting entirely. Fixed in `AmbientProbeExtension`; the general rule has no durable home yet.
- **A diagnostic emitted at a level the profiles filter has not been emitted.** Hit three separate
  times in one day - the revoke fork's INFO lines, the awaitility catch block's debug counts, and
  `docs/investigating.md`'s own recommended check. The third was corrected in place; the pattern
  deserves its own write-up.
- **Gate on progress, report timing.** The rate instrument turned a lane that had read as flaky for
  weeks into a four-to-tenfold regression visible on every run, including passing ones.
- **Two plausible hypotheses died to one line of instrument output** - cluster 2 overhead, then
  paused consumption. Both were what an experienced reader would have assumed.
- **A capability absent by accident reads as absent by decision.** The chaos suite had no
  transactional scenario because a producer was never wired, not because anyone decided against it -
  and the family's only upstream-verified bug lives in that mode.

## When

After the ingestion question the diagnosis has now narrowed to is settled. Doing it earlier means
writing up a mechanism that is still changing.
