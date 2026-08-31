# A compounding pass is owed for the confluentinc#857 diagnosis work

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->
<!-- inflight-state: deferred - one lesson left; the rest are discharged, see below -->

**Mostly discharged as of 2026-09-01.** It was deferred while the diagnosis was still moving, on the
grounds that compounding a moving target produces write-ups that are wrong by the time they land.
The diagnosis settled - the cause was found, fixed and measured - and the pass then ran.

**What landed, and where**, so nobody compounds the same lesson twice:

- The empty-search-result lesson is
  [`../solutions/workflow-issues/a-title-grep-is-not-a-search-2026-08-31.md`](../solutions/workflow-issues/a-title-grep-is-not-a-search-2026-08-31.md).
- The detector-bound lesson, the filtered-diagnostic lesson and the narrative of two hypotheses dying
  to one line of output are all in
  [`../solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md`](../solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md),
  which also added `Filtered diagnostic` to `CONCEPTS.md`.
- Gate-on-progress-report-timing was already owned by
  [`../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md);
  what this work added is the instrument rather than the principle, so it was cited, not restated.

**One lesson is still uncompounded**: a capability absent by accident reads as absent by decision.
It is being written now; when it lands, this note has nothing left and should be deleted rather than
kept as an empty ledger.

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
  and an upstream-verified member of the family lives in that mode.

## When

After the ingestion question the diagnosis has now narrowed to is settled. Doing it earlier means
writing up a mechanism that is still changing.
