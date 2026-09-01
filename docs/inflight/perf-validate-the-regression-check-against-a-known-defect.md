# The throughput regression check has never caught anything, and there is a known defect to point it at

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

**Update 2026-09-01: the primary evidence arrived from history instead, and the check now FAILS on
this defect rather than warning. What is left below is a confirmation, not the experiment that
settles it - see the last section.**

**A detector that has never fired is not a working detector.** `bin/check-throughput-regression.sh`
was reasoned into existence from a regression that was diagnosed by hand; nothing has yet confirmed it
would have caught that regression, or that it stays quiet on a clean tree. Both halves need showing,
and there is an unusually good subject available: a defect whose presence is a one-line edit.

## The subject

The control loop passed an O(shards) accessor as a plain `log.trace` argument, so it ran on every pass
at every log level. Removing and restoring that one line changes throughput and nothing else - no test
changes, no config changes, no rebase. That makes it a **positive control** in the strict sense: the
only variable is the defect.

Its fix, the write-up of the mechanism, and the sweep that found no second instance are on
`handoff/enable-large-number-of-instances`; the defect itself is still live on
astubbs/parallel-consumer#29, which is where the performance lane that fails is.

## The sequence, which is the whole reason this is a note rather than a line

Four steps, each blocked on the one before, spanning three branches and two owners:

1. **Land the monitoring** - artifact retention, the master-side lane, and the check itself - on
   master, so it exists somewhere both other branches can reach.
2. **astubbs/parallel-consumer#29 merges master**, picking the check up. Note it must NOT also pick up
   the control-loop fix at this point, or there is nothing left to detect.
3. **Red/black on astubbs/parallel-consumer#29**: run its performance lane with the offending line
   present, then with it removed, and compare what the check says.
4. **Read the result against the prediction below**, and either tighten the threshold or record why
   the design does not work.

## What the run should say, and why the prediction matters more than the result

Worked from the numbers already in hand - observed 43,552 against a 71,387 baseline, neighbour classes
~4.5% slow, giving a machine index near 0.957 and an expected near 68,300 - the ratio lands around
**0.64**. Against the thresholds as they ship, that is a **WARNING and not a failure**.

So the expected outcome of the red arm is a warning, and the black arm should sit near 1.0. Three
distinguishable results, and the middle one is the interesting one:

- **Red warns, black near 1.0** - the design works and the only open question is the threshold. Tighten
  the fail bound using the spread the master lane will by then have produced, and say what the spread
  was in that commit.
- **Red and black are indistinguishable** - the normalisation is eating the signal, or the machine
  index is dominated by something other than machine speed. The check is not fit and should be said so
  rather than left running.
- **Black is also low** - the baseline is stale rather than the tree slow. Re-baseline from a master
  run, and say which run.

**Write the prediction down before the run, which this note is doing.** A detector evaluated after
seeing its output is evaluated against whatever it happened to say.

## What this cannot establish

That the check catches regressions in general. It catches one that hits the throughput test harder
than its neighbours, which is the shape this defect has; a regression that slows everything equally is
invisible to within-run normalisation by construction, and no run of this experiment will say
otherwise. That limitation is in the script's header and belongs in whatever conclusion this reaches.


## Update, same day: history answered it before the experiment could

The threshold no longer needs this experiment to be chosen, because the spread was already recorded -
in CI logs nobody had mined. `bin/perf-backfill.sh` recovered every performance run inside GitHub's
log-retention window and computed the normalised ratio for each. Twelve carried a rate and they
separate with a gap and no observation inside it: every regressed run scored between 0.407 and 0.605,
every healthy one 0.778 or above.

So `FAIL_BELOW` is 0.70, derived rather than guessed, and the defect this note is about scores 0.578 -
**it fails the lane** rather than warning, which is what was wanted and what the earlier draft of this
note could not justify. `bin/test-check-throughput-regression.sh` pins those real observations as
cases, so the thresholds cannot drift away from the evidence they came from without a red test.

**astubbs/parallel-consumer#29 ran the control arm on the fix**, after merging the branch carrying it
(`5ed885612`). Calling that an independent rediscovery, as an earlier draft here did, was wrong and
inflates the evidence: it is one team measuring one change, not two arriving at the same mechanism
separately. What it IS: 43,552 rec/s failing at `b42ab61d7`, 76,950 passing at `92c5d5b70`, a single
main-code term changed, neighbour classes within 1-3%, and the lane-composition confound moved the
wrong way and it still improved. That is a controlled measurement of the fix's effect, which is worth
more than a rediscovery would have been anyway.

### What is still worth running, and what it is now for

The red/black arm is now a **confirmation of the detector**, not the source of the threshold - a
different and lesser job, but not a pointless one: nothing has yet observed this check fire in CI on a
tree that is actually regressed. Everything above is the check applied to recovered numbers, which is
one step short of watching it go red on a real run.

Two caveats belong with the numbers, and both survive into whatever this becomes:

- **Eight of the twelve observations are the same branch and the same defect**, so the regressed group
  is one phenomenon sampled eight times rather than eight independent regressions. The gap is real; its
  width is less established than twelve points suggests.
- **A regression that slows everything equally is invisible to THIS CHECK** - within-run normalisation
  cannot separate it from a slow runner. It is not invisible to the available data: 90 days of runs are
  queryable and nothing reads them across runs yet. See
  `perf-a-queryable-history-instead-of-a-single-committed-baseline.md`.


## RUN, 2026-09-01: the check fires - and the run exposed a weaker claim than expected

<!-- post-merge: checked-begin - both PRs are named by number and in the past tense, so this reads
     identically once either has merged or been closed -->
Red/black executed in CI as astubbs/parallel-consumer#402 (defect injected, verbatim) against
astubbs/parallel-consumer#401 (same tree, no defect). Both arms ran the real lane on hosted runners.
<!-- post-merge: checked-end -->

| Arm | rate | records | machine index | ratio | verdict |
|---|---|---|---|---|---|
| red | 24,207 | 1,464,340 / 3,000,000 | 0.9241 | **0.336** | FAILED, exit 1 |
| black | 62,970 | 3,000,000 / 3,000,000 | 0.9377 | **0.861** | OK |

**The detector works end to end.** That was the open item and it is closed: the check ran in CI, on a
genuinely regressed tree, and went red for the stated reason with the numbers shown.

**The prediction was directionally right and numerically wrong**, which is why it was written down
first. It said 0.407-0.605, the band every historical regression occupied. The actual was 0.336 -
below the band, because the injected defect ran on a tree where nothing else masked it, whereas the
historical instances were mixed in with other lane changes. Recorded rather than quietly rounded to
"as predicted".

### The uncomfortable finding: nothing here proves the check ADDS detection power

The red arm struck the 60-second `GATING_CEILING` as well (1,464,340 of 3,000,000), so the pre-existing
wall-clock assertion failed that run too. Checking the history, **every regression observed so far also
breached the ceiling** - the 43,552, 44,992 and 43,629 runs all failed their lane on the deadline.

So on the evidence to date the check has never caught something the ceiling would have missed. What it
demonstrably adds is different and should be claimed as that and nothing more:

- **A number instead of a binary.** The ceiling says "slower than the bound on this runner today"; the
  ratio says how much, normalised, which is what makes a verdict readable.
- **The other direction.** A healthy tree on a slow runner breaches the ceiling and reads as a
  regression. The ratio is what separates those, and it is the failure that wasted weeks here.
- **The band under the ceiling.** A regression that is real but still finishes inside 60s is invisible
  to the deadline and visible to the ratio. Nothing has yet observed one, so this is the design intent
  rather than a demonstrated catch.

**What would demonstrate the added power** is an injected regression sized to slow the test without
breaching the ceiling - roughly a 20-30% cut. That is a better next experiment than repeating this one.

### More evidence that 0.70 cannot tighten

<!-- post-merge: checked-begin - names the PR a measurement came from, in the past tense; stays true
     after any merge -->
The black arm scored 0.861 on a tree differing from the baseline run only by documentation and
scripts. Together with the docs-only 0.778 already recorded, and an earlier run of the same branch at
1.000 (astubbs/parallel-consumer#401), that is a spread of 0.778 to 1.000 on effectively identical
code, from three separate runs.
<!-- post-merge: checked-end --> The floor is noise, not sensitivity, and a bound above about 0.72 would fail
documentation changes.
