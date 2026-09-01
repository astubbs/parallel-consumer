# The performance history should be a self-carrying artifact, not one hand-written baseline row

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->

**Largely IMPLEMENTED 2026-09-01 - see "What landed" at the foot. The committed baseline is gone; what
remains open is the accumulating artifact.**

`bin/check-throughput-regression.mjs` used to compare against a committed `perf-baseline.tsv`: **one run's numbers,
updated by hand.** That is enough to gate on, and it has three consequences that a queryable history
removes rather than mitigates.

- **The baseline never rises.** If the product gets faster, the file still says what it said, so the
  ratio drifts upward and the check quietly loses sensitivity - a later regression is measured against
  a floor the code left behind years ago. Nothing goes red to say so.
- **The threshold cannot be tightened by better comparison alone - MEASURED, and it was a surprise.**
  The obvious reading of the healthy band (0.778 to 1.000) is that most of the spread is baseline
  staleness, curable by comparing against master runs near a PR's own merge-base. It is not. The 0.778
  run is `docs/beads-evaluation`, which is **docs-only** - four markdown files, zero main-code change -
  so a tree byte-identical to master in every line of code lost 22% AFTER normalisation.

  That says the proportionality assumption underneath neighbour-normalising is only approximately true:
  in that run the neighbours were 6% slow while the subject was 27% slow, so the subject AMPLIFIES
  machine variance rather than tracking it. Plausibly because it is the multi-instance case - twelve
  consumers, KEY ordering, the most contention-sensitive thing in the lane - but that is a hypothesis,
  not a finding.

  So a nearest-neighbour comparison fixes staleness and does nothing for this. What would: comparing a
  run against the DISTRIBUTION of recent master runs rather than any single point, which cuts the noise
  contribution with the square root of the sample and is only affordable if the history is cheap to
  query. That is the argument for the design below, and it is a stronger one than baseline staleness.
- **A uniform slowdown is invisible.** The check normalises within a run, so a regression that slows
  everything equally reads as a slow runner. Cross-run absolute rates would catch it - and those exist.

**The data is not missing. Nothing reads it.** Every performance run inside GitHub's log-retention
window carries per-class failsafe times, and recent ones carry rates; `bin/perf-backfill.mjs` already
recovers them read-only via the run-logs archive.

## The idea: each run carries the whole history forward

Operator's design, and it removes the expiry problem without a data branch or a write grant:

1. A run downloads the aggregate artifact published by the previous run.
2. It collects only what it does not already have - the runs since that aggregate was built.
3. It appends, and **uploads the whole aggregate again** as its own artifact.

The series then lives in the most recent run's artifact rather than in any single expiring one, so
retention only bites if nothing runs for the whole window. The tool queries a prebuilt cache instead
of re-mining logs, which is the difference between a check that can afford to consult history on every
run and one that cannot.

**Only master runs may append.** A PR run - especially from a fork - must consume the aggregate and
never publish one, or the series becomes whatever the last PR to touch it said.

**The cache is a cache, and must be rebuildable.** If it is lost, corrupted, or its schema changes,
`bin/perf-backfill.mjs` reconstructs it from logs within the retention window. Anything that cannot be
rebuilt that way does not belong in it - which is the rule that keeps this from quietly becoming the
durable store the operator declined.

**Worth extracting as a GitHub Action** (operator's suggestion): "collect a metric, carry the series in
your own artifacts, compare against it" is not specific to this project, and the artifact-as-transport
trick is the whole novelty - it needs no branch, no token beyond the default, and no third-party
service. Extract it only once it has worked here.

## What it enables, in the order the value arrives

1. **Comparison against a distribution, not a point.** Compare a PR's run against the spread of recent
   master runs near its merge-base. This is the one that can actually tighten the threshold - a single
   nearest neighbour inherits the same 22%-on-identical-code noise measured above, and averaging is the
   only thing that removes it.
2. **A self-raising baseline.** A rolling statistic over recent master runs replaces the hand-updated
   row, so the product getting faster raises the bar automatically.
3. **Absolute-rate trend.** The one thing within-run normalisation structurally cannot see.

## What has to be got right

- **Say which runs a verdict used.** A comparison against "recent master" is unreadable unless the
  output names the runs, or a re-run that quietly picked different neighbours looks like a regression.
- **Cancelled and partial runs.** Several already-collected runs are `cancelled` yet carry a complete
  performance job, and others carry none. The rule is the one `bin/performance-test.sh` already uses -
  a run that measured nothing must be visibly absent, never silently averaged in as a zero.
- **Do not gate on the aggregate before it is trusted.** The current fixed-baseline check should stay
  the gate until a history-based one has been shown to agree with it.


## What landed, 2026-09-01

The committed baseline is deleted. The check now reads the last N `perf baseline (master)` runs from
their artifacts and takes the **median**, so there is no table to maintain and nothing to go stale.
Three of the four problems above are gone with it: no rot, no hand-updating, and a reference as fresh
as the last push to master.

**And the comparison is now derived by conservation rather than by correction**, which removed the
defect review found in the old form. It compared a rate against a machine index built from control
class times - mixing dimensions, and applying a correction that had been MEASURED not to hold (controls
moved 5% while the subject moved 30%). The conserved quantity is the share, `subjectSeconds /
controlSeconds`, both from the same run: every test does a fixed amount of work, so a runner twice as
slow doubles both terms and the ratio is unchanged. Nothing has to be corrected because nothing was
uncorrected. Per-method times, because a class time is `work + setup` and setup is the non-conserved
term.

**This is also what makes the accumulating artifact viable long-term**, which was not obvious when this
note was written. A stored SHARE is dimensionless and machine-independent, so an entry from one
runner generation stays comparable to one from the next with no fingerprint and no re-baselining - and
it compresses to almost nothing. A store of absolute rates would have needed all of that and still
rotted.

### Still open

The artifact that carries the whole history forward - each run downloading the previous aggregate,
appending, and re-uploading - is not built. Today the check downloads the last N runs individually,
which is bounded by log retention and by N. The design above stands; what changed is that it should
store conserved shares rather than raw numbers.

### Not open any more

Raising the baseline when the product gets faster. There is no baseline to raise: a median over recent
master runs moves on its own as master moves.
