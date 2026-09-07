# The performance lane gates on a wall clock, and the machine varies 1.5x

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`MultiInstanceHighVolumeTest` asserts **3,000,000 records within a 60-second `GATING_CEILING`**. On
GitHub-hosted runners the same test, on the same code, in the same lane, has been observed at:

| elapsedMs | records/second | |
|---|---|---|
| 27,298 | 109,898 | PASS |
| 36,361 | 82,505 | PASS |
| 42,024 | 71,387 | PASS |

| 60,154 | 32,394 | FAIL - 1,948,661 of 3,000,000 by the ceiling; astubbs/parallel-consumer#442 at b23c13db, the same code having passed this lane at four earlier heads of that PR <!-- post-merge: checked --> |
**A 1.54x spread on identical code**, and the slowest passing run already consumed 70% of the
deadline. A draw 43% below the best fails on arithmetic alone. That is not a hypothetical: it is the
observed spread of the instrument, measured against its own ceiling.

## Two consequences, and the second one poisons the evidence

**A failing run reports a number that is not a rate.** Once the ceiling is struck the test stops and
reports what it reached, so `PC-THROUGHPUT` emits records-reached divided by sixty seconds. The
39,684 and 44,992 figures that read as a 45% throughput regression on
<!-- post-merge: checked - names the PR the figures were observed on, in the past tense -->
astubbs/parallel-consumer#29 were arithmetic on a deadline, not measurements of throughput.

**A single-run comparison between branches carries almost no information.** 109,898 and 71,387 are
the same code. Any argument of the form "branch X is slower than baseline Y" built on one run per
side is inside the noise - including arguments made during the investigation that produced this note.
A like-for-like pair on ONE machine in one session is worth more than two CI runs.

## What is actually established

<!-- post-merge: checked-begin - findings about that PR's tree, recorded as history -->
- **WITHDRAWN: "there was no product throughput regression on astubbs/parallel-consumer#29."** This
  bullet asserted exactly that, on a local like-for-like pair: same tree, machine and session, 73,722
  records/second alone and 72,498 in the full lane, both completing all 3,000,000. The pair is real
  and is kept; the CONCLUSION drawn from it was retracted on 2026-09-01 by the neighbour table below,
  which showed the two CI machines were comparable and so left the local pair unable to stand in for
  the comparison. Unproven in both directions - do not re-derive "no regression" from these numbers.
- **Lane composition has a real effect and is NOT the cause.** Adding `MultiInstanceRebalanceTest`'s
  capacity profiles to the lane - they share one reused JVM with the throughput test - moved the CI
  number to 39,684, and disabling the heaviest recovered about 5,000. But with all three `@Disabled`
  the class cost 0.020 s with every test skipped, and the throughput test still failed at 43,552.
  Ruled out by measurement rather than by argument.
- **Runner speed is ruled out as well, by the neighbours inside the same run.** A uniformly slower
  machine slows everything proportionally. These did not move together:

  | class | baseline run | failing run | delta |
  |---|---|---|---|
  | `VeryLargeMessageVolumeTest` | 51.25 s | 53.86 s | +5% |
  | `LargeVolumeInMemoryTests` | 37.99 s | 39.45 s | +4% |
  | **`MultiInstanceHighVolumeTest`** | **71,387 rec/s** | **43,552 rec/s** | **-39%** |

- **The mechanism was found AND measured on 2026-09-01: the shortfall was a defect, not variance.**
  The control loop evaluated a shard-wide sum as an unguarded `log.trace` argument on every pass.
  With that fixed and nothing else in main code changed, `MultiInstanceHighVolumeTest` went from
  FAILING at 43,552 rec/s to PASSING at 76,950 rec/s while the three neighbouring classes stayed
  within 1-3% - so not a faster machine - and while the capacity profiles were re-enabled ahead of
  it, which moves the confound against the result.
  `docs/solutions/performance-issues/slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md` carries the table and the
  caveats. **This does not retire this note**: a wall-clock deadline on a machine that varies 1.5x is
  still unsound, and it happened to be right this time.
<!-- post-merge: checked-end -->
- **It does not reproduce locally.** The full lane passes here at 72,498. A development machine has
  headroom a hosted runner does not, so no local experiment can verify a fix for this.

## The fix this repo has already written down twice

Not a bigger ceiling - that moves the line without removing its load-bearing role, and a slow run and
a stalled run still fail identically.

`MultiInstanceRebalanceTest`'s own javadoc states the principle for its correctness arm: the
assertion must be **progress** - the consumed count advancing while work remains - *"never 'all N
records within T', which fails a slow run and a stalled run identically"*. `MultiInstanceHighVolumeTest`
does exactly what that sentence forbids. The general rule is
`docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`.

So the shape is: **gate on progress, report time-to-N as a number.** `ThroughputReport` already emits
that number on every run, pass or fail.

## The part that is genuinely unsolved

Reporting a number does not gate, and **the threshold cannot be chosen yet** -
`perf-throughput-regression-gate.md` records that collection landed and gating deliberately did not,
because nobody has the spread to pick a bound from data. This note supplies the first three samples
of that spread and they are wide, which makes the naive bound worse than useless.

Three routes, none costed:

- **Compare against the merge-base in the same run**, so runner speed cancels. Doubles the lane's
  cost and needs the base checked out.
- **Take the best of N**, which measures capability rather than the day's luck, at N times the cost.
- **Assert a floor far below the spread** - catches a 5x collapse, misses a 30% regression. Cheap,
  and honest about what it does not catch.

## Why this is filed rather than fixed

<!-- post-merge: checked-begin - describes where this surfaced and what that branch did, both of
     which stay true after it lands -->
It surfaced during astubbs/parallel-consumer#29, which was a deadlock fix and had no business
redesigning a performance assertion. That branch held the capacity profiles out of its lane with
`@Disabled` so it stopped being gated on this, and
`handoff/enable-large-number-of-instances` carries the re-enablement work. The assertion redesign is
larger than either and belongs to whoever picks this up.
<!-- post-merge: checked-end -->

## Wrong paths already taken, so they are not retaken

Carried here when the throughput-shortfall handoff was deleted on 2026-09-01. It was a transport
artefact for another session and this note is the durable home for the problem, so it was folded in
rather than left as a second copy. Retrieve the original with
`git show 88e43991d:docs/handoffs/perf-lane-throughput-shortfall.md` if the full narrative is wanted.
<!-- file-refs: N/A - names a deleted document, retrievable only from history, which the git command above gives -->

- **Fork isolation** (`-DreuseForks=false` in `bin/performance-test.sh`) was added and reverted. It
  buys headroom under the wall rather than removing the wall's load-bearing role, and composition
  turned out not to be the cause anyway. The user property is `reuseForks`, **not**
  `failsafe.reuseForks` - the qualified guess is accepted and silently does nothing.
- **Reading CI logs with `gh run view --log`.** It silently returned 7,138 of 9,968 lines and
  produced a confident wrong conclusion about which tests had run. Use the run-archive route in
  `docs/solutions/workflow-issues/gh-run-view-log-truncation.md`, and note it 404s until the run
  completes, which is why the shortcut keeps being taken.
- **Believing a single-run CI comparison.** 109,898 and 71,387 rec/s are the same code.
- **Mining CI history for this test's failure rate.** It was `@Disabled` on master and never ran, so
  there is no history to mine - the search returns nothing and the nothing reads as a result.

## 2026-09-01: a second measure landed beside the deadline, and did NOT replace it

astubbs/parallel-consumer#401 added a throughput check that compares the subject against control
classes **inside the same run** - `subjectSeconds / controlSeconds` against a median reference share -
so it is dimensionless and does not care how fast the machine is. That is the right shape for the
problem this note describes.

**It does not close this note, for two reasons, and the second is the one that matters.**

- **It is ADVISORY, not blocking.** `maven.yml` runs it, and the verdict does not fail the lane. The
  only thing that still blocks a merge on the performance lane is `GATING_CEILING` - a 60-second wall
  clock on a machine whose spread on identical code is 1.54x. Every argument above stands untouched.
- **Its own calibration data argues against normalising on one box.** Eight full-lane runs on a single
  unchanged commit put the raw subject at 13.4% robust spread and the normalised share at 16.6% -
  dividing by a control compounds the control's variance instead of cancelling anything, because on
  one idle machine there is no machine-to-machine variance left to cancel. The lib says so in its own
  header and ships advisory for exactly that reason.

So the state is: **collection improved, gating unchanged.** A test that fails a slow run and a stalled
run identically is still the thing standing between this lane and a signal, and the fix this note
argues for - gate on PROGRESS, not on "all N records within T" - has not been done.

**What would actually retire this note**, now that the numbers exist: change
`MultiInstanceHighVolumeTest` to assert progress with a generous outer bound, and let the advisory
share carry the "is it slower than it was" question it is already measuring. That splits the two
questions the deadline currently conflates, which is the whole argument above.
