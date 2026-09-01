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

<!-- post-merge: checked-begin - a finding about that PR's tree, recorded as history -->
- **There was no product throughput regression on astubbs/parallel-consumer#29.** Same tree, same
  machine, same session: 73,722 records/second alone and 72,498 in the full lane, both completing all
  3,000,000. That pair is like-for-like; the CI comparison is not.
<!-- post-merge: checked-end -->
- **Lane composition has a real effect, but it is not the whole story.** Adding
  `MultiInstanceRebalanceTest`'s capacity profiles to the lane - they share one reused JVM with the
  throughput test - moved the CI number to 39,684. Disabling the heaviest recovered ~5,000 and no
  more. Restoring the lane to the baseline's composition still failed, which is what shows runner
  variance is sufficient on its own.
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
