# MultiInstanceHighVolumeTest got ~20% slower between 2026-08-25 and 2026-09-01 - CONFIRMED locally

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->

**Confirmed 2026-09-01 by local A/B - see the last section. What follows is how it was first seen.**

**Originally filed as a candidate:** It is recorded because the signal survived the two checks that kill
most such claims, and because nobody would otherwise go looking - there was no way to see it until
`bin/perf-backfill.sh` existed.

## The observation

Sampling CI logs across the retention window and computing the subject's class time against a fixed
set of three neighbour classes in the same run:

| Window | ratios observed | neighbour seconds |
|---|---|---|
| 2026-08-19 to 08-26 | 0.227, 0.281, 0.285, 0.296, 0.309, 0.439 | 122 - 140 |
| 2026-08-31 to 09-01 | 0.370, 0.377, 0.393, 0.401, 0.405, 0.432, 0.478 | 124 - 139 |

Median moves from about 0.29 to about 0.40. The subject's own class time roughly doubles, from the
high twenties and thirties into the fifties and sixties.

## Why it is not obviously an artefact

- **The neighbours are flat.** 122-140 seconds in both windows. A slower runner fleet moves everything;
  this moves one class.
- **The test did not change.** `git log` over `MultiInstanceHighVolumeTest.java` on master shows one
  commit in the window, and it added a `ThroughputReport` call - no change to `GATING_VOLUME`,
  `GATING_CEILING`, partition count or max poll.
- **The branches are unrelated.** The late window spans proxy work, load-factor work, chaos
  observability and docs branches. One branch regressing would show on one branch.

## Why it is not yet a finding

- **Thirteen samples.** Six early, seven late.
- **The early window contains a 0.439**, which sits inside the late range, so the separation is not
  clean.
- **Class time is a coarse instrument** - it includes broker startup and teardown, and it saturates
  when a test strikes its deadline rather than continuing to grow. Rates would be better and do not
  exist before 2026-09-01, which is when `ThroughputReport` gained its call sites.
- **Nothing separates product from harness from runner fleet.** A change to test infrastructure, or to
  what the hosted runners are, produces the same picture.

## How to settle it

The window is small enough to bisect directly: **19 main-code commits landed on master between
2026-08-26 and 2026-09-01**, and the perf lane takes minutes. Several touch paths where a correctness
fix plausibly costs throughput - a compare-and-set on the shard's available-count spend, the work claim
becoming one atomic transition, a volatile on the dirty flag, the broker-poller load gate derived by
conservation, and the back-pressure pause derived from Kafka rather than mirrored. **Do not read that
list as a suspect list**: each was measured for correctness, none was measured for throughput, and
naming them is a starting order for a bisect rather than an accusation.

The mechanism is the same one used to find this: cut a branch at a candidate commit, open a draft PR so
the `pull_request`-only performance lane fires, and read the ratio. Roughly five runs settles a
19-commit window.

**Do this before trusting `docs/perf-baseline.tsv`.** Its rate row comes from a 2026-09-01 run, which
is inside the late window. If this is a real regression, the baseline has already absorbed it and the
check is calibrated against a degraded tree - it would still catch a further regression, but it would
never report the one already there.


## CONFIRMED, 2026-09-01: two local A/B pairs on an idle box

The CI evidence was thirteen sampled runs across unrelated branches - suggestive, and open to the
objection that it was comparing different trees on different machines. So it was run directly:
`67a06282a` (last master commit before the window) against `66a9a35e0` (master), full performance lane,
same machine, back to back, nothing else running.

| | subject | neighbour total | ratio |
|---|---|---|---|
| pair 1, before | 26.11 | 80.95 | 0.325 |
| pair 1, after | 31.14 | 80.89 | 0.392 |
| pair 2, before | 26.90 | 81.79 | 0.329 |
| pair 2, after | 32.14 | 80.77 | 0.398 |

**+19.3% and +19.5%.** The second pair ran the arms in the opposite order, so warm-up and ordering are
excluded. Neighbour totals span 80.77 to 81.79 - 1.3% - across all four runs.

### Per-method, which is the better instrument and was available all along

`<testcase>` elements carry per-METHOD times, excluding container startup and `@BeforeAll`. Same pair,
method level:

| method | before | after | delta |
|---|---|---|---|
| `LargeVolumeInMemoryTests#timingOfDifferentOrderingTypes[1]` | 13.294 | 13.049 | -1.8% |
| `[2]` | 11.902 | 11.580 | -2.7% |
| `[3]` | 11.389 | 11.574 | +1.6% |
| `LoadTest#asyncConsumeAndProcessAtVolume` | 29.577 | 29.292 | -1.0% |
| `VeryLargeMessageVolumeTest#shouldNotThrowBitSetTooLongException` | 11.255 | 11.453 | +1.8% |
| **`MultiInstanceHighVolumeTest#multiInstance`** | **23.347** | **28.603** | **+22.5%** |

Eight control methods across three classes, all within 2.7% and most slightly FASTER after. The subject
alone moves.

### The confounds that were checked and cleared

- **Same method set.** `tests="1"` on the subject class in both arms; one `@Test` in both commits.
- **The test's own change does not explain it.** The subject class gained 29 lines in the window - the
  `ThroughputReport` call - which runs ONCE per test at the end: an `Instant.now()`, a `size()` and a
  log line. No other performance class changed.
- **Not infrastructure.** `VeryLargeMessageVolumeTest` and `LoadTest` both drive real brokers and did
  not move, so a broker image or Testcontainers change would have shown there.
- **Not the machine.** Same box, minutes apart, idle, both orderings.

### What is still unknown

**Which commit.** 19 main-code commits landed in the window. The A/B harness is now trivial - two
worktrees and `bin/performance-test.sh` - so bisecting is roughly five runs at about four minutes each.
Several candidates touch what this test exercises (a compare-and-set on the shard available-count, the
work claim as one atomic transition, a volatile on the dirty flag, the broker-poller load gate derived
by conservation, back-pressure derived from Kafka), but that is a bisect order and NOT a suspect list -
each was measured for correctness and none for throughput.

**Whether it is a real cost or a fair price.** Several of those commits fix genuine concurrency defects.
A correctness fix that costs 20% may be the right trade; it should be a decision somebody made, not one
that happened.
