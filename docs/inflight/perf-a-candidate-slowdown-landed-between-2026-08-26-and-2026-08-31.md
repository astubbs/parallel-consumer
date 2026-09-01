# MultiInstanceHighVolumeTest looks ~40% slower relative to its neighbours since late August

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->

**A candidate, not a finding.** It is recorded because the signal survived the two checks that kill
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
