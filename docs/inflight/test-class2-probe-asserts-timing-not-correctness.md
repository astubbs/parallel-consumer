# `CLASS2_STALL` gates on a TIMING bound in a CORRECTNESS suite

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

## The critique

The chaos suite exists to establish correctness: no record lost, duplicates bounded, ordering held,
work completes. `ProgressProbe`'s Class 2 detector instead asserts **"no partition's committed offset
may be still for 150 seconds"** - a performance claim, used as a proxy for the liveness property
actually wanted, which is *"it is not wedged forever"*.

**If the system recovers, it is correct.** Slow recovery is an optimisation question, and an
optimisation question does not belong in a gate. Raised by Antony on 2026-08-19, after four
measurement arms had been spent arguing about whether the legitimate window was 100s, 150s or 281s -
arithmetic that only matters because the assertion is aimed at the wrong property.

## What the proxy costs, measured

Seed `4734674029169027864`, four arms, all recorded in
`test-857-revoke-under-work-sightings.md`. Every arm **completed**: no loss, bounded duplicates,
`done=true`. By the correctness criterion all four pass. By the 150s bound, three of four fail.

The false-positive mechanism is not subtle once the counters are on both ends of the user function:
a committed offset is SILENT while a long record runs, so a busy fleet and a wedged one look
identical to a watermark-stagnation detector. The diagnostic mode's `inFlight` counter shows the
eager arm holding 20-29 records in flight continuously, with `consumed` advancing on every poll,
throughout the window the probe was calling it stalled.

## What to replace it with

**Gate on progress; report timing.**

- **Gate**: the run completes, nothing is lost, duplicates stay bounded, ordering holds - and a real
  liveness check, e.g. in-flight work never sits at zero while a backlog exists. That detects a
  genuine wedge and *cannot* fire on slowness, which is the whole property the bound fails to have.
- **Report, do not gate**: recovery time, stagnation peaks, throughput. A regression there is worth
  seeing and is not a correctness failure. Put it in the run output where it is visible, not in an
  assertion that fails the build.

Read that way, the four arms are one sentence: all four are correct; eager takes 281s and
cooperative 104s, so eager is slower, which is an optimisation matter.

## Why this is not just "raise the bound"

Raising it keeps a timing assertion in a correctness gate and buys time until a slower box or a
denser workload crosses the new line. The July recalibration already did this once - 90s/45s retuned
to 60s/20s so `60+20+20=100s` sat under 150s - and the arithmetic assumed ONE restart of an in-flight
heavy record. Measured reality with several restarts is 281s. A third retune would be the same move
a third time.

## Related

- `docs/inflight/test-857-revoke-under-work-sightings.md` - the four arms and their numbers
- `docs/inflight/test-truth-probes-for-internal-state.md` - the same shape of question for internal state
- `docs/testing.md` - the chaos suite and its probes
- `docs/inflight/bug-857-family.md` - the sighting ledger. Its **fourteenth sighting** (the merge of
  astubbs#325 into astubbs#57 renumbered it from twelfth; grep the date, since ordinals in that
  ledger are not stable anchors) is the live test
  of this critique: the new drain control arm fired `CLASS2_STALL` twice on 2026-08-20, and whether
  that is a stall or the slowness this note describes is decided by one
  `-Dchaos.diagnoseStallRecovery=true` replay, which nobody has run yet
<!-- file-refs: N/A - the sightings ledger arrives with astubbs/parallel-consumer#29, which this branch was split out of; it resolves once that merges -->
