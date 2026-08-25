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

## RESOLVED 2026-08-25 - the replay was run, and this note's prescription is implemented

The `-Dchaos.diagnoseStallRecovery=true` replay the paragraph above says "nobody has run yet" has now
been run twice, on the two seeds the ledger itself nominated. Both fired the bound and then drained
completely on a contended box: `6825864417772979246` (2 findings) and `4044221734199516240` (46
findings) each reached `inFlight=0` with full key coverage. Numbers and the reasoning in
[`bug-857-family.md`](bug-857-family.md)'s 2026-08-25 entry.

**"Gate on progress; report timing" is implemented as written.** `CLASS2_STALL/LAG_STAGNATION` now
lands in `ProgressProbe`'s non-gating `observations`; `INSTANCE_STALL` - the "in-flight work never
sits at zero while a backlog exists" liveness check this note asked for - gates, alongside the
correctness ledger. `Class2ObservationIT` guards the routing and is untagged, so it runs in every
default integration build.

**The before/after control arm, same seed and same box:** `4044221734199516240` on
`ChaosRevokeUnderWorkDrainIT` failed with 46 gating violations, and passes with 41 non-gating
observations - while the ledger still balanced, no failure cause went unclassified, and
`INSTANCE_STALL` stayed silent. The suite did not lose its verdict; it lost a false one.

**Why this note is not deleted yet.** The thing it warns against is a demotion being quietly reverted
by someone who finds a silent detector and "repairs" it. `Class2ObservationIT` is the mechanical
guard; this note is the reason, and the reason has to outlive the memory of the argument. Delete it
once the demotion has survived a release and the reasoning has a home in `docs/solutions/`.

## Related

- `docs/inflight/test-857-revoke-under-work-sightings.md` - the four arms and their numbers
- `docs/inflight/test-truth-probes-for-internal-state.md` - the same shape of question for internal state
- `docs/testing.md` - the chaos suite and its probes
- `docs/inflight/bug-857-family.md` - the sighting ledger, and the record of the replays that
  settled this. Its **twelfth sighting** was the live test of this critique; its 2026-08-25 entry is
  the result
<!-- file-refs: N/A - the sightings ledger arrives with astubbs/parallel-consumer#29, which this branch was split out of; it resolves once that merges -->
