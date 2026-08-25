# Windowed aggregation across the boundary: the floors, the baseline, and what would settle it

<!-- inflight-type: register -->

The pre-registration and results record for the windowing falsification spike,
[`../plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md`](../plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md)
(astubbs#242, on astubbs#334's branch). **Everything in this note's first five sections was written
and committed before any broker arm ran** - that is the note's whole point: a floor chosen after the
number comes out is not a floor, and the plan's Verification Contract checks this file's commit
precedes U6's. Results are appended below the line as the units produce them, each beside the
prediction it confirmed or refuted.

## 1. The floors

Both floors are pre-registered as **rules**, argued rather than measured, and the argument is
recorded so a reader can disagree with the choice rather than with the arithmetic.

- **F1, the parity floor: 1,000 records per second sustained**, at 1 KB values, eight partitions,
  eight stream threads, keys spread across the partitions. Below F1 the wrapper cannot keep up with
  a moderately busy topic, and "it exists" stops being a usable claim because nothing usable runs on
  it. This is a chosen line, not a derived one - plenty of production topics run under 1,000
  records per second, which is why U6's fitted-multiplier deliverable exists: it lets a reader apply
  their own line to their own window specification.
- **F2, the hard floor: whatever arm H measures, per window specification.** Arm H is a
  single-threaded Python program consuming the same input topic with `confluent_kafka` and doing the
  same windowed aggregation in a dictionary - no wrapper, no crossing, and deliberately
  **stateless and non-durable**: no store, no changelog, no rebalance recovery, no late-record
  handling. Its rate is therefore an **upper bound** on what a real reimplementation sustains, and
  every verdict taken against F2 is recorded as taken *against a non-durable single-threaded
  reimplementation*, with that gap listed beside H's rate. Beating the host doing it itself is the
  one comparison the wrapper must win, because reimplementing is what Faust, Quix Streams and
  Bytewax did. What is pre-registered is the rule; the number is measured, not chosen.
- **The verdict lattice is evaluated F2 first**, so it partitions the outcome space whichever way F1
  and arm H's measured rate order themselves: fails F2 -> the bet is off; clears F2 and F1 ->
  Viable; clears F2 only -> Marginal. Nothing bounds H below F1 - a single-threaded consume path can
  plausibly measure above 1,000, or above the wrapper's plateau - and the ordering is fixed here,
  before any arm runs, so a surprising H cannot rewrite the bands after the number is in hand.
  "Clears" means the arm's rate minus its reported spread is at or above the floor (for F2, at or
  above H's rate plus H's spread); a spread straddling a floor routes to the resweep action, not to
  either adjacent branch.
- **The withdrawn 100 records per second, recorded with its defect.** Earlier drafts set F2 at 100
  on the beat-the-reimplementation argument. The argument does not reach that number: 100 records
  per second is 10ms per record, twelve crossings cost roughly 1.8ms at the published 150us, and an
  in-process dictionary update costs microseconds. What the argument omits is that the host
  reimplementation must also *consume from Kafka single-threaded*, which is where its real ceiling
  sits - a rate nobody here had measured. The choice was neither derived nor measured, which is the
  defect; it is replaced by arm H's rule rather than re-argued.

## 2. The authoritative baseline

**The baseline for every comparison is the control arm measured in the same session as its
treatment arm, never a cited constant** (the plan's KTD18). The published figures - roughly
6,500-7,000 invocations per second single-thread, and 9,501 records per second at eight threads
([`perf-streams-crossing-attribution.md`](perf-streams-crossing-attribution.md),
[`perf-crossing-is-cpu-and-serialised.md`](perf-crossing-is-cpu-and-serialised.md)) - are recorded
here for pre-registering predictions only, with the caveat that they were measured on an
**independent per-record transform** (`mapValues`), not a per-key serial aggregate. An aggregate arm
falling short of them is expected and is not by itself evidence of anything; the floors, however,
read on the delivered rate whatever its cause.

## 3. The withdrawn absolute-ceiling derivation

"One lock at 120us fixed is about 8,300 crossings per second for the whole JVM" is **withdrawn**.
The `transmitLock` guards each outbound *message*, not the whole crossing; the serialised fraction
of the 120us fixed cost is unmeasured; and the measured plateau (9,501/s) exceeds the derived
ceiling - the tell that the derivation was wrong rather than the measurement. No absolute whole-JVM
crossing rate may be derived from the fixed cost until the serialised fraction is measured. Every
verdict in the spike rests on a within-session ratio instead.

## 4. The transport scope

Every verdict this spike records reads **"over the current single-session transport"**. One gRPC
stream per stream thread is named by
[`perf-crossing-is-cpu-and-serialised.md`](perf-crossing-is-cpu-and-serialised.md) as the thing to
measure before the bundling plan proceeds, and it is not measured here. A result recorded without
that scope would later be read as a property of the approach rather than of one transport.

## 5. The inherited premise

**"Windowing is not optional in practice" is inherited from dimension 4 of
[`streams-coupling-dimensions.md`](streams-coupling-dimensions.md), where it is asserted without
evidence, and nothing in this spike tests it.** It is the premise that turns a confirmed ceiling
into a direction-closing result. Every conclusion that closes a direction is bounded by it in
writing - and the bound extends to the Marginal branch: a specification recorded as not offered
narrows the same claim a bet-off falsifies, so the strategy text carries the premise there too,
marked untested at per-specification granularity.

---

## Results

Appended by the units as they run. Nothing above this line changes after U6 starts; a correction to
the pre-registration is recorded here as a dated entry, never edited in place.
