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

### U1 - aggregator calls and window emits (TTD), 2026-08-25

Predictions restated compactly from the plan's U1, written here before the first run. All arms:
100 records over one key, event times one minute apart, base timestamp two hours past the epoch
(`TimeWindows.windowsFor` clamps the earliest window start at zero, so every arm must start at
least `size - advance` = 55 minutes past the epoch or the hopping arms under-count).

| # | Prediction | Predicted count | Outcome |
|---|---|---|---|
| 1 | Tumbling 1h: calls == records | 100 calls | **confirmed** - 100 calls observed |
| 2 | Hopping 1h/5m: calls == 12x records | 1,200 calls | **confirmed** - 1,200 calls observed |
| 3 | Hopping 1h/30m (linearity): calls == 2x records | 200 calls | **confirmed** - 200 calls observed |
| 4 | suppress(untilWindowCloses): calls unchanged; emits == closed (key,window) pairs | 1,200 calls, 19 emits | **confirmed** - 1,200 calls, 19 emits observed |
| 5 | Without suppression, emits == 12x records in TTD (commit per record) | 1,200 emits | **confirmed** - 1,200 emits observed, equal to the aggregator call count |
| 6 | advanceWallClockTime does not close a window | 0 emits | **confirmed** - output topic empty, 0 emits, after advanceWallClockTime(1 day) on the suppressed topology |
| 7 | emitStrategy(onWindowClose) matches suppression's emit count, no buffer | 1,200 calls, 19 emits | **confirmed** - 1,200 calls, 19 emits observed, matching the suppressed arm |
| - | Record below every matched window's close bound: zero calls, dropped-records once per matched window | 0 calls, 12 drops | **confirmed** - 0 calls, dropped-records-total 12.0 |
| - | Companion at windowCloseTime - 1: calls for every still-open matched window | 11 calls, 1 drop | **confirmed** - 11 calls, dropped-records-total 1.0; scenario 6 measured the boundary, not a coincidence |

The 19 closed pairs: records span minutes 120-219 past epoch; hopping 1h/5m windows containing
records have starts 65..215 (31 windows); with grace zero, closed means window end <= observed
stream time 219 min, i.e. starts 65..155 - 19 windows, one key each.

**Observations, environment and caveats:**

- Test: `parallel-consumer-proxy-streams/src/test/java/bz/stub/parallelconsumer/streams/WindowedAggregatorCallCountTest.java`,
  nine tests, all green in the full module suite (83 tests, 0 failures).
- Kafka Streams **3.9.2** (the parent pom's `kafka.version`; the surefire classpath resolved
  `kafka-streams-3.9.2.jar` and `kafka-streams-test-utils-3.9.2.jar`). Epoch offset: every
  100-record arm starts at **two hours past the epoch** (clamp margin is `size - advance` = 55
  minutes); the close-bound arms anchor stream time at ten hours past the epoch.
- The zero-call bound is exactly where the plan's corrected scenario 6 put it: in 3.9.2
  `KStreamWindowAggregate` tests `windowEnd > windowCloseTime`, so mere lateness still aggregates -
  the companion arm's record at `windowCloseTime - 1ms` was aggregated into all 11 still-open
  windows and dropped only from the one whose end had passed.
- One measurement note: the on-window-close arm is only deterministic with the internal config
  `__emit.interval.ms.kstreams.windowed.aggregation__` set to 0 - the emit-final pass is throttled
  by a 200ms WALL-clock interval, and TTD's mock wall clock never moves on its own, so at the
  default the closed windows sit unemitted behind the throttle. The test names this.
- **TTD over-count limitation restated (KTD11): the emit counts here are the UPPER bound - TTD
  commits (and so flushes the cache) per record, making unsuppressed emits equal P1's call count by
  construction; only a broker run can measure the caching collapse, and that is U6's job.**
- **Instrument check (R4):** scenario 3 sabotaged by setting its advance from 30 to 5 minutes; the
  test failed red with `expected: 200 / but was : 1200`
  (`WindowedAggregatorCallCountTest.hoppingOneHourAdvancingThirtyMinutesCallsTheAggregatorTwicePerRecord`),
  then the advance was restored and the suite re-ran green. The counter moves.

Consequence for the placements: P1's per-record crossing count under a 1h/5m hopping window is
12x the record count, and the linearity arm pins it to `ceil(size / advance)` rather than anything
window-count-shaped. P2 under a close-only emit rule (suppress or onWindowClose) crosses once per
closed (key, window) pair - independent of the record count - which is the collapse P2's value
rests on; the record-rate-driven half of P2's count (unsuppressed, cache-flush-driven) cannot be
distinguished from P1 in TTD and waits on U6's broker arms.
