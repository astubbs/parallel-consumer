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

### U8 - sources and protocol findings, 2026-08-25

Source-read unit, the practice `streams-verify-against-the-kafka-sources.md` owns applied as a
unit. Sources: the Kafka Streams **3.9.2** sources jar
(`~/.m2/repository/org/apache/kafka/kafka-streams/3.9.2/kafka-streams-3.9.2-sources.jar`; paths
below are within it) and this branch's own
`parallel-consumer-proxy-streams/src/main/proto/parallelconsumer/streams/v1alpha1/streams.proto`.
Each claim was stated as a prediction before the read; every one below is marked confirmed or
refuted against the named file.

#### A. Suppression - a confirmation exercise

All four claims were already checked during the plan's review and held; this is the written
finding, the version pin and the consequence, not an open question.

1. **`suppress(untilWindowCloses(...))` statically requires a `StrictBufferConfig`** - **confirmed**.
   `org/apache/kafka/streams/kstream/Suppressed.java`:
   `static Suppressed<Windowed> untilWindowCloses(final StrictBufferConfig bufferConfig)`. A
   lenient (`EagerBufferConfig`) configuration is a compile error, not a runtime refusal.
2. **`Suppressed.BufferConfig.unbounded()` grows until the JVM dies rather than shedding** -
   **confirmed**. `unbounded()` returns `new StrictBufferConfigImpl()`
   (`Suppressed.java`), whose no-arg constructor sets `maxRecords = Long.MAX_VALUE`,
   `maxBytes = Long.MAX_VALUE`, `bufferFullStrategy = SHUT_DOWN`
   (`org/apache/kafka/streams/kstream/internals/suppress/StrictBufferConfigImpl.java`). In
   `KTableSuppressProcessorSupplier.java`'s `enforceConstraints`, `overCapacity()` compares against
   those bounds, so at `MAX_VALUE` neither the `EMIT` shed path (strict configs never carry it) nor
   the `SHUT_DOWN` throw is reachable - heap exhaustion arrives first.
3. **The suppress processor schedules no punctuator** - **confirmed**.
   `KTableSuppressProcessorSupplier.java` contains no `schedule(` call and no punctuator; the whole
   emission path is `process()` -> `buffer(record)` -> `enforceConstraints()`, and
   `enforceConstraints` evicts against `observedStreamTime`, which moves only in `process()` via
   `Math.max(observedStreamTime, record.timestamp())`. Emission is driven solely by records
   arriving and raising stream time.
4. **Consequence** - a quiet partition never emits its final window, and the host cannot tell that
   from a stuck engine: the wire's server->host message set is `Ready`, `HandleAssigned`,
   `Invocation`, `Fault`, `TopologyDescription`, `GetResult` (`StreamsServerMessage` in
   `streams.proto`) - no engine-state, stream-time or liveness signal exists. **P2 inherits this
   risk unchanged**, because P2's crossings are emits and a close-only emit rule emits nothing on a
   quiet partition.

#### B. `EmitStrategy.onWindowClose()` - P2's main emit lever

- **Public DSL in 3.9.2** - **confirmed**. `TimeWindowedKStream.emitStrategy(EmitStrategy)`
  (`org/apache/kafka/streams/kstream/TimeWindowedKStream.java`), `EmitStrategy.onWindowClose()`
  (`org/apache/kafka/streams/kstream/EmitStrategy.java`). One restriction found:
  `TimeWindowedKStreamImpl.emitStrategy` throws `IllegalArgumentException` for `UnlimitedWindows`.
- **Not a suppression buffer** - **confirmed**. In
  `org/apache/kafka/streams/kstream/internals/AbstractKStreamTimeWindowAggregateProcessor.java`,
  `maybeForwardUpdate` returns immediately when the strategy is `ON_WINDOW_CLOSE`, and emission
  happens in `fetchAndEmit`: a `windowStore.fetchAll(emitRangeLowerBound, emitRangeUpperBound)`
  range fetch over closed windows, called from `maybeForwardFinalResult`, which
  `KStreamWindowAggregate`'s `process()` calls on record arrival. No buffer exists to grow, and no
  `StrictBufferConfig` is involved anywhere on the path.
- **The emit interval: engine default is 1000 ms, wall-clock** - verified. `init` reads
  `EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION`
  (`__emit.interval.ms.kstreams.windowed.aggregation__`, `StreamsConfig.InternalConfig`) with
  default `1000L`; `shouldEmitFinal` throttles on `internalProcessorContext.currentSystemTimeMs()`
  (wall clock), then requires the window close time to have progressed.
- **Correction to the U1 entry above, recorded here rather than edited in place:** the U1
  observation note says the throttle "defaults to 200ms". **No 200 ms constant exists on this path
  in 3.9.2** - the engine default is 1000 ms, and `TopologyTestDriver` 3.9.2 additionally does
  `putIfAbsent(__emit.interval.ms.kstreams.windowed.aggregation__, 0L)` in its own setup, so TTD's
  *effective* default is zero (verified by `javap -c` over
  `kafka-streams-test-utils-3.9.2.jar`'s `TopologyTestDriver.class`; no sources jar for test-utils
  is in `~/.m2`, so this one fact is bytecode-verified rather than source-read). U1's explicit
  `0L` config is therefore redundant-but-harmless in TTD, and load-bearing only against a broker.
  The same wrong "default 200ms" figure is in the test's own comment
  (`WindowedAggregatorCallCountTest.java`, "default 200ms") and should be fixed when that file is
  next touched; its origin could not be established from the 3.9.2 sources.
- **Which of suppression's exclusion arguments carry over:** neither buffer argument does - there
  is no `StrictBufferConfig` requirement and no unbounded buffer. **The quiet-partition hazard
  carries over in full**, because `maybeForwardFinalResult` runs only inside `process()`: no
  record, no range fetch, no emit, and (per A.4) nothing on the wire distinguishes that from a
  stuck engine. Whether `onWindowClose` should be offered on the wire is a deferred surface
  decision, out of this unit's scope.

#### C. Reverse reads - the correction, verified implementation by implementation

Prediction: the `backward*` methods throw only as defaults on the bare `ReadOnlyWindowStore`
interface, and every implementation on the interactive-query path overrides them. **Confirmed for
all five**, in 3.9.2:

| Implementation | All four `backward*` overridden? | Shape of the override |
|---|---|---|
| `state/ReadOnlyWindowStore.java` (interface) | n/a - the four `default` methods each `throw new UnsupportedOperationException()` | the origin of the plan's earlier wrong claim |
| `state/internals/CompositeReadOnlyWindowStore.java` | yes | iterates the per-task stores, delegating `backwardFetch` etc. to each |
| `state/internals/ReadOnlyWindowStoreFacade.java` | yes | delegates every `backward*` to `inner` - and this is what `StreamThreadStateStoreProvider` wraps a `TimestampedWindowStore` in for `QueryableStoreTypes.WindowStoreType` (`new ReadOnlyWindowStoreFacade<>((TimestampedWindowStore...) store)` in `StreamThreadStateStoreProvider.java`) |
| `state/internals/MeteredWindowStore.java` | yes | wraps `wrapped().backwardFetch(...)` in metered iterators |
| `state/internals/RocksDBWindowStore.java` | yes | delegates to `wrapped().backwardFetch(...)` on the segmented bytes store, which implements all four (`AbstractRocksDBSegmentedBytesStore.java`) |
| `state/internals/InMemoryWindowStore.java` | yes | shared `fetch(key, from, to, forward=false)` path over a navigable map |

No implementation on the path fails to override - none found that does not hold. One documented
caveat, from the interface's own javadoc: across *multiple local stores* on one instance, forward
and backward range-key fetches do not interleave ordering between stores (per-store order only).
That bounds what "reverse order" means for a composite, not whether it exists.

**Conclusion: the plan's R19 refusal of reverse order is a scope choice about response shape,
never a capability limit.** The capability is implemented all the way down in 3.9.2.

#### D. Invocation identity - the finding that replaces the cut unclean-stop experiment

Read from this branch's `streams.proto` and the engine that speaks it, not from Kafka.

1. **`Invocation` does carry `correlation` and `function_token`** - **confirmed**
   (`message Invocation`, fields 1 and 2). Its full field set is `correlation`, `function_token`,
   `key`, `value`, `aggregate`, `kind`, `right`. **`correlation` is minted per call**, not per
   record: `InvocationRegistry.awaitResult` does `nextCorrelation.getAndIncrement()` for every
   invocation
   (`parallel-consumer-proxy-streams/src/main/java/bz/stub/parallelconsumer/streams/InvocationRegistry.java`).
   Nothing on the wire says which record a call is about - no topic, partition, offset, timestamp
   or record id travels.
2. **Therefore a host cannot distinguish a replayed record after an unclean stop from one of
   twelve legitimate overlapping-window calls with the same key and value.** Both arrive with a
   fresh correlation - a fresh correlation is exactly what a legitimate second call looks like too.
   (They are not byte-identical on the wire; the point does not need them to be.)
3. **Therefore a host aggregator cannot be made idempotent by the host**, whatever contract this
   plan writes - there is no contract the host can honour without identity. The cures are
   engine-side (exactly-once) or wire-side (an identity field on `Invocation`), and both are owned
   by other dimensions of the register, not this spike.
4. **At P2 the question changes shape rather than disappearing:** the host is called per emit, and
   P2's planned emit carries decomposed window bounds (KTD2's decomposition is the design; P2 is
   not built, so this is the plan's shape, not a read fact). A replay then presents as a re-emit of
   a window the host already folded - at least *distinguishable* by its bounds. A second,
   unlooked-for argument for the placement.

#### Falsifiers - searched for, honestly, and not found

- **A punctuator in the suppress processor or on the `onWindowClose` emit path** would dissolve the
  quiet-partition hazard. Searched `KTableSuppressProcessorSupplier.java`,
  `KStreamWindowAggregate.java` and `AbstractKStreamTimeWindowAggregateProcessor.java` for
  `schedule`/punctuator: **none exists** in 3.9.2. Both emit paths run only inside `process()`.
- **A record-identifying field on `Invocation`** would make the idempotency contract writable. The
  message's seven fields (above) contain none: `key` is shared by every call for that key, and
  `correlation` identifies the call. **Not found.**

#### The three Scope Boundaries exclusions: all three stand

- **Suppression as built surface: stands.** Both driving facts confirmed at 3.9.2 (A.1-A.3), and
  the sharpened `onWindowClose` distinction the boundary already records is right: neither buffer
  argument reaches it, only the quiet-partition hazard does, and it stays unbuilt for want of a
  question rather than for suppression's reasons.
- **Reverse reads (R19): stands, as corrected.** The refusal is a scope choice about response
  shape; the capability exists all the way down (C). The plan's current text already states this;
  the source read now backs it per implementation.
- **The cut unclean-stop experiment: stands.** The finding it was reaching for is delivered here by
  reading (D), boundary-specifically - which the experiment never was - and dimension 5 stays
  unclaimed by this plan.

**What could not be established by reading, named rather than papered over:** the origin of the
U1 note's "200ms" figure (no such constant on the 3.9.2 path); and the TTD emit-interval override,
which was established from bytecode (`javap`) rather than sources because no
`kafka-streams-test-utils` sources jar is present in `~/.m2`.

### U2 - hot-key throughput, normalised to invocations, 2026-08-25

Predictions restated from the plan's U2, written here before the first measured run. Harness:
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python/demo/streams_windowing_lab.py`
(new, experiment `hot-key`), using the existing `reduce` operator - already a per-key serial
dependency across the boundary. Arms matched on host invocations `I` (reduce skips each key's
first value): arm A ("hot") sends `I + 1` records under ONE key; arm B ("spread") sends `I + K`
records over `K = 8,000` keys. 1 KB payloads, eight partitions, eight stream threads,
`commit.interval.ms` set explicitly. Sweep `I` over 32,000 / 64,000 / 128,000, arms interleaved
A,B, throughput read from the sink topic's broker log-append clock, slope fitted across the
sweep; every point is at or above the 32,000-invocation warm-up line.

| # | Prediction | Outcome |
|---|---|---|
| 1 | Arm A lands near the single-thread band, 6,500-7,000 inv/s (one key -> one partition -> one stream thread) | **shortfall, not a refutation** - 5,502-5,636 inv/s, ~15% below the band; the registered caveat covers it (serial aggregate, different hardware). It did **not exceed** the band, so the interesting branch did not open |
| 2 | Arm B lands near 9,500 inv/s (the eight-thread plateau), NOT eight times arm A | **the load-bearing half held**: 1.22x arm A at I=64,000, strictly under 4x, nowhere near eight times. The absolute plateau also fell short (6,233-7,003 inv/s vs 9,501), covered by the same caveat. No near-linear scaling, so nothing here reopens bundling or one-session-per-stream-thread |
| 3 | The achievable bundle size for arm A is one, by construction: the next record for a key cannot cross until the previous accumulator returns | holds by construction |

**Caveat, binding (section 2 above):** a shortfall against 1 or 2 is not a refutation - those
baselines were measured on an independent per-record transform (`mapValues`), not a per-key serial
aggregate. What would be interesting: arm A *exceeding* the band, or arm B scaling near-linearly
with threads, which would reopen bundling and one-session-per-stream-thread. A further hardware
caveat registered before the run: the published bands were measured on an Apple Silicon macOS
developer machine; this run is on a 32-core Linux box, so the *ratio* between the arms, the fitted
slopes and the instrument check are the load-bearing results, and any absolute agreement with the
published bands is checked but not relied on.

**Conditions.** 32-core Linux box; compose broker `confluentinc/cp-kafka:7.9.0` on loopback
(`127.0.0.1:19095`, fresh for this session, torn down after); engine on Temurin 17.0.20+8 - with
the box's ambient `JAVA_TOOL_OPTIONS` capping the engine JVM at `ActiveProcessorCount=8` and
`MaxRAM 48g` at 20%, applied identically to every run and both arms; Python 3.13.5; gRPC over
loopback; `num.stream.threads=8`, 8 partitions, `statestore.cache.max.bytes=0`,
**`commit.interval.ms=200`** (set explicitly by the lab and printed - neither `demo_kafka.py` nor
`demo_options.py` sets it); 1 KB payloads. Machine quietness: 1-minute load read before **every**
measured run and recorded with it; runs started only under the load-8 line (per-run start loads
2.13-7.85; the gate paused 13 times, and the elevated load it waited out was the previous run's
own decaying workload - the box ran nothing else). Both arms' printed `Topology.describe()` are a
single sub-topology, source -> reduce -> toStream -> sink, no repartition - groupByKey preserves
the key, so the arms differ only in the seeded key distribution.

**Validity, all 24 runs:** host-counted invocations equalled `I` exactly (reduce skipped exactly
one first value per key), sink updates equalled the derived record count (cache off), the engine's
consumer group read STABLE/8 on every sample after joining, and every sink record carried a broker
log-append timestamp. No run was discarded.

**Rates, in invocations per second (broker log-append clock; derived record counts beside):**

| Arm | I | records | inv/s mean | min-max | n |
|---|---|---|---|---|---|
| A hot (1 key) | 32,000 | 32,001 | 5,617 | 5,109-5,935 | 3 |
| A hot | 64,000 | 64,001 | 5,636 | 4,780-6,244 | 3 |
| A hot | 128,000 | 128,001 | 5,542 | 5,285-5,972 | 3 |
| B spread (8,000 keys) | 32,000 | 40,000 | 7,003 | 6,380-7,603 | 3 |
| B spread | 64,000 | 72,000 | 6,867 | 5,845-7,698 | 3 |
| B spread | 128,000 | 136,000 | 6,233 | 4,468-7,699 | 3 |

Fitted slope across the sweep (all nine points per arm at or above the 32,000-invocation warm-up
line, none discarded): **arm A 182us/invocation (5,502 inv/s steady-state, intercept -0.11s); arm
B 179us/invocation (5,579 inv/s, intercept -1.50s)**. The two marginal costs are
indistinguishable at this noise level (arm B's reps at I=128,000 span 16.6-28.7s windows); arm
B's point-rate advantage sits in its intercept, not its slope.

**Instrument check (R4):** three paired (plain, +1ms-in-the-reducer) arm-A runs at I=32,000.
Per-invocation cost moved **198 -> 1,290us (delta 1,091us against 1,000us added)**; throughput
fell 5,446-5,859 -> 753-792 inv/s. The number moves by roughly the added delay per invocation, so
the harness measures what it claims to.

**Did key spread rescue the hot key? No.** At matched invocations, spreading the serial chain over
8,000 keys with eight stream threads and eight partitions bought **1.22x** at I=64,000 - and
nothing at all in the fitted marginal cost (179 vs 182us/invocation). The single-session
transport's serialised boundary dominates before per-key parallelism can bind, consistent with the
`transmitLock` finding in
[`perf-crossing-is-cpu-and-serialised.md`](perf-crossing-is-cpu-and-serialised.md). For the hot
key itself the achievable bundle size is one by construction - accumulator `n+1` needs accumulator
`n`, so there is never a second invocation in flight to bundle. **Consequence for the parked
bundling work: as the plan stated, bundling cannot amortise a serial chain.** A hot-key
aggregation is outside what bundling can rescue; and on this transport even the spread case left
little for per-key concurrency, so nothing measured here reopens bundling or
one-session-per-stream-thread (the latter stays open on its own prior grounds, not this unit's).
Scope: over the current single-session transport, as section 4 requires.

Harness: `streams_windowing_lab.py` (path above), experiment `hot-key` - one lab, an experiment
selector, later units add a function rather than a sibling script.
