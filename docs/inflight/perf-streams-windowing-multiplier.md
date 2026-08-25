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

### U6 - placement comparison, 2026-08-25

Harness: `streams_windowing_lab.py`, experiments `placement` (arms A-E, phases `shared`/`emit`)
and `host-reimpl` (arm H standalone). Rates in **records per second** - the floors' unit (R6 names
U6 as the exception to invocation-normalising, because the multiplier under test IS the
invocations-per-record ratio). **Both verdicts are BET OFF, decided F2-first**; details, the
partial arm-E result, and what never ran are all below.

**Conditions (the shared load).** 1 KB payloads, 8,000 keys over eight partitions, eight stream
threads, `commit.interval.ms=200` and `statestore.cache.max.bytes=0` (both set explicitly and
printed; cache off makes every put forward, so emit counts are exact), engine Temurin 17.0.20+8
under the box's ambient `JAVA_TOOL_OPTIONS` (`MaxRAM=48g`, `MaxRAMPercentage=20`,
`ActiveProcessorCount=8`), compose broker `confluentinc/cp-kafka:7.9.0` on loopback
(`127.0.0.1:19095`), Python 3.13.5, 32-core Linux box. **Event times: every record carries the
same producer-assigned constant timestamp (1,750,000,000,000 ms), far past the epoch clamp** - a
constant keeps each record in exactly `ceil(size/advance)` windows, so crossing counts are exact,
no window closes, and nothing is late. Sweep in crossings (32,000 / 64,000 / 128,000; each P1
arm's record count is the point over its multiplier; arm D, having zero crossings, runs at arm
A's record counts so its windows are measurable), arms interleaved per point, throughput from the
sink topic's broker log-append clock, completion by **quiescence** (no new sink record for 15
commit intervals = 3.0 s, printed; emit counts validated post-hoc as bands, never used to stop -
the inherited last-value-per-key predicate is invalid over `to_stream`'s colliding inner keys).
Every arm sinks through `to_stream` (R29).

**Contention deviation, named.** The pre-registered quiet-machine gate (start only under
1-minute load 8) was **disabled for the decisive session** on the orchestrator's instruction: the
box carried ambient load 6-19 from unrelated agent sessions and the gate stalled a first session
14 x 30 s with no decay in sight. Per-run 1-minute load is recorded beside every run instead.
Consequences: the in-session ratios (B/D attribution, B/A, C/A linearity, and every F2 comparison
- arm H ran interleaved under the same ambient load) are protected by interleaving; **absolute
rates are biased low against F1**, and any F1 comparison within plausible contention bias would
be routed to unsettled rather than forced - it never mattered, because both verdicts fell at F2
by two orders of magnitude, which contention cannot manufacture. An earlier same-day session on a
quieter box (loads 1.2-8) produced rates inside the spreads reported below; its valid runs are
pooled into the per-point spreads and its H rates into H's.

**Rates (pooled valid runs at the largest sweep point, n=4: one gate-stalled quiet-box session
n=2, the decisive gate-off session n=2; fitted figures from the decisive session's 6-run
regression per arm across all three sweep points, intercept absorbing startup):**

| Arm | Placement / role | Records | Crossings/record | rec/s mean (min-max, n) | Fitted steady-state |
|---|---|---|---|---|---|
| A | P1 tumbling 1h, host at aggregator | 128,000 | 1.00 exact | 7,809 (5,787-8,681, 4) | 7,305 rec/s (137us/rec) |
| B | P1 hopping 1h/5m, host at aggregator | 10,666 | 12.00 exact | 629 (511-707, 4) | 603 rec/s (1,657us/rec) |
| C | P1 hopping 1h/30m (linearity, no verdict) | 64,000 | 2.00 exact | 3,560 (2,694-4,172, 4) | 3,938 rec/s (254us/rec) |
| D | crossing-free control (LAST_BYTES, no host fn) | 128,000 | 0.00 measured | 20,767 (18,097-22,695, 4) | 20,062 rec/s (50us/rec) |
| H | reimpl floor, tumbling (defines F2-tumbling) | 128,000 | n/a (in-process dict) | 723,265 (596,384-1,015,572, 4) | - |
| H | reimpl floor, hopping-12 (defines F2-hopping) | 128,000 | n/a (12 dict updates/rec) | 89,821 (88,484-91,619, 4) | - |

Crossings counted **client-side**: the host is the invocation target, so the registered function
counts every crossing exactly; arm D registers no function at all, making its zero a measurement
- an engine invocation would name an unregistered token, error the answer, and fail the run.
Emit counts were exact on every valid run (multiplier x records, cache off). Arm H is
single-threaded `confluent_kafka` batch-consume into a dict via the same `windowsFor` arithmetic
(dict updates asserted = multiplier x records on every run), stateless and non-durable, run only
while the engine was idle.

**The two verdicts, F2-first ("clears" = arm min at or above floor max), scoped to the current
single-session transport, bounded by the untested "windowing is not optional in practice"
premise (section 5):**

- **Tumbling: BET OFF.** Best (only) arm A: max 8,681 rec/s vs tumbling-H min 596,384 - fails F2
  by ~69x with non-overlapping spreads. F1 is moot (A clears it; clearing the parity floor while
  losing to the reimplementation is still a loss - the pre-registered lattice). The plan's
  contingent tumbling-P2 arm was conditioned on A *missing F1*, which did not happen, so it was
  not run - and no P2 arm could close a 69x gap whose cause is the crossing itself (D, the same
  topology with zero crossings, reaches only 20,767). Verdict taken **against a non-durable
  single-threaded reimplementation** (H's scope, section 1).
- **Hopping-by-twelve: BET OFF.** Arm B at the shared load (the unconditional verdict carrier):
  max 725 rec/s vs hopping-H min 88,484 - fails F2 by ~122x, non-overlapping. B also fails F1
  (max 725 < 1,000; even the quiet-box session's max was 725) - reported for completeness, moot
  under the F2 fail. Same H scope note. Per the exit criteria, no further measurement for either
  specification.

**Attribution, B against D (the only pair that isolates the boundary, R5):** ratio **0.0303**
(pooled largest-point means; the decisive session's in-process figure 0.0320, fitted 0.0301).
This is **below 1/16, therefore recorded as the pre-registered anomalous band - the mechanism
neither confirmed nor refuted in magnitude, ratio reported as itself**. The magnitude anomaly
decomposes cleanly and the multiplier is visible in it: **B/A = 0.0805 = 1/12.4**, almost exactly
the multiplier - but the band read on D, and D (zero crossings, same operator, same store, same
emit volume) runs 2.7x above A, because even ONE crossing per record costs more than the whole
native hopping topology. The band was drawn expecting D near A; D exceeding A by 2.7x pushes B/D
past 1/16 for a reason that *strengthens* rather than weakens the boundary attribution. Scenario
4 (arm B, 20 keys, 480 records, cache off vs 64 MB): aggregator calls **5,760 = 5,760
identical**, emits **5,760 vs 888** - caching dedups emits and never calls, the broker-side half
of U1's caching question.

**The fitted multiplier (the deliverable meant to outlive the verdict).** Fit over arms A, C, B
(multipliers 1, 2, 12; per-record cost linear in m): **t(m) = 33us + m x 135us** (decisive
session, 6 points per arm). The fitted rate crosses **F1 (1,000 rec/s) at multiplier 7.14**
(per-rep fits span 6.17-8.49): at this load a window specification with `ceil(size/advance)` up
to ~7 clears F1 at P1, wider does not. It crosses **F2-tumbling (608,612 rec/s in-session) at
m = 0.05 and F2-hopping (89,590) at m = 0.12** - both below 1, i.e. **no multiplier as low as
even one crossing per record reaches either F2**; the wrapper loses to the reimplementation
before windowing enters at all, which relocates the problem from the multiplier to the
per-crossing cost itself.

**Arm E - PARTIAL, one operating condition only; the swept curve did not run.** The plan's
0.5-20 records-per-key-per-flush sweep with matched B and H re-runs per point was cut by the
orchestrator's session time-box under contention, and is additionally non-decisive under the exit
criteria once both specifications fell at F2 ("no further measurement runs for that
specification"). What DID run, at one condition (keys=100, records-per-key=8, cache 19,660,800 B
by the pre-registered 2x end-of-run formula, commit 200 ms, single runs): **E crossings/record
1.50 against matched-B's exact 12.00** - the collapse is real and E stayed under B (prediction 7
held where measured) - at naive rho (rate x interval / keys) 6.18, predicting 12/6.18 = 1.94;
measured 1.50 is consistent with commit-duration stretch (flush rounds longer than the interval
lower the effective flush rate). **Zero cache evictions asserted** on that run and its matched B.
The eviction instrument itself was proven able to show a non-zero first: an undersized-cache run
(1 MB) reported **19,080 evictions** where evictions were forced. Instrument detail worth
keeping: Kafka Streams 3.9.2 exposes **no eviction metric** (hit-ratio and
`cache-size-bytes-total` only), so the lab reads `ThreadCache`'s own counters through its TRACE
logging (slf4j-simple onto the engine classpath for E-family runs, per-run log file, per-put
"Evicted n entries" sum cross-checked against the flush-stats cumulative `#evicts`), applied
identically to E and its matched B. E rates at the measured condition: E 3,089 rec/s vs matched
B 746 rec/s (single runs, scoped to that condition, never the general offer).

**Predictions 1-8 (plan U6), against what ran:**

| # | Prediction | Outcome |
|---|---|---|
| 1 | A near the eight-thread plateau (serial-aggregate caveat) | **confirmed with the registered caveat** - 7,809 mean vs 9,501 published (~18% short; different topology shape and hardware, per section 2) |
| 2 | B near A/12 | **confirmed** - B/A = 0.0805 = 1/12.4 |
| 3 | C near A/2 (linearity, not threshold) | **confirmed** - C/A = 0.456 = 1/2.19; A, C, B sit on the fitted line above |
| 4 | D at or above A, flat against the multiplier | **confirmed, beyond its own expectation** - D = 2.7x A while carrying multiplier 12; native hopping with a bounded combine outruns even one-crossing tumbling |
| 5 | B's counter reads 12x records; D's reads zero | **confirmed exactly, every valid run** |
| 6 | E crossings/record = 12 / records-per-key-per-flush | **partial** - at the one measured condition, 1.50 measured vs 1.94 naive-rho prediction (direction and order right; shortfall consistent with commit stretch); the sweep that would settle it did not run |
| 7 | E never exceeds B's crossing count | **held where measured** (1.50 < 12.00); untested across the sweep |
| 8 | H clears the withdrawn 100 rec/s comfortably | **confirmed, emphatically** - 88,484 minimum, ~900x the withdrawn floor; the old F2 was not accidentally right |

**Instrument checks (R4).** The crossing counter: arm C read exactly two per record and arm D
exactly zero on every valid run (PASS, printed by the harness); no arm showed a throughput drop
without a matching crossing-count rise - B's 12.4x drop against A carries its 12x counter. The
eviction reader: non-zero proven (19,080 where forced) before the zero was believed. The rate
instrument itself carries U2's +1 ms delay check (198 -> 1,290us/invocation), not repeated here.

**Not run, with reasons:** arm E's rho sweep with matched B and H per point (time-box under
contention; non-decisive after the F2 verdicts); the contingent tumbling-P2 arm (its
precondition - A missing F1 - did not occur); resweeps (no floor was straddled - every floor
comparison was decisive with non-overlapping spreads). **Method trap recorded:** lab records
carry a constant 2025 event time, and the broker's time-based retention deletes "old" data past
a 5-minute retention check even from the active segment - a reused topic silently emptied and
cost one session (arm H read 0 records); every lab topic now sets `retention.ms=-1`, and the
harness comments say why.

**Consequence, in the plan's terms.** The organising question - where must the host function sit
- gets an answer neither placement expected: **nowhere**. P1 tumbling loses to the
reimplementation 69x with only one crossing per record; the fitted line says F2 is unreachable at
ANY multiplier including m < 1; P2's collapse is real (1.50 vs 12 crossings/record where
measured) but collapses toward a per-emit crossing cost that is itself the losing term. The
falsified quantity is the **per-crossing cost against an in-process consumer**, not the window
multiplier - the multiplier (confirmed, linear, 12x) only multiplies a loss already present at
one. `STRATEGY.md`'s Kafka Streams claim is falsified for windowed aggregation at both measured
specifications over the current single-session transport (U10 owns writing that in), against a
non-durable single-threaded reimplementation, under the untested windowing-is-not-optional
premise, at the loads named above.
