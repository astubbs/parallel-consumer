package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * U1 of the windowed-aggregation falsification spike: how many times would a windowed aggregation cross the host
 * boundary per record, at each placement? At P1 (host at the aggregator) the crossing count is the aggregator call
 * count; at P2 (JVM combine, host at emit) it is the emit count. Built directly against {@link StreamsBuilder} -
 * not {@code TopologyAssembler} - so this runs before any wire code exists, and counts with plain
 * {@link AtomicInteger}s: one inside the aggregator, one on the node downstream of {@code toStream()}.
 *
 * <p>Predictions were recorded in {@code docs/inflight/perf-streams-windowing-multiplier.md} before the first run;
 * the plan is {@code docs/plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md} (U1).
 *
 * <p><b>Recorded limitation (the plan's KTD11):</b> TopologyTestDriver commits - and so flushes the record cache -
 * after every processed record, which makes its emit counts the UPPER bound: exactly {@code ceil(size / advance)}
 * per record, which is P1's count. In TTD, P2's crossing count equals P1's by construction; only a broker run,
 * where caching deduplicates across records, can show the collapse (U6's job). The suppressed and
 * emit-on-window-close arms are the exception - their emit counts are close-driven, not cache-driven, so they are
 * deterministic here.
 *
 * <p><b>The base timestamp is load-bearing (never {@link Instant#EPOCH}):</b> {@code TimeWindows.windowsFor}
 * clamps the earliest window start at zero, so a record less than {@code size - advance} past the epoch falls
 * into fewer than {@code ceil(size / advance)} windows. At {@code Instant.EPOCH}, 100 records a minute apart give
 * 870 aggregator calls under a 1h/5m hopping window, not 1,200 - which would read as a refutation of the
 * multiplier for a reason that has nothing to do with it. Every arm here starts two hours past the epoch,
 * comfortably past the largest {@code size - advance} in use (55 minutes).
 */
class WindowedAggregatorCallCountTest {

    private static final String INPUT = "in";
    private static final String OUTPUT = "out";
    private static final String STORE = "window-agg-store";

    private static final Duration ONE_HOUR = Duration.ofHours(1);
    private static final int RECORDS = 100;

    /** Two hours past the epoch: past the 55-minute clamp margin the class javadoc explains. */
    private static final Instant BASE = Instant.EPOCH.plus(Duration.ofHours(2));

    /** How the aggregation's results reach the downstream counter. */
    private enum EmitMode {
        /** Stock DSL: cache-flush-driven updates (in TTD, one per dirty window per record - KTD11). */
        EAGER,
        /** {@code suppress(untilWindowCloses(...))}: only closed (key, window) pairs pass downstream. */
        SUPPRESSED,
        /** {@code emitStrategy(EmitStrategy.onWindowClose())}: closed pairs, through public DSL, no buffer. */
        ON_WINDOW_CLOSE
    }

    // Scenario 1 / prediction 1: tumbling windows have no overlap, so the multiplier is exactly one.
    @Test
    void tumblingOneHourCallsTheAggregatorExactlyOncePerRecord(@TempDir Path stateDir) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger emits = new AtomicInteger();
        // KTD12: ofSizeWithNoGrace/ofSizeAndGrace only - the deprecated TimeWindows.of silently carries
        // max(24h - size, 0) grace.
        TimeWindows tumbling = TimeWindows.ofSizeWithNoGrace(ONE_HOUR);

        try (TopologyTestDriver driver =
                new TopologyTestDriver(topology(tumbling, EmitMode.EAGER, calls, emits), config(stateDir))) {
            pipeOneHundredRecordsOneMinuteApart(driver);

            assertThat(calls.get()).isEqualTo(RECORDS);
            // Read while the driver is OPEN: close() deletes the state directory, and a post-close read has
            // produced a green test asserting nothing in this module before. Records at minutes 120..219 span
            // exactly the [120,180) and [180,240) tumbling windows.
            WindowStore<byte[], byte[]> store = driver.getWindowStore(STORE);
            int windowsHoldingState = 0;
            try (var iterator = store.all()) {
                while (iterator.hasNext()) {
                    KeyValue<Windowed<byte[]>, byte[]> ignoredEntry = iterator.next();
                    windowsHoldingState++;
                }
            }
            assertThat(windowsHoldingState).isEqualTo(2);
        }
    }

    // Scenario 2 / prediction 2: the headline multiplier - every record lands in ceil(size / advance) = 12
    // overlapping windows, and the aggregator is called once per window. Near 100 here would mean Kafka lifts
    // hopping aggregation, the multiplier premise collapses, and the plan's stop condition fires.
    @Test
    void hoppingOneHourAdvancingFiveMinutesCallsTheAggregatorTwelveTimesPerRecord(@TempDir Path stateDir) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger emits = new AtomicInteger();
        TimeWindows hopping = TimeWindows.ofSizeWithNoGrace(ONE_HOUR).advanceBy(Duration.ofMinutes(5));

        try (TopologyTestDriver driver =
                new TopologyTestDriver(topology(hopping, EmitMode.EAGER, calls, emits), config(stateDir))) {
            pipeOneHundredRecordsOneMinuteApart(driver);
        }

        assertThat(calls.get()).isEqualTo(12 * RECORDS);
    }

    // Scenario 3 / linearity arm: only the advance changes, so the count must track ceil(size / advance) = 2 -
    // twice scenario 1, not twelve times. This is also the sabotage target for the instrument check (R4): setting
    // the advance to five minutes must fail this test at 1,200, proving the counter can move.
    @Test
    void hoppingOneHourAdvancingThirtyMinutesCallsTheAggregatorTwicePerRecord(@TempDir Path stateDir) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger emits = new AtomicInteger();
        TimeWindows hopping = TimeWindows.ofSizeWithNoGrace(ONE_HOUR).advanceBy(Duration.ofMinutes(30));

        try (TopologyTestDriver driver =
                new TopologyTestDriver(topology(hopping, EmitMode.EAGER, calls, emits), config(stateDir))) {
            pipeOneHundredRecordsOneMinuteApart(driver);
        }

        assertThat(calls.get()).isEqualTo(2 * RECORDS);
    }

    // Prediction 5, the load-bearing one for P2: WITHOUT suppression, TTD emits every intermediate update -
    // commit per record flushes the cache, forwarding one update per dirty window per record. 12 x 100 = 1,200,
    // exactly P1's aggregator call count: in TTD, P2 equals P1 by construction (KTD11). Only a broker run, where
    // one flush covers many records, can show the emit count collapse - that is U6's job, not this test's.
    @Test
    void unsuppressedHoppingEmitsEveryIntermediateUpdateMatchingTheAggregatorCallCount(@TempDir Path stateDir) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger emits = new AtomicInteger();
        TimeWindows hopping = TimeWindows.ofSizeWithNoGrace(ONE_HOUR).advanceBy(Duration.ofMinutes(5));

        try (TopologyTestDriver driver =
                new TopologyTestDriver(topology(hopping, EmitMode.EAGER, calls, emits), config(stateDir))) {
            pipeOneHundredRecordsOneMinuteApart(driver);
        }

        assertThat(emits.get()).isEqualTo(12 * RECORDS);
        assertThat(emits.get()).isEqualTo(calls.get());
    }

    // Scenario 4 / predictions 3 and 4: suppression leaves the aggregator call count untouched and cuts emits to
    // the closed (key, window) pairs only - independent of the record count. Records span minutes 120..219, so
    // stream time ends at 219; with grace zero the closed hopping windows are those ending at or before 219:
    // starts 65, 70, .., 155 - nineteen windows, one key each.
    @Test
    void suppressionLeavesAggregatorCallsUnchangedAndEmitsOnlyClosedWindows(@TempDir Path stateDir) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger emits = new AtomicInteger();
        TimeWindows hopping = TimeWindows.ofSizeWithNoGrace(ONE_HOUR).advanceBy(Duration.ofMinutes(5));

        try (TopologyTestDriver driver =
                new TopologyTestDriver(topology(hopping, EmitMode.SUPPRESSED, calls, emits), config(stateDir))) {
            pipeOneHundredRecordsOneMinuteApart(driver);
        }

        assertThat(calls.get()).isEqualTo(12 * RECORDS);
        assertThat(emits.get()).isEqualTo(19);
    }

    // Scenario 5 / prediction 6: wall clock is not stream time. On the SUPPRESSED topology - on the unsuppressed
    // one this cannot fail, because the single record's update emits immediately and the test would pass whether
    // or not wall clock closed windows, a green test asserting nothing.
    @Test
    void advancingTheWallClockDoesNotCloseAWindow(@TempDir Path stateDir) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger emits = new AtomicInteger();
        TimeWindows hopping = TimeWindows.ofSizeWithNoGrace(ONE_HOUR).advanceBy(Duration.ofMinutes(5));

        try (TopologyTestDriver driver =
                new TopologyTestDriver(topology(hopping, EmitMode.SUPPRESSED, calls, emits), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    INPUT, new ByteArraySerializer(), new ByteArraySerializer());
            // KTD13: reading a windowed topic takes the two-argument TimeWindowedDeserializer only.
            TestOutputTopic<Windowed<byte[]>, byte[]> out = driver.createOutputTopic(OUTPUT,
                    new TimeWindowedDeserializer<>(new ByteArrayDeserializer(), ONE_HOUR.toMillis()),
                    new ByteArrayDeserializer());

            in.pipeInput(bytes("k"), bytes("v"), BASE);
            driver.advanceWallClockTime(Duration.ofDays(1));

            assertThat(out.readKeyValuesToList()).isEmpty();
            assertThat(emits.get()).isEqualTo(0);
        }
    }

    // Scenario 6: the zero-call bound. In 3.9.2 KStreamWindowAggregate computes
    // windowCloseTime = observedStreamTime - gracePeriodMs and tests each matched window with
    // windowEnd > windowCloseTime - so mere lateness still aggregates, and only a record for which EVERY matched
    // window has ended (a further `size` of margin, since windowsFor(t) yields ends up to t + size) produces zero
    // calls, with the dropped-records sensor incremented once per matched window.
    @Test
    void aRecordWhoseEveryMatchedWindowHasEndedNeverReachesTheAggregator(@TempDir Path stateDir) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger emits = new AtomicInteger();
        TimeWindows hopping = TimeWindows.ofSizeWithNoGrace(ONE_HOUR).advanceBy(Duration.ofMinutes(5));
        Instant streamTimeAnchor = Instant.EPOCH.plus(Duration.ofHours(10));

        try (TopologyTestDriver driver =
                new TopologyTestDriver(topology(hopping, EmitMode.EAGER, calls, emits), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    INPUT, new ByteArraySerializer(), new ByteArraySerializer());

            // Advance stream time to 600 minutes. This record itself matches 12 windows: 12 calls.
            in.pipeInput(bytes("k"), bytes("anchor"), streamTimeAnchor);
            int callsAfterAnchor = calls.get();
            assertThat(callsAfterAnchor).isEqualTo(12);
            assertThat(droppedRecords(driver)).isEqualTo(0.0);

            // 535 minutes: its matched windows end at 540..595 min, all at or before windowCloseTime = 600, so
            // the aggregator is never called and every one of the 12 matched windows records a drop.
            in.pipeInput(bytes("k"), bytes("stale"), streamTimeAnchor.minus(Duration.ofMinutes(65)));

            assertThat(calls.get() - callsAfterAnchor).isEqualTo(0);
            assertThat(droppedRecords(driver)).isEqualTo(12.0);
        }
    }

    // Scenario 7, the companion arm that proves scenario 6 measured the boundary rather than a coincidence: one
    // millisecond inside windowCloseTime, the record matches the same 12 windows but 11 of them end AFTER
    // windowCloseTime (605..655 min) and still aggregate; only the window ending exactly at 600 has closed and
    // records the single drop. Without this arm, scenario 6 passes for any timestamp low enough and says nothing
    // about where the cutoff sits.
    @Test
    void aRecordJustInsideTheCloseBoundaryStillReachesTheAggregatorForEveryOpenWindow(@TempDir Path stateDir) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger emits = new AtomicInteger();
        TimeWindows hopping = TimeWindows.ofSizeWithNoGrace(ONE_HOUR).advanceBy(Duration.ofMinutes(5));
        Instant streamTimeAnchor = Instant.EPOCH.plus(Duration.ofHours(10));

        try (TopologyTestDriver driver =
                new TopologyTestDriver(topology(hopping, EmitMode.EAGER, calls, emits), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    INPUT, new ByteArraySerializer(), new ByteArraySerializer());

            in.pipeInput(bytes("k"), bytes("anchor"), streamTimeAnchor);
            int callsAfterAnchor = calls.get();

            in.pipeInput(bytes("k"), bytes("edge"), streamTimeAnchor.minusMillis(1));

            assertThat(calls.get() - callsAfterAnchor).isEqualTo(11);
            assertThat(droppedRecords(driver)).isEqualTo(1.0);
        }
    }

    // Scenario 8 / prediction 7: EmitStrategy.onWindowClose() reaches scenario 4's emit count through public DSL
    // with no suppression buffer - the aggregator call count is untouched, and emits are the same 19 closed
    // (key, window) pairs.
    @Test
    void onWindowCloseEmitStrategyMatchesSuppressionsEmitCountWithoutABuffer(@TempDir Path stateDir) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger emits = new AtomicInteger();
        TimeWindows hopping = TimeWindows.ofSizeWithNoGrace(ONE_HOUR).advanceBy(Duration.ofMinutes(5));

        try (TopologyTestDriver driver = new TopologyTestDriver(
                topology(hopping, EmitMode.ON_WINDOW_CLOSE, calls, emits),
                emitFinalImmediatelyConfig(stateDir))) {
            pipeOneHundredRecordsOneMinuteApart(driver);
        }

        assertThat(calls.get()).isEqualTo(12 * RECORDS);
        assertThat(emits.get()).isEqualTo(19);
    }

    private static Topology topology(
            TimeWindows windows, EmitMode mode, AtomicInteger aggregatorCalls, AtomicInteger emits) {
        StreamsBuilder builder = new StreamsBuilder();
        TimeWindowedKStream<byte[], byte[]> windowed = builder
                .stream(INPUT, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .groupByKey(Grouped.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .windowedBy(windows);
        if (mode == EmitMode.ON_WINDOW_CLOSE) {
            windowed = windowed.emitStrategy(EmitStrategy.onWindowClose());
        }

        // The counting aggregator: P1's crossing count is exactly this call count. In-memory store, matching the
        // module's convention; retention must cover size + grace, and two hours does for every arm here.
        KTable<Windowed<byte[]>, byte[]> aggregated = windowed.aggregate(
                () -> new byte[0],
                (key, value, aggregate) -> {
                    aggregatorCalls.incrementAndGet();
                    return aggregate;
                },
                Materialized.<byte[], byte[]>as(
                                Stores.inMemoryWindowStore(STORE, Duration.ofHours(2), ONE_HOUR, false))
                        .withKeySerde(Serdes.ByteArray())
                        .withValueSerde(Serdes.ByteArray()));

        if (mode == EmitMode.SUPPRESSED) {
            aggregated = aggregated.suppress(Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig.maxRecords(10_000).shutDownWhenFull()));
        }

        // The downstream counter: P2's crossing count is this emit count.
        aggregated.toStream()
                .peek((windowedKey, value) -> emits.incrementAndGet())
                .to(OUTPUT, Produced.with(windowedSerde(windows.size()), Serdes.ByteArray()));
        return builder.build();
    }

    private static void pipeOneHundredRecordsOneMinuteApart(TopologyTestDriver driver) {
        TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                INPUT, new ByteArraySerializer(), new ByteArraySerializer(), BASE, Duration.ofMinutes(1));
        for (int i = 0; i < RECORDS; i++) {
            in.pipeInput(bytes("k"), bytes("v" + i));
        }
    }

    /** KTD13: the two-argument {@link TimeWindowedDeserializer}; the single-argument path is deprecated. */
    private static Serde<Windowed<byte[]>> windowedSerde(long windowSizeMs) {
        return Serdes.serdeFrom(
                new TimeWindowedSerializer<>(new ByteArraySerializer()),
                new TimeWindowedDeserializer<>(new ByteArrayDeserializer(), windowSizeMs));
    }

    private static double droppedRecords(TopologyTestDriver driver) {
        return driver.metrics().entrySet().stream()
                .filter(entry -> entry.getKey().name().equals("dropped-records-total"))
                .mapToDouble(entry -> (Double) entry.getValue().metricValue())
                .sum();
    }

    private static Properties config(Path stateDir) {
        Properties properties = new Properties();
        properties.putAll(new HashMap<>(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "window-call-count-test",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                StreamsConfig.STATE_DIR_CONFIG, stateDir.toString())));
        return properties;
    }

    /**
     * The on-window-close emit pass is throttled by a WALL-clock interval (default 1000ms in the engine; TTD
     * itself overrides it to zero via putIfAbsent - U8 verified both against the 3.9.2 sources), and TTD's mock wall
     * clock never moves on its own - so with the default, closed windows sit unemitted behind the throttle and
     * the count depends on construction-time wall clock. Interval zero makes the emit pass run on every record,
     * which is what makes scenario 8 deterministic.
     */
    private static Properties emitFinalImmediatelyConfig(Path stateDir) {
        Properties properties = config(stateDir);
        properties.put(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 0L);
        return properties;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
