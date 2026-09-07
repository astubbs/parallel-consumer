package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * One test per U3 scenario, each asserting values <strong>derived by hand from Kafka Streams' documented
 * semantics</strong> - never a recording of what the oracle happened to produce. A recorded expectation would make
 * this suite a change detector: it would go green on whatever the oracle did, including doing it wrong.
 * <p>
 * Cases are written as YAML strings and loaded through {@link CaseLoader}, so every one of them is a case the corpus
 * could hold - a test-only builder would let a test assert over a shape the loader refuses.
 *
 * <h2>The base instant, and why the numbers in here are small</h2>
 *
 * Every case here starts <strong>two hours past the epoch</strong> ({@code 7200000}ms), the discipline the wrapper's
 * {@code WindowedAggregatorCallCountTest} records: {@code TimeWindows.windowsFor} clamps the earliest window start
 * at zero, so a record less than {@code size - advance} past the epoch falls into fewer windows than the hopping
 * multiplier says, which reads as a window bug that is really a clamp. Two hours clears every {@code size - advance}
 * in this file. Absolute epoch milliseconds are what the store rendering carries, so keeping the base small keeps
 * the hand-derived expectations legible; the committed corpus uses a real date for the same reason it uses real
 * topic names.
 *
 * <h2>What the first test establishes</h2>
 *
 * {@link #readingAStoreAfterTheDriverClosesObservesItGone} is the negative control for KTD3 and is deliberately
 * first: it pins what Kafka 3.9.2 actually does to a post-close read, which is <em>nothing at all</em> - no
 * exception, an accessor returning {@code null}, and a store handle held from before {@code close()} that reports
 * itself shut and iterates empty. That is precisely the failure mode the snapshot's placement exists to prevent: a
 * snapshot taken after {@code close()} would produce an empty outcome that every assertion over it passes.
 */
class OracleTest {

    /** Two hours past the epoch. Every {@code at-ms} in this file is an offset from here. */
    private static final long BASE_MS = 7_200_000L;

    private static final String BASE_INSTANT = "1970-01-01T02:00:00Z";

    // ------------------------------------------------------------------- the negative control, written first

    /**
     * Scenario 11 (integration): reading a store after the driver has closed observes it gone.
     * <p>
     * <b>What 3.9.2 does, measured rather than assumed:</b> {@code close()} suspends the task and cleans the state
     * directory, after which {@link TopologyTestDriver#getKeyValueStore} returns {@code null} - it does not throw -
     * and a handle obtained while the driver was open reports {@code isOpen() == false}, answers {@code get} with
     * {@code null}, iterates zero entries and reports {@code approximateNumEntries() == 0}. Every one of those is
     * silent, which is why moving {@link Oracle}'s snapshot after the try-with-resources would not redden a single
     * assertion - it would empty every outcome and let the suite agree with itself.
     */
    @Test
    void readingAStoreAfterTheDriverClosesObservesItGone(@TempDir Path stateDir) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("in", Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .groupByKey(Grouped.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .count(Materialized.<byte[], Long>as(Stores.inMemoryKeyValueStore("counts"))
                        .withKeySerde(Serdes.ByteArray())
                        .withValueSerde(Serdes.Long()));

        Properties configuration = new Properties();
        configuration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "post-close-negative-control");
        configuration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configuration.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        configuration.setProperty(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");

        TopologyTestDriver driver = new TopologyTestDriver(builder.build(), configuration);
        KeyValueStore<byte[], Long> heldOpen;
        try {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic("in",
                    Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());
            in.pipeInput(ascii("a"), ascii("1"), Instant.ofEpochMilli(BASE_MS));

            // While the driver is open the store holds exactly what was piped - the control arm for the reads below.
            heldOpen = driver.getKeyValueStore("counts");
            assertThat(heldOpen).isNotNull();
            assertThat(heldOpen.isOpen()).isTrue();
            assertThat(heldOpen.get(ascii("a"))).isEqualTo(1L);
            assertThat(countEntries(heldOpen)).isEqualTo(1);
        } finally {
            driver.close();
        }

        // The accessor no longer finds the store at all - and returns null rather than throwing, so a caller that
        // did not check would NPE somewhere else entirely, or quietly snapshot nothing.
        assertThat(driver.getKeyValueStore("counts")).isNull();

        // The handle held from before close() is worse: it answers, and every answer is empty.
        assertThat(heldOpen.isOpen()).isFalse();
        assertThat(heldOpen.get(ascii("a"))).isNull();
        assertThat(heldOpen.approximateNumEntries()).isEqualTo(0L);
        assertThat(countEntries(heldOpen)).isEqualTo(0);
    }

    // -------------------------------------------------------------------------------------------- happy paths

    /**
     * Scenario 1: a source-groupByKey-count-toStream-sink case yields a count store and a sink whose entries match
     * the counts by key.
     * <p>
     * Derived: key {@code a} appears twice and {@code b} once, so the store holds {@code a -> 2, b -> 1}. The sink
     * is fed by a non-windowed handle, so it folds to the last record per key (R3) - {@code a}'s last emission is
     * the one its second record triggered.
     */
    @Test
    void aCountByKeyCaseYieldsTheCountStoreAndTheLastSinkRecordPerKey(@TempDir Path directory) {
        FinalState state = Oracle.run(loadOne(directory, "count-by-key", ""
                + "name: count-by-key\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: in, source: {topic: in}}\n"
                + "  - {id: g, group-by-key: {of: in}}\n"
                + "  - {id: c, count: {of: g, store: counts}}\n"
                + "  - {id: s, to-stream: {of: c}}\n"
                + "  - {sink: {of: s, topic: out}}\n"
                + "inputs:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: b, value: \"2\", at-ms: 1000}\n"
                + "  - {key: a, value: \"3\", at-ms: 2000}\n"
                + "perturbation:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: a, value: \"2\", at-ms: 1000}\n"
                + "  - {key: a, value: \"3\", at-ms: 2000}\n"));

        assertThat(state.stores().keySet()).containsExactly("counts");
        assertThat(state.stores().get("counts")).containsExactly("\"a\" -> 2", "\"b\" -> 1").inOrder();
        assertThat(state.sinks().keySet()).containsExactly("out");
        assertThat(state.sinks().get("out")).containsExactly(
                "\"a\" -> 2 @" + (BASE_MS + 2000),
                "\"b\" -> 1 @" + (BASE_MS + 1000)).inOrder();
    }

    /**
     * Scenario 2: a join yields the joined value per matching key and nothing for an unmatched one.
     * <p>
     * Two sources, so the fan-out {@link Oracle} documents applies: each record reaches {@code left} and then
     * {@code right}, one record at a time. The stream side reads {@code left} and the table is built from
     * {@code right}, so the first record of a key hits an empty table and produces nothing; the second finds the
     * previous record's value there. {@code b} arrives once, so it never joins - which is the "nothing for unmatched
     * keys" half.
     * <p>
     * Derived: {@code a}'s second record ({@code y}) joins against the table's {@code X} (the upper-cased first
     * record), giving {@code y|X} at the second record's timestamp. {@code b} produces no sink record at all.
     */
    @Test
    void aJoinYieldsTheJoinedValuePerMatchingKeyAndNothingForAnUnmatchedOne(@TempDir Path directory) {
        FinalState state = Oracle.run(loadOne(directory, "join-stream-table", joinCase("join-stream-table",
                // The table is built from the SECOND source, so the stream side runs first for every record.
                "  - {id: u, map-values: {of: r, fn: upper}}\n"
                        + "  - {id: g, group-by-key: {of: u}}\n"
                        + "  - {id: t, reduce: {of: g, fn: last-wins, store: latest}}\n"
                        + "  - {id: j, join: {stream: s, table: t, fn: concat-sides}}\n")));

        assertThat(state.stores().get("latest"))
                .containsExactly("\"a\" -> \"Y\"", "\"b\" -> \"Z\"").inOrder();
        // Only a joined. b arrived once, found no table entry, and produced nothing.
        assertThat(state.sinks().get("out")).containsExactly("\"a\" -> \"y|X\" @" + (BASE_MS + 1000));
    }

    /**
     * Scenario 8: {@code concat-sides} puts the stream-side value first, and swapping which source feeds the table
     * changes the outcome - which is the whole point of the separator (KTD13): a transposed binding produces a
     * different answer rather than an ambiguous one.
     * <p>
     * Derived: with the table built from the FIRST source, every record updates the table before the join sees it,
     * so every key joins - including {@code b} - and each joins against its own upper-cased value: {@code y|Y} and
     * {@code z|Z}. Lower-case first is the stream side; upper-case second is the table side.
     */
    @Test
    void swappingWhichSideFeedsTheTableChangesTheJoinedOutcome(@TempDir Path directory) {
        FinalState straight = Oracle.run(loadOne(directory.resolve("straight"), "join-stream-table",
                joinCase("join-stream-table",
                        "  - {id: u, map-values: {of: r, fn: upper}}\n"
                                + "  - {id: g, group-by-key: {of: u}}\n"
                                + "  - {id: t, reduce: {of: g, fn: last-wins, store: latest}}\n"
                                + "  - {id: j, join: {stream: s, table: t, fn: concat-sides}}\n")));

        FinalState swapped = Oracle.run(loadOne(directory.resolve("swapped"), "join-swapped",
                joinCase("join-swapped",
                        "  - {id: u, map-values: {of: s, fn: upper}}\n"
                                + "  - {id: g, group-by-key: {of: u}}\n"
                                + "  - {id: t, reduce: {of: g, fn: last-wins, store: latest}}\n"
                                + "  - {id: j, join: {stream: r, table: t, fn: concat-sides}}\n")));

        assertThat(swapped.sinks().get("out")).containsExactly(
                "\"a\" -> \"y|Y\" @" + (BASE_MS + 1000),
                "\"b\" -> \"z|Z\" @" + (BASE_MS + 2000)).inOrder();
        assertThat(swapped).isNotEqualTo(straight);
    }

    /**
     * Scenario 3: a windowed count with two records in one window and one in the next yields two window entries in
     * the store, and the sink - fed by a windowed handle - keeps its full ordered record list (R3).
     * <p>
     * Derived: a one-hour tumbling window over records at {@code +0}, {@code +60s} and {@code +1h} puts the first
     * two in {@code [7200000, 10800000)} and the third in {@code [10800000, 14400000)}. Every update reaches the
     * sink because the record cache is off, and a windowed aggregation forwards at the window's greatest record
     * timestamp so far.
     */
    @Test
    void aWindowedCountYieldsOneStoreEntryPerWindowAndTheFullSinkList(@TempDir Path directory) {
        FinalState state = Oracle.run(loadOne(directory, "tumbling-count-two-windows", ""
                + "name: tumbling-count-two-windows\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: in, source: {topic: in}}\n"
                + "  - {id: g, group-by-key: {of: in}}\n"
                + "  - {id: w, windowed-by: {of: g, size-ms: 3600000, advance-ms: 3600000, grace-ms: 0, "
                + "retention-ms: 86400000}}\n"
                + "  - {id: c, count: {of: w, store: counts}}\n"
                + "  - {id: s, to-stream: {of: c}}\n"
                + "  - {sink: {of: s, topic: out}}\n"
                + "inputs:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: a, value: \"2\", at-ms: 60000}\n"
                + "  - {key: a, value: \"3\", at-ms: 3600000}\n"
                + "perturbation:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: a, value: \"2\", at-ms: 60000}\n"
                + "  - {key: b, value: \"3\", at-ms: 3600000}\n"));

        // Sorted as strings, so the window starting at 10800000 sorts before the one starting at 7200000.
        assertThat(state.stores().get("counts")).containsExactly(
                "\"a\"@[10800000,14400000) -> 1",
                "\"a\"@[7200000,10800000) -> 2").inOrder();
        assertThat(state.sinks().get("out")).containsExactly(
                "\"a\" -> 1 @" + BASE_MS,
                "\"a\" -> 2 @" + (BASE_MS + 60000),
                "\"a\" -> 1 @" + (BASE_MS + 3600000)).inOrder();
    }

    /**
     * Scenario 4 (AE6): a case declaring no agreement level yields an outcome with stores and sinks populated and no
     * update stream captured. The level defaults to final state, and the update-stream observable does not exist on
     * this rung at all (KTD5) - {@link FinalState#hasUpdateStream()} is the assertable form of that absence.
     */
    @Test
    void aCaseWithNoAgreementLevelComparesFinalStateOnly(@TempDir Path directory) {
        ConformanceCase noLevel = loadOne(directory, "no-agreement-level", ""
                + "name: no-agreement-level\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: in, source: {topic: in}}\n"
                + "  - {id: g, group-by-key: {of: in}}\n"
                + "  - {id: c, count: {of: g, store: counts}}\n"
                + "  - {id: s, to-stream: {of: c}}\n"
                + "  - {sink: {of: s, topic: out}}\n"
                + "inputs:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "perturbation:\n"
                + "  - {key: b, value: \"1\", at-ms: 0}\n");
        assertThat(noLevel.agreement()).isEqualTo(ConformanceCase.Agreement.FINAL_STATE);

        FinalState state = Oracle.run(noLevel);

        assertThat(state.stores().get("counts")).containsExactly("\"a\" -> 1");
        assertThat(state.sinks().get("out")).containsExactly("\"a\" -> 1 @" + BASE_MS);
        assertThat(state.hasUpdateStream()).isFalse();
    }

    // --------------------------------------------------------------------------------------------------- edges

    /**
     * Scenario 5: a case with {@code emit: on-window-close} and a trailing record past window end plus grace yields
     * a sink holding only closed windows' results, with the still-open window absent.
     * <p>
     * Derived: the first two records fill {@code [7200000, 10800000)}; the third sits exactly at that window's end
     * with zero grace, which is what advances stream time far enough for the suppression buffer to evict it. The
     * third record's own window is still open, so nothing about {@code z} reaches the sink - while the store, which
     * suppression does not touch, holds both windows.
     */
    @Test
    void aPinnedEmitCaseSinksOnlyClosedWindows(@TempDir Path directory) {
        FinalState state = Oracle.run(loadOne(directory, "pinned-emit-tumbling", ""
                + "name: pinned-emit-tumbling\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: in, source: {topic: in}}\n"
                + "  - {id: g, group-by-key: {of: in}}\n"
                + "  - {id: w, windowed-by: {of: g, size-ms: 3600000, advance-ms: 3600000, grace-ms: 0, "
                + "retention-ms: 86400000}}\n"
                + "  - {id: c, count: {of: w, store: counts}}\n"
                + "  - {id: s, to-stream: {of: c}}\n"
                + "  - {sink: {of: s, topic: out}}\n"
                + "emit: on-window-close\n"
                + "inputs:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: a, value: \"2\", at-ms: 60000}\n"
                + "  - {key: z, value: \"-\", at-ms: 3600000}\n"
                + "perturbation:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: b, value: \"2\", at-ms: 60000}\n"
                + "  - {key: z, value: \"-\", at-ms: 3600000}\n"));

        // Suppression is downstream of the store, so the store still holds the open window too.
        assertThat(state.stores().get("counts")).containsExactly(
                "\"a\"@[7200000,10800000) -> 2",
                "\"z\"@[10800000,14400000) -> 1").inOrder();
        // Only the closed window reached the sink; z's window is still open when the driver stops.
        assertThat(state.sinks().get("out")).containsExactly("\"a\" -> 2 @" + (BASE_MS + 60000));
    }

    /**
     * Scenario 6: a sink fed by a hopping window keeps the full record list - many records under one inner key, not
     * one survivor.
     * <p>
     * Derived: a one-hour window advancing every thirty minutes puts each record in {@code ceil(size / advance) = 2}
     * windows, so two records produce four emissions, all under the inner key {@code a} once {@code to-stream} has
     * dropped the window. Last-per-key would keep one of the four, which is exactly why R3 sends a windowed-fed sink
     * down the full-list path.
     */
    @Test
    void aHoppingFedSinkKeepsEveryRecordUnderOneInnerKey(@TempDir Path directory) {
        FinalState state = Oracle.run(loadOne(directory, "hopping-full-list", ""
                + "name: hopping-full-list\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: in, source: {topic: in}}\n"
                + "  - {id: g, group-by-key: {of: in}}\n"
                + "  - {id: w, windowed-by: {of: g, size-ms: 3600000, advance-ms: 1800000, grace-ms: 0, "
                + "retention-ms: 86400000}}\n"
                + "  - {id: c, count: {of: w, store: counts}}\n"
                + "  - {id: s, to-stream: {of: c}}\n"
                + "  - {sink: {of: s, topic: out}}\n"
                + "inputs:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: a, value: \"2\", at-ms: 60000}\n"
                + "perturbation:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: b, value: \"2\", at-ms: 60000}\n"));

        assertThat(state.stores().get("counts")).containsExactly(
                "\"a\"@[5400000,9000000) -> 2",
                "\"a\"@[7200000,10800000) -> 2").inOrder();
        // Four emissions, one per (record, window), and every one of them under the inner key a.
        assertThat(state.sinks().get("out")).containsExactly(
                "\"a\" -> 1 @" + BASE_MS,
                "\"a\" -> 1 @" + BASE_MS,
                "\"a\" -> 2 @" + (BASE_MS + 60000),
                "\"a\" -> 2 @" + (BASE_MS + 60000)).inOrder();
    }

    /**
     * Scenario 7: two sink records under one byte-array key fold to one entry.
     * <p>
     * This is the KTD3 trap in its smallest form. The two emissions carry two distinct {@code byte[]} instances that
     * are equal only by content, so a fold keyed on the arrays themselves would keep both and the determinism proof
     * would red for a Java reason. Derived: {@code last-wins} leaves {@code second} in the store, and the sink keeps
     * one entry - the later of the two.
     */
    @Test
    void twoSinkRecordsUnderOneByteArrayKeyFoldToOneEntry(@TempDir Path directory) {
        FinalState state = Oracle.run(loadOne(directory, "two-emissions-one-key", ""
                + "name: two-emissions-one-key\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: in, source: {topic: in}}\n"
                + "  - {id: g, group-by-key: {of: in}}\n"
                + "  - {id: r, reduce: {of: g, fn: last-wins, store: latest}}\n"
                + "  - {id: s, to-stream: {of: r}}\n"
                + "  - {sink: {of: s, topic: out}}\n"
                + "inputs:\n"
                + "  - {key: k, value: first, at-ms: 0}\n"
                + "  - {key: k, value: second, at-ms: 1000}\n"
                + "perturbation:\n"
                + "  - {key: k, value: first, at-ms: 0}\n"
                + "  - {key: k, value: third, at-ms: 1000}\n"));

        assertThat(state.stores().get("latest")).containsExactly("\"k\" -> \"second\"");
        assertThat(state.sinks().get("out")).containsExactly("\"k\" -> \"second\" @" + (BASE_MS + 1000));
    }

    /**
     * Scenario 9: a case whose inputs all land in one window yields one window entry, not one per record.
     * <p>
     * Derived: three records inside one hour under a one-hour tumbling window are one {@code (key, window)} pair
     * with a count of three. It is the control for the hopping case above - if window arithmetic were producing a
     * window per record, this would show three entries.
     */
    @Test
    void inputsInOneWindowYieldOneWindowEntry(@TempDir Path directory) {
        FinalState state = Oracle.run(loadOne(directory, "one-window", ""
                + "name: one-window\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: in, source: {topic: in}}\n"
                + "  - {id: g, group-by-key: {of: in}}\n"
                + "  - {id: w, windowed-by: {of: g, size-ms: 3600000, advance-ms: 3600000, grace-ms: 0, "
                + "retention-ms: 86400000}}\n"
                + "  - {id: c, count: {of: w, store: counts}}\n"
                + "inputs:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: a, value: \"2\", at-ms: 60000}\n"
                + "  - {key: a, value: \"3\", at-ms: 120000}\n"
                + "perturbation:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "  - {key: a, value: \"2\", at-ms: 60000}\n"
                + "  - {key: b, value: \"3\", at-ms: 120000}\n"));

        assertThat(state.stores().get("counts")).containsExactly("\"a\"@[7200000,10800000) -> 3");
        assertThat(state.sinks()).isEmpty();
    }

    // -------------------------------------------------------------------------------------------------- errors

    /**
     * Scenario 10: a topology the builder rejects surfaces as an oracle-execution failure naming the case, not as an
     * empty outcome (KTD10).
     * <p>
     * {@code windowed-by} on an ungrouped stream is well-formed <em>data</em> - the loader checks that handles
     * resolve, not what kind of handle each one is - so the refusal has to come from the oracle, and it has to name
     * the case and the operation rather than producing a case that observed nothing.
     */
    @Test
    void aTopologyTheBuilderRejectsBecomesAnOracleExecutionFailureNamingTheCase(@TempDir Path directory) {
        ConformanceCase ungrouped = loadOne(directory, "windowed-by-on-a-stream", ""
                + "name: windowed-by-on-a-stream\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: in, source: {topic: in}}\n"
                + "  - {id: w, windowed-by: {of: in, size-ms: 3600000, advance-ms: 3600000, grace-ms: 0, "
                + "retention-ms: 86400000}}\n"
                + "  - {id: c, count: {of: w, store: counts}}\n"
                + "inputs:\n"
                + "  - {key: a, value: \"1\", at-ms: 0}\n"
                + "perturbation:\n"
                + "  - {key: b, value: \"1\", at-ms: 0}\n");

        Oracle.OracleExecutionException refused =
                assertThrows(Oracle.OracleExecutionException.class, () -> Oracle.run(ungrouped));

        assertThat(refused.caseName()).isEqualTo("windowed-by-on-a-stream");
        assertThat(refused).hasMessageThat().contains("windowed-by-on-a-stream");
        assertThat(refused).hasMessageThat().contains("windowed-by[w]");
        assertThat(refused).hasMessageThat().contains("grouped stream");
    }

    /**
     * A refusal-class case (R15) is rejected rather than executed, naming the case.
     * <p>
     * It declares the fault the <em>wire</em> must raise for an invalid specification, and plain Kafka Streams never
     * refuses what the wire invented - so there is no oracle row to compute for one, and running it would be a
     * category error rather than a failing case. The corpus's own refusal case is the fixture, so this cannot pass
     * against a shape the corpus does not actually hold.
     */
    @Test
    void aRefusalClassCaseIsRejectedRatherThanExecuted() {
        ConformanceCase refusalClass = named(CaseLoader.loadClasspathDirectory("cases"),
                "aggregate-names-function-and-combine");
        assertThat(refusalClass.refusalClass()).isTrue();

        Oracle.OracleExecutionException refused =
                assertThrows(Oracle.OracleExecutionException.class, () -> Oracle.run(refusalClass));

        assertThat(refused.caseName()).isEqualTo("aggregate-names-function-and-combine");
        assertThat(refused).hasMessageThat().contains("never executed");
    }

    // ---------------------------------------------------------------------------- the aggregate vocabulary

    /**
     * {@code aggregate} with {@code count-bytes} - the one KTD13 function no other scenario reaches, and the only
     * one that reads back what it wrote.
     * <p>
     * Derived: the running total starts at the ASCII {@code 0}, gains the two bytes of {@code ab}, then the three of
     * {@code cde}, so the window's final value is {@code 5} and the sink - windowed-fed, so the full list - carries
     * the intermediate {@code 2} first.
     */
    @Test
    void aWindowedAggregateCountsValueBytesAsDecimalAscii(@TempDir Path directory) {
        FinalState state = Oracle.run(loadOne(directory, "windowed-aggregate-count-bytes", ""
                + "name: windowed-aggregate-count-bytes\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: in, source: {topic: in}}\n"
                + "  - {id: g, group-by-key: {of: in}}\n"
                + "  - {id: w, windowed-by: {of: g, size-ms: 3600000, advance-ms: 3600000, grace-ms: 0, "
                + "retention-ms: 86400000}}\n"
                + "  - {id: a, aggregate: {of: w, fn: count-bytes, store: totals}}\n"
                + "  - {id: s, to-stream: {of: a}}\n"
                + "  - {sink: {of: s, topic: out}}\n"
                + "inputs:\n"
                + "  - {key: k, value: ab, at-ms: 0}\n"
                + "  - {key: k, value: cde, at-ms: 60000}\n"
                + "perturbation:\n"
                + "  - {key: k, value: ab, at-ms: 0}\n"
                + "  - {key: k, value: cdef, at-ms: 60000}\n"));

        assertThat(state.stores().get("totals")).containsExactly("\"k\"@[7200000,10800000) -> \"5\"");
        assertThat(state.sinks().get("out")).containsExactly(
                "\"k\" -> \"2\" @" + BASE_MS,
                "\"k\" -> \"5\" @" + (BASE_MS + 60000)).inOrder();
    }

    // ------------------------------------------------------------------------------------------------ fixtures

    /**
     * The two-source join shape both join scenarios share: {@code left} is declared first, so the fan-out reaches it
     * first for every record, and the caller supplies the entries between the sources and the sink.
     */
    private static String joinCase(String name, String middle) {
        return ""
                + "name: " + name + "\n"
                + "base-instant: " + BASE_INSTANT + "\n"
                + "topology:\n"
                + "  - {id: s, source: {topic: left}}\n"
                + "  - {id: r, source: {topic: right}}\n"
                + middle
                + "  - {sink: {of: j, topic: out}}\n"
                + "inputs:\n"
                + "  - {key: a, value: x, at-ms: 0}\n"
                + "  - {key: a, value: y, at-ms: 1000}\n"
                + "  - {key: b, value: z, at-ms: 2000}\n"
                + "perturbation:\n"
                + "  - {key: a, value: x, at-ms: 0}\n"
                + "  - {key: a, value: q, at-ms: 1000}\n"
                + "  - {key: b, value: z, at-ms: 2000}\n";
    }

    /** Writes one case file into its own directory and loads it, so every fixture is a case the loader accepts. */
    private static ConformanceCase loadOne(Path directory, String fileName, String yaml) {
        try {
            Path created = Files.createDirectories(directory);
            Path written = Files.write(created.resolve(fileName + ".yaml"), yaml.getBytes(StandardCharsets.UTF_8));
            assertThat(Files.exists(written)).isTrue();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write the fixture " + fileName, e);
        }
        List<ConformanceCase> loaded = CaseLoader.load(directory);
        assertThat(loaded).hasSize(1);
        return loaded.get(0);
    }

    private static ConformanceCase named(List<ConformanceCase> corpus, String name) {
        return corpus.stream()
                .filter(candidate -> candidate.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no case named " + name + " in the corpus"));
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static int countEntries(KeyValueStore<byte[], Long> store) {
        int seen = 0;
        try (KeyValueIterator<byte[], Long> iterator = store.all()) {
            while (iterator.hasNext()) {
                Object ignoredEntry = iterator.next(); // counting, not reading - the values are asserted elsewhere
                seen++;
            }
        }
        return seen;
    }
}
