package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shape of the comparison table, which is the one part of this demo that binds ten other
 * languages.
 *
 * <h2>Why this test exists, written after the cost was paid</h2>
 *
 * <b>Column identity and order are contract; column width is not.</b> Arm labels differ in length
 * between languages, so widths cannot be shared - but a reader who has run one demo has run them
 * all, and that only holds if the columns arrive in the same sequence everywhere.
 * <p>
 * The seed had no test for this. Eleven implementations then read one contract document and
 * returned <b>three different column orders</b> - six beside {@code arm}, four appended after
 * {@code vs AK core}, one in the middle - and Java's own was one of the wrong ones. Three of the
 * eleven guarded their order with a test; those three were not the problem. The document had simply
 * never stated the order, and nothing anywhere could have failed to say so.
 * <p>
 * So this is not a test about Java. It is the seed carrying the assertion the contract needs every
 * language to carry, in the language the others copy from.
 *
 * <h2>Why {@code renderTable} exists at all</h2>
 *
 * {@link ReferenceDemo#report} logs. A logged table can only be asserted by capturing an appender,
 * which tests the logging framework as much as the table. The C++ and Rust demos split render from
 * report for exactly this reason, and Java now matches them.
 */
class DemoTableTest {

    private static final List<ArmResult> ONE_ARM =
            List.of(new ArmResult("AK core", "KafkaConsumer", Duration.ofMillis(1_400), 2_000, 1_000));

    /**
     * The contract order, asserted by position rather than by matching the whole header line: the
     * padding between columns is deliberately not contract, so an assertion on the literal string
     * would go red for a width change that breaks nothing.
     */
    @Test
    void theTableCarriesTheContractColumnsInOrder() {
        String header = headerOf(ReferenceDemo.renderTable("Small replay", ONE_ARM, null, false));

        assertThat(header)
                .withFailMessage("every column is contract, and a missing one is not a narrower "
                        + "table but a different one: %s", header)
                .contains("arm", "records", "keys", "elapsed", "msg/s", "vs AK core");
        assertThat(List.of(
                header.indexOf("arm"),
                header.indexOf("records"),
                header.indexOf("keys"),
                header.indexOf("elapsed"),
                header.indexOf("msg/s"),
                header.indexOf("vs AK core")))
                .withFailMessage("arm | records | keys | elapsed | msg/s | vs AK core is the "
                        + "contract's order, and ten other languages mirror this one: %s", header)
                .isSorted();
    }

    /**
     * Records and keys are the only two figures in the row that are deterministic - every language
     * replaying the same backlog reports the same pair - which is what lets a cross-language check
     * compare them when elapsed and msg/s never could.
     */
    @Test
    void theRowReportsWhatTheArmDidAndNotOnlyHowFast() {
        String row = rowOf(ReferenceDemo.renderTable("Small replay", ONE_ARM, null, false));

        assertThat(row)
                .withFailMessage("the row names the client that ran, so a reader can judge it: %s", row)
                .contains("AK core (KafkaConsumer)");
        assertThat(row)
                .withFailMessage("records and keys must be legible as separate figures - a keys "
                        + "count equal to the records count would mean the backlog never spread: %s", row)
                .contains("2,000")
                .contains("1,000");
    }

    /**
     * The big replay's ratio compares across replays, so the column is marked and footnoted rather
     * than left to imply a like-for-like comparison it is not.
     */
    @Test
    void theAcrossReplayRatioIsMarkedAndExplained() {
        String table = ReferenceDemo.renderTable("Big replay", ONE_ARM, null, true);

        assertThat(headerOf(table)).contains("vs AK core*");
        assertThat(table)
                .withFailMessage("a starred column with no footnote is worse than no star: %s", table)
                .contains("* against the SMALL replay's AK core arm");
    }

    /** No latency, in either table - the backlog is pre-produced, so per-record timings flatter. */
    @Test
    void noLatencyIsReported() {
        String table = ReferenceDemo.renderTable("Small replay", ONE_ARM, null, false);

        assertThat(table.toLowerCase(java.util.Locale.ROOT))
                .withFailMessage("the workload is closed-loop; a latency figure here would be "
                        + "flattered by however far an arm fell behind: %s", table)
                .doesNotContain("latency")
                .doesNotContain("p99");
    }

    private static String headerOf(String table) {
        return table.lines()
                .filter(line -> line.contains("msg/s"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no header row in:\n" + table));
    }

    private static String rowOf(String table) {
        return table.lines()
                .filter(line -> line.contains("AK core ("))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no arm row in:\n" + table));
    }
}
