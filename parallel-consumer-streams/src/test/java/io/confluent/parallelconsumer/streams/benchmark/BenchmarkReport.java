package io.confluent.parallelconsumer.streams.benchmark;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.List;

/**
 * Renders a run's findings as a block that stands on its own.
 * <p>
 * <b>This exists because a benchmark whose result is buried is a benchmark nobody trusts.</b> A Kafka Streams
 * arm emits several hundred lines of broker, assignment and rebalance logging, and three interesting numbers
 * scattered through that is a bad report even when the numbers are good - the reader cannot tell which lines
 * carry the finding, or whether their own run did the same thing as the one in the write-up.
 * <p>
 * So every benchmark ends by printing one framed block containing the configuration it ran, both arms, the
 * ratio, and - importantly - <b>the verdict in words</b>. The words matter more than the number: "PC is slower
 * here, as predicted" is the sentence that makes the flattering cells credible, and a reader scanning a log
 * will find a sentence long before they will interpret a ratio.
 *
 * @author Antony Stubbs
 */
public final class BenchmarkReport {

    private static final String RULE = "==============================================================================";

    private final String title;
    private final List<String> configuration = new ArrayList<>();
    private final List<String[]> rows = new ArrayList<>();
    private final List<String> findings = new ArrayList<>();

    public BenchmarkReport(final String title) {
        this.title = title;
    }

    /**
     * A term the run was configured with. Every term that could change the answer belongs here, because the
     * first question asked of any benchmark figure is "run with what?".
     */
    public BenchmarkReport configured(final String term, final Object value) {
        configuration.add(String.format("  %-22s %s", term, value));
        return this;
    }

    /**
     * One measured line: what was measured, the stock arm, the PC arm, and how they compare.
     */
    public BenchmarkReport measurement(final String what, final String stock, final String pc, final String ratio) {
        rows.add(new String[]{what, stock, pc, ratio});
        return this;
    }

    /**
     * A sentence a reader can quote. Prefix a refuted prediction with {@code REFUTED} and a confirmed one with
     * {@code HELD}, so the two are equally visible - the reporting order this repository requires puts
     * refutations first, and a reader skimming needs to see them without decoding a table.
     */
    public BenchmarkReport finding(final String sentence) {
        findings.add(sentence);
        return this;
    }

    /**
     * The whole block, ready to hand to a single log call. One call rather than a line each, because
     * interleaved logging from a worker pool would otherwise cut the report in half.
     */
    public String render() {
        StringBuilder out = new StringBuilder();
        out.append(System.lineSeparator()).append(RULE).append(System.lineSeparator());
        out.append("  ").append(title).append(System.lineSeparator());
        out.append(RULE).append(System.lineSeparator());

        if (!configuration.isEmpty()) {
            out.append("CONFIGURATION").append(System.lineSeparator());
            for (String line : configuration) {
                out.append(line).append(System.lineSeparator());
            }
            out.append(System.lineSeparator());
        }

        if (!rows.isEmpty()) {
            out.append(String.format("  %-30s %14s %14s %12s", "MEASUREMENT", "STOCK", "PC", "PC vs STOCK"))
                    .append(System.lineSeparator());
            out.append("  ").append(RULE, 0, 72).append(System.lineSeparator());
            for (String[] row : rows) {
                out.append(String.format("  %-30s %14s %14s %12s", row[0], row[1], row[2], row[3]))
                        .append(System.lineSeparator());
            }
            out.append(System.lineSeparator());
        }

        if (!findings.isEmpty()) {
            out.append("WHAT THIS SHOWS").append(System.lineSeparator());
            for (String finding : findings) {
                out.append("  - ").append(finding).append(System.lineSeparator());
            }
        }
        out.append(RULE);
        return out.toString();
    }

    /**
     * Formats a ratio the way every row of every report should, so two reports can be read side by side.
     * Below 1.00x means PC was slower, and that is printed plainly rather than inverted into a flattering
     * "stock was 1.4x faster".
     */
    public static String ratio(final double pcValue, final double stockValue) {
        if (stockValue <= 0d) {
            return "n/a";
        }
        return String.format("%.2fx", pcValue / stockValue);
    }
}
