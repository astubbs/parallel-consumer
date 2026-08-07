package io.confluent.parallelconsumer.examples.support;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Locale;
import java.util.OptionalInt;

/**
 * Renders the one multi-line block an example prints when its run finishes.
 * <p>
 * Example scaffolding, not a library type.
 * <p>
 * What the block is careful about, and why:
 * <ul>
 *     <li><b>It names the partition count.</b> "peak in-flight 4" means nothing on its own; concurrency
 *     beyond the partition count is the library's whole differentiating claim, so the number it should
 *     be read against is printed beside it.</li>
 *     <li><b>The timing window comes from the {@link ConcurrencyObserver}</b> - first record in to last
 *     record out - not from application start, which at these record counts would be mostly Parallel
 *     Consumer's startup cost.</li>
 *     <li><b>The serial figure is labelled implied</b> ({@code records x simulated latency}) because it
 *     is arithmetic, not a measurement. Nothing ran serially.</li>
 *     <li><b>No speed multiplier is printed.</b> The achievable concurrency is arithmetically determined
 *     by {@code min(max concurrency, ordering ceiling)}, so a single headline figure would read as a
 *     benchmark result that these numbers cannot support.</li>
 * </ul>
 */
@Slf4j
@Value
public class RunSummary {

    private static final String DIVIDER = "--------------------------------------------------------------------------------";

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /**
     * What ran - e.g. "Payment authorisation (core)". Appears as the block's heading.
     */
    String exampleName;

    long recordCount;

    /**
     * Partitions in the input topic. The number "peak in-flight" has to be read against.
     */
    int partitionCount;

    /**
     * Distinct keys across the input. Under {@link ProcessingOrder#KEY} this, not the configured max
     * concurrency, is what caps concurrency.
     */
    int distinctKeys;

    int maxConcurrency;

    @NonNull
    ProcessingOrder ordering;

    /**
     * The latency the simulated dependency imposed per record. Simulated - the concurrency around it is
     * real, the wait itself is a {@code sleep}.
     */
    @NonNull
    Duration simulatedLatency;

    @NonNull
    ConcurrencyObserver observer;

    @Builder
    private RunSummary(String exampleName,
                       long recordCount,
                       int partitionCount,
                       int distinctKeys,
                       int maxConcurrency,
                       @NonNull ProcessingOrder ordering,
                       @NonNull Duration simulatedLatency,
                       @NonNull ConcurrencyObserver observer) {
        if (exampleName == null || exampleName.trim().isEmpty()) {
            throw new IllegalArgumentException("exampleName must be set - the block's heading names what ran");
        }
        requirePositive("recordCount", recordCount);
        requirePositive("partitionCount", partitionCount);
        requirePositive("distinctKeys", distinctKeys);
        requirePositive("maxConcurrency", maxConcurrency);
        if (simulatedLatency.isNegative()) {
            throw new IllegalArgumentException("simulatedLatency must not be negative, was: " + simulatedLatency);
        }
        this.exampleName = exampleName;
        this.recordCount = recordCount;
        this.partitionCount = partitionCount;
        this.distinctKeys = distinctKeys;
        this.maxConcurrency = maxConcurrency;
        this.ordering = ordering;
        this.simulatedLatency = simulatedLatency;
        this.observer = observer;
    }

    private static void requirePositive(String name, long value) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be at least 1, was: " + value);
        }
    }

    /**
     * The concurrency cap the chosen {@link ProcessingOrder} imposes, independent of the configured max
     * concurrency: distinct keys under {@link ProcessingOrder#KEY}, partitions under
     * {@link ProcessingOrder#PARTITION}, and empty under {@link ProcessingOrder#UNORDERED}, where
     * ordering imposes no cap at all.
     */
    public OptionalInt getOrderingCeiling() {
        switch (ordering) {
            case KEY:
                return OptionalInt.of(distinctKeys);
            case PARTITION:
                return OptionalInt.of(partitionCount);
            default:
                return OptionalInt.empty();
        }
    }

    /**
     * {@code records x simulated latency}. Implied, never measured - nothing ran serially.
     */
    public Duration getImpliedSerialTime() {
        return simulatedLatency.multipliedBy(recordCount);
    }

    /**
     * Logs {@link #render()} as one block, so the summary is not interleaved with per-record logging.
     */
    public void log() {
        log.info("{}", render());
    }

    public String render() {
        StringBuilder out = new StringBuilder(LINE_SEPARATOR);
        out.append(DIVIDER).append(LINE_SEPARATOR);
        out.append(" ").append(exampleName).append(" - Parallel Consumer run summary").append(LINE_SEPARATOR);
        out.append(DIVIDER).append(LINE_SEPARATOR);

        field(out, "records processed", Long.toString(recordCount));
        field(out, "input partitions", Integer.toString(partitionCount));
        field(out, "ordering", ordering.name());
        field(out, "ordering ceiling", renderOrderingCeiling());
        field(out, "configured max concurrency", Integer.toString(maxConcurrency));
        field(out, "peak in-flight", Integer.toString(observer.getPeakInFlight()));
        field(out, "peak in-flight per partition", format("%.2f", observer.getPeakInFlight() / (double) partitionCount));
        field(out, "distinct worker threads", Integer.toString(observer.getDistinctThreadCount()));
        // NOT the same as "records processed": this counts user-function invocations, so a record that
        // failed and was retried is counted once per attempt. Reporting both under similar names invited
        // the reader to treat the difference as records lost.
        field(out, "user function invocations", Long.toString(observer.getCompleted()));
        field(out, "observed processing window", renderWindow());
        field(out, "throughput", renderThroughput());
        field(out, "implied serial time", renderImpliedSerialTime());

        out.append(DIVIDER).append(LINE_SEPARATOR);
        out.append(" A demonstration, not a benchmark. The per-record latency is simulated; the concurrency").append(LINE_SEPARATOR);
        out.append(" around it is real. No multiplier is printed - the achievable concurrency here is").append(LINE_SEPARATOR);
        out.append(" arithmetically determined by the ordering ceiling and the configured max concurrency,").append(LINE_SEPARATOR);
        out.append(" so a single headline figure would read as a measurement these numbers cannot support.").append(LINE_SEPARATOR);
        out.append(DIVIDER);
        return out.toString();
    }

    private String renderOrderingCeiling() {
        OptionalInt ceiling = getOrderingCeiling();
        if (!ceiling.isPresent()) {
            return "none imposed by ordering (UNORDERED - capped only by the configured max concurrency)";
        }
        String source = ordering == ProcessingOrder.KEY ? "distinct keys" : "input partitions";
        return ceiling.getAsInt() + " concurrent records (" + source + ")";
    }

    private String renderWindow() {
        if (!observer.hasObservations()) {
            return "not observed - no records were processed";
        }
        return format("%.3f s", seconds(observer.getObservedWindow()))
                + " (first record in to last record out - excludes Parallel Consumer startup)";
    }

    private String renderThroughput() {
        double windowSeconds = seconds(observer.getObservedWindow());
        if (windowSeconds <= 0d) {
            return "not observed - the processing window was too short to measure";
        }
        return format("%.2f records/sec", observer.getCompleted() / windowSeconds);
    }

    private String renderImpliedSerialTime() {
        return format("%.3f s", seconds(getImpliedSerialTime()))
                + " (" + recordCount + " records x " + simulatedLatency.toMillis()
                + " ms simulated latency - implied, not measured)";
    }

    private static double seconds(Duration duration) {
        return duration.toNanos() / (double) NANOS_PER_SECOND;
    }

    private static void field(StringBuilder out, String label, String value) {
        out.append(format("  %-30s : %s", label, value)).append(LINE_SEPARATOR);
    }

    private static String format(String template, Object... arguments) {
        // ROOT so the block reads the same whatever locale the example happens to run under
        return String.format(Locale.ROOT, template, arguments);
    }
}
