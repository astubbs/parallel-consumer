package bz.stub.parallelconsumer.examples.support;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Asserts on the rendered text, because the wording is the contract: the block has to be readable as a
 * demonstration and unreadable as a benchmark.
 */
@Slf4j
class RunSummaryTest {

    private static final int RECORDS = 100;

    private static final int PARTITIONS = 4;

    private static final int DISTINCT_KEYS = 10;

    private static final int MAX_CONCURRENCY = 100;

    private static final Duration SIMULATED_LATENCY = Duration.ofMillis(200);

    /**
     * A numeral immediately followed by "x" - i.e. a speed multiple like "9.3x".
     */
    private static final Pattern MULTIPLIER = Pattern.compile("\\d+(?:\\.\\d+)?\\s*[xX](?![a-zA-Z])");

    @Test
    void orderingCeilingUnderKeyOrderingIsTheDistinctKeyCountNotMaxConcurrency() {
        String rendered = summary(ProcessingOrder.KEY).render();

        String ceilingLine = lineContaining(rendered, "ordering ceiling");
        assertThat(ceilingLine)
                .as("under KEY ordering the cap is the distinct key count, whatever max concurrency is set to")
                .contains("10 concurrent records", "distinct keys");
        assertThat(ceilingLine).doesNotContain(String.valueOf(MAX_CONCURRENCY));
    }

    @Test
    void orderingCeilingUnderPartitionOrderingIsThePartitionCount() {
        String ceilingLine = lineContaining(summary(ProcessingOrder.PARTITION).render(), "ordering ceiling");

        assertThat(ceilingLine).contains("4 concurrent records", "input partitions");
    }

    @Test
    void unorderedProcessingHasNoOrderingCeiling() {
        RunSummary summary = summary(ProcessingOrder.UNORDERED);

        assertThat(summary.getOrderingCeiling()).isEmpty();
        assertThat(lineContaining(summary.render(), "ordering ceiling"))
                .contains("none imposed by ordering", "max concurrency");
    }

    @Test
    void theSerialFigureIsLabelledImpliedFromTheSimulatedLatencyNotMeasured() {
        String serialLine = lineContaining(summary(ProcessingOrder.KEY).render(), "implied serial time");

        assertThat(serialLine).contains("100 records x 200 ms simulated latency");
        assertThat(serialLine)
                .as("nothing ran serially - the figure is arithmetic")
                .contains("implied, not measured");
        assertThat(serialLine).contains("20.000 s");
    }

    @Test
    void noSpeedMultipleIsPrinted() {
        String rendered = summary(ProcessingOrder.KEY).render();

        assertThat(MULTIPLIER.matcher(rendered).find())
                .as("a bare multiple invites a benchmark reading these numbers cannot support, in:%n%s", rendered)
                .isFalse();
        assertThat(rendered.toLowerCase(Locale.ROOT))
                .doesNotContain("faster")
                .doesNotContain("speedup")
                .doesNotContain("speed-up")
                .doesNotContain("times quicker");
    }

    @Test
    void theBlockCarriesEveryFigureTheReaderNeedsToInterpretIt() {
        RunSummary summary = summary(ProcessingOrder.KEY);
        summary.log(); // so anyone running this suite sees the block an example actually prints
        String rendered = summary.render();

        assertThat(lineContaining(rendered, "records processed")).contains("100");
        assertThat(lineContaining(rendered, "input partitions"))
                .as("peak in-flight is meaningless without the partition count beside it")
                .contains("4");
        assertThat(lineContaining(rendered, "peak in-flight")).contains("10");
        assertThat(lineContaining(rendered, "peak in-flight per partition")).contains("2.50");
        assertThat(lineContaining(rendered, "distinct worker threads")).contains("1");
        assertThat(lineContaining(rendered, "throughput")).contains("records/sec");
        assertThat(rendered).contains("Payment authorisation (core)");
    }

    @Test
    void theBlockSaysItIsADemonstrationAndWhichHalfIsSimulated() {
        String rendered = summary(ProcessingOrder.KEY).render();

        assertThat(rendered).contains("A demonstration, not a benchmark.");
        assertThat(rendered).contains("The per-record latency is simulated; the concurrency");
        assertThat(rendered).contains("around it is real");
        assertThat(rendered).contains("No multiplier is printed");
    }

    @Test
    void theWindowIsTheObserversFirstEnterToLastExitNotApplicationStart() {
        ConcurrencyObserver observer = observer(DISTINCT_KEYS, RECORDS);
        String rendered = summaryWith(ProcessingOrder.KEY, observer).render();

        // exact arithmetic rather than a wall-clock threshold: whatever the machine's speed, the figure
        // printed must be the observed window and nothing wider
        double expectedSeconds = (observer.getLastExitNanos() - observer.getFirstEnterNanos()) / 1_000_000_000d;
        assertThat(lineContaining(rendered, "observed processing window"))
                .contains(String.format(Locale.ROOT, "%.3f s", expectedSeconds))
                .contains("excludes Parallel Consumer startup");
    }

    /**
     * The examples deliberately inject retries, so invocations exceed records. The field is labelled
     * {@code records/sec}, and deriving it from the invocation count would have overstated it by exactly
     * the retry rate - reporting the retries as though they were extra records delivered.
     */
    @Test
    void throughputCountsRecordsNotUserFunctionInvocations() {
        int invocations = RECORDS + 50; // 50 retries
        ConcurrencyObserver observer = observer(DISTINCT_KEYS, invocations);
        String rendered = summaryWith(ProcessingOrder.KEY, observer).render();

        double windowSeconds = (observer.getLastExitNanos() - observer.getFirstEnterNanos()) / 1_000_000_000d;
        assertThat(lineContaining(rendered, "throughput"))
                .contains(String.format(Locale.ROOT, "%.2f records/sec", RECORDS / windowSeconds))
                .doesNotContain(String.format(Locale.ROOT, "%.2f records/sec", invocations / windowSeconds));
        assertThat(lineContaining(rendered, "user function invocations"))
                .as("the retries are still reported - under the name that describes them")
                .contains(String.valueOf(invocations));
    }

    @Test
    void aRunThatProcessedNothingSaysSoRatherThanDividingByZero() {
        String rendered = summaryWith(ProcessingOrder.KEY, new ConcurrencyObserver()).render();

        assertThat(lineContaining(rendered, "observed processing window")).contains("not observed");
        assertThat(lineContaining(rendered, "throughput")).contains("not observed");
    }

    @Test
    void nonsensicalInputsAreRejectedRatherThanRenderedAsNonsense() {
        assertThatThrownBy(() -> baseBuilder().partitionCount(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partitionCount");
        assertThatThrownBy(() -> baseBuilder().exampleName(" ").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exampleName");
        assertThatThrownBy(() -> baseBuilder().simulatedLatency(Duration.ofMillis(-1)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("simulatedLatency");
        assertThatThrownBy(() -> baseBuilder().ordering(null).build())
                .isInstanceOf(NullPointerException.class);
    }

    private RunSummary summary(ProcessingOrder ordering) {
        return summaryWith(ordering, observer(DISTINCT_KEYS, RECORDS));
    }

    private RunSummary summaryWith(ProcessingOrder ordering, ConcurrencyObserver observer) {
        return baseBuilder().ordering(ordering).observer(observer).build();
    }

    private RunSummary.RunSummaryBuilder baseBuilder() {
        return RunSummary.builder()
                .exampleName("Payment authorisation (core)")
                .recordCount(RECORDS)
                .partitionCount(PARTITIONS)
                .distinctKeys(DISTINCT_KEYS)
                .maxConcurrency(MAX_CONCURRENCY)
                .ordering(ProcessingOrder.KEY)
                .simulatedLatency(SIMULATED_LATENCY)
                .observer(new ConcurrencyObserver());
    }

    /**
     * An observer that has seen {@code completed} units of work, {@code peak} of which were open at once.
     * Nested scopes on one thread, so the figures are exact and no timing is involved - the concurrency
     * these tests care about is the observer's arithmetic, which
     * {@link ConcurrencyObserverTest} already proves holds under real threads.
     */
    private ConcurrencyObserver observer(int peak, int completed) {
        ConcurrencyObserver observer = new ConcurrencyObserver();
        Deque<ConcurrencyObserver.Scope> open = new ArrayDeque<>(peak);
        for (int i = 0; i < peak; i++) {
            open.push(observer.enter());
        }
        while (!open.isEmpty()) {
            open.pop().close();
        }
        for (int i = peak; i < completed; i++) {
            observer.enter().close();
        }
        return observer;
    }

    /**
     * The rendered "{@code  <label>   : <value>}" line for exactly {@code label} - matched on the whole
     * label so that, say, "peak in-flight" cannot silently return the "peak in-flight per partition"
     * line instead.
     */
    private String lineContaining(String rendered, String label) {
        Pattern field = Pattern.compile("^\\s*" + Pattern.quote(label) + "\\s*:.*$");
        for (String line : rendered.split("\\R")) {
            if (field.matcher(line).matches()) {
                return line;
            }
        }
        throw new AssertionError("no '" + label + "' field in:\n" + rendered);
    }
}
