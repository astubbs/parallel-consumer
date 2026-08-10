package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A distribution rather than a single figure, because the tail is the finding.
 * <p>
 * Head-of-line blocking means <em>some</em> records wait for an unrelated record. A mean, or a wall-clock
 * for the run, averages that away and reports a small difference where the real one is enormous.
 * <p>
 * <b>Kept deliberately identical to the private {@code Latencies} inside
 * {@code HeadOfLineBlockingBenchmarkTest}</b> in {@code parallel-consumer-streams}, including the
 * percentile formula, so the demo and the regression test report the same statistic of the same data. The
 * two are separate because a {@code src/main} module cannot import another module's test classes. Change
 * one and check the other, because nothing else will.
 *
 * @author Antony Stubbs
 */
final class Latencies {

    private final List<Long> sorted;

    Latencies(final List<Long> raw) {
        this.sorted = new ArrayList<>(raw);
        Collections.sort(this.sorted);
    }

    int count() {
        return sorted.size();
    }

    /**
     * <b>The statistic that states the claim.</b> "A fast record does not have to wait for the slow one" is
     * falsified if even the luckiest fast record waited, and demonstrated if a single one did not.
     */
    long min() {
        return sorted.get(0);
    }

    long p50() {
        return percentile(50);
    }

    /**
     * Reported, but deliberately not the headline. At this sample size the p99 IS the single worst sample,
     * and the worst sample is the last record queued through the worker pool - a measure of pool depth
     * rather than of blocking.
     */
    long p99() {
        return percentile(99);
    }

    private long percentile(final int percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
}
