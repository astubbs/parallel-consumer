package io.confluent.parallelconsumer.streams.benchmark;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A distribution rather than a single figure, because on every experiment in this module the shape is the
 * finding and a mean would hide it.
 * <p>
 * Extracted from {@code HeadOfLineBlockingBenchmarkTest.Latencies}, which was the only percentile machinery in
 * the repository. Moved rather than reimplemented deliberately: the percentile index formula is reasoned about
 * by name in {@code docs/solutions/best-practices/choose-the-statistic-that-states-the-claim.md}, and a second
 * copy that rounded differently would silently make two benchmarks' p99s incomparable.
 * <p>
 * <b>Reporting is not asserting.</b> Everything here gets logged by every arm; which of these figures carries a
 * claim is decided per experiment and argued there. The learning that produced that rule came from this exact
 * class: at {@code n=24} the p99 IS the maximum, and the maximum was measuring how deep a queue the last record
 * sat in rather than the property under test.
 *
 * @author Antony Stubbs
 */
public final class LatencyDistribution {

    private final String label;

    private final List<Long> sorted;

    /**
     * @param label identifies the arm in logs - a distribution with no name is unreadable next to three others
     * @param raw   the samples, in any order; copied and sorted, so the caller's list is untouched
     */
    public LatencyDistribution(final String label, final List<Long> raw) {
        if (label == null || label.trim().isEmpty()) {
            throw new IllegalArgumentException("label must be set - it is what identifies the arm in the report");
        }
        if (raw == null || raw.isEmpty()) {
            throw new IllegalArgumentException("a distribution of nothing cannot be reported on. An empty sample "
                    + "usually means the run measured a different thing than it thought - fail here rather than "
                    + "returning zeros that read like a fast arm (label: " + label + ")");
        }
        this.label = label;
        this.sorted = new ArrayList<>(raw);
        Collections.sort(this.sorted);
    }

    public int count() {
        return sorted.size();
    }

    public long min() {
        return sorted.get(0);
    }

    public long max() {
        return sorted.get(sorted.size() - 1);
    }

    public long p50() {
        return percentile(50);
    }

    public long p90() {
        return percentile(90);
    }

    public long p99() {
        return percentile(99);
    }

    /**
     * The sample at the given percentile, by the nearest-rank convention.
     * <p>
     * At small {@code n} this degenerates into {@link #max()} - for {@code n=24}, p99 lands on index 23, the
     * last one. That is not a defect to round away; it is the reason the choose-the-statistic learning exists,
     * and {@code LatencyDistributionTest} pins it so nobody "fixes" it and quietly changes what an existing
     * benchmark's logged p99 means.
     */
    public long percentile(final int percentile) {
        if (percentile < 0 || percentile > 100) {
            throw new IllegalArgumentException("percentile must be in [0, 100], was " + percentile);
        }
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    @Override
    public String toString() {
        return String.format("%s n=%d min=%dms p50=%dms p90=%dms p99=%dms max=%dms",
                label, count(), min(), p50(), p90(), p99(), max());
    }
}
