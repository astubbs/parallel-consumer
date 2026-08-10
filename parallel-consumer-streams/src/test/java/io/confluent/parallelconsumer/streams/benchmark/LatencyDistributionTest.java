package io.confluent.parallelconsumer.streams.benchmark;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the percentile convention, because two benchmarks that round differently produce figures nobody can
 * compare - and because the small-{@code n} degeneracy in {@link LatencyDistribution#percentile(int)} is
 * load-bearing evidence in a learnings document rather than an accident to be tidied away.
 *
 * @author Antony Stubbs
 */
class LatencyDistributionTest {

    @Test
    void aSingleSampleIsEveryStatistic() {
        LatencyDistribution one = new LatencyDistribution("one", Collections.singletonList(42L));

        assertThat(one.count()).isEqualTo(1);
        assertThat(one.min()).isEqualTo(42L);
        assertThat(one.p50()).isEqualTo(42L);
        assertThat(one.p99()).isEqualTo(42L);
        assertThat(one.max()).isEqualTo(42L);
    }

    @Test
    void nearestRankOverOneToOneHundred() {
        LatencyDistribution hundred = new LatencyDistribution("hundred", range(1, 100));

        assertThat(hundred.min()).isEqualTo(1L);
        assertThat(hundred.p50()).isEqualTo(50L);
        assertThat(hundred.p90()).isEqualTo(90L);
        assertThat(hundred.p99()).isEqualTo(99L);
        assertThat(hundred.max()).isEqualTo(100L);
    }

    /**
     * The degeneracy that produced this module's statistic-choice learning, pinned so a later "improvement" to
     * the index formula cannot silently change what an existing benchmark's logged p99 means.
     */
    @Test
    void atTwentyFourSamplesP99IsTheMaximum() {
        LatencyDistribution small = new LatencyDistribution("n24", range(1, 24));

        assertThat(small.p99())
                .as("at n=24 the nearest-rank p99 lands on the last index - it IS the maximum, which is why "
                        + "HeadOfLineBlockingBenchmarkTest asserts on the minimum and merely reports the tail")
                .isEqualTo(small.max())
                .isEqualTo(24L);
    }

    @Test
    void inputOrderDoesNotChangeAnyStatistic() {
        List<Long> ascending = range(1, 100);
        List<Long> shuffled = new ArrayList<>(ascending);
        Collections.shuffle(shuffled, new java.util.Random(20260811L));

        assertThat(new LatencyDistribution("shuffled", shuffled).toString())
                .isEqualTo(new LatencyDistribution("shuffled", ascending).toString());
    }

    @Test
    void anEmptySampleFailsLoudlyRatherThanReadingAsAFastArm() {
        assertThatThrownBy(() -> new LatencyDistribution("empty", Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void anUnnamedDistributionIsRejected() {
        assertThatThrownBy(() -> new LatencyDistribution("  ", Collections.singletonList(1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("label");
    }

    @Test
    void anOutOfRangePercentileIsRejected() {
        LatencyDistribution ten = new LatencyDistribution("ten", range(1, 10));

        assertThatThrownBy(() -> ten.percentile(101)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ten.percentile(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theReportNamesTheArmAndTheSampleSize() {
        assertThat(new LatencyDistribution("stock", range(1, 10)).toString())
                .startsWith("stock n=10 ")
                .contains("min=1ms")
                .contains("max=10ms");
    }

    private static List<Long> range(final int fromInclusive, final int toInclusive) {
        List<Long> values = new ArrayList<>();
        for (long value = fromInclusive; value <= toInclusive; value++) {
            values.add(value);
        }
        return values;
    }
}
