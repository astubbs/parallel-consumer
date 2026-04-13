package io.confluent.parallelconsumer.metrics;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;

/**
 * Regression tests for #859 — PCMetrics memory leak from duplicate meter registrations.
 * <p>
 * The bug: {@code registeredMeters} was an {@code ArrayList} that accumulated duplicate
 * {@code Meter.Id} entries every time the same meter was registered. After 3 days in
 * production with frequent offset commits, this consumed 96% of heap.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/859">#859</a>
 */
class PCMetricsTest859 {

    private SimpleMeterRegistry registry;
    private PCMetrics pcMetrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        pcMetrics = new PCMetrics(registry, Collections.emptyList(), "test-instance");
    }

    @AfterEach
    void tearDown() {
        pcMetrics.close();
    }

    /**
     * Core regression test: registering the same timer multiple times must not grow
     * the tracking set beyond 1 entry. Before the fix, each call added a duplicate.
     */
    @Test
    void duplicateTimerRegistrationShouldNotGrowTrackingSet() {
        // Register the same timer 100 times (simulating 100 rebalances)
        for (int i = 0; i < 100; i++) {
            pcMetrics.getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);
        }

        Set<?> registeredMeters = getRegisteredMeters();
        assertThat(registeredMeters).hasSize(1);
    }

    /**
     * Same test for counters with identical tags.
     */
    @Test
    void duplicateCounterRegistrationShouldNotGrowTrackingSet() {
        Tag encoding = Tag.of("encoding", "RunLength");
        for (int i = 0; i < 100; i++) {
            pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE, encoding);
        }

        Set<?> registeredMeters = getRegisteredMeters();
        assertThat(registeredMeters).hasSize(1);
    }

    /**
     * Different tags should create separate entries (this is correct behaviour, not a leak).
     */
    @Test
    void differentTagsShouldCreateSeparateEntries() {
        pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE, Tag.of("encoding", "RunLength"));
        pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE, Tag.of("encoding", "BitSet"));
        pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE, Tag.of("encoding", "BitSetCompressed"));

        Set<?> registeredMeters = getRegisteredMeters();
        assertThat(registeredMeters).hasSize(3);
    }

    /**
     * After close, the tracking set should be empty.
     */
    @Test
    void closeShouldClearAllRegisteredMeters() {
        pcMetrics.getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);
        pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE, Tag.of("encoding", "RunLength"));

        Set<?> registeredMeters = getRegisteredMeters();
        assertThat(registeredMeters).hasSize(2);

        pcMetrics.close();

        assertThat(registeredMeters).isEmpty();
    }

    /**
     * removeMetersByPrefixAndCommonTags should also clean the tracking set,
     * not just the registry. Before the fix, it left stale Meter.Id references.
     */
    @Test
    void removeMetersByPrefixShouldCleanTrackingSet() {
        pcMetrics.getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);

        Set<?> registeredMeters = getRegisteredMeters();
        assertThat(registeredMeters).hasSize(1);

        pcMetrics.removeMetersByPrefixAndCommonTags(PCMetricsDef.OFFSETS_ENCODING_TIME.getName());

        assertThat(registeredMeters).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private Set<?> getRegisteredMeters() {
        try {
            Field field = PCMetrics.class.getDeclaredField("registeredMeters");
            field.setAccessible(true);
            return (Set<?>) field.get(pcMetrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access registeredMeters field", e);
        }
    }
}
