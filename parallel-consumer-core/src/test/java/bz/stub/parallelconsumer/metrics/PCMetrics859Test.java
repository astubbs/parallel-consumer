package bz.stub.parallelconsumer.metrics;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.google.common.truth.Truth.assertThat;

/**
 * Regression tests for <a href="https://github.com/confluentinc/parallel-consumer/issues/859">confluentinc#859</a> — PCMetrics memory leak from duplicate meter registrations.
 * <p>
 * The bug: {@code registeredMeters} was an {@code ArrayList} that accumulated duplicate
 * {@code Meter.Id} entries every time the same meter was registered. After 3 days in
 * production with frequent offset commits, this consumed 96% of heap.
 * <p>
 * Assertions use the {@link PCMetrics#registeredMeterCount()} accessor rather than
 * reflecting into the private field.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/859">confluentinc#859</a>
 */
class PCMetrics859Test {

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

        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(1);
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

        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(1);
    }

    /**
     * Different tags should create separate entries (this is correct behaviour, not a leak).
     */
    @Test
    void differentTagsShouldCreateSeparateEntries() {
        pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE, Tag.of("encoding", "RunLength"));
        pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE, Tag.of("encoding", "BitSet"));
        pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE, Tag.of("encoding", "BitSetCompressed"));

        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(3);
    }

    /**
     * After close, the tracking set should be empty.
     */
    @Test
    void closeShouldClearAllRegisteredMeters() {
        pcMetrics.getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);
        pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE, Tag.of("encoding", "RunLength"));

        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(2);

        pcMetrics.close();

        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(0);
    }

    /**
     * The two halves of this file's history meeting: the confluentinc#859 leak fix made this method
     * prune {@code registeredMeters}, and the never-throws teardown contract made it survive a
     * hostile registry. Together they imply a <b>per-meter</b> guard - with the guard around the
     * loop instead, the first throwing meter aborts the iteration and leaves the tail of the
     * tracking set un-pruned, which is the leak the first half exists to prevent.
     *
     * <p>Neither branch could see this: the never-throws change was written against a
     * registry-only loop with nothing to prune, and the leak fix against an unguarded one. It is
     * asserted here because it is a merge-time decision, and an unproven guard is indistinguishable
     * from a missing one.
     */
    @Test
    void aThrowingRegistryMustNotStopThePrefixRemovalCleaningTheTrackingSet() {
        // Override remove(Meter.Id), not remove(Meter): Micrometer's remove(Meter) is a one-line delegate
        // to it, so the Id overload is the single choke point and intercepts close()'s sweep too. Overriding
        // the Meter overload would leave the close() path below silently unguarded by this double.
        MeterRegistry throwOnRemove = new SimpleMeterRegistry() {
            @Override
            public Meter remove(Meter.Id id) {
                throw new IllegalStateException("registry refuses removal");
            }
        };
        PCMetrics metrics = new PCMetrics(throwOnRemove, Collections.emptyList(), "throwing-instance");
        try {
            metrics.getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);
            assertThat(metrics.registeredMeterCount()).isEqualTo(1);

            // must not propagate - teardown is reporting, and cannot be allowed to break shutdown
            metrics.removeMetersByPrefixAndCommonTags(PCMetricsDef.OFFSETS_ENCODING_TIME.getName());

            // and the tracking set must still be emptied, whatever the registry did
            assertThat(metrics.registeredMeterCount()).isEqualTo(0);
        } finally {
            metrics.close();
        }
    }

    /**
     * removeMetersByPrefixAndCommonTags should also clean the tracking set,
     * not just the registry. Before the fix, it left stale Meter.Id references.
     */
    @Test
    void removeMetersByPrefixShouldCleanTrackingSet() {
        pcMetrics.getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);

        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(1);

        pcMetrics.removeMetersByPrefixAndCommonTags(PCMetricsDef.OFFSETS_ENCODING_TIME.getName());

        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(0);
    }

    /**
     * astubbs#57 shutdown race: a meter registered after {@code close()} must be left neither in the tracking
     * set NOR orphaned in the meter registry. This models the timeout/exception shutdown path where the
     * broker-poll thread is still alive (a rebalance/commit registering meters) while the control thread
     * runs {@code close()}. The register* path does {@code register()} (outside the lock) then
     * {@code track()} (under the lock); {@code track()} sees {@code isClosed} and both skips the set and
     * removes the just-registered meter from the registry. Without the fix the tracking set stays empty
     * but the meter is orphaned in the registry (never removed), so asserting only on the set was a
     * false-green - hence the explicit registry assertions below.
     */
    @Test
    void lateRegistrationAfterCloseIsNotOrphanedInRegistry() {
        pcMetrics.getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);
        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(1);

        pcMetrics.close();
        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(0);
        assertThat(registry.getMeters()).isEmpty();

        // Register again after close (rebalance/commit on the poll thread racing a timeout-shutdown):
        // track() must skip the set AND undo the registration - nothing orphaned in the registry.
        pcMetrics.getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);
        assertThat(pcMetrics.registeredMeterCount()).isEqualTo(0);
        assertThat(registry.getMeters()).isEmpty(); // fails WITHOUT the fix (orphaned meter present)
    }
}
