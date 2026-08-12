package bz.stub.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.State;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Proves the snapshot a {@link StateSampler} builds actually matches the meters in the registry - including the two
 * things a dashboard gets wrong by default: treating an absent meter as zero, and asserting a confident reading from
 * a registry that has not been populated yet.
 */
class MeterRegistrySamplerTest {

    private static StateSampler samplerOver(MeterRegistry registry) {
        return new StateSampler(new MeterSource(registry), new DirectStateSource(null));
    }

    /**
     * The names this module reads are core's, not this module's. If core renames one, the dashboard silently reports
     * every value from that family as absent - which looks exactly like an idle consumer. Pin them.
     */
    @Test
    void meterNamesMatchCoreDefinitions() {
        assertThat(PCMetricsDef.PARTITION_HIGHEST_SEEN_OFFSET.getName()).isEqualTo(PcMeterFixture.PARTITION_HIGHEST_SEEN);
        assertThat(PCMetricsDef.PARTITION_HIGHEST_COMPLETED_OFFSET.getName()).isEqualTo(PcMeterFixture.PARTITION_HIGHEST_COMPLETED);
        assertThat(PCMetricsDef.PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET.getName()).isEqualTo(PcMeterFixture.PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED);
        assertThat(PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET.getName()).isEqualTo(PcMeterFixture.PARTITION_LATEST_COMMITTED);
        assertThat(PCMetricsDef.PARTITION_INCOMPLETE_OFFSETS.getName()).isEqualTo(PcMeterFixture.PARTITION_INCOMPLETE_OFFSETS);
        assertThat(PCMetricsDef.PARTITION_ASSIGNMENT_EPOCH.getName()).isEqualTo(PcMeterFixture.PARTITION_ASSIGNMENT_EPOCH);
        assertThat(PCMetricsDef.PROCESSED_RECORDS.getName()).isEqualTo(PcMeterFixture.PROCESSED_RECORDS);
        assertThat(PCMetricsDef.FAILED_RECORDS.getName()).isEqualTo(PcMeterFixture.FAILED_RECORDS);
        assertThat(PCMetricsDef.SLOW_RECORDS.getName()).isEqualTo(PcMeterFixture.SLOW_RECORDS);
        assertThat(PCMetricsDef.INFLIGHT_RECORDS.getName()).isEqualTo(PcMeterFixture.INFLIGHT_RECORDS);
        assertThat(PCMetricsDef.WAITING_RECORDS.getName()).isEqualTo(PcMeterFixture.WAITING_RECORDS);
        assertThat(PCMetricsDef.NUMBER_OF_SHARDS.getName()).isEqualTo(PcMeterFixture.SHARDS);
        assertThat(PCMetricsDef.SHARDS_SIZE.getName()).isEqualTo(PcMeterFixture.SHARDS_SIZE);
        assertThat(PCMetricsDef.INCOMPLETE_OFFSETS_TOTAL.getName()).isEqualTo(PcMeterFixture.INCOMPLETE_OFFSETS_TOTAL);
        assertThat(PCMetricsDef.DYNAMIC_EXTRA_LOAD_FACTOR.getName()).isEqualTo(PcMeterFixture.DYNAMIC_LOAD_FACTOR);
        assertThat(PCMetricsDef.NUM_PAUSED_PARTITIONS.getName()).isEqualTo(PcMeterFixture.PARTITIONS_PAUSED);
        assertThat(PCMetricsDef.NUMBER_OF_PARTITIONS.getName()).isEqualTo(PcMeterFixture.PARTITIONS_NUMBER);
        assertThat(PCMetricsDef.PC_STATUS.getName()).isEqualTo(PcMeterFixture.STATUS);
        assertThat(PCMetricsDef.PC_POLLER_STATUS.getName()).isEqualTo(PcMeterFixture.POLLER_STATUS);
        assertThat(PCMetricsDef.OFFSETS_ENCODING_TIME.getName()).isEqualTo(PcMeterFixture.ENCODING_TIME);
        assertThat(PCMetricsDef.OFFSETS_ENCODING_USAGE.getName()).isEqualTo(PcMeterFixture.ENCODING_USAGE);
        assertThat(PCMetricsDef.METADATA_SPACE_USED.getName()).isEqualTo(PcMeterFixture.METADATA_SPACE_USED);
        assertThat(PCMetricsDef.PAYLOAD_RATIO_USED.getName()).isEqualTo(PcMeterFixture.PAYLOAD_RATIO_USED);
    }

    @Test
    void populatedRegistryProducesOneRowPerTopicPartition() {
        PcSnapshot snapshot = samplerOver(PcMeterFixture.fullyPopulated().getRegistry()).sample();

        assertThat(snapshot.isRegistryPopulated()).isTrue();
        assertThat(snapshot.isEmpty()).isFalse();
        assertThat(snapshot.getSampleSequence()).isEqualTo(1);
        assertThat(snapshot.getCaptureEpochMillis()).isGreaterThan(0);

        // ordered by topic then partition, so the page's table does not reshuffle between ticks
        assertThat(snapshot.getPartitions()).extracting(PartitionSnapshot::getKey)
                .containsExactly("orders-0", "orders-1", "payments-7");

        PartitionSnapshot orders0 = snapshot.getPartitions().get(0);
        assertThat(orders0.getTopic()).isEqualTo("orders");
        assertThat(orders0.getPartition()).isEqualTo(0);
        // the gauge carries the next offset to consume, so a commit that has taken offset 100 reads 101
        assertThat(orders0.getLastCommittedOffset()).isEqualTo(101L);
        assertThat(orders0.getHighestSequentialSucceededOffset()).isEqualTo(100L);
        assertThat(orders0.getHighestCompletedOffset()).isEqualTo(140L);
        assertThat(orders0.getHighestSeenOffset()).isEqualTo(150L);
        assertThat(orders0.getIncompleteOffsets()).isEqualTo(12L);
        assertThat(orders0.getAssignmentEpoch()).isEqualTo(3L);
        assertThat(orders0.getProcessedRecords()).isEqualTo(100d);
        assertThat(orders0.getFailedRecords()).isEqualTo(2d);
        assertThat(orders0.getSlowRecords()).isEqualTo(1d);
    }

    /**
     * The invariant the offset ribbon is drawn from. If it can be violated the graphic is meaningless, so it is
     * asserted here rather than assumed downstream.
     * <p>
     * The committed marker is compared as {@code committed - 1}, because the gauge is exclusive and the other three
     * are not. Comparing it raw is what made this invariant read as violated on every caught-up partition.
     */
    @Test
    void offsetMarkersAreNonDecreasing() {
        PcSnapshot snapshot = samplerOver(PcMeterFixture.fullyPopulated().getRegistry()).sample();

        assertThat(snapshot.getPartitions()).isNotEmpty();
        for (PartitionSnapshot row : snapshot.getPartitions()) {
            assertThat(row.isOffsetOrderingConsistent())
                    .as("offset markers must be non-decreasing for %s", row.getKey())
                    .isTrue();
            assertThat(row.getLastCommittedOffset() - 1)
                    .as("highest committed offset <= sequential-succeeded for %s", row.getKey())
                    .isLessThanOrEqualTo(row.getHighestSequentialSucceededOffset());
            assertThat(row.getHighestSequentialSucceededOffset())
                    .as("sequential-succeeded <= completed for %s", row.getKey())
                    .isLessThanOrEqualTo(row.getHighestCompletedOffset());
            assertThat(row.getHighestCompletedOffset())
                    .as("completed <= seen for %s", row.getKey())
                    .isLessThanOrEqualTo(row.getHighestSeenOffset());
        }
    }

    /**
     * The other half of the detector: a partition whose commit has caught up is the shape a real instance spends most
     * of its life in, and the committed gauge then sits one <em>above</em> every other marker. Compared raw it looks
     * like an inversion, which would un-hide the "markers sampled out of order" badge permanently and make the page's
     * whole caught-up path unreachable.
     */
    @Test
    void aCommitThatHasCaughtUpIsConsistentRatherThanInverted() {
        PcMeterFixture fixture = new PcMeterFixture()
                // everything through 900 done and committed, so the gauge reads 901
                .partitionOffsets("orders", 0, 901, 900, 900, 900, 0, 3);

        PcSnapshot snapshot = samplerOver(fixture.getRegistry()).sample();

        PartitionSnapshot row = snapshot.getPartitions().get(0);
        assertThat(row.getLastCommittedOffset())
                .as("the wire keeps the raw gauge value - it is the offset a restart resumes from")
                .isEqualTo(901L);
        assertThat(row.isOffsetOrderingConsistent()).isTrue();
    }

    /**
     * One past caught up is still a genuine inversion and must still be reported: the normalisation subtracts exactly
     * one, it does not grant the committed marker a free pass.
     */
    @Test
    void aCommittedOffsetTwoAboveTheSequentialMarkerIsStillInconsistent() {
        PcMeterFixture fixture = new PcMeterFixture()
                .partitionOffsets("orders", 0, 902, 900, 900, 900, 0, 3);

        PcSnapshot snapshot = samplerOver(fixture.getRegistry()).sample();

        assertThat(snapshot.getPartitions().get(0).isOffsetOrderingConsistent()).isFalse();
    }

    /**
     * The detector for the assertion above: an inverted registry must be reported as inconsistent, otherwise
     * {@link #offsetMarkersAreNonDecreasing()} is asserting nothing.
     */
    @Test
    void invertedOffsetMarkersAreReportedAsInconsistent() {
        PcMeterFixture fixture = new PcMeterFixture()
                // committed above seen - impossible in a healthy instance
                .partitionOffsets("orders", 0, 900, 100, 140, 150, 12, 3);

        PcSnapshot snapshot = samplerOver(fixture.getRegistry()).sample();

        assertThat(snapshot.getPartitions()).hasSize(1);
        assertThat(snapshot.getPartitions().get(0).isOffsetOrderingConsistent()).isFalse();
    }

    @Test
    void absentMeterYieldsAbsentValueNotZero() {
        // only the "seen" gauge and the processed counter exist for this partition
        PcMeterFixture fixture = new PcMeterFixture()
                .gauge(PcMeterFixture.PARTITION_HIGHEST_SEEN, "orders", 0, 150)
                .counter(PcMeterFixture.PROCESSED_RECORDS, "orders", 0, 7);

        PcSnapshot snapshot = samplerOver(fixture.getRegistry()).sample();

        PartitionSnapshot row = snapshot.getPartitions().get(0);
        assertThat(row.getHighestSeenOffset()).isEqualTo(150L);
        assertThat(row.getProcessedRecords()).isEqualTo(7d);
        // absent, NOT zero - zero would claim nothing has been committed, which is a different statement
        assertThat(row.getLastCommittedOffset()).isNull();
        assertThat(row.getHighestCompletedOffset()).isNull();
        assertThat(row.getHighestSequentialSucceededOffset()).isNull();
        assertThat(row.getIncompleteOffsets()).isNull();
        assertThat(row.getAssignmentEpoch()).isNull();
        assertThat(row.getFailedRecords()).isNull();
        assertThat(row.getSlowRecords()).isNull();

        assertThat(snapshot.getWork().getInflightRecords()).isNull();
        assertThat(snapshot.getWork().getDynamicLoadFactor()).isNull();
        assertThat(snapshot.getLifecycle().getState()).isNull();
        assertThat(snapshot.getLifecycle().getStateValue()).isNull();
        assertThat(snapshot.getEncoding().getEncodingCount()).isNull();
        assertThat(snapshot.getEncoding().getUsageByEncoding()).isEmpty();
    }

    /**
     * A real measurement of zero must survive as zero - otherwise the absence rule above has quietly swallowed a
     * legitimate reading.
     */
    @Test
    void measuredZeroSurvivesAsZero() {
        PcSnapshot snapshot = samplerOver(PcMeterFixture.fullyPopulated().getRegistry()).sample();

        PartitionSnapshot payments = snapshot.getPartitions().get(2);
        assertThat(payments.getKey()).isEqualTo("payments-7");
        assertThat(payments.getIncompleteOffsets()).isEqualTo(0L);
        assertThat(payments.getProcessedRecords()).isEqualTo(0d);
    }

    /**
     * NaN is what Micrometer reports once a weakly-referenced gauge target has been collected. It is a missing
     * reading, so it must be absent rather than rendered as a NaN offset.
     */
    @Test
    void nanGaugeIsAbsentRatherThanNaN() {
        PcMeterFixture fixture = new PcMeterFixture().nanGauge(PcMeterFixture.INFLIGHT_RECORDS);

        PcSnapshot snapshot = samplerOver(fixture.getRegistry()).sample();

        assertThat(snapshot.isRegistryPopulated()).isTrue();
        assertThat(snapshot.getWork().getInflightRecords()).isNull();
    }

    @Test
    void emptyRegistryProducesAnEmptyButValidSnapshot() {
        PcSnapshot snapshot = samplerOver(new SimpleMeterRegistry()).sample();

        assertThat(snapshot.isRegistryPopulated()).isFalse();
        assertThat(snapshot.isEmpty()).isTrue();
        assertThat(snapshot.getPartitions()).isEmpty();
        // valid, not null - the page renders absence, it does not null-check every branch
        assertThat(snapshot.getWork()).isNotNull();
        assertThat(snapshot.getLifecycle()).isNotNull();
        assertThat(snapshot.getEncoding()).isNotNull();
        assertThat(snapshot.getEncoding().getUsageByEncoding()).isEmpty();
        assertThat(snapshot.getCaptureEpochMillis()).isGreaterThan(0);
        assertThat(snapshot.getSampleSequence()).isEqualTo(1);
    }

    /**
     * A registry holding only unrelated application meters is still "not populated" as far as PC is concerned.
     */
    @Test
    void registryWithOnlyForeignMetersIsNotConsideredPopulated() {
        PcMeterFixture fixture = new PcMeterFixture().gauge("my.app.queue.depth", 42);

        PcSnapshot snapshot = samplerOver(fixture.getRegistry()).sample();

        assertThat(snapshot.isRegistryPopulated()).isFalse();
        assertThat(snapshot.isEmpty()).isTrue();
    }

    @Test
    void aggregateWorkStateIsRead() {
        PcSnapshot snapshot = samplerOver(PcMeterFixture.fullyPopulated().getRegistry()).sample();

        WorkSnapshot work = snapshot.getWork();
        assertThat(work.getInflightRecords()).isEqualTo(9L);
        assertThat(work.getWaitingRecords()).isEqualTo(31L);
        assertThat(work.getShards()).isEqualTo(4L);
        assertThat(work.getShardsSize()).isEqualTo(40L);
        assertThat(work.getIncompleteOffsetsTotal()).isEqualTo(16L);
        assertThat(work.getPausedPartitions()).isEqualTo(1L);
        assertThat(work.getNumberOfPartitions()).isEqualTo(3L);
        assertThat(work.getDynamicLoadFactor()).isEqualTo(2.5d);
    }

    @Test
    void numericStateGaugesAreMappedBackToStateNames() {
        PcMeterFixture fixture = new PcMeterFixture()
                .lifecycle(State.RUNNING.getValue(), State.DRAINING.getValue());

        LifecycleSnapshot lifecycle = samplerOver(fixture.getRegistry()).sample().getLifecycle();

        assertThat(lifecycle.getStateValue()).isEqualTo(State.RUNNING.getValue());
        assertThat(lifecycle.getState()).isEqualTo("RUNNING");
        assertThat(lifecycle.getPollerStateValue()).isEqualTo(State.DRAINING.getValue());
        assertThat(lifecycle.getPollerState()).isEqualTo("DRAINING");
    }

    /**
     * A number no {@link State} maps to means core has a state this module was not built against. The number still
     * renders; the name is left absent rather than guessed.
     */
    @Test
    void unknownStateNumberLeavesTheNameAbsentButKeepsTheNumber() {
        PcMeterFixture fixture = new PcMeterFixture().gauge(PcMeterFixture.STATUS, 99);

        LifecycleSnapshot lifecycle = samplerOver(fixture.getRegistry()).sample().getLifecycle();

        assertThat(lifecycle.getStateValue()).isEqualTo(99);
        assertThat(lifecycle.getState()).isNull();
    }

    @Test
    void encodingHealthIsRead() {
        EncodingSnapshot encoding = samplerOver(PcMeterFixture.fullyPopulated().getRegistry()).sample().getEncoding();

        assertThat(encoding.getEncodingCount()).isEqualTo(2L);
        assertThat(encoding.getEncodingTotalTimeMillis()).isEqualTo(10d);
        assertThat(encoding.getEncodingMaxTimeMillis()).isEqualTo(7d);
        // the live counter is tagged "encoding", although PCMetricsDef documents the tag as "codec"
        assertThat(encoding.getUsageByEncoding())
                .containsEntry("RunLength", 4d)
                .containsEntry("BitSetV2Compressed", 2d);
        assertThat(encoding.getMetadataSpaceUsedCount()).isEqualTo(2L);
        assertThat(encoding.getMetadataSpaceUsedMean()).isEqualTo(0.5d);
        assertThat(encoding.getMetadataSpaceUsedMax()).isEqualTo(0.75d);
        assertThat(encoding.getPayloadRatioUsedCount()).isEqualTo(1L);
        assertThat(encoding.getPayloadRatioUsedMax()).isEqualTo(0.5d);
    }

    /**
     * A partition-tagged meter missing either tag cannot be attributed to a row, so it is skipped rather than
     * creating a row keyed on a guess.
     */
    @Test
    void partitionMeterMissingItsTagsIsSkippedRatherThanCreatingABogusRow() {
        PcMeterFixture fixture = new PcMeterFixture()
                .gauge(PcMeterFixture.PARTITION_HIGHEST_SEEN, 150)
                .gauge(PcMeterFixture.PARTITION_HIGHEST_SEEN, "orders", 0, 150);

        PcSnapshot snapshot = samplerOver(fixture.getRegistry()).sample();

        assertThat(snapshot.getPartitions()).extracting(PartitionSnapshot::getKey).containsExactly("orders-0");
    }

    @Test
    void directStateSourceContributesIdentityAndClosedFlagWhenAPcInstanceIsSupplied() {
        AbstractParallelEoSStreamProcessor<?, ?> pc = mock(AbstractParallelEoSStreamProcessor.class);
        when(pc.isClosedOrFailed()).thenReturn(true);
        when(pc.getMyId()).thenReturn(Optional.of("pc-7"));

        StateSampler sampler = new StateSampler(new MeterSource(PcMeterFixture.fullyPopulated().getRegistry()),
                new DirectStateSource(pc));
        LifecycleSnapshot lifecycle = sampler.sample().getLifecycle();

        assertThat(lifecycle.getClosedOrFailed()).isTrue();
        assertThat(lifecycle.getInstanceId()).isEqualTo("pc-7");
        // and the meter-derived fields are still there
        assertThat(lifecycle.getState()).isEqualTo("RUNNING");
    }

    @Test
    void withoutAPcInstanceTheDirectFieldsAreAbsentRatherThanDefaulted() {
        DirectStateSource source = new DirectStateSource(null);
        assertThat(source.isAvailable()).isFalse();

        LifecycleSnapshot lifecycle = samplerOver(PcMeterFixture.fullyPopulated().getRegistry()).sample().getLifecycle();

        assertThat(lifecycle.getClosedOrFailed()).isNull();
        assertThat(lifecycle.getInstanceId()).isNull();
    }

    @Test
    void aNullRegistryIsToleratedAndReportsEverythingAbsent() {
        PcSnapshot snapshot = samplerOver(null).sample();

        assertThat(snapshot.isRegistryPopulated()).isFalse();
        assertThat(snapshot.isEmpty()).isTrue();
        assertThat(snapshot.getWork().getInflightRecords()).isNull();
        assertThat(snapshot.getEncoding().getUsageByEncoding()).isEmpty();
    }

    @Test
    void sampleSequenceCountsSamplesTaken() {
        StateSampler sampler = samplerOver(PcMeterFixture.fullyPopulated().getRegistry());

        assertThat(sampler.sample().getSampleSequence()).isEqualTo(1);
        assertThat(sampler.sample().getSampleSequence()).isEqualTo(2);
        assertThat(sampler.getSampleCount()).isEqualTo(2);
    }

    /**
     * Deep immutability is the property that makes the snapshot safe to hand to any thread - so the collections in it
     * must actually reject mutation rather than merely be documented as immutable.
     */
    @Test
    void snapshotCollectionsRejectMutation() {
        PcSnapshot snapshot = samplerOver(PcMeterFixture.fullyPopulated().getRegistry()).sample();

        assertThat(snapshot.getPartitions()).isNotEmpty();
        try {
            snapshot.getPartitions().clear();
            org.junit.jupiter.api.Assertions.fail("partition list must be unmodifiable");
        } catch (UnsupportedOperationException expected) {
            // the point of the test
        }
        try {
            snapshot.getEncoding().getUsageByEncoding().clear();
            org.junit.jupiter.api.Assertions.fail("encoding usage map must be unmodifiable");
        } catch (UnsupportedOperationException expected) {
            // the point of the test
        }
    }

    @Test
    void ageIsMeasuredAgainstTheCaptureTimestampAndNeverNegative() {
        PcSnapshot snapshot = samplerOver(new SimpleMeterRegistry()).sample();

        assertThat(snapshot.ageMillis(snapshot.getCaptureEpochMillis() + 1_500)).isEqualTo(1_500);
        // a clock that steps backwards must not produce a snapshot from the future
        assertThat(snapshot.ageMillis(snapshot.getCaptureEpochMillis() - 1_000)).isEqualTo(0);
    }
}
