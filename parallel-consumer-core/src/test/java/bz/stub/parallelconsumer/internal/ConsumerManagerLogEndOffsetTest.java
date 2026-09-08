package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.OptionalLong;

import static com.google.common.truth.Truth.assertThat;

/**
 * {@link ConsumerManager#logEndOffsetIfKnownWithoutBlocking} is the ground truth behind the offset-map plausibility
 * check ({@code PartitionState#maybeVerifyLoadedOffsetMapAgainstThePartition}), and it has exactly one hard
 * requirement beyond being right: it must never block, never throw, and never send a request of its own - it runs on
 * the poll thread, inside the poll loop, for a check that must not be able to slow ingestion down.
 * <p>
 * <b>What {@link MockConsumer} actually does here, verified rather than assumed</b> (kafka-clients 3.9.1): its
 * {@code currentLag} answers {@code endOffsets.get(tp) - position(tp)} when the partition has been given an end
 * offset with {@link MockConsumer#updateEndOffsets}, and <b>{@code OptionalLong.of(0)} when it has not</b> - not
 * empty, as a real consumer answers before its first fetch. Lag zero reads as "the partition ends at the current
 * position", so an unprimed mock understates where the partition ends, and a test that needs this check armed has to
 * prime end offsets. Nothing in the suite was affected (the check only engages for a partition whose committed
 * metadata claims offsets above the committed one), but the next test to reach for it will meet this, so it is
 * pinned here rather than left to be rediscovered.
 *
 * @author Antony Stubbs
 */
@Slf4j
class ConsumerManagerLogEndOffsetTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 0);

    static final Duration TIMEOUT = Duration.ofSeconds(1);

    private ConsumerManager<String, String> managerFor(MockConsumer<String, String> mockConsumer) {
        return new ConsumerManager<>(new ThreadConfinedConsumer<>(mockConsumer), TIMEOUT, TIMEOUT, TIMEOUT);
    }

    /**
     * The reconstruction that matters: the log end offset is {@code position + lag}, and both are read together.
     */
    @Test
    void theLogEndOffsetIsThePositionPlusTheLagTheLastFetchReported() {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(UniLists.of(TP));
        mockConsumer.updateBeginningOffsets(UniMaps.of(TP, 0L));
        mockConsumer.updateEndOffsets(UniMaps.of(TP, 500L));
        mockConsumer.seek(TP, 100L);

        assertThat(managerFor(mockConsumer).logEndOffsetIfKnownWithoutBlocking(TP))
                .isEqualTo(OptionalLong.of(500L));
    }

    /**
     * A partition the consumer has no position for cannot be answered without asking the broker, and asking is
     * exactly what this method may not do. The answer is "not established", which the caller re-checks on the next
     * batch - it is never read as a bound.
     */
    @Test
    void aPartitionWithNoPositionAnswersNotEstablishedRatherThanBlocking() {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(UniLists.of(TP));
        mockConsumer.updateEndOffsets(UniMaps.of(TP, 500L)); // an end offset, but nowhere to measure it from

        assertThat(managerFor(mockConsumer).logEndOffsetIfKnownWithoutBlocking(TP))
                .isEqualTo(OptionalLong.empty());
    }

    /**
     * The mock's answer with no end offset given to it, pinned because the prose above rests on it and because a
     * test that assumed {@code empty()} here would be quietly wrong: the mock says lag zero, so this method answers
     * "the partition ends at the position", which is a bound - just a mistaken one. Prime end offsets in any test
     * that needs this check to engage truthfully.
     */
    @Test
    void anUnprimedMockConsumerClaimsZeroLagRatherThanNotKnowing() {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(UniLists.of(TP));
        mockConsumer.updateBeginningOffsets(UniMaps.of(TP, 0L));
        mockConsumer.seek(TP, 100L);

        assertThat(managerFor(mockConsumer).logEndOffsetIfKnownWithoutBlocking(TP))
                .isEqualTo(OptionalLong.of(100L)); // position + 0, not empty - MockConsumer.currentLag's default
    }

    /**
     * And a partition that is not ours at all - which happens when a rebalance lands between the poll and this read.
     */
    @Test
    void anUnassignedPartitionAnswersNotEstablished() {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);

        assertThat(managerFor(mockConsumer).logEndOffsetIfKnownWithoutBlocking(TP))
                .isEqualTo(OptionalLong.empty());
    }
}
