package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

/**
 * {@link ConsumerManager#getPausedPartitionSize()} must describe the pause state of the poll that is
 * <b>currently in flight</b>, because its one non-diagnostic reader acts during exactly that window:
 * the control thread's back-pressure wakeup
 * ({@code AbstractParallelEoSStreamProcessor#maybeWakeupPoller} via
 * {@code BrokerPollSystem#isSubscriptionsPausedForBackPressure}) fires only while the poll thread is
 * asleep inside a paused long poll, and only if the cache says "paused".
 * <p>
 * The poll loop pauses the subscription immediately BEFORE calling {@code poll()}
 * ({@code BrokerPollSystem#checkStateForPausingSubscriptions}), so a cache refreshed only AFTER
 * {@code poll()} returns always reports the pause one poll late: it reads "not paused" for the whole
 * of the paused sleep. The control thread then never wakes the poller, the paused long poll runs its
 * full timeout with the pipeline drained, and ingestion degrades to one burst per long-poll timeout -
 * the 4-10x transactional throughput regression diagnosed on the confluentinc#857 branch
 * (astubbs/parallel-consumer#29). The cache must therefore be refreshed on ENTRY to
 * {@link ConsumerManager#poll(Duration)}, after the caller's pause/resume decision, as well as on
 * exit (the exit refresh is what keeps assignment and group metadata current across a rebalance,
 * which happens inside {@code poll()}).
 */
@Slf4j
@Timeout(30)
class ConsumerManagerPauseCacheTest {

    private static final TopicPartition TP = new TopicPartition("ConsumerManagerPauseCacheTest", 0);
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    /**
     * Pause the subscription after one completed poll (so the cache holds a stale "not paused"), then
     * observe the cache from inside the next poll - the deterministic stand-in for the control
     * thread's mid-poll read. {@link MockConsumer#schedulePollTask} runs the observation at the exact
     * moment a paused long poll would be sleeping.
     */
    @Test
    void pausedPartitionCacheIsFreshDuringThePollItDescribes() {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        mockConsumer.updateBeginningOffsets(UniMaps.of(TP, 0L));
        mockConsumer.assign(UniLists.of(TP));
        var consumerManager = new ConsumerManager<>(new ThreadConfinedConsumer<>(mockConsumer),
                TIMEOUT, TIMEOUT, TIMEOUT);

        // a completed poll while unpaused - leaves the exit-refreshed cache reading "not paused"
        consumerManager.poll(Duration.ofMillis(1));
        assertThat(consumerManager.getPausedPartitionSize()).isEqualTo(0);

        // the poll loop's pause decision, made between polls, exactly as managePauseOfSubscription does
        consumerManager.pause(UniSets.of(TP));

        // observed from within the following poll - the window in which maybeWakeupPoller decides
        AtomicInteger pausedSizeSeenDuringPoll = new AtomicInteger(-1);
        mockConsumer.schedulePollTask(() ->
                pausedSizeSeenDuringPoll.set(consumerManager.getPausedPartitionSize()));
        consumerManager.poll(Duration.ofMillis(1));

        assertThat(pausedSizeSeenDuringPoll.get()).isEqualTo(1);
    }
}
