package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import io.confluent.parallelconsumer.streams.PcTaskDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The memory bound, proven rather than asserted (astubbs#255, U14).
 * <p>
 * <b>A test that only checks the consumer was paused is not enough.</b> Pausing is the mechanism; the
 * property is that records stop accumulating in memory under a processor slower than the broker feed. Those
 * are different claims, and only the second one is the reason this work exists - before it, PC held every
 * record Kafka Streams handed it, {@code partitionGroup.numBuffered()} reported zero, the pause in
 * {@code addRecords} never fired, and nothing bounded inflow but heap.
 * <p>
 * <b>Two arms differing in exactly one term</b>, {@link PcDispatchSwitch#setBackpressure(boolean)}. Same
 * broker, same topology, same data, same processing cost, same JVM. That is the only way to show the bound
 * is the fix's doing rather than an accident of how fast this machine happens to be - a single arm that
 * stayed under the bound would prove nothing, because a fast enough consumer never fills a buffer.
 * <p>
 * <b>The instrument is a sampler, and it has to be.</b> Occupancy peaks during the run and is zero once
 * processing drains, so any assertion made afterwards sees the same zero whether the run stayed bounded or
 * first accumulated the entire topic. The watcher thread below reads
 * {@link PcTaskDispatcher#bufferedRecordsAcrossActive()} throughout and keeps the maximum - which is also
 * the reason that value is published rather than merely computed on the owner thread.
 *
 * @author Antony Stubbs
 * @see PcTaskDispatcher#bufferedRecordsAcrossActive()
 */
@Slf4j
// The dispatch switch and the backpressure switch are both process-wide - see PcDispatchSwitch for why they
// have to be - and this module runs test classes concurrently.
@Isolated
class BackpressureBoundIntegrationTest extends BrokerStreamsIntegrationTest {

    private static final int TOTAL = 600;

    /**
     * Distinct keys throughout, so KEY ordering never becomes the thing limiting inflow. If records shared
     * keys, PC would hand out fewer of them and the buffer would fill for a reason that has nothing to do
     * with backpressure - and the control arm would look bounded too.
     */
    private static final int POOL_SIZE = 4;

    /**
     * Slow enough that the broker feed outruns the processor, which is the condition the bound exists for.
     * At {@link #POOL_SIZE} threads this drains roughly 400 records/second against a broker that delivers
     * {@link #TOTAL} in well under a second.
     */
    private static final Duration PROCESSING_COST = Duration.ofMillis(10);

    /** Kafka's own knob, set low so the bound is reachable inside a short run. */
    private static final int BUFFERED_RECORDS_PER_PARTITION = 10;

    /**
     * Also load-bearing, and the reason is worth stating: {@code addRecords} is handed a whole poll batch at
     * once and can only pause <em>after</em> registering it, so one fetch can always overshoot the threshold
     * by up to this much. Stock Kafka Streams has exactly the same property. Left at its default of 1000 a
     * single poll could deliver the entire topic and the bound would be meaningless.
     */
    private static final int MAX_POLL_RECORDS = 20;

    /**
     * What the fixed arm must stay under. Derived, not tuned: the threshold plus one poll batch, plus the
     * pool because a record handed to a worker has left the buffer but a replacement can arrive immediately.
     */
    private static final int BOUND = BUFFERED_RECORDS_PER_PARTITION + MAX_POLL_RECORDS + POOL_SIZE;

    private final AtomicInteger peakBuffered = new AtomicInteger();

    private final AtomicBoolean sampling = new AtomicBoolean();

    private Thread sampler;

    @AfterEach
    void restoreDefaults() {
        stopSampling();
        PcDispatchSwitch.resetToDefault();
    }

    /**
     * The arm that matters. With backpressure on, the consumer is paused whenever PC is holding more than
     * the configured threshold, so records stop arriving and occupancy stays bounded however far the
     * processor falls behind.
     */
    @Test
    void withBackpressureOnRecordsStopAccumulating() {
        PcDispatchSwitch.enable(POOL_SIZE);
        PcDispatchSwitch.setBackpressure(true);
        assertThat(PcDispatchSwitch.isBackpressureEnabled())
                .as("the varied term, read back rather than assumed - an arm that silently ran the other "
                        + "configuration would compare a run against itself")
                .isTrue();

        int peak = runAndSamplePeakOccupancy("pc-bp-on");

        assertThat(peak)
                .as("THE assertion of this unit: with a processor slower than the feed, records stop "
                        + "accumulating. Peak occupancy %s must stay within %s = threshold %s + one poll "
                        + "batch %s + pool %s", peak, BOUND, BUFFERED_RECORDS_PER_PARTITION, MAX_POLL_RECORDS,
                        POOL_SIZE)
                .isLessThanOrEqualTo(BOUND);

        assertThat(peak)
                .as("and the buffer must actually have filled - a run that never approached its own bound "
                        + "is not exercising the thing it bounds, and would pass with the feature deleted")
                .isGreaterThanOrEqualTo(BUFFERED_RECORDS_PER_PARTITION);
    }

    /**
     * The control. Identical in every respect except the one switch, and it must show the unbounded growth
     * the fixed arm prevents - otherwise the fixed arm's bound is an artefact of this machine's timing and
     * proves nothing.
     */
    @Test
    void withBackpressureOffRecordsAccumulateWithoutBound() {
        PcDispatchSwitch.enable(POOL_SIZE);
        PcDispatchSwitch.setBackpressure(false);
        assertThat(PcDispatchSwitch.isBackpressureEnabled())
                .as("the varied term, read back rather than assumed")
                .isFalse();

        int peak = runAndSamplePeakOccupancy("pc-bp-off");

        assertThat(peak)
                .as("the control must overshoot the bound the fixed arm holds, or the two arms are not "
                        + "measuring anything. Peak occupancy was %s against a bound of %s", peak, BOUND)
                .isGreaterThan(BOUND);
    }

    /**
     * Runs the topology end to end and returns the highest occupancy any sample saw.
     * <p>
     * Every record is also required to come out the far end. A bound achieved by dropping records is not a
     * bound, and it is the failure mode a backpressure bug would most plausibly produce.
     */
    private int runAndSamplePeakOccupancy(final String namePrefix) {
        String inputTopic = setupTopic(namePrefix + "-in");
        String outputTopic = setupTopic(namePrefix + "-out");
        // One partition, so the per-partition bound and the JVM-wide sampled total are the same number.
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        // Produced BEFORE the topology starts, so the backlog exists from the first poll and the processor
        // is behind immediately. Starting first would let the topology keep pace with the producer and no
        // buffer would ever build, in either arm.
        produce(inputTopic);

        startSampling();
        KafkaStreams streams = startTopology(namePrefix, inputTopic, outputTopic);
        try {
            awaitAllProcessed(outputTopic);
        } finally {
            stopSampling();
            streams.close(Duration.ofSeconds(30));
        }

        int peak = peakBuffered.get();
        log.info("{}: peak buffered occupancy {} (bound {})", namePrefix, peak, BOUND);
        return peak;
    }

    private void startSampling() {
        peakBuffered.set(0);
        sampling.set(true);
        sampler = new Thread(() -> {
            while (sampling.get()) {
                peakBuffered.accumulateAndGet(PcTaskDispatcher.bufferedRecordsAcrossActive(), Math::max);
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "backpressure-occupancy-sampler");
        sampler.setDaemon(true);
        sampler.start();
    }

    private void stopSampling() {
        sampling.set(false);
        if (sampler != null) {
            try {
                sampler.join(Duration.ofSeconds(10).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            sampler = null;
        }
    }

    private KafkaStreams startTopology(final String namePrefix, final String inputTopic, final String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);
        stream.mapValues((key, value) -> {
            sleep(PROCESSING_COST);
            return value;
        }).to(outputTopic);

        Properties props = baseStreamsProps(namePrefix + "-" + System.nanoTime());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, BUFFERED_RECORDS_PER_PARTITION);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), MAX_POLL_RECORDS);

        return startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT);
    }

    private void produce(final String inputTopic) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < TOTAL; i++) {
                producer.send(new ProducerRecord<>(inputTopic, "key-" + i, "v" + i));
            }
            producer.flush();
        }
        log.info("Produced {} records across {} distinct keys", TOTAL, TOTAL);
    }

    private void awaitAllProcessed(final String outputTopic) {
        AtomicInteger consumed = new AtomicInteger();
        try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.subscribe(java.util.Collections.singletonList(outputTopic));
            await().atMost(Duration.ofSeconds(180)).until(() -> {
                consumed.addAndGet(consumer.poll(Duration.ofMillis(500)).count());
                return consumed.get() >= TOTAL;
            });
        }
        assertThat(consumed.get())
                .as("every record must come out the far end - a bound achieved by dropping records is not a "
                        + "bound, and is what a backpressure defect would most plausibly produce")
                .isGreaterThanOrEqualTo(TOTAL);
    }

    private static void sleep(final Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while simulating processing cost", e);
        }
    }
}
