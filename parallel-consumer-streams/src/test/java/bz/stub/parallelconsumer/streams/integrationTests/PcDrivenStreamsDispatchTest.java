package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The U5 end-to-end arm: a real broker, a real {@code KafkaStreams} instance, and a real topology, with the
 * task's records reaching the processor chain through Parallel Consumer's {@code WorkManager} and a worker
 * pool instead of through {@code partitionGroup.nextRecord()} on the StreamThread.
 * <p>
 * The two arms below are the same topology and the same data; the only difference is
 * {@link PcDispatchSwitch}. That is what makes the dispatch marker meaningful - correct output is asserted in
 * both, so output alone would prove nothing about which path ran.
 * <p>
 * {@code @Isolated} is not decoration. The switch is process-wide (see {@link PcDispatchSwitch} for why it has
 * to be), and this module runs tests concurrently, so without isolation the flag-on arm could turn PC
 * dispatch on underneath {@link ShadowedStreamsControlTest} - silently converting the control arm into a
 * second PC arm and destroying the only baseline the module has.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.streams.PcTaskDispatcherTest
 * @see ShadowedStreamsControlTest
 */
@Slf4j
@Isolated
class PcDrivenStreamsDispatchTest extends BrokerStreamsIntegrationTest {

    private static final int KEYS = 6;
    private static final int RECORDS_PER_KEY = 5;
    private static final int TOTAL = KEYS * RECORDS_PER_KEY;

    private static final int POOL_SIZE = 4;

    /**
     * Slow enough that records genuinely overlap in the chain, short enough that the whole run is quick. With
     * stock serial dispatch, {@link #TOTAL} records at this cost take at least
     * {@code TOTAL * PROCESSING_COST}; concurrency is what keeps this test from being tedious as well as what
     * it is asserting.
     */
    private static final Duration PROCESSING_COST = Duration.ofMillis(150);

    private static final String SUFFIX = "-processed";

    private final ChainConcurrencyProbe probe = new ChainConcurrencyProbe();

    @BeforeEach
    void resetDispatchState() {
        PcDispatchCounters.reset();
        probe.reset();
    }

    /** Hand the JVM back at the artifact's default (on), so the next test states its own requirement. */
    @AfterEach
    void restoreDefaultDispatch() {
        PcDispatchSwitch.resetToDefault();
    }

    @Test
    void recordsReachTheChainThroughTheWorkManagerAndRunConcurrently() {
        PcDispatchSwitch.enable(POOL_SIZE);

        Map<String, List<String>> received = runTopology("pc-on");
        logDispatchEvidence("PC ON");

        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("THE assertion of this unit: records were handed to a worker pool by PcTaskDispatcher. "
                        + "This counter moves at exactly one place in the codebase, so a non-zero reading "
                        + "cannot be produced by the stock path however correct its output looks.")
                .isGreaterThanOrEqualTo(TOTAL);

        assertThat(PcDispatchCounters.getRecordsAcceptedByWorkManager())
                .as("and none were dropped for want of a partition-assignment epoch on the way in")
                .isEqualTo(PcDispatchCounters.getRecordsOfferedToWorkManager());
        assertThat(PcDispatchCounters.getRecordsOfferedToWorkManager())
                .as("every produced record must have been offered to PC")
                .isGreaterThanOrEqualTo(TOTAL);

        assertThat(probe.peakConcurrency.get())
                .as("with a pool of %s and a %s processor, at least 3 records must be inside the processor "
                                + "chain at once - this is the property the whole module exists to test",
                        POOL_SIZE, PROCESSING_COST)
                .isGreaterThanOrEqualTo(3);
        assertThat(probe.keysSeenConcurrentlyWithThemselves)
                .as("but never two records of the same key - KEY ordering is what lets a Streams topology "
                        + "survive parallel dispatch at all")
                .isEmpty();

        assertOutputIsCorrect(received);
    }

    /**
     * The flag-off arm. Same topology, same data, stock Kafka Streams dispatch - the marker must not move.
     */
    @Test
    void withTheSwitchOffNothingIsDispatchedAndTheOutputIsStillCorrect() {
        // Explicit, and load-bearing: the seam defaults ON, so without this line this arm is not a control.
        PcDispatchSwitch.disable();

        Map<String, List<String>> received = runTopology("pc-off");
        logDispatchEvidence("PC OFF");

        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("with the switch off, no record may reach the worker pool")
                .isZero();
        assertThat(PcDispatchCounters.getRecordsOfferedToWorkManager())
                .as("nor may any record be handed to PC at all - addRecords must go to the partition group")
                .isZero();

        assertThat(probe.peakConcurrency.get())
                .as("and the chain must be walked one record at a time, on the StreamThread")
                .isEqualTo(1);

        assertOutputIsCorrect(received);
    }

    /**
     * The failure arm, end to end - the half of the retry story the dispatcher-level test cannot reach,
     * because it exercises the patched {@code StreamTask.pcProcess} re-throw rather than the bridge.
     * <p>
     * Stock Kafka Streams answers a processor exception by surfacing it to the uncaught-exception handler and
     * killing the thread. Parallel Consumer's answer is to re-dispatch, which here would re-run the whole
     * chain including {@code forward()} calls that already emitted downstream - duplicates stock never
     * produces. So retries are disabled and the failure is surfaced instead: the assertion below is that the
     * poison record is attempted <b>exactly once</b>, and that the client reports the failure rather than
     * hanging.
     */
    @Test
    void aProcessorThatThrowsSurfacesTheFailureAndRunsTheRecordExactlyOnce() {
        PcDispatchSwitch.enable(POOL_SIZE);
        probe.poisonKey = "key-3";

        String inputTopic = setupTopic("pc-boom-in");
        String outputTopic = setupTopic("pc-boom-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        KafkaStreams streams = startTopology("pc-boom", inputTopic, outputTopic);
        try {
            produce(inputTopic);

            await().atMost(Duration.ofSeconds(90)).until(() ->
                    streams.state() == KafkaStreams.State.ERROR
                            || streams.state() == KafkaStreams.State.NOT_RUNNING);

            logDispatchEvidence("PC BOOM");

            assertThat(probe.poisonAttempts.get())
                    .as("exactly one attempt. A retry would re-run the chain from the source node and re-emit "
                            + "every record the first attempt already forwarded - a duplicate stock Kafka "
                            + "Streams never produces, which is why PC's retries are disabled here.")
                    .isEqualTo(1);
            assertThat(PcDispatchCounters.getRecordsFailed())
                    .as("and the dispatcher must have counted exactly that one failure")
                    .isEqualTo(1);
            assertThat(PcDispatchCounters.getRecordsCompletedSuccessfully())
                    .as("records of other keys must still have run - a failure blocks its own KEY shard, not "
                            + "the task")
                    .isGreaterThan(0);
        } finally {
            streams.close(Duration.ofSeconds(60));
        }
    }

    // ---------------------------------------------------------------------------------------------------

    /**
     * The numbers a result document can quote, printed rather than only asserted - "at least 3 in flight"
     * passing tells you the bar was cleared, not by how much.
     */
    private void logDispatchEvidence(final String arm) {
        log.info("=== {} arm | offered={} accepted={} dispatchedToPool={} succeeded={} failed={} "
                        + "peakChainConcurrency={} keysConcurrentWithThemselves={} poisonAttempts={}",
                arm,
                PcDispatchCounters.getRecordsOfferedToWorkManager(),
                PcDispatchCounters.getRecordsAcceptedByWorkManager(),
                PcDispatchCounters.getRecordsDispatchedToPool(),
                PcDispatchCounters.getRecordsCompletedSuccessfully(),
                PcDispatchCounters.getRecordsFailed(),
                probe.peakConcurrency.get(),
                probe.keysSeenConcurrentlyWithThemselves.keySet(),
                probe.poisonAttempts.get());
    }

    private void assertOutputIsCorrect(final Map<String, List<String>> received) {
        assertThat(flatten(received))
                .as("every input record must produce exactly one output record - no drops, no duplicates")
                .hasSize(TOTAL);
        assertThat(received.keySet())
                .as("every key that went in must come out")
                .hasSize(KEYS);

        for (Map.Entry<String, List<String>> perKey : received.entrySet()) {
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < RECORDS_PER_KEY; i++) {
                expected.add("v" + i + SUFFIX);
            }
            assertThat(perKey.getValue())
                    .as("key %s must come out in the order it went in - under KEY ordering this survives "
                            + "parallel dispatch, and it is the first thing to break if it does not",
                            perKey.getKey())
                    .containsExactlyElementsOf(expected);
        }
    }

    private Map<String, List<String>> runTopology(final String namePrefix) {
        String inputTopic = setupTopic(namePrefix + "-in");
        String outputTopic = setupTopic(namePrefix + "-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        KafkaStreams streams = startTopology(namePrefix, inputTopic, outputTopic);
        try {
            produce(inputTopic);
            return consumeByKey(outputTopic);
        } finally {
            streams.close(Duration.ofSeconds(60));
        }
    }

    private KafkaStreams startTopology(final String namePrefix, final String inputTopic, final String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);
        // The key-aware mapValues overload, so the probe can tell "two records of the same key at once"
        // from "two different keys at once" - they are the two halves of the KEY-ordering claim, and the
        // plain ValueMapper cannot distinguish them.
        stream.mapValues((key, value) -> probe.processOneRecord(key, value)).to(outputTopic);

        Properties props = baseStreamsProps(namePrefix + "-" + System.nanoTime());
        // One StreamThread, so the only concurrency observed inside the chain is the one this unit
        // introduced. With two threads a probe reading "2 at once" would prove nothing.
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        // __processing.threads.enabled__ is deliberately left UNSET (default false). It selects
        // SynchronizedPartitionGroup inside StreamTask - a lock around a partition group that the PC path
        // does not use at all - and it also puts Kafka Streams' own async task executor between the poll loop
        // and process(), which is precisely the seam being measured. Leaving it off keeps the flag-off arm
        // stock and the flag-on arm attributable.

        return startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT);
    }

    private void produce(final String inputTopic) {
        try (KafkaProducer<String, String> producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < RECORDS_PER_KEY; i++) {
                for (int k = 0; k < KEYS; k++) {
                    producer.send(new ProducerRecord<>(inputTopic, "key-" + k, "v" + i));
                }
            }
            producer.flush();
        }
        log.info("Produced {} records across {} keys", TOTAL, KEYS);
    }

    private Map<String, List<String>> consumeByKey(final String outputTopic) {
        Map<String, List<String>> byKey = new LinkedHashMap<>();
        try (KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.subscribe(UniLists.of(outputTopic));

            await().atMost(Duration.ofSeconds(120)).until(() -> {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    byKey.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(record.value());
                }
                int total = flatten(byKey).size();
                log.debug("Consumed {}/{} so far", total, TOTAL);
                return total >= TOTAL;
            });
        }
        return byKey;
    }

    private static List<String> flatten(final Map<String, List<String>> byKey) {
        List<String> all = new ArrayList<>();
        byKey.values().forEach(all::addAll);
        return all;
    }

    /**
     * Sits inside the topology as the {@code mapValues} body, so what it measures is concurrency <em>within
     * the processor chain</em> - not within the dispatcher. That distinction matters: the dispatcher could
     * happily run four threads that all serialise on some lock inside Kafka Streams, and only a probe on this
     * side of the seam would notice.
     */
    private static final class ChainConcurrencyProbe {

        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peakConcurrency = new AtomicInteger();
        private final Map<String, AtomicInteger> inFlightPerKey = new ConcurrentHashMap<>();
        private final Map<String, Boolean> keysSeenConcurrentlyWithThemselves = new ConcurrentHashMap<>();

        /** When set, records with this key throw - see the failure arm. */
        private volatile String poisonKey;

        private final AtomicInteger poisonAttempts = new AtomicInteger();

        private void reset() {
            inFlight.set(0);
            peakConcurrency.set(0);
            inFlightPerKey.clear();
            keysSeenConcurrentlyWithThemselves.clear();
            poisonKey = null;
            poisonAttempts.set(0);
        }

        private String processOneRecord(final String key, final String value) {
            if (key.equals(poisonKey)) {
                poisonAttempts.incrementAndGet();
                throw new IllegalStateException("processor blew up on " + key + "/" + value);
            }
            int concurrent = inFlight.incrementAndGet();
            peakConcurrency.accumulateAndGet(concurrent, Math::max);

            AtomicInteger perKey = inFlightPerKey.computeIfAbsent(key, ignored -> new AtomicInteger());
            if (perKey.incrementAndGet() > 1) {
                keysSeenConcurrentlyWithThemselves.put(key, Boolean.TRUE);
            }
            try {
                Thread.sleep(PROCESSING_COST.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                perKey.decrementAndGet();
                inFlight.decrementAndGet();
            }
            return value + SUFFIX;
        }
    }
}
