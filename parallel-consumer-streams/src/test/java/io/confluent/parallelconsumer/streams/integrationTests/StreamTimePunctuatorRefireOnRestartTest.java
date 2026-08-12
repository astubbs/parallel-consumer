package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import lombok.SneakyThrows;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Does stream time survive a restart, or do STREAM_TIME punctuators re-fire over event time already
 * covered? (astubbs#255)
 *
 * <h2>Why this, and why it is uncovered</h2>
 * This is the claim U13's inflight entry actually ranked highest - a <b>duplication</b> question, not a
 * durability one - and nothing had tested it. {@code PcTaskDispatcher.seedStreamTime} exists to prevent
 * it by restoring the mark from the committed partition time, but its <em>call site</em> in
 * {@code pc-streams.patch} has no coverage: the dispatcher test calls the method directly, pinning the
 * arithmetic and not the wiring, and this module contained no STREAM_TIME punctuator test at all.
 * <p>
 * The patch's own comment at that call site states the limit plainly:
 * <blockquote>a group whose commits were written by THIS module carries PC's frontier payload, not
 * Streams' {@code TopicPartitionMetadata}, so the decode above yields UNKNOWN and there is nothing to
 * seed from.</blockquote>
 * That is a prediction this class turns into a measurement.
 *
 * <h2>The experiment, which needs no crash</h2>
 * Run 1 processes records whose event times climb to {@code T} and closes cleanly, committing. Run 2
 * restarts the <b>same application id</b> and produces records whose event times are <b>below</b> {@code T}
 * - legal late arrivals, on new offsets.
 * <ul>
 *   <li><b>Stream time restored:</b> the mark is still at {@code T}, late records cannot advance it past
 *       the next scheduled fire point, and the punctuator stays silent.</li>
 *   <li><b>Stream time restarted at UNKNOWN:</b> the punctuation queue re-anchors on the first record run 2
 *       sees, and fires over event time run 1 already covered. That is the re-fire.</li>
 * </ul>
 * The observable is each punctuation's own stream time, forwarded to an output topic - read from Kafka,
 * not inferred.
 *
 * <h2>The control varies exactly one term, and was built first</h2>
 * The stock arm runs both phases with the seam off, so Kafka writes and decodes its own
 * {@code TopicPartitionMetadata} and the seed has something to read. It was written and run <b>before</b>
 * the PC arm was looked at, because three claims died in this branch's history for want of exactly that
 * discipline - and it earned that twice over:
 * <ul>
 *   <li>It first failed because run 2's "late" records fell straight out of Kafka's retention - see
 *       {@link #runOneBaseTimestamp} for why event times are wall-clock-anchored now.</li>
 *   <li>It then failed because the arm expected <em>silence</em> from run 2. A restored mark still
 *       punctuates once, at the restored value. The discriminator is therefore not whether run 2
 *       punctuates but <b>at what stream time</b>.</li>
 * </ul>
 *
 * <h2>Measured</h2>
 * Stock run 2 punctuates at the restored mark - run 1's highest. The PC path punctuates ~69 seconds
 * <em>below</em> it, down at run 2's late event time. The mark is lost and the punctuator re-fires over
 * covered event time, exactly as the patch comment predicts.
 * <p>
 * The PC arm asserts the defective behaviour so the suite stays green and the defect stays visible; its
 * message says how to invert it on fix. The alternative - leaving it red - would make the point more
 * loudly at the cost of a permanently failing suite.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.streams.PcTaskDispatcher#seedStreamTime(long)
 */
@Slf4j
@Isolated
class StreamTimePunctuatorRefireOnRestartTest extends BrokerStreamsIntegrationTest {

    private static final int POOL_SIZE = 4;

    private static final int RUN_ONE_RECORDS = 10;

    private static final long STEP = 1_000L;

    private static final int RUN_TWO_RECORDS = 5;

    /**
     * How far below run 1's <em>base</em> run 2's late records sit. Comfortably under run 1's whole span,
     * so a late record cannot advance a restored mark to the next fire point.
     */
    private static final long LATENESS = 60_000L;

    /**
     * Event times are anchored to wall clock, not to small absolute numbers.
     * <p>
     * <b>An earlier version used 10_000 and 2_000 literally, and the control caught it.</b> Kafka's
     * time-based retention keys on the <em>record's</em> timestamp, so a record stamped 2_000ms after the
     * epoch is ~56 years old on arrival and is eligible for deletion immediately - run 2 then processed
     * nothing and the arm timed out. Anchoring to now keeps every record inside retention while preserving
     * the only property this experiment needs: run 2's records are earlier in event time than run 1's.
     */
    private final long runOneBaseTimestamp = System.currentTimeMillis();

    /** Small enough that run 1 punctuates several times across its event-time span. */
    private static final Duration PUNCTUATE_EVENT_INTERVAL = Duration.ofMillis(2_000);

    /**
     * Four commit intervals. STREAM_TIME punctuation fires synchronously during record processing, so once
     * the records are through, what is still outstanding is only the forwarded record's send and one
     * commit landing before close - both bounded by COMMIT_INTERVAL, not by seconds.
     */
    private static final Duration SETTLE = Duration.ofSeconds(2);

    private static final AtomicInteger recordsProcessed = new AtomicInteger();

    @BeforeEach
    void resetCounters() {
        recordsProcessed.set(0);
        PcDispatchCounters.reset();
    }

    @AfterEach
    void resetSwitch() {
        PcDispatchSwitch.resetToDefault();
    }

    /**
     * The control, and it runs first for a reason. Seam off throughout: Kafka writes its own partition-time
     * metadata and reads it back, so the mark survives and late records cannot punctuate.
     */
    @Test
    void stockKeepsStreamTimeAcrossARestartSoLateRecordsDoNotPunctuate() {
        PcDispatchSwitch.disable();

        Phases phases = runBothPhases("stock-refire");

        assertThat(phases.runOne)
                .as("premise: run 1 must actually punctuate, or run 2 has no covered event time to "
                        + "re-fire over and the whole experiment is void")
                .isNotEmpty();

        assertThat(lowestStreamTime(phases.runTwo))
                .as("CONTROL: with the seam off, the committed TopicPartitionMetadata carries the "
                        + "partition time and the restart restores the mark to %s. Run 2 may still "
                        + "punctuate once AT the restored mark - it does, and an earlier version of this "
                        + "arm wrongly expected silence - but it must never punctuate down at run 2's "
                        + "late event time %s. That is the discriminator: restored fires high, lost fires "
                        + "low, and the two are %sms apart.",
                        phases.highestRunOneStreamTime, lateTimestamp(),
                        phases.highestRunOneStreamTime - lateTimestamp())
                .isGreaterThanOrEqualTo(phases.highestRunOneStreamTime);
    }

    /**
     * The PC arm. The patch's own comment predicts the seed finds nothing here, because the group's
     * committed metadata is PC's frontier payload rather than Streams' partition time.
     */
    @Test
    void pcPathAcrossARestart() {
        PcDispatchSwitch.enable(POOL_SIZE);

        Phases phases = runBothPhases("pc-refire");

        assertThat(phases.dispatchedToPool)
                .as("premise: this arm must have gone through the PC dispatch seam")
                .isPositive();

        assertThat(phases.runOne)
                .as("premise: run 1 must actually punctuate")
                .isNotEmpty();

        log.info("=== PC RESTART: run 1 punctuated at {} (highest {}), run 2 punctuated at {}",
                phases.runOne, phases.highestRunOneStreamTime, phases.runTwo);

        assertThat(lowestStreamTime(phases.runTwo))
                .as("THE DEFECT, PINNED AS IT CURRENTLY BEHAVES. Run 2's records are late (event time "
                        + "%s, below run 1's highest %s), and the PC path punctuates down there - the "
                        + "mark was not restored, the queue re-anchored on the late record, and the "
                        + "punctuator fired over event time run 1 had already covered. That is the "
                        + "re-fire U13's inflight entry ranked first. The stock control fires HIGH at "
                        + "the restored mark under the identical fixture, so this is attributable to the "
                        + "seam and not to how the test is built. Cause is named in pc-streams.patch's "
                        + "own seedStreamTime comment: a PC-written group's committed metadata is PC's "
                        + "frontier payload, not Streams' TopicPartitionMetadata, so the decode yields "
                        + "UNKNOWN and there is nothing to seed from. WHEN THIS IS FIXED this assertion "
                        + "INVERTS to isGreaterThanOrEqualTo(highest), matching the control.",
                        lateTimestamp(), phases.highestRunOneStreamTime)
                .isLessThan(phases.highestRunOneStreamTime);
    }

    /** Runs phase 1, closes, restarts the same application id, runs phase 2. */
    private Phases runBothPhases(final String name) {
        String inputTopic = setupTopic(name + "-in");
        String outputTopic = setupTopic(name + "-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);
        String appId = name + "-" + System.nanoTime();

        // Phase 1: climbing event time, so stream time advances and the punctuator fires.
        produceAt(inputTopic, runOneBaseTimestamp, STEP, RUN_ONE_RECORDS, "early");
        KafkaStreams runOne = null;
        long dispatched;
        try {
            // Assigned INSIDE the try: startAndAwaitRunning start()s the client before awaiting RUNNING,
            // so a timeout there would orphan a live client - threads, consumer, producer, group
            // membership - that no finally ever closed, and it would linger into the next arm.
            runOne = startTopology(appId, inputTopic, outputTopic);
            await().atMost(Duration.ofSeconds(60))
                    .until(() -> recordsProcessed.get() >= RUN_ONE_RECORDS);
            sleepThrough(SETTLE, "letting run 1 punctuate and commit in " + name);
            dispatched = PcDispatchCounters.getRecordsDispatchedToPool();
        } finally {
            // Clean close: this arm is about what a restart restores, not about crash behaviour, and a
            // clean close is what writes the committed metadata the restart reads back.
            if (runOne != null) {
                runOne.close(Duration.ofSeconds(30));
            }
        }
        List<String> runOnePunctuations = drainAll(outputTopic);
        long highest = runOneBaseTimestamp + ((RUN_ONE_RECORDS - 1) * STEP);
        log.info("=== [{}] run 1 punctuated at {} (records spanned {}..{})",
                name, runOnePunctuations, runOneBaseTimestamp, highest);

        // Phase 2: same application id, LATE records only.
        long outputEnd = endOffset(outputTopic);
        // Second reset, and load-bearing: phase 2's await must not be satisfied by phase 1's
        // leftover count, which is already larger than RUN_TWO_RECORDS.
        recordsProcessed.set(0);
        produceAt(inputTopic, lateTimestamp(), STEP, RUN_TWO_RECORDS, "late");
        KafkaStreams runTwo = null;
        try {
            runTwo = startTopology(appId, inputTopic, outputTopic);
            await().atMost(Duration.ofSeconds(60))
                    .until(() -> recordsProcessed.get() >= RUN_TWO_RECORDS);
            sleepThrough(SETTLE, "letting run 2 punctuate if it is going to in " + name);
        } finally {
            if (runTwo != null) {
                runTwo.close(Duration.ofSeconds(30));
            }
        }
        // Scoped past run 1's output, so run 1's punctuations cannot satisfy a run 2 assertion.
        List<String> runTwoPunctuations = drainFrom(outputTopic, outputEnd);
        log.info("=== [{}] run 2 punctuated at {}", name, runTwoPunctuations);

        return new Phases(runOnePunctuations, runTwoPunctuations, highest, dispatched);
    }

    /**
     * The lowest stream time any punctuation in the list fired at, or {@link Long#MAX_VALUE} when there
     * were none. MAX_VALUE is the right identity here: "never punctuated" satisfies "never punctuated
     * below the restored mark", which is the property both arms assert.
     */
    private static long lowestStreamTime(final List<String> punctuations) {
        long lowest = Long.MAX_VALUE;
        for (String value : punctuations) {
            lowest = Math.min(lowest, Long.parseLong(value.trim()));
        }
        return lowest;
    }

    /** Run 2's event time: below run 1's base, so it cannot advance a restored mark. */
    private long lateTimestamp() {
        return runOneBaseTimestamp - LATENESS;
    }

    private void produceAt(final String topic, final long baseTimestamp, final long step,
                           final int count, final String tag) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < count; i++) {
                long timestamp = baseTimestamp + (i * step);
                producer.send(new ProducerRecord<>(topic, null, timestamp,
                        tag + "-key-" + i, tag + "-value-" + i));
            }
            producer.flush();
        }
        log.info("Produced {} '{}' records into {} at event times {}..{}",
                count, tag, topic, baseTimestamp, baseTimestamp + ((count - 1) * step));
    }

    private List<String> drainAll(final String topic) {
        return drainFrom(topic, 0L);
    }

    /**
     * Records at or after {@code fromOffset}. A fixed poll budget rather than an await-for-count: the
     * question is often whether anything is there, and awaiting at-least-one would hang on exactly the
     * silence this class expects.
     */
    private List<String> drainFrom(final String topic, final long fromOffset) {
        List<String> values = new ArrayList<>();
        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.assign(UniLists.of(partition));
            consumer.seek(partition, fromOffset);
            // Stop after a few consecutive quiet polls rather than always burning the whole budget.
            // This keeps the property that matters - an arm may legitimately observe nothing, so there is
            // no await-for-at-least-one that would hang on silence - while returning as soon as the topic
            // has gone quiet, whether that is because nothing came or because everything already did.
            int quiet = 0;
            for (int poll = 0; poll < 8 && quiet < 3; poll++) {
                int before = values.size();
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    values.add(record.value());
                }
                quiet = values.size() == before ? quiet + 1 : 0;
            }
        }
        return values;
    }

    /**
     * The shared AdminClient, as the rest of the suite does for end-offset lookups - a whole
     * {@code KafkaConsumer} for one stateless metadata call is construction cost for nothing.
     */
    @SneakyThrows
    private long endOffset(final String topic) {
        TopicPartition partition = new TopicPartition(topic, 0);
        return getKcu().getAdmin()
                .listOffsets(UniMaps.of(partition, OffsetSpec.latest()))
                .partitionResult(partition).get().offset();
    }

    private KafkaStreams startTopology(final String appId,
                                       final String inputTopic,
                                       final String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
                .process((ProcessorSupplier<String, String, String, String>) () ->
                        new Processor<String, String, String, String>() {

                            private ProcessorContext<String, String> context;

                            @Override
                            public void init(final ProcessorContext<String, String> context) {
                                this.context = context;
                                context.schedule(PUNCTUATE_EVENT_INTERVAL, PunctuationType.STREAM_TIME,
                                        timestamp -> {
                                            // Forward the stream time this punctuation fired at - the
                                            // observable, straight onto a topic.
                                            log.info("PUNCTUATED at stream time {}", timestamp);
                                            this.context.forward(new Record<>(
                                                    "punctuation", String.valueOf(timestamp), timestamp));
                                        });
                            }

                            @Override
                            public void process(final Record<String, String> record) {
                                recordsProcessed.incrementAndGet();
                            }
                        })
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        Properties props = baseStreamsProps(appId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500L);
        return startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT);
    }

    /** Both phases' observations. Plain fields: the Java 8 release target rules out a record. */
    private static final class Phases {

        private final List<String> runOne;

        private final List<String> runTwo;

        private final long highestRunOneStreamTime;

        private final long dispatchedToPool;

        private Phases(final List<String> runOne, final List<String> runTwo,
                       final long highestRunOneStreamTime, final long dispatchedToPool) {
            this.runOne = runOne;
            this.runTwo = runTwo;
            this.highestRunOneStreamTime = highestRunOneStreamTime;
            this.dispatchedToPool = dispatchedToPool;
        }
    }
}
