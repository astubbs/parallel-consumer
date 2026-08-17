
/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */
package bz.stub.parallelconsumer.integrationTests;

import bz.stub.parallelconsumer.internal.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ParallelConsumerOptionsBuilder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every broker IT also inherits the {@link AmbientProbeExtension} flight recorder: a background
 * observer that turns generic test timeouts into diagnosed failures (see its javadoc). It never
 * fails a test; opt out with {@link NoAmbientProbe} or {@code -Dambient.probe=off}.
 *
 * @author Antony Stubbs
 */
@Testcontainers
@ExtendWith(AmbientProbeExtension.class)
@Slf4j
public abstract class BrokerIntegrationTest<K, V> {

    static {
        System.setProperty("flogger.backend_factory", "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
    }

    int numPartitions = 1;
    int partitionNumber = 0;

    @Getter
    String topic;

    /**
     * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
     * https://github.com/testcontainers/testcontainers-java/pull/1781
     */
    public static KafkaContainer kafkaContainer = createKafkaContainer(null);

    /**
     * Derives the Confluent Platform version from the Apache Kafka client version so that
     * the broker under test matches the client. The CI matrix overrides {@code kafka.version}
     * via {@code -Dkafka.version=X.Y.Z}, so we read it at runtime from the client jar.
     * <p>
     * Mapping: CP major = AK major + 4 (e.g., AK 3.1 → CP 7.1, AK 3.9 → CP 7.9).
     */
    private static final String FALLBACK_CP_IMAGE = "confluentinc/cp-kafka:7.9.0";

    static String deriveCpKafkaImage() {
        String akVersion = org.apache.kafka.common.utils.AppInfoParser.getVersion();
        log.info("Kafka client version detected: {}", akVersion);

        try {
            // Strip pre-release suffixes (e.g. "4.0.0-SNAPSHOT" -> "4.0.0")
            String cleanVersion = akVersion.split("-")[0];
            String[] parts = cleanVersion.split("\\.");
            int akMajor = Integer.parseInt(parts[0]);
            int akMinor = Integer.parseInt(parts[1]);

            // CP major = AK major + 4, CP minor = AK minor
            int cpMajor = akMajor + 4;
            int cpMinor = akMinor;

            String cpImage = "confluentinc/cp-kafka:" + cpMajor + "." + cpMinor + ".0";
            log.info("Using CP Kafka image: {} (derived from AK {})", cpImage, akVersion);
            return cpImage;
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            log.warn("Could not parse Kafka version '{}', falling back to {}", akVersion, FALLBACK_CP_IMAGE, e);
            return FALLBACK_CP_IMAGE;
        }
    }

    public static KafkaContainer createKafkaContainer(String logSegmentSize) {
        KafkaContainer base = new KafkaContainer(DockerImageName.parse(deriveCpKafkaImage()))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") //transaction.state.log.num.partitions
                //todo need to customise this for this test
                // default produce batch size is - must be at least higher than it: 16KB
                // try to speed up initial consumer group formation
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500") // group.initial.rebalance.delay.ms default: 3000
                .withReuse(true);

        if (StringUtils.isNotBlank(logSegmentSize)) {
            base = base.withEnv("KAFKA_LOG_SEGMENT_BYTES", logSegmentSize);
        }

        return base;
    }

    static {
        kafkaContainer.start();
    }

    @Getter(AccessLevel.PROTECTED)
    private final KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer);

    @BeforeAll
    static void followKafkaLogs() {
        if (log.isDebugEnabled()) {
            FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
            kafkaContainer.followOutput(logConsumer);
        }
    }

    @BeforeEach
    void open() {
        kcu.open();
    }

    @AfterEach
    void close() {
        kcu.close();
    }

    protected void setupTopic() {
        String name = LoadTest.class.getSimpleName();
        setupTopic(name);
    }

    protected String setupTopic(String name) {
        assertThat(kafkaContainer.isRunning()).isTrue(); // sanity

        topic = name + "-" + nextInt();

        ensureTopic(topic, numPartitions);

        return topic;
    }

    protected CreateTopicsResult ensureTopic(String topic, int numPartitions) {
        // Delegates to the canonical blocking helper so topic-creation logic lives in one place
        // (avoids the drift that reintroduced a flaky short timeout here). See KafkaClientUtils#createTopic.
        return kcu.createTopic(topic, numPartitions);
    }

    /**
     * <b>Start here when writing a PC integration test.</b> Does the setup nearly every one of them opens with -
     * a fresh topic, a consumer in a new group, and a {@link ParallelEoSStreamProcessor} subscribed to that topic
     * - so the test declares only the options it actually cares about, and the interesting configuration is not
     * buried in boilerplate:
     * <pre>{@code
     * @BeforeEach
     * void setUp() {
     *     pc = startPcOnNewTopic(options -> options.ordering(KEY).maxConcurrency(10));
     * }
     * }</pre>
     * Then use the siblings here rather than rebuilding them: {@link #produceMessages(int)} /
     * {@link #produceMessages(int, String)} to feed the topic just created, {@link #getTopic()} for its
     * generated name, {@link #getKcu()} for raw client access, and {@link #setupTopic(String)} /
     * {@link #ensureTopic(String, int)} if a test needs a second or multi-partition topic. Every subclass also
     * inherits the {@link AmbientProbeExtension} flight recorder, so a timeout here arrives pre-diagnosed.
     * <p>
     * The consumer is already wired into the builder handed to {@code options}; it and the built
     * {@link ParallelConsumerOptions} are otherwise scaffolding, so they are deliberately not exposed as fields
     * (several subclasses declare their own {@code consumer}/{@code pc} fields, which inherited ones would
     * silently shadow).
     *
     * @param options the options this test needs, applied to a builder already holding the consumer
     * @return the subscribed processor, ready to {@code poll}
     */
    protected ParallelEoSStreamProcessor<K, V> startPcOnNewTopic(UnaryOperator<ParallelConsumerOptionsBuilder<K, V>> options) {
        setupTopic();

        Consumer<K, V> consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);
        ParallelConsumerOptions<K, V> pcOpts = options.apply(ParallelConsumerOptions.<K, V>builder()
                .consumer(consumer)).build();

        var pc = new ParallelEoSStreamProcessor<K, V>(pcOpts);
        pc.subscribe(UniSets.of(getTopic()));
        return pc;
    }

    protected List<String> produceMessages(int quantity) {
        return produceMessages(quantity, "");
    }

    @SneakyThrows
    protected List<String> produceMessages(int quantity, String prefix) {
        return getKcu().produceMessages(getTopic(), quantity, prefix);
    }

    /**
     * Await an assertion about records flowing through a PC whose consumer may currently be positioned AT
     * THE TAIL of the topic - producing one "nudge" record into the topic before each assertion attempt so
     * that the awaited condition is actually reachable.
     * <p>
     * Why this exists: a consumer with {@code auto.offset.reset=latest} and no committed offset resolves
     * its start position at whatever the log end offset is <b>when the reset executes</b>. Under load,
     * consumer-group bootstrap can take seconds - so a single nudge record produced <i>before</i> an await
     * can be leapfrogged by the reset, leaving the consumer positioned past every record that will ever
     * exist. Any await for "some record arrives" is then unwinnable regardless of its timeout - the
     * failure mode behind the long-running {@code committedOffsetRemoved[latest]} CI flake. Producing the
     * nudge <b>inside</b> the await loop (before the assertion, so it can be observed by the next attempt)
     * makes the condition reachable at any bootstrap latency. See
     * {@code docs/solutions/test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md}.
     * <p>
     * On timeout, logs a self-diagnosis (topic end offset vs group committed offset and nudges sent) so
     * this class of positioning race names itself instead of presenting as a generic empty-collection
     * timeout.
     *
     * <p>
     * <b>SIDE EFFECT - read this before asserting on offsets or record counts.</b> This PRODUCES RECORDS
     * into the topic under test: one per poll interval, so {@code atMost / pollInterval} of them in the
     * worst case, and how many you actually get depends on how loaded the machine is. It is NOT one.
     * <p>
     * Any assertion downstream of this call that needs to know how many records exist must ask the
     * BROKER (e.g. {@code endOffsets}), not derive it from how many the test produced. Deriving it is a
     * real bug this project has already paid for twice: {@code committedOffsetRemoved} carried
     * {@code producedCount + 1 // run sends one} and a {@code TO_PRODUCE + 2} search window, both written
     * when this helper nudged once up front. Moving the nudge inside the await made the count unbounded,
     * the arithmetic silently wrong, and the test an intermittent CI failure that was quarantined rather
     * than diagnosed. See {@code docs/plans/2026-08-05-001-investigate-committedoffset-latest-reflake.md}.
     *
     * @param nudgeCounter incremented for each nudge record produced. Both original callers passed one and
     *                     never read it, which is how the count went unnoticed - if you are asserting
     *                     anything about the contents of this topic, this number is load-bearing, not
     *                     decoration.
     */
    protected void awaitWithTopicNudge(ParallelConsumer<?, ?> pc,
                                       Duration pollInterval,
                                       Duration atMost,
                                       AtomicLong nudgeCounter,
                                       ThrowingRunnable assertion) {
        // correlation marker: ties this await to the consumer's own (pcId-tagged) client logs - the line
        // that let the nudge-race capture be attributed to the right instance among 16 forks
        log.info("awaitWithTopicNudge START: topic={} groupId={} atMost={}", getTopic(), getKcu().getGroupId(), atMost);
        try {
            Awaitility.await()
                    .pollInterval(pollInterval)
                    .atMost(atMost)
                    .failFast(pc::isClosedOrFailed)
                    .untilAsserted(() -> {
                        // nudge FIRST: must go before the failing assertion, otherwise it is never reached
                        getKcu().produceMessages(getTopic(), 1, "nudge-");
                        nudgeCounter.incrementAndGet();
                        assertion.run();
                    });
        } catch (ConditionTimeoutException timeout) {
            logTailPositionDiagnosis(nudgeCounter.get());
            throw timeout;
        }
    }

    /**
     * Best-effort diagnosis for {@link #awaitWithTopicNudge}: compares what the broker holds against what
     * the group has committed, to make offset-reset positioning races self-identifying in CI logs.
     */
    private void logTailPositionDiagnosis(long nudgesSent) {
        try {
            var tp = new TopicPartition(getTopic(), 0);
            long endOffset = getKcu().getAdmin()
                    .listOffsets(UniMaps.of(tp, OffsetSpec.latest()))
                    .partitionResult(tp).get(5, TimeUnit.SECONDS).offset();
            var committed = getKcu().getAdmin()
                    .listConsumerGroupOffsets(getKcu().getGroupId())
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                    .get(tp);
            log.error("awaitWithTopicNudge timed out. Diagnosis: topic {} end offset={}, group '{}' committed={}, " +
                            "nudges sent={}. If the consumer saw nothing while the end offset kept advancing, suspect " +
                            "an offset reset (e.g. LATEST) that resolved PAST the records the test expected it to see.",
                    getTopic(), endOffset, getKcu().getGroupId(), committed, nudgesSent);
        } catch (Exception diagnosisProblem) {
            log.warn("awaitWithTopicNudge timed out and the tail-position diagnosis itself failed", diagnosisProblem);
        }
    }

}
