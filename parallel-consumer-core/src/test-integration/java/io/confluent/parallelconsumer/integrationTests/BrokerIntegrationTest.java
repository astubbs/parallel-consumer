
/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 */

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.csid.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

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

    protected List<String> produceMessages(int quantity) {
        return produceMessages(quantity, "");
    }

    @SneakyThrows
    protected List<String> produceMessages(int quantity, String prefix) {
        return getKcu().produceMessages(getTopic(), quantity, prefix);
    }

}
