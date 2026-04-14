
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
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Antony Stubbs
 */
@Testcontainers
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
     * Mapping: CP major = AK major + 4 (e.g., AK 2.8 → CP 6.2, AK 3.9 → CP 7.9).
     */
    static String deriveCpKafkaImage() {
        String akVersion = org.apache.kafka.common.utils.AppInfoParser.getVersion();
        log.info("Kafka client version detected: {}", akVersion);

        // Parse major.minor from the AK version
        String[] parts = akVersion.split("\\.");
        int akMajor = Integer.parseInt(parts[0]);
        int akMinor = Integer.parseInt(parts[1]);

        // CP major = AK major + 4, CP minor = AK minor
        int cpMajor = akMajor + 4;
        int cpMinor = akMinor;

        String cpImage = "confluentinc/cp-kafka:" + cpMajor + "." + cpMinor + ".0";
        log.info("Using CP Kafka image: {} (derived from AK {})", cpImage, akVersion);
        return cpImage;
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
        NewTopic e1 = new NewTopic(topic, numPartitions, (short) 1);
        CreateTopicsResult topics = kcu.getAdmin().createTopics(UniLists.of(e1));
        try {
            Void all = topics.all().get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            // fine
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return topics;
    }

    protected List<String> produceMessages(int quantity) {
        return produceMessages(quantity, "");
    }

    @SneakyThrows
    protected List<String> produceMessages(int quantity, String prefix) {
        return getKcu().produceMessages(getTopic(), quantity, prefix);
    }

}
