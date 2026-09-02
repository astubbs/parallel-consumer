package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * The producer-configuration path of {@link ParallelConsumerOptions}: how it combines with the deprecated instance
 * option, what validation says, and what the options render when logged (astubbs#225, R1, R3, R7, R16, R17, R19,
 * R21).
 */
class ParallelConsumerOptionsProducerConfigTest {

    @SuppressWarnings("unchecked")
    private final Consumer<String, String> consumer = mock(Consumer.class);

    @SuppressWarnings("unchecked")
    private final Producer<String, String> producerInstance = mock(Producer.class);

    private static Map<String, Object> minimalProducerConfig() {
        return UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
    }

    /**
     * Covers AE6.
     */
    @Test
    void supplyingBothAnInstanceAndConfigurationFailsNamingBoth() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producer(producerInstance)
                .producerConfig(minimalProducerConfig())
                .build();

        var thrown = assertThrows(IllegalArgumentException.class, options::validate);

        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.producer);
        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.producerConfig);
    }

    /**
     * Covers AE5, the validation half: the one WARN the deprecated path gets, at validation, naming the remedy.
     */
    @Test
    void anInstanceAloneLogsOneWarnNamingTheReplacementAndTheRemovalRelease() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producer(producerInstance)
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        List<ILoggingEvent> warns = captureWarns(() -> options.validate());

        assertThat(warns).hasSize(1);
        String message = warns.get(0).getFormattedMessage();
        assertThat(message).contains(ParallelConsumerOptions.Fields.producerConfig);
        assertThat(message).contains(ParallelConsumerOptions.Fields.producerFactory);
        assertThat(message).contains("cannot build another producer");
        assertThat(message).contains(ParallelConsumerOptions.PRODUCER_INSTANCE_REMOVAL_RELEASE);
        assertThat(options.isProducerSupplied()).isTrue();
    }

    @Test
    void configurationAloneValidatesInTransactionalModeAndCountsAsAProducer() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producerConfig(minimalProducerConfig())
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        List<ILoggingEvent> warns = captureWarns(options::validate);

        assertThat(warns).isEmpty();
        assertThat(options.isProducerSupplied()).isTrue();
        assertThat(options.isProducerInstanceSupplied()).isFalse();
    }

    @Test
    void neitherInstanceNorConfigurationInTransactionalModeStillFails() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        var thrown = assertThrows(IllegalArgumentException.class, options::validate);

        assertThat(thrown).hasMessageThat().contains("Transaction Producer mode");
        assertThat(options.isProducerSupplied()).isFalse();
    }

    /**
     * Covers AE7.
     */
    @Test
    void toStringRendersNoSecretValueAndKeepsTheAllowListedOnes() {
        var secretJaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"svc\" password=\"hunter2-jaas\";";
        var secretKeystore = "hunter2-keystore";
        var secretSchemaRegistry = "sr-user:hunter2-sr";
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producerConfig(UniMaps.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092",
                        "sasl.jaas.config", secretJaas,
                        "ssl.keystore.password", secretKeystore,
                        "basic.auth.user.info", secretSchemaRegistry))
                .build();

        String rendered = options.toString();

        assertThat(rendered).contains("broker:9092");
        assertThat(rendered).contains("sasl.jaas.config=<redacted>");
        assertThat(rendered).contains("ssl.keystore.password=<redacted>");
        assertThat(rendered).contains("basic.auth.user.info=<redacted>");
        assertWithMessage("no credential material may appear in toString")
                .that(rendered).doesNotContain("hunter2");
    }

    @Test
    void theDefaultFactoryBuildsAKafkaProducerFromTheConfiguration() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producerConfig(UniMaps.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))
                .build();

        try (Producer<String, String> built = options.getProducerFactory().create(options.getProducerConfig())) {
            assertThat(built).isInstanceOf(KafkaProducer.class);
        }
    }

    private static List<ILoggingEvent> captureWarns(Runnable action) {
        var logger = (Logger) LoggerFactory.getLogger(ParallelConsumerOptions.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try {
            action.run();
        } finally {
            logger.detachAppender(appender);
        }
        // The options logger is shared, and the suite runs test classes concurrently: any other test building
        // options around a producer instance logs the same WARN at the same moment, into this appender. The line is
        // logged synchronously on the validating thread, so the thread name is what separates ours from theirs -
        // seen as "expected 1 but was 2", the same message twice, in a full parallel run only.
        String ownThread = Thread.currentThread().getName();
        return appender.list.stream()
                .filter(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
                .filter(event -> ownThread.equals(event.getThreadName()))
                .collect(Collectors.toList());
    }
}
