package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * The producer-configuration option of {@link ParallelConsumerOptions}: how it combines with the instance option,
 * what validation says, and that the configuration never reaches {@link ParallelConsumerOptions#toString()}
 * (astubbs#225).
 */
class ParallelConsumerOptionsProducerConfigTest {

    @SuppressWarnings("unchecked")
    private final Consumer<String, String> consumer = mock(Consumer.class);

    @SuppressWarnings("unchecked")
    private final Producer<String, String> producerInstance = mock(Producer.class);

    private static Map<String, Object> minimalProducerConfig() {
        return UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
    }

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

    @Test
    void configurationAloneValidatesInTransactionalModeAndCountsAsAProducer() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producerConfig(minimalProducerConfig())
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        options.validate();

        assertThat(options.isProducerSupplied()).isTrue();
        assertThat(options.isProducerInstanceSupplied()).isFalse();
    }

    @Test
    void anInstanceAloneStillValidatesAndIsTheInstancePath() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producer(producerInstance)
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        options.validate();

        assertThat(options.isProducerSupplied()).isTrue();
        assertThat(options.isProducerInstanceSupplied()).isTrue();
    }

    @Test
    void neitherInstanceNorConfigurationInTransactionalModeStillFailsNamingTheConfigurationOption() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        var thrown = assertThrows(IllegalArgumentException.class, options::validate);

        assertThat(thrown).hasMessageThat().contains("Transaction Producer mode");
        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.producerConfig);
        assertThat(options.isProducerSupplied()).isFalse();
    }

    /**
     * The map is where credentials live (SASL JAAS, keystore passwords), and the options are logged at start-up.
     */
    @Test
    void toStringRendersNoneOfTheConfiguration() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producerConfig(UniMaps.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092",
                        "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"svc\" password=\"hunter2\";"))
                .build();

        String rendered = options.toString();

        assertWithMessage("no credential material may appear in toString").that(rendered).doesNotContain("hunter2");
        assertThat(rendered).doesNotContain("broker:9092");
    }
}
