package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * The consumer-configuration option of {@link ParallelConsumerOptions}: how it combines with the instance option,
 * what validation says about the two keys PC cannot choose for the caller and the one it will not let them choose,
 * and that the configuration never reaches {@link ParallelConsumerOptions#toString()} (astubbs#504).
 * <p>
 * The mirror of {@link ParallelConsumerOptionsProducerConfigTest}, deliberately: the two options are the same shape,
 * so the two suites should read as the same suite.
 * <p>
 * <b>Proved by sabotage, not by assumption</b>, per the test-tree rules. Each refusal in
 * {@code ParallelConsumerOptions.consumerSourceValidation()} was removed in turn, and each removal reddens only the
 * test that owns it: deleting the both-supplied throw reddens
 * {@link #supplyingBothAnInstanceAndConfigurationFailsNamingBoth}; deleting the neither-supplied throw reddens
 * {@link #supplyingNeitherAnInstanceNorConfigurationFailsNamingBoth}; making {@code requireDeserialiser} a no-op
 * reddens both deserializer tests and nothing else; dropping the {@code refuseAutoCommit()} call reddens both
 * auto-commit-true tests. The last mutation is the one worth recording: narrowing the refusal from
 * {@code Boolean.parseBoolean(value.toString())} to a boxed-{@link Boolean} check reddens
 * {@link #anExplicitAutoCommitOfTheStringTrueIsRefusedToo} <em>alone</em>, so the string spelling a properties file
 * produces is genuinely covered rather than incidentally passing.
 */
class ParallelConsumerOptionsConsumerConfigTest {

    @SuppressWarnings("unchecked")
    private final Consumer<String, String> consumerInstance = mock(Consumer.class);

    /**
     * Everything PC insists on and nothing else, so a test that removes one key is removing the only copy of it.
     */
    private static Map<String, Object> minimalConsumerConfig() {
        var config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return config;
    }

    private static ParallelConsumerOptions<String, String> optionsWith(Map<String, Object> consumerConfig) {
        return ParallelConsumerOptions.<String, String>builder()
                .consumerConfig(consumerConfig)
                .build();
    }

    @Test
    void supplyingBothAnInstanceAndConfigurationFailsNamingBoth() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerInstance)
                .consumerConfig(minimalConsumerConfig())
                .build();

        var thrown = assertThrows(IllegalArgumentException.class, options::validate);

        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.consumer);
        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.consumerConfig);
    }

    /**
     * A consumer remains the one required option; what changed is that there are now two ways to supply it, so the
     * refusal has to name both rather than only the instance.
     */
    @Test
    void supplyingNeitherAnInstanceNorConfigurationFailsNamingBoth() {
        var options = ParallelConsumerOptions.<String, String>builder().build();

        var thrown = assertThrows(IllegalArgumentException.class, options::validate);

        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.consumer);
        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.consumerConfig);
    }

    @Test
    void configurationAloneValidatesAndIsNotTheInstancePath() {
        var options = optionsWith(minimalConsumerConfig());

        options.validate();

        assertThat(options.isConsumerInstanceSupplied()).isFalse();
    }

    @Test
    void anInstanceAloneStillValidatesAndIsTheInstancePath() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerInstance)
                .build();

        options.validate();

        assertThat(options.isConsumerInstanceSupplied()).isTrue();
    }

    /**
     * PC never decodes a record itself, so there is no pair it could default to - a byte-array default would be
     * right only for a caller whose key and value types are byte arrays.
     */
    @Test
    void aMissingKeyDeserialiserIsRefusedNamingTheKeyAndBothWaysOut() {
        var config = minimalConsumerConfig();
        Object ignoredRemoved = config.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);

        var thrown = assertThrows(IllegalArgumentException.class, optionsWith(config)::validate);

        assertThat(thrown).hasMessageThat().contains(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.consumerConfig);
        assertWithMessage("the other way out is a finished instance, so the message should say so")
                .that(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.consumer);
    }

    @Test
    void aMissingValueDeserialiserIsRefusedNamingTheKey() {
        var config = minimalConsumerConfig();
        Object ignoredRemoved = config.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        var thrown = assertThrows(IllegalArgumentException.class, optionsWith(config)::validate);

        assertThat(thrown).hasMessageThat().contains(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }

    /**
     * Refused rather than overridden: PC commits offsets itself, so the caller asked for something incompatible and
     * silently doing the opposite of what their map says is how that survives to the day somebody reads the map.
     */
    @Test
    void anExplicitAutoCommitOfTrueIsRefused() {
        var config = minimalConsumerConfig();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        var thrown = assertThrows(IllegalArgumentException.class, optionsWith(config)::validate);

        assertThat(thrown).hasMessageThat().contains(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.consumerConfig);
    }

    /**
     * Kafka's own client accepts either spelling for a boolean property, so a refusal that only saw the boxed
     * {@link Boolean} would wave through the spelling a properties file produces.
     */
    @Test
    void anExplicitAutoCommitOfTheStringTrueIsRefusedToo() {
        var config = minimalConsumerConfig();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        var thrown = assertThrows(IllegalArgumentException.class, optionsWith(config)::validate);

        assertThat(thrown).hasMessageThat().contains(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    }

    /**
     * A caller who spelled out what PC would have done anyway has asked for nothing incompatible.
     */
    @Test
    void anExplicitAutoCommitOfFalseIsAccepted() {
        var config = minimalConsumerConfig();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        optionsWith(config).validate();
    }

    /**
     * The map is where credentials live (SASL JAAS, keystore passwords), and the options are logged at start-up.
     */
    @Test
    void toStringRendersNoneOfTheConfiguration() {
        var config = minimalConsumerConfig();
        config.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"svc\" password=\"hunter2\";");

        String rendered = optionsWith(config).toString();

        assertWithMessage("no credential material may appear in toString").that(rendered).doesNotContain("hunter2");
        assertThat(rendered).doesNotContain("broker:9092");
    }
}
