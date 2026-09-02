package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ProducerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * R2, R4, R6 at the module: what the factory receives, how often it is called, and that a replacement is only
 * available on the configuration path (KTD2, KTD8).
 */
class PcBuiltProducerTest {

    private static final String GROUP = "app";

    /** Every configuration the factory was handed, in order. */
    private final List<Map<String, Object>> handedConfigs = new ArrayList<>();

    private final ProducerFactory<String, String> capturingFactory = config -> {
        handedConfigs.add(new HashMap<>(config));
        return new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    };

    @SuppressWarnings("unchecked")
    private static Consumer<String, String> consumerInGroup(String groupId) {
        Consumer<String, String> consumer = mock(Consumer.class);
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata(groupId));
        when(consumer.paused()).thenReturn(UniSets.of());
        return consumer;
    }

    private PCModule<String, String> moduleWith(ProducerFactory<String, String> factory, Map<String, Object> producerConfig, CommitMode mode) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerInGroup(GROUP))
                .producerConfig(producerConfig)
                .producerFactory(factory)
                .commitMode(mode)
                .build();
        return new PCModule<>(options);
    }

    private static Map<String, Object> minimalConfig() {
        return UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
    }

    /**
     * Covers AE4.
     */
    @Test
    void twoModulesDeriveDifferentIdsAndOneModuleReusesItsIdForEveryReplacement() {
        var moduleA = moduleWith(capturingFactory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        var moduleB = moduleWith(capturingFactory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);

        // the wrappers themselves are not the subject - what the factory was handed is
        var ignoredInitialA = moduleA.producerWrap();
        var ignoredReplacementA1 = moduleA.replacementProducerWrap().get().get();
        var ignoredReplacementA2 = moduleA.replacementProducerWrap().get().get();
        var ignoredInitialB = moduleB.producerWrap();

        assertThat(handedConfigs).hasSize(4);
        String idA = (String) handedConfigs.get(0).get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        assertThat(idA).startsWith(TransactionalIdDerivation.prefixFor(GROUP));
        assertThat(handedConfigs.get(1).get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isEqualTo(idA);
        assertThat(handedConfigs.get(2).get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isEqualTo(idA);
        String idB = (String) handedConfigs.get(3).get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        assertThat(idB).startsWith(TransactionalIdDerivation.prefixFor(GROUP));
        assertWithMessage("two instances of the same application never share an id").that(idB).isNotEqualTo(idA);
    }

    /**
     * Covers AE4.
     */
    @Test
    void aCallerSetIdIsAbsentFromWhatTheFactoryReceives() {
        var module = moduleWith(capturingFactory,
                UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092", ProducerConfig.TRANSACTIONAL_ID_CONFIG, "callers-id"),
                CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);

        var ignoredWrapper = module.producerWrap(); // what the factory received is the subject

        assertThat(handedConfigs.get(0).get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).isNotEqualTo("callers-id");
        assertThat((String) handedConfigs.get(0).get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).startsWith("pc-3-app-");
    }

    @Test
    void inANonTransactionalModeTheFactoryReceivesNoTransactionalId() {
        var module = moduleWith(capturingFactory,
                UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092", ProducerConfig.TRANSACTIONAL_ID_CONFIG, "callers-id"),
                CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS);

        var wrapper = module.producerWrap();

        assertThat(handedConfigs.get(0)).doesNotContainKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        assertThat(wrapper.isConfiguredForTransactions()).isFalse();
    }

    @Test
    void aFactoryReturningTheSameInstanceTwiceIsRejected() {
        var shared = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        ProducerFactory<String, String> cachingFactory = config -> shared;
        var module = moduleWith(cachingFactory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        var ignoredFirst = module.producerWrap(); // the first call is legitimate; the repeat is the defect

        var thrown = assertThrows(IllegalStateException.class, () -> module.replacementProducerWrap().get().get());

        assertThat(thrown).hasMessageThat().contains("ProducerFactory");
        assertThat(thrown).hasMessageThat().contains("new");
    }

    @Test
    void theInstancePathWrapsTheCallersProducerAndOffersNoReplacement() {
        @SuppressWarnings("unchecked")
        Producer<String, String> instance = mock(Producer.class);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerInGroup(GROUP))
                .producer(instance)
                .build();
        var module = new PCModule<>(options);

        var wrapper = module.producerWrap();

        assertThat(module.replacementProducerWrap()).isEmpty();
        assertThat(wrapper.isConfiguredForTransactions()).isFalse();
        assertThat(handedConfigs).isEmpty();
    }

    /**
     * KTD8: the factory contract is checked at construction, not discovered at the first transactional call.
     */
    @Test
    void aFactoryThatDropsTheTransactionalIdFailsAtConstructionNamingTheContract() {
        ProducerFactory<String, String> droppingFactory = config -> {
            Map<String, Object> without = new HashMap<>(config);
            without.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            return new KafkaProducer<>(without, new StringSerializer(), new StringSerializer());
        };
        var module = moduleWith(droppingFactory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);

        var thrown = assertThrows(IllegalArgumentException.class, module::producerWrap);

        assertThat(thrown).hasMessageThat().contains("ProducerFactory");
        assertThat(thrown).hasMessageThat().contains(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    }

    @Test
    void aFactoryThatHonoursTheMapPassesTheConstructionCheck() {
        ProducerFactory<String, String> honestFactory = config -> new KafkaProducer<>(config, new StringSerializer(), new StringSerializer());
        var module = moduleWith(honestFactory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);

        var wrapper = module.producerWrap();

        assertThat(wrapper.isConfiguredForTransactions()).isTrue();
        wrapper.close();
    }
}
