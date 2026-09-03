package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The configuration path at the module (astubbs#225): what PC builds from the map, what the construction seam
 * receives, and that a producer PC built for a start-up that fails is closed rather than leaked.
 */
class PcBuiltProducerTest {

    @SuppressWarnings("unchecked")
    private static Consumer<String, String> consumerInGroup() {
        Consumer<String, String> consumer = mock(Consumer.class);
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("app"));
        when(consumer.paused()).thenReturn(UniSets.of());
        return consumer;
    }

    /**
     * A literal address, not a hostname: the default seam builds a real KafkaProducer from this map, whose
     * constructor resolves bootstrap.servers, and a hostname resolves on some networks and not on CI.
     */
    private static Map<String, Object> realProducerConfig(String transactionalId) {
        var config = new java.util.HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        if (transactionalId != null) {
            config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        return config;
    }

    private static ParallelConsumerOptions<String, String> optionsWith(Map<String, Object> producerConfig, CommitMode mode) {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerInGroup())
                .producerConfig(producerConfig)
                .commitMode(mode)
                .build();
    }

    /** A module whose construction seam is the given function, so a test can see the map or substitute the producer. */
    private static PCModule<String, String> moduleBuildingWith(ParallelConsumerOptions<String, String> options,
                                                              Function<Map<String, Object>, Producer<String, String>> seam) {
        return new PCModule<>(options) {
            @Override
            protected Producer<String, String> buildProducer(Map<String, Object> producerConfig) {
                return seam.apply(producerConfig);
            }
        };
    }

    @Test
    void theDefaultSeamBuildsATransactionalKafkaProducerFromTheMapAsGiven() {
        var module = new PCModule<>(optionsWith(realProducerConfig("pc-test-" + UUID.randomUUID()), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER));

        var wrapper = module.producerWrap();

        try {
            assertThat(wrapper.isConfiguredForTransactions()).isTrue();
            assertThat(wrapper.isMockProducer()).isFalse();
        } finally {
            wrapper.close(Duration.ZERO);
        }
    }

    @Test
    void theDefaultSeamBuildsANonTransactionalKafkaProducerWhenTheMapCarriesNoId() {
        var module = new PCModule<>(optionsWith(realProducerConfig(null), CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS));

        var wrapper = module.producerWrap();

        try {
            assertThat(wrapper.isConfiguredForTransactions()).isFalse();
        } finally {
            wrapper.close(Duration.ZERO);
        }
    }

    @Test
    void theSeamReceivesACopyOfTheCallersMapWithEveryKey() {
        Map<String, Object> callers = UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "callers-id");
        var received = new java.util.concurrent.atomic.AtomicReference<Map<String, Object>>();
        var module = moduleBuildingWith(optionsWith(callers, CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER), config -> {
            received.set(config);
            return new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        });

        var wrapper = module.producerWrap();

        assertThat(received.get()).containsExactlyEntriesIn(callers);
        assertWithMessage("a copy, so an override editing it cannot edit the options")
                .that(received.get()).isNotSameInstanceAs(callers);
        assertThat(wrapper.isConfiguredForTransactions()).isTrue();
        assertWithMessage("one producer per module").that(module.producerWrap()).isSameInstanceAs(wrapper);
    }

    @Test
    void theInstancePathWrapsTheCallersProducerAndBuildsNothing() {
        @SuppressWarnings("unchecked")
        Producer<String, String> instance = mock(Producer.class);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerInGroup())
                .producer(instance)
                .build();
        var module = moduleBuildingWith(options, config -> {
            throw new AssertionError("the instance path must not build a producer");
        });

        var wrapper = module.producerWrap();

        assertThat(wrapper.isConfiguredForTransactions()).isFalse();
        wrapper.send(null);
        verify(instance).send(null);
    }

    /**
     * The manager's constructor initialises transactions; when that throws at start-up, the producer PC built for it
     * belongs to nobody - the processor is never returned to the caller - so it is closed rather than leaked one per
     * start-up attempt.
     */
    @Test
    void aProducerBuiltForAManagerThatFailsToConstructIsClosed() {
        var producer = spy(new MockProducer<>(true, new StringSerializer(), new StringSerializer()));
        doThrow(new KafkaException("coordinator not available")).when(producer).initTransactions();
        var module = moduleBuildingWith(optionsWith(realProducerConfig("pc-test"), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER), config -> producer);

        assertThrows(KafkaException.class, module::producerManager);

        verify(producer).close(any(Duration.class));
    }

    /**
     * The wrapper's transactional discovery reads a {@code KafkaProducer} field reflectively, and a subclass does not
     * declare it - so a producer built as a subclass (a user's instrumenting subclass, say) fails at the wrapper, one
     * frame before the manager guard, with nobody else holding the producer PC just built. Found by the review of
     * astubbs#426.
     */
    @Test
    void aProducerBuiltForAWrapperThatFailsToConstructIsClosed() {
        var closed = new java.util.concurrent.atomic.AtomicBoolean();
        var module = moduleBuildingWith(optionsWith(realProducerConfig("pc-test"), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER),
                config -> new org.apache.kafka.clients.producer.KafkaProducer<String, String>(config) {
                    @Override
                    public void close(Duration timeout) {
                        closed.set(true);
                        super.close(timeout);
                    }
                });

        assertThrows(NoSuchFieldException.class, module::producerWrap);

        assertWithMessage("the built producer is PC's alone, so PC closes it").that(closed.get()).isTrue();
    }

    /**
     * The close is best effort: a producer that fails to close as well must not hide the failure that made PC close
     * it, which is the one the caller has to act on.
     */
    @Test
    void aBuiltProducerWhoseCloseAlsoFailsStillSurfacesTheConstructionFailure() {
        var module = moduleBuildingWith(optionsWith(realProducerConfig("pc-test"), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER),
                config -> new org.apache.kafka.clients.producer.KafkaProducer<String, String>(config) {
                    @Override
                    public void close(Duration timeout) {
                        super.close(timeout);
                        throw new IllegalStateException("close failed too");
                    }
                });

        var thrown = assertThrows(NoSuchFieldException.class, module::producerWrap);

        assertThat(thrown).hasMessageThat().contains("transactionManager");
    }

    /**
     * The caller's own instance is the caller's to close: they may hold it, and they never handed PC its lifecycle.
     */
    @Test
    void theCallersInstanceIsNotClosedWhenTheManagerFailsToConstruct() {
        var instance = spy(new MockProducer<>(true, new StringSerializer(), new StringSerializer()));
        doThrow(new KafkaException("coordinator not available")).when(instance).initTransactions();
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerInGroup())
                .producer(instance)
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();
        var module = new PCModule<>(options);

        assertThrows(KafkaException.class, module::producerManager);

        verify(instance, never()).close(any(Duration.class));
        verify(instance, never()).close();
    }
}
