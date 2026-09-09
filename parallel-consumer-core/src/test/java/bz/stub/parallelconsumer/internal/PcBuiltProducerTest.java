package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.KafkaException;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.ArgumentMatchers.any;
import java.time.Duration;
import static org.mockito.Mockito.verify;
import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ProducerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.util.ArrayList;
import java.util.Arrays;
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
        // A literal address, not a hostname: the default factory builds a real KafkaProducer from this map, whose
        // constructor resolves bootstrap.servers. `broker` resolved on the author's network and nowhere else, and
        // the two construction cases failed on every CI runner with "Failed to construct kafka producer".
        return UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
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
        var ignoredReplacementA1 = moduleA.replacementProducerWrap().get().build();
        var ignoredReplacementA2 = moduleA.replacementProducerWrap().get().build();
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

    /**
     * The id is resolved once per instance, so the WARN a caller-set id earns fires once, not once per rebuild: an
     * operator watching a recovery loop sees the recovery lines, not a repeat of a start-up warning.
     */
    @Test
    void aCallerSetIdIsWarnedAboutOnceAcrossAStartAndTwoReplacements() {
        String callersId = "callers-id-" + java.util.UUID.randomUUID();
        var module = moduleWith(capturingFactory,
                UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092", ProducerConfig.TRANSACTIONAL_ID_CONFIG, callersId),
                CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(TransactionalIdDerivation.class);
        var appender = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try {
            var ignoredInitial = module.producerWrap();
            var source = module.replacementProducerWrap().get();
            var ignoredFirstReplacement = source.build();
            var ignoredSecondReplacement = source.build();
        } finally {
            logger.detachAppender(appender);
        }

        long warnsNamingTheCallersId = appender.list.stream()
                .filter(event -> event.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.WARN))
                .filter(event -> event.getFormattedMessage().contains(callersId))
                .count();
        assertThat(warnsNamingTheCallersId).isEqualTo(1);
        assertThat(handedConfigs).hasSize(3);
        assertWithMessage("every build received the same derived id")
                .that(handedConfigs.stream().map(config -> config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).distinct().count()).isEqualTo(1);
    }

    @Test
    void aFactoryReturningTheSameInstanceTwiceIsRejected() {
        var shared = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        ProducerFactory<String, String> cachingFactory = config -> shared;
        var module = moduleWith(cachingFactory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        var ignoredFirst = module.producerWrap(); // the first call is legitimate; the repeat is the defect

        var thrown = assertThrows(ProducerFactoryContractException.class, () -> module.replacementProducerWrap().get().build());

        assertThat(thrown).hasMessageThat().contains("ProducerFactory");
        assertThat(thrown).hasMessageThat().contains("new");
    }

    /**
     * A pool alternating two instances passes a last-only identity check on its third call, then fails
     * initTransactions on a producer PC has already closed - forever, as a retriable failure. Every instance the
     * factory ever returned is remembered.
     */
    @Test
    void aFactoryReturningAnEarlierInstanceAgainIsRejectedNotOnlyTheImmediatelyPreviousOne() {
        var a = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        var b = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        var pool = new ArrayList<>(Arrays.asList(a, b, a));
        ProducerFactory<String, String> poolingFactory = config -> pool.remove(0);
        var module = moduleWith(poolingFactory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        var ignoredFirst = module.producerWrap(); // a
        var ignoredSecond = module.replacementProducerWrap().get().build(); // b - legitimate

        var thrown = assertThrows(ProducerFactoryContractException.class, () -> module.replacementProducerWrap().get().build());

        assertThat(thrown).hasMessageThat().contains("already returned");
    }

    /**
     * A source of further producers exists only where PC built the first one: the instance path carries no
     * configuration to build from. Each build asks the factory again for a fresh producer from the same resolved
     * map, the derived transactional id with it, so a replacement can be initialised under the id that fences the
     * producer it replaces.
     */
    @Test
    void theConfigurationPathOffersAReplacementSourceThatBuildsAFreshProducerEachTimeUnderTheSameId() {
        var built = new ArrayList<MockProducer<String, String>>();
        ProducerFactory<String, String> factory = config -> {
            handedConfigs.add(new HashMap<>(config));
            var producer = new MockProducer<String, String>(true, new StringSerializer(), new StringSerializer());
            built.add(producer);
            return producer;
        };
        var module = moduleWith(factory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        var initial = module.producerWrap();

        var source = module.replacementProducerWrap();

        assertThat(source).isPresent();
        assertThat(source.get().getTransactionalId()).startsWith(TransactionalIdDerivation.prefixFor(GROUP));
        var first = source.get().build();
        var second = source.get().build();
        assertWithMessage("three producers built: the initial one and one per build").that(built).hasSize(3);
        assertThat(first).isNotSameInstanceAs(initial);
        assertThat(second).isNotSameInstanceAs(first);
        assertThat(first.isConfiguredForTransactions()).isTrue();
        assertWithMessage("every build received the same derived id")
                .that(handedConfigs.stream().map(config -> config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).distinct().count()).isEqualTo(1);
    }

    @Test
    void theReplacementSourceCarriesNoIdInAConsumerCommitMode() {
        ProducerFactory<String, String> factory = config -> new MockProducer<>(false, new StringSerializer(), new StringSerializer());
        var module = moduleWith(factory, minimalConfig(), CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS);

        var source = module.replacementProducerWrap();

        assertThat(source).isPresent();
        assertThat(source.get().getTransactionalId()).isNull();
    }

    /**
     * The start-up build is the path every caller already has, and its failures reach whoever is constructing PC -
     * so a configuration the default factory's client refuses fails as the client refuses it, the type and message
     * intact, not renamed as a failure of "code supplied by user". Found by the review of astubbs#472.
     */
    @Test
    void theStartUpBuildSurfacesAConstructionFailureAsTheFactoryThrewIt() {
        var noSerializers = new HashMap<String, Object>();
        noSerializers.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        var module = new PCModule<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerInGroup(GROUP))
                .producerConfig(noSerializers)
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build());

        var thrown = assertThrows(ConfigException.class, module::producerWrap);

        assertThat(thrown).hasMessageThat().contains(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
    }

    /**
     * A replacement build runs on the recovery path, inside the instance, where an {@link Error} from the factory -
     * a serializer's static initialiser failing, say - would escape every catch and leave the instance RUNNING
     * with its workers parked. So the source wraps whatever the factory throws, and the policy reads the wrapped
     * failure as terminal: the build is not going to succeed on retry.
     */
    @Test
    void aReplacementBuildWrapsWhatTheFactoryThrowsSoAnErrorCannotEscapeTheRecoveryPath() {
        var builds = new java.util.concurrent.atomic.AtomicInteger();
        ProducerFactory<String, String> factory = config -> {
            if (builds.getAndIncrement() == 0) {
                return new MockProducer<>(true, new StringSerializer(), new StringSerializer());
            }
            throw new NoClassDefFoundError("the serializer's static initialiser failed");
        };
        var module = moduleWith(factory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        var ignoredInitial = module.producerWrap(); // built unwrapped, and first
        var source = module.replacementProducerWrap().get();

        var thrown = assertThrows(ExceptionInUserFunctionException.class, source::build);

        assertThat(thrown).hasCauseThat().isInstanceOf(NoClassDefFoundError.class);
        assertWithMessage("an Error from the build is terminal however it arrives")
                .that(ProducerRecoveryPolicy.isTerminalBuildFailure(thrown)).isTrue();
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
        ProducerFactory<String, String> factory = config -> producer;
        var module = moduleWith(factory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);

        assertThrows(KafkaException.class, module::producerManager);

        verify(producer).close(any(Duration.class));
    }

    /**
     * The wrapper's transactional discovery reads a {@code KafkaProducer} field reflectively, and a subclass does not
     * declare it - so a factory returning a subclass (an instrumenting subclass, say) fails at the wrapper, one frame
     * after the contract checks, with nobody else holding the producer it just built. Found by the review of
     * astubbs#426; the rung-1 guard is the try around {@code ProducerWrapper.forPcBuilt} here.
     */
    @Test
    void aProducerBuiltForAWrapperThatFailsToConstructIsClosed() {
        var closed = new java.util.concurrent.atomic.AtomicBoolean();
        ProducerFactory<String, String> subclassingFactory = config -> new KafkaProducer<String, String>(config, new StringSerializer(), new StringSerializer()) {
            @Override
            public void close(Duration timeout) {
                closed.set(true);
                super.close(timeout);
            }
        };
        var module = moduleWith(subclassingFactory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);

        assertThrows(NoSuchFieldException.class, module::producerWrap);

        assertWithMessage("the built producer is PC's alone, so PC closes it").that(closed.get()).isTrue();
    }

    /**
     * The close is best effort: a producer that fails to close as well must not hide the failure that made PC close
     * it, which is the one the caller has to act on.
     */
    @Test
    void aBuiltProducerWhoseCloseAlsoFailsStillSurfacesTheConstructionFailure() {
        ProducerFactory<String, String> factory = config -> new KafkaProducer<String, String>(config, new StringSerializer(), new StringSerializer()) {
            @Override
            public void close(Duration timeout) {
                super.close(timeout);
                throw new IllegalStateException("close failed too");
            }
        };
        var module = moduleWith(factory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);

        var thrown = assertThrows(NoSuchFieldException.class, module::producerWrap);

        assertThat(thrown).hasMessageThat().contains("transactionManager");
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

        var thrown = assertThrows(ProducerFactoryContractException.class, module::producerWrap);

        assertThat(thrown).hasMessageThat().contains("ProducerFactory");
        assertThat(thrown).hasMessageThat().contains(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    }

    /**
     * A substituted id is still transactional, so the presence check alone would pass it - and a replacement
     * initialised under it would fence nothing. A real producer can say which id it was built under, so the check
     * compares the value. Found by the review of astubbs#420.
     */
    @Test
    void aFactoryThatSubstitutesTheTransactionalIdFailsAtConstructionNamingBothIds() {
        ProducerFactory<String, String> substitutingFactory = config -> {
            Map<String, Object> altered = new HashMap<>(config);
            altered.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "the-factorys-own-id");
            return new KafkaProducer<>(altered, new StringSerializer(), new StringSerializer());
        };
        var module = moduleWith(substitutingFactory, minimalConfig(), CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);

        var thrown = assertThrows(ProducerFactoryContractException.class, module::producerWrap);

        assertThat(thrown).hasMessageThat().contains("the-factorys-own-id");
        assertThat(thrown).hasMessageThat().contains(TransactionalIdDerivation.prefixFor(GROUP));
    }

    /**
     * A consumer-commit mode builds a producer that carries no id, so it must not need the consumer's group id to do
     * it: a manual-assignment consumer, or an unstubbed test double, has none. Found by the review of astubbs#420.
     */
    @Test
    void aConsumerCommitModeBuildsWithoutTheConsumersGroupMetadata() {
        @SuppressWarnings("unchecked")
        Consumer<String, String> noGroup = mock(Consumer.class); // groupMetadata() answers null, as an unstubbed double does
        when(noGroup.paused()).thenReturn(UniSets.of());
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(noGroup)
                .producerConfig(minimalConfig())
                .producerFactory(capturingFactory)
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build();

        var wrapper = new PCModule<>(options).producerWrap();

        assertThat(wrapper.isConfiguredForTransactions()).isFalse();
        assertThat(handedConfigs.get(0)).doesNotContainKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
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
