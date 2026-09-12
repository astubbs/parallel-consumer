package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

/**
 * The consumer-configuration path at the module (astubbs#504): what PC builds from the map, what the construction
 * seam receives, and that a supplied instance still wins and builds nothing.
 * <p>
 * The mirror of {@link PcBuiltProducerTest}, including its close-on-failed-construction cases. This suite used to
 * disclaim those on the premise that nothing can throw between building the consumer and wiring it up - which was
 * wrong, and the review of astubbs/parallel-consumer#506 named the route: the engine's own
 * {@code validateConfiguration()} runs after the module has built the client, so every check in it throws with a
 * live consumer that only the half-built processor could have closed.
 * <p>
 * <b>Proved by sabotage, not by assumption</b>, per the test-tree rules. Five mutations of
 * {@link PCModule#consumer()} and its helpers, and what each showed:
 * <ul>
 *     <li>dropping the {@code enable.auto.commit} {@code put} reddens
 *     {@link #theSeamReceivesACopyOfTheCallersMapWithAutoCommitForcedOff} and
 *     {@link #anExplicitAutoCommitOfFalseReachesTheSeamAsFalse}, and no options test - so the forcing is detected
 *     here and nowhere else;</li>
 *     <li>collapsing the resolution to {@code options().getConsumer()} reddens all four configuration-path tests
 *     and leaves the instance-path one green;</li>
 *     <li>collapsing it the other way, so a supplied instance is ignored, reddens
 *     {@link #theInstancePathHandsBackTheCallersConsumerAndBuildsNothing} alone - which is what makes this suite
 *     evidence that the additive change is behaviour-preserving for every existing caller;</li>
 *     <li>handing the seam {@code options().getConsumerConfig()} instead of a copy reddens
 *     {@link #theSeamReceivesACopyOfTheCallersMapWithAutoCommitForcedOff};</li>
 *     <li>wrapping {@code optionsInstance.getConsumer()} in {@code consumerManager()} again reddens
 *     {@link #theConsumerManagerIsWiredToTheBuiltConsumer} alone, with the null delegate the configuration path
 *     would otherwise have shipped;</li>
 *     <li>deleting the {@code if (this.resolvedConsumer == null)} guard - so every caller gets a brand-new client -
 *     reddens {@link #oneConsumerIsBuiltPerModuleHoweverManyAsk} alone. That mutation used to survive the whole
 *     suite: the memoisation assertion was made against a seam that returned the same instance whatever happened,
 *     so it could not tell one build from two;</li>
 *     <li>removing the {@code closeClientsBuiltByPc} call from the processor's constructor reddens
 *     {@link #aConsumerBuiltForAProcessorThatFailsToConstructIsClosed} alone, and extending it to close a supplied
 *     instance too reddens {@link #theCallersInstanceIsNotClosedWhenTheProcessorFailsToConstruct} alone.</li>
 * </ul>
 */
class PcBuiltConsumerTest {

    /**
     * A literal address, not a hostname: the default seam builds a real KafkaConsumer from this map, whose
     * constructor resolves bootstrap.servers, and a hostname resolves on some networks and not on CI.
     */
    private static Map<String, Object> realConsumerConfig() {
        var config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "pc-test");
        return config;
    }

    private static ParallelConsumerOptions<String, String> optionsWith(Map<String, Object> consumerConfig) {
        return ParallelConsumerOptions.<String, String>builder()
                .consumerConfig(consumerConfig)
                .build();
    }

    /** A module whose construction seam is the given function, so a test can see the map or substitute the consumer. */
    private static PCModule<String, String> moduleBuildingWith(ParallelConsumerOptions<String, String> options,
                                                               Function<Map<String, Object>, Consumer<String, String>> seam) {
        return new PCModule<>(options) {
            @Override
            protected Consumer<String, String> buildConsumer(Map<String, Object> consumerConfig) {
                return seam.apply(consumerConfig);
            }
        };
    }

    @Test
    void theDefaultSeamBuildsARealKafkaConsumerFromTheMapAsGiven() {
        var module = new PCModule<>(optionsWith(realConsumerConfig()));

        var consumer = module.consumer();

        try {
            assertThat(consumer).isInstanceOf(KafkaConsumer.class);
        } finally {
            consumer.close();
        }
    }

    /**
     * The whole point of the seam: a test hands back a {@link MockConsumer} and PC never touches a broker.
     */
    @Test
    void theSeamReceivesACopyOfTheCallersMapWithAutoCommitForcedOff() {
        Map<String, Object> callers = UniMaps.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        var received = new AtomicReference<Map<String, Object>>();
        var substitute = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var module = moduleBuildingWith(optionsWith(callers), config -> {
            received.set(config);
            return substitute;
        });

        var consumer = module.consumer();

        assertThat(consumer).isSameInstanceAs(substitute);
        assertWithMessage("PC commits offsets itself, so the consumer it builds must not")
                .that(received.get()).containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        assertThat(received.get()).containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        assertWithMessage("a copy, so forcing auto-commit off cannot edit the caller's options")
                .that(callers).doesNotContainKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    }

    /**
     * <b>The seam has to hand back a DIFFERENT consumer each time for this to be able to fail.</b> Against a seam
     * that returns one constant instance, asking twice and comparing by identity is green whether the memo guard is
     * there or not - which is what this assertion did until the review of astubbs/parallel-consumer#506 traced it.
     * So the seam counts its calls, and the count is the assertion.
     * <p>
     * What the lost guard would do in production is build a brand-new {@code KafkaConsumer} - another broker
     * connection, another member of the group - on every {@code consumer()} call, of which the engine makes several
     * during start-up alone.
     */
    @Test
    void oneConsumerIsBuiltPerModuleHoweverManyAsk() {
        var builds = new AtomicInteger();
        var module = moduleBuildingWith(optionsWith(realConsumerConfig()), config -> {
            int ignoredCallNumber = builds.incrementAndGet();
            return new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        });

        var first = module.consumer();
        var second = module.consumer();

        assertWithMessage("the second ask must not reach the construction seam").that(builds.get()).isEqualTo(1);
        assertWithMessage("...and must be handed the client the first ask built").that(second).isSameInstanceAs(first);
    }

    /**
     * An explicit false is the one value PC replaces with itself, so the map the seam sees is identical either way.
     */
    @Test
    void anExplicitAutoCommitOfFalseReachesTheSeamAsFalse() {
        var config = realConsumerConfig();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        var received = new AtomicReference<Map<String, Object>>();
        var module = moduleBuildingWith(optionsWith(config), seen -> {
            received.set(seen);
            return new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        });

        Consumer<String, String> ignoredConsumer = module.consumer();

        assertThat(received.get()).containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }

    @Test
    void theInstancePathHandsBackTheCallersConsumerAndBuildsNothing() {
        @SuppressWarnings("unchecked")
        Consumer<String, String> instance = mock(Consumer.class);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(instance)
                .build();
        var module = moduleBuildingWith(options, config -> {
            throw new AssertionError("the instance path must not build a consumer");
        });

        assertThat(module.consumer()).isSameInstanceAs(instance);
    }

    /**
     * The consumer the rest of the engine polls through is the built one - if the manager reached past
     * {@link PCModule#consumer()} to the options, the configuration path would wrap a null. Proved through the
     * cache the manager primes from its consumer at construction, rather than by identity, so the assertion fails
     * if the wiring is right but the manager is holding a different client.
     */
    @Test
    void theConsumerManagerIsWiredToTheBuiltConsumer() {
        var partition = new TopicPartition("pc-built-consumer-test", 0);
        var substitute = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        substitute.assign(UniLists.of(partition));
        substitute.pause(UniSets.of(partition));
        var module = moduleBuildingWith(optionsWith(realConsumerConfig()), config -> substitute);

        var manager = module.consumerManager();

        assertWithMessage("the manager primed its paused-partition cache from the substituted consumer")
                .that(manager.getPausedPartitionSize()).isEqualTo(1);
        assertThat(module.consumer()).isSameInstanceAs(substitute);
    }

    /**
     * A consumer that already has a subscription of its own: PC manages subscriptions itself, so the engine's
     * {@code checkNotSubscribed()} refuses it - and that check runs in {@code validateConfiguration()}, which the
     * constructor reaches only <em>after</em> the module has built the client. So the throw happens with a live
     * consumer that nobody outside the half-built processor is holding: the caller gets an exception and no handle to
     * clean up with, and a start-up retry loop leaks one consumer, one broker connection and one group member per
     * attempt.
     * <p>
     * Not a {@code MockConsumer}, deliberately - {@code checkNotSubscribed} skips those by design, so a mock is the
     * only way to reach the refusal this test needs.
     */
    @Test
    void aConsumerBuiltForAProcessorThatFailsToConstructIsClosed() {
        Consumer<String, String> built = alreadySubscribedConsumer();
        var options = optionsWith(realConsumerConfig());
        var module = moduleBuildingWith(options, config -> built);

        var thrown = assertThrows(IllegalStateException.class, () -> new ParallelEoSStreamProcessor<>(options, module));

        assertThat(thrown).hasMessageThat().contains("subscription must be managed");
        assertWithMessage("the built consumer is PC's alone, so PC closes it rather than leaking it")
                .that(closeCalls(built)).isGreaterThan(0);
    }

    /**
     * The caller's own instance is the caller's to close: they may still hold it, and they never handed PC its
     * lifecycle. The same failure, the opposite outcome.
     */
    @Test
    void theCallersInstanceIsNotClosedWhenTheProcessorFailsToConstruct() {
        Consumer<String, String> instance = alreadySubscribedConsumer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(instance)
                .build();
        var module = new PCModule<>(options);

        var ignoredThrown = assertThrows(IllegalStateException.class,
                () -> new ParallelEoSStreamProcessor<>(options, module));

        assertWithMessage("a consumer PC did not build is not PC's to close")
                .that(closeCalls(instance)).isEqualTo(0);
    }

    /**
     * A consumer that reports a subscription, so the engine's {@code checkNotSubscribed()} refuses it - the one
     * refusal that fires after the module has already built the client.
     */
    @SuppressWarnings("unchecked")
    private static Consumer<String, String> alreadySubscribedConsumer() {
        Consumer<String, String> consumer = mock(Consumer.class);
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("pc-test"));
        when(consumer.paused()).thenReturn(UniSets.of());
        when(consumer.subscription()).thenReturn(UniSets.of("a-topic-the-caller-subscribed-to"));
        when(consumer.assignment()).thenReturn(UniSets.of());
        return consumer;
    }

    /** Either close overload counts - the question is whether PC closed the client at all. */
    private static int closeCalls(Consumer<String, String> consumer) {
        return (int) mockingDetails(consumer).getInvocations().stream()
                .filter(invocation -> "close".equals(invocation.getMethod().getName()))
                .count();
    }
}
