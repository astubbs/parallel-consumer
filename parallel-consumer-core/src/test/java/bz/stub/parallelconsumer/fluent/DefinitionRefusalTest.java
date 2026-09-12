package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import static bz.stub.parallelconsumer.fluent.AfterRetries.park;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers AE7: every definition-time refusal fires with a message naming the offending topic or setting, and no
 * client is constructed by any of them.
 * <p>
 * The "no client" half is the one that cannot be seen from a message, so every case runs
 * {@link ParallelConsumerDefinition#buildOptions} against a {@link RecordingClientRuntime} and asserts it was never
 * asked for anything: a refusal that happened after a socket was opened would still read as a refusal.
 *
 * @see ParallelConsumerDefinition#validate()
 */
class DefinitionRefusalTest extends AbstractFluentEngineTest {

    private ParallelConsumerDefinition define() {
        return ParallelConsumer.connect(props());
    }

    /**
     * Runs the definition as far as it can go without a client, and returns the refusal.
     */
    private IllegalArgumentException refusal(ParallelConsumerDefinition definition) {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> definition.buildOptions(runtime));
        assertThat(runtime.builtNothing()).isTrue();
        return thrown;
    }

    /**
     * U2: a supplied producer and the commit mode must agree at definition time, in both directions - and the
     * producer's mere presence is not evidence that they do.
     * <p>
     * Supplying one used to excuse the transactional definition from every check, on the reasoning that the id is
     * already on the client the caller built. Nothing verified that, so a non-transactional producer under the
     * transactional commit mode passed validation and was refused later from inside the engine's producer manager.
     */
    @Test
    void aNonTransactionalSuppliedProducerIsRefusedUnderTheTransactionalCommitMode() {
        try (Producer<byte[], byte[]> plain = realProducer(null)) {
            var pc = define().withCommitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER).withProducer(plain);
            pc.string("orders").process(context -> Outcome.succeeded());

            assertThat(refusal(pc)).hasMessageThat().contains("can never open a transaction");
        }
    }

    /**
     * The other direction, which is the half nothing caught at definition time either: a transactional producer under
     * a commit mode that never opens a transaction, so the guarantee the caller built it for silently does not apply.
     */
    @Test
    void aTransactionalSuppliedProducerIsRefusedUnderAConsumerCommitMode() {
        try (Producer<byte[], byte[]> transactional = realProducer("refusal-test-transactional")) {
            var pc = define()
                    .withCommitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                    .withProducer(transactional);
            pc.string("orders").process(context -> Outcome.succeeded());

            assertThat(refusal(pc)).hasMessageThat().contains("would silently not apply");
        }
    }

    /**
     * The agreeing case is accepted, so the refusals above are about disagreement and not about supplying a producer.
     */
    @Test
    void aTransactionalSuppliedProducerIsAcceptedUnderTheTransactionalCommitMode() {
        try (Producer<byte[], byte[]> transactional = realProducer("refusal-test-agreeing")) {
            var pc = define().withCommitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER).withProducer(transactional);
            pc.string("orders").process(context -> Outcome.succeeded());

            pc.validate();
        }
    }

    /**
     * A producer whose configuration cannot be read refuses nothing: the probe reads client internals and can
     * decline, and "could not tell" is not "not transactional" - refusing on it would fail a well formed definition
     * for a fact nobody established. A {@code MockProducer} can act as either, which is exactly that case.
     */
    @Test
    void aProducerWhoseConfigurationCannotBeReadIsNotRefused() {
        var pc = define()
                .withCommitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .withProducer(new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer()));
        pc.string("orders").process(context -> Outcome.succeeded());

        pc.validate();
    }

    /**
     * A real producer, built offline - a {@code KafkaProducer} connects on its first send, not in its constructor -
     * so the probe meets the client internals it will actually meet rather than a mock's.
     *
     * @param transactionalId the id to build it with, or null for a plain producer
     */
    private Producer<byte[], byte[]> realProducer(String transactionalId) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        if (transactionalId != null) {
            producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        return new KafkaProducer<>(producerProperties, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Test
    void secondRouteOnARoutedTopicNamesTheTopic() {
        var pc = define();
        pc.string("orders").process(context -> Outcome.succeeded());

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> pc.string("orders").process(context -> Outcome.succeeded()));

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("exactly one");
        assertThat(runtime.builtNothing()).isTrue();
    }

    @Test
    void aTopicSetOverlappingAnotherRouteNamesTheOverlappingTopic() {
        var pc = define();
        pc.string("orders").process(context -> Outcome.succeeded());

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> pc.bytes(Arrays.asList("audit", "orders")));

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().doesNotContain("audit is already");
    }

    @Test
    void aCommitModeOnARouteNamesTheSettingAndTheTopic() {
        var pc = define();
        var route = pc.string("orders");

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> route.commitMode(CommitMode.PERIODIC_CONSUMER_SYNC));

        assertThat(thrown).hasMessageThat().contains("commitMode");
        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("instance-wide");
    }

    @Test
    void perRouteOrderingNamesTheSettingTheTopicAndWhereOrderingIsDeclared() {
        var pc = define();
        var route = pc.string("orders");

        var thrown = assertThrows(IllegalArgumentException.class, () -> route.ordering(ProcessingOrder.PARTITION));

        assertThat(thrown).hasMessageThat().contains("ordering");
        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("withDefaultOrdering");
    }

    @Test
    void aKeyDeserialiserInThePropertiesNamesTheSettingAndTheRouteThatSupersedesIt() {
        Properties properties = props();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        var pc = new ParallelConsumerDefinition(properties);
        pc.string("orders").process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        assertThat(thrown).hasMessageThat().contains("orders");
    }

    @Test
    void aValueDeserialiserInThePropertiesIsRefusedToo() {
        Properties properties = props();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        var pc = new ParallelConsumerDefinition(properties);
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(refusal(pc)).hasMessageThat().contains(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }


    @Test
    void aPatternSubscriptionNamesThePatternAndTheAlternative() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> define().topics(Pattern.compile("orders-.*")));

        assertThat(thrown).hasMessageThat().contains("orders-.*");
        assertThat(thrown).hasMessageThat().contains("topics(Collection)");
    }

    @Test
    void aTransactionalIdUnderANonTransactionalCommitModeNamesBoth() {
        Properties properties = props();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "refusal-test");
        var pc = new ParallelConsumerDefinition(properties);
        pc.string("orders").process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        assertThat(thrown).hasMessageThat().contains(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS.name());
    }

    @Test
    void theTransactionalCommitModeWithNoTransactionalIdNamesTheSetting() {
        var pc = define().withCommitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(refusal(pc)).hasMessageThat().contains(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    }

    @Test
    void aRouteWithNoProcessingFunctionNamesTheTopic() {
        var pc = define();
        pc.string("orders");

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("process");
    }

    @Test
    void aDefinitionWithNoRoutesSaysSo() {
        assertThat(refusal(define())).hasMessageThat().contains("no routes");
    }

    @Test
    void aSecondFunctionOnOneRouteNamesTheTopic() {
        var pc = define();
        var route = pc.string("orders");
        route.process(context -> Outcome.succeeded());

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> route.process(context -> Outcome.succeeded()));

        assertThat(thrown).hasMessageThat().contains("orders");
    }

    /**
     * The two reactions are alternatives, so a stopping policy refuses a park setting where it is written rather
     * than carrying one that could never fire.
     */
    @Test
    void aParkSettingOnAStoppingPolicyNamesTheSetting() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> AfterRetries.stop().thenRetryAfter(Duration.ofSeconds(1)));

        assertThat(thrown).hasMessageThat().contains("thenRetryAfter");
        assertThat(thrown).hasMessageThat().contains("stop()");
    }

    /**
     * A park delay and a cycle count are one setting written as two calls (R27): a delay with no cycles grants no
     * attempt and a cycle count with no delay has no schedule, so either alone is a setting that does nothing.
     */
    @Test
    void aParkCycleCountWithNoDelayNamesTheSettingAndTheTopic() {
        var definition = define();
        definition.string("orders").afterRetries(park().forCycles(3)).process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("forCycles(3)");
        assertThat(thrown).hasMessageThat().contains("thenRetryAfter");
        assertThat(thrown).hasMessageThat().contains("orders");
    }

    @Test
    void aParkDelayWithNoCycleCountNamesTheSettingAndTheTopic() {
        var definition = define();
        definition.string("orders")
                .afterRetries(park().thenRetryAfter(Duration.ofMinutes(5)))
                .process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("thenRetryAfter");
        assertThat(thrown).hasMessageThat().contains("forCycles");
        assertThat(thrown).hasMessageThat().contains("orders");
    }

    /**
     * The same pair declared as the instance default is refused just the same, because every route took a copy of
     * it (R6) - the refusal names the route that carries it.
     */
    @Test
    void theInstanceDefaultParkCycleIsRefusedThroughTheRouteThatCopiedIt() {
        var definition = define().withDefaultAfterRetries(park().forCycles(2));
        definition.string("orders").process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("forCycles(2)");
        assertThat(thrown).hasMessageThat().contains("orders");
    }

    @Test
    void aParkCycleSettingOnAStoppingPolicyNamesTheSetting() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> AfterRetries.stop().thenRetryAfter(Duration.ofMinutes(1)));

        assertThat(thrown).hasMessageThat().contains("thenRetryAfter");
        assertThat(thrown).hasMessageThat().contains("stop()");
    }

    @Test
    void aNonPositiveParkDelayIsRefusedWhereItIsWritten() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> park().thenRetryAfter(Duration.ZERO));

        assertThat(thrown).hasMessageThat().contains("thenRetryAfter");
        assertThat(thrown).hasMessageThat().contains("positive");
    }

    @Test
    void aCycleCountBelowOneIsRefusedWhereItIsWritten() {
        var thrown = assertThrows(IllegalArgumentException.class, () -> park().forCycles(0));

        assertThat(thrown).hasMessageThat().contains("forCycles");
        assertThat(thrown).hasMessageThat().contains("at least one");
    }

    // ---------------------------------------------------------------- retry-forever leaves nothing to react to

    /**
     * The owner's question on astubbs/parallel-consumer#502: what happens when retry-forever meets an
     * after-retries policy? Nothing did - exhaustion is the only thing that consults a policy, and a record that
     * retries forever never exhausts, so the reaction and the park cycles were both inert and the definition said
     * nothing. Either half may be declared at either scope, so all four pairings are covered here, in both
     * reactions.
     */
    @Test
    void theInstanceDefaultPolicyIsRefusedBesideTheInstanceDefaultRetryForever() {
        var definition = define().withDefaultRetryForever().withDefaultAfterRetries(park());
        definition.string("orders").process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("withDefaultRetryForever()");
        assertThat(thrown).hasMessageThat().contains("withDefaultAfterRetries(...)");
        assertThat(thrown).hasMessageThat().contains("nothing to react to");
    }

    /**
     * The stopping reaction is inert on exactly the same terms, which is why the refusal never mentions which
     * reaction was declared: it is the consulting that never happens, not the reacting.
     */
    @Test
    void theStoppingReactionIsRefusedBesideRetryForeverJustAsParkingIs() {
        var definition = define().withDefaultRetryForever().withDefaultAfterRetries(AfterRetries.stop());
        definition.string("orders").process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("nothing to react to");
    }

    @Test
    void aRoutesOwnPolicyIsRefusedBesideItsOwnRetryForever() {
        var definition = define();
        definition.string("orders")
                .retryForever()
                .afterRetries(park())
                .process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("retryForever()");
        assertThat(thrown).hasMessageThat().contains("after-retries policy of its own");
        assertThat(thrown).hasMessageThat().contains("retryLimit(...) on this route instead");
    }

    @Test
    void aRoutesOwnStoppingPolicyIsRefusedBesideItsOwnRetryForever() {
        var definition = define();
        definition.string("orders")
                .retryForever()
                .afterRetries(AfterRetries.stop())
                .process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("retryForever()");
    }

    /**
     * The two halves declared at different scopes are the same mistake, so the refusal names the scope each half
     * came from rather than the route where they met.
     */
    @Test
    void aRoutesRetryForeverIsRefusedBesideTheInstanceDefaultPolicy() {
        var definition = define().withDefaultAfterRetries(park());
        definition.string("orders").retryForever().process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("retryForever()");
        assertThat(thrown).hasMessageThat().contains("withDefaultAfterRetries(...)");
    }

    @Test
    void aRoutesOwnPolicyIsRefusedBesideTheInstanceDefaultRetryForever() {
        var definition = define().withDefaultRetryForever();
        definition.string("orders").afterRetries(park()).process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("withDefaultRetryForever()");
        assertThat(thrown).hasMessageThat().contains("after-retries policy of its own");
        assertThat(thrown).hasMessageThat().contains("withDefaultRetryLimit(...)");
    }

    /**
     * The control arm that keeps the refusal from being over-broad. Retrying forever with no policy declared
     * anywhere is the classic API's behaviour and stays legal: every route resolves to a parking policy, but a
     * resolved default is not a setting anybody wrote, and refusing it would make retryForever() unusable.
     */
    @Test
    void retryForeverWithNoPolicyDeclaredAnywhereIsAccepted() {
        var definition = define().withDefaultRetryForever();
        definition.string("orders").process(context -> Outcome.succeeded());

        definition.buildOptions(runtime);
    }

    /**
     * The other control arm: a definition where one route bounds its retries is a perfectly good use of a default
     * policy, and the route that can react is the one that keeps it legal.
     */
    @Test
    void aBoundedRouteKeepsTheInstanceDefaultPolicyLegalBesideDefaultRetryForever() {
        var definition = define().withDefaultRetryForever().withDefaultAfterRetries(park());
        definition.string("orders").retryLimit(3).process(context -> Outcome.succeeded());

        definition.buildOptions(runtime);
    }
}
