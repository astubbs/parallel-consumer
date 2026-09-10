package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import static bz.stub.parallelconsumer.fluent.AfterRetries.dlqImmediately;
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
class DefinitionRefusalTest {

    private final RecordingClientRuntime runtime = new RecordingClientRuntime();

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "definition-refusal-test");
        return properties;
    }

    private static ParallelConsumerDefinition define() {
        return ParallelConsumer.define(props());
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
    void aCommitFailurePolicyOnARouteNamesTheSettingAndTheTopic() {
        var pc = define();
        var route = pc.string("orders");

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> route.commitFailure(CommitFailurePolicy.SHUT_DOWN));

        assertThat(thrown).hasMessageThat().contains("commitFailure");
        assertThat(thrown).hasMessageThat().contains("orders");
    }

    /**
     * The seam it needs (astubbs#352) has not landed, so the instance-wide form refuses too rather than storing a
     * value that would silently do nothing.
     */
    @Test
    void theInstanceWideCommitFailurePolicyNamesTheSeamItWaitsFor() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> define().commitFailure(CommitFailurePolicy.SHUT_DOWN));

        assertThat(thrown).hasMessageThat().contains("commitFailure");
        assertThat(thrown).hasMessageThat().contains("astubbs#352");
    }

    @Test
    void perRouteOrderingNamesTheSettingTheTopicAndWhereOrderingIsDeclared() {
        var pc = define();
        var route = pc.string("orders");

        var thrown = assertThrows(IllegalArgumentException.class, () -> route.ordering(ProcessingOrder.PARTITION));

        assertThat(thrown).hasMessageThat().contains("ordering");
        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("defaultOrdering");
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
    void exportImmediatelyWithNoDestinationNamesTheSettingAndTheTopic() {
        var pc = define();
        pc.string("orders").afterRetries(park().dlqImmediately()).process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("dlqImmediately");
        assertThat(thrown).hasMessageThat().contains("orders");
    }

    @Test
    void anAgeBoundWithNoDestinationNamesTheSettingAndTheTopic() {
        var pc = define();
        pc.string("orders").afterRetries(park().dlqOlderThan(Duration.ofDays(2)))
                .process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("dlqOlderThan");
        assertThat(thrown).hasMessageThat().contains("orders");
    }

    @Test
    void aDestinationWithNoTriggerNamesTheTopicAndTheDestination() {
        var pc = define();
        pc.string("orders").afterRetries(park().dlqTo("orders.dlq")).process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("orders.dlq");
        assertThat(thrown).hasMessageThat().contains("no trigger");
    }

    /**
     * KTD5: in this version the payload-fraction trigger has no engine accessor to read, so <em>any</em> explicit
     * percentage is refused - not only one above the ceiling.
     */
    @Test
    void anyExplicitExportPercentageIsRefusedNamingTheSetting() {
        var pc = define();
        pc.string("orders").afterRetries(park().dlqTo("orders.dlq").dlqWhenOffsetPayloadReaches(50))
                .process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("dlqWhenOffsetPayloadReaches");
        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("50");
    }

    @Test
    void anExportPercentageAboveTheCeilingAlsoNamesTheCeilingAndThePauseThreshold() {
        var pc = define();
        pc.string("orders").afterRetries(park().dlqTo("orders.dlq")
                .dlqWhenOffsetPayloadReaches(AfterRetries.MAX_PAYLOAD_PERCENTAGE + 1))
                .process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("ceiling");
        assertThat(thrown).hasMessageThat().contains(String.valueOf(AfterRetries.MAX_PAYLOAD_PERCENTAGE));
        assertThat(thrown).hasMessageThat().contains("75");
    }

    @Test
    void theInstanceWideExportPercentageIsRefusedNamingTheSetting() {
        var pc = define().dlqWhenOffsetPayloadReaches(60);
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(refusal(pc)).hasMessageThat().contains("dlqWhenOffsetPayloadReaches");
    }

    /**
     * The ceiling is derived from the engine's own pause threshold rather than written down twice, so a change to
     * that threshold moves both together.
     */
    @Test
    void theCeilingIsFivePointsBelowTheEnginesPauseThreshold() {
        assertThat(AfterRetries.MAX_PAYLOAD_PERCENTAGE).isEqualTo(70);
    }

    @Test
    void aDestinationThatIsOneOfTheInstancesOwnTopicsNamesBoth() {
        var pc = define();
        pc.string("audit").process(context -> Outcome.succeeded());
        pc.string("orders").afterRetries(dlqImmediately("audit")).process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("audit");
        assertThat(thrown).hasMessageThat().contains("own exports");
    }

    @Test
    void aDestinationUnderTheTransactionalCommitModeNamesTheDependencyItWaitsFor() {
        Properties properties = props();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "refusal-test");
        var pc = new ParallelConsumerDefinition(properties)
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        pc.string("orders").afterRetries(dlqImmediately("orders.dlq")).process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("astubbs#410");
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
        var pc = define().commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
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
     * The two reactions are alternatives, so a stopping policy refuses an export setting where it is written rather
     * than carrying one that could never fire.
     */
    @Test
    void anExportSettingOnAStoppingPolicyNamesTheSetting() {
        var thrown = assertThrows(IllegalArgumentException.class, () -> AfterRetries.stop().dlqTo("orders.dlq"));

        assertThat(thrown).hasMessageThat().contains("dlqTo");
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
        var definition = define().defaultAfterRetries(park().forCycles(2));
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
}
