package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.Percent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import static bz.stub.parallelconsumer.Percent.percentOf;
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
        assertThat(thrown).hasMessageThat().contains("50%");
    }

    /**
     * The same refusal through the other door: a percentage declared as a {@link Percent} reaches validation the
     * same way the bare number does, and is quoted back with its unit either way.
     */
    @Test
    void anExportPercentageDeclaredAsATypeIsRefusedTheSameWay() {
        var pc = define();
        pc.string("orders").afterRetries(park().dlqTo("orders.dlq").dlqWhenOffsetPayloadReaches(percentOf(50)))
                .process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("dlqWhenOffsetPayloadReaches");
        assertThat(thrown).hasMessageThat().contains("50%");
    }

    /**
     * What is not a percentage is refused where it is written, not carried as far as validation: the type is
     * constructed on the spot by the plain-number overload, so both doors refuse at the call. The distinction
     * matters because validation's own refusal explains a percentage as unsupported in this version, which would be
     * the wrong sentence for a number that was never a percentage.
     */
    @Test
    void whatIsNotAPercentageIsRefusedAtTheCallRatherThanAtValidation() {
        var pc = define();
        var route = pc.string("orders");

        assertThat(assertThrows(IllegalArgumentException.class,
                () -> park().dlqTo("orders.dlq").dlqWhenOffsetPayloadReaches(-5)))
                .hasMessageThat().contains("above zero");
        assertThat(assertThrows(IllegalArgumentException.class,
                () -> park().dlqTo("orders.dlq").dlqWhenOffsetPayloadReaches(700)))
                .hasMessageThat().contains("above a hundred");
        assertThat(assertThrows(IllegalArgumentException.class, () -> define().withDlqWhenOffsetPayloadReaches(0)))
                .hasMessageThat().contains("above zero");
        assertThat(assertThrows(NullPointerException.class,
                () -> define().withDlqWhenOffsetPayloadReaches((Percent) null)))
                .hasMessageThat().contains("percentage must be supplied");
        assertThat(assertThrows(NullPointerException.class,
                () -> park().dlqTo("orders.dlq").dlqWhenOffsetPayloadReaches((Percent) null)))
                .hasMessageThat().contains("percentage must be supplied");

        // Completing the route proves the definition was a usable one all along: every refusal above fired at the
        // call that wrote the percentage, with nothing having been built.
        route.process(context -> Outcome.succeeded());
        assertThat(runtime.builtNothing()).isTrue();
    }

    @Test
    void anExportPercentageAboveTheCeilingAlsoNamesTheCeilingAndThePauseThreshold() {
        var pc = define();
        pc.string("orders").afterRetries(park().dlqTo("orders.dlq")
                .dlqWhenOffsetPayloadReaches(AfterRetries.MAX_PAYLOAD_PERCENTAGE.percentage() + 1))
                .process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("ceiling");
        assertThat(thrown).hasMessageThat().contains(AfterRetries.MAX_PAYLOAD_PERCENTAGE.toString());
        assertThat(thrown).hasMessageThat().contains(AfterRetries.PAUSE_THRESHOLD_PERCENTAGE.toString());
    }

    @Test
    void theInstanceWideExportPercentageIsRefusedNamingTheSetting() {
        var pc = define().withDlqWhenOffsetPayloadReaches(60);
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(refusal(pc)).hasMessageThat().contains("withDlqWhenOffsetPayloadReaches");
    }

    /**
     * The ceiling is derived from the engine's own pause threshold rather than written down twice, so a change to
     * that threshold moves both together.
     */
    @Test
    void theCeilingIsFivePointsBelowTheEnginesPauseThreshold() {
        assertThat(AfterRetries.PAUSE_THRESHOLD_PERCENTAGE).isEqualTo(percentOf(75));
        assertThat(AfterRetries.MAX_PAYLOAD_PERCENTAGE).isEqualTo(percentOf(70));
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
                .withCommitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
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
     * The three reactions are alternatives, so a stopping policy refuses an export setting where it is written
     * rather than carrying one that could never fire.
     */
    @Test
    void anExportSettingOnAStoppingPolicyNamesTheSetting() {
        var thrown = assertThrows(IllegalArgumentException.class, () -> AfterRetries.stop().dlqTo("orders.dlq"));

        assertThat(thrown).hasMessageThat().contains("dlqTo");
        assertThat(thrown).hasMessageThat().contains("stop()");
    }

    /**
     * A dead-letter policy turns the same settings away for the opposite reason: not that it never exports, but
     * that it exports on exhaustion itself, so a trigger has no park left to qualify. The refusal says which of
     * the two it is looking at, because "you cannot declare this here" is only actionable once the reader knows
     * why - and the fix differs: park() beside it is what makes the trigger meaningful again.
     */
    @Test
    void anExportTriggerOnADeadLetterPolicyNamesTheSettingAndTheReaction() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> AfterRetries.dlq("orders.dlq").dlqOlderThan(Duration.ofDays(2)));

        assertThat(thrown).hasMessageThat().contains("dlqOlderThan");
        assertThat(thrown).hasMessageThat().contains("dlq(...)");
        assertThat(thrown).hasMessageThat().contains("park()");
    }

    /**
     * The destination is not optional on this reaction - it is half of what the reaction says - so it is refused
     * at the call rather than at validation, where the reader would have to work out which route it meant.
     */
    @Test
    void aDeadLetterReactionWithNoDestinationIsRefusedAtTheCall() {
        assertThrows(NullPointerException.class, () -> AfterRetries.dlq(null));
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
     * retries forever never exhausts, so the reaction, the park cycles and the export triggers were all inert and
     * the definition said nothing. Either half may be declared at either scope, so all four pairings are covered
     * here, in both reactions.
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
