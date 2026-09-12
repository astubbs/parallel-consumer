package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.Percent;
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

            var thrown = refusal(pc);
            assertThat(thrown).hasMessageThat().contains("can never open a transaction");
            assertThat(thrown).hasMessageThat().contains("withProducer(...)");
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

            var thrown = refusal(pc);
            assertThat(thrown).hasMessageThat().contains("would silently not apply");
            assertThat(thrown).hasMessageThat().contains("withProducer(...)");
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
        pc.string("orders").afterRetries(park().dlqTo("orders.dlq").dlqAtOffsetPayload(50))
                .process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("dlqAtOffsetPayload");
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
        pc.string("orders").afterRetries(park().dlqTo("orders.dlq").dlqAtOffsetPayload(percentOf(50)))
                .process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("dlqAtOffsetPayload");
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
                () -> park().dlqTo("orders.dlq").dlqAtOffsetPayload(-5)))
                .hasMessageThat().contains("above zero");
        assertThat(assertThrows(IllegalArgumentException.class,
                () -> park().dlqTo("orders.dlq").dlqAtOffsetPayload(700)))
                .hasMessageThat().contains("above a hundred");
        assertThat(assertThrows(IllegalArgumentException.class, () -> define().withDlqAtOffsetPayload(0)))
                .hasMessageThat().contains("above zero");
        assertThat(assertThrows(NullPointerException.class,
                () -> define().withDlqAtOffsetPayload((Percent) null)))
                .hasMessageThat().contains("percentage must be supplied");
        assertThat(assertThrows(NullPointerException.class,
                () -> park().dlqTo("orders.dlq").dlqAtOffsetPayload((Percent) null)))
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
                .dlqAtOffsetPayload(AfterRetries.MAX_PAYLOAD_PERCENTAGE.percentage() + 1))
                .process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains("ceiling");
        assertThat(thrown).hasMessageThat().contains(AfterRetries.MAX_PAYLOAD_PERCENTAGE.toString());
        assertThat(thrown).hasMessageThat().contains(AfterRetries.PAUSE_THRESHOLD_PERCENTAGE.toString());
    }

    @Test
    void theInstanceWideExportPercentageIsRefusedNamingTheSetting() {
        var pc = define().withDlqAtOffsetPayload(60);
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(refusal(pc)).hasMessageThat().contains("withDlqAtOffsetPayload");
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

    /**
     * Names the setting, and names the remedy by a method that exists. Every instance-wide setting took a
     * {@code with} prefix (KD16), and this refusal kept the pre-rename spelling {@code producer(...)} - a call that
     * is on no class in the API, so the one reader who tried to act on it had nothing to find and nothing to grep
     * for. It is the failure mode a wrong-scope spelling has, with the scope missing altogether.
     */
    @Test
    void theTransactionalCommitModeWithNoTransactionalIdNamesTheSettingAndASpellingThatExists() {
        var pc = define().withCommitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        pc.string("orders").process(context -> Outcome.succeeded());

        var thrown = refusal(pc);

        assertThat(thrown).hasMessageThat().contains(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        assertThat(thrown).hasMessageThat().contains("withProducer(...)");
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
     * <p>
     * It names the calls that made the policy and no hand-in call, for the reason the park-setting test beside it
     * records: at this throw the policy has not been handed anywhere (KD16).
     */
    @Test
    void anExportSettingOnAStoppingPolicyNamesTheSettingAndNeitherScope() {
        var thrown = assertThrows(IllegalArgumentException.class, () -> AfterRetries.stop().dlqTo("orders.dlq"));

        assertThat(thrown).hasMessageThat().contains("dlqTo");
        assertThat(thrown).hasMessageThat().contains("stop()");
        assertThat(thrown).hasMessageThat().contains("park()");
        assertThat(thrown).hasMessageThat().doesNotContain("afterRetries(");
        assertThat(thrown).hasMessageThat().doesNotContain("withDefaultAfterRetries");
    }

    /**
     * The two reactions are alternatives, so a stopping policy refuses a park setting where it is written rather
     * than carrying one that could never fire.
     * <p>
     * It names the calls that made the policy and no hand-in call, because at this throw the policy has not been
     * handed anywhere: it used to name {@code afterRetries(stop())}, the route spelling, which an author declaring
     * an instance default never wrote and could not write (KD16).
     */
    @Test
    void aParkSettingOnAStoppingPolicyNamesTheSettingAndNeitherScope() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> AfterRetries.stop().thenRetryAfter(Duration.ofSeconds(1)));

        assertThat(thrown).hasMessageThat().contains("thenRetryAfter");
        assertThat(thrown).hasMessageThat().contains("stop()");
        assertThat(thrown).hasMessageThat().contains("park()");
        assertThat(thrown).hasMessageThat().doesNotContain("afterRetries(");
        assertThat(thrown).hasMessageThat().doesNotContain("withDefaultAfterRetries");
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
        assertThat(thrown).hasMessageThat().contains("its own afterRetries(...)");
        assertThat(thrown).hasMessageThat().doesNotContain("withDefaultAfterRetries");
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
        assertThat(thrown).hasMessageThat().contains("its own afterRetries(...)");
        assertThat(thrown).hasMessageThat().doesNotContain("withDefaultAfterRetries");
    }

    /**
     * The same pair declared as the instance default is refused just the same, because every route took a copy of it
     * (R6) - and the refusal names <em>both</em> the call that declared it and the topic that takes it.
     * <p>
     * Naming only the topic is what this used to do, and it sent the author to the wrong place: the message read
     * "Topic orders declares forCycles(2)" when {@code orders} declares nothing at all, so the author opened that
     * route, found no park cycle on it, and the refusal had nothing further to say. The {@code doesNotContain} is the
     * half that keeps this honest - a message naming both spellings would pass the positive assertion here and in
     * the route-scoped tests above while telling neither author which call was theirs.
     */
    @Test
    void theInstanceDefaultParkCycleNamesTheCallThatDeclaredItAndTheTopicThatTakesIt() {
        var definition = define().withDefaultAfterRetries(park().forCycles(2));
        definition.string("orders").process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("forCycles(2)");
        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("withDefaultAfterRetries(...)");
        assertThat(thrown).hasMessageThat().doesNotContain("its own afterRetries(...)");
    }

    /**
     * The other half of the pair, at the instance scope, because the two arms are separate refusals and only one of
     * them was covered here.
     */
    @Test
    void theInstanceDefaultParkDelayWithNoCycleCountNamesTheCallThatDeclaredIt() {
        var definition = define().withDefaultAfterRetries(park().thenRetryAfter(Duration.ofMinutes(5)));
        definition.string("orders").process(context -> Outcome.succeeded());

        var thrown = refusal(definition);

        assertThat(thrown).hasMessageThat().contains("thenRetryAfter");
        assertThat(thrown).hasMessageThat().contains("forCycles(...)");
        assertThat(thrown).hasMessageThat().contains("orders");
        assertThat(thrown).hasMessageThat().contains("withDefaultAfterRetries(...)");
        assertThat(thrown).hasMessageThat().doesNotContain("its own afterRetries(...)");
    }

    @Test
    void aParkCycleCountOnAStoppingPolicyNamesTheSettingAndNeitherScope() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> AfterRetries.stop().forCycles(2));

        assertThat(thrown).hasMessageThat().contains("forCycles");
        assertThat(thrown).hasMessageThat().contains("stop()");
        assertThat(thrown).hasMessageThat().contains("park()");
        assertThat(thrown).hasMessageThat().doesNotContain("afterRetries(");
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
        assertThat(thrown).hasMessageThat().contains("its own afterRetries(...)");
        assertThat(thrown).hasMessageThat().doesNotContain("withDefaultAfterRetries");
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
        assertThat(thrown).hasMessageThat().contains("its own afterRetries(...)");
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
