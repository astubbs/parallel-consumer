package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A route naming a topic the cluster does not have (owner decision, 2026-09-11): refused by default, created on
 * request, reported and started under IGNORE.
 *
 * <h2>What this replaces</h2>
 * Nothing was asked at all. Kafka's consumer auto-creates a topic it is subscribed to unless the broker forbids it,
 * so a misspelled name became an empty topic with a partition count nobody chose; where the broker did forbid it,
 * the route was assigned nothing and the instance's warning named two possible causes without being able to decide
 * between them.
 *
 * <h2>The double</h2>
 * {@link MockAdminClient}, which Kafka ships beside the interface it implements, reached through the same
 * {@link ClientRuntime} seam the consumer and the producer come through - so what is exercised is the production
 * path, with the cluster replaced rather than the check.
 */
@Timeout(180)
class MissingTopicPolicyTest extends AbstractFluentEngineTest {

    private static final String MISSING_TOPIC = "orders-mispelt";

    /**
     * One broker, because the check asks what exists and never asks where it is.
     */
    private final Node broker = new Node(0, "localhost", 9092);

    private final MockAdminClient admin = new MockAdminClient(Collections.singletonList(broker), broker);

    /**
     * The runtime under test: the mock consumer and producer of every other fluent suite, plus an admin client the
     * default {@link RecordingClientRuntime} deliberately does not supply.
     * <p>
     * It hands out the same instance on every call rather than a fresh one, so that a test can read afterwards what
     * the check did to the cluster. The facade closes what it is given; {@link MockAdminClient#close()} does not
     * stop it answering, which is what makes that possible.
     */
    private final RecordingClientRuntime runtimeWithAdmin = new RecordingClientRuntime() {

        @Override
        public Optional<Admin> admin(DefinitionView definition) {
            return Optional.of(admin);
        }
    };

    @AfterEach
    void closeTheAdmin() {
        admin.close();
    }

    private void clusterHas(String... topics) {
        for (String topic : topics) {
            admin.addTopic(false, topic,
                    Collections.singletonList(new TopicPartitionInfo(0, broker,
                            Collections.singletonList(broker), Collections.singletonList(broker))),
                    Collections.emptyMap());
        }
    }

    private List<String> topicsOnTheCluster() throws ExecutionException, InterruptedException {
        return new java.util.ArrayList<>(admin.listTopics().names().get());
    }

    /**
     * The default, and the whole point: the start is refused, by name, before anything joins the group.
     */
    @Test
    void aMissingTopicFailsTheStartByDefault() throws Exception {
        clusterHas(TOPIC);
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        pc.string(MISSING_TOPIC).process(context -> Outcome.succeeded());

        MissingTopicsException refused = assertThrows(MissingTopicsException.class,
                () -> pc.start(runtimeWithAdmin));

        assertThat(refused).hasMessageThat().contains(MISSING_TOPIC);
        assertThat(refused.missingTopics()).containsExactly(MISSING_TOPIC);
        assertWithMessage("the topic that does exist is not reported as missing")
                .that(refused.missingTopics()).doesNotContain(TOPIC);
        // Refused before the instance was built, not after it had joined the group and gone quiet.
        assertWithMessage("no consumer and no producer were built for a definition that cannot be satisfied")
                .that(runtimeWithAdmin.builtNothing()).isTrue();
        assertWithMessage("and nothing was created - FAIL refuses, it does not repair")
                .that(topicsOnTheCluster()).containsExactly(TOPIC);
    }

    /**
     * The policy is instance-wide, so a definition whose every topic is there starts exactly as it did before this
     * check existed - the case that must not have become slower or louder.
     */
    @Test
    void anExistingTopicIsUnaffected() throws Exception {
        clusterHas(TOPIC);
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());

        handle = runtimeWithAdmin.startAndAssign(pc, 1);

        assertThat(handle.processor().isClosedOrFailed()).isFalse();
        assertThat(topicsOnTheCluster()).containsExactly(TOPIC);
    }

    /**
     * CREATE: the topics appear, with the broker's own defaults, and the instance starts.
     */
    @Test
    void createMakesTheMissingTopicAndStarts() throws Exception {
        clusterHas(TOPIC);
        var pc = ParallelConsumer.connect(props()).whenTopicMissing(MissingTopic.CREATE);
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        pc.string(MISSING_TOPIC).process(context -> Outcome.succeeded());

        handle = runtimeWithAdmin.startAndAssign(pc, 1);

        assertThat(topicsOnTheCluster()).containsExactly(TOPIC, MISSING_TOPIC);
        assertThat(handle.processor().isClosedOrFailed()).isFalse();
    }

    /**
     * IGNORE: nothing is refused, nothing is created, and the missing topic is named in a warning - so the empty
     * route has an explanation beside it rather than only silence.
     */
    @Test
    void ignoreStartsAndNamesWhatIsMissing() throws Exception {
        clusterHas(TOPIC);
        var pc = ParallelConsumer.connect(props()).whenTopicMissing(MissingTopic.IGNORE);
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        pc.string(MISSING_TOPIC).process(context -> Outcome.succeeded());

        try (LogCapture logs = LogCapture.of(TopicExistenceCheck.class, Level.WARN)) {
            handle = runtimeWithAdmin.startAndAssign(pc, 1);

            assertThat(logs.messagesAt(Level.WARN, "do not exist on the cluster", MISSING_TOPIC)).isNotEmpty();
        }
        assertThat(handle.processor().isClosedOrFailed()).isFalse();
        assertWithMessage("IGNORE says so and starts; it does not repair").that(topicsOnTheCluster())
                .containsExactly(TOPIC);
    }

    /**
     * A runtime with no cluster behind it declines to supply an admin client, and the check is then skipped rather
     * than faked - which is what lets every other fluent suite in this package go on starting definitions over mock
     * clients without inventing a cluster for them.
     */
    @Test
    void aRuntimeWithNoClusterSkipsTheCheckEntirely() {
        var pc = ParallelConsumer.connect(props());
        pc.string(MISSING_TOPIC).process(context -> Outcome.succeeded());

        // The plain recording runtime, which supplies no admin client - and the topic exists nowhere.
        handle = runtime.startAndAssign(pc, 1);

        assertThat(handle.processor().isClosedOrFailed()).isFalse();
    }

    /**
     * <b>A pattern route cannot reach this check, because there are no pattern routes.</b> The question was whether
     * a pattern should be refused alongside the policy or exempted from it, and neither is needed: a pattern
     * subscription is refused outright by this version, where the route table is keyed by topic name, so the
     * refusal a user meets is the one about their pattern rather than one about a topic named {@code orders-.*}.
     * This test exists so that the day patterns are accepted, it goes red and the policy has to answer for them.
     */
    @Test
    void aPatternRouteIsRefusedBeforeAnyTopicIsChecked() throws Exception {
        clusterHas(TOPIC);
        var pc = ParallelConsumer.connect(props());

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> pc.topics(Pattern.compile("orders-.*")));

        assertThat(refused).hasMessageThat().contains("pattern subscription");
        assertWithMessage("the refusal is the definition's, so the cluster was never asked anything")
                .that(topicsOnTheCluster()).containsExactly(TOPIC);
    }

    /**
     * Missing and unanswerable are different answers, and only one of them is about the definition. A describe that
     * fails for any reason other than "no such topic" is the cluster declining to answer, and under FAIL that is
     * raised as its own fault rather than being reported as a missing topic - which would refuse a definition whose
     * topics are all present.
     */
    @Test
    void aClusterThatCannotAnswerIsNotTheSameAsAMissingTopic() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        ClientRuntime unreachable = new RecordingClientRuntime() {

            @Override
            public Optional<Admin> admin(DefinitionView definition) {
                MockAdminClient failing = new MockAdminClient(Collections.singletonList(broker), broker);
                // Not UnknownTopicOrPartitionException: this is the broker refusing to say, which is the case that
                // must not be read as an answer.
                failing.timeoutNextRequest(1);
                return Optional.of(failing);
            }
        };

        IllegalStateException raised = assertThrows(IllegalStateException.class, () -> pc.start(unreachable));

        assertWithMessage("it is not reported as a missing topic, which is a claim about the definition")
                .that(raised).isNotInstanceOf(MissingTopicsException.class);
        assertThat(raised).hasMessageThat().contains(TOPIC);
    }

    /**
     * The same unanswerable cluster under IGNORE: the policy that says this question may not stop a start does not
     * stop it for an unreachable broker either.
     */
    @Test
    void ignoreStartsEvenWhenTheClusterCannotAnswer() {
        var pc = ParallelConsumer.connect(props()).whenTopicMissing(MissingTopic.IGNORE);
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        RecordingClientRuntime unreachable = new RecordingClientRuntime() {

            @Override
            public Optional<Admin> admin(DefinitionView definition) {
                MockAdminClient failing = new MockAdminClient(Collections.singletonList(broker), broker);
                failing.timeoutNextRequest(1);
                return Optional.of(failing);
            }
        };

        handle = unreachable.startAndAssign(pc, 1);

        assertThat(handle.processor().isClosedOrFailed()).isFalse();
    }

    /**
     * The setting refuses null rather than reading it as "the default", which would be a definition silently
     * meaning something other than what it says.
     */
    @Test
    void thePolicyMayNotBeNull() {
        var pc = ParallelConsumer.connect(props());

        NullPointerException refused = assertThrows(NullPointerException.class, () -> pc.whenTopicMissing(null));

        assertThat(refused).hasMessageThat().contains("missing-topic policy");
    }
}
