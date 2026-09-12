package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Every setting on the definition took a {@code with} prefix (KD16), and every spelling it replaced stayed behind as
 * a deprecated delegate (KD15). This suite is what notices if one of them goes: removing a deprecated member is a
 * separate, release-gated decision belonging to the breaking-change queue, and nothing else in the build fails when
 * one quietly disappears - a caller that stopped compiling is the user's build, not ours.
 *
 * <h2>What it asserts, and why in two halves</h2>
 * The first half is the whole mapping by name: each old spelling is still declared, still carries
 * {@link Deprecated}, and still returns the definition so it can sit in a chain. The second half proves the
 * delegation is real rather than a second implementation that has since drifted - the old name is called and the
 * value is read back through the same view the new name is read through.
 */
class DeprecatedSettingNamesTest {

    /**
     * The KD16 mapping, spelled out so that this suite fails by name rather than by absence: a deleted member is a
     * missing key here, not a reflective sweep that finds nothing and passes.
     */
    private static Map<String, String> mapping() {
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put("commitMode", "withCommitMode");
        mapping.put("whenClosing", "withClosePath");
        mapping.put("whenTopicMissing", "withMissingTopicPolicy");
        mapping.put("meterRegistry", "withMetrics");
        mapping.put("defaultOrdering", "withDefaultOrdering");
        mapping.put("defaultConcurrency", "withDefaultConcurrency");
        mapping.put("defaultRetryLimit", "withDefaultRetryLimit");
        mapping.put("defaultRetryForever", "withDefaultRetryForever");
        mapping.put("defaultRetryDelay", "withDefaultRetryDelay");
        mapping.put("defaultAfterRetries", "withDefaultAfterRetries");
        mapping.put("defaultOnParked", "withDefaultOnParked");
        mapping.put("consumer", "withConsumer");
        mapping.put("producer", "withProducer");
        mapping.put("rebalanceListener", "withRebalanceListener");
        return mapping;
    }

    private static ParallelConsumerDefinition define() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "deprecated-setting-names-test");
        return ParallelConsumer.connect(properties);
    }

    /**
     * The pair, for every setting in the mapping: the new name takes the arguments the old one took, and the old one
     * is still there and still marked.
     */
    @Test
    void everyReplacedSpellingIsStillDeclaredAndStillDeprecated() {
        mapping().forEach((oldName, newName) -> {
            Method replacement = onlyMethodNamed(newName);
            Method deprecated = onlyMethodNamed(oldName);

            assertWithMessage("%s must still be deprecated - KD15 keeps it, it does not un-mark it", oldName)
                    .that(deprecated.isAnnotationPresent(Deprecated.class))
                    .isTrue();
            assertWithMessage("%s must not be deprecated - it is the name to use", newName)
                    .that(replacement.isAnnotationPresent(Deprecated.class))
                    .isFalse();
            assertWithMessage("%s must take what %s takes, or it is not a delegate", oldName, newName)
                    .that(deprecated.getParameterTypes())
                    .isEqualTo(replacement.getParameterTypes());
            assertWithMessage("%s must return the definition, so it still sits in a chain", oldName)
                    .that(deprecated.getReturnType())
                    .isEqualTo(ParallelConsumerDefinition.class);
        });
    }

    /**
     * The one setting of that name: the settings all return the definition, which is what tells a setter apart from
     * the same-named accessor {@link DefinitionView} publishes. More than one is an overload a caller could bind to
     * by accident, so it fails here rather than at whichever call site got unlucky.
     */
    private static Method onlyMethodNamed(String name) {
        Method found = null;
        for (Method method : ParallelConsumerDefinition.class.getDeclaredMethods()) {
            if (method.getName().equals(name) && method.getReturnType() == ParallelConsumerDefinition.class) {
                assertWithMessage("more than one %s on the definition", name).that(found).isNull();
                found = method;
            }
        }
        assertWithMessage("%s is gone from the definition - removing a deprecated member is a release-gated "
                + "decision for the breaking-change queue, not a rename pass", name).that(found).isNotNull();
        return found;
    }

    /**
     * The instance-wide pair the definition view can be read for, called by their old names.
     */
    @Test
    @SuppressWarnings("deprecation") // the point of the test: the old spellings are called deliberately
    void theOldInstanceWideSpellingsSetWhatTheNewOnesSet() {
        ParallelConsumerDefinition definition = define();

        ParallelConsumerDefinition returned = definition
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .defaultOrdering(ProcessingOrder.KEY);

        assertThat(returned).isSameInstanceAs(definition);
        assertThat(definition.commitMode()).isEqualTo(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS);
        assertThat(definition.ordering()).isEqualTo(ProcessingOrder.KEY);
    }

    /**
     * The per-route defaults, called by their old names and read back off the route that copies them - the same
     * route view the new names are asserted through elsewhere in this suite's package.
     */
    @Test
    @SuppressWarnings("deprecation") // the point of the test: the old spellings are called deliberately
    void theOldPerRouteDefaultSpellingsReachTheRouteThatCopiesThem() {
        ParallelConsumerDefinition definition = define()
                .defaultConcurrency(42)
                .defaultRetryLimit(3)
                .defaultRetryDelay(Duration.ofSeconds(7))
                .defaultAfterRetries(AfterRetries.stop());
        definition.string("orders");

        RouteView route = definition.route("orders");
        assertThat(route.concurrency()).isEqualTo(42);
        assertThat(route.retryLimit()).isEqualTo(OptionalInt.of(3));
        assertThat(route.retryDelay()).isEqualTo(Duration.ofSeconds(7));
        assertThat(route.afterRetries().reaction()).isEqualTo(AfterRetries.Reaction.STOP);
    }

    /**
     * Unbounded retries by the old spelling, which is a setting of its own rather than a value.
     */
    @Test
    @SuppressWarnings("deprecation") // the point of the test: the old spelling is called deliberately
    void theOldRetryForeverSpellingStillMeansUnbounded() {
        ParallelConsumerDefinition definition = define().defaultRetryForever();
        definition.string("orders");

        assertThat(definition.route("orders").retryLimit()).isEqualTo(OptionalInt.empty());
    }

    /**
     * The settings whose value the definition view does not publish. What is asserted is that the old spelling
     * returns the definition, which only a delegate returning the new method's result can do - and that they chain,
     * so a caller who wrote them in a row still compiles. What a supplied client or a declared policy then does is
     * {@link ClientConstructionTest}'s and {@link MissingTopicPolicyTest}'s, under the new names.
     */
    @Test
    @SuppressWarnings("deprecation") // the point of the test: the old spellings are called deliberately
    void theRemainingOldSpellingsReturnTheDefinitionTheNewOnesReturn() {
        ParallelConsumerDefinition definition = define();

        ParallelConsumerDefinition returned = definition
                .whenClosing(ClosePath.DONT_DRAIN_FIRST)
                .whenTopicMissing(MissingTopic.IGNORE)
                .meterRegistry(new SimpleMeterRegistry())
                .defaultOnParked((record, failure, attempts) -> { })
                .rebalanceListener(new SilentRebalanceListener())
                .consumer(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST))
                .producer(new MockProducer<>());

        assertThat(returned).isSameInstanceAs(definition);
    }

    /**
     * A listener that does nothing: what is under test is that the old spelling still accepts one and hands it on,
     * not what a rebalance does with it - {@link UsersRebalanceListenerTest} owns that.
     */
    private static class SilentRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // nothing: this listener exists to be accepted, not to react
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // nothing: this listener exists to be accepted, not to react
        }
    }
}
