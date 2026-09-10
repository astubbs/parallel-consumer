package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * A rebalance listener declared on the definition reaches the engine, and sees the same callbacks it would on the
 * classic API (KTD2, R10).
 *
 * <h2>What this used to be, and why it is smaller</h2>
 * It tested a listener of the facade's own, chained in front of the user's, whose job was to clear two
 * per-assignment stores - an attempt ledger and a parked set - for partitions this instance no longer owns. Both
 * stores are gone: the attempt count and the parked set are the engine's, and the engine already drops a revoked
 * partition's containers from both. So there is no ordering left to assert, and what remains is the one claim that
 * still has content: a listener the definition declares is actually installed.
 */
@Timeout(60)
class UsersRebalanceListenerTest extends AbstractFluentEngineTest {

    /**
     * Records what happened and in which order, so a claim about what a listener saw is read rather than assumed.
     */
    private static class RecordingListener implements ConsumerRebalanceListener {

        final List<String> calls = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            calls.add("revoked " + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            calls.add("assigned " + partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            calls.add("lost " + partitions);
        }
    }

    @Test
    void aDeclaredListenerIsToldAboutTheAssignment() {
        var listener = new RecordingListener();
        var pc = ParallelConsumer.connect(props()).rebalanceListener(listener);
        pc.string(TOPIC).process(context -> Outcome.succeeded());

        handle = runtime.startAndAssign(pc, 2);

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(listener.calls).isNotEmpty());
        assertThat(listener.calls.get(0)).startsWith("assigned ");
        assertThat(listener.calls.get(0)).contains(TOPIC + "-0");
        assertThat(listener.calls.get(0)).contains(TOPIC + "-1");
    }

    /**
     * A definition that declares no listener still starts and still consumes - the subscribe path without one is a
     * different call, so it needs its own arm.
     */
    @Test
    void aDefinitionWithNoListenerStillConsumes() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.succeededCount()).isEqualTo(1));
    }
}
