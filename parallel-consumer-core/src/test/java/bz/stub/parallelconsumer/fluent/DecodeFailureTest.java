package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static com.google.common.truth.Truth.assertThat;

/**
 * A payload a route cannot read never ends the poll thread, and which of the three decode results it was decides
 * what happens to it (R12, F4, AE6's decode half).
 * <p>
 * <b>Transient by default</b>, because a stock deserialiser that throws cannot tell a corrupt payload from a
 * registry outage - so the default is the one that keeps trying. A route that can tell says so with
 * {@link Formats#classifyDecodeFailures}, and a permanent failure parks at once <em>without</em> spending an
 * attempt: there is nothing to try again.
 */
@Timeout(60)
class DecodeFailureTest extends AbstractFluentEngineTest {



    /**
     * AE6's transient half: the record follows the ordinary retry path and parks once its attempts run out, while
     * the other route keeps working and the poll thread stays alive throughout - which is the whole point, since
     * on the classic API a deserialisation failure ends that thread.
     */
    @Test
    void aTransientDecodeFailureRetriesAndThenParksWhileTheOtherRouteIsUnaffected() {
        var auditSeen = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.topic("orders")
                .consumed(Consumed.with(Serdes.String(), rejecting()))
                .retryLimit(2)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> Outcome.succeeded());
        pc.string("audit").process(context -> {
            auditSeen.incrementAndGet();
            return Outcome.succeeded();
        });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish("orders", 0, 0, "key-0", POISON);
        runtime.publish("orders", 0, 1, "key-1", "a readable order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.parkedCount()).isEqualTo(1));

        // A limit of two counts the attempts after the first, so three decode attempts happened and no fourth.
        assertThat(onlyParked(dispatcher).attempts()).isEqualTo(3);
        // The readable record on the same route and partition still succeeded.
        assertThat(dispatcher.succeededCount()).isAtLeast(1L);

        // The poll thread is alive: records published after the poison one still arrive, on the other route.
        runtime.publish("audit", 0, 0, "key-0", "audited");
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(auditSeen.get()).isEqualTo(1));
        assertThat(handle.failureCause().isPresent()).isFalse();
    }

    /**
     * AE6's permanent half: parked at once, with <b>no attempt spent</b>. The throw that parks it says so - it is a
     * park that is not an attempt - so the one attempt count there is stays at zero, and the parked entry reports
     * zero rather than a number the view had to be told separately.
     */
    @Test
    void aPermanentDecodeFailureIsParkedAtOnceWithoutSpendingAnAttempt() {
        var ran = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.topic("orders")
                .consumed(Consumed.with(Serdes.String(),
                        Formats.classifyDecodeFailures(Serdes.serdeFrom(Serdes.String().serializer(), rejecting()),
                                failure -> Decode.permanentFailure(failure))))
                .retryLimit(5)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    ran.incrementAndGet();
                    return Outcome.succeeded();
                });

        RouteDispatcher dispatcher = runUntilThePoisonRecordParks(pc);

        assertThat(onlyParked(dispatcher).attempts()).isEqualTo(0);
        assertThat(onlyParked(dispatcher).reason()).contains("never be decoded");
        assertThat(ran.get()).isEqualTo(0);

        // Parked means parked: no further attempt arrives however long the instance runs.
        Awaitility.await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(ran.get()).isEqualTo(0));
        assertThat(dispatcher.parkedCount()).isEqualTo(1);
    }

    /**
     * The same classifier, answering the other way: a failure it calls transient behaves exactly as an ordinary
     * failed attempt, so the distinction really is the classifier's and not the wrapper's.
     */
    @Test
    void aFailureTheClassifierCallsTransientSpendsAttemptsAsUsual() {
        var pc = ParallelConsumer.connect(props());
        pc.topic("orders")
                .consumed(Consumed.with(Serdes.String(),
                        Formats.classifyDecodeFailures(Serdes.serdeFrom(Serdes.String().serializer(), rejecting()),
                                failure -> Decode.transientFailure(failure))))
                .retryLimit(1)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> Outcome.succeeded());

        RouteDispatcher dispatcher = runUntilThePoisonRecordParks(pc);

        assertThat(onlyParked(dispatcher).attempts()).isEqualTo(2);
    }

    /**
     * Start the definition, publish the one payload no route here can read, and wait for it to park. Every scenario
     * below is that, around a different decode classifier.
     */
    private RouteDispatcher runUntilThePoisonRecordParks(ParallelConsumerDefinition pc) {
        handle = runtime.startAndAssign(pc, 1);
        runtime.publish("orders", 0, 0, "key-0", POISON);
        RouteDispatcher dispatcher = pc.dispatcher();
        // Waits on the parked VIEW, not on the park counter. The counter is incremented by the worker at the moment
        // it hands the record back; the record reaches the engine's retry queue - which is the view - a moment
        // later, on the control thread. Waiting on the counter and then reading the view is a race, and it goes red
        // under a loaded box rather than on a keystroke.
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute("orders")).hasSize(1));
        return dispatcher;
    }

    /**
     * The one parked record, read from the engine's own retry queue through the wrapper - which is the only attempt
     * count there is now, so a test asserting on attempts asserts on what the parked view will show an operator.
     */
    private static ParkedRecord onlyParked(RouteDispatcher dispatcher) {
        List<ParkedRecord> parked = dispatcher.parkedForRoute("orders");
        assertThat(parked).hasSize(1);
        return parked.get(0);
    }
}
