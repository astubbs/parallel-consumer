package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static com.google.common.truth.Truth.assertThat;

/**
 * The parked view and the park observer: what an operator can read about a parked record, and who is told (R16,
 * R27, R28, AE12).
 * <p>
 * The view is the facade's half of the parked set - the key as the route read it, the attempts the ledger counted,
 * the last failure, why it parked, and since when. The engine's half is the incomplete offset, which
 * {@link RetryAndParkTest} asserts on; the two are reconciled by the handle, which is a later unit.
 * <p>
 * The observer is sugar over the outcome (KD3): everything it reports is in the view already. What it adds is the
 * moment - once per record per assignment, on the way out, before the record is handed back.
 */
@Timeout(120)
class ParkedViewAndObserverTest extends AbstractFluentEngineTest {




    /**
     * What the observer was handed, kept so the test can assert on it after the fact rather than inside the
     * observer, where a failed assertion would be swallowed as "an observer that threw".
     */
    private static class ObservedPark {

        final ProcessContext<?, ?> record;

        final Throwable failure;

        final int attempts;

        final long committedOffsetAtTheTime;

        ObservedPark(ProcessContext<?, ?> record, Throwable failure, int attempts, long committedOffsetAtTheTime) {
            this.record = record;
            this.failure = failure;
            this.attempts = attempts;
            this.committedOffsetAtTheTime = committedOffsetAtTheTime;
        }
    }

    /**
     * AE12's first clause. Once per record, with the record, the last failure and the attempt count - after the
     * last attempt, and before anything of this record's has been committed.
     */
    @Test
    void theObserverFiresOnceAfterTheLastAttemptWithTheFailureAndTheCount() {
        var observed = new CopyOnWriteArrayList<ObservedPark>();
        var attempts = new AtomicInteger();
        var lastAttemptAt = new AtomicLong();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(2)
                .retryDelay(Duration.ofMillis(10))
                .onParked((record, failure, count) -> observed.add(new ObservedPark(record, failure, count,
                        runtime.committedOffset(TOPIC, 0))))
                .process(context -> {
                    attempts.incrementAndGet();
                    lastAttemptAt.set(System.nanoTime());
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "hopeless", "an order nothing can process");

        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(observed).hasSize(1));

        ObservedPark park = observed.get(0);
        // The count it reports is the count the limit was measured against: a limit of two allows three runs.
        assertThat(park.attempts).isEqualTo(3);
        assertThat(attempts.get()).isEqualTo(3);
        assertThat(park.failure).isInstanceOf(FakeRuntimeException.class);
        assertThat(park.failure).hasMessageThat().contains("this record never succeeds");
        assertThat(park.record.offset()).isEqualTo(0);
        assertThat(park.record.value()).isEqualTo("an order nothing can process");
        // Before the offset commits: nothing of this partition's had been committed past the parked record when
        // the observer ran, and nothing ever will be - the record stays incomplete.
        assertThat(park.committedOffsetAtTheTime).isAtMost(0L);

        // ...and once. No further attempt, and no second call however long the instance runs.
        Awaitility.await().pollDelay(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(observed).hasSize(1);
            assertThat(attempts.get()).isEqualTo(3);
        });
    }

    /**
     * AE12's second clause. A payload that can never be decoded has no typed value to report, so the observer gets
     * the raw envelope - the original bytes and headers, which is also what an export would copy - and an attempt
     * count of zero, because nothing was ever attempted.
     */
    @Test
    void theObserverGetsTheRawEnvelopeAndZeroAttemptsWhenDecodingFailedPermanently() {
        var observed = new CopyOnWriteArrayList<ObservedPark>();
        var ran = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.topic(TOPIC)
                .consumed(Consumed.with(Serdes.String(),
                        Formats.classifyDecodeFailures(Serdes.serdeFrom(Serdes.String().serializer(), rejecting()),
                                Decode::permanentFailure)))
                .retryLimit(5)
                .onParked((record, failure, count) -> observed.add(new ObservedPark(record, failure, count,
                        runtime.committedOffset(TOPIC, 0))))
                .process(context -> {
                    ran.incrementAndGet();
                    return Outcome.succeeded();
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", POISON);

        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(observed).hasSize(1));

        ObservedPark park = observed.get(0);
        assertThat(park.attempts).isEqualTo(0);
        assertThat(ran.get()).isEqualTo(0);
        assertThat(park.failure).isInstanceOf(PermanentDecodeFailureException.class);
        // Nothing decoded, so there is no typed value - and the bytes are all still there.
        assertThat(park.record.value()).isNull();
        assertThat(new String(park.record.raw().value(), StandardCharsets.UTF_8)).isEqualTo(POISON);
        assertThat(park.record.raw().key()).isNotNull();
    }

    /**
     * The parked view is what the handle will read (R28), so everything an operator needs to choose between resume
     * and export is on the entry itself, without going back to the broker.
     */
    @Test
    void theParkedEntryCarriesTheKeyAttemptsFailureReasonAndParkedSince() {
        var before = Instant.now();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(1)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    throw new FakeRuntimeException("the downstream service rejected it");
                });
        pc.string("audit").process(context -> Outcome.succeeded());

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 4, "customer-7", "an order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));

        ParkedRecord parked = dispatcher.parkedForRoute(TOPIC).get(0);
        assertThat(parked.topic()).isEqualTo(TOPIC);
        assertThat(parked.partition()).isEqualTo(0);
        assertThat(parked.offset()).isEqualTo(4);
        assertThat(parked.key()).isEqualTo("customer-7");
        assertThat(parked.attempts()).isEqualTo(2);
        assertThat(parked.cycles()).isEqualTo(0);
        assertThat(parked.failure()).hasMessageThat().contains("the downstream service rejected it");
        assertThat(parked.reason()).contains("ran out of attempts");
        assertThat(parked.parkedSince()).isAtLeast(before);

        // The roll-up spans every route; the other route has nothing parked, so it is the same one record.
        assertThat(dispatcher.parkedAcrossAllRoutes()).hasSize(1);
        assertThat(dispatcher.parkedForRoute("audit")).isEmpty();
    }

    /**
     * A record the function itself declares hopeless: the reason it gave is what the view reports, there is no
     * failure to report because nothing failed, and the remaining attempts are skipped (R8).
     */
    @Test
    void aParkOutcomeParksAtOnceWithTheReasonTheFunctionGave() {
        var observed = new CopyOnWriteArrayList<ObservedPark>();
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(10)
                .retryDelay(Duration.ofMillis(10))
                .onParked((record, failure, count) -> observed.add(new ObservedPark(record, failure, count, -1)))
                .process(context -> {
                    attempts.incrementAndGet();
                    return Outcome.park("schema version 9 needs a deploy");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order from the future");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));

        ParkedRecord parked = dispatcher.parkedForRoute(TOPIC).get(0);
        assertThat(parked.reason()).isEqualTo("schema version 9 needs a deploy");
        assertThat(parked.failure()).isNull();
        assertThat(parked.attempts()).isEqualTo(1);
        // One run, and no more: nine remaining attempts were skipped.
        assertThat(observed).hasSize(1);
        assertThat(observed.get(0).failure).isNull();
        Awaitility.await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(attempts.get()).isEqualTo(1));
    }

    /**
     * A route's observer overrides the instance default for that route only, exactly as every other setting does
     * (R6). Which is the whole reason the default exists: one line on the definition covers every route that has
     * nothing to say of its own.
     */
    @Test
    void aRouteObserverOverridesTheInstanceDefaultForThatRouteAlone() {
        var toTheDefault = new CopyOnWriteArrayList<String>();
        var toTheRoute = new CopyOnWriteArrayList<String>();
        var pc = ParallelConsumer.connect(props())
                .defaultRetryLimit(0)
                .defaultRetryDelay(Duration.ofMillis(10))
                .defaultOnParked((record, failure, attempts) -> toTheDefault.add(record.topic()));
        pc.string(TOPIC)
                .onParked((record, failure, attempts) -> toTheRoute.add(record.topic()))
                .process(context -> {
                    throw new FakeRuntimeException("orders fail");
                });
        pc.string("audit").process(context -> {
            throw new FakeRuntimeException("audit fails too");
        });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        runtime.publish("audit", 0, 0, "key-0", "an audit line");

        Awaitility.await().atMost(defaultTimeout).untilAsserted(() -> {
            assertThat(toTheRoute).containsExactly(TOPIC);
            assertThat(toTheDefault).containsExactly("audit");
        });
    }

    /**
     * An observer is a report. A report that could change the outcome would make parking depend on the reporting,
     * so a throw from it is caught, logged once, and costs the record nothing (R16).
     */
    @Test
    void anObserverThatThrowsIsContainedAndLoggedAndTheRecordParksAnyway() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .onParked((record, failure, attempts) -> {
                    throw new FakeRuntimeException("the observer is broken");
                })
                .process(context -> {
                    throw new FakeRuntimeException("this record never succeeds");
                });

        RouteDispatcher dispatcher;
        List<String> contained;
        try (LogCapture logs = LogCapture.of(RouteDispatcher.class, Level.WARN)) {
            handle = runtime.startAndAssign(pc, 1);
            runtime.publish(TOPIC, 0, 0, "key-0", "an order");

            dispatcher = pc.dispatcher();
            Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                    assertThat(logs.messagesAt(Level.WARN, "park observer", TOPIC)).hasSize(1));
            contained = logs.messagesAt(Level.WARN, "park observer", TOPIC);
        }

        assertThat(contained.get(0)).contains("the record parked anyway");
        // The outcome is untouched: the record is parked, listed and counted. Awaited rather than read, because the
        // log line above is written by the worker at the moment of the hand-back, and the record reaches the
        // engine's retry queue - which is what the view reads - a moment later, on the control thread.
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));
        assertThat(dispatcher.parkedCount()).isEqualTo(1);
        assertThat(handle.failureCause().isPresent()).isFalse();
    }

    /**
     * The stale worker. A record blocked in the function while its partition is revoked finishes on a partition
     * this instance no longer owns - and its throw still runs the whole park path.
     * <p>
     * Nothing of that park may be recorded: an entry would be a <b>phantom</b>, a record listed as parked here
     * while its new owner holds the only copy that can be resumed, and the observer would report a park that this
     * instance had no business having an opinion about. What must still happen is the hand-back itself, so the
     * record does not complete under this instance either way.
     * <p>
     * Then the partition comes back, and everything starts from zero (R10): one attempt, one park, one entry, one
     * observation.
     */
    @Test
    void aStaleWorkerFinishingAfterARevokeLeavesNoPhantomEntryAndTellsNoObserver() throws InterruptedException {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var observed = new CopyOnWriteArrayList<ObservedPark>();
        var attemptsSeen = new CopyOnWriteArrayList<Integer>();
        var runs = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .onParked((record, failure, count) -> {
                    observed.add(new ObservedPark(record, failure, count, -1));
                    attemptsSeen.add(count);
                })
                .process(context -> {
                    if (runs.incrementAndGet() == 1) {
                        entered.countDown();
                        // Block until the test has revoked the partition underneath this worker.
                        release.await(30, TimeUnit.SECONDS);
                    }
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        assertThat(entered.await(30, TimeUnit.SECONDS)).isTrue();

        var partition = new TopicPartition(TOPIC, 0);
        runtime.mockConsumer().revoke(Collections.singletonList(partition));
        release.countDown();

        RouteDispatcher dispatcher = pc.dispatcher();
        // The stale worker's park is dropped: no entry, no observation, and the outcome counter does not move.
        Awaitility.await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(dispatcher.parkedAcrossAllRoutes()).isEmpty();
        });

        // The partition comes back and the record is delivered again to its new owner - which is us.
        runtime.mockConsumer().assign(Collections.singletonList(partition));
        runtime.mockConsumer().seek(partition, 0);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(observed).hasSize(1));
        // One attempt in the new assignment: the count restarted, so the limit of zero exhausted on the first run.
        assertThat(attemptsSeen).containsExactly(1);
        // Awaited, for the same reason as above: the observer fires on the worker thread and the record reaches the
        // engine's retry queue a moment later.
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));
        assertThat(dispatcher.parkedForRoute(TOPIC).get(0).attempts()).isEqualTo(1);
    }

}
