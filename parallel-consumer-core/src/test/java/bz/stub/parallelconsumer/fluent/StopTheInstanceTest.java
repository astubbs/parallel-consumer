package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * Stopping the instance from inside a route's function, and from a record that has run out of attempts (R24, R27,
 * F6, AE14).
 *
 * <h2>What is actually being tested</h2>
 * Stop is four steps in a fixed order, and each of them is here because a simpler design does not work (KTD6):
 * <ul>
 *     <li><b>The mark</b> - the stopping record is handed back never-due and without spending an attempt, so a
 *     drain does not re-invoke it in the window before the close.</li>
 *     <li><b>The pause</b> - non-blocking, and it stops both halves: the controller hands out no further work, and
 *     the controller takes back the batches already queued in the worker pool. The facade used to need a flag of
 *     its own for that second half; the engine does it now (KTD14), and
 *     {@link #stopBoundsDispatchWithThousandsBufferedBehindIt()} is what measures it.</li>
 *     <li><b>The close, from a thread of its own</b> - a worker cannot close the engine it runs in, because the
 *     close awaits the worker pool it belongs to. The close-duration bound in
 *     {@link #theDontDrainPathCompletesInFlightWorkAndStartsNothingNew()} is what would catch a regression to an
 *     inline close: it would not deadlock, it would spend the whole shutdown timeout.</li>
 *     <li><b>The throw</b> - the record is handed back incomplete, so a restart delivers it again.</li>
 * </ul>
 */
@Timeout(180)
class StopTheInstanceTest extends AbstractFluentEngineTest {


    private static final String OTHER_TOPIC = "audit";



    /**
     * AE14 and F6. The dont-drain path: in-flight work completes and commits, nothing new starts, and the stopping
     * record is left incomplete so a restart delivers it again.
     */
    @Test
    void theDontDrainPathCompletesInFlightWorkAndStartsNothingNew() {
        int concurrency = 2;
        var holdEntered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var laterInvoked = new AtomicInteger();
        var holdCompleted = new AtomicInteger();

        var pc = ParallelConsumer.connect(props())
                .closePath(ClosePath.DONT_DRAIN_FIRST)
                .defaultOrdering(ProcessingOrder.UNORDERED)
                // Two workers: one holds a record in flight while the other reaches the stopping record, which is
                // the situation AE14 describes - in-flight work and a stop at the same moment.
                .defaultConcurrency(concurrency);
        pc.string(TOPIC).process(context -> {
            String value = context.value();
            if ("hold".equals(value)) {
                holdEntered.countDown();
                if (!release.await(60, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the test never released the in-flight record");
                }
                holdCompleted.incrementAndGet();
                return Outcome.succeeded();
            }
            if ("stop".equals(value)) {
                return Outcome.stop("the deployment cannot handle this record");
            }
            laterInvoked.incrementAndGet();
            return Outcome.succeeded();
        });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "hold");
        runtime.publish(TOPIC, 0, 1, "key-1", "stop");
        for (int offset = 2; offset < 52; offset++) {
            runtime.publish(TOPIC, 0, offset, "key-" + offset, "later");
        }

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handle.stopRequest().isPresent()).isTrue());
        StopRequest stop = handle.stopRequest().get();
        assertThat(stop.topic()).isEqualTo(TOPIC);
        assertThat(stop.offset()).isEqualTo(1);
        assertThat(stop.reason()).contains("the deployment cannot handle this record");

        // The record that was already in flight is still in flight: the close is waiting for it, not abandoning it.
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> holdEntered.getCount() == 0);
        release.countDown();

        Instant releasedAt = Instant.now();
        assertThat(handle.awaitShutdown(Duration.ofSeconds(30))).isTrue();
        Duration closeTook = Duration.between(releasedAt, Instant.now());
        handle = null;

        // Almost nothing new started, and the bound is what AE14 now says rather than zero: the stop is reported on
        // a worker thread and the controller takes the queued batches back on its next pass, so the records already
        // handed to this two-worker pool can still start in that window. Fifty of the fifty-two published records
        // are "later" ones, so anything approaching them would mean the pause reached nothing at all.
        assertThat(laterInvoked.get()).isAtMost(concurrency * 2);
        // The in-flight record completed, and its offset committed: offset 0 succeeded, so the commit is 1 - which
        // is also the proof that the stopping record's own offset did NOT commit.
        assertThat(holdCompleted.get()).isEqualTo(1);
        assertThat(runtime.committedOffset(TOPIC, 0)).isEqualTo(1);
        // Well inside the ten-second shutdown timeout: a close made from the worker thread itself would wait for
        // the pool it is standing in, and spend all of it.
        assertThat(closeTook).isLessThan(Duration.ofSeconds(8));
        // Not a terminal outcome of the record: it is neither parked nor succeeded (R24, R7).
        assertThat(pc.dispatcher().parkedCount()).isEqualTo(0);
    }

    /**
     * AE14's last clause. The stopping record was left incomplete, so the next instance is handed it again - and
     * stops again, which is the loop R24 says the definition's author owns.
     */
    @Test
    void afterARestartTheStoppingRecordIsDeliveredAgain() {
        var firstRunSaw = new AtomicInteger();
        var pc = ParallelConsumer.connect(props()).closePath(ClosePath.DONT_DRAIN_FIRST);
        pc.string(TOPIC).process(context -> {
            firstRunSaw.incrementAndGet();
            return Outcome.stop("the schema is not supported");
        });
        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        assertThat(handle.awaitShutdown(Duration.ofSeconds(30))).isTrue();
        handle = null;
        assertThat(firstRunSaw.get()).isEqualTo(1);
        // Nothing was committed for the partition at all, so the restart re-polls from where the last commit left
        // off - which is before this record.
        assertThat(runtime.committedOffset(TOPIC, 0)).isLessThan(1L);

        var restartRuntime = new RecordingClientRuntime();
        var restartSaw = new AtomicInteger();
        var restarted = ParallelConsumer.connect(props()).closePath(ClosePath.DONT_DRAIN_FIRST);
        restarted.string(TOPIC).process(context -> {
            restartSaw.incrementAndGet();
            return Outcome.stop("the schema is not supported");
        });
        ConsumerHandle restartHandle = restartRuntime.startAndAssign(restarted, 1);
        restartRuntime.publish(TOPIC, 0, 0, "key-0", "an order");
        assertThat(restartHandle.awaitShutdown(Duration.ofSeconds(30))).isTrue();

        assertThat(restartSaw.get()).isEqualTo(1);
        assertThat(restartHandle.stopRequest().isPresent()).isTrue();
    }

    /**
     * The pause, measured. Two thousand records buffered behind a stop on the fifth: what bounds the damage is the
     * pause reaching the worker pool, not luck - the batches the controller had already handed over would otherwise
     * run to completion however far down the instance was.
     * <p>
     * <b>The engine's take-back count is printed here, not asserted on.</b> How many batches are queued in the pool
     * at the instant of a stop depends on the dynamic load factor, which starts at one - so the queue is often
     * empty and the count is legitimately zero. Measured: two failures in sixteen runs when it was asserted to be
     * at least one. {@code PausedWorkIsTakenBackOutOfThePoolTest} asserts the mechanism where it can be made
     * certain, by driving the controller's pass directly; what this test asserts is the consequence.
     */
    @Test
    void stopBoundsDispatchWithThousandsBufferedBehindIt() {
        int records = 2000;
        int concurrency = 16;
        var invokedOffsets = ConcurrentHashMap.<Long>newKeySet();

        var pc = ParallelConsumer.connect(props())
                .closePath(ClosePath.DONT_DRAIN_FIRST)
                .defaultOrdering(ProcessingOrder.UNORDERED)
                .defaultConcurrency(concurrency);
        var invokedWhenTheStopWasReported = new AtomicInteger(-1);
        pc.string(TOPIC).process(context -> {
            invokedOffsets.add(context.offset());
            if (context.offset() == 4) {
                invokedWhenTheStopWasReported.set(invokedOffsets.size());
                return Outcome.stop("the fifth record asked to stop");
            }
            return Outcome.succeeded();
        });

        handle = runtime.startAndAssign(pc, 1);
        for (int offset = 0; offset < records; offset++) {
            runtime.publish(TOPIC, 0, offset, "key-" + offset, "an order");
        }

        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(handle.stopRequest().isPresent()).isTrue());
        assertThat(handle.awaitShutdown(Duration.ofSeconds(60))).isTrue();
        ConsumerHandle stopped = handle;
        handle = null;

        int invoked = invokedOffsets.size();
        long takenBack = ((AbstractParallelEoSStreamProcessor<?, ?>) stopped.processor())
                .getRecordsPurgedWhilePaused();
        int afterTheStop = invoked - invokedWhenTheStopWasReported.get();
        System.out.printf("Stop bounds dispatch: %d of %d records reached the function, %d of them after the stop "
                + "was reported, %d taken back out of the pool%n", invoked, records, afterTheStop, takenBack);

        // The bound that matters, and the one AE14 is about. It is a bound rather than zero on purpose: the stop is
        // reported on a worker thread and the controller takes the queued batches back on its next pass, so a batch
        // that starts in that window runs to completion. What can run is therefore the batches inside a worker plus
        // whatever the pool started in that window - bounded by the pool's parallelism, not by the backlog - so
        // four times the concurrency is generous and still thirty times tighter than the two thousand records that
        // would run with no pause reaching the pool at all.
        assertThat(afterTheStop).isAtMost(concurrency * 4);
        assertThat(invoked).isLessThan(records / 4);
        // Those records were never completed, so a restart delivers every one of them again.
        assertThat(runtime.committedOffset(TOPIC, 0)).isAtMost(4L);
    }

    /**
     * Stop as the exhaustion reaction (R27): the route's author says a record that runs out of attempts here means
     * the deployment is wrong, not the record. The record does not park - it stops the instance and stays
     * incomplete.
     */
    @Test
    void aRouteMayStopTheInstanceWhenARecordRunsOutOfAttempts() {
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.connect(props()).closePath(ClosePath.DONT_DRAIN_FIRST);
        pc.string(TOPIC)
                .retryLimit(2)
                .retryDelay(Duration.ofMillis(10))
                .afterRetries(AfterRetries.stop())
                .process(context -> {
                    attempts.incrementAndGet();
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        assertThat(handle.awaitShutdown(Duration.ofSeconds(30))).isTrue();
        ConsumerHandle stopped = handle;
        handle = null;

        // A limit of two allows three runs, and the third is the one that stops the instance.
        assertThat(attempts.get()).isEqualTo(3);
        StopRequest stop = stopped.stopRequest().get();
        assertThat(stop.offset()).isEqualTo(0);
        assertThat(stop.reason()).contains("ran out of attempts after 3 attempt(s)");
        assertThat(stop.reason()).contains(TOPIC + "-0@0");
        // It stopped instead of parking, so the parked OUTCOME count did not move: a stop is a request about the
        // instance, not a terminal outcome of the record (R24, R7).
        assertThat(pc.dispatcher().parkedCount()).isEqualTo(0);
        // The record itself is held where it is, and the parked view is where an operator finds it - which is what
        // they want when they are asking why the instance stopped. It carries the stop as its reason, so the view
        // does not read as "this record ran out of attempts".
        List<ParkedRecord> heldBack = pc.dispatcher().parkedAcrossAllRoutes();
        assertThat(heldBack).hasSize(1);
        assertThat(heldBack.get(0).offset()).isEqualTo(0);
        assertThat(heldBack.get(0).reason()).contains("asked the instance to stop");
        assertThat(runtime.committedOffset(TOPIC, 0)).isLessThan(1L);
    }

    /**
     * The reaction is per route (R6): a second route with the ordinary park reaction parks its exhausted record and
     * the instance keeps running, until a record on the stopping route runs out.
     */
    @Test
    void theExhaustionReactionIsPerRoute() {
        var pc = ParallelConsumer.connect(props()).closePath(ClosePath.DONT_DRAIN_FIRST);
        pc.string(TOPIC)
                .retryLimit(2)
                .retryDelay(Duration.ofMillis(10))
                .afterRetries(AfterRetries.stop())
                .process(context -> {
                    throw new FakeRuntimeException("the stopping route never succeeds");
                });
        pc.string(OTHER_TOPIC)
                .retryLimit(2)
                .retryDelay(Duration.ofMillis(10))
                .afterRetries(AfterRetries.park())
                .process(context -> {
                    throw new FakeRuntimeException("the parking route never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(OTHER_TOPIC, 0, 0, "key-0", "an audit record");

        // The parking route's record parks, and the instance carries on: nothing about the other route's
        // declaration reaches it.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(pc.dispatcher().parkedForRoute(OTHER_TOPIC)).hasSize(1));
        assertThat(handle.stopRequest().isPresent()).isFalse();
        assertThat(handle.awaitShutdown(Duration.ofMillis(500))).isFalse();
        // Read while the instance is still running: the parked set is per assignment, so closing revokes the
        // partitions and empties it (R10).
        Set<String> parkedTopics = ConcurrentHashMap.newKeySet();
        pc.dispatcher().parkedAcrossAllRoutes().forEach(parked -> parkedTopics.add(parked.topic()));
        assertThat(parkedTopics).containsExactly(OTHER_TOPIC);

        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        assertThat(handle.awaitShutdown(Duration.ofSeconds(30))).isTrue();
        ConsumerHandle stopped = handle;
        handle = null;

        assertThat(stopped.stopRequest().get().topic()).isEqualTo(TOPIC);
        // One park event, from the other route, and only one: the stopping route's record never parked.
        assertThat(pc.dispatcher().parkedCount()).isEqualTo(1);
    }
}
