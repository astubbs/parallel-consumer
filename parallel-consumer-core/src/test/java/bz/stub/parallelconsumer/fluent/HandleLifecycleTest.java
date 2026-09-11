package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The handle's lifecycle: close, drain, await, and the things a caller can do twice or not at all (R17, AE9).
 * <p>
 * The three ways an instance ends are the subject here - closed, stopped, failed - minus the stop, which
 * {@link StopTheInstanceTest} owns because stopping is a good deal more than a way for {@code awaitShutdown} to
 * return.
 */
@Timeout(180)
class HandleLifecycleTest extends AbstractFluentEngineTest {




    /**
     * AE9. The block exits with work in flight and a backlog already fetched behind it; the close drains, and the
     * offsets of everything it drained commit.
     * <p>
     * <b>What separates a drain from a plain close here</b> is the third assertion: records that had not entered
     * the function when the block exited do enter it afterwards. Measured against the dont-drain path on the same
     * shape, which stops at exactly the records already inside the function - four entered and four completed,
     * against eight and eight for the drain.
     * <p>
     * The drain is bounded, not exhaustive: it does not promise that every record the engine had fetched is
     * processed, and here four of the twelve are not. Those stay incomplete and are redelivered - which the
     * committed offset map, carrying the frontier and not the whole partition, is the evidence of.
     */
    @Test
    void tryWithResourcesDrainsTheWorkAlreadyFetched() {
        int records = 12;
        int concurrency = 4;
        var entered = new AtomicInteger();
        var completed = new AtomicInteger();
        var pc = ParallelConsumer.connect(props())
                .defaultOrdering(ProcessingOrder.UNORDERED)
                .defaultConcurrency(concurrency);
        pc.string(TOPIC).process(context -> {
            entered.incrementAndGet();
            // Long enough that the block below exits inside the first wave, which is what bounds enteredAtExit.
            Thread.sleep(300);
            completed.incrementAndGet();
            return Outcome.succeeded();
        });

        ConsumerHandle started = runtime.startAndAssign(pc, 1);
        handle = started;
        for (int offset = 0; offset < records; offset++) {
            runtime.publish(TOPIC, 0, offset, "key-" + offset, "an order");
        }

        int enteredAtExit;
        long backlogAtExit;
        try (ConsumerHandle inBlock = started) {
            // Work in flight, everything fetched, and nothing finished yet - which is what pins the exit to the
            // FIRST wave of records. Waiting on a looser condition let the block exit two waves in, with almost
            // nothing left to drain, and the drain assertion below then failed on load rather than on behaviour.
            Awaitility.await().atMost(defaultTimeout).until(() ->
                    entered.get() >= 1 && inBlock.processor().workRemaining() == records);
            enteredAtExit = entered.get();
            backlogAtExit = inBlock.processor().workRemaining();
        }
        handle = null;

        assertThat(backlogAtExit).isEqualTo((long) records);
        // Nothing had completed, so at most one wave of workers had started: the rest is backlog for the drain.
        assertThat(enteredAtExit).isAtMost(concurrency);
        // Nothing was abandoned half-processed: every record that entered the function came out of it.
        assertThat(completed.get()).isEqualTo(entered.get());
        // The drain started records that had not started when the block exited - which the dont-drain path does not.
        assertThat(entered.get()).isGreaterThan(enteredAtExit);
        // ...and the offsets of everything it drained committed. One partition, unordered, nothing failed, so the
        // committed offset is the count of records completed.
        assertThat(runtime.committedOffset(TOPIC, 0)).isEqualTo(completed.get());
        assertThat(started.stopRequest().isPresent()).isFalse();
        assertThat(started.failureCause().isPresent()).isFalse();
    }

    /**
     * The first await exit: another thread closes the handle, and the waiter is released.
     */
    @Test
    void awaitReturnsWhenAnotherThreadClosesTheHandle() throws Exception {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        ConsumerHandle started = runtime.startAndAssign(pc, 1);
        handle = started;

        var returned = new CountDownLatch(1);
        var thrown = new AtomicReference<Throwable>();
        Thread waiter = new Thread(() -> {
            try {
                started.awaitShutdown();
            } catch (Throwable failed) {
                thrown.set(failed);
            } finally {
                returned.countDown();
            }
        }, "await-shutdown-waiter");
        waiter.setDaemon(true);
        waiter.start();

        // Still waiting: nothing has closed it.
        assertThat(returned.await(500, TimeUnit.MILLISECONDS)).isFalse();

        started.close();
        handle = null;

        assertThat(returned.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(thrown.get()).isNull();
    }

    /**
     * The third await exit: the control thread died, so the instance is not consuming and never will again. The
     * cause is rethrown <b>wrapped</b>, which is what tells it apart from a definition fault - and the alternative,
     * a caller left blocked on a latch nobody will ever count down, is the failure this replaces.
     * <p>
     * The control thread is killed the way the engine itself documents: a loop-end callback that throws is run as
     * user code and is not swallowed. That is also why the facade's own hook contains everything - see
     * {@link ParkedSnapshots}.
     */
    @Test
    void awaitRethrowsAControlThreadFailureWrapped() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        ConsumerHandle started = runtime.startAndAssign(pc, 1);
        handle = started;

        started.processor().addLoopEndCallBack(() -> {
            throw new FakeRuntimeException("a loop-end callback that throws stops the consumer");
        });

        InstanceFailedException failed = assertThrows(InstanceFailedException.class,
                () -> started.awaitShutdown(Duration.ofSeconds(30)));
        handle = null;

        assertThat(failed).hasMessageThat().contains("control thread failed");
        assertThat(failed).hasCauseThat().isNotNull();
        // The cause is the engine's own record of what killed it, reachable without waiting too.
        assertThat(started.failureCause().isPresent()).isTrue();
        assertThat(started.failureCause().get()).isSameInstanceAs(failed.getCause());
    }

    /**
     * The same third exit, told from the waiter's side: it is released <b>at the moment the control thread ends</b>,
     * rather than on the next tick of a poll.
     * <p>
     * <b>Two assertions, and the first one is the deterministic half.</b> The wait is now composed on the engine's
     * own completion, so a handle that is waiting the right way shows up as a <em>dependent</em> of that future -
     * which is true whether or not anything has ended yet, and cannot be passed by an implementation that polls
     * beside it. The second measures the consequence: the gap between the engine completing and the waiter being
     * told, which the implementation this replaces could not make smaller than its two-hundred-millisecond poll
     * interval and averaged half of it. Everything else about the close is excluded from that measurement on
     * purpose - it is stamped from the engine's completion, not from the throw that caused it, so engine shutdown
     * latency cannot enter the figure.
     *
     * @see #awaitRethrowsAControlThreadFailureWrapped()
     */
    @Test
    void aControlThreadFailureReachesTheWaiterWithoutAPollInterval() throws Exception {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        ConsumerHandle started = runtime.startAndAssign(pc, 1);
        handle = started;

        assertWithMessage("the handle waits ON the engine's completion, so it is registered as a dependent of it - "
                + "a handle that polled beside it would register none")
                .that(started.processor().controlThreadCompletion().getNumberOfDependents())
                .isGreaterThan(0);

        // Stamped on the engine's completing thread, so it records when the control thread ENDED rather than when
        // this test noticed. The future is discarded deliberately: the stamp is the whole point of the callback,
        // and nothing waits on it - the waiter below is what this test synchronises with.
        var engineEndedAt = new AtomicReference<Instant>();
        CompletableFuture<Void> ignoredStamp = started.processor().controlThreadCompletion()
                .whenComplete((ignoredResult, ignoredFailure) -> engineEndedAt.set(Instant.now()));

        var releasedAt = new AtomicReference<Instant>();
        var thrown = new AtomicReference<Throwable>();
        var returned = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try {
                started.awaitShutdown();
            } catch (Throwable failed) {
                thrown.set(failed);
            } finally {
                releasedAt.set(Instant.now());
                returned.countDown();
            }
        }, "await-shutdown-waiter");
        waiter.setDaemon(true);
        waiter.start();

        // Still waiting: nothing has ended the instance, and nothing will until the callback below is registered.
        assertThat(returned.await(500, TimeUnit.MILLISECONDS)).isFalse();

        // Killed from the inside, as ControlThreadCompletionTest does it: nobody calls close, so the only thing
        // that can release the waiter is the control thread reporting its own death.
        started.processor().addLoopEndCallBack(() -> {
            throw new FakeRuntimeException("a loop-end callback that throws stops the consumer");
        });

        assertThat(returned.await(30, TimeUnit.SECONDS)).isTrue();
        handle = null;

        assertThat(thrown.get()).isInstanceOf(InstanceFailedException.class);
        assertThat(engineEndedAt.get()).isNotNull();
        Duration betweenEndingAndBeingTold = Duration.between(engineEndedAt.get(), releasedAt.get());
        assertWithMessage("the waiter is released by the engine's completion, not by the next tick of a poll")
                .that(betweenEndingAndBeingTold)
                .isLessThan(Duration.ofMillis(200));
    }

    /**
     * The second exit, from the waiter's side - and the thing the composed wait must not break. A stop request
     * closes through this handle, so the handle's own shutdown is what completes first; the engine's completion
     * follows, cleanly, and must not turn a stop into a failure.
     * <p>
     * It lives here rather than in {@link StopTheInstanceTest}, which owns everything stopping does to the records
     * and the offsets: what is asserted below is only the way {@code awaitShutdown} returns for one.
     */
    @Test
    void aStopRequestThatEndsItFirstReleasesTheWaiterCleanly() throws Exception {
        var pc = ParallelConsumer.connect(props()).whenClosing(ClosePath.DONT_DRAIN_FIRST);
        pc.string(TOPIC).process(context -> Outcome.stop("the deployment cannot handle this record"));
        ConsumerHandle started = runtime.startAndAssign(pc, 1);
        handle = started;

        var thrown = new AtomicReference<Throwable>();
        var returned = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try {
                started.awaitShutdown();
            } catch (Throwable failed) {
                thrown.set(failed);
            } finally {
                returned.countDown();
            }
        }, "await-shutdown-waiter");
        waiter.setDaemon(true);
        waiter.start();

        // Nothing has been published, so nothing has asked to stop yet.
        assertThat(returned.await(500, TimeUnit.MILLISECONDS)).isFalse();

        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        assertThat(returned.await(30, TimeUnit.SECONDS)).isTrue();
        handle = null;

        assertWithMessage("a stop is the definition's author asking for this, so there is nothing to rethrow")
                .that(thrown.get())
                .isNull();
        assertThat(started.stopRequest().isPresent()).isTrue();
        assertThat(started.stopRequest().get().reason()).contains("the deployment cannot handle this record");
        assertThat(started.failureCause().isPresent()).isFalse();
    }

    /**
     * Closing twice is closing once: the second caller waits for the first to finish rather than closing an
     * instance that is already gone.
     */
    @Test
    void doubleCloseIsIdempotent() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        ConsumerHandle started = runtime.startAndAssign(pc, 1);
        handle = started;

        started.close();
        Instant secondCallAt = Instant.now();
        started.close();
        handle = null;

        assertThat(Duration.between(secondCallAt, Instant.now())).isLessThan(Duration.ofSeconds(5));
        assertThat(started.failureCause().isPresent()).isFalse();
    }

    /**
     * A definition may be held in try-with-resources and never started - writing one is not starting one, and a
     * block that returns early must not fail on the way out.
     */
    @Test
    void closingADefinitionThatWasNeverStartedIsANoOp() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());

        pc.close();
        pc.close();

        // And it stays true of the promise the definition makes about itself: nothing was built.
        assertThat(runtime.builtNothing()).isTrue();
    }

    /**
     * A definition started twice would be two instances sharing one route table, one attempt ledger and one parked
     * set. It is refused - and refused before a second client is built, which is what the recording runtime is
     * counting here.
     */
    @Test
    void aSecondStartIsRefusedBeforeASecondProcessorIsBuilt() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> Outcome.succeeded());
        handle = runtime.startAndAssign(pc, 1);
        int clientsBuiltByTheFirstStart = runtime.consumerCalls + runtime.producerCalls;

        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> pc.start(runtime));

        assertThat(refused).hasMessageThat().contains("already been started");
        assertThat(runtime.consumerCalls + runtime.producerCalls).isEqualTo(clientsBuiltByTheFirstStart);
    }

    /**
     * A definition held in try-with-resources closes the instance it started, so a user who prefers one closeable
     * to two gets the same drain (R17).
     */
    @Test
    void closingTheDefinitionClosesTheInstanceItStarted() {
        var processed = new AtomicInteger();
        var entered = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> {
            processed.incrementAndGet();
            return Outcome.succeeded();
        });
        ConsumerHandle started = runtime.startAndAssign(pc, 1);
        handle = started;
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        Awaitility.await().atMost(defaultTimeout).until(() -> processed.get() == 1);

        pc.close();
        handle = null;

        assertThat(started.awaitShutdown(Duration.ofSeconds(5))).isTrue();
        assertThat(started.processor().isClosedOrFailed()).isTrue();
    }
}
