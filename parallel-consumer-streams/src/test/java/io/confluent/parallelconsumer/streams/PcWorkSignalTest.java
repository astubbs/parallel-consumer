package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.confluent.parallelconsumer.streams.PreparedRecords.prepared;
import static io.confluent.parallelconsumer.streams.PreparedRecords.record;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The wake-on-work condition (astubbs#255, item 3), tested without Kafka Streams in the picture.
 * <p>
 * Everything here is about <em>timing</em>, which is the whole point: the defect being fixed is a thread that
 * waits when it should not, and the failure mode of a broken fix is a thread that waits anyway. So every
 * assertion below is on observed elapsed time, never on a counter that would read the same whether the wait
 * blocked or not. The budgets are chosen so that a correct implementation finishes in single-digit
 * milliseconds against a budget measured in seconds - wide enough for a loaded CI box, far too wide for a
 * broken implementation to slip through.
 * <p>
 * <b>The coincidences are forced, not merely made possible.</b> "A completion arrived while the thread was
 * parked" is a two-thread race, and a test that only creates the opportunity will pass for the wrong reason
 * most of the time. The releasing thread here waits on {@link PcDispatchCounters#getSplitPollWaits()}, which
 * is incremented at the moment the production code enters the wait, so the completion provably lands after
 * the wait began.
 *
 * @author Antony Stubbs
 * @see PcWorkSignal
 */
@Slf4j
// The registry, the counters and the switch are all process-wide - see PcDispatchSwitch on why there is no
// injection seam - so a concurrently running class would answer this one's gate and read its counters.
@Execution(ExecutionMode.SAME_THREAD)
@Isolated
class PcWorkSignalTest {

    private static final String TOPIC = "pc-streams-wake";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Set<TopicPartition> INPUT_PARTITIONS = UniSets.of(PARTITION);

    /**
     * Deliberately far larger than any real {@code poll.ms}. A correct early wake returns in milliseconds, so
     * the gap between "woke on work" and "waited it out" is three orders of magnitude and cannot be confused
     * with scheduler noise.
     */
    private static final Duration GENEROUS_BUDGET = Duration.ofSeconds(10);

    /** Used where the test wants the wait to actually run out, so it has to be short enough to be bearable. */
    private static final Duration SHORT_BUDGET = Duration.ofMillis(400);

    private PcTaskDispatcher dispatcher;

    @BeforeEach
    void reset() {
        PcDispatchCounters.reset();
        PcWorkSignal.resetRegistryForTests();
        PcDispatchSwitch.resetToDefault();
    }

    @AfterEach
    void closeDispatcher() {
        if (dispatcher != null) {
            dispatcher.close();
            dispatcher = null;
        }
        PcWorkSignal.resetRegistryForTests();
        PcDispatchSwitch.resetToDefault();
    }

    /**
     * The seam-off / no-dispatcher case, and the most important one to get right: with nothing registered the
     * StreamThread must take the stock full-budget poll. Splitting the wait when nothing can wake it would
     * shorten every poll to a millisecond and delay broker records for no gain whatever - a regression against
     * stock rather than a fix.
     */
    @Test
    void withNoDispatcherOnThisThreadTheGateIsShutAndTheWaitIsANoOp() {
        assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread())
                .as("no dispatcher belongs to this thread, so nothing can ever wake a split wait - the "
                        + "StreamThread must keep its stock poll")
                .isFalse();

        long elapsed = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(GENEROUS_BUDGET));

        assertThat(elapsed)
                .as("and awaiting on a thread with no signal must return at once rather than parking for the "
                        + "budget - a StreamThread that reached here by mistake must not be stalled by it")
                .isLessThan(1_000L);
        assertThat(PcDispatchCounters.getSplitPollWaits())
                .as("no wait was taken, so nothing may be counted as one")
                .isZero();
    }

    /** A registered but idle dispatcher is still the stock case: nothing is running, so nothing can complete. */
    @Test
    void anIdleDispatcherLeavesTheGateShut() {
        dispatcher = new PcTaskDispatcher("wake-idle", INPUT_PARTITIONS, 4);

        assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread())
                .as("nothing in flight and no outcome pending means the full-budget poll is exactly right")
                .isFalse();
    }

    /** In-flight work is what makes splitting the wait worth anything. */
    @Test
    void workInFlightOpensTheGate() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("wake-inflight", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);

        awaitWorkerRunning(dispatchOneBlockingOn(release));

        try {
            assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread())
                    .as("a record is in the chain, so a completion can arrive and end the wait early")
                    .isTrue();
        } finally {
            release.countDown();
        }
    }

    /**
     * <b>The lost-wakeup case.</b> A completion that landed <em>before</em> the wait began must still end it.
     * <p>
     * This is the test that an edge-triggered "a completion happened" flag fails: the flag would be armed at
     * the start of the wait, after the completion set it, so the signal would be cleared unread and the thread
     * would park for the whole budget with work already in hand. The predicate here is the live completions
     * queue, which is why it cannot happen.
     */
    @Test
    void aCompletionThatArrivedBeforeTheWaitStillEndsIt() {
        dispatcher = new PcTaskDispatcher("wake-prior", INPUT_PARTITIONS, 4);

        dispatchOneBlockingOn(new CountDownLatch(0));
        await().atMost(Duration.ofSeconds(30))
                .until(() -> dispatcher.hasPendingCompletions());

        long elapsed = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(GENEROUS_BUDGET));

        assertThat(elapsed)
                .as("the outcome was already in the mailbox when the wait started, so the wait must not have "
                        + "blocked at all. Parking here is the lost-wakeup defect, and it costs a full poll "
                        + "cycle per record.")
                .isLessThan(1_000L);
        assertThat(PcDispatchCounters.getWakesOnWork())
                .as("and it must be recorded as a wake, not as a timeout")
                .isEqualTo(1);
    }

    /**
     * The live case: a worker completes while the thread is parked, and the thread leaves immediately.
     * <p>
     * The completion is forced to land after the wait began - see the class javadoc - rather than merely being
     * likely to.
     */
    @Test
    void aCompletionDuringTheWaitEndsItImmediately() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("wake-during", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);

        awaitWorkerRunning(dispatchOneBlockingOn(release));

        Thread releaser = runOnceTheWaitHasBegun("wake-on-work-releaser", release::countDown);

        long elapsed = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(GENEROUS_BUDGET));
        releaser.join(TimeUnit.SECONDS.toMillis(30));

        assertThat(elapsed)
                .as("the worker finished while the thread was parked, so the thread must leave on the "
                        + "completion rather than on the %s budget. This IS wake-on-work.", GENEROUS_BUDGET)
                .isLessThan(GENEROUS_BUDGET.toMillis() / 2);
        assertThat(PcDispatchCounters.getSplitPollWaits()).isEqualTo(1);
        assertThat(PcDispatchCounters.getWakesOnWork())
                .as("and it left because work arrived, which is the distinction the two counters exist to make")
                .isEqualTo(1);
        assertThat(dispatcher.getInFlightCount())
                .as("THE ORDERING INVARIANT. The signal is raised after the in-flight count is decremented, so "
                        + "a woken thread sees the freed pool slot. Signal first and it would drain the "
                        + "completion, compute capacity against a count not yet decremented, dispatch nothing, "
                        + "and park again with an empty mailbox and no further signal coming - a full "
                        + "poll-budget stall, microseconds wide, that would never reproduce on demand.")
                .isZero();
    }

    /**
     * The failure path raises the signal too. A failed record frees its pool slot exactly like a successful
     * one, and this is the branch a later refactor forgets - leaving a StreamThread parked for the full budget
     * after a processor threw.
     */
    @Test
    void aWorkerFailureAlsoEndsTheWait() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("wake-failure", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);

        registerOneRecord();
        int dispatched = dispatcher.dispatchAvailable(record -> prepared(record, () -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("processor blew up");
        }));
        assertThat(dispatched).isEqualTo(1);
        awaitWorkerRunning(started);

        Thread releaser = runOnceTheWaitHasBegun("wake-on-work-failure-releaser", release::countDown);

        long elapsed = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(GENEROUS_BUDGET));
        releaser.join(TimeUnit.SECONDS.toMillis(30));

        assertThat(elapsed)
                .as("a failure is an outcome, and an outcome frees a slot - the thread must wake for it")
                .isLessThan(GENEROUS_BUDGET.toMillis() / 2);
        assertThat(dispatcher.getRecordsFailed()).isEqualTo(1);
        assertThat(dispatcher.getInFlightCount())
                .as("and the same ordering invariant holds on the failure path")
                .isZero();
    }

    /**
     * The negative control for the test above. With work in flight but no completion coming, the wait must run
     * to its deadline - if it returned early anyway, the early return in the other test would prove nothing.
     * <p>
     * This also covers spurious wakeups: {@code Object.wait} may return without any notify at all, and the
     * loop re-checks its predicate rather than trusting the return.
     */
    @Test
    void withNothingCompletingTheWaitRunsToItsDeadline() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("wake-timeout", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);

        awaitWorkerRunning(dispatchOneBlockingOn(release));

        try {
            long elapsed = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(SHORT_BUDGET));

            assertThat(elapsed)
                    .as("nothing completed, so the wait must last its budget. A wait that ends early anyway "
                            + "would make the early return in the wake test meaningless.")
                    .isGreaterThanOrEqualTo(SHORT_BUDGET.toMillis() - PcWorkSignal.SHORT_POLL.toMillis() - 50);
            assertThat(PcDispatchCounters.getSplitPollWaits()).isEqualTo(1);
            assertThat(PcDispatchCounters.getWakesOnWork())
                    .as("it timed out - counting that as a wake would hide a mechanism that never pays")
                    .isZero();
        } finally {
            release.countDown();
        }
    }

    /**
     * The shutdown path (R5), and the reason the trap this design avoids is a shutdown-path hazard.
     * <p>
     * {@code StreamThread.shutdown()} runs on the closing thread, not the parked one, and only sets a state
     * flag - so the patched shutdown asks this class to end the wait. Note it must end the wait even though
     * <em>no work arrived</em>, which a plain {@code notifyAll} against the work predicate cannot do.
     */
    @Test
    void wakeOwnerEndsTheWaitEvenWithNoWorkArriving() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("wake-shutdown", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);

        awaitWorkerRunning(dispatchOneBlockingOn(release));

        Thread owner = Thread.currentThread();
        Thread shutdowner = runOnceTheWaitHasBegun("wake-on-work-shutdowner", () -> PcWorkSignal.wakeOwner(owner));

        try {
            long elapsed = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(GENEROUS_BUDGET));
            shutdowner.join(TimeUnit.SECONDS.toMillis(30));

            assertThat(elapsed)
                    .as("a shutdown must not have to sit out the remaining poll budget - this is the wait we "
                            + "own, so we can end it")
                    .isLessThan(GENEROUS_BUDGET.toMillis() / 2);
            assertThat(PcDispatchCounters.getWakesOnWork())
                    .as("but it is NOT a wake-on-work: no completion arrived, and conflating the two would "
                            + "make a shutdown look like the mechanism paying off")
                    .isZero();
        } finally {
            release.countDown();
        }
    }

    /**
     * A closed dispatcher must stop answering the gate. Left registered, it would keep its StreamThread on the
     * split-wait branch forever, waiting for completions from a worker pool that is gone - and both close
     * paths matter, because {@code abortClose} is the crash-injection path a test drives deliberately.
     */
    @Test
    void closingADispatcherShutsTheGateAgain() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("wake-close", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);

        awaitWorkerRunning(dispatchOneBlockingOn(release));
        assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread()).isTrue();

        release.countDown();
        dispatcher.close();
        dispatcher = null;

        // Asserted on the REGISTRY, not on the gate. An orderly close drains and awaits its pool, so the gate
        // reads false afterwards whether or not the deregistration happened - an assertion on the gate here
        // passes with the production deregistration deleted, which makes it worth nothing.
        assertThat(PcWorkSignal.registeredDispatchersOnCurrentThread())
                .as("a closed dispatcher must deregister, or it keeps answering the gate for work that can "
                        + "never arrive")
                .isZero();
        assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread()).isFalse();
    }

    /**
     * <b>The anti-spin property, and the one that a purely level-triggered wait gets wrong.</b>
     * <p>
     * The wake predicate is "an outcome is pending", and outcomes are only cleared by the StreamThread inside
     * {@code dispatchAvailable}. Kafka does not always get there: {@code KafkaStreams.pause()} makes
     * {@code TaskExecutionMetadata.canProcessTask} answer no, so {@code process()} is skipped entirely while
     * the thread stays RUNNING and keeps polling. A wait that left on the pending outcome every time would
     * then return instantly on every pass, turning the poll phase into a ~1kHz loop of 1ms polls - a pinned
     * core and a thousand fetches a second - until something else happened to drain it.
     * <p>
     * So the second wait, with the outcome still undrained and nothing new signalled, must take its budget.
     */
    @Test
    void aSecondWaitWithNothingNewDrainedMustNotReturnInstantly() {
        dispatcher = new PcTaskDispatcher("wake-nodrain", INPUT_PARTITIONS, 4);

        dispatchOneBlockingOn(new CountDownLatch(0));
        await().atMost(Duration.ofSeconds(30))
                .until(() -> dispatcher.hasPendingCompletions());

        long first = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(SHORT_BUDGET));
        assertThat(first)
                .as("the first wait leaves at once - there is a real outcome to feed back")
                .isLessThan(SHORT_BUDGET.toMillis() / 2);

        // Deliberately NOT draining, which is exactly what a paused topology does to this thread.
        assertThat(dispatcher.hasPendingCompletions())
                .as("the fixture only means anything while the outcome is still undrained")
                .isTrue();

        long second = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(SHORT_BUDGET));
        assertThat(second)
                .as("nothing new arrived and nobody drained, so this wait must take its budget. Returning "
                        + "instantly here is a busy-spin on the StreamThread, not a fast path.")
                .isGreaterThanOrEqualTo(SHORT_BUDGET.toMillis() - PcWorkSignal.SHORT_POLL.toMillis() - 50);
    }

    /** The same, through the crash path, which does not drain and must still deregister. */
    @Test
    void abortingADispatcherShutsTheGateAgain() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("wake-abort", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);

        awaitWorkerRunning(dispatchOneBlockingOn(release));

        dispatcher.abortClose();
        release.countDown();

        assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread())
                .as("a crashed dispatcher deregisters too - otherwise the crash-injection tests would leave a "
                        + "phantom contributor holding every later StreamThread on the split-wait branch")
                .isFalse();
    }

    /**
     * <b>The rebalance re-check</b>, which is the one branch the gate cannot cover.
     * <p>
     * The StreamThread asks the gate, then runs a short poll, and only then waits - and a poll runs
     * {@code ConsumerRebalanceListener} callbacks inline. A revocation there tears this thread's tasks down and
     * deregisters their dispatchers, so by the time the wait begins the work the gate said yes to is gone.
     * Without the second check inside {@link PcWorkSignal#awaitWorkForRemainderOf} the thread would then park
     * for its whole budget on work that can never complete, in the middle of a rebalance, which is exactly when
     * the consumer most needs polling.
     * <p>
     * Deleting that check leaves every other test in this class green, because they all reach the wait with
     * either no signal at all or genuine work still in flight. This is the test that turns red.
     */
    @Test
    void aDispatcherThatWentAwayBeforeTheWaitMakesItANoOp() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("wake-revoked", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);

        awaitWorkerRunning(dispatchOneBlockingOn(release));
        assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread())
                .as("the gate must be open first, or the re-check below is not the thing being tested")
                .isTrue();

        // The revocation, standing in for the one that would have run inside the short poll: the task goes away
        // on this very thread, between the gate saying yes and the wait starting.
        dispatcher.abortClose();
        release.countDown();

        long elapsed = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(GENEROUS_BUDGET));

        assertThat(elapsed)
                .as("the work the gate approved no longer exists, so the wait must be a no-op rather than a "
                        + "%s park during a rebalance", GENEROUS_BUDGET)
                .isLessThan(GENEROUS_BUDGET.toMillis() / 2);
        assertThat(PcDispatchCounters.getSplitPollWaits())
                .as("and it must not even be counted as a split wait - the thread never parked, and a counter "
                        + "that says otherwise would overstate how often the mechanism ran")
                .isZero();
    }

    /**
     * The kill switch, which is also the benchmark's control arm - so it has to genuinely restore the stock
     * path rather than merely skip the wait.
     */
    @Test
    void theKillSwitchShutsTheGateWhileWorkIsStillInFlight() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("wake-killswitch", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);

        awaitWorkerRunning(dispatchOneBlockingOn(release));

        try {
            assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread()).isTrue();

            PcDispatchSwitch.setWakeOnWork(false);
            assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread())
                    .as("-D%s=false must put the StreamThread back on one full-budget poll, which is what "
                            + "makes the before/after measurement a one-term control",
                            PcDispatchSwitch.WAKE_ON_WORK_PROPERTY)
                    .isFalse();

            PcDispatchSwitch.disable();
            PcDispatchSwitch.setWakeOnWork(true);
            assertThat(PcWorkSignal.hasActiveWorkOnCurrentThread())
                    .as("and with the seam itself off there are no workers at all, so wake-on-work must stay "
                            + "shut regardless of its own property")
                    .isFalse();
        } finally {
            release.countDown();
        }
    }

    // ---------------------------------------------------------------------------------------------------

    /**
     * Registers one record and dispatches it to a worker that blocks until {@code release} opens.
     *
     * @return a latch that opens once the worker is genuinely inside the chain. Waiting on it matters:
     *         {@code inFlight} is incremented on the <em>dispatching</em> thread before submission, so a test
     *         that only checked the count could set up its race before any worker existed, and the
     *         "completion arrived during the wait" it thinks it forced would be something else entirely.
     */
    private CountDownLatch dispatchOneBlockingOn(final CountDownLatch release) {
        registerOneRecord();

        CountDownLatch started = new CountDownLatch(1);
        int dispatched = dispatcher.dispatchAvailable(record -> prepared(record, () -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        assertThat(dispatched)
                .as("the fixture must actually dispatch, or every timing assertion below measures nothing")
                .isEqualTo(1);
        return started;
    }

    private void registerOneRecord() {
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>();
        // The shared fixture, not a hand-rolled ConsumerRecord: the short constructor leaves the timestamp
        // at NO_TIMESTAMP, and prepared() takes the stream timestamp from the record (astubbs#255, U13).
        batch.add(record(PARTITION, 0L, "the-key"));
        dispatcher.registerRecords(PARTITION, batch);
    }

    /**
     * Starts a daemon thread that runs {@code action} once the production code has provably entered its wait.
     * <p>
     * The gate is {@link PcDispatchCounters#getSplitPollWaits()}, which the production code increments at the
     * moment it parks - a latch on the real hook rather than sleep arithmetic. That is what turns "a completion
     * could arrive while the thread is parked" into "a completion did", and it is the reason these tests are
     * not passing for the wrong reason most of the time. See the class javadoc.
     * <p>
     * Named and shared rather than repeated at each site: three tests force the same coincidence and only the
     * action differs, and a copy that quietly loses the counter gate would still pass while proving nothing.
     *
     * @return the started thread, so the caller can join it before asserting
     */
    private static Thread runOnceTheWaitHasBegun(final String threadName, final Runnable action) {
        Thread thread = new Thread(() -> {
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> PcDispatchCounters.getSplitPollWaits() > 0);
            action.run();
        }, threadName);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void awaitWorkerRunning(final CountDownLatch started) throws InterruptedException {
        assertThat(started.await(30, TimeUnit.SECONDS))
                .as("the worker must actually be inside the chain before the race is set up")
                .isTrue();
    }

    private static long timeMillis(final Runnable action) {
        long startNanos = System.nanoTime();
        action.run();
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
