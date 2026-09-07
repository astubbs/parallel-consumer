package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModule;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.awaitLatch;

/**
 * Test instrument for the confluentinc#909 registration race (fork fix: astubbs#31): parks the CONTROL
 * thread at the head of exactly one designated insert of
 * {@code PartitionState.maybeRegisterNewPollBatchAsWork}'s per-record loop - i.e. AFTER that batch has
 * already passed the {@code epochIsStale} guard - so a test can land a real broker-driven rebalance
 * inside the check-then-act window the race needs, instead of soaking and hoping one lands there by
 * chance.
 * <p>
 * One-shot by design: the pause fires on the FIRST insert of {@code pauseAtOffset} only. The re-delivered
 * (fresh-epoch) insert of the same offset - the arrival whose collision with the stale resident is the
 * defect under test - must sail through. {@link #getFreshEpochInsertCount()} counts the inserts arriving
 * with an epoch NEWER than the paused batch's, so the test can tell when the re-delivered batch has fully
 * registered (collisions included) and only then unblock the worker pipeline.
 * <p>
 * Lives in this package because {@link ShardManager#addWorkContainer(long, ConsumerRecord)} is
 * deliberately package-private; injected via {@link PCModule#shardManager(WorkManager)}, so production
 * code carries no test hook.
 */
@Slf4j
public class PausableInsertShardManager extends ShardManager<String, String> {

    /**
     * Park budget. The clock starts at the pause point, and everything the test does while the control
     * thread is parked must fit inside it: parking-topic creation plus the rebalance-settle await (itself
     * up to 60s, and its 5s-quiet condition resets on every rebalance event). At the original 60s the park
     * could expire INSIDE those budgets on a slow CI box, killing a valid run as a misleading
     * "wiring failure" - in a repo where a flake fails the build.
     * <p>
     * It bounds this park ONLY. A test's own gates are anchored to the test's overall budget instead - the
     * park is not the longest thing a worker or a latch waits through, so deriving another deadline from
     * this constant anchors it to an event that has not happened yet.
     * <p>
     * Worst case it is the whole of that budget: {@code awaitLatch} re-arms after each swallowed interrupt
     * (see below), so an interrupt storm can spend the full deadline here, and the instrument's own
     * narration is mute below {@code warn} while it does.
     */
    public static final int PARK_DEADLINE_SECONDS = 150;

    private final long pauseAtOffset;
    private final CountDownLatch reachedPausePoint = new CountDownLatch(1);
    private final CountDownLatch resume = new CountDownLatch(1);
    private final AtomicBoolean pauseFired = new AtomicBoolean(false);
    private volatile long pausedBatchEpoch = -1;

    /** Inserts seen with an epoch newer than the paused batch's - the re-delivered records. */
    @Getter
    private final AtomicInteger freshEpochInsertCount = new AtomicInteger();

    public PausableInsertShardManager(PCModule<String, String> module, WorkManager<String, String> wm,
                                      long pauseAtOffset) {
        super(module, wm);
        this.pauseAtOffset = pauseAtOffset;
    }

    @Override
    void addWorkContainer(long epochOfInboundRecords, ConsumerRecord<String, String> aRecord) {
        if (aRecord.offset() == pauseAtOffset && pauseFired.compareAndSet(false, true)) {
            pausedBatchEpoch = epochOfInboundRecords;
            log.info("Pausing control thread mid-registration-loop at first insert of offset {} (epoch {})",
                    pauseAtOffset, epochOfInboundRecords);
            reachedPausePoint.countDown();
            // PC wakes its control thread by INTERRUPTING it (notifySomethingToDo) whenever the poll
            // thread delivers new work - including the rebalance this test triggers on purpose. So the
            // park must dwell THROUGH interrupts, or the wake-up aborts the experiment - exactly what the
            // shared awaitLatch does: swallow the interrupt, re-wait, and fail loudly on deadline. The
            // swallowed interrupts are advisory wake-ups only - the mailbox data they announce is still
            // there when the loop resumes.
            try {
                awaitLatch(resume, PARK_DEADLINE_SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Test wiring failure: resume latch never released within " + PARK_DEADLINE_SECONDS
                                + "s - failing loudly rather than hanging", e);
            }
            log.info("Control thread released; completing registration of the now-stale batch (epoch {})",
                    epochOfInboundRecords);
        }
        super.addWorkContainer(epochOfInboundRecords, aRecord);
        // Counted AFTER the insert, not before: the test's gate-open condition polls this from the
        // awaitility thread and its comment claims every re-delivered record has REGISTERED. Counting
        // first makes the counter mean "arrived at the insert", so the final record's registration
        // could still be pending when the gate opens. One statement's window on the control thread,
        // but the cheap fix is to make the code say what the comment says.
        // pausedBatchEpoch >= 0 alone implies the pause fired: it is written only inside the CAS-guarded
        // block above, and addWorkContainer is only ever called on the (single) control thread
        if (pausedBatchEpoch >= 0 && epochOfInboundRecords > pausedBatchEpoch) {
            freshEpochInsertCount.incrementAndGet();
        }
    }

    /** Blocks until the control thread is parked inside the registration loop. */
    public boolean awaitPausePoint(long timeout, TimeUnit unit) throws InterruptedException {
        return reachedPausePoint.await(timeout, unit);
    }

    /** Lets the parked registration loop finish inserting the rest of its (now stale) batch. */
    public void releaseRegistrationLoop() {
        resume.countDown();
    }
}
