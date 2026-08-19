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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

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
            // park must dwell THROUGH interrupts (same idiom as the chaos suite's non-interruptible heavy
            // sleep), or the wake-up aborts the experiment. The swallowed interrupts are advisory
            // wake-ups only - the mailbox data they announce is still there when the loop resumes.
            long deadline = System.currentTimeMillis() + SECONDS.toMillis(60);
            boolean released = false;
            while (!released) {
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) {
                    throw new IllegalStateException(
                            "Test wiring failure: resume latch never released - failing loudly rather than hanging");
                }
                try {
                    released = resume.await(left, MILLISECONDS);
                } catch (InterruptedException e) {
                    log.debug("Ignoring control-thread wake-up interrupt while parked at the pause point");
                }
            }
            log.info("Control thread released; completing registration of the now-stale batch (epoch {})",
                    epochOfInboundRecords);
        }
        if (pauseFired.get() && pausedBatchEpoch >= 0 && epochOfInboundRecords > pausedBatchEpoch) {
            freshEpochInsertCount.incrementAndGet();
        }
        super.addWorkContainer(epochOfInboundRecords, aRecord);
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
