package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.internal.DrainingCloseable.DrainingMode.DONT_DRAIN;
import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.awaitLatch;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * Deterministic regression guard for the close-path interrupt livelock fixed in commit {@code 82d0bcfcb}
 * (astubbs#119).
 * <p>
 * <b>The defect:</b> the worker-pool {@code awaitTermination} retry loop in
 * {@code AbstractParallelEoSStreamProcessor#innerDoClose} caught {@link InterruptedException}, restored the
 * interrupt flag with {@code Thread.currentThread().interrupt()}, and retried. {@code awaitTermination} throws
 * <i>immediately</i> while the flag is set, so restore-then-retry is an infinite 100% CPU spin:
 * {@code shutdownNow()} is never reached, the user function is never interrupted, the pool never terminates,
 * and {@code waitForClose} times out (~24s, identical in every commit mode, because the spin sits upstream of
 * any mode-specific code).
 * <p>
 * <b>Why the existing guard was not one:</b> {@code executorThreadsInterruptedOnShutdownTimeout} only fails
 * when {@code pc-control} happens to carry a pending interrupt into the retry loop. {@code close()} interrupts
 * {@code pc-control} via {@code notifySomethingToDo()}, but unloaded, {@code pc-control} is almost always
 * parked in {@code processWorkCompleteMailBox}, whose catch consumes that interrupt - so the test passed in
 * isolation and failed only ~50% of the time under full-suite load.
 * <p>
 * <b>What this test does instead - arrange the interrupt, don't hope for it:</b>
 * <ol>
 *     <li>park a user function on a latch, so the pool cannot terminate and {@code pc-control} must sit
 *     blocked inside the first {@code awaitTermination} for the whole (short) shutdown timeout;</li>
 *     <li>wait until {@code pc-control}'s stack shows it inside
 *     {@code ThreadPoolExecutor.awaitTermination} - the exact frame the defect loops through;</li>
 *     <li>interrupt it, exactly once, from the test.</li>
 * </ol>
 * <b>The mechanism assertion:</b> the parked user function must observe {@link InterruptedException}. Only
 * {@code shutdownNow()} interrupts pool workers on this path, and reaching {@code shutdownNow()} after an
 * interrupt landed inside {@code awaitTermination} is precisely what the livelock makes impossible - the
 * defect arm spins in the catch and never interrupts the worker, so the latch never counts down. A generic
 * "close timed out" symptom cannot distinguish this livelock from any other hang; a worker interrupt
 * delivered <i>after</i> an in-{@code awaitTermination} interrupt can only mean the retry loop survived the
 * interrupt and progressed to {@code shutdownNow()}.
 * <p>
 * The arrangement is itself asserted (the injected interrupt must land before the shutdown timeout expires),
 * so a machine slow enough to void the setup fails loudly as an arrangement failure rather than passing
 * vacuously on both arms.
 */
@Timeout(90)
@Slf4j
class CloseInterruptLivelockTest extends ParallelEoSStreamProcessorTestBase {

    /**
     * Short, so the fixed arm resolves fast (one interrupted wait + one full retry ≈ 2 windows), while still
     * leaving a window orders of magnitude wider than the ~100ms the stack-poll needs to land the interrupt.
     */
    private static final Duration SHUTDOWN_TIMEOUT = ofSeconds(3);

    /**
     * Suffixes the control thread's name ({@code pc-control-<id>}), so the stack scan cannot confuse it with
     * another PC instance's control thread when the suite runs in parallel.
     */
    private static final String INSTANCE_ID = "close-livelock-guard";

    @Test
    void interruptLandingInAwaitWorkerTerminationStillReachesShutdownNow() throws InterruptedException {
        var workStarted = new CountDownLatch(1);
        var workInterrupted = new CountDownLatch(1);
        var releaseWork = new CountDownLatch(1);

        parallelConsumer.setMyId(Optional.of(INSTANCE_ID));

        // in-flight work that parks: the pool cannot terminate, so pc-control must WAIT inside
        // awaitTermination - giving the injected interrupt a wide, deterministic window to land in
        parallelConsumer.poll(recordContexts -> {
            workStarted.countDown();
            try {
                releaseWork.await();
            } catch (InterruptedException e) {
                // only shutdownNow() interrupts pool workers on the close path - reaching here after the
                // control-thread interrupt is the mechanism this test guards
                workInterrupted.countDown();
                Thread.currentThread().interrupt();
                throw new RuntimeException("parked work interrupted by shutdownNow, as close intends", e);
            }
        });
        primeFirstRecord();
        awaitLatch(workStarted);

        // close on another thread - close() blocks its caller in waitForClose
        Thread closer = new Thread(() -> parallelConsumer.close(SHUTDOWN_TIMEOUT, DONT_DRAIN),
                "test-livelock-closer");
        closer.start();
        try {
            // deterministic arrangement: wait until pc-control is inside ThreadPoolExecutor.awaitTermination,
            // then deliver the interrupt the defect turns into a livelock. Exactly once - repeated interrupts
            // would legitimately keep even a correct retry loop waiting.
            var controlThread = new AtomicReference<Thread>();
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(20)).until(() -> {
                Thread found = findControlThreadInsideAwaitTermination();
                controlThread.set(found);
                return found != null;
            });

            assertWithMessage("arrangement: the interrupt must be injected while the first awaitTermination " +
                    "window is still open - if the worker was already interrupted, shutdownNow() already ran " +
                    "and this run proves nothing about the livelock")
                    .that(workInterrupted.getCount())
                    .isEqualTo(1);

            log.debug("Interrupting {} inside awaitTermination", controlThread.get().getName());
            controlThread.get().interrupt();

            // MECHANISM: the retry loop must survive the interrupt and progress to shutdownNow(), which
            // interrupts the parked user function. Fixed arm: one full retry window (~3s) then shutdownNow.
            // Defect arm: restore-then-retry spins forever in the catch; this latch never counts down.
            boolean shutdownNowReachedWorker = workInterrupted.await(20, SECONDS);
            assertWithMessage("the parked user function was never interrupted: an interrupt delivered inside " +
                    "the awaitTermination retry loop prevented close from ever reaching shutdownNow() - the " +
                    "restore-then-retry interrupt livelock (astubbs#119, fix commit 82d0bcfcb)")
                    .that(shutdownNowReachedWorker)
                    .isTrue();
        } finally {
            // always unpark the work so close can complete even when the assertion fails (on the defect arm
            // the terminated pool is what breaks the spin - awaitTermination returns true without waiting)
            releaseWork.countDown();
            closer.join(SECONDS.toMillis(30));
        }

        assertWithMessage("close should have completed well within waitForClose's grace window")
                .that(closer.isAlive())
                .isFalse();
        await().atMost(ofSeconds(10)).until(parallelConsumer::isClosedOrFailed);
    }

    /**
     * Finds this test's control thread only if its current stack is inside
     * {@link java.util.concurrent.ThreadPoolExecutor#awaitTermination} - the frame the defect loops through.
     */
    private Thread findControlThreadInsideAwaitTermination() {
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            if (!entry.getKey().getName().equals("pc-control-" + INSTANCE_ID)) {
                continue;
            }
            for (StackTraceElement frame : entry.getValue()) {
                if (frame.getClassName().equals("java.util.concurrent.ThreadPoolExecutor")
                        && frame.getMethodName().equals("awaitTermination")) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }
}
