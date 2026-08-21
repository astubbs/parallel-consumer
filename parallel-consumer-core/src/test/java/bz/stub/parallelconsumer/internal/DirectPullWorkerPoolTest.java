package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.state.ModelUtils;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.ArgumentMatchers.anyInt;

/**
 * The idle path of the direct-pull engine - the part the 2022 attempt at this got wrong and nobody noticed.
 * <p>
 * WHY THIS EXISTS, and why it is worth more than the rest of the direct-pull coverage put together. The 2022
 * branch ({@code origin/refactor/gpt3-central-queue-direct-pull}) left its workers' idle path as
 * {@code while (element == null) element = tryOnce();} with the blocking wait commented out underneath. Every
 * idle worker burned a core. Nothing failed, no test noticed, and the throughput number that killed the branch
 * was measuring the spin rather than the architecture - a defect that cost a whole design and was invisible to
 * the entire suite. {@link #parkedWorkersDoNotSpin()} is the test that would have caught it.
 * <p>
 * The other three properties are the ones a park-based idle path can get wrong: a wakeup lost because work
 * arrived between a worker's scan and its park, a worker that never notices shutdown, and an announcement that
 * wakes fewer workers than there is work for.
 * <p>
 * WHY THE {@link WorkManager} IS A MOCK. These tests are about the pool's own protocol - how often it scans,
 * when it parks, when it wakes - so the scan has to be something the test can count and can hold open at an
 * exact moment. A real {@link WorkManager} would make the scan an uncontrollable black box and the assertions
 * would become timing guesses. The pool's behaviour against a real one is covered by
 * {@code DirectPullConcurrentSelectionTest} and by the whole core suite run with {@code -Dpc.directPull=true}.
 *
 * @author Antony Stubbs
 * @see DirectPullWorkerPool
 */
@Slf4j
class DirectPullWorkerPoolTest {

    /**
     * Comfortably longer than any scheduling hiccup, and comfortably SHORTER than the pool's five-second idle
     * safety net - so a test that passes proves the signal did the waking, not the timeout.
     */
    static final long PROMPTLY_MS = 2_000;

    final ModelUtils mu = new ModelUtils();

    /**
     * Work the stubbed {@link WorkManager} will hand out, one call at a time.
     */
    final ConcurrentLinkedQueue<WorkContainer<String, String>> available = new ConcurrentLinkedQueue<>();

    /**
     * How many times a worker has scanned. A spin shows up here as a number with six or more digits.
     */
    final AtomicInteger scans = new AtomicInteger();

    /**
     * Lets a test hold a worker INSIDE the scan, which is the window the lost-wakeup argument turns on.
     */
    volatile CountDownLatch scanGate = null;

    final AtomicBoolean canTakeWork = new AtomicBoolean(true);

    final List<List<WorkContainer<String, String>>> delivered = Collections.synchronizedList(new ArrayList<>());

    ExecutorService executor;
    DirectPullWorkerPool<String, String> pool;

    @SuppressWarnings("unchecked")
    private WorkManager<String, String> stubbedWorkManager() {
        WorkManager<String, String> wm = Mockito.mock(WorkManager.class);
        Mockito.when(wm.getWorkIfAvailable(anyInt())).thenAnswer(invocation -> {
            scans.incrementAndGet();
            int max = invocation.getArgument(0);
            List<WorkContainer<String, String>> batch = new ArrayList<>();
            for (int i = 0; i < max; i++) {
                var next = available.poll();
                if (next == null) {
                    break;
                }
                batch.add(next);
            }
            // The gate is held AFTER the queue is read, not before, and the order is the whole point: a worker
            // parked here has already decided it found nothing, so work published while it waits is work its
            // scan genuinely missed. Reading the queue after the gate instead makes the scan pick that work up
            // on the way out, the park is never reached, and the test proves nothing - which is exactly what
            // happened, and a mutation that removed the version re-check survived it.
            CountDownLatch gate = scanGate;
            if (gate != null) {
                gate.await();
            }
            return batch;
        });
        return wm;
    }

    private DirectPullWorkerPool<String, String> startPool(int concurrency) {
        return startPool(concurrency, batch -> delivered.add(batch));
    }

    private DirectPullWorkerPool<String, String> startPool(int concurrency,
                                                           java.util.function.Consumer<List<WorkContainer<String, String>>> runner) {
        executor = Executors.newFixedThreadPool(concurrency);
        pool = new DirectPullWorkerPool<>(stubbedWorkManager(), 1, canTakeWork::get, runner);
        pool.start(executor, concurrency);
        return pool;
    }

    @AfterEach
    void tearDown() {
        if (scanGate != null) {
            // never leave a worker wedged inside the stub on the way out
            scanGate.countDown();
        }
        if (pool != null) {
            pool.stop();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /**
     * THE TEST THE 2022 BRANCH NEEDED. Idle workers must park, not spin.
     * <p>
     * Four workers are started with nothing to do and left alone for a quarter of a second. Each should scan
     * once, find nothing and park - so the scan count settles in single figures. A busy-spin produces hundreds
     * of thousands, which is why the bound can be generous and still be decisive: there is no bound between
     * "parked" and "spinning" that any plausible scheduler could straddle.
     * <p>
     * The second assertion is the one that stops this passing for the wrong reason. A pool that never started
     * its workers at all would also record almost no scans - so every worker must be shown to have scanned at
     * least once before concluding that the low count means parking.
     */
    @Test
    void parkedWorkersDoNotSpin() throws Exception {
        int workers = 4;
        startPool(workers);

        // let them all reach their first scan and park
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (scans.get() < workers && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertWithMessage("every worker scanned at least once, so the low count below means parked, not unstarted")
                .that(scans.get()).isAtLeast(workers);

        int afterFirstScans = scans.get();
        Thread.sleep(250);
        int duringIdle = scans.get() - afterFirstScans;

        assertWithMessage("%s workers idle for 250ms rescanned %s times. Parked workers rescan only when told "
                        + "to, or once every five seconds; a busy-spin records this in the hundreds of thousands.",
                workers, duringIdle)
                .that(duringIdle).isAtMost(workers);
    }

    /**
     * NO LOST WAKEUPS. Work that becomes available while a worker is mid-scan must not leave it parked.
     * <p>
     * This is the race the version counter exists for, and it is driven here rather than hoped for: the worker
     * is held inside the scan by a latch, and the work is published and announced while it is stuck there. The
     * announcement finds nothing parked, so it signals nobody - the signal is genuinely lost, which is the
     * whole point. What has to save the worker is the version having been bumped before it re-checks under the
     * lock, so that it rescans instead of parking.
     * <p>
     * The deadline is two seconds against the pool's five-second safety net, so a pass cannot be the net firing.
     */
    @Test
    void workArrivingWhileAWorkerIsMidScanIsNotLost() throws Exception {
        var heldInScan = new CountDownLatch(1);
        scanGate = heldInScan;

        startPool(1);

        // wait until the worker is inside the scan, holding the (stale) version it read just before it
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (scans.get() < 1 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertWithMessage("the worker reached the scan").that(scans.get()).isAtLeast(1);

        // work appears, and is announced, while the worker is still inside the scan it will return empty from.
        // Nothing is parked, so this signals no one.
        var work = mu.createWorkFor(1L);
        available.add(work);
        pool.onWorkMaybeAvailable(1);

        // the scan the worker is in now completes, empty
        scanGate = null;
        heldInScan.countDown();

        assertWithMessage("the worker rescanned rather than parking on a wakeup that had already been missed")
                .that(awaitDelivery(1)).isTrue();
        assertThat(delivered.get(0)).containsExactly(work);
    }

    /**
     * An announcement must wake as many workers as it says there is work for.
     * <p>
     * The pool wakes {@code min(howMuch, parked)} rather than everything, so that a burst of newly polled
     * records does not send several thousand threads to contend over a handful of them. The failure mode on the
     * other side is starvation: wake fewer than there is work for and the surplus workers sit out the next five
     * seconds while records they could be processing wait. Its own javadoc says over-announcing is harmless and
     * under-announcing is not; this is what holds it to that.
     * <p>
     * THE RUNNER BLOCKS EACH WORKER UNTIL ALL OF THEM HAVE ARRIVED, and that is what makes the test decisive
     * rather than lucky. Counting delivered records alone does not distinguish "four workers woken" from "one
     * worker woken that went round the loop four times" - and it does not: a wake-one mutation passed a
     * record-counting version of this test. Holding each arrival makes the second case impossible, because the
     * one woken worker cannot go round again while it is inside the runner.
     */
    @Test
    void anAnnouncementWakesAsManyWorkersAsItHasWorkFor() throws Exception {
        int workers = 4;
        var allArrived = new CountDownLatch(workers);
        startPool(workers, batch -> {
            allArrived.countDown();
            try {
                // bounded, so a failing run tears down instead of wedging the executor
                allArrived.await(PROMPTLY_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (scans.get() < workers && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertWithMessage("all workers parked before the announcement").that(scans.get()).isAtLeast(workers);

        for (int i = 0; i < workers; i++) {
            available.add(mu.createWorkFor(i));
        }
        pool.onWorkMaybeAvailable(workers);

        assertWithMessage("all %s workers were woken and took a record each. %s were still parked, which is what "
                        + "a wake count that understates the announced work leaves behind - for the next five "
                        + "seconds", workers, allArrived.getCount())
                .that(allArrived.await(PROMPTLY_MS, TimeUnit.MILLISECONDS)).isTrue();
    }

    /**
     * SHUTDOWN with the workers parked, which is where direct pull differs from the engine PC ships: its
     * workers occupy their threads in a loop rather than sitting in the executor's queue, so
     * {@code ThreadPoolExecutor.shutdown()} alone would never terminate them - it only stops new tasks being
     * accepted.
     * <p>
     * Two seconds against the five-second safety net again: a pass means {@code stop()} woke them, not that
     * they timed out.
     */
    @Test
    void stopWakesEveryParkedWorkerRatherThanLeavingItOnTheSafetyNet() throws Exception {
        int workers = 4;
        startPool(workers);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (scans.get() < workers && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertWithMessage("all workers parked before the stop").that(scans.get()).isAtLeast(workers);

        pool.stop();
        executor.shutdown();

        assertWithMessage("every parked worker returned from its loop within %sms of stop()", PROMPTLY_MS)
                .that(executor.awaitTermination(PROMPTLY_MS, TimeUnit.MILLISECONDS)).isTrue();
    }

    /**
     * A worker whose user function throws must keep working.
     * <p>
     * The runner has already recorded the failure against every record in the batch and returned them to the
     * controller by the time it rethrows - it only rethrows because the executor-queue engine needs a failed
     * {@code Future}. Letting that out of the loop would retire the worker and shrink the configured
     * concurrency by one on every user-function exception, silently, with a throughput number nobody can
     * explain.
     */
    @Test
    void aWorkerSurvivesTheUserFunctionThrowing() throws Exception {
        var seen = Collections.synchronizedList(new ArrayList<List<WorkContainer<String, String>>>());
        var first = new AtomicBoolean(true);
        startPool(1, batch -> {
            seen.add(batch);
            if (first.getAndSet(false)) {
                throw new FakeRuntimeException("the user function threw, as user functions do");
            }
        });

        available.add(mu.createWorkFor(1L));
        pool.onWorkMaybeAvailable(1);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (seen.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertWithMessage("the first batch reached the runner").that(seen).hasSize(1);

        // the worker must still be alive to take this one
        available.add(mu.createWorkFor(2L));
        pool.onWorkMaybeAvailable(1);

        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (seen.size() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertWithMessage("the worker kept its thread after the user function threw - it must not retire and "
                + "take a slice of the configured concurrency with it")
                .that(seen).hasSize(2);
    }

    /**
     * Nothing may be taken while the engine says work is not to be handed out - which is what makes a pause
     * under direct pull exact rather than approximate, since there is no pre-loaded executor queue still
     * draining behind it.
     */
    @Test
    void noWorkIsTakenWhileTheEngineDisallowsIt() throws Exception {
        canTakeWork.set(false);
        startPool(2);

        available.add(mu.createWorkFor(1L));
        available.add(mu.createWorkFor(2L));
        pool.onWorkMaybeAvailable(2);

        Thread.sleep(300);
        assertWithMessage("no record may be handed to a worker while the engine is not accepting work")
                .that(delivered).isEmpty();
        assertWithMessage("and none may have been taken out of the work manager either")
                .that(available).hasSize(2);

        canTakeWork.set(true);
        pool.onWorkMaybeAvailable(2);

        assertWithMessage("and they are all delivered once it is accepting again")
                .that(awaitDelivery(2)).isTrue();
    }

    /**
     * @return true if at least {@code records} records were delivered within {@link #PROMPTLY_MS}
     */
    private boolean awaitDelivery(int records) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(PROMPTLY_MS);
        while (System.nanoTime() < deadline) {
            int count;
            synchronized (delivered) {
                count = delivered.stream().mapToInt(List::size).sum();
            }
            if (count >= records) {
                return true;
            }
            Thread.sleep(5);
        }
        return false;
    }
}
