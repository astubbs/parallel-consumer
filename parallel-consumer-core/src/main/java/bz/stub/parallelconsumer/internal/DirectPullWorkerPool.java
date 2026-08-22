package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * MEASUREMENT ONLY. Workers that pull their own work straight out of the shards, with no intermediate queue.
 * <p>
 * The engine PC ships pre-loads a {@link java.util.concurrent.ThreadPoolExecutor}'s work queue: the control loop is
 * the only thread that ever selects work, and a pressure system ({@code DynamicLoadFactor},
 * {@code checkPipelinePressure}, {@code isPoolQueueLow}, {@code getQueueTargetLoaded}) exists purely to keep that
 * queue at the right depth, which it measures by reading {@code ThreadPoolExecutor.getQueue().size()}. Here the queue
 * is gone: each worker calls {@link WorkManager#getWorkIfAvailable(int)} itself, so there is no depth to target and
 * none of that machinery runs. What it buys, and what it costs, is the subject of
 * {@code docs/inflight/perf-direct-pull-measured.md}.
 *
 * <h2>The blocking wait, which is the whole point</h2>
 *
 * The 2022 attempt at this ({@code origin/refactor/gpt3-central-queue-direct-pull}, class
 * {@code QueuedShardManager}) left its idle path as {@code while (element == null) element = tryOnce();} - a
 * busy-spin, with the blocking wait commented out beneath it. Idle workers burned a core each, and the throughput
 * number that killed the branch measured that rather than the architecture.
 * <p>
 * So the idle path here parks, and does so without a lost-wakeup window and without a scan under a shared lock:
 * <ol>
 *     <li>a worker reads {@link #workVersion}, <em>then</em> scans the shards, lock-free;</li>
 *     <li>finding nothing, it takes {@link #lock} and compares the version it read against the current one. A
 *     producer that made work available in between has already bumped the version, so the worker rescans instead of
 *     parking;</li>
 *     <li>otherwise it {@link Condition#await(long, TimeUnit)}s, releasing the lock atomically.</li>
 * </ol>
 * The producer side ({@link #onWorkMaybeAvailable(int)}) bumps the version <em>before</em> signalling, and signals at
 * most as many workers as there is work to do, so a burst of newly polled records does not wake every parked thread
 * to lose a race. The timeout on the await is a safety net for work that becomes selectable through the passage of
 * time alone - a retry delay expiring - and is deliberately far longer than any scheduling latency, so it can never
 * be doing the scheduling itself.
 */
@Slf4j
public class DirectPullWorkerPool<K, V> {

    /**
     * Upper bound on how long a parked worker sleeps before rescanning unprompted.
     * <p>
     * Not a poll interval: every path that makes work available signals, and the version check makes that signal
     * impossible to miss. This exists only for work that becomes selectable with nobody to signal it - a retry delay
     * expiring while the control loop happens to be blocked - and is long enough that if it were ever the mechanism
     * keeping the system moving, throughput would collapse visibly rather than quietly degrade.
     */
    private static final long IDLE_SAFETY_NET_MS = 5_000;

    private final WorkManager<K, V> wm;

    /**
     * How many records one worker claims per pull. Matches the configured batch size, so a worker's unit of work is
     * the same as the one the pre-loaded-queue engine submits.
     */
    private final int batchSize;

    private final Consumer<List<WorkContainer<K, V>>> runner;

    /**
     * Whether the engine is in a state that permits handing out work, mirroring the {@code RUNNING || DRAINING} test
     * the default engine applies before distributing.
     */
    private final BooleanSupplier canTakeWork;

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition workAvailable = lock.newCondition();

    /**
     * Bumped every time work may have become selectable. Read before a scan and re-read under the lock before
     * parking, which is what closes the lost-wakeup window without holding the lock across a shard scan.
     */
    private final AtomicLong workVersion = new AtomicLong();

    /**
     * Parked worker count, guarded by {@link #lock}. Lets the producer wake exactly as many workers as it has work
     * for, rather than {@code signalAll} sending several thousand threads to contend over a handful of records.
     */
    private int parked = 0;

    private volatile boolean running = true;

    /**
     * Set when a worker was allowed to take work and found none - the direct-pull equivalent of the shipped engine's
     * {@code isPoolQueueLow()}.
     * <p>
     * Direct pull removes the executor queue, but it does <em>not</em> remove the buffer of records polled from the
     * broker and sitting in the shards: {@code WorkManager#isSufficientlyLoaded()} still sizes that against
     * {@link DynamicLoadFactor}, and still pauses the poller when it is full. If nothing ever stepped that factor up
     * here, direct pull would be measured against a buffer two deep while the shipped engine ran with one up to a
     * hundred deep, and the comparison would be of buffer depths rather than of engines. So the factor survives -
     * what is gone is the {@code ThreadPoolExecutor.getQueue().size()} reading that used to drive it.
     */
    private final AtomicBoolean starvedSinceLastCheck = new AtomicBoolean();

    public DirectPullWorkerPool(WorkManager<K, V> wm,
                                int batchSize,
                                BooleanSupplier canTakeWork,
                                Consumer<List<WorkContainer<K, V>>> runner) {
        this.wm = wm;
        this.batchSize = Math.max(1, batchSize);
        this.canTakeWork = canTakeWork;
        this.runner = runner;
    }

    /**
     * Starts {@code concurrency} worker loops on the supplied executor. Each occupies one thread for the pool's
     * lifetime, so the executor must have at least that many threads - which the core pool already does, it is sized
     * to {@code maxConcurrency}.
     */
    public void start(Executor executor, int concurrency) {
        log.info("Direct-pull engine: starting {} workers pulling straight from the shards", concurrency);
        for (int i = 0; i < concurrency; i++) {
            executor.execute(this::workerLoop);
        }
    }

    private void workerLoop() {
        Thread.currentThread().setName("pc-pull-" + Thread.currentThread().getId());
        try {
            while (running) {
                List<WorkContainer<K, V>> work = takeBlocking();
                if (work == null) {
                    return;
                }
                try {
                    runner.accept(work);
                } catch (Exception e) {
                    // The runner has already recorded the failure against every record in the batch and returned
                    // them to the controller; it rethrows only because the executor-queue engine needs a failed
                    // Future. Letting it out of the loop would retire this worker and silently shrink the
                    // configured concurrency by one on every user-function exception.
                    log.debug("User function threw, already handled by the runner - worker continues", e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Direct-pull worker interrupted, exiting");
        } catch (Exception e) {
            // Never silently: a worker that dies takes a slice of the concurrency with it, and the symptom is a
            // throughput number nobody can explain.
            log.error("Direct-pull worker died", e);
        }
    }

    /**
     * @return work claimed by this worker, or {@code null} if the pool is shutting down
     */
    private List<WorkContainer<K, V>> takeBlocking() throws InterruptedException {
        while (running) {
            // Read the version BEFORE the scan - the whole no-lost-wakeup argument rests on that order.
            long seen = workVersion.get();

            if (canTakeWork.getAsBoolean()) {
                List<WorkContainer<K, V>> work = wm.getWorkIfAvailable(batchSize);
                if (!work.isEmpty()) {
                    return work;
                }
                // Allowed to work, nothing there: the buffer is not keeping the workers fed.
                starvedSinceLastCheck.set(true);
            }

            lock.lock();
            try {
                if (!running) {
                    return null;
                }
                if (workVersion.get() != seen) {
                    // Something became available while we were scanning. Rescan rather than park.
                    continue;
                }
                parked++;
                try {
                    workAvailable.await(IDLE_SAFETY_NET_MS, TimeUnit.MILLISECONDS);
                } finally {
                    parked--;
                }
            } finally {
                lock.unlock();
            }
        }
        return null;
    }

    /**
     * Announces that up to {@code howMuch} records may now be selectable, waking at most that many parked workers.
     * <p>
     * Called from the control loop once per pass, after it has drained the mailbox - which is where newly polled
     * records are registered and where returned records become selectable again. Over-announcing is harmless (the
     * woken worker finds nothing and parks again); under-announcing is not, which is why the count is the shards'
     * own view of available work rather than a delta.
     */
    public void onWorkMaybeAvailable(int howMuch) {
        if (howMuch <= 0) {
            return;
        }
        // Bump before signalling, so a worker that scanned and missed sees the change when it re-checks under the
        // lock, whether or not it was parked in time to be signalled.
        workVersion.incrementAndGet();
        lock.lock();
        try {
            int toWake = Math.min(howMuch, parked);
            for (int i = 0; i < toWake; i++) {
                workAvailable.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return whether any worker went hungry since this was last called, clearing the signal
     * @see #starvedSinceLastCheck
     */
    public boolean consumeStarvationSignal() {
        return starvedSinceLastCheck.getAndSet(false);
    }

    /**
     * Stops the workers. In-flight user functions run to completion - this only stops the loops asking for more.
     */
    public void stop() {
        log.debug("Direct-pull engine: stopping workers");
        running = false;
        workVersion.incrementAndGet();
        lock.lock();
        try {
            workAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
