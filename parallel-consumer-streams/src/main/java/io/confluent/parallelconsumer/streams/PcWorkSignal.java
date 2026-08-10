package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * The condition a {@code StreamThread} waits on instead of sitting out the rest of {@code poll.ms} while
 * Parallel Consumer workers finish behind its back. <b>Wake-on-work</b> (astubbs#255, item 3).
 *
 * <h2>The problem this exists for</h2>
 * {@code StreamThread} polls the consumer and runs the topology on one thread. Under stock Kafka Streams
 * blocking in {@code Consumer#poll()} for up to {@code poll.ms} is free - while the thread is parked there
 * is by definition no processing it could be doing instead, because it is the same thread. Hand records to a
 * worker pool and that assumption inverts: records complete <em>during</em> the poll wait, and neither their
 * completions nor the records they unblock can move until poll returns. Throughput starts tracking poll
 * cadence rather than the work. Measured: at the default {@code poll.ms} of 100ms the single-key control arm
 * ran at <b>0.69x</b> against stock - the asynchronous path <em>slower</em> than the synchronous one, which
 * absence of concurrency can never explain.
 * <p>
 * It is worse than "an idle loop waits a bit". {@code StreamThread}'s inner work loop breaks back out to poll
 * whenever a pass dispatched nothing. Under stock that means "the buffers are empty and blocking is correct".
 * Under an asynchronous dispatcher it <em>also</em> means "the pool is full" or "every available key is
 * already in flight" - states that resolve on a worker completion and never on a broker fetch. So the loop
 * reliably chooses to block at exactly the moments a completion is imminent.
 *
 * <h2>What this is, and what it deliberately is not</h2>
 * The patched {@code StreamThread} polls briefly to collect whatever the broker already has, and then waits
 * <em>here</em> for the rest of its configured budget. A worker completion ends that wait immediately.
 * <p>
 * <b>It is not {@code KafkaConsumer#wakeup()}</b>, which is the obvious mechanism and the wrong one.
 * {@code wakeup()} throws {@code WakeupException} and is the framework's word for <em>shutdown</em>; a wake
 * delivered while the thread is not polling arms the <em>next</em> poll instead, so a stray completion signal
 * can swallow a shutdown one - a failure that shows up once in a thousand shutdowns and never reproduces on
 * demand. Owning the condition costs one small class and removes that entire failure mode.
 *
 * <h2>Why no "work is ready" flag</h2>
 * Both questions below are answered by <b>reading live dispatcher state</b>, never by a boolean this class
 * maintains alongside it. An edge-triggered flag would have to be armed before the wait and cleared after,
 * and the arming point necessarily sits <em>after</em> the previous dispatch pass - so a completion landing
 * in that gap sets a flag nobody reads, gets cleared, and the thread waits out the full budget with work in
 * hand. That is the same shape as this repository's own silent-stall defect, where a shadow copy of the run
 * state made a long-poll quietly stop happening and the loop span at ~10kHz
 * ({@code docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md}). One source of
 * truth, read directly:
 * <ul>
 *   <li><b>Should the wait be split at all?</b> {@link #hasActiveWorkOnCurrentThread()} - is anything in
 *       flight, or is an outcome waiting to be fed back. If not, the thread takes the stock full-budget poll,
 *       because nothing can wake it and shortening the poll would only delay broker records.</li>
 *   <li><b>Should the wait end?</b> Completions pending. Level-triggered, and therefore impossible to lose:
 *       the completions queue is drained <em>only</em> by the StreamThread inside
 *       {@code PcTaskDispatcher.dispatchAvailable}, and a StreamThread that is waiting here is by definition
 *       not draining it. A worker enqueues its outcome before it signals, so a signal that races the waiter
 *       either finds the predicate already true or is delivered under this monitor.</li>
 * </ul>
 *
 * <h2>Scoped per owning thread, and safe when the scoping is wrong</h2>
 * A single JVM-wide monitor would let one StreamThread's wait be ended by an unrelated task's completion:
 * correct, but a spin. So signals are keyed by the thread that constructed the dispatcher, which is the
 * StreamThread - {@code StreamTask} is built by {@code TaskManager} on it.
 * <p>
 * If that ever stops being true the lookup simply finds nothing, the gate reads "no work in flight", and the
 * thread takes the stock poll. <b>The failure mode of mis-scoping is today's behaviour, not a stall</b> -
 * which is the property that makes it acceptable to depend on a construction thread at all. Owners and
 * dispatchers are held <b>weakly</b>, so a dispatcher that is never deregistered (Kafka has teardown paths
 * that drop a task without closing it) cannot turn this registry into a leak; a live dispatcher is always
 * strongly reachable from its {@code StreamTask} and from {@code PcTaskDispatcher}'s own active set.
 *
 * <h2>Raised by work becoming available - not by "a worker finished"</h2>
 * {@link #signalWorkAvailable()} is named for the condition, not for today's only raiser. Three things can
 * make work dispatchable and only one arrives through the consumer: broker records (the poll handles those),
 * a worker completion, and - once retries return - a <b>retry timer</b>, where a record sitting out its
 * backoff becomes dispatchable with no poll and no completion involved. Retries are disabled today, so the
 * timer cannot raise this yet; the method is shaped so that it will not need a second one when they return.
 *
 * @author Antony Stubbs
 * @see PcTaskDispatcher
 * @see PcDispatchSwitch#isWakeOnWorkEnabled()
 */
@Slf4j
public final class PcWorkSignal {

    /**
     * How long the patched {@code StreamThread} blocks in {@code Consumer#poll()} before handing the rest of
     * its budget to this class.
     * <p>
     * Long enough to collect what the consumer has already fetched - the fetch requests are in flight
     * regardless, so this is a collection step rather than a wait - and short enough that it contributes
     * nothing measurable per dispatch. The controlled experiment that found this whole problem ran the entire
     * {@code poll.ms} at 1ms and brought the single-key overhead from ~1695ms to ~24ms, so 1ms of genuine
     * polling per pass is demonstrably affordable.
     * <p>
     * It is deliberately not {@code Duration.ZERO}: zero is already Kafka's own value for the
     * "unblock something else" poll phases, and a millisecond gives the fetcher a chance to hand over a
     * response that landed moments ago rather than deferring it to the next pass.
     */
    public static final Duration SHORT_POLL = Duration.ofMillis(1);

    /**
     * Weak in both directions. The key is the owning {@code StreamThread}, so a dead thread's entry
     * disappears with it; see the class javadoc on why this registry must never be the thing that keeps a
     * dispatcher alive.
     */
    private static final Map<Thread, PcWorkSignal> BY_OWNER =
            Collections.synchronizedMap(new WeakHashMap<Thread, PcWorkSignal>());

    /**
     * The dispatchers whose work this signal speaks for - normally one, since this module's proven shape is
     * one task per thread, but a thread with several tasks gets one signal covering all of them.
     */
    private final Set<PcTaskDispatcher> dispatchers =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<PcTaskDispatcher, Boolean>()));

    private final Object monitor = new Object();

    /**
     * Counts requests to <em>abandon</em> the wait outright - shutdown, and a dispatcher going away - as
     * opposed to work having arrived. A waiter captures this on entry and leaves as soon as it changes, so
     * there is no flag to arm or clear and no way to consume someone else's wake.
     * <p>
     * This is not the "is there work" flag the class javadoc rules out: it never answers whether work exists,
     * and nothing reads it to decide whether to dispatch. It exists because {@code notifyAll} alone cannot end
     * a wait whose predicate is still false, and shutdown needs exactly that.
     * <p>
     * Guarded by {@link #monitor}.
     */
    private long wakeRequests;

    private PcWorkSignal() {
    }

    /**
     * Called from {@link PcTaskDispatcher}'s constructor, which runs on the StreamThread. Whatever thread it
     * really is becomes this dispatcher's signal owner.
     *
     * @return the signal the dispatcher's workers should raise
     */
    static PcWorkSignal registerForCurrentThread(final PcTaskDispatcher dispatcher) {
        final Thread owner = Thread.currentThread();
        final PcWorkSignal signal;
        synchronized (BY_OWNER) {
            signal = BY_OWNER.computeIfAbsent(owner, ignored -> new PcWorkSignal());
        }
        signal.dispatchers.add(dispatcher);
        log.debug("PC wake-on-work: dispatcher registered to owner thread {}", owner.getName());
        return signal;
    }

    /**
     * Both close paths call this. A closed dispatcher must stop contributing to
     * {@link #hasActiveWorkOnCurrentThread()}, or a StreamThread would keep taking the split-wait branch for
     * work that can never complete.
     */
    void deregister(final PcTaskDispatcher dispatcher) {
        dispatchers.remove(dispatcher);
        // An abandon, not a "work arrived": if this was the last dispatcher, no completion is ever coming and
        // a waiter would otherwise sit out its whole budget for work that no longer exists.
        requestWake();
    }

    /**
     * <b>The gate.</b> True when a completion can still arrive for this thread - so splitting the poll wait
     * can pay for itself - and false when nothing is running, which is the case where the stock full-budget
     * poll is exactly right and shortening it would only delay broker records for no gain.
     * <p>
     * Note that it deliberately includes <em>pending completions</em> as well as in-flight work: a worker
     * enqueues its outcome and only then decrements the in-flight count, so an outcome can be sitting
     * undrained with nothing in flight. Asking only "is anything running" would send the thread into a full
     * {@code poll.ms} block with a completion already in hand.
     *
     * @return whether the calling thread should split its poll wait
     */
    public static boolean hasActiveWorkOnCurrentThread() {
        if (!PcDispatchSwitch.isWakeOnWorkEnabled()) {
            return false;
        }
        final PcWorkSignal signal = BY_OWNER.get(Thread.currentThread());
        return signal != null && signal.hasActiveWork();
    }

    /**
     * Wait out the remainder of the caller's poll budget, or return the instant an outcome is ready to be fed
     * back to Parallel Consumer.
     * <p>
     * Called from the patched {@code StreamThread} only, and only after {@link #hasActiveWorkOnCurrentThread()}
     * said yes and a {@link #SHORT_POLL} returned nothing from the broker.
     *
     * @param fullPollBudget the {@code poll.ms} the thread would otherwise have spent blocked in the consumer
     */
    public static void awaitWorkForRemainderOf(final Duration fullPollBudget) {
        final PcWorkSignal signal = BY_OWNER.get(Thread.currentThread());
        if (signal == null) {
            return;
        }
        final long remainingMillis = fullPollBudget.toMillis() - SHORT_POLL.toMillis();
        if (remainingMillis <= 0) {
            return;
        }
        signal.await(remainingMillis);
    }

    /**
     * End the given thread's wait now, from another thread, whether or not work has arrived.
     * <p>
     * Exists so that a shutdown does not have to sit out the remaining budget:
     * {@code StreamThread.shutdown()} only sets {@code PENDING_SHUTDOWN} and lets the loop notice, so without
     * this the close would pay up to {@code poll.ms} - the same bill stock pays, but there is no reason to pay
     * it once we own the wait.
     * <p>
     * A wake that arrives just <em>before</em> the wait starts is not lost in any way that matters: shutdown
     * sets the state first, and the split wait only happens on the {@code RUNNING}/{@code STARTING} branch, so
     * the next pass takes the {@code PENDING_SHUTDOWN} branch and does not wait at all. The residual window
     * costs one poll budget, which is exactly what stock pays.
     */
    public static void wakeOwner(final Thread owner) {
        final PcWorkSignal signal = BY_OWNER.get(owner);
        if (signal != null) {
            signal.requestWake();
        }
    }

    /**
     * Raised whenever something became dispatchable - today a worker outcome, and a retry timer once retries
     * return. See the class javadoc on why this is not called {@code signalCompletion}.
     * <p>
     * Deliberately only a {@code notifyAll}: the waiter's predicate is the live completions queue, which the
     * caller has already written to, so there is nothing to record here. That is what makes this safe to call
     * from a worker on every outcome, whether or not anyone is waiting.
     */
    void signalWorkAvailable() {
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    /** Abandon the wait, regardless of the work predicate. See {@link #wakeRequests}. */
    private void requestWake() {
        synchronized (monitor) {
            wakeRequests++;
            monitor.notifyAll();
        }
    }

    private boolean hasActiveWork() {
        synchronized (dispatchers) {
            for (PcTaskDispatcher dispatcher : dispatchers) {
                if (dispatcher.getInFlightCount() > 0 || dispatcher.hasPendingCompletions()) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasPendingCompletions() {
        synchronized (dispatchers) {
            for (PcTaskDispatcher dispatcher : dispatchers) {
                if (dispatcher.hasPendingCompletions()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Loops on the predicate against a deadline, so a spurious wakeup cannot end the wait early and a real
     * signal cannot be missed. Interruption ends the wait and restores the flag - the caller is the
     * StreamThread, and swallowing an interrupt there would strand a shutdown.
     * <p>
     * A wait that finds the predicate already true counts as both a split wait and a wake: the thread did take
     * this branch, and it did leave it because work was ready rather than because time ran out, which is what
     * the two counters are asked to distinguish.
     */
    private void await(final long remainingMillis) {
        PcDispatchCounters.onSplitPollWait();
        final long deadlineMillis = System.currentTimeMillis() + remainingMillis;
        boolean workArrived = false;
        synchronized (monitor) {
            final long wakeRequestsAtEntry = wakeRequests;
            while (true) {
                if (hasPendingCompletions()) {
                    workArrived = true;
                    break;
                }
                if (wakeRequests != wakeRequestsAtEntry) {
                    // Shutdown, or the last dispatcher going away. Not work, so it does not count as a wake.
                    break;
                }
                final long leftMillis = deadlineMillis - System.currentTimeMillis();
                // Never call wait(0) - that means "wait forever", and the whole point here is a bounded wait.
                if (leftMillis <= 0) {
                    break;
                }
                try {
                    monitor.wait(leftMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (workArrived) {
            PcDispatchCounters.onWakeOnWork();
        }
    }

    /**
     * Test seam. The registry is process-wide and surefire reuses forks, so a signal left behind by one test
     * would answer the gate for the next one.
     */
    static void resetRegistryForTests() {
        synchronized (BY_OWNER) {
            for (PcWorkSignal signal : new HashSet<>(BY_OWNER.values())) {
                signal.dispatchers.clear();
                signal.requestWake();
            }
            BY_OWNER.clear();
        }
    }
}
