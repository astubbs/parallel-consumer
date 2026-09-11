package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One thread, publishing one record per topic per tick, at the declared rate, until the bound.
 *
 * <h2>The rate is per topic; the bound is total</h2>
 * "Fifty per second" over three routes is a hundred and fifty records a second, because a rate is a property of a
 * topic and reads as one - a definition that gains a route should not quietly halve the traffic on the routes it
 * already had. A count bound is the opposite: it is a property of the run, so it counts every record.
 *
 * <h2>Pacing against a deadline, not by sleeping for an interval</h2>
 * Sleeping for the interval after each tick makes the actual rate {@code interval + however long publishing took},
 * and the error accumulates: a ten-second run at fifty a second lands well short and a test asserting "about five
 * hundred" fails for a reason that has nothing to do with what it is testing. Each tick is instead due at
 * {@code start + n * interval}, so a slow tick is absorbed by the next one rather than added to it.
 */
@Slf4j
final class RecordGenerator implements AutoCloseable {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final List<TopicFeed> feeds;

    private final long intervalNanos;

    private final Bound bound;

    /**
     * What to do when the bound is reached: wait for the instance to account for every record already published -
     * completed, or parked - then close it. Run on this generator's own thread, because a close cannot be run from inside the
     * engine it closes (KTD6) and this thread is outside it.
     * <p>
     * It runs <b>before</b> {@link #finished} counts down, so {@link #awaitFinished(Duration)} covers the whole
     * bound sequence rather than only the generating half - and a wait that refuses is carried to whoever is
     * waiting on the bound through {@link #rethrowAnyFailure()}, instead of dying unseen on this thread.
     */
    private final Runnable onBoundReached;

    private final AtomicLong generated = new AtomicLong();

    private final AtomicBoolean running = new AtomicBoolean();

    private final CountDownLatch finished = new CountDownLatch(1);

    private final AtomicBoolean boundWasReached = new AtomicBoolean();

    /**
     * What killed the generator thread, if anything did. Kept rather than only logged, so that
     * {@link #rethrowAnyFailure()} can put it in front of whoever is waiting: a generator that dies leaves a run
     * that consumes nothing, and a test then fails on its own timeout with no mention of the actual cause. That is
     * how a jackson-core/databind version split first showed up here - as a sixty-second timeout.
     */
    private volatile Throwable failure;

    private Thread thread;

    RecordGenerator(List<TopicFeed> feeds, double perSecondPerTopic, Bound bound, Runnable onBoundReached) {
        if (perSecondPerTopic <= 0) {
            throw new IllegalArgumentException("A generator rate of " + perSecondPerTopic + " records per second "
                    + "would generate nothing");
        }
        this.feeds = feeds;
        this.intervalNanos = (long) (NANOS_PER_SECOND / perSecondPerTopic);
        this.bound = bound;
        this.onBoundReached = onBoundReached;
    }

    void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("This generator is already running");
        }
        thread = new Thread(this::generate, "pc-sandbox-generator");
        // A daemon so that a demo whose main method returns without closing its handle does not hang the JVM. The
        // bound and close() are the real stops; this is only the backstop.
        thread.setDaemon(true);
        thread.start();
    }

    long generatedRecords() {
        return generated.get();
    }

    boolean boundWasReached() {
        return boundWasReached.get();
    }

    /**
     * Rethrows whatever killed the generator thread, wrapped so the stack trace of the waiting thread is kept too.
     * Called from {@link #awaitBound(Duration)}, so a failure surfaces in front of whoever waited on the run
     * rather than only in the log.
     */
    private void rethrowAnyFailure() {
        Throwable died = failure;
        if (died != null) {
            throw new IllegalStateException("The sandbox generator failed after " + generated.get()
                    + " records: " + died, died);
        }
    }

    /**
     * Waits for the run to finish - the bound reached <b>and its close completed</b>, or the generator closed.
     * <p>
     * The close is inside the wait deliberately: what a bounded run promises is that the state readable
     * afterwards is the end of the run, and that is not true until the engine has committed what it was given.
     * So the caller's timeout has to be larger than the bound's own wait for those commits - see
     * {@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}.
     *
     * @return false if it was still running when the wait ran out
     */
    boolean awaitFinished(Duration timeout) {
        try {
            return finished.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Waits for the run to reach its bound and finish what reaching it starts, and answers whether it got there.
     * <p>
     * The tail both sandboxes share, here rather than written twice: the wait, then the rethrow - so a generator
     * that died puts its failure in front of the caller rather than being reported as a bound that was not
     * reached - then the two conditions together, because a wait that ran out and a run that ended without its
     * bound are both false and neither is a failure. The guard clauses stay with the callers: what is refused
     * differs between the two APIs, and their tests assert those messages.
     *
     * @return false if the wait ran out, or if the run ended without reaching its bound
     * @throws IllegalStateException wrapping whatever killed the generator thread
     */
    boolean awaitBound(Duration timeout) {
        boolean finishedInTime = awaitFinished(timeout);
        rethrowAnyFailure();
        return finishedInTime && boundWasReached();
    }

    private void generate() {
        long startNanos = System.nanoTime();
        long tick = 0;
        boolean boundReached = false;
        try {
            while (running.get()) {
                for (TopicFeed feed : feeds) {
                    if (!running.get()) {
                        break;
                    }
                    if (!feed.publish(tick)) {
                        // The consumer was closed under us: the run is over, and for an unbounded run that is
                        // the ordinary way it ends. Anything else that goes wrong throws, and is caught below
                        // rather than swallowed - a generator that stops silently reads exactly like a
                        // definition that consumes nothing, which is the harder bug of the two to find.
                        log.debug("Sandbox consumer closed while generating for {}", feed.topic());
                        return;
                    }
                    if (bound.reachedByCount(generated.incrementAndGet())) {
                        boundReached = true;
                        return;
                    }
                }
                tick++;
                if (bound.reachedByTime(System.nanoTime() - startNanos)) {
                    boundReached = true;
                    return;
                }
                if (!sleepUntil(startNanos + tick * intervalNanos)) {
                    return;
                }
                if (bound.reachedByTime(System.nanoTime() - startNanos)) {
                    boundReached = true;
                    return;
                }
            }
        } catch (Throwable e) {
            // Throwable, not Exception: the failure that made this catch necessary was a NoSuchMethodError from a
            // library version split, and an Error killing this thread is exactly as invisible as an exception.
            // Recorded as well as logged - see the failure field.
            failure = e;
            log.error("The sandbox generator stopped after {} records", generated.get(), e);
        } finally {
            running.set(false);
            boundWasReached.set(boundReached);
            if (boundReached) {
                log.info("Sandbox bound reached ({}) after {} records - waiting for the instance to account for "
                        + "them, then closing", bound, generated.get());
                try {
                    onBoundReached.run();
                } catch (Throwable e) {
                    // Recorded rather than only logged, and recorded here rather than left to escape this
                    // thread: an exception thrown out of a Thread's run method is invisible, and the run would
                    // then fail as somebody else's timeout with no mention of what actually went wrong.
                    failure = e;
                    log.error("The sandbox bound's close failed after {} records", generated.get(), e);
                }
            }
            // Last, so that a caller waiting on the bound is released only once the whole sequence - stop
            // generating, wait for the engine to account for what was published, close - has run.
            finished.countDown();
        }
    }

    /**
     * @return false when the wait was interrupted, which is how {@link #close()} stops a generator mid-sleep
     */
    private boolean sleepUntil(long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        while (remaining > 0 && running.get()) {
            try {
                TimeUnit.NANOSECONDS.sleep(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            remaining = deadlineNanos - System.nanoTime();
        }
        return true;
    }

    /**
     * Stops generating without running the bound's close - the caller is closing this down itself.
     *
     * <h2>An interrupt is for a generator that is still generating, and for nothing else</h2>
     * The interrupt exists to end a {@link #sleepUntil} between two ticks. If the bound has already been reached
     * the thread is somewhere else entirely: inside {@link #onBoundReached}, which waits for the instance to
     * account for what was published and then closes it. Interrupting it there turns an orderly end into a
     * possibly-uncommitted one - {@link SandboxConsumer#awaitEveryPublishedRecordCommitted(Duration)} catches the
     * interrupt, re-arms the flag and returns as though it had succeeded, and {@code handle.close()} then runs on
     * a thread carrying an interrupt. Parallel Consumer's close path is interrupt-sensitive by design, and says
     * so: "Control thread carries an interrupt into the close sequence ... If the transactional commit lock
     * cannot be acquired below, this is the likely reason". The final commit can be skipped, and the offsets the
     * test is about to assert on are then missing - which reads as a flake, attributed to the engine.
     * <p>
     * The shape that hits it is ordinary: a run reaches its bound and, in the same instant, the test exits its
     * try-with-resources.
     * <p>
     * So the bound sequence is waited out rather than interrupted, and the join below is what waits. A window
     * remains between {@code boundReached} being decided and {@link #boundWasReached} being published, in which a
     * close still interrupts; it is one field write wide, it needs the close to land inside it, and closing it
     * would mean waiting on {@link #finished} before every ordinary close instead - which is the far commoner
     * path and has nothing to wait for.
     */
    @Override
    public void close() {
        running.set(false);
        Thread generator = thread;
        if (generator != null && generator != Thread.currentThread()) {
            if (!boundWasReached.get()) {
                generator.interrupt();
            }
            try {
                generator.join(Duration.ofSeconds(10).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (generator.isAlive()) {
                // Loud, because a generator that outlives its sandbox goes on publishing into a closed consumer
                // and the resulting exception is attributed to whatever runs next.
                log.error("The sandbox generator thread did not stop within the close's ten seconds (bound "
                        + "reached: {} - if true it was waited out rather than interrupted, and what it is "
                        + "waiting for is the instance accounting for what was published)", boundWasReached.get());
            }
        }
        finished.countDown();
    }
}
