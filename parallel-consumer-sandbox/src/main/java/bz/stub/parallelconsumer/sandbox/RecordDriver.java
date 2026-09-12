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
final class RecordDriver implements AutoCloseable {

    /**
     * Named rather than written as a literal beside the division that turns a rate into an interval, which is the
     * one place a factor-of-a-thousand slip would go unnoticed - the run would simply be a thousand times slower.
     */
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /**
     * One feed per topic, served in order within a tick. The list is the definition's topics, so a topic no route
     * claims is not in it and never gets a record.
     */
    private final List<TopicFeed> feeds;

    /**
     * The gap between ticks, derived once from the declared rate. Per topic, not in total: a two-topic definition
     * at a hundred a second generates two hundred records a second.
     */
    private final long intervalNanos;

    /**
     * When to stop, asked after every record and after every tick - see {@link Bound}.
     */
    private final Bound bound;

    /**
     * What to do when the bound is reached: wait for the instance to account for every record already published -
     * completed, or parked - then close it. Run on this driver's own thread, because a close cannot be run from inside the
     * engine it closes (KTD6) and this thread is outside it.
     * <p>
     * It runs <b>before</b> {@link #finished} counts down, so {@link #awaitFinished(Duration)} covers the whole
     * bound sequence rather than only the generating half - and a wait that refuses is carried to whoever is
     * waiting on the bound through {@link #rethrowAnyFailure()}, instead of dying unseen on this thread.
     */
    private final Runnable onBoundReached;

    /**
     * Records published across every topic. Atomic because a caller reads it from its own thread while this
     * driver's thread increments it, and because the count bound is tested against the value the increment
     * returned rather than against a later read of it.
     */
    private final AtomicLong generated = new AtomicLong();

    /**
     * Whether the generating loop should keep going. Set by {@link #start()} with a compare-and-set, so a second
     * start is refused rather than quietly running two threads over one set of feeds; cleared by {@link #close()}
     * and by the loop's own exit.
     */
    private final AtomicBoolean running = new AtomicBoolean();

    /**
     * Counted down when the run is over - <b>after</b> the bound's close, not after the last record - so that a
     * caller waiting on it sees the end of the run rather than the end of generating.
     */
    private final CountDownLatch finished = new CountDownLatch(1);

    /**
     * Whether the loop ended by reaching its bound rather than by being closed. Read by {@link #close()} to decide
     * whether an interrupt is safe, and by {@link #awaitBound(Duration)} to tell a bound that was reached from a
     * wait that merely returned.
     */
    private final AtomicBoolean boundWasReached = new AtomicBoolean();

    /**
     * What killed the driver thread, if anything did. Kept rather than only logged, so that
     * {@link #rethrowAnyFailure()} can put it in front of whoever is waiting: a driver that dies leaves a run
     * that consumes nothing, and a test then fails on its own timeout with no mention of the actual cause. That is
     * how a jackson-core/databind version split first showed up here - as a sixty-second timeout.
     */
    private volatile Throwable failure;

    /**
     * The driver's own thread, or null before {@link #start()}.
     * <p>
     * Volatile because {@link #close()} reads it from whatever thread closes the sandbox, and that is not always
     * the thread that started it: a demo closes from a shutdown hook, and an instance can be closed by any caller
     * holding it. A close that read a stale null would return having neither interrupted nor joined a driver
     * that is still running - and a driver outliving its sandbox goes on publishing into a closed consumer.
     * The tests do not reach the race, because a closer thread they start themselves inherits the write through
     * {@code Thread.start()}'s own happens-before edge; a thread that already existed inherits nothing.
     */
    private volatile Thread thread;

    /**
     * @param feeds             one per topic the definition routes, each knowing how to generate and publish its
     *                          own record
     * @param perSecondPerTopic the declared rate, which refuses zero and below rather than generating nothing and
     *                          leaving the caller to work out why
     * @param bound             when to stop, or {@link Bound#none()}
     * @param onBoundReached    what reaching the bound starts - see the field
     */
    RecordDriver(List<TopicFeed> feeds, double perSecondPerTopic, Bound bound, Runnable onBoundReached) {
        if (perSecondPerTopic <= 0) {
            throw new IllegalArgumentException("A driver rate of " + perSecondPerTopic + " records per second "
                    + "would generate nothing");
        }
        this.feeds = feeds;
        this.intervalNanos = (long) (NANOS_PER_SECOND / perSecondPerTopic);
        this.bound = bound;
        this.onBoundReached = onBoundReached;
    }

    /**
     * Starts publishing on a thread of this driver's own, and returns immediately.
     *
     * @throws IllegalStateException if it is already running - two threads over one set of feeds would interleave
     *                               their record indices, and a seeded run would stop being reproducible
     */
    void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("This driver is already running");
        }
        thread = new Thread(this::generate, "pc-sandbox-driver");
        // A daemon so that a demo whose main method returns without closing its instance does not hang the JVM. The
        // bound and close() are the real stops; this is only the backstop.
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Records published so far, across every topic - a live count while the run is going, and the run's total
     * afterwards.
     */
    long generatedRecords() {
        return generated.get();
    }

    /**
     * Whether the run ended at its bound rather than by being closed. False for an unbounded run, always.
     */
    boolean boundWasReached() {
        return boundWasReached.get();
    }

    /**
     * Rethrows whatever killed the driver thread, wrapped so the stack trace of the waiting thread is kept too.
     * Called from {@link #awaitBound(Duration)}, so a failure surfaces in front of whoever waited on the run
     * rather than only in the log.
     */
    private void rethrowAnyFailure() {
        Throwable died = failure;
        if (died != null) {
            throw new IllegalStateException("The sandbox driver failed after " + generated.get()
                    + " records: " + died, died);
        }
    }

    /**
     * Waits for the run to finish - the bound reached <b>and its close completed</b>, or the driver closed.
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
     * The tail both sandboxes share, here rather than written twice: the wait, then the rethrow - so a driver
     * that died puts its failure in front of the caller rather than being reported as a bound that was not
     * reached - then the two conditions together, because a wait that ran out and a run that ended without its
     * bound are both false and neither is a failure. The guard clauses stay with the callers: what is refused
     * differs between the two APIs, and their tests assert those messages.
     *
     * @return false if the wait ran out, or if the run ended without reaching its bound
     * @throws IllegalStateException wrapping whatever killed the driver thread
     */
    boolean awaitBound(Duration timeout) {
        boolean finishedInTime = awaitFinished(timeout);
        rethrowAnyFailure();
        return finishedInTime && boundWasReached();
    }

    /**
     * The driving loop, which is the whole of what the driver thread does.
     * <p>
     * One record per feed per tick, the bound asked after each record and on both sides of the sleep - before it
     * so a reached duration does not wait out one more interval first, after it because the sleep is where the
     * time passes. Every exit runs the finally below, which is what publishes {@link #boundWasReached}, starts the
     * bound's close and counts {@link #finished} down; there is no return from this method that skips it.
     */
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
                        // rather than swallowed - a driver that stops silently reads exactly like a
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
            log.error("The sandbox driver stopped after {} records", generated.get(), e);
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
     * @return false when the wait was interrupted, which is how {@link #close()} stops a driver mid-sleep
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
     * Stops publishing without running the bound's close - the caller is closing this down itself.
     *
     * <h2>An interrupt is for a driver that is still publishing, and for nothing else</h2>
     * The interrupt exists to end a {@link #sleepUntil} between two ticks. If the bound has already been reached
     * the thread is somewhere else entirely: inside {@link #onBoundReached}, which waits for the instance to
     * account for what was published and then closes it. Interrupting it there turns an orderly end into a
     * possibly-uncommitted one - {@link SandboxConsumer#awaitEveryPublishedRecordCommitted(Duration)} catches the
     * interrupt, re-arms the flag and returns as though it had succeeded, and {@code instance.close()} then runs on
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
        Thread driver = thread;
        if (driver != null && driver != Thread.currentThread()) {
            if (!boundWasReached.get()) {
                driver.interrupt();
            }
            try {
                driver.join(Duration.ofSeconds(10).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (driver.isAlive()) {
                // Loud, because a driver that outlives its sandbox goes on publishing into a closed consumer
                // and the resulting exception is attributed to whatever runs next.
                log.error("The sandbox driver thread did not stop within the close's ten seconds (bound "
                        + "reached: {} - if true it was waited out rather than interrupted, and what it is "
                        + "waiting for is the instance accounting for what was published)", boundWasReached.get());
            }
        }
        finished.countDown();
    }
}
