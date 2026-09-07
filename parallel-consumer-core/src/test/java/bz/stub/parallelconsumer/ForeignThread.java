package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * One named thread that is not the test's own, for the tests whose whole subject is <em>which thread</em>
 * called - the thread-confinement guards, which can only be exercised by a second party.
 * <p>
 * <b>Why this is shared rather than written out per test.</b> {@code ThreadConfinedConsumerTestBase}'s javadoc
 * records that two independent copies of this fixture had already drifted: only one of them unwrapped the
 * {@link ExecutionException}, so an assertion failing on the foreign thread surfaced in the other as a wrapper
 * naming neither the assertion nor the thread. That base was the second copy; this is the extraction that stops
 * a third, made when the retry-queue iterator's confinement needed the same fixture from another package.
 * <p>
 * <b>Two ways to run, because confinement tests want both.</b> {@link #run(Runnable)} rethrows whatever the
 * action threw, for the calls that are expected to succeed; {@link #catching(Runnable)} returns it instead, for
 * the calls whose refusal is the assertion. Neither returns the {@link ExecutionException} wrapper.
 *
 * @author Antony Stubbs
 */
public class ForeignThread implements AutoCloseable {

    /**
     * The thread's name. A constant because a confinement guard's rejection message must carry the offending
     * thread's name, and a test asserting that should not restate the literal.
     */
    public static final String DEFAULT_NAME = "test-foreign-thread";

    private static final long TIMEOUT_SECONDS = 10;

    private final ExecutorService pool;

    public ForeignThread() {
        this(DEFAULT_NAME);
    }

    public ForeignThread(String name) {
        this.pool = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, name);
            // daemon so a leaked pool cannot hold a JVM open - the close() below is still the contract
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Runs {@code action} over there and waits for it, rethrowing whatever it threw rather than the
     * {@link ExecutionException} wrapper - an assertion that fails over there has to fail the test over here,
     * naming its own cause.
     */
    public void run(Runnable action) throws Exception {
        try {
            pool.submit(action).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) throw cause;
            throw e;
        }
    }

    /**
     * Runs {@code action} over there and returns what it threw, or {@code null} if it returned normally. For the
     * calls whose refusal IS the assertion - the wrapper would otherwise have to be unwrapped at every site,
     * which is the drift this class exists to stop.
     */
    public Throwable catching(Runnable action) {
        try {
            pool.submit(action).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for the foreign thread", e);
        } catch (Exception e) {
            // A timeout here is not a flake to retry: the action is blocked on a lock the test believed was
            // free, which is the defect these tests look for. Say so rather than reporting "no exception".
            throw new AssertionError("the foreign thread did not finish within " + TIMEOUT_SECONDS + "s", e);
        }
    }

    @Override
    public void close() {
        pool.shutdownNow();
        try {
            boolean ignoredStopped = pool.awaitTermination(5, TimeUnit.SECONDS);
            // the return is not acted on: the threads are daemons, so a pool that outlives the wait cannot
            // hold the JVM open, and failing a test on a slow shutdown would report the fixture, not the code
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
