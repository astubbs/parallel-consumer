package bz.stub.parallelconsumer.proxy.lifecycle;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Dies when its parent dies (KTD19, R52). The sidecar is the application's child, and an orphaned sidecar is
 * a process holding a group membership on behalf of an application that no longer exists - so this is a
 * correctness mechanism, not housekeeping.
 *
 * <h2>Why an inherited pipe, and not {@code PR_SET_PDEATHSIG}</h2>
 *
 * The parent holds the write end and never writes down it. When the parent dies <b>for any reason, including
 * SIGKILL</b>, the kernel closes the last write end and this side's read returns -1. That is pure Java,
 * identical on Linux and macOS, and unchanged between the JVM jar and the native image.
 * <p>
 * {@code PR_SET_PDEATHSIG} was rejected on the merits as well as on reach: it needs {@code java.lang.foreign},
 * which is unavailable at this module's Java 17 release level; it fires on parent <em>thread</em> death; it is
 * cleared across {@code fork} and {@code setuid}; and it races a parent that died before the call.
 *
 * <h2>Why there is a second signal</h2>
 *
 * The pipe signal has one hole, and it is a hole the client can fall into by accident: <b>a wrapper process
 * between the application and the sidecar inherits the write end and holds it open</b> after the real parent
 * is gone, so EOF never arrives and this watchdog would wait forever. Launching through a shell is the
 * everyday way to create one.
 * <p>
 * So the client must launch the proxy <b>directly, never through a shell</b> - and because "must" is not a
 * mechanism, a {@link ProcessHandle} parent-pid poll runs beside the pipe and catches the case anyway. The
 * pipe stays primary because it is immediate; the poll is a backstop with a visible interval.
 *
 * <h2>What it does not do</h2>
 *
 * It reports death. It does not decide what happens next - that is {@link DrainCoordinator}'s job, and
 * separating them is what lets the drain be tested without killing a process.
 *
 * @author Antony Stubbs
 */
@Slf4j
public final class ParentDeathWatchdog implements AutoCloseable {

    /** Which signal fired, so a shutdown can say why it is shutting down rather than merely that it is. */
    public enum Cause {
        /** EOF or an error on the inherited pipe: the primary signal. */
        LIFELINE_CLOSED,
        /** The parent pid is no longer alive: the backstop, and evidence a wrapper process held the pipe. */
        PARENT_PROCESS_GONE
    }

    private final InputStream parentLifeline;

    private final BooleanSupplier parentAlive;

    private final Duration pollInterval;

    private final CountDownLatch died = new CountDownLatch(1);

    private final AtomicReference<Cause> cause = new AtomicReference<>();

    /** Set before the threads are torn down, so a deliberate close is never mistaken for the parent dying. */
    private volatile boolean closing;

    private Thread lifelineWatcher;

    private ScheduledExecutorService parentPoller;

    private ParentDeathWatchdog(InputStream parentLifeline, BooleanSupplier parentAlive, Duration pollInterval) {
        this.parentLifeline = parentLifeline;
        this.parentAlive = parentAlive;
        this.pollInterval = pollInterval;
    }

    /**
     * The seam form: both signals are supplied, so a test can exercise either one without a real process.
     *
     * @param parentLifeline the inherited pipe - {@code System.in} in the real sidecar
     * @param parentAlive    the second signal; consulted every {@code pollInterval}
     */
    public static ParentDeathWatchdog watching(InputStream parentLifeline, BooleanSupplier parentAlive,
                                               Duration pollInterval) {
        return new ParentDeathWatchdog(parentLifeline, parentAlive, pollInterval);
    }

    /**
     * The production form: watches the pipe it was given and the pid that started this JVM.
     * <p>
     * The parent handle is captured <b>once, now</b>, rather than re-read on each poll. Re-reading is the
     * subtle bug: once the real parent dies this process is re-parented (to init, or to a subreaper), so a
     * fresh lookup would find a living process and report the parent healthy forever - the poll would answer
     * "does this process have a parent" when the question is "is the parent it started with still alive".
     * <p>
     * A JVM with no visible parent gets a poll that always says alive: unable to tell is not the same as
     * dead, and killing the sidecar on that basis would be a fabricated signal.
     */
    public static ParentDeathWatchdog watchingParentOf(InputStream parentLifeline, Duration pollInterval) {
        var parent = ProcessHandle.current().parent();
        if (parent.isEmpty()) {
            log.warn("No parent process is visible, so the pid poll is disabled and EOF on the lifeline is the "
                    + "only parent-death signal. A wrapper process holding the write end would now go unnoticed.");
            return watching(parentLifeline, () -> true, pollInterval);
        }
        var handle = parent.get();
        log.debug("Watching parent pid {}", handle.pid());
        return watching(parentLifeline, handle::isAlive, pollInterval);
    }

    /** Starts both watchers. Daemon threads throughout: neither may keep a shutting-down JVM alive. */
    public synchronized void start() {
        if (lifelineWatcher != null) {
            throw new IllegalStateException("already started");
        }

        lifelineWatcher = new Thread(this::watchLifeline, "pc-proxy-parent-lifeline");
        lifelineWatcher.setDaemon(true);
        lifelineWatcher.start();

        parentPoller = Executors.newSingleThreadScheduledExecutor(runnable -> {
            var thread = new Thread(runnable, "pc-proxy-parent-poll");
            thread.setDaemon(true);
            return thread;
        });
        long intervalMs = Math.max(1, pollInterval.toMillis());
        parentPoller.scheduleWithFixedDelay(this::pollParent, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /** Blocks until the parent dies. */
    public void awaitDeath() throws InterruptedException {
        died.await();
    }

    /**
     * @return true if the parent died within the bound, false if it is still alive - so a caller can assert
     * that nothing happened, which is the property a spurious-fire regression breaks
     */
    public boolean awaitDeath(Duration timeout) throws InterruptedException {
        return died.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Which signal fired, or null while the parent is alive. */
    public Cause cause() {
        return cause.get();
    }

    private void watchLifeline() {
        try {
            while (parentLifeline.read() != -1) {
                // The parent never writes; only its death matters. Anything it does send is discarded rather
                // than parsed - this pipe is a lifetime signal and deliberately not a second control channel.
                log.trace("Ignoring a byte on the parent lifeline; it carries no protocol");
            }
            signal(Cause.LIFELINE_CLOSED);
        } catch (IOException parentGone) {
            // A broken pipe reads the same as EOF. Propagating it would leave a sidecar running with no
            // supervisor, which is the exact state this class exists to prevent.
            signal(Cause.LIFELINE_CLOSED);
        }
    }

    private void pollParent() {
        try {
            if (!parentAlive.getAsBoolean()) {
                signal(Cause.PARENT_PROCESS_GONE);
            }
        } catch (RuntimeException e) {
            // A poll that throws must not kill the scheduled task - that would silently retire the backstop
            // and leave the wrapper-process case uncovered for the rest of the run.
            log.warn("Parent liveness poll failed; the pipe signal still stands", e);
        }
    }

    private void signal(Cause observed) {
        if (closing) {
            return;
        }
        if (cause.compareAndSet(null, observed)) {
            log.info("Parent death detected: {}", observed);
            died.countDown();
        }
    }

    @Override
    public void close() {
        closing = true;
        if (parentPoller != null) {
            parentPoller.shutdownNow();
        }
        if (lifelineWatcher != null) {
            lifelineWatcher.interrupt();
        }
    }
}
