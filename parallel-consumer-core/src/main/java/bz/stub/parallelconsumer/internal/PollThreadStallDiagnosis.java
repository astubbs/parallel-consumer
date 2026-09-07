package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

/**
 * Answers the one question a commit-response timeout cannot answer about itself: is the broker-poll
 * thread <b>blocked</b>, or merely <b>slow</b>?
 * <p>
 * {@link ConsumerOffsetCommitter}'s timeout message already says the poll thread "is blocked or
 * slower than the configured offsetCommitTimeout" - correctly, because at that point nothing has
 * looked. Those two branches want opposite responses (a lock-ordering defect versus a broker or
 * sizing problem), and telling them apart after the fact needs a thread dump taken at the moment of
 * the timeout, which nothing captured. So every such failure has been triaged by matching
 * preconditions and arguing from plausibility.
 * <p>
 * <b>The incident this exists to end.</b> On 2026-08-25 a CI run failed
 * {@code MultiInstanceRebalanceTest.consumeWithMultipleInstancesPeriodicConsumerSync} with exactly
 * that message. It landed on all three preconditions recorded for the revoke-path AB-BA cycle -
 * multiple consumers, a rebalance, and {@code PERIODIC_CONSUMER_SYNC} - and still could not be
 * called: a broker slow enough to miss a 10s deadline produces an identical message. The sighting is
 * in {@code docs/inflight/bug-857-family.md}, and it names this missing thread dump as the
 * discriminator. This class is that discriminator.
 * <p>
 * <b>Cost is paid only on the failure path.</b> Nothing here runs unless a commit request has
 * already missed its deadline, so a healthy run never touches the {@link ThreadMXBean}.
 *
 * @see ConsumerOffsetCommitter
 */
@Slf4j
final class PollThreadStallDiagnosis {

    /** Enough frames to identify where a thread is parked without pasting an entire dump into a log line. */
    private static final int STACK_FRAMES_REPORTED = 12;

    private PollThreadStallDiagnosis() {
    }

    /**
     * The facts a diagnosis is drawn from, separated from the {@link ThreadMXBean} that supplies them
     * so {@link #verdict} can be tested against a real monitor deadlock without arranging one.
     */
    @Value
    @lombok.Builder
    static class ThreadFacts {
        /** {@code null} when no thread of that name was found - it may already have exited. */
        String state;
        /** The lock this thread is waiting to acquire or waiting on, or {@code null}. */
        String lockName;
        /** The thread holding {@link #lockName}, or {@code null} when the lock is unheld or unknown. */
        String lockOwnerName;
        /** Top frames, outermost first; may be empty. */
        List<String> stack;
        /** Threads the JVM itself reports as deadlocked; empty when it reports none. */
        List<String> jvmReportedDeadlock;
        /**
         * Whether the diagnosed thread is IN that cycle. A cycle elsewhere in the embedding JVM is
         * real, but it is not why this commit timed out - and reporting it as the cause would be the
         * same misattribution this class exists to prevent, arrived at from the other direction.
         */
        boolean pollThreadInCycle;
    }

    /**
     * Read the named thread's current state, and ask the JVM whether it can see a deadlock.
     *
     * @param pollThread the broker-poll thread, the one this committer's {@code claim()} recorded
     */
    static ThreadFacts capture(Thread pollThread) {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();

        List<String> deadlocked = new ArrayList<>();
        // findDeadlockedThreads() also covers ownable synchronizers (ReentrantLock) but is optional;
        // findMonitorDeadlockedThreads() only covers `synchronized` monitors and is always present.
        // The revoke-path cycle is a monitor deadlock, so the fallback still catches the case that
        // motivated this class.
        long[] cycle = bean.isSynchronizerUsageSupported()
                ? bean.findDeadlockedThreads()
                : bean.findMonitorDeadlockedThreads();
        boolean inCycle = false;
        if (cycle != null) {
            for (long id : cycle) {
                if (id == pollThread.getId()) {
                    inCycle = true;
                }
            }
            for (ThreadInfo info : bean.getThreadInfo(cycle, STACK_FRAMES_REPORTED)) {
                if (info != null) {
                    deadlocked.add(info.getThreadName() + " (blocked on " + info.getLockName()
                            + " held by " + info.getLockOwnerName() + ")");
                }
            }
        }

        // getThreadInfo(id, depth) rather than scanning dumpAllThreads() by name: the caller already
        // holds the thread, and the two return the same getLockName()/getLockOwnerName() "blocked-on"
        // facts. dumpAllThreads' extra flags only add the OWNED monitor/synchronizer lists, which
        // nothing here reads - so this is the same evidence at O(1) instead of O(threads in the
        // embedding JVM). That matters because this is a library other people embed.
        ThreadInfo info = bean.getThreadInfo(pollThread.getId(), STACK_FRAMES_REPORTED);
        if (info == null) {
            // The thread died between the timeout and this read.
            return ThreadFacts.builder().jvmReportedDeadlock(deadlocked).pollThreadInCycle(inCycle)
                    .stack(new ArrayList<>()).build();
        }
        List<String> frames = new ArrayList<>();
        for (StackTraceElement frame : info.getStackTrace()) {
            frames.add(frame.toString());
        }
        return ThreadFacts.builder()
                .state(info.getThreadState().name())
                .lockName(info.getLockName())
                .lockOwnerName(info.getLockOwnerName())
                .stack(frames)
                .jvmReportedDeadlock(deadlocked)
                .pollThreadInCycle(inCycle)
                .build();
    }

    /**
     * Turn the facts into a verdict a reader can act on, leading with which of the timeout message's
     * two branches actually happened.
     *
     * <p>Deliberately states a conclusion rather than only dumping the stack: the stack was always
     * obtainable in principle, and the reason these failures stayed ambiguous is that nobody had one
     * at the right moment and nobody wrote down what it would have meant.
     */
    static String verdict(String threadName, ThreadFacts facts) {
        StringBuilder sb = new StringBuilder(512);

        if (facts.isPollThreadInCycle()) {
            sb.append("DEADLOCK - the JVM itself reports a cycle CONTAINING this thread, so this is a ")
                    .append("lock-ordering defect and NOT a slow broker: ")
                    .append(facts.getJvmReportedDeadlock());
            return sb.toString();
        }
        // A cycle that does NOT contain the poll thread is real but is not why this commit timed out.
        // findDeadlockedThreads() is JVM-GLOBAL, so in an embedding application it can report a
        // deadlock in entirely unrelated code. Carry it as context and let the per-state branches
        // below describe the thread that actually missed the deadline.
        String unrelatedCycle = facts.getJvmReportedDeadlock().isEmpty() ? ""
                : " (note: an unrelated deadlock exists elsewhere in this JVM, not involving this "
                + "thread: " + facts.getJvmReportedDeadlock() + ")";
        if (facts.getState() == null) {
            return "INCONCLUSIVE - the thread '" + threadName + "' is no longer live. It exited without "
                    + "being recorded as dead; neither branch of the timeout message is established."
                    + unrelatedCycle;
        }

        switch (facts.getState()) {
            case "BLOCKED":
                sb.append("BLOCKED - the poll thread is waiting to acquire a monitor, so this is contention ")
                        .append("or a lock-ordering defect, NOT a slow broker. Lock: ")
                        .append(facts.getLockName()).append(", held by: ").append(facts.getLockOwnerName());
                break;
            case "WAITING":
            case "TIMED_WAITING":
                sb.append("WAITING - the poll thread is parked rather than working, so it is not being ")
                        .append("slowed by the broker. Waiting on: ").append(facts.getLockName())
                        .append(facts.getLockOwnerName() == null ? "" : ", held by: " + facts.getLockOwnerName());
                break;
            case "RUNNABLE":
                sb.append("RUNNABLE - the poll thread is executing, so nothing is holding it: read this as ")
                        .append("SLOWER THAN THE TIMEOUT rather than blocked. Raise offsetCommitTimeout, or ")
                        .append("look at broker latency and batch sizing, before suspecting a lock.");
                break;
            default:
                sb.append(facts.getState()).append(" - unexpected for the poll thread at a commit timeout.");
        }
        sb.append(". Top frames: ").append(facts.getStack()).append(unrelatedCycle);
        return sb.toString();
    }

    /**
     * Capture and judge in one call, for the throw site. An unavailable or restricted management API
     * is turned into text rather than into a different failure - a diagnostic must not replace the
     * failure it is describing. A {@link VirtualMachineError} still propagates, deliberately: it is
     * not something to report on, and swallowing it would hide a dying JVM behind a commit timeout.
     */
    static String diagnose(Thread pollThread) {
        try {
            return verdict(pollThread.getName(), capture(pollThread));
        } catch (Exception | LinkageError e) {
            // A management API that is unavailable or restricted must not turn a commit timeout into
            // a different, more confusing failure - but do not whisper about it either. Reaching here
            // means the blocked-vs-slow question went unanswered for an incident that asked it, and a
            // class whose whole purpose is to stop silent non-answers should not log its own at DEBUG.
            // The commit timeout is already failing, so this costs no noise on a healthy run.
            log.warn("Could not capture poll-thread diagnosis - the blocked-vs-slow question is "
                    + "unanswered for this timeout", e);
            return "UNAVAILABLE - could not read thread state (" + e + ")";
        }
    }
}
