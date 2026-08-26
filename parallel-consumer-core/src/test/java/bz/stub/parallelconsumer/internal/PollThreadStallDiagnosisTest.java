package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Covers {@link PollThreadStallDiagnosis}, whose whole job is to answer a question the commit-timeout
 * message cannot: is the broker-poll thread <b>blocked</b> or merely <b>slow</b>?
 * <p>
 * The verdict cases are driven through the {@link PollThreadStallDiagnosis.ThreadFacts} seam so each
 * branch is asserted exactly. {@link #aRealMonitorDeadlockIsDetectedAndNamedAsSuch()} then arranges a
 * genuine AB-BA deadlock between two live threads and asserts the JVM route reports it - because a
 * detector whose only evidence is hand-built facts has never been shown to observe anything, and this
 * one exists precisely because nobody had real evidence at the moment it was needed.
 */
class PollThreadStallDiagnosisTest {

    private static final String POLL_THREAD = "pc-broker-poll-7";

    private static PollThreadStallDiagnosis.ThreadFacts facts(String state, String lock, String owner) {
        return PollThreadStallDiagnosis.ThreadFacts.builder()
                .state(state).lockName(lock).lockOwnerName(owner)
                .stack(Arrays.asList("a.b.C.method(C.java:1)"))
                .jvmReportedDeadlock(new ArrayList<>())
                .build();
    }

    /**
     * The case the whole class exists for. A BLOCKED poll thread means something holds a monitor it
     * wants - so the timeout is contention or lock ordering, and raising the timeout would only make
     * the wedge take longer to notice.
     */
    @Test
    void blockedIsReportedAsALockProblemAndExplicitlyNotASlowBroker() {
        String v = PollThreadStallDiagnosis.verdict(POLL_THREAD,
                facts("BLOCKED", "java.lang.Object@1", "pc-control-1"));

        assertThat(v).startsWith("BLOCKED");
        assertWithMessage("naming the holder is the whole value - a lock with no owner names no suspect")
                .that(v).contains("held by: pc-control-1");
        assertWithMessage("the reader must not be left to re-derive which branch of the message this is")
                .that(v).ignoringCase().contains("not a slow broker");
    }

    /**
     * The opposite verdict, and the one that must NOT read as a defect: a running thread is not being
     * held by anything, so the honest response is timeout/broker sizing rather than a lock hunt.
     */
    @Test
    void runnableIsReportedAsSlownessAndPointsAtTheTimeoutRatherThanALock() {
        String v = PollThreadStallDiagnosis.verdict(POLL_THREAD, facts("RUNNABLE", null, null));

        assertThat(v).startsWith("RUNNABLE");
        assertThat(v).contains("offsetCommitTimeout");
        assertWithMessage("a RUNNABLE thread must never be reported as blocked")
                .that(v).doesNotContain("BLOCKED");
    }

    @Test
    void waitingIsDistinguishedFromBothBlockedAndRunnable() {
        String v = PollThreadStallDiagnosis.verdict(POLL_THREAD,
                facts("TIMED_WAITING", "java.util.concurrent.locks.AbstractQueuedSynchronizer@2", null));

        assertThat(v).startsWith("WAITING");
        assertWithMessage("parked is not the broker's fault, and must not be reported as slowness")
                .that(v).ignoringCase().contains("not being");
    }

    /**
     * A thread that has vanished must produce an explicit non-answer. Reporting "not blocked" for an
     * absent thread would silently support the slow-broker branch on no evidence, which is the exact
     * unestablished claim this class was added to stop.
     */
    @Test
    void aMissingThreadIsInconclusiveRatherThanEvidenceForEitherBranch() {
        String v = PollThreadStallDiagnosis.verdict(POLL_THREAD, PollThreadStallDiagnosis.ThreadFacts.builder()
                .stack(new ArrayList<>()).jvmReportedDeadlock(new ArrayList<>()).build());

        assertThat(v).startsWith("INCONCLUSIVE");
        assertThat(v).contains(POLL_THREAD);
        assertThat(v).doesNotContain("RUNNABLE");
    }

    /** A JVM-confirmed cycle outranks the per-thread state: it is proof, not inference. */
    @Test
    void aJvmReportedCycleOutranksTheThreadState() {
        PollThreadStallDiagnosis.ThreadFacts f = PollThreadStallDiagnosis.ThreadFacts.builder()
                .state("RUNNABLE")
                .stack(new ArrayList<>())
                .pollThreadInCycle(true)
                .jvmReportedDeadlock(Arrays.asList("pc-broker-poll-7 (blocked on X held by pc-control-1)"))
                .build();

        String v = PollThreadStallDiagnosis.verdict(POLL_THREAD, f);

        assertThat(v).startsWith("DEADLOCK");
        assertWithMessage("proof must not be downgraded to the thread's own momentary state")
                .that(v).doesNotContain("RUNNABLE - ");
    }

    /**
     * The case the P1 review finding named, and the one the class most easily gets wrong:
     * {@code findDeadlockedThreads()} is JVM-GLOBAL, so in an embedding application it can report a
     * deadlock in entirely unrelated code. Reporting that as the cause of THIS commit timeout would
     * be exactly the misattribution the class exists to prevent, reached from the other direction.
     */
    @Test
    void aCycleNotContainingThisThreadIsContextRatherThanTheVerdict() {
        PollThreadStallDiagnosis.ThreadFacts f = PollThreadStallDiagnosis.ThreadFacts.builder()
                .state("RUNNABLE")
                .stack(new ArrayList<>())
                .pollThreadInCycle(false)
                .jvmReportedDeadlock(Arrays.asList("some-unrelated-app-thread (blocked on X held by Y)"))
                .build();

        String v = PollThreadStallDiagnosis.verdict(POLL_THREAD, f);

        assertWithMessage("the verdict must describe the thread that actually missed the deadline, not "
                + "an unrelated cycle elsewhere in the JVM")
                .that(v).startsWith("RUNNABLE");
        assertWithMessage("the cycle is still worth surfacing - just as context, not as the cause")
                .that(v).contains("unrelated deadlock exists elsewhere");
        assertThat(v).doesNotContain("DEADLOCK - ");
    }

    /**
     * The non-vacuity proof: build a real AB-BA deadlock and assert the live capture path sees it.
     * Without this, every other case here is assertions about a formatter.
     *
     * <p><b>{@link ReentrantLock#lockInterruptibly()}, not {@code synchronized} - and that is not a
     * style choice.</b> Surefire runs with {@code reuseForks=true}, so a JVM fork is shared by many
     * test classes, and {@code ThreadMXBean.findDeadlockedThreads()} is JVM-GLOBAL. Threads left
     * deadlocked on intrinsic monitors cannot be released - interrupt does not free a thread blocked
     * entering {@code synchronized} - so they would survive for the rest of the fork and any later
     * test reaching this class would read a {@code DEADLOCK} verdict caused by THIS test rather than
     * by its own scenario. {@code CommitResponseTimeoutSymptomTest} drives the same code path, so the
     * contamination is reachable, not hypothetical. An ownable synchronizer is detected by
     * {@code findDeadlockedThreads()} exactly as a monitor is, and stays interruptible - so the arms
     * can be released in a {@code finally} instead of leaking.
     */
    @Test
    @Timeout(30)
    void aRealMonitorDeadlockIsDetectedAndNamedAsSuch() throws InterruptedException {
        ReentrantLock lockA = new ReentrantLock();
        ReentrantLock lockB = new ReentrantLock();
        CountDownLatch bothHoldOne = new CountDownLatch(2);
        List<Thread> arms = new ArrayList<>();
        arms.add(new Thread(() -> deadlockArm(lockA, lockB, bothHoldOne), "pcs-diag-test-arm-A"));
        arms.add(new Thread(() -> deadlockArm(lockB, lockA, bothHoldOne), "pcs-diag-test-arm-B"));

        try {
            arms.forEach(t -> {
                t.setDaemon(true);
                t.start();
            });
            assertWithMessage("both arms must hold their first lock before either reaches for the second")
                    .that(bothHoldOne.await(10, TimeUnit.SECONDS)).isTrue();

            // The cycle forms once both arms reach their second acquisition; poll rather than sleep a
            // fixed amount, so a slow machine lengthens the test instead of failing it.
            String verdict = "";
            for (int i = 0; i < 100 && !verdict.startsWith("DEADLOCK"); i++) {
                verdict = PollThreadStallDiagnosis.diagnose(arms.get(0));
                if (!verdict.startsWith("DEADLOCK")) {
                    Thread.sleep(100);
                }
            }

            assertWithMessage("a real AB-BA deadlock must be reported as proof rather than inferred "
                    + "from thread state - this is the case the class exists for")
                    .that(verdict).startsWith("DEADLOCK");
            assertThat(verdict).contains("pcs-diag-test-arm-");
        } finally {
            // Release the cycle so it cannot outlive this test in a reused fork - see the javadoc.
            arms.forEach(Thread::interrupt);
            for (Thread t : arms) {
                t.join(TimeUnit.SECONDS.toMillis(5));
                // ASSERTED, not merely attempted: an unasserted join that silently timed out would
                // leave the fork polluted exactly as before, and nothing would say so.
                assertWithMessage("arm '%s' must not outlive this test - findDeadlockedThreads() is "
                        + "JVM-global and surefire reuses forks", t.getName())
                        .that(t.isAlive()).isFalse();
            }
        }
    }

    /**
     * The one corner of this class the final review named as having no direct evidence: the catch
     * boundary itself. The contract under test is not "null is handled" - the only call site maps
     * over an {@code Optional}, so null cannot arrive there - but the boundary's actual promise:
     * <b>when anything inside the capture throws, the diagnosis degrades to text and never
     * propagates</b>, because a diagnostic that replaces the failure it is describing is worse than
     * no diagnostic at all.
     * <p>
     * A null thread is simply the cheapest available trigger. {@link Thread#getName()} and
     * {@link Thread#getId()} are both {@code final} and the {@code ThreadMXBean} is read through
     * {@code ManagementFactory} with no injection seam, so a hostile subclass and a throwing bean
     * are both unavailable - which is precisely why this corner had no test until now.
     */
    @Test
    void aFailureInsideTheCaptureDegradesToTextRatherThanReplacingTheTimeout() {
        String v = PollThreadStallDiagnosis.diagnose(null);

        assertThat(v).startsWith("UNAVAILABLE");
        assertWithMessage("the reader must be told the blocked-vs-slow question went unanswered for "
                + "this timeout, never handed a verdict the class never actually reached")
                .that(v).doesNotContain("BLOCKED");
    }

    /**
     * Deliberately deadlocks against its sibling, and stays deadlocked until interrupted.
     * {@code lockInterruptibly} is what makes that last part possible.
     */
    private static void deadlockArm(ReentrantLock first, ReentrantLock second, CountDownLatch holdingFirst) {
        try {
            first.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            holdingFirst.countDown();
            // Wait until the sibling also holds its first lock, so the cycle is guaranteed rather
            // than raced - then reach for the other one and stay there until interrupted.
            holdingFirst.await(10, TimeUnit.SECONDS);
            second.lockInterruptibly();
            second.unlock();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            first.unlock();
        }
    }
}
