package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests the cast-free health surface - {@link ParallelConsumer#getHealth()} and the {@link PCHealth} snapshot it
 * returns.
 *
 * @author Antony Stubbs
 * @see PCHealth
 * @see State
 */
@Slf4j
class ParallelConsumerHealthTest extends ParallelEoSStreamProcessorTestBase {

    /**
     * AE1 - a running instance reports healthy with a controller state of {@link State#RUNNING}.
     * <p>
     * The variable is deliberately declared as the <em>interface</em> type. That declaration is the regression guard:
     * a test written against the concrete type would still compile and pass if {@code getHealth()} were only present
     * on the implementation, which is precisely the bug this API exists to fix.
     */
    @Test
    void runningConsumerIsHealthyAndReportsRunningWithoutACast() {
        ParallelConsumer<String, String> pc = parallelConsumer;

        parallelConsumer.poll(context -> log.debug("Processing {}", context));

        PCHealth health = pc.getHealth();

        assertThat(health.getControllerState()).isEqualTo(State.RUNNING);
        assertThat(health.isHealthy()).isTrue();
        assertThat(health.getFailureCause()).isEmpty();
    }

    /**
     * AE3 - a clean shutdown is not healthy, but is distinguishable from a crash by the absence of a failure cause.
     */
    @Test
    void cleanCloseIsNotHealthyAndCarriesNoFailureCause() {
        ParallelConsumer<String, String> pc = parallelConsumer;
        parallelConsumer.poll(context -> log.debug("Processing {}", context));

        pc.close();

        PCHealth health = pc.getHealth();

        assertThat(health.getControllerState()).isEqualTo(State.CLOSED);
        assertThat(health.isHealthy()).isFalse();
        assertThat(health.getFailureCause()).isEmpty();
        assertThat(health.isStateObserved())
                .as("the real processor observes its states rather than deriving them")
                .isTrue();
        // Every other poller assertion in this class expects RUNNING, which is the field's own initialiser - so
        // without this one, wiring the accessor to a constant would pass the whole suite.
        assertThat(health.getPollerState())
                .as("the poller state must be read, not echoed from its initial value")
                .isIn(State.CLOSING, State.CLOSED);
    }

    /**
     * AE2 - a crashed control loop is not healthy, and the recorded exception is reported.
     * <p>
     * Induced with the same mechanism {@code ParallelEoSStreamProcessorTest#controlFlowException} uses: a loop-end
     * callback that throws.
     */
    @Test
    void crashedControlLoopIsNotHealthyAndReportsTheRecordedFailure() {
        ParallelConsumer<String, String> pc = parallelConsumer;

        parallelConsumer.addLoopEndCallBack(() -> {
            throw new FakeRuntimeException("My fake control loop error");
        });

        parallelConsumer.poll(context -> log.debug("Processing {}", context));

        // Wait for the instance to reach its terminal state, not merely for the cause to be recorded. The cause is
        // written before the close transition, so awaiting only that leaves the close still in flight - and the test
        // base deliberately skips closing an already-errored PC, so a half-closed one gets closed in @AfterEach and
        // rethrows the recorded failure out of teardown.
        await().atMost(defaultTimeout)
                .untilAsserted(() -> assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

        PCHealth health = pc.getHealth();

        assertThat(health.isHealthy()).isFalse();
        assertThat(health.getFailureCause()).isPresent();
        assertThat(health.getFailureCause()).containsSame(parallelConsumer.getFailureCause());
        assertThat(health.getFailureCause().get()).hasRootCauseInstanceOf(FakeRuntimeException.class);
    }

    /**
     * A control thread killed by something that is not an {@link Exception} records nothing - the control task's
     * handler catches {@link Exception}, so an {@link Error} escapes it, leaving the run state on {@link State#RUNNING}
     * and the failure cause null. The instance is dead all the same.
     * <p>
     * This is the failure a liveness probe exists to catch: a subsystem gone inside a JVM that is still up. The
     * verdict has to be reachable from the control thread's own liveness, not only from the two fields the failure
     * paths happen to write.
     */
    @Test
    void aControlThreadKilledByAnErrorIsNotHealthy() {
        ParallelConsumer<String, String> pc = parallelConsumer;

        parallelConsumer.addLoopEndCallBack(() -> {
            throw new StackOverflowError("fake control loop Error - not an Exception");
        });

        parallelConsumer.poll(context -> log.debug("Processing {}", context));

        await().atMost(defaultTimeout)
                .untilAsserted(() -> assertThat(parallelConsumer.isClosedOrFailed())
                        .as("control thread should be dead")
                        .isTrue());

        PCHealth health = pc.getHealth();

        assertThat(health.isHealthy())
                .as("a dead control thread is not healthy, even though nothing was recorded on it")
                .isFalse();
    }

    /**
     * The new snapshot must never be weaker than the boolean it supersedes. Whenever {@code isClosedOrFailed()} says
     * the instance is done, the verdict has to agree - otherwise a caller who migrated from one to the other silently
     * loses coverage of whichever failures only the old one saw.
     */
    @Test
    void aHealthyVerdictNeverContradictsIsClosedOrFailed() {
        ParallelConsumer<String, String> pc = parallelConsumer;

        assertHealthAgreesWithIsClosedOrFailed(pc, "before poll");

        parallelConsumer.poll(context -> log.debug("Processing {}", context));
        assertHealthAgreesWithIsClosedOrFailed(pc, "running");

        parallelConsumer.pauseIfRunning();
        assertHealthAgreesWithIsClosedOrFailed(pc, "paused");

        parallelConsumer.resumeIfPaused();
        parallelConsumer.close();
        assertHealthAgreesWithIsClosedOrFailed(pc, "closed");
    }

    private static void assertHealthAgreesWithIsClosedOrFailed(ParallelConsumer<String, String> pc, String phase) {
        if (pc.isClosedOrFailed()) {
            assertThat(pc.getHealth().isHealthy())
                    .as("%s: isClosedOrFailed() is true, so the verdict must not be healthy", phase)
                    .isFalse();
        }
    }

    /**
     * AE4 and AE5 - a deliberate pause is not a reason to restart the process, and the snapshot reports the
     * controller/poller divergence rather than collapsing it.
     */
    @Test
    void pausedControllerIsHealthyAndTheStillRunningPollerIsReportedSeparately() {
        ParallelConsumer<String, String> pc = parallelConsumer;
        parallelConsumer.poll(context -> log.debug("Processing {}", context));

        pc.pauseIfRunning();

        PCHealth health = pc.getHealth();

        assertThat(health.getControllerState()).isEqualTo(State.PAUSED);
        assertThat(health.getPollerState()).isEqualTo(State.RUNNING);
        assertThat(health.isHealthy()).isTrue();
    }

    /**
     * Before {@code poll()} is ever called the controller has not started, but the poller's run state field
     * initialises to {@link State#RUNNING}. The snapshot must report that divergence honestly rather than mirroring
     * one field onto the other - and must not throw.
     */
    @Test
    void beforePollTheControllerIsUnusedWhileThePollerAlreadyReadsRunning() {
        ParallelConsumer<String, String> pc = parallelConsumer;

        PCHealth health = pc.getHealth();

        assertThat(health.getControllerState()).isEqualTo(State.UNUSED);
        assertThat(health.getPollerState()).isEqualTo(State.RUNNING);
        assertThat(health.isHealthy()).isTrue();
        assertThat(health.getFailureCause()).isEmpty();
    }

    /**
     * AE6 - the only in-repo proof that a third-party implementor of {@link ParallelConsumer} still compiles. This
     * class overrides no health method at all, so the fact that it compiles is the test, and the assertions below
     * check the inherited default derives a sensible verdict from {@code isClosedOrFailed()}.
     */
    @Test
    void thirdPartyImplementorCompilesAndInheritsAUsableDefault() {
        StubParallelConsumer stub = new StubParallelConsumer();
        ParallelConsumer<String, String> pc = stub;

        PCHealth alive = pc.getHealth();
        assertThat(alive.getControllerState()).isEqualTo(State.RUNNING);
        assertThat(alive.getPollerState()).isEqualTo(State.RUNNING);
        assertThat(alive.isHealthy()).isTrue();
        assertThat(alive.getFailureCause()).isEmpty();

        stub.closedOrFailed = true;

        PCHealth dead = pc.getHealth();
        assertThat(dead.getControllerState()).isEqualTo(State.CLOSED);
        assertThat(dead.getPollerState()).isEqualTo(State.CLOSED);
        assertThat(dead.isHealthy()).isFalse();
        // The default cannot tell a crash from a clean close, so it must not let an absent cause be read as one.
        assertThat(dead.getFailureCause()).isEmpty();
        assertThat(dead.isStateObserved())
                .as("a derived snapshot must say so, or its empty failure cause reads as a clean shutdown")
                .isFalse();
        assertThat(alive.isStateObserved()).isFalse();
    }

    /**
     * A third-party shaped implementor: implements only the abstract methods {@link ParallelConsumer} and its
     * supertype {@code DrainingCloseable} declare, and overrides nothing else.
     * <p>
     * <strong>If this class stops compiling, a non-{@code default} method was added to {@link ParallelConsumer} or
     * {@code DrainingCloseable}, and every third-party implementor just broke.</strong> The fix is to make that method
     * {@code default}, or to take the breaking change deliberately and record it in {@code docs/refactoring.md} -
     * never to implement the new method here. Implementing it here turns the build green and silently deletes the
     * only guard this repo has for that guarantee.
     */
    private static class StubParallelConsumer implements ParallelConsumer<String, String> {

        boolean closedOrFailed = false;

        @Override
        public boolean isClosedOrFailed() {
            return closedOrFailed;
        }

        @Override
        public void subscribe(Collection<String> topics) {
            // no-op
        }

        @Override
        public void subscribe(Pattern pattern) {
            // no-op
        }

        @Override
        public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
            // no-op
        }

        @Override
        public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
            // no-op
        }

        @Override
        public void pauseIfRunning() {
            // no-op
        }

        @Override
        public void resumeIfPaused() {
            // no-op
        }

        @Override
        public void close(Duration timeout, DrainingMode drainingMode) {
            closedOrFailed = true;
        }

        @Override
        public void close(DrainingMode drainingMode) {
            closedOrFailed = true;
        }

        @Override
        public long workRemaining() {
            return 0;
        }
    }
}
