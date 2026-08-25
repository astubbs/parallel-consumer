package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitFailureContinueMode;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.CommitFailureSeamState;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * What an operator sees of the commit-failure seam (astubbs#317, confluentinc#833): the four meters in the
 * {@code committer} subsystem, and the ERROR line every exhaustion lands.
 * <p>
 * The scenarios pin the registration under the declared public names, the readings while a CONTINUE decision keeps
 * a failing instance alive (counter, streak gauge, seam state, time-since-last-success, and the log loudness), and
 * the seam-state gauge across the PAUSE_INTAKE engage/release pair. These are the only scenarios that set
 * {@code meterRegistry}; every other one runs with PC's metrics in their no-op mode.
 * <p>
 * The fixture - the failing {@link MockConsumer}, the recording handler, the waits - is
 * {@link MockConsumerCommitFailureSeamTestBase}, which also names the other slices of the seam.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 * @see PCMetricsDef
 */
class MockConsumerCommitFailureMetricsTest extends MockConsumerCommitFailureSeamTestBase {

    /**
     * The registration pin: the seam's four meters register under their declared public names, in the
     * {@code committer} subsystem. The names are asserted as string literals, not via the enum's own getters, so a
     * rename of the public metric name cannot slip through by staying self-consistent.
     */
    @Test
    void seamMetersRegisterUnderTheDeclaredNamesInTheCommitterSubsystem() {
        meterRegistry = new SimpleMeterRegistry();
        var healed = new AtomicBoolean(true); // commits healthy throughout - registration needs no failure
        useCommitsTimingOut(healed);
        // the handler is wired but, with commits healthy, never consulted - so nothing here asserts on it
        startPc(SMALL_BUDGET, continuingHandler());
        addRecordsAndProcess();
        awaitCommittedOffset(RECORDS);

        assertWithMessage("the exhaustions counter must register under its fixed name")
                .that(meterRegistry.find("pc.commit.failure.exhaustions")
                        .tag("subsystem", "committer").counter()).isNotNull();
        assertWithMessage("the consecutive-exhaustions gauge must register under its fixed name")
                .that(meterRegistry.find("pc.commit.failure.consecutive.exhaustions")
                        .tag("subsystem", "committer").gauge()).isNotNull();
        assertWithMessage("the time-since-last-success gauge must register under its fixed name")
                .that(meterRegistry.find("pc.commit.time.since.last.success")
                        .tag("subsystem", "committer").gauge()).isNotNull();
        assertWithMessage("the seam-state gauge must register under its fixed name")
                .that(meterRegistry.find("pc.commit.failure.seam.state")
                        .tag("subsystem", "committer").gauge()).isNotNull();

        // and a healthy instance reads healthy: no exhaustions, no streak, seam state HEALTHY
        assertThat(committerCounterValue(PCMetricsDef.COMMIT_FAILURE_EXHAUSTIONS)).isEqualTo(0.0);
        assertThat(committerGaugeValue(PCMetricsDef.COMMIT_FAILURE_CONSECUTIVE_EXHAUSTIONS)).isEqualTo(0.0);
        assertThat(committerGaugeValue(PCMetricsDef.COMMIT_FAILURE_SEAM_STATE))
                .isEqualTo((double) CommitFailureSeamState.HEALTHY.getValue());
    }

    /**
     * The loudness guarantee under CONTINUE: while commits fail and the handler keeps deciding CONTINUE, the
     * exhaustions counter counts every exhaustion, the consecutive gauge tracks the streak, the seam-state gauge
     * reports FAILING_CONTINUING (the KEEP_PROCESSING half of the transition set - the PAUSE_INTAKE half is
     * {@link #seamStateGaugeReportsPauseEngageAndRelease()}), the time-since-last-success gauge spans the whole
     * failing period measured from the assignment (the epoch rule: nothing ever succeeded here, so a
     * measure-from-last-success bug would read near zero), and one ERROR log line lands per exhaustion, naming the
     * CONTINUE decision. On heal, the streak resets while the counter stays monotonic.
     */
    @Test
    void continueExhaustionsAreLoudCountedAndReportedByTheGauges() {
        meterRegistry = new SimpleMeterRegistry();
        var healed = new AtomicBoolean(false);
        useCommitsTimingOut(healed);
        var handler = continuingHandler();

        var processorLogger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        processorLogger.addAppender(appender);
        try {
            startPc(SMALL_BUDGET, handler);
            addRecordsAndProcess();

            awaitAsserted(() -> assertThat(handler.contexts.size()).isAtLeast(2));

            // no commit ever succeeded and healed is untouched, so the streak the handler saw is still current
            int exhaustionsSeen = handler.contexts.size();
            assertThat(committerCounterValue(PCMetricsDef.COMMIT_FAILURE_EXHAUSTIONS))
                    .isAtLeast((double) exhaustionsSeen);
            assertThat(committerGaugeValue(PCMetricsDef.COMMIT_FAILURE_CONSECUTIVE_EXHAUSTIONS))
                    .isAtLeast((double) exhaustionsSeen);
            assertThat(committerGaugeValue(PCMetricsDef.COMMIT_FAILURE_SEAM_STATE))
                    .isEqualTo((double) CommitFailureSeamState.FAILING_CONTINUING.getValue());
            // two exhausted budgets deep, at least one whole budget has passed since the assignment started -
            // and nothing ever succeeded, so the epoch rule is what makes this reachable at all
            assertThat(committerGaugeValue(PCMetricsDef.COMMIT_TIME_SINCE_LAST_SUCCESS))
                    .isAtLeast(SMALL_BUDGET.toMillis() / 1000.0);

            // loudness: one ERROR per exhaustion regardless of decision, and the CONTINUE branch names its decision
            long terminalFailureErrors = appender.list.stream()
                    .filter(event -> event.getLevel() == Level.ERROR)
                    .filter(event -> event.getFormattedMessage()
                            .contains("failed terminally - retry budget exhausted"))
                    .count();
            assertWithMessage("every exhaustion must land one ERROR - a continuing instance is never quiet")
                    .that(terminalFailureErrors).isAtLeast((long) exhaustionsSeen);
            assertWithMessage("the CONTINUE branch's ERROR must name the decision")
                    .that(appender.list.stream()
                            .filter(event -> event.getLevel() == Level.ERROR)
                            .anyMatch(event -> event.getFormattedMessage().contains("decided CONTINUE")))
                    .isTrue();

            // heal: the streak resets, the seam reads healthy again, and the counter never goes backwards
            healed.set(true);
            awaitCommittedOffset(RECORDS);
            awaitAsserted(() -> {
                assertThat(committerGaugeValue(PCMetricsDef.COMMIT_FAILURE_CONSECUTIVE_EXHAUSTIONS)).isEqualTo(0.0);
                assertThat(committerGaugeValue(PCMetricsDef.COMMIT_FAILURE_SEAM_STATE))
                        .isEqualTo((double) CommitFailureSeamState.HEALTHY.getValue());
                // the epoch moved to the successful commit; commits keep succeeding on the (100ms) cadence, so
                // this stays far below the failing period's reading
                assertThat(committerGaugeValue(PCMetricsDef.COMMIT_TIME_SINCE_LAST_SUCCESS)).isLessThan(5.0);
            });
            assertThat(committerCounterValue(PCMetricsDef.COMMIT_FAILURE_EXHAUSTIONS))
                    .isAtLeast((double) exhaustionsSeen);
        } finally {
            processorLogger.detachAppender(appender);
            appender.stop();
        }
    }

    /**
     * The PAUSE_INTAKE half of the seam-state transitions: HEALTHY while commits succeed, FAILING_PAUSED once a
     * CONTINUE decision engages the intake pause, HEALTHY again when a successful commit releases it. Structure per
     * the pause-intake scenario: a clean opening commit first, so the exhaustion lands deterministically after
     * HEALTHY was
     * observed.
     */
    @Test
    void seamStateGaugeReportsPauseEngageAndRelease() {
        meterRegistry = new SimpleMeterRegistry();
        var commitsHealthy = new AtomicBoolean(true);
        useCommitsTimingOut(commitsHealthy);
        var handler = continuingHandler();
        startPc(SMALL_BUDGET, handler, CommitFailureContinueMode.PAUSE_INTAKE);
        addRecordsAndProcess();
        awaitCommittedOffset(RECORDS);
        assertThat(committerGaugeValue(PCMetricsDef.COMMIT_FAILURE_SEAM_STATE))
                .isEqualTo((double) CommitFailureSeamState.HEALTHY.getValue());

        // break commits and drive one record through: the exhaustion's CONTINUE engages the seam pause
        commitsHealthy.set(false);
        addRecords(RECORDS, 1);
        awaitAsserted(() -> assertThat(committerGaugeValue(PCMetricsDef.COMMIT_FAILURE_SEAM_STATE))
                .isEqualTo((double) CommitFailureSeamState.FAILING_PAUSED.getValue()));

        // the next successful commit releases the pause and the gauge heals with it
        commitsHealthy.set(true);
        awaitAsserted(() -> assertThat(committerGaugeValue(PCMetricsDef.COMMIT_FAILURE_SEAM_STATE))
                .isEqualTo((double) CommitFailureSeamState.HEALTHY.getValue()));
        awaitCommittedOffset(RECORDS + 1);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
        assertThat(parallelConsumer.getFailureCause()).isNull();
    }

    /** The registered committer-subsystem gauge's current value; fails the test if it is not registered. */
    private double committerGaugeValue(PCMetricsDef def) {
        var gauge = meterRegistry.find(def.getName()).tag("subsystem", "committer").gauge();
        assertWithMessage("gauge %s must be registered", def.getName()).that(gauge).isNotNull();
        return gauge.value();
    }

    /** The registered committer-subsystem counter's current count; fails the test if it is not registered. */
    private double committerCounterValue(PCMetricsDef def) {
        var counter = meterRegistry.find(def.getName()).tag("subsystem", "committer").counter();
        assertWithMessage("counter %s must be registered", def.getName()).that(counter).isNotNull();
        return counter.count();
    }
}
