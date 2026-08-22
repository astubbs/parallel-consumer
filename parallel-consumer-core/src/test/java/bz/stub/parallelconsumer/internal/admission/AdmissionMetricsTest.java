package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.internal.admission.AdmissionController.Outcome;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.LoggerFactory;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_MIN_SAMPLES_PER_WINDOW;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.SAMPLE_WINDOW_DURATION;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_CAP;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.COOLDOWN;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.NO_DECISION_YET_VALUE;
import static com.google.common.truth.Truth.assertThat;

/**
 * The controller's OBSERVABILITY surface: the four {@code pc.admission.*} meters, and the rate-limited line that
 * reports the binding constraint whether or not a {@code MeterRegistry} was ever configured.
 * <p>
 * Everything the control law itself does is {@link AdmissionControlLawTest}'s and {@link AdmissionControllerTest}'s
 * job - the windows fed here exist only to make the reported state move. Time is the injected
 * {@link MutableClock} as everywhere else in this package, so no test sleeps.
 * <p>
 * No percentile or histogram meter is asserted anywhere here, deliberately: Micrometer rotates those buckets on
 * wall-clock time, which no injected clock can hold still.
 */
// The no-registry test counts events on the shared AdmissionController class logger, which any concurrently running
// admission test would also write to - the same hazard PipelinePressureLoggingTest documents.
@Isolated("captures a shared class logger")
class AdmissionMetricsTest {

    private static final long MS = 1_000_000L; // nanos per millisecond

    /** Comfortably above the law's per-window minimum so every fed window is acted on. */
    private static final int SAMPLES = DEFAULT_MIN_SAMPLES_PER_WINDOW + 2;

    private final MutableClock clock = MutableClock.epochUTC();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private PCMetrics pcMetrics;

    @AfterEach
    void closeRegistry() {
        registry.close();
    }

    // ------------------------------------------------------------------
    // OBSERVE: the would-be target is the mode's product, and the gauges say so
    // ------------------------------------------------------------------

    @Test
    void observeMovesTheWouldBeTargetGaugeWhileTheLiveTargetGaugeStaysStatic() {
        var controller = instrumented(AdaptiveConcurrencyMode.OBSERVE, DEFAULT_MAX_CONCURRENCY);

        assertThat(gauge(PCMetricsDef.ADMISSION_TARGET)).isEqualTo((double) DEFAULT_MAX_CONCURRENCY);
        assertThat(gauge(PCMetricsDef.ADMISSION_WOULD_BE_TARGET)).isEqualTo((double) DEFAULT_MAX_CONCURRENCY);

        for (int window = 0; window < 5; window++) {
            feedWindowAndTick(controller, 10 * MS, controller.wouldBeTarget());
        }

        // The finding: what ENFORCE would have published...
        assertThat(gauge(PCMetricsDef.ADMISSION_WOULD_BE_TARGET)).isGreaterThan((double) DEFAULT_MAX_CONCURRENCY);
        // ...while the mode published nothing at all.
        assertThat(gauge(PCMetricsDef.ADMISSION_TARGET)).isEqualTo((double) DEFAULT_MAX_CONCURRENCY);
        // A would-be movement is still a movement - it is the mode's whole product.
        assertThat(counter(PCMetricsDef.ADMISSION_MOVEMENTS)).isGreaterThan(0.0);
    }

    // ------------------------------------------------------------------
    // The constraint gauge
    // ------------------------------------------------------------------

    @Test
    void theConstraintGaugeReadsTheReasonsHandAssignedValue() {
        // User-set ceiling 4, healthy saturated windows: the gradient wants more than 4, so the cap binds.
        var controller = instrumented(AdaptiveConcurrencyMode.ENFORCE, 4);

        // Nothing has been decided yet, and zero is reserved to say exactly that.
        assertThat(gauge(PCMetricsDef.ADMISSION_CONSTRAINT)).isEqualTo((double) NO_DECISION_YET_VALUE);

        // Stay under the law's probe-down cadence, so this pins the cap rather than the re-measure probe.
        for (int window = 0; window < 4; window++) {
            feedWindowAndTick(controller, 10 * MS, controller.currentTarget());
        }

        assertThat(controller.lastDecisionReason()).hasValue(AT_CAP);
        assertThat(gauge(PCMetricsDef.ADMISSION_CONSTRAINT)).isEqualTo((double) AT_CAP.getValue());
        // Hand-assigned, so it must not track the declaration position.
        assertThat(AT_CAP.getValue()).isNotEqualTo(AT_CAP.ordinal());
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Test
    void theAdmissionMetersAreReclaimedWhenTheMetricsSubsystemCloses() {
        instrumented(AdaptiveConcurrencyMode.ENFORCE, 8);

        assertThat(admissionMeterNames()).containsExactly(
                PCMetricsDef.ADMISSION_TARGET.getName(),
                PCMetricsDef.ADMISSION_WOULD_BE_TARGET.getName(),
                PCMetricsDef.ADMISSION_CONSTRAINT.getName(),
                PCMetricsDef.ADMISSION_MOVEMENTS.getName());

        pcMetrics.close();

        // Reclaimed by the SHARED path, not one of the controller's own - nothing of ours is left behind.
        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void disabledRegistersNothing() {
        instrumented(AdaptiveConcurrencyMode.DISABLED, 8);

        // A flat gauge would read as "measured and steady" rather than "switched off".
        assertThat(registry.getMeters()).isEmpty();
    }

    // ------------------------------------------------------------------
    // R12: the reporting works with NO MeterRegistry configured
    // ------------------------------------------------------------------

    @Test
    void withNoMeterRegistryConfiguredTheBindingConstraintIsStillReportedAndRateLimited() {
        // What a user trying OBSERVE out actually has: metrics constructed, no registry behind them.
        var noRegistryMetrics = new PCMetrics(null, UniLists.of(), "no-registry-test");
        var appender = attachAppender();
        try {
            var controller = new AdmissionController(options(AdaptiveConcurrencyMode.OBSERVE, 4, 0), clock,
                    AdmissionControlLaw.newBuilder(), noRegistryMetrics);

            // A real assignment delta freezes the target - a constraint with no arithmetic in it, so which one is
            // binding is not a function of how the law happened to read four synthetic windows.
            controller.onPartitionsAssigned(UniLists.of(new TopicPartition("topic", 0)));
            controller.onPartitionsAssigned(UniLists.of(new TopicPartition("topic", 1)));
            controller.tick();

            for (int window = 0; window < 4; window++) {
                feedWindowAndTick(controller, 10 * MS, controller.wouldBeTarget());
            }

            assertThat(controller.lastDecisionReason()).hasValue(COOLDOWN);

            var reports = constraintReports(appender);

            // Fired at all - with no registry this line IS the observability channel...
            assertThat(reports).hasSize(1);
            // ...and it names the constraint, the mode and the target, which is what makes it actionable.
            assertThat(reports.get(0)).contains(COOLDOWN.name());
            assertThat(reports.get(0)).contains("OBSERVE");
            assertThat(reports.get(0)).contains(String.valueOf(controller.wouldBeTarget()));
            // Four closed windows, one line: a steady state is reported once per rate-limit window, not per window.
        } finally {
            detachAppender(appender);
            noRegistryMetrics.close();
        }
    }

    @Test
    void aConstraintReachedThroughTheControlLawIsReportedToo() {
        // The other call site: the constraint the law itself named on a decided window, rather than the lifecycle
        // freeze above. Ceiling 4 with healthy saturated windows, so the cap binds (see the gauge test).
        var appender = attachAppender();
        try {
            var controller = instrumented(AdaptiveConcurrencyMode.ENFORCE, 4);

            for (int window = 0; window < 4; window++) {
                feedWindowAndTick(controller, 10 * MS, controller.currentTarget());
            }
            assertThat(controller.lastDecisionReason()).hasValue(AT_CAP);

            var reports = constraintReports(appender);
            assertThat(reports).hasSize(1);
            assertThat(reports.get(0)).contains("live target 4 slot(s)");
        } finally {
            detachAppender(appender);
        }
    }

    // --- helpers ---

    private ParallelConsumerOptions<?, ?> options(AdaptiveConcurrencyMode mode, int maxConcurrency, int seed) {
        return ParallelConsumerOptions.builder()
                .adaptiveConcurrencyMode(mode)
                .maxConcurrency(maxConcurrency)
                .adaptiveConcurrencyInitialTarget(seed)
                .build();
    }

    /** A controller whose meters are bound to this test's own registry. */
    private AdmissionController instrumented(AdaptiveConcurrencyMode mode, int maxConcurrency) {
        pcMetrics = new PCMetrics(registry, UniLists.of(), "admission-metrics-test");
        return new AdmissionController(options(mode, maxConcurrency, 0), clock, pcMetrics);
    }

    /**
     * Feeds one healthy, saturated window (every outcome a success, in-flight pinned at {@code inFlightMedian}) and
     * ticks past the time bound - {@link AdmissionControllerTest}'s pattern.
     */
    private void feedWindowAndTick(AdmissionController controller, long meanServiceTimeNanos, int inFlightMedian) {
        for (int i = 0; i < SAMPLES; i++) {
            controller.recordServiceTime(meanServiceTimeNanos);
            controller.recordInFlight(inFlightMedian);
            controller.recordOutcome(Outcome.SUCCESS);
        }
        clock.add(SAMPLE_WINDOW_DURATION);
        controller.tick();
    }

    private double gauge(PCMetricsDef def) {
        Gauge found = registry.find(def.getName()).gauge();
        assertThat(found).isNotNull();
        return found.value();
    }

    private double counter(PCMetricsDef def) {
        var found = registry.find(def.getName()).counter();
        assertThat(found).isNotNull();
        return found.count();
    }

    private List<String> admissionMeterNames() {
        return registry.getMeters().stream()
                .map(meter -> meter.getId().getName())
                .filter(name -> name.startsWith("pc.admission."))
                .collect(Collectors.toList());
    }

    private static Logger controllerLogger() {
        return (Logger) LoggerFactory.getLogger(AdmissionController.class);
    }

    private Level levelBeforeCapture;

    private ListAppender<ILoggingEvent> attachAppender() {
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        var logger = controllerLogger();
        levelBeforeCapture = logger.getLevel();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);
        return appender;
    }

    /** Restores the shared logger's level as well as detaching - PipelinePressureLoggingTest's pattern. */
    private void detachAppender(ListAppender<ILoggingEvent> appender) {
        var logger = controllerLogger();
        logger.detachAppender(appender);
        logger.setLevel(levelBeforeCapture);
        appender.stop();
    }

    /** The binding-constraint lines only - other INFO from the controller is not this test's business. */
    private static List<String> constraintReports(ListAppender<ILoggingEvent> appender) {
        return messagesAt(appender, Level.INFO).stream()
                .filter(message -> message.contains("admission target is being held"))
                .collect(Collectors.toList());
    }

    private static List<String> messagesAt(ListAppender<ILoggingEvent> appender, Level level) {
        return appender.list.stream()
                .filter(event -> event.getLevel() == level)
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }
}
