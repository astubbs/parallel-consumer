package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.integrationTests.chaostests.ProgressProbe;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Ambient "flight recorder" for every broker integration test: runs {@link ProgressProbe} in
 * observer mode in the background so that when a test fails or times out, the generic Awaitility
 * timeout comes with a diagnosis - an autopsy block of probe violations, peak signatures and
 * per-partition frozen-committed detail ("zombie member blocked rebalance 40s" vs "partition 12
 * committed frozen lag=2100" vs "probe clean - fault is in the test").
 * <p>
 * Observer semantics: this extension NEVER fails a test. Violations use the chaos-calibrated
 * thresholds but only ever gate inside the chaos suite, which constructs its own gating probe.
 * <p>
 * Lifecycle: registered on {@link BrokerIntegrationTest} so every broker IT inherits it. The sampler
 * (one daemon thread per test, 1s group-state cadence, offsets every 5th tick) starts in
 * {@code beforeEach} - before the base class's {@code @BeforeEach} opens the clients - so
 * {@link ProgressProbe} skips samples until the admin client exists, and a group that never forms
 * simply never trips anything. It is always stopped in {@code afterEach}; {@link TestWatcher}
 * callbacks then report from the retained probe state (they run after {@code afterEach}).
 * <p>
 * Escape hatches: {@code -Dambient.probe=off} globally, {@link NoAmbientProbe} per class/method.
 */
@Slf4j
public class AmbientProbeExtension implements BeforeEachCallback, AfterEachCallback, TestWatcher {

    /** Set {@code -Dambient.probe=off} to disable the flight recorder globally. */
    public static final String DISABLE_PROPERTY = "ambient.probe";
    private static final String DISABLE_VALUE = "off";

    /**
     * Frozen-partition autopsy detail: report partitions whose committed offset has not moved for at
     * least this long while lag remains. Deliberately far below
     * {@link ProgressProbe#LAG_STAGNATION_BOUND} - at autopsy time ANY frozen backlog is diagnostic
     * signal, not a verdict.
     */
    static final Duration FROZEN_REPORT_MIN_STAGNATION = Duration.ofSeconds(10);
    static final long FROZEN_REPORT_MIN_LAG = 1;

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(AmbientProbeExtension.class);
    private static final String PROBE_KEY = "ambientProbe";

    @Override
    public void beforeEach(ExtensionContext context) {
        if (isDisabled(context)) {
            log.debug("[ambient-probe] disabled for {}", context.getDisplayName());
            return;
        }
        Object instance = context.getRequiredTestInstance();
        if (!(instance instanceof BrokerIntegrationTest)) {
            return;
        }
        // never let flight-recorder setup break a test - observer mode is best-effort
        try {
            KafkaClientUtils kcu = ((BrokerIntegrationTest<?, ?>) instance).getKcu();
            if (kcu == null) {
                return;
            }
            // group id is read per sample, so tests that switch to a NEW_GROUP stay watched
            ProgressProbe probe = ProgressProbe.ambientObserver(kcu, kcu::getGroupId);
            probe.start();
            context.getStore(NAMESPACE).put(PROBE_KEY, probe);
        } catch (Exception e) {
            log.debug("[ambient-probe] could not start (test proceeds unobserved): {}", e.getMessage());
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        ProgressProbe probe = probeOf(context);
        if (probe == null) {
            return;
        }
        try {
            probe.stop(); // always stop the sampler, pass or fail; state is retained for TestWatcher
        } catch (Exception e) {
            log.debug("[ambient-probe] stop error (ignored): {}", e.getMessage());
        }
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        ProgressProbe probe = probeOf(context);
        if (probe == null) {
            return;
        }
        log.error("{}", buildAutopsy(context, probe, cause));
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        ProgressProbe probe = probeOf(context);
        if (probe == null) {
            return;
        }
        log.debug("[ambient-probe] clean pass '{}': peaks rebalanceDwell={}ms lagStagnation={}ms violations={}",
                context.getDisplayName(), probe.getPeakRebalanceDwellMs(), probe.getPeakLagStagnationMs(),
                probe.getViolations().size());
    }

    // testAborted / testDisabled: TestWatcher's no-op defaults are deliberate - the observer stays silent

    private static boolean isDisabled(ExtensionContext context) {
        if (DISABLE_VALUE.equalsIgnoreCase(System.getProperty(DISABLE_PROPERTY))) {
            return true;
        }
        // @Inherited covers annotated superclasses; then check the individual method
        if (context.getRequiredTestClass().isAnnotationPresent(NoAmbientProbe.class)) {
            return true;
        }
        return context.getTestMethod()
                .map(method -> method.isAnnotationPresent(NoAmbientProbe.class))
                .orElse(false);
    }

    private static ProgressProbe probeOf(ExtensionContext context) {
        return context.getStore(NAMESPACE).get(PROBE_KEY, ProgressProbe.class);
    }

    private static String buildAutopsy(ExtensionContext context, ProgressProbe probe, Throwable cause) {
        List<String> violations = new ArrayList<>(probe.getViolations());
        List<String> frozen = frozenPartitionLines(probe);

        var sb = new StringBuilder(512);
        sb.append("\n=== AMBIENT PROBE AUTOPSY (test failed): ").append(context.getDisplayName()).append(" ===\n");
        sb.append("failure: ").append(describe(cause)).append('\n');
        boolean nothingObserved = violations.isEmpty() && frozen.isEmpty()
                && probe.getPeakRebalanceDwellMs() == 0 && probe.getPeakLagStagnationMs() == 0;
        if (nothingObserved) {
            sb.append("probe clean - no rebalance dwell, no lag stagnation, no frozen partitions observed: ")
                    .append("the fault is likely in the test itself, not consumer-group progress\n");
        } else {
            sb.append("violations (").append(violations.size()).append("):\n");
            if (violations.isEmpty()) {
                sb.append("  (none crossed the chaos-calibrated bounds - see peaks/frozen detail below)\n");
            }
            for (String violation : violations) {
                sb.append("  - ").append(violation).append('\n');
            }
            sb.append("peaks: rebalanceDwell=").append(probe.getPeakRebalanceDwellMs())
                    .append("ms lagStagnation=").append(probe.getPeakLagStagnationMs()).append("ms\n");
            sb.append("frozen partitions (committed stagnant >= ").append(FROZEN_REPORT_MIN_STAGNATION.getSeconds())
                    .append("s with lag >= ").append(FROZEN_REPORT_MIN_LAG).append("):\n");
            if (frozen.isEmpty()) {
                sb.append("  (none)\n");
            }
            for (String line : frozen) {
                sb.append("  - ").append(line).append('\n');
            }
        }
        sb.append("=== END AMBIENT PROBE AUTOPSY ===");
        return sb.toString();
    }

    private static String describe(Throwable cause) {
        if (cause == null) {
            return "(no cause reported)";
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    private static List<String> frozenPartitionLines(ProgressProbe probe) {
        return probe.getPartitionLagSnapshots().values().stream()
                .filter(snapshot -> snapshot.getLag() >= FROZEN_REPORT_MIN_LAG)
                .filter(snapshot -> snapshot.stagnantSeconds() >= FROZEN_REPORT_MIN_STAGNATION.getSeconds())
                .sorted(Comparator.comparing(snapshot -> snapshot.getTopicPartition().toString()))
                .map(snapshot -> snapshot.getTopicPartition()
                        + ": committed=" + snapshot.getCommitted()
                        + " end=" + snapshot.getEndOffset()
                        + " lag=" + snapshot.getLag()
                        + " stagnant=" + snapshot.stagnantSeconds() + "s")
                .collect(Collectors.toList());
    }
}
