package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.chaostests.ProgressProbe;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

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
 * simply never trips anything. It is stopped in {@code afterTestExecution} - immediately after the
 * test method, BEFORE {@code @AfterEach} methods run (JUnit invokes {@code @AfterEach} METHODS
 * before {@code AfterEachCallback} extensions, so stopping any later would leave the sampler racing
 * the base class's {@code @AfterEach} admin-client close). If {@code @BeforeEach} throws, the test
 * method never executes and {@code afterTestExecution} is NOT invoked (verified against the pinned
 * junit-jupiter-engine: a {@code @BeforeEach} failure branches past the AfterTestExecutionCallback
 * phase) - {@code afterEach} remains as an idempotent safety-net stop for exactly that path. In that
 * path only, the safety-net stop runs after the base class's {@code @AfterEach} admin close, so the
 * sampler may issue one last call into a closing client - contained by the sample loop's catch and
 * {@code adminIfOpen()} returning empty once closed. {@link TestWatcher} callbacks then report
 * from the retained probe state (they run after all teardown).
 * <p>
 * Escape hatches: {@code -Dambient.probe=off} globally, {@link NoAmbientProbe} per class/method.
 */
@Slf4j
public class AmbientProbeExtension implements BeforeEachCallback, AfterTestExecutionCallback, AfterEachCallback, TestWatcher {

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
        // never let flight-recorder setup break a test - observer mode is best-effort
        try {
            Object instance = context.getRequiredTestInstance();
            if (!(instance instanceof BrokerIntegrationTest)) {
                return;
            }
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

    /**
     * Primary stop point: runs immediately after the test method, before {@code @AfterEach} methods
     * close the admin client - so the sampler never races a closing/closed client.
     */
    @Override
    public void afterTestExecution(ExtensionContext context) {
        stopProbe(context);
    }

    /**
     * Safety net: if {@code @BeforeEach} threw, the test method never ran and
     * {@link #afterTestExecution} was never invoked - but {@code AfterEachCallback}s still fire.
     * {@link ProgressProbe#stop()} is idempotent, so the common double-invocation is harmless.
     */
    @Override
    public void afterEach(ExtensionContext context) {
        stopProbe(context);
    }

    private static void stopProbe(ExtensionContext context) {
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

    /**
     * Public for unit testing only ({@code AmbientProbeExtensionTest} must live outside this package:
     * surefire excludes every class in an {@code integrationTest*} package from the unit suite).
     */
    public static boolean isDisabled(ExtensionContext context) {
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

    /** Public for unit testing only - see {@link #isDisabled(ExtensionContext)}. */
    public static String buildAutopsy(ExtensionContext context, ProgressProbe probe, Throwable cause) {
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
        appendEnvironment(sb);
        sb.append("=== END AMBIENT PROBE AUTOPSY ===");
        return sb.toString();
    }

    /**
     * Emitted on the first autopsy in each JVM, and pointed at thereafter rather than repeating a few
     * hundred lines per failing test. Note "each JVM", not each CI run: the integration lane forks
     * several JVMs, so a run can carry one dump per fork, each attached to whichever test failed
     * first there. That is the cost of not repeating it; the alternative buries the probe findings
     * the autopsy exists for.
     * <p>
     * This is what {@code JavaEnvTest} was doing by hand. Its javadoc said so - <em>"used to
     * manually inspect the java environment at runtime, particularly useful for CI
     * environments"</em> - and it was deleted in {@code cadf4c95} for asserting nothing, which was
     * true and beside the point: it was a diagnostic, not a test, and deleting it removed the tool
     * without automating what the tool was for. The autopsy is where a reader already looks when a
     * broker integration test fails, per {@code AGENTS.md}, so the information now arrives there
     * without anyone remembering to go and get it.
     */
    private static void appendEnvironment(StringBuilder sb) {
        if (!ENVIRONMENT_DUMPED.compareAndSet(false, true)) {
            sb.append("environment: dumped in this JVM's first autopsy\n");
            return;
        }
        sb.append("environment (once per JVM):\n");
        new TreeMap<>(System.getProperties().entrySet().stream()
                .collect(toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue()), (a, b) -> a)))
                .forEach((key, value) -> sb.append("  ").append(key).append('=')
                        .append(redact(key, value)).append('\n'));
    }

    /**
     * The autopsy prints straight to CI logs, which anyone with access to the Actions run can read, so a property
     * whose NAME reads like a credential has its value masked. Nothing in this repo passes a secret as {@code -D}
     * today - this stops a future one from being dumped rather than fixing a present leak.
     * <p>
     * Masking by name rather than filtering to an allowlist of known-interesting prefixes is deliberate: an
     * allowlist silently drops the next knob somebody adds, which is precisely when the dump is wanted, and it
     * would need hand-maintaining against a set the code cannot derive. The key is always printed, so the reader
     * still sees the property was set - only the value is withheld.
     */
    private static String redact(String key, String value) {
        String lower = key.toLowerCase(Locale.ROOT);
        for (String marker : SECRET_KEY_MARKERS) {
            if (lower.contains(marker)) {
                return "***";
            }
        }
        if (value != null && SECRET_IN_VALUE.matcher(value).find()) {
            return "*** (value matched a credential pattern)";
        }
        return value == null ? "null" : value.replace("\n", "\\n").replace("\r", "\\r");
    }

    private static final String[] SECRET_KEY_MARKERS =
            {"password", "passwd", "pwd", "passphrase", "secret", "token", "credential",
                    "apikey", "api.key", "accesskey", "access.key", "privatekey", "private.key"};

    /**
     * Matching the key name alone leaves the more likely leak wide open: people name a property for what it
     * configures, not for the fact that a credential happens to be inside it. A JDBC URL
     * ({@code ...?user=svc&password=hunter2}) or a URL with userinfo ({@code https://user:tok3n@host/x}) sails
     * past every marker above, because {@code test.db.url} contains none of them.
     * <p>
     * So the value is scanned too, for embedded {@code key=value} credential pairs and for URL userinfo. This
     * is a denylist and denylists fail open, which is the wrong direction for a masking control - but the
     * alternative here is an allowlist of interesting properties, which fails the diagnostic instead, and the
     * dump exists to be complete. Two overlapping checks narrow the gap without capping what can be reported.
     */
    private static final Pattern SECRET_IN_VALUE = Pattern.compile(
            "(?i)(password|passwd|passphrase|secret|token|credential|api[._-]?key)\\s*[=:]\\s*\\S"
                    + "|://[^/@\\s]+:[^/@\\s]+@");

    /**
     * Public for unit testing only - see {@link #isDisabled(ExtensionContext)}. Resets the
     * once-per-run guard between tests of {@link #buildAutopsy}; production code never calls it,
     * because the guard is the point.
     */
    public static void resetEnvironmentDumpForTest() {
        ENVIRONMENT_DUMPED.set(false);
    }

    private static final AtomicBoolean ENVIRONMENT_DUMPED = new AtomicBoolean();

    private static String describe(Throwable cause) {
        if (cause == null) {
            return "(no cause reported)";
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    /** Public for unit testing only - see {@link #isDisabled(ExtensionContext)}. */
    public static List<String> frozenPartitionLines(ProgressProbe probe) {
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
