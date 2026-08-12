package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import bz.stub.parallelconsumer.integrationTests.AmbientProbeExtension;
import bz.stub.parallelconsumer.integrationTests.NoAmbientProbe;
import bz.stub.parallelconsumer.integrationTests.chaostests.ProgressProbe;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link AmbientProbeExtension} flight recorder and the observer-mode contract of
 * {@link ProgressProbe} - no broker, no Testcontainers, pure surefire.
 * <p>
 * Deliberately NOT in the extension's own package: surefire excludes every class in an
 * {@code integrationTest*} package from the unit suite (failsafe owns those), so a test placed next
 * to the extension would only ever run with the Docker-bound ITs. The extension exposes its pure
 * helpers publicly (marked visible-for-testing) for exactly this cross-package coverage.
 * <p>
 * {@code BrokerIntegrationTest} itself never appears here: merely initialising that class starts a
 * Kafka Testcontainer (static initialiser), so the happy path (probe stored) and the
 * {@code getKcu()}-throwing path are exercised via the equivalent in-try failure seam
 * ({@code getRequiredTestInstance()} throwing) and by the ITs themselves.
 */
class AmbientProbeExtensionTest {

    /**
     * Every {@link AmbientProbeExtension#buildAutopsy} call mutates the same static once-per-JVM
     * environment-dump guard, whether or not the test cares about the dump - the first caller wins it
     * and every later one gets the "already dumped" line. This module runs JUnit thread-parallel
     * outside {@code -Pci}, so any two of them that are not serialised can red each other. Hold this
     * lock on every test that calls {@code buildAutopsy}, not just the two asserting on the dump.
     */
    private static final String ENVIRONMENT_DUMP_LOCK = "ambient-probe-environment-dump";

    // --- fixtures for the isDisabled() matrix ---

    static class PlainFixture {
        void plainMethod() {
        }
    }

    @NoAmbientProbe
    static class OptedOutClassFixture {
        void anyMethod() {
        }
    }

    /** No own annotation - must be covered by the superclass's, since {@link NoAmbientProbe} is {@code @Inherited}. */
    static class InheritsOptOutFixture extends OptedOutClassFixture {
    }

    static class MethodOptOutFixture {
        @NoAmbientProbe
        void optedOutMethod() {
        }

        void normalMethod() {
        }
    }

    @Test
    @ClearSystemProperty(key = AmbientProbeExtension.DISABLE_PROPERTY)
    void enabledByDefault() {
        assertThat(AmbientProbeExtension.isDisabled(contextFor(PlainFixture.class, "plainMethod"))).isFalse();
        assertThat(AmbientProbeExtension.isDisabled(contextFor(MethodOptOutFixture.class, "normalMethod"))).isFalse();
    }

    @Test
    @SetSystemProperty(key = AmbientProbeExtension.DISABLE_PROPERTY, value = "off")
    void disabledGloballyBySystemProperty() {
        assertThat(AmbientProbeExtension.isDisabled(contextFor(PlainFixture.class, "plainMethod"))).isTrue();
    }

    @Test
    @SetSystemProperty(key = AmbientProbeExtension.DISABLE_PROPERTY, value = "on")
    void otherPropertyValuesDoNotDisable() {
        assertThat(AmbientProbeExtension.isDisabled(contextFor(PlainFixture.class, "plainMethod"))).isFalse();
    }

    @Test
    void disabledByClassLevelAnnotation() {
        assertThat(AmbientProbeExtension.isDisabled(contextFor(OptedOutClassFixture.class, "anyMethod"))).isTrue();
    }

    @Test
    void disabledByInheritedClassLevelAnnotation() {
        assertThat(AmbientProbeExtension.isDisabled(contextFor(InheritsOptOutFixture.class, null))).isTrue();
    }

    @Test
    void disabledByMethodLevelAnnotation() {
        assertThat(AmbientProbeExtension.isDisabled(contextFor(MethodOptOutFixture.class, "optedOutMethod"))).isTrue();
    }

    // --- environment dump (what JavaEnvTest used to do by hand) ---

    /** Asserts on the guard itself, so it resets it first - see {@link #ENVIRONMENT_DUMP_LOCK}. */
    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    void autopsyCarriesTheEnvironmentDumpOncePerRun() {
        AmbientProbeExtension.resetEnvironmentDumpForTest();
        var probe = observerProbe();
        var context = contextFor(PlainFixture.class, "plainMethod");

        String first = AmbientProbeExtension.buildAutopsy(context, probe, new AssertionError("first failure"));
        String second = AmbientProbeExtension.buildAutopsy(context, probe, new AssertionError("second failure"));

        // the first autopsy of a run carries the dump
        assertThat(first).contains("environment (once per run):");
        assertThat(first).contains("java.version=");
        // a second failing test does not repeat a few hundred lines
        assertThat(second).contains("environment: dumped in this run's first autopsy");
        assertThat(second).doesNotContain("environment (once per run):");
    }

    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    @SetSystemProperty(key = "ambient.probe.test.multiline", value = "alpha\nbeta")
    void environmentDumpEscapesNewlinesSoOneLinePerProperty() {
        AmbientProbeExtension.resetEnvironmentDumpForTest();

        String autopsy = AmbientProbeExtension.buildAutopsy(
                contextFor(PlainFixture.class, "plainMethod"), observerProbe(), new AssertionError("x"));

        assertThat(autopsy).contains("ambient.probe.test.multiline=alpha\\nbeta");
    }

    // --- autopsy rendering ---

    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    void autopsyReportsProbeCleanWhenNothingObserved() {
        var probe = observerProbe();

        String autopsy = AmbientProbeExtension.buildAutopsy(
                contextFor(PlainFixture.class, "plainMethod"), probe, new AssertionError("await timed out"));

        assertThat(autopsy).contains("=== AMBIENT PROBE AUTOPSY (test failed):");
        assertThat(autopsy).contains("failure: AssertionError: await timed out");
        assertThat(autopsy).contains("probe clean - no rebalance dwell, no lag stagnation, no frozen partitions observed");
        assertThat(autopsy).contains("=== END AMBIENT PROBE AUTOPSY ===");
    }

    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    void autopsyListsViolationsWithCount() {
        var probe = observerProbe();
        probe.getViolations().add("ZOMBIE_MEMBER/REBALANCE_BLOCKED: synthetic dwell violation");
        probe.getViolations().add("CLASS2_STALL/LAG_STAGNATION: synthetic stall violation");

        String autopsy = AmbientProbeExtension.buildAutopsy(
                contextFor(PlainFixture.class, "plainMethod"), probe, new RuntimeException("boom"));

        assertThat(autopsy).contains("violations (2):");
        assertThat(autopsy).contains("- ZOMBIE_MEMBER/REBALANCE_BLOCKED: synthetic dwell violation");
        assertThat(autopsy).contains("- CLASS2_STALL/LAG_STAGNATION: synthetic stall violation");
        assertThat(autopsy).doesNotContain("probe clean");
    }

    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    void autopsyShowsFrozenPartitionDetailWhenNoViolationCrossedBounds() {
        var probe = observerProbe();
        var tp = new TopicPartition("in-topic", 12);
        // frozen: real lag, committed offset stagnant for 2 minutes - observed but below violation bounds
        probe.getPartitionLagSnapshots().put(tp,
                new ProgressProbe.PartitionLagSnapshot(tp, 100, 2200, 2100, Instant.now().minusSeconds(120)));

        String autopsy = AmbientProbeExtension.buildAutopsy(
                contextFor(PlainFixture.class, "plainMethod"), probe, null);

        assertThat(autopsy).contains("(none crossed the chaos-calibrated bounds");
        assertThat(autopsy).contains("in-topic-12: committed=100 end=2200 lag=2100 stagnant=");
        assertThat(autopsy).contains("failure: (no cause reported)");
        assertThat(autopsy).doesNotContain("probe clean");
    }

    @Test
    void frozenPartitionLinesFilterOutHealthyPartitions() {
        var probe = observerProbe();
        var caughtUp = new TopicPartition("topic", 0);
        var freshlyCommitted = new TopicPartition("topic", 1);
        // no lag - long stagnation is irrelevant (nothing left to consume)
        probe.getPartitionLagSnapshots().put(caughtUp,
                new ProgressProbe.PartitionLagSnapshot(caughtUp, 500, 500, 0, Instant.now().minusSeconds(300)));
        // lag, but committed offset moved just now - not frozen
        probe.getPartitionLagSnapshots().put(freshlyCommitted,
                new ProgressProbe.PartitionLagSnapshot(freshlyCommitted, 100, 400, 300, Instant.now()));

        assertThat(AmbientProbeExtension.frozenPartitionLines(probe)).isEmpty();
    }

    // --- beforeEach fallback paths + callback no-ops ---

    @Test
    void nonBrokerTestInstanceIsIgnoredAndAllCallbacksNoOp() {
        var extension = new AmbientProbeExtension();
        var store = new StubStore();
        ExtensionContext context = contextFor(PlainFixture.class, "plainMethod");
        doReturn(store).when(context).getStore(any(ExtensionContext.Namespace.class));
        doReturn(new PlainFixture()).when(context).getRequiredTestInstance();

        extension.beforeEach(context);

        assertThat(store.backing).isEmpty(); // not a BrokerIntegrationTest - no probe stored

        // with no probe stored, every later callback must be a silent no-op
        extension.afterTestExecution(context);
        extension.afterEach(context);
        extension.testFailed(context, new RuntimeException("test blew up"));
        extension.testSuccessful(context);
    }

    @Test
    void probeSetupFailureIsCaughtAndTestProceedsUnobserved() {
        var extension = new AmbientProbeExtension();
        var store = new StubStore();
        ExtensionContext context = contextFor(PlainFixture.class, "plainMethod");
        doReturn(store).when(context).getStore(any(ExtensionContext.Namespace.class));
        // the whole instance/kcu resolution runs inside the best-effort try - any failure there
        // (this seam, or getKcu() throwing) must be swallowed and leave the test unobserved
        doThrow(new IllegalStateException("no test instance")).when(context).getRequiredTestInstance();

        extension.beforeEach(context); // must not throw

        assertThat(store.backing).isEmpty();
        extension.afterTestExecution(context);
        extension.afterEach(context);
    }

    // --- FIX 1 / FIX 3 behavior pinning ---

    @Test
    void progressProbeStopIsIdempotent() {
        var probe = observerProbe();
        // stop before start: no sampler thread yet
        assertThat(probe.stop()).isEmpty();

        probe.start();
        assertThat(probe.stop()).isEmpty();
        // double stop on a dead sampler - the afterTestExecution + afterEach safety-net path
        assertThat(probe.stop()).isEmpty();
    }

    @Test
    void observerModeRecordsViolationsButNeverLogsError() throws Exception {
        var observer = observerProbe();
        var chaos = new ProgressProbe(mock(KafkaClientUtils.class), "group", "topic", () -> 0L, 100);
        assertThat(observer.isObserverMode()).isTrue();
        assertThat(chaos.isObserverMode()).isFalse();

        var probeLogger = (Logger) LoggerFactory.getLogger(ProgressProbe.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        probeLogger.addAppender(appender);
        try {
            violate(observer, "AMBIENT_SYNTHETIC: observer violation");
            violate(chaos, "CHAOS_SYNTHETIC: chaos violation");
        } finally {
            probeLogger.detachAppender(appender);
        }

        // violations are recorded identically in both modes...
        assertThat(observer.getViolations()).containsExactly("AMBIENT_SYNTHETIC: observer violation");
        assertThat(chaos.getViolations()).containsExactly("CHAOS_SYNTHETIC: chaos violation");

        // ...but only chaos mode reports at ERROR - the ambient observer is silent on a green test
        // (its violations surface through the failure-time autopsy instead)
        List<ILoggingEvent> errors = appender.list.stream()
                .filter(event -> event.getLevel() == Level.ERROR)
                .collect(Collectors.toList());
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0).getFormattedMessage()).contains("CHAOS_SYNTHETIC");
    }

    // --- helpers ---

    private static ProgressProbe observerProbe() {
        // mocked kcu: adminIfOpen() defaults to Optional.empty() (Mockito), so a started sampler skips silently
        return ProgressProbe.ambientObserver(mock(KafkaClientUtils.class), () -> "unit-test-group");
    }

    /**
     * {@code violate} stays private (nothing outside the probe should inject violations); the real
     * trigger paths need a broker plus minutes of stall, so this pins the recording/logging contract
     * reflectively instead.
     */
    private static void violate(ProgressProbe probe, String message) throws Exception {
        Method violate = ProgressProbe.class.getDeclaredMethod("violate", String.class);
        violate.setAccessible(true);
        violate.invoke(probe, message);
    }

    private static ExtensionContext contextFor(Class<?> testClass, String methodName) {
        ExtensionContext context = mock(ExtensionContext.class);
        doReturn(testClass).when(context).getRequiredTestClass();
        Optional<Method> method = Optional.ofNullable(methodName).map(name -> {
            try {
                return testClass.getDeclaredMethod(name);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("fixture method missing: " + name, e);
            }
        });
        when(context.getTestMethod()).thenReturn(method);
        when(context.getDisplayName()).thenReturn("mockedTest()");
        return context;
    }

    /** Minimal real in-memory {@link ExtensionContext.Store} so put/get semantics are genuine, not stubbed. */
    static class StubStore implements ExtensionContext.Store {
        final Map<Object, Object> backing = new HashMap<>();

        @Override
        public Object get(Object key) {
            return backing.get(key);
        }

        @Override
        public <V> V get(Object key, Class<V> requiredType) {
            return requiredType.cast(backing.get(key));
        }

        @Override
        public <K, V> Object getOrComputeIfAbsent(K key, Function<K, V> defaultCreator) {
            return backing.computeIfAbsent(key, ignored -> defaultCreator.apply(key));
        }

        @Override
        public <K, V> V getOrComputeIfAbsent(K key, Function<K, V> defaultCreator, Class<V> requiredType) {
            return requiredType.cast(getOrComputeIfAbsent(key, defaultCreator));
        }

        @Override
        public void put(Object key, Object value) {
            backing.put(key, value);
        }

        @Override
        public Object remove(Object key) {
            return backing.remove(key);
        }

        @Override
        public <V> V remove(Object key, Class<V> requiredType) {
            return requiredType.cast(backing.remove(key));
        }
    }
}
