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
import bz.stub.parallelconsumer.integrationTests.chaostests.ChaosSeed;
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
        assertThat(first).contains("environment (once per JVM):");
        assertThat(first).contains("java.version=");
        // a second failing test does not repeat a few hundred lines
        assertThat(second).contains("environment: dumped in this JVM's first autopsy");
        assertThat(second).doesNotContain("environment (once per JVM):");
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

    /**
     * The autopsy goes straight to CI logs, so a property whose name reads like a credential is masked - while the
     * key itself still prints, because knowing it was set is the diagnostic.
     */
    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    @SetSystemProperty(key = "ambient.probe.test.password", value = "hunter2")
    @SetSystemProperty(key = "ambient.probe.test.plain", value = "not-a-secret")
    void environmentDumpMasksValuesOfCredentialLookingKeys() {
        AmbientProbeExtension.resetEnvironmentDumpForTest();

        String autopsy = AmbientProbeExtension.buildAutopsy(
                contextFor(PlainFixture.class, "plainMethod"), observerProbe(), new AssertionError("x"));

        assertThat(autopsy).contains("ambient.probe.test.password=***");
        assertThat(autopsy).doesNotContain("hunter2");
        // an ordinary knob is untouched - the masking is by key name, not a blanket filter
        assertThat(autopsy).contains("ambient.probe.test.plain=not-a-secret");
    }

    /**
     * The likelier leak: people name a property for what it configures, so the credential rides in the VALUE
     * under a key that announces nothing. Matching key names alone would print these verbatim.
     */
    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    @SetSystemProperty(key = "ambient.probe.test.db.url", value = "jdbc:postgresql://h/db?user=svc&password=hunter2")
    @SetSystemProperty(key = "ambient.probe.test.hook", value = "https://user:tok3n@hooks.example.com/x")
    @SetSystemProperty(key = "ambient.probe.test.gpg.passphrase", value = "correct-horse")
    void environmentDumpMasksCredentialsCarriedInsideValues() {
        AmbientProbeExtension.resetEnvironmentDumpForTest();

        String autopsy = AmbientProbeExtension.buildAutopsy(
                contextFor(PlainFixture.class, "plainMethod"), observerProbe(), new AssertionError("x"));

        // embedded key=value credential, and URL userinfo - neither key contains a secret marker
        assertThat(autopsy).doesNotContain("hunter2");
        assertThat(autopsy).doesNotContain("tok3n");
        // gpg.passphrase is a real Maven release flag, and "passphrase" is not "password"
        assertThat(autopsy).doesNotContain("correct-horse");
        // the keys still print - knowing the property was set is the diagnostic
        assertThat(autopsy).contains("ambient.probe.test.db.url=");
        assertThat(autopsy).contains("ambient.probe.test.hook=");
    }

    /**
     * The masking must not eat the dump it protects. {@code java.library.path} is why {@code pat} was rejected
     * as a key marker - it would have masked every path property on the JVM.
     */
    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    void environmentDumpDoesNotOverMaskOrdinaryProperties() {
        AmbientProbeExtension.resetEnvironmentDumpForTest();

        String autopsy = AmbientProbeExtension.buildAutopsy(
                contextFor(PlainFixture.class, "plainMethod"), observerProbe(), new AssertionError("x"));

        assertThat(autopsy).contains("java.version=" + System.getProperty("java.version"));
        assertThat(autopsy).contains("java.library.path=" + System.getProperty("java.library.path"));
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

    // --- chaos seed replay handle ---

    /**
     * The seed is the only handle that replays a chaos failure, and the run-start console line carrying
     * it is precisely what a truncated CI log eats - so it has to be in the autopsy, which travels as
     * {@code system-out} inside the uploaded failsafe XML. Resolved here the way a scenario resolves it,
     * so this covers the real {@code -Dchaos.seed} path and not a hand-built value.
     */
    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    @SetSystemProperty(key = ChaosSeed.SEED_PROPERTY, value = "4734674029169027864")
    void autopsyCarriesTheChaosSeedAndItsReplayCommand() {
        ChaosSeed seed = ChaosSeed.resolve();
        assertThat(seed.getValue()).isEqualTo(4734674029169027864L);

        var context = contextFor(PlainFixture.class, "plainMethod");
        doReturn(Optional.of(new SeededFixture(seed))).when(context).getTestInstance();
        // the capture point: the live test instance is only guaranteed to be there this early
        new AmbientProbeExtension().afterTestExecution(context);

        String autopsy = AmbientProbeExtension.buildAutopsy(context, observerProbe(), new AssertionError("stalled"));

        assertThat(autopsy).contains("chaos seed: 4734674029169027864");
        assertThat(autopsy).contains("chaos replay: ./mvnw -Pci -pl parallel-consumer-core -am verify"
                + " -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups="
                + " -Dchaos.seed=4734674029169027864");
    }

    /** Every other broker IT is unseeded - it must not grow two lines of empty chaos boilerplate. */
    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    void autopsyOmitsTheSeedLinesForTestsThatHaveNone() {
        var context = contextFor(PlainFixture.class, "plainMethod");
        doReturn(Optional.of(new PlainFixture())).when(context).getTestInstance();
        new AmbientProbeExtension().afterTestExecution(context);

        String autopsy = AmbientProbeExtension.buildAutopsy(context, observerProbe(), new AssertionError("stalled"));

        assertThat(autopsy).doesNotContain("chaos seed:");
        assertThat(autopsy).doesNotContain("chaos replay:");
    }

    /** A scenario that failed before resolving a seed holds null - the capture must not publish it. */
    @Test
    @ResourceLock(ENVIRONMENT_DUMP_LOCK)
    void autopsyOmitsTheSeedLinesWhenTheScenarioNeverResolvedOne() {
        var context = contextFor(PlainFixture.class, "plainMethod");
        doReturn(Optional.of(new SeededFixture(null))).when(context).getTestInstance();
        new AmbientProbeExtension().afterTestExecution(context);

        String autopsy = AmbientProbeExtension.buildAutopsy(context, observerProbe(), new AssertionError("stalled"));

        assertThat(autopsy).doesNotContain("chaos seed:");
    }

    /** Unset means a fresh schedule every run - a resolve() that returned a constant would be silent. */
    @Test
    @ClearSystemProperty(key = ChaosSeed.SEED_PROPERTY)
    void chaosSeedIsRandomisedWhenTheReplayPropertyIsUnset() {
        assertThat(ChaosSeed.resolve().getValue()).isNotEqualTo(ChaosSeed.resolve().getValue());
    }

    /** Stands in for {@code ChaosScenarioBase}, which cannot be loaded here - it extends
     * {@code BrokerIntegrationTest}, whose static initialiser starts a Kafka Testcontainer. */
    static class SeededFixture implements ChaosSeed.Holder {
        private final ChaosSeed seed;

        SeededFixture(ChaosSeed seed) {
            this.seed = seed;
        }

        @Override
        public ChaosSeed getChaosSeed() {
            return seed;
        }
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
        // one real store per context: the extension hands state from its callbacks to buildAutopsy
        // through it, so a null (unstubbed) store would only ever exercise half the path
        var store = new StubStore();
        doReturn(store).when(context).getStore(any(ExtensionContext.Namespace.class));
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
