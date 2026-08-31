package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.navigator.NavigatorDecisionReason;
import bz.stub.parallelconsumer.internal.navigator.ResourceAllocator;
import bz.stub.parallelconsumer.internal.navigator.ResourceContract;
import bz.stub.parallelconsumer.internal.navigator.StubResourceAllocator;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The navigator's resource-deferral attribution end to end (U4, R9): the real {@link ProcessingShard} refusal
 * branch, driving {@link WorkManager}'s selection machinery exactly as {@code NavigatorSelectionTest} does, but
 * asserting the OBSERVABILITY surface - the defer-moment log line, its once-per-episode dedup across many
 * passes, and the {@code pc.navigator.*} meters - rather than the claim mechanics those cover. The metrics
 * shape itself (per-resource tags, hand-assigned reason values) is unit-tested directly against
 * {@link bz.stub.parallelconsumer.internal.navigator.NavigatorParticipant} in
 * {@code NavigatorAttributionMetricsTest}; what only a real shard walk can prove is HERE - that the engine
 * actually calls the attribution site, once per episode, from the refusal branch.
 *
 * @see bz.stub.parallelconsumer.internal.navigator.NavigatorAttributionMetricsTest
 * @see NavigatorSelectionTest
 */
// Captures the shared ProcessingShard class logger - the same hazard AdmissionMetricsTest and
// PipelinePressureLoggingTest document for their own shared loggers.
@Isolated("captures a shared class logger")
class NavigatorAttributionTest {

    static final String TOPIC = "navigator-attribution-topic";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    static final String API_A = "api-a";
    static final String API_B = "api-b";
    static final String MEMBER = "navigator-attribution-member";

    static final Duration ONE_SECOND = Duration.ofSeconds(1);

    MutableClock clock;
    StubResourceAllocator allocator;
    SimpleMeterRegistry registry;
    PCModuleTestEnv module;
    WorkManager<String, String> wm;

    @AfterEach
    void closeRegistry() {
        if (registry != null) {
            registry.close();
        }
    }

    void setupTagged(List<ResourceContract> contracts, List<String> tags) {
        clock = MutableClock.epochUTC();
        allocator = new StubResourceAllocator(clock);
        for (ResourceContract contract : contracts) {
            allocator.register(contract);
        }
        registry = new SimpleMeterRegistry();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceTags(tags)
                .resourceAllocator(allocator)
                .meterRegistry(registry)
                .build();
        module = new PCModuleTestEnv(options, clock);
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
        allocator.join(MEMBER, clock.instant());
        clock.add(ONE_SECOND);
        allocator.readQuantum(MEMBER, clock.instant());
    }

    void register(int fromOffset, int count) {
        List<ConsumerRecord<String, String>> recs = new ArrayList<>(count);
        for (int i = fromOffset; i < fromOffset + count; i++) {
            recs.add(new ConsumerRecord<>(TOPIC, 0, i, "key-" + i, "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(TP, recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));
    }

    // -----------------------------------------------------------------------------------------------------
    // AE5 / R9: exactly one defer-moment log line, naming the resource and its next credit time
    // -----------------------------------------------------------------------------------------------------

    @Test
    void aDeferralEmitsExactlyOneDeferMomentLogLineNamingResourceAndNextCreditTime() {
        setupTagged(UniLists.of(new ResourceContract(API_A, 1.0, 1, ONE_SECOND)), UniLists.of(API_A));
        register(0, 2);
        var appender = attachAppender();

        try {
            var taken = wm.getWorkIfAvailable(2);

            assertWithMessage("one credit must admit exactly one of the two eligible records")
                    .that(taken).hasSize(1);
            var deferMoments = deferMomentLines(appender);
            assertWithMessage("exactly one defer-moment line for the one newly-deferred record")
                    .that(deferMoments).hasSize(1);
            assertThat(deferMoments.get(0)).contains(API_A);
            assertThat(deferMoments.get(0)).contains("next credit " + java.time.Instant.ofEpochSecond(2));
            assertThat(deferMoments.get(0)).contains(NavigatorDecisionReason.SINGLE_RESOURCE_BLOCKED.name());

            assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRED_RECORDS)).isEqualTo(1.0);
            assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRAL_REASON))
                    .isEqualTo((double) NavigatorDecisionReason.SINGLE_RESOURCE_BLOCKED.getValue());
        } finally {
            detachAppender(appender);
        }
    }

    // -----------------------------------------------------------------------------------------------------
    // Multi-resource: both names and both next-credit times present in the ONE line and in the metrics
    // -----------------------------------------------------------------------------------------------------

    @Test
    void multiResourceDeferralNamesBothResourcesAndBothNextCreditTimesInTheLineAndTheMetrics() {
        var apiB = new ResourceContract(API_B, 0.5, 1, Duration.ofSeconds(2));
        setupTagged(UniLists.of(new ResourceContract(API_A, 1.0, 1, ONE_SECOND), apiB), UniLists.of(API_A, API_B));
        // step to t=2s so the member is live for BOTH resources' current quanta, then drain both credits
        clock.add(ONE_SECOND);
        allocator.readQuantum(MEMBER, clock.instant());
        register(0, 2);
        var firstTaken = wm.getWorkIfAvailable(1);
        assertThat(firstTaken).hasSize(1);

        var appender = attachAppender();
        try {
            var taken = wm.getWorkIfAvailable(1);
            assertThat(taken).isEmpty(); // both resources drained - the second record must defer

            var deferMoments = deferMomentLines(appender);
            assertWithMessage("exactly one defer-moment line").that(deferMoments).hasSize(1);
            String line = deferMoments.get(0);
            assertThat(line).contains(API_A);
            assertThat(line).contains(API_B);
            assertThat(line).contains("next credit " + java.time.Instant.ofEpochSecond(3)); // api-a's next quantum
            assertThat(line).contains("next credit " + java.time.Instant.ofEpochSecond(4)); // api-b's next quantum
            assertThat(line).contains(NavigatorDecisionReason.MULTI_RESOURCE_BLOCKED.name());

            assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRAL_REASON))
                    .isEqualTo((double) NavigatorDecisionReason.MULTI_RESOURCE_BLOCKED.getValue());
        } finally {
            detachAppender(appender);
        }
    }

    // -----------------------------------------------------------------------------------------------------
    // Cadence: a deferral lasting many passes emits no per-pass spam (one log per EPISODE, not per pass)
    // -----------------------------------------------------------------------------------------------------

    @Test
    void aDeferralLastingManyPassesEmitsNoPerPassLogSpam() {
        setupTagged(UniLists.of(new ResourceContract(API_A, 1.0, 1, ONE_SECOND)), UniLists.of(API_A));
        register(0, 2);

        var appender = attachAppender();
        try {
            // the FIRST pass spends the one credit and starts the episode; clock held still, so every pass
            // after it re-observes the SAME still-open episode - the dedup case this test is about
            for (int pass = 0; pass < 30; pass++) {
                wm.getWorkIfAvailable(2);
            }

            assertWithMessage("30 passes over one deferral must produce exactly one defer-moment line - the "
                            + "transition, not the 29 re-observations of the same still-open episode")
                    .that(deferMomentLines(appender)).hasSize(1);
            assertWithMessage("the deferred-count gauge must not accumulate per pass - one record, one count")
                    .that(gauge(PCMetricsDef.NAVIGATOR_DEFERRED_RECORDS)).isEqualTo(1.0);
        } finally {
            detachAppender(appender);
        }
    }

    /**
     * The rate-limited steady-state line's OWN engagement, without depending on real wall-clock elapsed time
     * (its {@code RateLimiter} is wall-clock-based - no injected clock reaches it, per
     * {@code AdmissionController}'s own {@code constraintReportLimiter} precedent, so this test respects the
     * no-sleep discipline by asserting SUPPRESSION across many fast passes rather than waiting out the real
     * 5-second interval for a second firing). The appender attaches BEFORE the very first pass, so the
     * RateLimiter's own "always fires the first call" rule is captured exactly once - every pass after it,
     * all well inside the real-time interval, must be suppressed.
     */
    @Test
    void theSteadyStateLineIsRateLimitedNotFiredOnEveryPass() {
        setupTagged(UniLists.of(new ResourceContract(API_A, 1.0, 1, ONE_SECOND)), UniLists.of(API_A));
        register(0, 2);

        var appender = attachAppender();
        try {
            for (int pass = 0; pass < 11; pass++) {
                wm.getWorkIfAvailable(2);
            }
            var steadyStateLines = messagesAt(appender, Level.INFO).stream()
                    .filter(message -> message.contains("resource deferral continues"))
                    .collect(Collectors.toList());
            assertWithMessage("the FIRST pass's steady-state attempt always fires (RateLimiter's own first-call "
                            + "rule) but every pass after it, all well inside the 5s rate-limit window, must not")
                    .that(steadyStateLines).hasSize(1);
        } finally {
            detachAppender(appender);
        }
    }

    // -----------------------------------------------------------------------------------------------------
    // Dispatch closes the episode: the deferred-count gauge returns to zero
    // -----------------------------------------------------------------------------------------------------

    @Test
    void dispatchingTheDeferredRecordReturnsTheDeferredGaugeToZero() {
        setupTagged(UniLists.of(new ResourceContract(API_A, 1.0, 1, ONE_SECOND)), UniLists.of(API_A));
        register(0, 2);
        wm.getWorkIfAvailable(2);
        assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRED_RECORDS)).isEqualTo(1.0);

        clock.add(ONE_SECOND);
        allocator.readQuantum(MEMBER, clock.instant());
        var second = wm.getWorkIfAvailable(2);

        assertThat(second).hasSize(1);
        assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRED_RECORDS)).isEqualTo(0.0);
    }

    // -----------------------------------------------------------------------------------------------------
    // R3/AE4: the untagged instance registers no navigator meters and logs no attribution
    // -----------------------------------------------------------------------------------------------------

    @Test
    void untaggedInstanceRegistersNoNavigatorMetersAndLogsNoAttribution() {
        ResourceAllocator untouchedAllocator = Mockito.mock(ResourceAllocator.class);
        clock = MutableClock.epochUTC();
        registry = new SimpleMeterRegistry();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceAllocator(untouchedAllocator)
                .meterRegistry(registry)
                .build();
        module = new PCModuleTestEnv(options, clock);
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
        register(0, 3);

        var appender = attachAppender();
        try {
            assertThat(wm.getWorkIfAvailable(3)).hasSize(3);
            assertWithMessage("an untagged instance must register no pc.navigator.* meters (R3)")
                    .that(navigatorMeterNames()).isEmpty();
            assertWithMessage("an untagged instance must never log a navigator attribution line")
                    .that(deferMomentLines(appender)).isEmpty();
        } finally {
            detachAppender(appender);
        }
        Mockito.verifyNoInteractions(untouchedAllocator);
    }

    // --- helpers ---

    private double gauge(PCMetricsDef def) {
        Gauge found = registry.find(def.getName()).gauge();
        assertThat(found).isNotNull();
        return found.value();
    }

    private List<String> navigatorMeterNames() {
        return registry.getMeters().stream()
                .map(meter -> meter.getId().getName())
                .filter(name -> name.startsWith("pc.navigator."))
                .collect(Collectors.toList());
    }

    private static Logger shardLogger() {
        return (Logger) LoggerFactory.getLogger(ProcessingShard.class);
    }

    private Level levelBeforeCapture;

    private ListAppender<ILoggingEvent> attachAppender() {
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        var logger = shardLogger();
        levelBeforeCapture = logger.getLevel();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);
        return appender;
    }

    private void detachAppender(ListAppender<ILoggingEvent> appender) {
        var logger = shardLogger();
        logger.detachAppender(appender);
        logger.setLevel(levelBeforeCapture);
        appender.stop();
    }

    private static List<String> deferMomentLines(ListAppender<ILoggingEvent> appender) {
        return messagesAt(appender, Level.INFO).stream()
                .filter(message -> message.contains("entered resource deferral"))
                .collect(Collectors.toList());
    }

    private static List<String> messagesAt(ListAppender<ILoggingEvent> appender, Level level) {
        return appender.list.stream()
                .filter(event -> event.getLevel() == level)
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }
}
