package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.state.ShardKey;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;

/**
 * The navigator's {@code pc.navigator.*} meters (U4): the deferred-count and latest-reason gauges, one episode
 * counter per {@link NavigatorDecisionReason} (hand-assigned values), and the per-tagged-resource
 * spent/overdraft/next-credit gauges read live from the allocator's {@link ConservationLedger} - mirrors
 * {@code AdmissionMetricsTest}'s structure for the sibling subsystem. Drives {@link NavigatorParticipant}
 * directly (bypassing {@code PCModule}) since {@link NavigatorParticipant#initMetrics} is the whole registration
 * surface; the log-line and real-engine dedup half lives in
 * {@code bz.stub.parallelconsumer.state.NavigatorAttributionTest}.
 */
class NavigatorAttributionMetricsTest {

    private static final String API_A = "api-a";
    private static final String API_B = "api-b";
    private static final String MEMBER = "attribution-metrics-member";

    private final MutableClock clock = MutableClock.epochUTC();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private PCMetrics pcMetrics;

    /** Any stable shard key - the episode transitions now carry one for U5's per-shard breakdown. */
    private static ShardKey someShardKey() {
        return ShardKey.ofTopicPartition(
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("metrics-test-topic", 0, 0, "k", "v"));
    }

    @AfterEach
    void closeRegistry() {
        registry.close();
    }

    // ------------------------------------------------------------------
    // R3: the inert shape registers nothing
    // ------------------------------------------------------------------

    @Test
    void inertParticipantRegistersNoMeters() {
        pcMetrics = new PCMetrics(registry, UniLists.of(), "navigator-metrics-test");

        NavigatorParticipant.inert().initMetrics(pcMetrics, clock);

        assertThat(navigatorMeterNames()).isEmpty();
    }

    @Test
    void nullPcMetricsIsANoOp() {
        var allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(API_A, 1.0, 1, Duration.ofSeconds(1)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(API_A), MEMBER);

        // must not throw
        participant.initMetrics(null, clock);
    }

    // ------------------------------------------------------------------
    // The deferred-count and latest-reason gauges
    // ------------------------------------------------------------------

    @Test
    void beforeAnyEpisodeTheGaugesReadZeroAndNoDecisionYet() {
        var participant = instrumented(API_A);

        assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRED_RECORDS)).isEqualTo(0.0);
        assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRAL_REASON))
                .isEqualTo((double) NavigatorDecisionReason.NO_DEFERRAL_VALUE);
    }

    @Test
    void anEpisodeStartingBumpsTheDeferredGaugeAndPublishesItsReason() {
        var participant = instrumented(API_A);
        var deferral = new ResourceDeferral(API_A, java.util.Optional.empty());
        var decision = NavigatorDecision.of(UniLists.of(deferral), false);

        participant.onDeferralEpisodeStarted(decision, someShardKey());

        assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRED_RECORDS)).isEqualTo(1.0);
        assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRAL_REASON))
                .isEqualTo((double) NavigatorDecisionReason.SINGLE_RESOURCE_BLOCKED.getValue());
        assertThat(episodeCounter(NavigatorDecisionReason.SINGLE_RESOURCE_BLOCKED)).isEqualTo(1.0);
        // the OTHER reasons' counters must not move
        assertThat(episodeCounter(NavigatorDecisionReason.MULTI_RESOURCE_BLOCKED)).isEqualTo(0.0);
        assertThat(episodeCounter(NavigatorDecisionReason.RESOURCE_AND_SLOTS_BLOCKED)).isEqualTo(0.0);
    }

    @Test
    void anEpisodeEndingDecrementsTheDeferredGauge() {
        var participant = instrumented(API_A);
        var decision = NavigatorDecision.of(UniLists.of(new ResourceDeferral(API_A, java.util.Optional.empty())), false);

        participant.onDeferralEpisodeStarted(decision, someShardKey());
        participant.onDeferralEpisodeEnded(someShardKey());

        assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRED_RECORDS)).isEqualTo(0.0);
        // the reason gauge is "most recent", so it is NOT reset by the episode ending
        assertThat(gauge(PCMetricsDef.NAVIGATOR_DEFERRAL_REASON))
                .isEqualTo((double) NavigatorDecisionReason.SINGLE_RESOURCE_BLOCKED.getValue());
    }

    // ------------------------------------------------------------------
    // KTD2 visibility: spent/overdraft/next-credit read live from the ledger, tagged by resource
    // ------------------------------------------------------------------

    @Test
    void spentAndOverdraftGaugesReadLiveFromTheConservationLedgerPerResource() {
        var allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(API_A, 1.0, 1, Duration.ofSeconds(1)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(API_A), MEMBER);
        pcMetrics = new PCMetrics(registry, UniLists.of(), "navigator-metrics-test");
        participant.initMetrics(pcMetrics, clock);
        allocator.join(MEMBER, clock.instant());
        clock.add(Duration.ofSeconds(1));

        // no quantum ever pulled: a spend here has nothing to draw from and lands as OVERDRAFT (KD10)
        participant.spendOneCreditPerTag(clock.instant());

        assertWithResourceTag(PCMetricsDef.NAVIGATOR_CREDITS_SPENT, API_A, 1.0);
        assertWithMessageOverdraft(1.0);
    }

    @Test
    void nextCreditAtGaugeReportsTheResourcesNextQuantumBoundary() {
        var allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(API_A, 1.0, 1, Duration.ofSeconds(1)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(API_A), MEMBER);
        pcMetrics = new PCMetrics(registry, UniLists.of(), "navigator-metrics-test");
        participant.initMetrics(pcMetrics, clock);

        assertWithResourceTag(PCMetricsDef.NAVIGATOR_NEXT_CREDIT_AT, API_A, 1.0); // next 1s-quantum boundary
    }

    /** Two tagged resources register TWO of each per-resource gauge, tagged by their own resource name. */
    @Test
    void multipleTaggedResourcesEachGetTheirOwnTaggedGaugeSet() {
        var allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(API_A, 1.0, 1, Duration.ofSeconds(1)));
        allocator.register(new ResourceContract(API_B, 0.5, 1, Duration.ofSeconds(2)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(API_A, API_B), MEMBER);
        pcMetrics = new PCMetrics(registry, UniLists.of(), "navigator-metrics-test");
        participant.initMetrics(pcMetrics, clock);

        assertThat(registry.find(PCMetricsDef.NAVIGATOR_CREDITS_SPENT.getName()).gauges()).hasSize(2);
        assertWithResourceTag(PCMetricsDef.NAVIGATOR_CREDITS_SPENT, API_A, 0.0);
        assertWithResourceTag(PCMetricsDef.NAVIGATOR_CREDITS_SPENT, API_B, 0.0);
    }

    // --- helpers ---

    private NavigatorParticipant instrumented(String resourceName) {
        var allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(resourceName, 1.0, 1, Duration.ofSeconds(1)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(resourceName), MEMBER);
        pcMetrics = new PCMetrics(registry, UniLists.of(), "navigator-metrics-test");
        participant.initMetrics(pcMetrics, clock);
        return participant;
    }

    private double gauge(PCMetricsDef def) {
        Gauge found = registry.find(def.getName()).gauge();
        assertThat(found).isNotNull();
        return found.value();
    }

    private double episodeCounter(NavigatorDecisionReason reason) {
        var found = registry.find(PCMetricsDef.NAVIGATOR_DEFERRAL_EPISODES.getName())
                .tag("reason", reason.name()).counter();
        assertThat(found).isNotNull();
        return found.count();
    }

    private void assertWithResourceTag(PCMetricsDef def, String resourceName, double expected) {
        Gauge found = registry.find(def.getName()).tag("resource", resourceName).gauge();
        assertThat(found).isNotNull();
        assertThat(found.value()).isEqualTo(expected);
    }

    private void assertWithMessageOverdraft(double expected) {
        assertWithResourceTag(PCMetricsDef.NAVIGATOR_CREDITS_OVERDRAFT, API_A, expected);
    }

    private List<String> navigatorMeterNames() {
        return registry.getMeters().stream()
                .map(meter -> meter.getId().getName())
                .filter(name -> name.startsWith("pc.navigator."))
                .collect(Collectors.toList());
    }
}
