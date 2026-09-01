package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.navigator.ConservationLedger;
import bz.stub.parallelconsumer.internal.navigator.NavigatorParticipant;
import bz.stub.parallelconsumer.navigator.NavigatorView;
import bz.stub.parallelconsumer.navigator.ResourceContract;
import bz.stub.parallelconsumer.navigator.ResourceDeferral;
import bz.stub.parallelconsumer.navigator.StubResourceAllocator;
import bz.stub.parallelconsumer.state.ProcessingShard;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.REUSE_GROUP;
import static bz.stub.parallelconsumer.metrics.PCMetricsDef.NAVIGATOR_CREDITS_SPENT;
import static bz.stub.parallelconsumer.metrics.PCMetricsDef.NAVIGATOR_DEFERRAL_EPISODES;
import static bz.stub.parallelconsumer.metrics.PCMetricsDef.PC_INSTANCE_TAG;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.awaitility.Awaitility.await;

/**
 * The navigator micro-MVP's honest observable moment (U6): the ONE wall-clock lane, extending the
 * {@code MultiInstanceMetricsTest} construction pattern. Two PC instances tagged with the shared resource
 * {@value #RESOURCE} split its 2-credits/second policy at ~1Hz each (AE1); an untagged bystander in the same
 * consumer group drains its share of the backlog unthrottled and records zero navigator attributions (AE4);
 * closing one tagged instance converges the survivor toward the full 2Hz (AE2); and at least one deferral is
 * observed with its attribution log line and metric (AE5 smoke - the strict not-invoked-before-availableAt
 * assertion lives in the virtual-clock lane, {@code NavigatorSelectionPath}-family unit tests).
 * <p>
 * <b>Measurement discipline (KTD8, R13).</b> Firing is measured by the test's own hook around the user
 * function - a timestamped entry per invocation - never by PC's own metrics, and every assertion is a COUNT
 * over a window anchored at a firing (trigger and detector tied to one causal event). Elapsed-time and
 * inter-firing gaps are logged as observations, never gated: a timing bound used as a correctness gate
 * manufactures its own evidence, and this repo's CI has no retries.
 * <p>
 * <b>Calibration (the Goal Capsule's stop condition made concrete).</b> Window {@value #WINDOW_SECONDS}s,
 * tolerance {@value #TOLERANCE_PERCENT}%. The demo policy (KTD7: rate 2/s, quantum 1s, burst 2) phase-locks
 * firings to quantum starts, so an anchored window of W seconds can honestly contain
 * {@code floor(W/q)} to {@code floor(W/q)+1} firings per instance, plus one either side for scheduling jitter
 * around the window edges. At W=12 the expected per-instance count is 12; the 40% band [7.2, 16.8] absorbs
 * that +-2 phase-lock jitter with room to spare while still SEPARATING the adjacent hypotheses this test
 * exists to refute: an unthrottled instance (hundreds), a full-rate instance (24 &gt; 16.8), and a
 * half-stalled one (6 &lt; 7.2). For the survivor window the expected count is 24 and the band [14.4, 33.6]
 * excludes a survivor stuck at its old 1Hz share (12 &lt; 14.4). The aggregate bound is DERIVED, not tuned:
 * an unaligned W-second window intersects at most {@code floor(W/q)+1} quanta, each minting at most
 * {@code rate*q} credits, and spends beyond live credit land as overdraft bounded by burst under R8 - so
 * {@code rate*(W+q) + burst} = 28 credits is the most 12 anchored seconds can honestly fire.
 * <p>
 * <b>Why the group is stabilised before the backlog is produced.</b> Three consumers joining one group
 * rebalance a few times before settling at one partition each; a transient owner of another instance's
 * eventual partition would drain (bystander) or throttle (tagged) records the settled owner's measurement
 * depends on. Producing only after the admin API reports three members with one partition each removes that
 * race instead of tolerating it.
 * <p>
 * The adaptive admission target stays at its default DISABLED (non-enforcing) mode, so the hook-measured rate
 * isolates the navigator's admission gating (R15).
 */
@Slf4j
class NavigatorRateShareTest extends BrokerIntegrationTest<String, String> {

    private static final String RESOURCE = "api-x";
    private static final double RATE_PER_SECOND = 2.0;
    private static final int BURST = 2;
    private static final Duration QUANTUM = ofSeconds(1);

    private static final int WINDOW_SECONDS = 12;
    private static final Duration WINDOW = ofSeconds(WINDOW_SECONDS);
    private static final int TOLERANCE_PERCENT = 40;

    /**
     * ~500 records per partition: the tagged instances consume ~40 credits over the whole storyline, so their
     * partitions never drain, while the bystander has a real backlog to demonstrate unthrottled draining on.
     */
    private static final int BACKLOG_RECORDS = 1500;

    {
        super.numPartitions = 3; // one per instance, so every instance demonstrably owns work
    }

    private SimpleMeterRegistry simpleMeterRegistry;

    /** Captures {@link ProcessingShard}'s defer-moment attribution lines (AE5); detached before being read. */
    private ListAppender<ILoggingEvent> navigatorLogAppender;
    private Logger shardLogger;
    private Level shardLoggerPreviousLevel;
    private Level testLoggerPreviousLevel;

    @BeforeEach
    void setup() {
        setupTopic();
        simpleMeterRegistry = new SimpleMeterRegistry();

        // the harness default is warn (logback-test.xml) - raise the attribution site so AE5's log line
        // reaches the appender, and this test's own logger so the calibration observations always print
        shardLogger = (Logger) LoggerFactory.getLogger(ProcessingShard.class);
        shardLoggerPreviousLevel = shardLogger.getLevel();
        shardLogger.setLevel(Level.INFO);
        Logger testLogger = (Logger) LoggerFactory.getLogger(NavigatorRateShareTest.class);
        testLoggerPreviousLevel = testLogger.getLevel();
        testLogger.setLevel(Level.INFO);

        navigatorLogAppender = new ListAppender<>();
        navigatorLogAppender.start();
        shardLogger.addAppender(navigatorLogAppender);
    }

    @AfterEach
    void cleanup() {
        shardLogger.detachAppender(navigatorLogAppender);
        shardLogger.setLevel(shardLoggerPreviousLevel);
        ((Logger) LoggerFactory.getLogger(NavigatorRateShareTest.class)).setLevel(testLoggerPreviousLevel);
        simpleMeterRegistry.close();
    }

    @SneakyThrows
    @Test
    void sharedResourceRateIsSplitAcrossTaggedInstancesAndRedividesWhenOneCloses() {
        Instant testStart = Instant.now();

        // ONE allocator on the production clock, shared by every tagged instance (KTD3); the resource is
        // registered before any instance tagging it is built (KD5)
        StubResourceAllocator allocator = new StubResourceAllocator();
        allocator.register(new ResourceContract(RESOURCE, RATE_PER_SECOND, BURST, QUANTUM));

        String taggedTag1 = "navigator-tagged-1-" + UUID.randomUUID();
        String taggedTag2 = "navigator-tagged-2-" + UUID.randomUUID();
        String untaggedTag = "navigator-bystander-" + UUID.randomUUID();

        ParallelEoSStreamProcessor<String, String> taggedPc1 = buildPc(taggedTag1, NEW_GROUP, allocator);
        ParallelEoSStreamProcessor<String, String> taggedPc2 = buildPc(taggedTag2, REUSE_GROUP, allocator);
        ParallelEoSStreamProcessor<String, String> bystanderPc = buildPc(untaggedTag, REUSE_GROUP, null);
        List<ParallelEoSStreamProcessor<String, String>> allInstances =
                UniLists.of(taggedPc1, taggedPc2, bystanderPc);

        ConcurrentLinkedQueue<Instant> firings1 = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Instant> firings2 = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Instant> bystanderFirings = new ConcurrentLinkedQueue<>();

        AtomicReference<ViewSample> blockingSample1 = new AtomicReference<>();
        AtomicReference<ViewSample> blockingSample2 = new AtomicReference<>();
        AtomicReference<String> bystanderViewViolation = new AtomicReference<>();

        try {
            taggedPc1.poll(context -> {
                firings1.add(Instant.now());
                sampleBlockingViewMaybe(context.getNavigatorView(), blockingSample1);
            });
            taggedPc2.poll(context -> {
                firings2.add(Instant.now());
                sampleBlockingViewMaybe(context.getNavigatorView(), blockingSample2);
            });
            bystanderPc.poll(context -> {
                bystanderFirings.add(Instant.now());
                recordBystanderViewViolationMaybe(context.getNavigatorView(), bystanderViewViolation);
            });

            awaitGroupStableWithOnePartitionEach(3);
            getKcu().produceMessages(topic, BACKLOG_RECORDS);

            // --- AE1: both tagged instances fire at ~1Hz over anchored windows -----------------------------
            Instant anchor1 = awaitFirstFiringAtOrAfter(firings1, Instant.EPOCH, "tagged instance 1");
            Instant anchor2 = awaitFirstFiringAtOrAfter(firings2, Instant.EPOCH, "tagged instance 2");
            awaitWindowSettled(firings1, anchor1, "tagged instance 1");
            awaitWindowSettled(firings2, anchor2, "tagged instance 2");

            long count1 = countIn(firings1, anchor1, anchor1.plus(WINDOW));
            long count2 = countIn(firings2, anchor2, anchor2.plus(WINDOW));
            double expectedPerInstance = (RATE_PER_SECOND / 2) * WINDOW_SECONDS; // equal share at two members
            log.info("AE1 observation: window={}s expected={} counts: tagged1={} tagged2={} "
                            + "(anchors {} / {})",
                    WINDOW_SECONDS, expectedPerInstance, count1, count2, anchor1, anchor2);
            assertThat((double) count1)
                    .as("AE1/R10: tagged instance 1 fires at ~1Hz over its anchored window")
                    .isCloseTo(expectedPerInstance, withPercentage(TOLERANCE_PERCENT));
            assertThat((double) count2)
                    .as("AE1/R10: tagged instance 2 fires at ~1Hz over its anchored window")
                    .isCloseTo(expectedPerInstance, withPercentage(TOLERANCE_PERCENT));

            // aggregate over ONE common wall-clock window, against the derived R8 bound (see class javadoc)
            Instant commonStart = anchor1.isBefore(anchor2) ? anchor1 : anchor2;
            Instant commonEnd = commonStart.plus(WINDOW);
            awaitWindowSettled(firings1, commonStart, "tagged instance 1 (common window)");
            awaitWindowSettled(firings2, commonStart, "tagged instance 2 (common window)");
            long aggregate = countIn(firings1, commonStart, commonEnd) + countIn(firings2, commonStart, commonEnd);
            double aggregateBound = RATE_PER_SECOND * (WINDOW_SECONDS + QUANTUM.getSeconds()) + BURST;
            log.info("AE1 observation: aggregate over common window [{}, {}) = {} (bound {})",
                    commonStart, commonEnd, aggregate, aggregateBound);
            assertThat((double) aggregate)
                    .as("R8/R12: aggregate tagged firings within rate*(window+quantum)+burst")
                    .isLessThanOrEqualTo(aggregateBound);

            // --- AE1 attribution half: while throttled, the view names api-x and its next credit time ------
            assertBlockingSample(blockingSample1.get(), "tagged instance 1");
            assertBlockingSample(blockingSample2.get(), "tagged instance 2");

            // --- AE5 smoke: at least one deferral, with its metric and its attribution log line ------------
            await().atMost(ofSeconds(30)).untilAsserted(() -> assertThat(
                    sumCounters(Search.in(simpleMeterRegistry).name(NAVIGATOR_DEFERRAL_EPISODES.getName())))
                    .as("AE5: at least one deferral episode recorded by the tagged instances")
                    .isGreaterThan(0.0));
            assertThat(sumGauges(Search.in(simpleMeterRegistry).name(NAVIGATOR_CREDITS_SPENT.getName())
                    .tag("resource", RESOURCE)))
                    .as("AE5/KTD2: credits demonstrably spent against the shared resource")
                    .isGreaterThan(0.0);

            // detach before reading: ListAppender's list is a plain ArrayList, and the attribution site keeps
            // logging from PC threads - a snapshot of a still-written list is a torn read
            shardLogger.detachAppender(navigatorLogAppender);
            List<String> navigatorLogLines = navigatorLogAppender.list.stream()
                    .map(ILoggingEvent::getFormattedMessage)
                    .filter(message -> message.startsWith(NavigatorParticipant.LOG_PREFIX))
                    .collect(Collectors.toList());
            assertThat(navigatorLogLines)
                    .as("AE5/R9: the defer moment logged, naming the resource")
                    .anyMatch(message -> message.contains(RESOURCE)
                            && message.contains("entered resource deferral"));

            // --- AE4: the untagged bystander is unaffected and records zero attributions -------------------
            await().atMost(ofSeconds(60)).untilAsserted(() -> assertThat(bystanderFirings.size())
                    .as("AE4 progress gate: the bystander drains a real chunk of its backlog")
                    .isGreaterThanOrEqualTo(100));
            Instant bystanderAnchor = bystanderFirings.stream().min(Instant::compareTo)
                    .orElseThrow(IllegalStateException::new);
            long bystanderWindowCount =
                    countIn(bystanderFirings, bystanderAnchor, bystanderAnchor.plus(WINDOW));
            log.info("AE4 observation: bystander fired {} times in its first {}s window (throttled bound {})",
                    bystanderWindowCount, WINDOW_SECONDS, aggregateBound);
            assertThat((double) bystanderWindowCount)
                    .as("AE4/R3: the bystander's anchored-window throughput exceeds what the navigator "
                            + "would ever allow a tagged pair - it is demonstrably not throttled")
                    .isGreaterThan(aggregateBound);
            assertThat(bystanderViewViolation.get())
                    .as("AE4/AE6: every bystander view sample was inert - isActive false, empty counts, "
                            + "unconstrained rates")
                    .isNull();
            assertThat(navigatorMetersTaggedWith(untaggedTag))
                    .as("AE4: no pc.navigator.* meter ever registered for the untagged instance")
                    .isEmpty();
            assertThat(navigatorLogLines)
                    .as("AE4: no Navigator attribution line ever names the untagged instance")
                    .noneMatch(message -> message.contains("(" + untaggedTag + ")"));

            // --- AE2: close one tagged instance; the survivor converges toward the full 2Hz ----------------
            Instant closeInstant = Instant.now();
            taggedPc1.close(); // membership leaves at close ENTRY; re-division effective NEXT quantum (R16)
            // anchor the fresh window past the re-division boundary: one quantum for the leave to take
            // effect plus one for boundary partiality - the anchor then self-adjusts past any rebalance stall
            Instant convergenceFloor = closeInstant.plus(QUANTUM.multipliedBy(2));
            Instant survivorAnchor =
                    awaitFirstFiringAtOrAfter(firings2, convergenceFloor, "survivor (post-close)");
            awaitWindowSettled(firings2, survivorAnchor, "survivor (post-close)");
            long survivorCount = countIn(firings2, survivorAnchor, survivorAnchor.plus(WINDOW));
            double expectedSurvivor = RATE_PER_SECOND * WINDOW_SECONDS;
            log.info("AE2 observation: survivor fired {} times in its post-close {}s window (expected {}, "
                            + "anchor {} vs close at {})",
                    survivorCount, WINDOW_SECONDS, expectedSurvivor, survivorAnchor, closeInstant);
            assertThat((double) survivorCount)
                    .as("AE2/R11: the survivor converges toward the full 2Hz after re-division")
                    .isCloseTo(expectedSurvivor, withPercentage(TOLERANCE_PERCENT));

            // R12 across the whole storyline, from the allocator's own conservation ledger: everything ever
            // minted plus every overdraft stays within rate * elapsed (+1 quantum boundary partiality) + burst
            Instant ledgerReadAt = Instant.now();
            ConservationLedger ledger = allocator.conservationLedger(RESOURCE, ledgerReadAt);
            double elapsedSeconds = Duration.between(testStart, ledgerReadAt).toMillis() / 1000.0;
            double mintBound = RATE_PER_SECOND * (elapsedSeconds + QUANTUM.getSeconds()) + BURST;
            log.info("R12 observation: ledger over {}s - minted={} spent={} expired={} overdraft={} "
                            + "outstanding={} liveCredits={} (mint+overdraft bound {})",
                    String.format("%.1f", elapsedSeconds), ledger.getMinted(), ledger.getSpent(),
                    ledger.getExpired(), ledger.getOverdraft(), ledger.getOutstanding(),
                    ledger.getLiveCredits(), String.format("%.1f", mintBound));
            assertThat((double) (ledger.getMinted() + ledger.getOverdraft()))
                    .as("R12/KD10: minted + overdraft never exceeds the declared rate over the observed "
                            + "span plus burst")
                    .isLessThanOrEqualTo(mintBound);
            assertThat(ledger.getOutstanding())
                    .as("KTD2: the conservation identity closes at this observation point")
                    .isEqualTo(ledger.getLiveCredits());
        } finally {
            for (ParallelEoSStreamProcessor<String, String> instance : allInstances) {
                closeQuietly(instance);
            }
        }
    }

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    private ParallelEoSStreamProcessor<String, String> buildPc(String pcInstanceTag,
                                                               KafkaClientUtils.GroupOption groupOption,
                                                               StubResourceAllocator sharedAllocatorOrNull) {
        ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder =
                ParallelConsumerOptions.<String, String>builder()
                        .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                        .consumer(getKcu().createNewConsumer(groupOption))
                        .meterRegistry(simpleMeterRegistry)
                        .pcInstanceTag(pcInstanceTag)
                        .ordering(PARTITION); // the MultiInstance pattern - and at 1Hz, ordering never binds
        if (sharedAllocatorOrNull != null) {
            builder.resourceTags(Collections.singletonList(RESOURCE))
                    .resourceAllocator(sharedAllocatorOrNull);
        }
        ParallelConsumerOptions<String, String> options = builder.build();
        ParallelEoSStreamProcessor<String, String> pc =
                new ParallelEoSStreamProcessor<>(options, new PCModule<>(options));
        pc.subscribe(UniSets.of(topic));
        return pc;
    }

    private void closeQuietly(ParallelEoSStreamProcessor<String, String> instance) {
        try {
            if (!instance.isClosedOrFailed()) {
                instance.close();
            }
        } catch (Exception e) {
            log.warn("Ignoring exception closing instance during teardown", e);
        }
    }

    // group stabilisation lives on BrokerIntegrationTest (shared with NavigatorDemo)

    // ------------------------------------------------------------------
    // Anchored-window measurement (KTD8)
    // ------------------------------------------------------------------

    /** The earliest firing at or after {@code floor} - awaited, so the anchor is a real observed event. */
    private Instant awaitFirstFiringAtOrAfter(ConcurrentLinkedQueue<Instant> firings, Instant floor,
                                              String who) {
        await().atMost(ofSeconds(60)).untilAsserted(() -> assertThat(
                firings.stream().anyMatch(firing -> !firing.isBefore(floor)))
                .as("a firing for %s at or after %s - %s so far, latest %s", who, floor,
                        firings.size(), latestOf(firings))
                .isTrue());
        return firings.stream().filter(firing -> !firing.isBefore(floor)).min(Instant::compareTo)
                .orElseThrow(IllegalStateException::new);
    }

    /**
     * Waits until a firing PAST the window's end (plus half a quantum of settling margin) has been observed,
     * so counting the window afterwards cannot race a straggling in-window entry. Progress-gated: with the
     * backlog outlasting the storyline, an instance that stops firing fails this await - which is a real
     * finding, not a flake.
     */
    private void awaitWindowSettled(ConcurrentLinkedQueue<Instant> firings, Instant anchor, String who) {
        Instant settledMarker = anchor.plus(WINDOW).plus(QUANTUM.dividedBy(2));
        await().atMost(WINDOW.plus(ofSeconds(60))).untilAsserted(() -> assertThat(
                firings.stream().anyMatch(firing -> !firing.isBefore(settledMarker)))
                .as("%s keeps firing past its window end %s - %s firings so far, latest %s", who,
                        settledMarker, firings.size(), latestOf(firings))
                .isTrue());
    }

    /** The latest firing timestamp, for diagnostics in await messages - "none" before any firing. */
    private static String latestOf(ConcurrentLinkedQueue<Instant> firings) {
        return firings.stream().max(Instant::compareTo).map(Instant::toString).orElse("none");
    }

    /** Count of firings in {@code [start, end)} - insertion order is irrelevant, only the timestamps count. */
    private static long countIn(ConcurrentLinkedQueue<Instant> firings, Instant start, Instant end) {
        return firings.stream().filter(firing -> !firing.isBefore(start) && firing.isBefore(end)).count();
    }

    // ------------------------------------------------------------------
    // Navigator view sampling (the R13 hook, taken inside the user function)
    // ------------------------------------------------------------------

    /** One throttled-moment snapshot of a tagged instance's {@link NavigatorView} (AE1's attribution half). */
    private static final class ViewSample {
        final String resourceName;
        final boolean nextCreditAtPresent;
        final OptionalDouble localRatePerSecond;
        final OptionalDouble globalRatePerSecond;

        ViewSample(String resourceName, boolean nextCreditAtPresent, OptionalDouble localRatePerSecond,
                   OptionalDouble globalRatePerSecond) {
            this.resourceName = resourceName;
            this.nextCreditAtPresent = nextCreditAtPresent;
            this.localRatePerSecond = localRatePerSecond;
            this.globalRatePerSecond = globalRatePerSecond;
        }
    }

    /**
     * Captures the FIRST sample where the view reports a blocking resource - with a large backlog and one
     * credit per quantum, the moment a record fires its sibling records are already deferred again, so the
     * view read from inside the user function reports the throttle live.
     */
    private static void sampleBlockingViewMaybe(NavigatorView view, AtomicReference<ViewSample> target) {
        if (target.get() != null) {
            return;
        }
        List<ResourceDeferral> blocking = view.blockingResourceDeferrals();
        if (blocking.isEmpty()) {
            return;
        }
        ResourceDeferral first = blocking.get(0);
        target.compareAndSet(null, new ViewSample(
                first.getResourceName(),
                first.getNextCreditAt().isPresent(),
                view.localRatePerSecond(first.getResourceName()),
                view.globalRatePerSecond(first.getResourceName())));
    }

    private void assertBlockingSample(ViewSample sample, String who) {
        assertThat(sample)
                .as("AE1 attribution: %s sampled a throttled moment through its PollContext view", who)
                .isNotNull();
        assertThat(sample.resourceName).as("%s: the blocking resource is the tagged one", who)
                .isEqualTo(RESOURCE);
        assertThat(sample.nextCreditAtPresent)
                .as("%s: the deferral names when credit next arrives (R18)", who)
                .isTrue();
        // sampled during the two-member steady state: the group stabilised (seconds) long after both
        // instances joined the allocator at their running transitions, so the equal share is settled
        assertThat(sample.localRatePerSecond).as("%s: local share of the 2/s policy at two members", who)
                .hasValue(RATE_PER_SECOND / 2);
        assertThat(sample.globalRatePerSecond).as("%s: the resource's whole declared rate", who)
                .hasValue(RATE_PER_SECOND);
    }

    /**
     * Records the first way the bystander's view ever deviates from the inert shape (AE6) - checked on EVERY
     * firing, so a single active-looking sample anywhere in the run fails the test.
     */
    private static void recordBystanderViewViolationMaybe(NavigatorView view, AtomicReference<String> violation) {
        if (view.isActive()) {
            violation.compareAndSet(null, "isActive() returned true");
        } else if (!view.resourceTags().isEmpty()) {
            violation.compareAndSet(null, "resourceTags() not empty: " + view.resourceTags());
        } else if (view.resourceIneligibleCount() != 0) {
            violation.compareAndSet(null, "resourceIneligibleCount() " + view.resourceIneligibleCount());
        } else if (!view.blockingResourceDeferrals().isEmpty()) {
            violation.compareAndSet(null, "blockingResourceDeferrals() not empty");
        } else if (view.localRatePerSecond(RESOURCE).isPresent()
                || view.globalRatePerSecond(RESOURCE).isPresent()) {
            violation.compareAndSet(null, "rates constrained for an untagged instance");
        }
    }

    // ------------------------------------------------------------------
    // Meter helpers (the MultiInstanceMetricsTest idiom, extended to gauges)
    // ------------------------------------------------------------------

    private static double sumCounters(Search search) {
        return search.counters().stream().mapToDouble(Counter::count).sum();
    }

    private static double sumGauges(Search search) {
        return search.gauges().stream().mapToDouble(Gauge::value).sum();
    }

    private List<Meter> navigatorMetersTaggedWith(String pcInstanceTag) {
        List<Meter> matches = new ArrayList<>();
        for (Meter meter : simpleMeterRegistry.getMeters()) {
            if (meter.getId().getName().startsWith("pc.navigator")
                    && pcInstanceTag.equals(meter.getId().getTag(PC_INSTANCE_TAG))) {
                matches.add(meter);
            }
        }
        return matches;
    }
}
