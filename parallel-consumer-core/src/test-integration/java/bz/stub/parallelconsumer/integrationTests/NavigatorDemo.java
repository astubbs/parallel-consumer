package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.navigator.ConservationLedger;
import bz.stub.parallelconsumer.navigator.NavigatorView;
import bz.stub.parallelconsumer.navigator.ResourceContract;
import bz.stub.parallelconsumer.navigator.ResourceDeferral;
import bz.stub.parallelconsumer.navigator.StubResourceAllocator;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static java.time.Duration.ofSeconds;

/**
 * The navigator's human-watchable demonstration: global rate limiting, inside the consumer, that explains
 * itself. Three PC instances share one consumer group. Two of them tag the shared resource
 * {@value #RESOURCE} ({@value #RATE_PER_SECOND} credits/second, split equally); the third declares nothing.
 * The demo prints a clean per-second dashboard - one row per second, no log prefixes - so a person can watch:
 * <ul>
 *   <li>the two tagged instances each fire at ~1Hz (the 2/s policy split two ways),</li>
 *   <li>the untagged bystander drain its backlog flat-out, untouched,</li>
 *   <li>one tagged instance close, and the survivor pick up the whole 2Hz next quantum,</li>
 *   <li>and the throttle explain itself - the user function's own context names the blocking resource and
 *       when the next credit arrives.</li>
 * </ul>
 * Off by default, same discipline as the classic {@code Demo} and {@link AdaptiveConcurrencyDemo}: this is a
 * measurement with no assertions and must not run on every build. Run it with:
 * <pre>bin/demo-navigator.sh</pre>
 * or directly:
 * <pre>./mvnw -q verify -pl parallel-consumer-core -am -Dpc.demo=true -Dit.test=NavigatorDemo \
 *     -Dtest=skipall -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dfailsafe.failIfNoSpecifiedTests=false</pre>
 * The rate-share <b>assertions</b> live in {@link NavigatorRateShareTest}, which CI runs on every PR; this
 * class is the same storyline optimised for eyes instead of gates. The demo conventions this class follows
 * (the asserted-twin rule, the wrapper, the output discipline) are owned by {@code docs/demos.md}.
 */
public class NavigatorDemo extends BrokerIntegrationTest<String, String> {

    private static final String RESOURCE = "api-x";
    private static final double RATE_PER_SECOND = 2.0;
    private static final int BURST = 2;
    private static final Duration QUANTUM = ofSeconds(1);

    private static final int PHASE_1_SECONDS = 14;
    private static final int PHASE_2_SECONDS = 10;
    private static final int BACKLOG_RECORDS = 1500;
    private static final int BAR_CAP = 40;

    {
        super.numPartitions = 3; // one per instance, so every instance demonstrably owns work
    }

    static {
        quietLogsForTheAudience();
    }

    @SneakyThrows
    @Test
    @EnabledIfSystemProperty(named = "pc.demo", matches = "true")
    void watchTwoInstancesShareOneRateLimit() {
        setupTopic();

        StubResourceAllocator allocator = new StubResourceAllocator();
        allocator.register(new ResourceContract(RESOURCE, RATE_PER_SECOND, BURST, QUANTUM));

        ParallelEoSStreamProcessor<String, String> tagged1 = buildInstance(GroupOption.NEW_GROUP, allocator);
        ParallelEoSStreamProcessor<String, String> tagged2 = buildInstance(GroupOption.REUSE_GROUP, allocator);
        ParallelEoSStreamProcessor<String, String> bystander = buildInstance(GroupOption.REUSE_GROUP, null);
        List<ParallelEoSStreamProcessor<String, String>> all = UniLists.of(tagged1, tagged2, bystander);

        ConcurrentLinkedQueue<Instant> firings1 = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Instant> firings2 = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Instant> bystanderFirings = new ConcurrentLinkedQueue<>();
        AtomicReference<String> throttleExplanation = new AtomicReference<>();

        try {
            banner();

            tagged1.poll(context -> {
                firings1.add(Instant.now());
                explainThrottleMaybe(context.getNavigatorView(), throttleExplanation);
            });
            tagged2.poll(context -> firings2.add(Instant.now()));
            bystander.poll(context -> bystanderFirings.add(Instant.now()));

            System.out.println("  waiting for the consumer group to settle (three members, one partition each)...");
            awaitGroupStableWithOnePartitionEach(3);
            System.out.printf("  producing a backlog of %,d records...%n%n", BACKLOG_RECORDS);
            getKcu().produceMessages(topic, BACKLOG_RECORDS);

            System.out.printf("%7s  %-14s %-14s %-24s%n", "", "tagged-1", "tagged-2", "bystander (no tags)");
            Instant anchor = Instant.now();
            tick(anchor, 1, PHASE_1_SECONDS, firings1, firings2, bystanderFirings);

            System.out.println();
            System.out.println("  >>> closing tagged-2 - its share of api-x dies with it; the survivor");
            System.out.println("  >>> inherits the WHOLE 2/s from the next one-second quantum on.");
            System.out.println();
            tagged2.close();

            tick(anchor, PHASE_1_SECONDS + 1, PHASE_1_SECONDS + PHASE_2_SECONDS, firings1, firings2,
                    bystanderFirings);

            summary(anchor, firings1, firings2, bystanderFirings, throttleExplanation.get(), allocator);
        } finally {
            for (ParallelEoSStreamProcessor<String, String> instance : all) {
                closeQuietly(instance);
            }
        }
    }

    /**
     * The dashboard is the show, so the INFO sources that survive the harness's warn default are lowered for
     * this forked demo JVM. The two named loggers carry EXPLICIT info pins in {@code logback-test.xml}
     * (the harness's own diagnostics), so the exact pinned loggers must be re-levelled - a parent-level set
     * does not override an explicit child pin. Runs from the static initialiser because the base class's
     * {@code @BeforeEach} already opens Kafka clients before any test method. Demo-only - the asserted lane
     * ({@link NavigatorRateShareTest}) leaves levels alone.
     */
    private static void quietLogsForTheAudience() {
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger("org.apache.kafka.clients.consumer.internals.SubscriptionState"))
                .setLevel(ch.qos.logback.classic.Level.WARN);
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger("bz.stub.parallelconsumer.integrationTests"))
                .setLevel(ch.qos.logback.classic.Level.WARN);
    }

    // ------------------------------------------------------------------
    // The dashboard
    // ------------------------------------------------------------------

    private static void banner() {
        System.out.println();
        System.out.println("================================================================================");
        System.out.println("  PARALLEL CONSUMER NAVIGATOR - global rate limiting that explains itself");
        System.out.println("================================================================================");
        System.out.printf("  One shared resource: '%s' at %.0f credits/second (burst %d).%n",
                RESOURCE, RATE_PER_SECOND, BURST);
        System.out.println("  Three consumers in ONE consumer group, all with real Kafka backlogs:");
        System.out.printf("    tagged-1, tagged-2  declare they use '%s'  -> split the rate: ~1 rec/s each%n",
                RESOURCE);
        System.out.println("    bystander           declares nothing        -> completely unthrottled");
        System.out.println("  Each # is one record processed in that second.");
        System.out.println("================================================================================");
        System.out.println();
    }

    /** Prints one dashboard row per wall-clock second from {@code fromSecond} to {@code toSecond} inclusive. */
    @SneakyThrows
    private void tick(Instant anchor, int fromSecond, int toSecond,
                      ConcurrentLinkedQueue<Instant> firings1,
                      ConcurrentLinkedQueue<Instant> firings2,
                      ConcurrentLinkedQueue<Instant> bystanderFirings) {
        for (int second = fromSecond; second <= toSecond; second++) {
            Instant rowEnd = anchor.plusSeconds(second);
            long sleepMillis = Duration.between(Instant.now(), rowEnd).toMillis();
            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
            Instant rowStart = rowEnd.minusSeconds(1);
            long count1 = countIn(firings1, rowStart, rowEnd);
            long count2 = countIn(firings2, rowStart, rowEnd);
            long countBystander = countIn(bystanderFirings, rowStart, rowEnd);
            boolean bystanderDrained = countBystander == 0 && bystanderFirings.size() >= BACKLOG_RECORDS / 3;
            System.out.printf("  t=%3ds  %-14s %-14s %-24s%n",
                    second,
                    cell(count1),
                    cell(count2),
                    bystanderDrained ? "(backlog drained)" : cell(countBystander));
        }
    }

    private static String cell(long count) {
        int bar = (int) Math.min(count, BAR_CAP);
        StringBuilder rendered = new StringBuilder();
        for (int i = 0; i < bar; i++) {
            rendered.append('#');
        }
        if (count > BAR_CAP) {
            rendered.append('+');
        }
        return String.format("%-4s %3d", rendered, count);
    }

    // ------------------------------------------------------------------
    // The summary
    // ------------------------------------------------------------------

    private void summary(Instant anchor,
                         ConcurrentLinkedQueue<Instant> firings1,
                         ConcurrentLinkedQueue<Instant> firings2,
                         ConcurrentLinkedQueue<Instant> bystanderFirings,
                         String throttleExplanation,
                         StubResourceAllocator allocator) {
        // measure each phase's steady middle: skip the first two seconds of each phase (join/re-division)
        Instant phase1Start = anchor.plusSeconds(2);
        Instant phase1End = anchor.plusSeconds(PHASE_1_SECONDS);
        double phase1Seconds = PHASE_1_SECONDS - 2;
        Instant phase2Start = anchor.plusSeconds(PHASE_1_SECONDS + 2);
        Instant phase2End = anchor.plusSeconds(PHASE_1_SECONDS + PHASE_2_SECONDS);
        double phase2Seconds = PHASE_2_SECONDS - 2;

        System.out.println();
        System.out.println("================================================================================");
        System.out.println("  WHAT JUST HAPPENED");
        System.out.println("================================================================================");
        System.out.printf("  Phase 1 (two tagged members): tagged-1 %.1f rec/s, tagged-2 %.1f rec/s "
                        + "- the %.0f/s policy split two ways.%n",
                countIn(firings1, phase1Start, phase1End) / phase1Seconds,
                countIn(firings2, phase1Start, phase1End) / phase1Seconds,
                RATE_PER_SECOND);
        System.out.printf("  Phase 2 (tagged-2 closed):    tagged-1 %.1f rec/s - the survivor inherited "
                        + "the whole rate.%n",
                countIn(firings1, phase2Start, phase2End) / phase2Seconds);
        System.out.printf("  Bystander: %,d records processed, never throttled - no tags, zero cost.%n",
                bystanderFirings.size());
        System.out.println();
        if (throttleExplanation != null) {
            System.out.println("  The throttle explained itself. While waiting, tagged-1's own user function");
            System.out.println("  asked its context WHY, and was told:");
            System.out.printf("    %s%n", throttleExplanation);
            System.out.println();
        }
        ConservationLedger ledger = allocator.conservationLedger(RESOURCE, Instant.now());
        System.out.printf("  The allocator's books balance: minted %d + overdraft %d = spent %d + expired %d "
                        + "+ outstanding %d.%n",
                ledger.getMinted(), ledger.getOverdraft(), ledger.getSpent(), ledger.getExpired(),
                ledger.getOutstanding());
        System.out.println("  No instance ever blocked a thread waiting - ineligible records simply were not");
        System.out.println("  selected until their credit arrived (soft, cooperative, bounded overshoot).");
        System.out.println("================================================================================");
        System.out.println();
    }

    /** Captures, once, the human-readable throttle attribution from the user function's own context. */
    private static void explainThrottleMaybe(NavigatorView view, AtomicReference<String> target) {
        if (target.get() != null) {
            return;
        }
        List<ResourceDeferral> blocking = view.blockingResourceDeferrals();
        if (blocking.isEmpty()) {
            return;
        }
        ResourceDeferral first = blocking.get(0);
        target.compareAndSet(null, String.format(
                "blocked by '%s' - next credit at %s; my share %.1f/s of %.1f/s global",
                first.getResourceName(),
                first.getNextCreditAt().map(Instant::toString).orElse("(unknown)"),
                view.localRatePerSecond(first.getResourceName()).orElse(Double.NaN),
                view.globalRatePerSecond(first.getResourceName()).orElse(Double.NaN)));
    }

    // ------------------------------------------------------------------
    // Construction (the NavigatorRateShareTest pattern, without the measurement scaffolding)
    // ------------------------------------------------------------------

    private ParallelEoSStreamProcessor<String, String> buildInstance(GroupOption groupOption,
                                                                     StubResourceAllocator sharedAllocatorOrNull) {
        ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder =
                ParallelConsumerOptions.<String, String>builder()
                        .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                        .consumer(getKcu().createNewConsumer(groupOption))
                        .ordering(PARTITION);
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
            System.out.printf("  (ignoring exception closing an instance during teardown: %s)%n", e.getMessage());
        }
    }

    private static long countIn(ConcurrentLinkedQueue<Instant> firings, Instant start, Instant end) {
        return firings.stream().filter(firing -> !firing.isBefore(start) && firing.isBefore(end)).count();
    }
}
