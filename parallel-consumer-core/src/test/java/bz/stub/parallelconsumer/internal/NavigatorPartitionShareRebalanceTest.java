package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.navigator.AssignmentSnapshot;
import bz.stub.parallelconsumer.navigator.PartitionShareResourceAllocator;
import bz.stub.parallelconsumer.navigator.ResourceContract;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The rebalance callbacks under the default strategy (the partition-share plan's U3: R2, R4, R5, KTD2, KTD3):
 * each of {@code onPartitionsRevoked}, {@code onPartitionsAssigned} and {@code onPartitionsLost} maintains the
 * held-partition set and publishes one immutable {@link AssignmentSnapshot} to the engine-built
 * {@link PartitionShareResourceAllocator}; an assign re-reads every subscribed topic's partition total through
 * the timed {@code partitionsFor}, and ANY decline - a throw, a timeout, a null or an empty result - publishes an
 * unresolved snapshot rather than keeping the previous total, so the instance mints nothing until the next
 * assignment resolves it.
 * <p>
 * The callbacks are driven DIRECTLY on the test thread against a constructed-but-unstarted
 * {@link ParallelEoSStreamProcessor} over a {@link PCModuleTestEnv}: no control loop, no poll thread, so every
 * publication is observed through the allocator's own pure reads on the shared {@link MutableClock}. A
 * publication takes effect from the NEXT quantum (R4), so the assertions read the snapshot effective one
 * quantum after the callback, without moving the clock.
 * <p>
 * <b>Callback discipline, asserted by review (the Definition of Done's write-lock note).</b> The new callback
 * code in {@code AbstractParallelEoSStreamProcessor} - {@code publishNavigatorAssignmentAfterAssign},
 * {@code publishNavigatorAssignmentAfterLoss}, {@code publishNavigatorAssignment} and
 * {@code readSubscribedPartitionTotals} - acquires no monitor, lock or condition: it mutates two poll-thread-
 * confined collections, calls the consumer's timed metadata read (bounded by
 * {@code PARTITION_TOTAL_READ_TIMEOUT} per subscribed topic), and calls
 * {@link PartitionShareResourceAllocator#publish}, which is an atomic append to an immutable history and never
 * touches the allocator's {@code stateLock}. Nothing on that path can wait on the control thread, the commit
 * lock, or {@code RetryQueue}'s write lock. The
 * {@link #aCallbackCompletesWhileTheControlThreadMonitorIsHeld} test proves the one interaction that could have
 * introduced a wait - publishing while the control-thread monitor is held - completes.
 *
 * @author Antony Stubbs
 */
@Timeout(30)
class NavigatorPartitionShareRebalanceTest {

    static final String ORDERS = "orders";
    static final String PAYMENTS = "payments";
    static final String API_X = "api-x";
    /**
     * Unique per test instance: the suite runs test methods in parallel and every test's appender on the shared
     * processor logger sees every test's events, so the decline warnings are filtered by the member id the
     * processor names itself with.
     */
    final String memberId = "pc-partition-share-member-" + UUID.randomUUID();
    static final Duration QUANTUM = Duration.ofSeconds(1);

    /** 4 credits/sec on a one-second quantum: a share of {@code k/4} is {@code k} credits per quantum - legible. */
    static final ResourceContract POLICY = new ResourceContract(API_X, 4.0, 4, QUANTUM);

    final MutableClock clock = MutableClock.epochUTC();
    final ScriptedMetadataConsumer consumer = new ScriptedMetadataConsumer();

    PCModuleTestEnv module;
    ParallelEoSStreamProcessor<String, String> pc;
    PartitionShareResourceAllocator allocator;

    ListAppender<ILoggingEvent> processorLog;
    Logger processorLogger;

    @BeforeEach
    void setUp() {
        processorLogger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        processorLog = new ListAppender<>();
        processorLog.start();
        processorLogger.addAppender(processorLog);
    }

    @AfterEach
    void tearDown() {
        processorLogger.detachAppender(processorLog);
        if (pc != null) {
            pc.close();
        }
    }

    /** A processor under the DEFAULT strategy: tags + the contract, nothing else - F4's zero-code path. */
    void buildProcessor() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(memberId)
                .resourceTags(UniLists.of(API_X))
                .resourceContracts(UniLists.of(POLICY))
                .build();
        module = new PCModuleTestEnv(options, clock);
        pc = new ParallelEoSStreamProcessor<>(options, module);
        allocator = module.partitionShareAllocator().orElseThrow(
                () -> new AssertionError("the default strategy builds the partition-share allocator (F4)"));
    }

    static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

    static List<PartitionInfo> partitionsOf(String topic, int count) {
        List<PartitionInfo> infos = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            infos.add(new PartitionInfo(topic, i, null, null, null));
        }
        return infos;
    }

    /** The snapshot the NEXT quantum mints from - the one the most recent callback published (R4). */
    AssignmentSnapshot effectiveNextQuantum() {
        return allocator.effectiveAssignment(API_X, clock.instant().plus(QUANTUM));
    }

    double localRateNextQuantum() {
        return allocator.localRatePerSecond(memberId, API_X, clock.instant().plus(QUANTUM));
    }

    /** Moves into the next quantum and performs the control thread's per-pass pull there. */
    void nextQuantumAndPull() {
        clock.add(QUANTUM);
        allocator.readQuantum(memberId, clock.instant());
    }

    int creditsNow() {
        return allocator.currentLease(memberId, API_X, clock.instant())
                .map(lease -> lease.getAvailableCredits()).orElse(0);
    }

    List<ILoggingEvent> declineWarnings() {
        return processorLog.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .filter(event -> event.getFormattedMessage().contains("partition total for"))
                .filter(event -> event.getFormattedMessage().contains(memberId))
                .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------
    // R4, the eager protocol: revoke-all then assign - the gap mints nothing
    // ------------------------------------------------------------------

    @Test
    void eagerRevokeAllThenAssignPublishesNothingHeldThenTheNewSetWithARefreshedTotal() {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        pc.subscribe(UniLists.of(ORDERS));
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0), tp(ORDERS, 1)));
        assertThat(effectiveNextQuantum().getHeldPartitions()).containsExactly(tp(ORDERS, 0), tp(ORDERS, 1));
        assertThat(effectiveNextQuantum().getTotalPartitions()).isEqualTo(4);
        nextQuantumAndPull();
        assertWithMessage("2 of 4 at 4/sec: two credits").that(creditsNow()).isEqualTo(2);

        // the eager revoke-all: nothing held, the total kept (nothing changed it)
        pc.onPartitionsRevoked(UniLists.of(tp(ORDERS, 0), tp(ORDERS, 1)));
        AssignmentSnapshot afterRevoke = effectiveNextQuantum();
        assertThat(afterRevoke.getHeldPartitions()).isEmpty();
        assertThat(afterRevoke.isResolved()).isTrue();
        assertThat(afterRevoke.getTotalPartitions()).isEqualTo(4);

        // a quantum pull in the gap between the revoke and its paired assign mints NOTHING (undershoot, R4)
        nextQuantumAndPull();
        assertWithMessage("the eager gap mints nothing").that(creditsNow()).isEqualTo(0);
        assertThat(localRateNextQuantum()).isEqualTo(0.0);

        // the paired assign: the topic expanded meanwhile, and the assign's re-read sees it
        consumer.topics(ORDERS, 8);
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 5)));
        AssignmentSnapshot afterAssign = effectiveNextQuantum();
        assertThat(afterAssign.getHeldPartitions()).containsExactly(tp(ORDERS, 5));
        assertWithMessage("the assign refreshes the total").that(afterAssign.getTotalPartitions()).isEqualTo(8);
        assertThat(localRateNextQuantum()).isEqualTo(4.0 / 8);
    }

    // ------------------------------------------------------------------
    // R4, the cooperative protocol: an EMPTY assign after another member absorbed an expansion
    // ------------------------------------------------------------------

    @Test
    void cooperativeEmptyAssignRefreshesTheTotalAndShrinksTheFraction() {
        buildProcessor();
        consumer.topics(ORDERS, 2);
        pc.subscribe(UniLists.of(ORDERS));
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0)));
        assertThat(effectiveNextQuantum().fraction()).isEqualTo(0.5);

        // the topic grows to 4; the two new partitions went to another member, so this one's assign is empty
        consumer.topics(ORDERS, 4);
        pc.onPartitionsAssigned(Collections.emptyList());

        AssignmentSnapshot refreshed = effectiveNextQuantum();
        assertThat(refreshed.getHeldPartitions()).containsExactly(tp(ORDERS, 0));
        assertThat(refreshed.getTotalPartitions()).isEqualTo(4);
        assertWithMessage("the fraction shrinks with the refreshed total").that(refreshed.fraction()).isEqualTo(0.25);
    }

    // ------------------------------------------------------------------
    // KTD3: a declined read publishes UNRESOLVED - first, and again after a resolved state
    // ------------------------------------------------------------------

    @Test
    void aThrowingReadOnFirstAssignmentPublishesUnresolvedWarnsOnceAndTheNextAssignmentResolves() {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        consumer.partitionsForThrows(new TimeoutException("metadata not available within the timeout"));
        pc.subscribe(UniLists.of(ORDERS));

        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0), tp(ORDERS, 1)));
        AssignmentSnapshot unresolved = effectiveNextQuantum();
        assertThat(unresolved.isResolved()).isFalse();
        assertWithMessage("the numerator is still tracked while the denominator is unknown")
                .that(unresolved.getHeldPartitions()).containsExactly(tp(ORDERS, 0), tp(ORDERS, 1));
        nextQuantumAndPull();
        assertWithMessage("an unresolved total mints nothing (R5)").that(creditsNow()).isEqualTo(0);

        // a second decline in the same window: the warning is rate-limited to one
        pc.onPartitionsAssigned(Collections.emptyList());
        assertThat(declineWarnings()).hasSize(1);
        assertThat(declineWarnings().get(0).getFormattedMessage()).contains(ORDERS);
        assertThat(declineWarnings().get(0).getFormattedMessage()).contains("threw");

        // the next assignment resolves it, and minting starts at the next boundary
        consumer.partitionsForScript(null);
        pc.onPartitionsAssigned(Collections.emptyList());
        AssignmentSnapshot resolved = effectiveNextQuantum();
        assertThat(resolved.isResolved()).isTrue();
        assertThat(resolved.getTotalPartitions()).isEqualTo(4);
        assertThat(resolved.fraction()).isEqualTo(0.5);
        nextQuantumAndPull();
        assertThat(creditsNow()).isEqualTo(2);
    }

    @Test
    void aDeclineAfterAResolvedStatePublishesUnresolvedNeverThePreviousTotal() {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        pc.subscribe(UniLists.of(ORDERS));
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0)));
        assertThat(effectiveNextQuantum().isResolved()).isTrue();

        // the read declines at the very rebalance that could have changed the total
        consumer.partitionsForThrows(new TimeoutException("metadata not available within the timeout"));
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 1)));

        AssignmentSnapshot afterDecline = effectiveNextQuantum();
        assertWithMessage("a stale total at the rebalance that changed it is the one case that over-mints, so a " +
                "decline never keeps the previous total").that(afterDecline.isResolved()).isFalse();
        assertThat(afterDecline.getHeldPartitions()).containsExactly(tp(ORDERS, 0), tp(ORDERS, 1));
        nextQuantumAndPull();
        assertThat(creditsNow()).isEqualTo(0);
    }

    @Test
    void aRevokeKeepsTheTotalsTheLastAssignResolved() {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        pc.subscribe(UniLists.of(ORDERS));
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0), tp(ORDERS, 1), tp(ORDERS, 2)));

        // a revoke re-reads nothing - even a read that would now decline is never made
        consumer.partitionsForThrows(new TimeoutException("would decline, but a revoke does not read"));
        int readsBefore = consumer.partitionsForCalls;
        pc.onPartitionsRevoked(UniLists.of(tp(ORDERS, 2)));
        assertThat(consumer.partitionsForCalls).isEqualTo(readsBefore);

        AssignmentSnapshot afterRevoke = effectiveNextQuantum();
        assertThat(afterRevoke.isResolved()).isTrue();
        assertThat(afterRevoke.getTotalPartitions()).isEqualTo(4);
        assertThat(afterRevoke.getHeldPartitions()).containsExactly(tp(ORDERS, 0), tp(ORDERS, 1));
        assertThat(declineWarnings()).isEmpty();
    }

    /**
     * The publish precedes anything in the callback that can throw (the code review's finding, corroborated by
     * the independent cross-model pass): a commit that fails during a revoke still leaves the held set without
     * the revoked partitions, so the NEXT assign publishes held-plus over the right numerator. Before the fix
     * the publish sat after the commit, and a failed commit left this instance minting a share it no longer
     * owned - the one direction the fleet bound does not cover.
     */
    @Test
    void aRevokeWhoseCommitThrowsStillPublishesTheHeldSetWithoutTheRevokedPartitions() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(memberId)
                .resourceTags(UniLists.of(API_X))
                .resourceContracts(UniLists.of(POLICY))
                .build();
        module = new PCModuleTestEnv(options, clock);
        pc = new ParallelEoSStreamProcessor<String, String>(options, module) {
            @Override
            protected void commitOffsetsThatAreReady() {
                throw new IllegalStateException("commit refused (test)");
            }
        };
        allocator = module.partitionShareAllocator().orElseThrow(AssertionError::new);
        consumer.topics(ORDERS, 4);
        pc.subscribe(UniLists.of(ORDERS));
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0), tp(ORDERS, 1), tp(ORDERS, 2)));

        assertThrows(RuntimeException.class, () -> pc.onPartitionsRevoked(UniLists.of(tp(ORDERS, 2))));

        AssignmentSnapshot afterFailedRevoke = effectiveNextQuantum();
        assertWithMessage("the revoked partition left the held set although the commit threw")
                .that(afterFailedRevoke.getHeldPartitions()).containsExactly(tp(ORDERS, 0), tp(ORDERS, 1));

        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 3)));
        assertWithMessage("the next assign publishes held-plus over the corrected numerator")
                .that(effectiveNextQuantum().getHeldPartitions())
                .containsExactly(tp(ORDERS, 0), tp(ORDERS, 1), tp(ORDERS, 3));
    }

    @Test
    void aLossPublishesAsARevoke() {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        pc.subscribe(UniLists.of(ORDERS));
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0), tp(ORDERS, 1)));

        pc.onPartitionsLost(UniLists.of(tp(ORDERS, 0), tp(ORDERS, 1)));

        AssignmentSnapshot afterLoss = effectiveNextQuantum();
        assertThat(afterLoss.getHeldPartitions()).isEmpty();
        assertThat(afterLoss.isResolved()).isTrue();
        assertThat(localRateNextQuantum()).isEqualTo(0.0);
    }

    // ------------------------------------------------------------------
    // KTD3: empty and null results are declines too
    // ------------------------------------------------------------------

    @Test
    void anEmptyPartitionsForResultIsADecline() {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        consumer.partitionsForScript(topic -> Collections.emptyList());
        pc.subscribe(UniLists.of(ORDERS));

        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0)));

        assertThat(effectiveNextQuantum().isResolved()).isFalse();
        assertThat(declineWarnings()).hasSize(1);
        assertThat(declineWarnings().get(0).getFormattedMessage()).contains("no partitions");
    }

    @Test
    void aNullPartitionsForResultIsADecline() {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        consumer.partitionsForScript(topic -> null);
        pc.subscribe(UniLists.of(ORDERS));

        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0)));

        assertThat(effectiveNextQuantum().isResolved()).isFalse();
        assertThat(declineWarnings()).hasSize(1);
        assertThat(declineWarnings().get(0).getFormattedMessage()).contains("null");
    }

    /** A decline on the SECOND of two topics declines the whole read - a fraction over half a denominator is none. */
    @Test
    void aDeclineOnAnySubscribedTopicDeclinesTheWholeRead() {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        consumer.topics(PAYMENTS, 2);
        consumer.partitionsForScript(topic -> PAYMENTS.equals(topic) ? Collections.emptyList()
                : consumer.scriptedDefault(topic));
        pc.subscribe(UniLists.of(ORDERS, PAYMENTS));

        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0)));

        assertThat(effectiveNextQuantum().isResolved()).isFalse();
        assertThat(declineWarnings().get(0).getFormattedMessage()).contains(PAYMENTS);
    }

    // ------------------------------------------------------------------
    // KTD3: a pattern subscription - consumer.subscription() is the topic source
    // ------------------------------------------------------------------

    @Test
    void aPatternSubscriptionWhoseMatchSetGrowsCountsTheNewTopicAtTheNextAssign() {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        pc.subscribe(Pattern.compile("orders|payments"));
        consumer.subscriptionOverride(UniSets.of(ORDERS));
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0), tp(ORDERS, 1)));
        assertThat(effectiveNextQuantum().getTotalPartitions()).isEqualTo(4);
        assertThat(effectiveNextQuantum().fraction()).isEqualTo(0.5);

        // a new topic matches the pattern; the consumer's subscription now names both
        consumer.topics(PAYMENTS, 4);
        consumer.subscriptionOverride(UniSets.of(ORDERS, PAYMENTS));
        pc.onPartitionsAssigned(UniLists.of(tp(PAYMENTS, 3)));

        AssignmentSnapshot refreshed = effectiveNextQuantum();
        assertThat(refreshed.getPartitionsPerTopic()).containsExactly(ORDERS, 4, PAYMENTS, 4);
        assertThat(refreshed.getTotalPartitions()).isEqualTo(8);
        assertThat(refreshed.getHeldPartitions()).containsExactly(tp(ORDERS, 0), tp(ORDERS, 1), tp(PAYMENTS, 3));
        assertWithMessage("three of eight across both topics").that(refreshed.fraction()).isEqualTo(3.0 / 8);
    }

    /**
     * The torn case {@link AssignmentSnapshot#resolved} refuses: the totals name a topic the held set does not
     * belong to (the subscription was re-read before the assignment reached this member). Published as
     * unresolved with the same warning, never minted from.
     */
    @Test
    void aHeldPartitionOutsideTheResolvedTotalsIsPublishedUnresolved() {
        buildProcessor();
        consumer.topics(ORDERS, 2);
        pc.subscribe(UniLists.of(ORDERS));

        // metadata that predates the expansion: partition 3 is beyond a total of 2
        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 3)));

        AssignmentSnapshot torn = effectiveNextQuantum();
        assertThat(torn.isResolved()).isFalse();
        assertThat(torn.getHeldPartitions()).containsExactly(tp(ORDERS, 3));
        assertThat(declineWarnings()).hasSize(1);
    }

    // ------------------------------------------------------------------
    // Callback discipline: publish never waits on the control thread
    // ------------------------------------------------------------------

    /**
     * Another thread holds the allocator's control-thread monitor ({@code stateLock}, the one the per-pass
     * quantum read and every spend take) for the whole callback; the callback must still run to completion,
     * because its publish is an atomic append that never takes that monitor. The monitor is reached by
     * reflection - it is deliberately private, and this test exists precisely so that nothing on the callback
     * path can ever need it.
     */
    @Test
    void aCallbackCompletesWhileTheControlThreadMonitorIsHeld() throws Exception {
        buildProcessor();
        consumer.topics(ORDERS, 4);
        pc.subscribe(UniLists.of(ORDERS));
        Object stateLock = controlThreadMonitorOf(allocator);

        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            Future<?> holder = threads.submit(() -> {
                synchronized (stateLock) {
                    held.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();

            Future<?> callback = threads.submit(() -> {
                pc.onPartitionsRevoked(Collections.emptyList());
                pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0), tp(ORDERS, 1), tp(ORDERS, 2)));
                pc.onPartitionsLost(UniLists.of(tp(ORDERS, 2)));
            });
            // a callback that waited on the monitor would time out here, with the holder still parked
            callback.get(5, TimeUnit.SECONDS);
            assertWithMessage("the callbacks published while the monitor was held")
                    .that(effectiveNextQuantum().getHeldPartitions()).containsExactly(tp(ORDERS, 0), tp(ORDERS, 1));

            release.countDown();
            holder.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            threads.shutdownNow();
        }
    }

    private static Object controlThreadMonitorOf(PartitionShareResourceAllocator allocator) throws Exception {
        Field stateLock = PartitionShareResourceAllocator.class.getDeclaredField("stateLock");
        stateLock.setAccessible(true);
        return stateLock.get(allocator);
    }

    // ------------------------------------------------------------------
    // The untouched paths: no tags, and the in-process strategy - the callbacks publish nothing
    // ------------------------------------------------------------------

    @Test
    void anUntaggedInstanceBuildsNoAllocatorAndItsCallbacksReadNoMetadata() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .build();
        module = new PCModuleTestEnv(options, clock);
        pc = new ParallelEoSStreamProcessor<>(options, module);
        consumer.topics(ORDERS, 4);
        pc.subscribe(UniLists.of(ORDERS));

        pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 0)));
        pc.onPartitionsRevoked(UniLists.of(tp(ORDERS, 0)));

        assertThat(module.partitionShareAllocator().isPresent()).isFalse();
        assertThat(module.navigatorView().isActive()).isFalse();
        assertWithMessage("the untouched path makes no metadata read").that(consumer.partitionsForCalls).isEqualTo(0);
    }

    /**
     * A {@link MockConsumer} whose metadata answers are scripted: {@code partitionsFor(topic, timeout)} runs the
     * script when one is set (a throw, a null, an empty list, or a per-topic choice) and otherwise answers from
     * {@link #topics}; {@link #subscription()} can be overridden to simulate a pattern subscription's match set
     * growing, which the stock mock only recomputes at subscribe time.
     */
    static final class ScriptedMetadataConsumer extends MockConsumer<String, String> {

        private final Map<String, List<PartitionInfo>> known = new HashMap<>();
        private Function<String, List<PartitionInfo>> script;
        private Set<String> subscriptionOverride;
        int partitionsForCalls;

        ScriptedMetadataConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        void topics(String topic, int partitionCount) {
            known.put(topic, partitionsOf(topic, partitionCount));
            updatePartitions(topic, partitionsOf(topic, partitionCount));
        }

        void partitionsForScript(Function<String, List<PartitionInfo>> script) {
            this.script = script;
        }

        void partitionsForThrows(RuntimeException failure) {
            partitionsForScript(topic -> {
                throw failure;
            });
        }

        void subscriptionOverride(Set<String> topics) {
            this.subscriptionOverride = topics;
        }

        List<PartitionInfo> scriptedDefault(String topic) {
            return known.getOrDefault(topic, Collections.emptyList());
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
            partitionsForCalls++;
            assertWithMessage("the timed overload is the one the callback must use (KTD3)")
                    .that(timeout).isEqualTo(AbstractParallelEoSStreamProcessor.PARTITION_TOTAL_READ_TIMEOUT);
            return script != null ? script.apply(topic) : scriptedDefault(topic);
        }

        @Override
        public synchronized Set<String> subscription() {
            return subscriptionOverride != null ? new HashSet<>(subscriptionOverride) : super.subscription();
        }
    }
}
