package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.ShardKey;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy.IN_PROCESS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy.CUSTOM;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The context-object query surface (U5, R18, KTD9): {@link NavigatorView} answers "what is resource-ineligible
 * per ordering shard, for which resource, their {@code availableAt}, and what rate is available" - and reading
 * it is observably inert (AE6). Drives the REAL selection machinery the way {@code NavigatorAttributionTest}
 * does - a {@link PCModuleTestEnv} sharing one {@link MutableClock} with the {@link StubResourceAllocator} - so
 * the per-shard counts asserted here are the ones the engine's own defer/undefer transitions maintained, not
 * hand-fed fixtures. The full-loop plumbing test at the bottom proves the construction sites hand the module's
 * view to the user function's context.
 *
 * @author Antony Stubbs
 * @see NavigatorView
 * @see bz.stub.parallelconsumer.state.NavigatorAttributionTest the episode transitions this view reads
 */
class NavigatorViewTest {

    static final String TOPIC = "navigator-view-topic";
    static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

    static final String API_A = "api-a";
    static final String MEMBER = "navigator-view-member";

    static final Duration ONE_SECOND = Duration.ofSeconds(1);

    MutableClock clock;
    StubResourceAllocator allocator;
    PCModuleTestEnv module;
    WorkManager<String, String> wm;

    /**
     * A tagged env over {@code partitions}, with API_A at 1 credit/sec. The member JOINS but does not read a
     * quantum, so callers choose whether any credit exists: none until they advance the clock past a quantum
     * boundary and {@code readQuantum}.
     */
    void setupTagged(TopicPartition... partitions) {
        clock = MutableClock.epochUTC();
        allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(API_A, 1.0, 1, ONE_SECOND));
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceTags(UniLists.of(API_A))
                .allocationStrategy(IN_PROCESS)
                .resourceAllocator(allocator)
                .build();
        module = new PCModuleTestEnv(options, clock);
        wm = module.workManager();
        wm.onPartitionsAssigned(Arrays.asList(partitions));
        allocator.join(MEMBER, clock.instant());
    }

    void mintOneCredit() {
        clock.add(ONE_SECOND);
        allocator.readQuantum(MEMBER, clock.instant());
    }

    void register(TopicPartition tp, int fromOffset, int count) {
        List<ConsumerRecord<String, String>> recs = new ArrayList<>(count);
        for (int i = fromOffset; i < fromOffset + count; i++) {
            recs.add(new ConsumerRecord<>(tp.topic(), tp.partition(), i, "key-" + i, "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(tp, recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));
    }

    static ShardKey shardKeyOf(TopicPartition tp) {
        return ShardKey.of(new ConsumerRecord<>(tp.topic(), tp.partition(), 0, "ignored", "ignored"),
                ParallelConsumerOptions.ProcessingOrder.UNORDERED);
    }

    // -----------------------------------------------------------------------------------------------------
    // AE6: the untagged instance - empty counts, unconstrained rates, and reading registers NOTHING
    // -----------------------------------------------------------------------------------------------------

    @Test
    void untaggedInstanceViewIsEmptyUnconstrainedAndObservablyInert() {
        ResourceAllocator untouchedAllocator = Mockito.mock(ResourceAllocator.class);
        clock = MutableClock.epochUTC();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .allocationStrategy(CUSTOM)
                .resourceAllocator(untouchedAllocator)
                .build();
        module = new PCModuleTestEnv(options, clock);

        NavigatorView view = module.navigatorView();

        // repeated reads, deliberately - AE6's "nothing registered or allocated by the act of reading"
        for (int read = 0; read < 3; read++) {
            assertThat(view.isActive()).isFalse();
            assertThat(view.resourceTags()).isEmpty();
            assertThat(view.resourceIneligibleCount()).isEqualTo(0);
            assertThat(view.resourceIneligibleCountByShard()).isEmpty();
            assertThat(view.blockingResourceDeferrals()).isEmpty();
            assertThat(view.localRatePerSecond(API_A).isPresent()).isFalse();
            assertThat(view.globalRatePerSecond(API_A).isPresent()).isFalse();
            assertThat(view.localRatePerSecond(null).isPresent()).isFalse();
            assertThat(view.shareFraction(API_A).isPresent()).isFalse();
            assertThat(view.creditsPerQuantum(API_A).isPresent()).isFalse();
        }
        // AE6: an untagged instance's view must never touch the allocator
        Mockito.verifyNoInteractions(untouchedAllocator);
    }

    // -----------------------------------------------------------------------------------------------------
    // R9 / AE3 (partition-share): the share reads - fraction and credits per quantum, from the two rate reads
    // -----------------------------------------------------------------------------------------------------

    /**
     * Under the DEFAULT strategy the engine builds the partition-share allocator; a holder of three of the
     * subscription's four partitions reads a three-quarter share, worth three credits per quantum at 4/sec on a
     * one-second quantum - and the pair derives from {@code localRatePerSecond / globalRatePerSecond} alone.
     */
    @Test
    void partitionShareViewReportsTheFractionAndCreditsPerQuantum() {
        clock = MutableClock.epochUTC();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceTags(UniLists.of(API_A))
                .resourceContracts(UniLists.of(new ResourceContract(API_A, 4.0, 4, ONE_SECOND)))
                .build();
        module = new PCModuleTestEnv(options, clock);
        PartitionShareResourceAllocator partitionShare = module.partitionShareAllocator().orElseThrow(
                () -> new AssertionError("the default strategy builds the partition-share allocator"));
        NavigatorView view = module.navigatorView();

        // before any assignment: tagged, so constrained - and the share is a real zero (R5), not empty
        assertThat(view.shareFraction(API_A).getAsDouble()).isEqualTo(0.0);
        assertThat(view.creditsPerQuantum(API_A).getAsDouble()).isEqualTo(0.0);

        Map<String, Integer> totals = new HashMap<>();
        totals.put(TOPIC, 4);
        partitionShare.publish(AssignmentSnapshot.resolved(
                new HashSet<>(Arrays.asList(TP0, TP1, new TopicPartition(TOPIC, 2))), totals),
                clock.instant());
        clock.add(ONE_SECOND); // the publication is effective from the next quantum (R4)

        assertThat(view.shareFraction(API_A).getAsDouble()).isEqualTo(0.75);
        assertThat(view.creditsPerQuantum(API_A).getAsDouble()).isEqualTo(3.0);
        assertThat(view.localRatePerSecond(API_A).getAsDouble()).isEqualTo(3.0);
        assertThat(view.globalRatePerSecond(API_A).getAsDouble()).isEqualTo(4.0);
        // an un-tagged name stays unconstrained for the share reads too (AE6)
        assertThat(view.shareFraction("api-nobody-registered").isPresent()).isFalse();
        assertThat(view.creditsPerQuantum(null).isPresent()).isFalse();
    }

    /** The inert singleton answers the share reads with the unconstrained shape, like every other read. */
    @Test
    void inertViewShareReadsAreEmpty() {
        NavigatorView inert = NavigatorView.inert();

        assertThat(inert.shareFraction(API_A).isPresent()).isFalse();
        assertThat(inert.creditsPerQuantum(API_A).isPresent()).isFalse();
    }

    // -----------------------------------------------------------------------------------------------------
    // AE6: the tagged-but-idle instance - empty counts, real rates, and reads move no conservation counter
    // -----------------------------------------------------------------------------------------------------

    @Test
    void taggedIdleInstanceViewHasEmptyCountsAndReadsMoveNoConservationCounter() {
        setupTagged(TP0);
        mintOneCredit(); // live member, credit in hand, nothing deferred
        NavigatorView view = module.navigatorView();
        ConservationLedger before = allocator.conservationLedger(API_A, clock.instant());

        for (int read = 0; read < 5; read++) {
            assertThat(view.isActive()).isTrue();
            assertThat(view.resourceTags()).containsExactly(API_A);
            assertThat(view.resourceIneligibleCount()).isEqualTo(0);
            assertThat(view.resourceIneligibleCountByShard()).isEmpty();
            assertThat(view.blockingResourceDeferrals()).isEmpty();
            // the rates are REAL for a tagged resource (R18) - sole member of a 1/sec resource
            assertThat(view.globalRatePerSecond(API_A).getAsDouble()).isEqualTo(1.0);
            assertThat(view.localRatePerSecond(API_A).getAsDouble()).isEqualTo(1.0);
            // an un-tagged name stays unconstrained, never an exception (AE6)
            assertThat(view.globalRatePerSecond("api-nobody-registered").isPresent()).isFalse();
        }

        assertWithMessage("view reads must move no conservation counter (AE6's side-effect-free contract)")
                .that(allocator.conservationLedger(API_A, clock.instant())).isEqualTo(before);
    }

    @Test
    void aFinishedDeferralEpisodeLeavesNoStaleStateInTheView() {
        setupTagged(TP0);
        register(TP0, 0, 1);
        NavigatorView view = module.navigatorView();

        // no credit yet (joined, never read a quantum) - the one record defers
        assertThat(wm.getWorkIfAvailable(1)).isEmpty();
        assertThat(view.resourceIneligibleCount()).isEqualTo(1);
        assertThat(view.resourceIneligibleCountByShard()).containsExactly(shardKeyOf(TP0), 1L);

        // the credit arrives and the record dispatches - the episode ends
        mintOneCredit();
        assertThat(wm.getWorkIfAvailable(1)).hasSize(1);

        assertWithMessage("a tagged instance with nothing currently deferred must read exactly like idle - "
                + "no stale or leftover per-shard state (AE6)")
                .that(view.resourceIneligibleCountByShard()).isEmpty();
        assertThat(view.resourceIneligibleCount()).isEqualTo(0);

        // blockingResourceDeferrals is a LIVE resource-level read: the dispatch just spent the quantum's only
        // credit, so api-a legitimately reads as blocking again until the next credit - once one exists, the
        // list is empty too, with nothing left over from the finished episode
        mintOneCredit();
        assertThat(view.blockingResourceDeferrals()).isEmpty();
    }

    // -----------------------------------------------------------------------------------------------------
    // AE5/AE1: deferred records - count per shard, the resource, and availableAt consistent with the allocator
    // -----------------------------------------------------------------------------------------------------

    @Test
    void deferredRecordsReportCountPerShardResourceAndAvailableAtConsistentWithTheAllocator() {
        setupTagged(TP0, TP1);
        register(TP0, 0, 2);
        register(TP1, 0, 1);
        NavigatorView view = module.navigatorView();

        // no credit exists - all three records defer, split 2/1 across the two ordering shards
        assertThat(wm.getWorkIfAvailable(3)).isEmpty();

        assertThat(view.resourceIneligibleCount()).isEqualTo(3);
        assertThat(view.resourceIneligibleCountByShard())
                .containsExactly(shardKeyOf(TP0), 2L, shardKeyOf(TP1), 1L);

        List<ResourceDeferral> blocking = view.blockingResourceDeferrals();
        assertThat(blocking).hasSize(1);
        assertThat(blocking.get(0).getResourceName()).isEqualTo(API_A);
        Instant allocatorsNextCredit = allocator.nextCreditAt(MEMBER, API_A, clock.instant()).get();
        assertWithMessage("the view's availableAt must be the allocator's own next-credit time")
                .that(blocking.get(0).getNextCreditAt().get()).isEqualTo(allocatorsNextCredit);
    }

    @Test
    void dispatchingOneShardsRecordDecrementsOnlyThatShardsCount() {
        setupTagged(TP0, TP1);
        register(TP0, 0, 1);
        register(TP1, 0, 1);
        NavigatorView view = module.navigatorView();

        assertThat(wm.getWorkIfAvailable(2)).isEmpty(); // both defer - no credit yet
        assertThat(view.resourceIneligibleCountByShard())
                .containsExactly(shardKeyOf(TP0), 1L, shardKeyOf(TP1), 1L);

        mintOneCredit();
        List<WorkContainer<String, String>> taken = wm.getWorkIfAvailable(1);
        assertThat(taken).hasSize(1);
        TopicPartition dispatchedFrom = taken.get(0).getTopicPartition();
        TopicPartition stillDeferred = dispatchedFrom.equals(TP0) ? TP1 : TP0;

        Map<ShardKey, Long> after = view.resourceIneligibleCountByShard();
        assertWithMessage("only the dispatched record's shard entry may move - and to REMOVAL, not zero")
                .that(after).containsExactly(shardKeyOf(stillDeferred), 1L);
        assertThat(view.resourceIneligibleCount()).isEqualTo(1);
    }

    // -----------------------------------------------------------------------------------------------------
    // Thread-safety: reads from another thread during controller mutation - weakly consistent, never throwing
    // -----------------------------------------------------------------------------------------------------

    @Test
    @Timeout(30)
    void viewReadsFromAnotherThreadDuringControllerMutationNeverThrow() throws InterruptedException {
        setupTagged(TP0, TP1);
        register(TP0, 0, 20);
        register(TP1, 0, 20);
        NavigatorView view = module.navigatorView();

        AtomicBoolean writerDone = new AtomicBoolean(false);
        AtomicReference<Throwable> readerFailure = new AtomicReference<>();
        CountDownLatch readerStarted = new CountDownLatch(1);
        Thread reader = new Thread(() -> {
            readerStarted.countDown();
            try {
                while (!writerDone.get()) {
                    // every read the view offers, continuously, against live mutation
                    view.resourceIneligibleCountByShard().values().forEach(count -> assertThat(count).isNotNull());
                    view.resourceIneligibleCount();
                    view.blockingResourceDeferrals();
                    view.localRatePerSecond(API_A);
                    view.globalRatePerSecond(API_A);
                }
            } catch (Throwable t) {
                readerFailure.set(t);
            }
        }, "navigator-view-reader");
        reader.start();
        assertThat(readerStarted.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        // the controller thread's life: defer everything, mint, dispatch, repeat - every pass moves the
        // per-shard map through insert, decrement and removal while the reader snapshots it
        for (int quantum = 0; quantum < 40; quantum++) {
            wm.getWorkIfAvailable(5);
            mintOneCredit();
        }
        writerDone.set(true);
        reader.join(Duration.ofSeconds(10).toMillis());

        assertThat(reader.isAlive()).isFalse();
        assertWithMessage("a concurrent view read must see a consistent snapshot or a weakly-consistent view, "
                + "never an exception").that(readerFailure.get()).isNull();
    }

    // -----------------------------------------------------------------------------------------------------
    // U5 plumbing: the context carries the view - directly, and through a REAL control loop
    // -----------------------------------------------------------------------------------------------------

    @Test
    void pollContextCarriesTheViewItWasConstructedWithAndDefaultsToInert() {
        setupTagged(TP0);
        NavigatorView engineView = module.navigatorView();

        var withView = new PollContextInternal<String, String>(Collections.emptyList(), engineView);
        assertWithMessage("the context must hand back the engine's own view instance")
                .that(withView.getPollContext().getNavigatorView()).isSameInstanceAs(engineView);

        var withoutView = new PollContextInternal<String, String>(Collections.emptyList());
        NavigatorView defaulted = withoutView.getPollContext().getNavigatorView();
        assertWithMessage("the view-less constructor must yield the inert view, never null (AE6)")
                .that(defaulted).isNotNull();
        assertThat(defaulted.isActive()).isFalse();
    }

    /**
     * The end-to-end half (the construction sites in {@code AbstractParallelEoSStreamProcessor}): a REAL
     * control loop dispatches a record, and the user function's own context answers navigator queries with THIS
     * instance's live view - active, correctly tagged, rates present. The {@code NavigatorEngineLifecycleTest}
     * wiring: virtual credit clock shared between allocator and module, wall-clock control loop.
     */
    @Test
    @Timeout(60)
    void userFunctionReceivesAContextWhoseViewAnswersDuringProcessing() {
        clock = MutableClock.epochUTC();
        allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(API_A, 1.0, 1, ONE_SECOND));
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceTags(UniLists.of(API_A))
                .allocationStrategy(IN_PROCESS)
                .resourceAllocator(allocator)
                .build();
        module = new PCModuleTestEnv(options, clock);
        pc = new ParallelEoSStreamProcessor<>(options, module);

        pc.subscribe(UniLists.of(TOPIC));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TP0, 0L));
        mockConsumer.rebalance(Collections.singletonList(TP0));
        pc.onPartitionsAssigned(UniLists.of(TP0));

        AtomicReference<NavigatorView> viewSeenByUserFunction = new AtomicReference<>();
        AtomicReference<Double> globalRateSeen = new AtomicReference<>();
        pc.poll(context -> {
            viewSeenByUserFunction.set(context.getNavigatorView());
            context.getNavigatorView().globalRatePerSecond(API_A)
                    .ifPresent(rate -> globalRateSeen.set(rate));
        });
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "key-0", "value-0"));

        clock.add(ONE_SECOND); // quantum 1 - the loop's own readQuantum mints the credit and dispatches
        Awaitility.await("the user function runs and captures its context's view")
                .atMost(Duration.ofSeconds(10)).until(() -> viewSeenByUserFunction.get() != null);

        NavigatorView seen = viewSeenByUserFunction.get();
        assertWithMessage("the construction sites must pass the module's LIVE view, not the inert default")
                .that(seen.isActive()).isTrue();
        assertThat(seen.resourceTags()).containsExactly(API_A);
        assertWithMessage("the view must answer rate queries during processing (R18)")
                .that(globalRateSeen.get()).isEqualTo(1.0);
    }

    ParallelEoSStreamProcessor<String, String> pc;

    @AfterEach
    void tearDown() {
        Awaitility.reset();
        if (pc != null) {
            pc.close();
        }
    }
}
