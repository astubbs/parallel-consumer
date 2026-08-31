package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.navigator.ConservationLedger;
import bz.stub.parallelconsumer.internal.navigator.ResourceAllocator;
import bz.stub.parallelconsumer.internal.navigator.ResourceContract;
import bz.stub.parallelconsumer.internal.navigator.StubResourceAllocator;
import bz.stub.parallelconsumer.state.WorkContainer.ExecutionState;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The navigator's selection-path integration (the plan's U3, KTD1): the resource-eligibility term is a PURE
 * read inside the single-claim evaluation, the spend lands only after the claim CAS wins, a lost race spends
 * nothing, and revocation touches no credit state (KTD10). Untagged instances take the zero-cost path (R3).
 * <p>
 * Drives the REAL {@link WorkManager} selection machinery on the virtual clock, the
 * {@link WorkClaimStateMachineTest} pattern - and per the one-canonical-clock rule (KTD4) the
 * {@link StubResourceAllocator} is constructed with the SAME {@link MutableClock} the module serves, so quantum
 * boundaries and the engine's {@code availableAt} comparisons advance together.
 * <p>
 * The honest not-invoked-before-{@code availableAt} half of AE5 - the user function genuinely not running -
 * lives with the lifecycle wiring in {@code NavigatorEngineLifecycleTest}, where a real control loop dispatches;
 * what is pinned here is the claim-level mechanics that make it true.
 *
 * @author Antony Stubbs
 * @see bz.stub.parallelconsumer.internal.navigator.NavigatorParticipant
 * @see WorkContainer#onQueueingForExecution()
 */
@Slf4j
class NavigatorSelectionTest {

    static final String TOPIC = "navigator-selection-topic";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    static final String API_A = "api-a";
    static final String API_B = "api-b";

    /** Supplied as {@code pcInstanceTag}, so the member id the allocator sees is deterministic. */
    static final String MEMBER = "navigator-member-under-test";

    static final Duration ONE_SECOND = Duration.ofSeconds(1);

    MutableClock clock;
    StubResourceAllocator allocator;
    PCModuleTestEnv module;
    WorkManager<String, String> wm;

    /**
     * One member, one resource at {@code ratePerSecond} on a one-second quantum, membership already effective
     * and the current quantum already pulled - so the tests start from "this instance holds this quantum's
     * credits", which is the state the control loop's per-pass {@code readQuantum} (KTD4) establishes.
     */
    void setupTagged(double ratePerSecond, ResourceContract... extraContracts) {
        clock = MutableClock.epochUTC();
        allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(API_A, ratePerSecond, 1, ONE_SECOND));
        List<String> tags = new ArrayList<>();
        tags.add(API_A);
        for (ResourceContract contract : extraContracts) {
            allocator.register(contract);
            tags.add(contract.getName());
        }
        buildModule(tags, allocator);
        assertWithMessage("fixture: the participant's member id must be the supplied pcInstanceTag, or the "
                + "test would be driving the allocator as a different member than the engine spends as")
                .that(module.navigatorParticipant().memberId()).isEqualTo(MEMBER);
        // membership counts from the NEXT quantum after the join (R16), so join at epoch and step into quantum 1
        allocator.join(MEMBER, clock.instant());
        clock.add(ONE_SECOND);
        allocator.readQuantum(MEMBER, clock.instant());
    }

    void buildModule(List<String> tags, ResourceAllocator allocatorForOptions) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceTags(tags)
                .resourceAllocator(allocatorForOptions)
                .build();
        module = clock == null ? new PCModuleTestEnv(options) : new PCModuleTestEnv(options, clock);
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
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

    WorkContainer<String, String> containerInShardAt(long offset) {
        var sm = wm.getSm();
        var shard = sm.getShard(sm.computeShardKey(new ConsumerRecord<>(TOPIC, 0, offset, "key-" + offset, "v")));
        assertWithMessage("the shard for offset %s must exist for this test to prove anything", offset)
                .that(shard.isPresent()).isTrue();
        var wc = shard.get().getWorkContainerAt(offset);
        assertWithMessage("the shard must still hold offset %s", offset).that(wc).isNotNull();
        return wc;
    }

    ConservationLedger ledger(String resource) {
        return allocator.conservationLedger(resource, clock.instant());
    }

    void assertIdentityCloses(String resource) {
        ConservationLedger snapshot = ledger(resource);
        assertWithMessage("conservation identity minted + overdraft == spent + expired + outstanding (%s)", snapshot)
                .that(snapshot.getOutstanding()).isEqualTo(snapshot.getLiveCredits());
    }

    // -----------------------------------------------------------------------------------------------------
    // Spend-at-claim (KTD1): one credit, two eligible records
    // -----------------------------------------------------------------------------------------------------

    /**
     * One credit, two eligible records: exactly one dispatches and exactly one credit is spent; the loser
     * spent NOTHING and is deferred with {@code availableAt} = the next credit time. The single-threaded
     * selection engine's strict one-credit-one-dispatch case (KTD1/KD10).
     */
    @Test
    void oneCreditTwoRecordsExactlyOneDispatchesAndTheLoserSpendsNothing() {
        setupTagged(1.0);
        register(0, 2);

        var taken = wm.getWorkIfAvailable(2);

        assertWithMessage("one credit must admit exactly one of the two eligible records")
                .that(taken).hasSize(1);
        ConservationLedger snapshot = ledger(API_A);
        assertWithMessage("exactly one credit spent - the loser must not have spent")
                .that(snapshot.getSpent()).isEqualTo(1);
        assertThat(snapshot.getOverdraft()).isEqualTo(0);
        assertIdentityCloses(API_A);

        var loser = containerInShardAt(1L);
        assertWithMessage("the losing record must still be claimable-in-principle, just resource-deferred")
                .that(loser.getExecutionState()).isEqualTo(ExecutionState.AVAILABLE);
        assertWithMessage("the deferral names its lift time: the next credit at the next quantum boundary (R7)")
                .that(loser.resourceAvailableAt()).hasValue(Instant.ofEpochSecond(2));

        // the lease still EXISTS, with zero credits - eligibility is "lease present AND credits > 0" (KTD1)
        var drained = allocator.currentLease(MEMBER, API_A, clock.instant());
        assertWithMessage("fixture: the drained lease must still exist, pinning the zero-credit-lease case")
                .that(drained.isPresent()).isTrue();
        assertThat(drained.get().getAvailableCredits()).isEqualTo(0);
        assertWithMessage("a re-ask with the lease drained must dispatch nothing")
                .that(wm.getWorkIfAvailable(2)).isEmpty();
    }

    /**
     * AE5's claim-level mechanics on the virtual clock: the deferred record stays unselectable while the clock
     * sits before its {@code availableAt}, and becomes dispatchable once the clock passes it and the per-pass
     * quantum read has minted (KTD4 - the read is the control loop's job, driven by hand here).
     */
    @Test
    void advancingTheClockPastAvailableAtMakesTheDeferredRecordDispatchable() {
        setupTagged(1.0);
        register(0, 2);
        assertThat(wm.getWorkIfAvailable(2)).hasSize(1);
        var deferred = containerInShardAt(1L);
        Instant availableAt = deferred.resourceAvailableAt().get();

        // just before availableAt: still deferred, even with the quantum read renewed
        clock.add(Duration.between(clock.instant(), availableAt).minusMillis(1));
        allocator.readQuantum(MEMBER, clock.instant());
        assertWithMessage("one millisecond before availableAt the record must still be withheld")
                .that(wm.getWorkIfAvailable(2)).isEmpty();

        // at availableAt: the next quantum's credit exists once pulled, and the record dispatches
        clock.add(Duration.ofMillis(1));
        allocator.readQuantum(MEMBER, clock.instant());
        var taken = wm.getWorkIfAvailable(2);
        assertThat(taken).hasSize(1);
        assertThat(taken.get(0)).isSameInstanceAs(deferred);
        assertThat(ledger(API_A).getSpent()).isEqualTo(2);
        assertIdentityCloses(API_A);
    }

    // -----------------------------------------------------------------------------------------------------
    // No check-then-act window (KTD1): the eligibility decision is re-evaluated inside the claim
    // -----------------------------------------------------------------------------------------------------

    /**
     * The {@link WorkClaimStateMachineTest} interleaving, replayed against the resource term: a scanner
     * observes a record eligible (credit present), is descheduled, the last credit is spent by another
     * dispatch, and the scanner's claim then runs. The claim must re-evaluate the resource term against the
     * NOW state and refuse - eligibility is part of the single-claim evaluation, never a cached pre-filter,
     * so there is no window between the check and the spend for a decision to outlive its facts.
     */
    @Test
    void aClaimDecidedWhileACreditExistedIsRefusedOnceThatCreditIsSpent() {
        setupTagged(1.0);
        register(0, 2);

        // 1. the scanner observes offset 1 eligible - the one credit is still unspent
        var heldByScanner = containerInShardAt(1L);
        assertWithMessage("the scanner must have seen it available, or the interleaving never starts")
                .that(heldByScanner.isAvailableToTakeAsWork()).isTrue();

        // 2. another dispatch takes offset 0 and spends the only credit
        var taken = wm.getWorkIfAvailable(1);
        assertThat(taken).hasSize(1);
        assertThat(taken.get(0).offset()).isEqualTo(0L);
        assertThat(ledger(API_A).getSpent()).isEqualTo(1);

        // 3. the scanner resumes and attempts the claim it decided on in step 1
        boolean wonTheClaim = heldByScanner.onQueueingForExecution();

        assertWithMessage("a claim whose eligibility decision predates the credit being spent must be REFUSED "
                + "- winning it would dispatch without capacity, the two-step defect restated for resources")
                .that(wonTheClaim).isFalse();
        assertWithMessage("the refused claim must have spent nothing")
                .that(ledger(API_A).getSpent()).isEqualTo(1);
        assertThat(ledger(API_A).getOverdraft()).isEqualTo(0);
        assertThat(heldByScanner.getDeliveryCount()).isEqualTo(0L);
        assertIdentityCloses(API_A);
    }

    // -----------------------------------------------------------------------------------------------------
    // Multi-resource (R7): availableAt is the LATEST blocking next-credit time; dispatch spends from each
    // -----------------------------------------------------------------------------------------------------

    /**
     * Two resources on DIFFERENT quanta, so their next-credit times genuinely differ and the max path is
     * driven, not trivially satisfied: api-a mints every second, api-b every two seconds. Both blocked →
     * {@code availableAt} is the LATER time; only api-b blocked → still the later time (the blocked one's);
     * and the dispatch that finally goes spends ONE credit from EACH.
     */
    @Test
    void multiResourceDeferralUsesTheLatestNextCreditTimeAndDispatchSpendsFromEach() {
        var apiB = new ResourceContract(API_B, 0.5, 1, Duration.ofSeconds(2));
        clock = MutableClock.epochUTC();
        allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(API_A, 1.0, 1, ONE_SECOND));
        allocator.register(apiB);
        buildModule(UniLists.of(API_A, API_B), allocator);
        allocator.join(MEMBER, clock.instant());
        // step to t=2s so the member is live for BOTH resources' current quanta (a-quantum 2, b-quantum 1)
        clock.add(Duration.ofSeconds(2));
        allocator.readQuantum(MEMBER, clock.instant());
        register(0, 2);

        // one dispatch spends one credit from EACH resource
        var taken = wm.getWorkIfAvailable(2);
        assertWithMessage("one credit per resource admits exactly one record").that(taken).hasSize(1);
        assertThat(ledger(API_A).getSpent()).isEqualTo(1);
        assertThat(ledger(API_B).getSpent()).isEqualTo(1);

        // BOTH blocked, with different next-credit times: a → 3s, b → 4s. The max must win (R7).
        var deferred = containerInShardAt(1L);
        assertWithMessage("with both resources blocked, availableAt must be the LATER next-credit time")
                .that(deferred.resourceAvailableAt()).hasValue(Instant.ofEpochSecond(4));

        // a's next quantum arrives; b is still blocked - availableAt stays the blocked one's time
        clock.add(ONE_SECOND);
        allocator.readQuantum(MEMBER, clock.instant());
        assertWithMessage("with only api-b blocked the record must still be withheld")
                .that(wm.getWorkIfAvailable(2)).isEmpty();
        assertWithMessage("with one resource blocked, availableAt is that resource's next-credit time")
                .that(deferred.resourceAvailableAt()).hasValue(Instant.ofEpochSecond(4));

        // b's quantum arrives: dispatch, and both credits are spent
        clock.add(ONE_SECOND);
        allocator.readQuantum(MEMBER, clock.instant());
        var second = wm.getWorkIfAvailable(2);
        assertThat(second).hasSize(1);
        assertThat(second.get(0)).isSameInstanceAs(deferred);
        assertThat(ledger(API_A).getSpent()).isEqualTo(2);
        assertThat(ledger(API_B).getSpent()).isEqualTo(2);
        assertIdentityCloses(API_A);
        assertIdentityCloses(API_B);
    }

    // -----------------------------------------------------------------------------------------------------
    // KTD10: revocation is a credit no-op
    // -----------------------------------------------------------------------------------------------------

    /**
     * A deferred record never spent a credit (KTD1), so revoking its partition must refund nothing and touch
     * no ledger state - byte-for-byte identical conservation counters before and after (KTD10, KD10's
     * never-re-minted rule).
     */
    @Test
    void revokingADeferredRecordsPartitionChangesNoConservationCounter() {
        setupTagged(1.0);
        register(0, 2);
        assertThat(wm.getWorkIfAvailable(2)).hasSize(1);
        assertWithMessage("fixture: a resource-deferred record must exist for revocation to be interesting")
                .that(containerInShardAt(1L).resourceAvailableAt().isPresent()).isTrue();

        ConservationLedger before = ledger(API_A);
        wm.onPartitionsRevoked(UniLists.of(TP));
        ConservationLedger after = ledger(API_A);

        assertWithMessage("revocation must be a credit no-op: no refund, no expiry, no debit (KTD10)")
                .that(after).isEqualTo(before);
        assertIdentityCloses(API_A);
    }

    // -----------------------------------------------------------------------------------------------------
    // R3/AE4: the untagged instance's zero-cost path
    // -----------------------------------------------------------------------------------------------------

    /**
     * An untagged instance's records dispatch with ZERO navigator interaction - proven with a Mockito mock
     * allocator that would record any call: registration, lookup, lease reads, spends, anything. The
     * allocator being present in the options but unreferenced is deliberate: it pins that the zero-cost path
     * is decided by the TAGS, once per instance, not by the allocator's absence (R3).
     */
    @Test
    void untaggedInstanceRecordsDispatchWithZeroNavigatorInteraction() {
        ResourceAllocator untouchedAllocator = Mockito.mock(ResourceAllocator.class);
        clock = MutableClock.epochUTC();
        buildModule(UniLists.of(), untouchedAllocator);
        assertWithMessage("an untagged instance's participant must be inert (R3)")
                .that(module.navigatorParticipant().isActive()).isFalse();
        register(0, 3);

        var taken = wm.getWorkIfAvailable(3);
        assertWithMessage("untagged admission must behave exactly as today - everything dispatches")
                .that(taken).hasSize(3);
        for (var wc : taken) {
            assertThat(wc.resourceAvailableAt().isPresent()).isFalse();
            wc.onUserFunctionSuccess();
            wm.handleFutureResult(wc);
        }

        Mockito.verifyNoInteractions(untouchedAllocator);
    }
}
