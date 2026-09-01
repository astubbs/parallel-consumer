package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.navigator.CapacityLease;
import bz.stub.parallelconsumer.navigator.ConservationLedger;
import bz.stub.parallelconsumer.navigator.ResourceAllocator;
import bz.stub.parallelconsumer.navigator.ResourceContract;
import bz.stub.parallelconsumer.navigator.StubResourceAllocator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link NavigatorParticipant} - the engine's per-instance navigator seam (U3): the inert shape's guaranteed
 * no-ops (R3), the eligibility definition (lease present AND credits &gt; 0, KTD1), the max-vs-min split
 * between the per-record {@code availableAt} (R7) and the wakeup bound (KTD5), and the always-succeeds debit
 * landing as overdraft when the observed credit is gone (KD10) - the engine-seam half of the quantum-boundary
 * debit; the allocator-level half lives in
 * {@link StubResourceAllocatorMintingTest#quantumBoundaryDebitLandsAsOverdraftAndTheIdentityHolds()}.
 */
class NavigatorParticipantTest {

    private static final String API_A = "api-a";
    private static final String API_B = "api-b";
    private static final String MEMBER = "participant-member";

    private final MutableClock clock = MutableClock.epochUTC();

    private final StubResourceAllocator allocator = new StubResourceAllocator(clock);

    private Instant now() {
        return clock.instant();
    }

    // ------------------------------------------------------------------
    // The inert shape (R3)
    // ------------------------------------------------------------------

    /**
     * The untagged path's whole contract: inactive, always eligible, no times, and every mutating call a
     * guaranteed no-op - proven against a mock allocator... except the inert participant HOLDS no allocator,
     * so the proof is that none of these calls can throw and none can reach one.
     */
    @Test
    void inertParticipantIsANoOpEverywhere() {
        var inert = NavigatorParticipant.inert();

        assertThat(inert.isActive()).isFalse();
        assertThat(inert.resourceTags()).isEmpty();
        assertThat(inert.memberId()).isNull();
        assertWithMessage("an untagged instance is always eligible - the claim's resource term must not gate it")
                .that(inert.hasSpendableCreditForAllTags(now())).isTrue();
        assertThat(inert.availableAt(now()).isPresent()).isFalse();
        assertThat(inert.earliestBlockedResourceNextCreditAt(now()).isPresent()).isFalse();
        // and the mutating seams are no-ops rather than NPEs
        inert.spendOneCreditPerTag(now());
        inert.join(now());
        inert.leave(now());
        inert.readQuantum(now());
    }

    /** An "active" participant with no tags would silently behave as inert - the R19 configuration lie. */
    @Test
    void activeMemberWithNoTagsIsRefused() {
        assertThatThrownBy(() -> NavigatorParticipant.activeMember(allocator, UniLists.of(), MEMBER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one resource tag");
    }

    // ------------------------------------------------------------------
    // Eligibility (KTD1): lease present AND credits > 0
    // ------------------------------------------------------------------

    @Test
    void eligibilityRequiresALiveLeaseWithCreditsNotMerelyALease() {
        allocator.register(new ResourceContract(API_A, 1.0, 1, Duration.ofSeconds(1)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(API_A), MEMBER);
        allocator.join(MEMBER, now());
        clock.add(Duration.ofSeconds(1));

        assertWithMessage("no quantum pulled yet: no lease, not eligible")
                .that(participant.hasSpendableCreditForAllTags(now())).isFalse();

        allocator.readQuantum(MEMBER, now());
        assertWithMessage("one credit pulled: eligible")
                .that(participant.hasSpendableCreditForAllTags(now())).isTrue();

        participant.spendOneCreditPerTag(now());
        assertWithMessage("the lease still exists but holds ZERO credits - that is blocked, not eligible (KTD1)")
                .that(allocator.currentLease(MEMBER, API_A, now()).isPresent()).isTrue();
        assertThat(participant.hasSpendableCreditForAllTags(now())).isFalse();
    }

    // ------------------------------------------------------------------
    // The max-vs-min split: availableAt (R7) vs the wakeup bound (KTD5)
    // ------------------------------------------------------------------

    /**
     * Two resources on different quanta so the two times genuinely differ: at t=2.5s, api-a (1s quantum) next
     * mints at 3s and api-b (2s quantum) at 4s. The record-level {@code availableAt} needs BOTH, so the max
     * (4s); the wakeup bound needs the first instant anything could move, so the min (3s).
     */
    @Test
    void availableAtIsTheLatestBlockedTimeWhileTheWakeupBoundIsTheEarliest() {
        allocator.register(new ResourceContract(API_A, 1.0, 1, Duration.ofSeconds(1)));
        allocator.register(new ResourceContract(API_B, 0.5, 1, Duration.ofSeconds(2)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(API_A, API_B), MEMBER);
        allocator.join(MEMBER, now());
        clock.add(Duration.ofMillis(2500)); // both blocked: nothing was ever pulled

        assertWithMessage("a record needing both resources cannot run before the LAST of them has credit (R7)")
                .that(participant.availableAt(now())).hasValue(Instant.ofEpochSecond(4));
        assertWithMessage("the loop should wake at the FIRST instant any deferred work could move (KTD5)")
                .that(participant.earliestBlockedResourceNextCreditAt(now())).hasValue(Instant.ofEpochSecond(3));
    }

    /** With every tag holding credit there is nothing blocking, so neither time exists. */
    @Test
    void nothingBlockedMeansNoAvailableAtAndNoWakeupBound() {
        allocator.register(new ResourceContract(API_A, 1.0, 1, Duration.ofSeconds(1)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(API_A), MEMBER);
        allocator.join(MEMBER, now());
        clock.add(Duration.ofSeconds(1));
        allocator.readQuantum(MEMBER, now());

        assertThat(participant.availableAt(now()).isPresent()).isFalse();
        assertThat(participant.earliestBlockedResourceNextCreditAt(now()).isPresent()).isFalse();
    }

    // ------------------------------------------------------------------
    // The always-succeeds debit (KTD1/KD10) - the engine-seam half
    // ------------------------------------------------------------------

    /**
     * The engine's spend seam obeys the always-succeeds rule: a spend with no live credit - the quantum rolled
     * between the eligibility read and the debit, or a concurrent claimer got there first - lands as OVERDRAFT
     * and the conservation identity still closes. No rollback, no refund, no re-mint (KD10); R8's burst term
     * budgets exactly this.
     */
    @Test
    void spendWithNoLiveCreditLandsAsOverdraftAndTheIdentityCloses() {
        allocator.register(new ResourceContract(API_A, 1.0, 1, Duration.ofSeconds(1)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(API_A), MEMBER);
        allocator.join(MEMBER, now());
        clock.add(Duration.ofSeconds(1));

        participant.spendOneCreditPerTag(now()); // nothing pulled - no credit exists

        ConservationLedger snapshot = allocator.conservationLedger(API_A, now());
        assertThat(snapshot.getSpent()).isEqualTo(1);
        assertWithMessage("the debit with no credit must land as overdraft, never fail and never go negative")
                .that(snapshot.getOverdraft()).isEqualTo(1);
        assertWithMessage("conservation identity: outstanding (derived) must equal the scanned live credits")
                .that(snapshot.getOutstanding()).isEqualTo(snapshot.getLiveCredits());
    }

    /** A multi-tag spend debits every tagged resource, once each (R7's one-credit-per-resource rule). */
    @Test
    void spendDebitsOneCreditFromEveryTaggedResource() {
        allocator.register(new ResourceContract(API_A, 1.0, 1, Duration.ofSeconds(1)));
        allocator.register(new ResourceContract(API_B, 1.0, 1, Duration.ofSeconds(1)));
        var participant = NavigatorParticipant.activeMember(allocator, UniLists.of(API_A, API_B), MEMBER);
        allocator.join(MEMBER, now());
        clock.add(Duration.ofSeconds(1));
        allocator.readQuantum(MEMBER, now());

        participant.spendOneCreditPerTag(now());

        assertThat(allocator.conservationLedger(API_A, now()).getSpent()).isEqualTo(1);
        assertThat(allocator.conservationLedger(API_B, now()).getSpent()).isEqualTo(1);
    }

    /**
     * The pure reads must never mutate: a participant asked its eligibility and times any number of times
     * against a MOCK allocator only ever performs reads - no spend, no membership event, no quantum pull.
     */
    @Test
    void pureReadsNeverReachAMutatingAllocatorCall() {
        ResourceAllocator reads = Mockito.mock(ResourceAllocator.class);
        var participant = NavigatorParticipant.activeMember(reads, UniLists.of(API_A), MEMBER);

        participant.hasSpendableCreditForAllTags(now());
        participant.availableAt(now());
        participant.earliestBlockedResourceNextCreditAt(now());

        Mockito.verify(reads, Mockito.never()).spend(Mockito.anyString(), Mockito.anyString(), Mockito.any());
        Mockito.verify(reads, Mockito.never()).join(Mockito.anyString(), Mockito.any());
        Mockito.verify(reads, Mockito.never()).leave(Mockito.anyString(), Mockito.any());
        Mockito.verify(reads, Mockito.never()).readQuantum(Mockito.anyString(), Mockito.any());
    }

    // ------------------------------------------------------------------
    // Fail-safe on a throwing allocator - degrade, never crash
    // ------------------------------------------------------------------

    /**
     * The allocator is user-supplied (a public options seam) and its reads sit on the per-claim hot path, where
     * the only other boundary is the control task's catch-and-close - so a throwing allocator must DEGRADE this
     * instance, never kill it: eligibility fails safe as BLOCKED with no known next credit (a deferral, not a
     * free pass), view reads return their zero shapes, mutating calls are swallowed after logging, and every
     * failure lands on the monotonic counter the {@code pc.navigator.allocator.failures} gauge reads.
     */
    @Test
    void aThrowingAllocatorDegradesToBlockedAndNeverPropagates() {
        ResourceAllocator throwing = Mockito.mock(ResourceAllocator.class);
        IllegalStateException boom = new IllegalStateException("user allocator failure");
        Mockito.when(throwing.currentLease(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenThrow(boom);
        Mockito.when(throwing.nextCreditAt(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenThrow(boom);
        Mockito.when(throwing.globalRatePerSecond(Mockito.anyString())).thenThrow(boom);
        Mockito.when(throwing.localRatePerSecond(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenThrow(boom);
        Mockito.doThrow(boom).when(throwing).spend(Mockito.anyString(), Mockito.anyString(), Mockito.any());
        Mockito.doThrow(boom).when(throwing).join(Mockito.anyString(), Mockito.any());
        Mockito.doThrow(boom).when(throwing).leave(Mockito.anyString(), Mockito.any());
        Mockito.doThrow(boom).when(throwing).readQuantum(Mockito.anyString(), Mockito.any());
        var participant = NavigatorParticipant.activeMember(throwing, UniLists.of(API_A), MEMBER);

        assertWithMessage("an unreadable resource must fail SAFE as blocked - a deferral, never a free pass")
                .that(participant.hasSpendableCreditForAllTags(now())).isFalse();
        assertWithMessage("one failed eligibility read is one counted failure")
                .that(participant.allocatorFailureCount()).isEqualTo(1);

        assertWithMessage("blocked with no KNOWN next credit - no time to name, and no crash")
                .that(participant.availableAt(now()).isPresent()).isFalse();
        assertThat(participant.earliestBlockedResourceNextCreditAt(now()).isPresent()).isFalse();
        assertWithMessage("the attribution read still names the blocked resource, with its time unknown")
                .that(participant.blockingResourceDeferrals(now())).hasSize(1);

        assertWithMessage("view reads return their zero shapes rather than propagating")
                .that(participant.globalRatePerSecond(API_A)).isEqualTo(0.0);
        assertThat(participant.localRatePerSecond(API_A, now())).isEqualTo(0.0);

        long beforeSpend = participant.allocatorFailureCount();
        participant.spendOneCreditPerTag(now()); // must not throw - swallowed after logging
        assertWithMessage("a failed spend is swallowed AND counted")
                .that(participant.allocatorFailureCount()).isEqualTo(beforeSpend + 1);

        // the lifecycle mutations swallow too - a throwing allocator must never abort start or close
        participant.join(now());
        participant.leave(now());
        participant.readQuantum(now());
        assertThat(participant.allocatorFailureCount()).isEqualTo(beforeSpend + 4);
    }

    /**
     * The spend-failure latch: an allocator that throws ONLY from {@code spend()} is the one failure shape the
     * everything-throws test above cannot see, and without the latch it is fail-OPEN - eligibility reads
     * {@code currentLease}, which stays healthy because the failed debit never decremented it, so the instance
     * would dispatch unthrottled against a rate it never pays for. After a failed debit the tag must read
     * BLOCKED with no known next credit, without believing the lease; the next SUCCESSFUL mutating call - here
     * the per-pass {@code readQuantum}, since a latched tag blocks the very claims that would spend again -
     * clears the latch so a recovered allocator resumes normal flow.
     */
    @Test
    void aSpendOnlyThrowingAllocatorLatchesTheTagBlockedUntilAMutatingCallSucceeds() {
        ResourceAllocator spendThrows = Mockito.mock(ResourceAllocator.class);
        var healthyLease = new CapacityLease(API_A, 0, 1, now().plusSeconds(1));
        Mockito.when(spendThrows.currentLease(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenReturn(Optional.of(healthyLease));
        Mockito.doThrow(new IllegalStateException("user allocator fails only its spend seam"))
                .when(spendThrows).spend(Mockito.anyString(), Mockito.anyString(), Mockito.any());
        var participant = NavigatorParticipant.activeMember(spendThrows, UniLists.of(API_A), MEMBER);

        assertWithMessage("healthy lease with credit: eligible before anything fails")
                .that(participant.hasSpendableCreditForAllTags(now())).isTrue();

        participant.spendOneCreditPerTag(now()); // the debit throws - swallowed, counted, and LATCHED

        assertWithMessage("the failed spend is counted")
                .that(participant.allocatorFailureCount()).isEqualTo(1);
        assertWithMessage("the free pass must close: the lease still reads healthy, but the debit never landed, "
                + "so the tag is blocked until a mutating call succeeds - never dispatched unthrottled")
                .that(participant.hasSpendableCreditForAllTags(now())).isFalse();
        assertWithMessage("the latched deferral is attributable, like any other blocked resource")
                .that(participant.blockingResourceDeferrals(now())).hasSize(1);

        // recovery: the per-pass quantum pull succeeds (the mock's readQuantum is a healthy no-op) - unlatched
        participant.readQuantum(now());
        assertWithMessage("a recovered allocator resumes normal flow after its next successful mutating call")
                .that(participant.hasSpendableCreditForAllTags(now())).isTrue();
        assertWithMessage("recovery clears the latch, never the monotonic failure record")
                .that(participant.allocatorFailureCount()).isEqualTo(1);
    }
}
