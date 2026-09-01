package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The U2 mechanics of {@link StubResourceAllocator} on the virtual clock (R14): quantum-indexed lazy minting,
 * equal share with rotating remainder, membership lifecycle with lease TTL, expiry, soft-debit overdraft with
 * its per-quantum burst overshoot budget (R8), and the conservation identity (KTD2, KTD4, R5, R6, R16, R17).
 * <p>
 * All time comes from a field-held {@link MutableClock} shared with the allocator at construction (KTD4's one
 * canonical clock; the {@code AdmissionControllerTest} drive pattern) - no wall clock anywhere. The demo policy
 * throughout is KTD7's: 2 credits/sec, burst 2, one-second quantum, so two members get one credit each per
 * quantum and the arithmetic in the assertions stays legible.
 * <p>
 * The scheduler-controlled complement (concurrent reads/spends/membership against one allocator) lives in
 * {@link StubResourceAllocatorLincheckTest}, in the opt-in {@code lincheck} lane (KTD11).
 */
class StubResourceAllocatorMintingTest {

    private static final String API_X = "api-x";

    private static final Duration QUANTUM = Duration.ofSeconds(1);

    private static final ResourceContract DEMO_POLICY = new ResourceContract(API_X, 2.0, 2, QUANTUM);

    /** Epoch start means quantum indices count from zero and every boundary is a whole second - legible maths. */
    private final MutableClock clock = MutableClock.epochUTC();

    private final StubResourceAllocator allocator = new StubResourceAllocator(clock);

    {
        allocator.register(DEMO_POLICY);
    }

    private Instant now() {
        return clock.instant();
    }

    private void join(String memberId) {
        allocator.join(memberId, now());
    }

    private void leave(String memberId) {
        allocator.leave(memberId, now());
    }

    private void read(String memberId) {
        allocator.readQuantum(memberId, now());
    }

    private void spend(String memberId) {
        allocator.spend(memberId, API_X, now());
    }

    /**
     * Drives {@code spend} against an EXPLICIT observation instant rather than the clock's current reading -
     * simulating a straggler whose instant was captured earlier (outside {@code stateLock}, per
     * {@code WorkContainer.onQueueingForExecution}) but only reaches the allocator after the clock, and the
     * quantum index, has already moved on.
     */
    private void spendAt(String memberId, Instant at) {
        allocator.spend(memberId, API_X, at);
    }

    private int credits(String memberId) {
        return allocator.currentLease(memberId, API_X, now())
                .map(CapacityLease::getAvailableCredits)
                .orElse(0);
    }

    private ConservationLedger ledger() {
        return allocator.conservationLedger(API_X, now());
    }

    private void nextQuantum() {
        clock.add(QUANTUM);
    }

    /**
     * KTD2's identity, asserted the non-tautological way: outstanding DERIVED from the counters must equal the
     * independently-scanned live credits.
     */
    private void assertIdentityCloses() {
        ConservationLedger snapshot = ledger();
        assertWithMessage("conservation identity minted + overdraft == spent + expired + outstanding (%s)",
                snapshot)
                .that(snapshot.getOutstanding())
                .isEqualTo(snapshot.getLiveCredits());
    }

    // ------------------------------------------------------------------
    // AE1 mechanics: equal share per quantum, and expiry
    // ------------------------------------------------------------------

    /**
     * Covers AE1 mechanics / R5, R6. Two members at 2/sec on a 1s quantum: one credit each per quantum, and an
     * unspent credit is gone the moment the next quantum starts - never carried over.
     */
    @Test
    void twoMembersEachPullOneCreditPerQuantumAndUnspentCreditsExpire() {
        join("a");
        join("b");
        nextQuantum(); // quantum 1 - membership effective from the quantum AFTER the join

        read("a");
        read("b");

        assertThat(credits("a")).isEqualTo(1);
        assertThat(credits("b")).isEqualTo(1);
        CapacityLease lease = allocator.currentLease("a", API_X, now()).get();
        assertThat(lease.getQuantumIndex()).isEqualTo(1);
        assertThat(lease.getExpiresAt()).isEqualTo(Instant.ofEpochSecond(2));
        assertThat(ledger().getMinted()).isEqualTo(2);
        assertIdentityCloses();

        nextQuantum(); // quantum 2 - nothing was spent, so both credits expire

        assertThat(credits("a")).isEqualTo(0);
        assertThat(credits("b")).isEqualTo(0);
        assertThat(ledger().getExpired()).isEqualTo(2);
        assertThat(ledger().getOutstanding()).isEqualTo(0);
        assertIdentityCloses();

        read("a");
        read("b");

        assertThat(credits("a")).isEqualTo(1);
        assertThat(ledger().getMinted()).isEqualTo(4);
        assertIdentityCloses();
    }

    /**
     * Covers AE2 mechanics / R11, R16. Close one member mid-quantum: its live unspent credit is lost (expired,
     * never redistributed mid-window), and from the next quantum the survivor gets the full division.
     */
    @Test
    void closingOneMemberMidQuantumLosesItsShareThenSurvivorGetsFullDivision() {
        join("a");
        join("b");
        nextQuantum();
        read("a");
        read("b");

        clock.add(Duration.ofMillis(500)); // mid-quantum
        leave("b");

        assertThat(credits("b")).isEqualTo(0);
        assertThat(credits("a")).isEqualTo(1); // the survivor's CURRENT quantum share is untouched
        assertThat(ledger().getExpired()).isEqualTo(1); // b's unspent credit is lost, not redistributed
        assertIdentityCloses();

        clock.add(Duration.ofMillis(500)); // quantum 2 - re-division among survivors
        read("a");

        assertThat(credits("a")).isEqualTo(2);
        assertIdentityCloses();
    }

    // ------------------------------------------------------------------
    // R14: no re-mint of an issued quantum
    // ------------------------------------------------------------------

    /**
     * Covers R14. A repeated read of an issued quantum returns the IDENTICAL grant, never a fresh or topped-up
     * one - v1's no-re-mint mechanism (the successor-allocator epoch proof belongs to the Kafka rung, not
     * here).
     */
    @Test
    void repeatedReadsOfAnIssuedQuantumReturnTheIdenticalGrant() {
        join("a");
        nextQuantum();
        read("a");
        assertThat(credits("a")).isEqualTo(2);

        spend("a");
        assertThat(credits("a")).isEqualTo(1);

        read("a"); // same quantum again - must NOT top the lease back up

        assertThat(credits("a")).isEqualTo(1);
        assertThat(ledger().getMinted()).isEqualTo(2);
        assertIdentityCloses();
    }

    // ------------------------------------------------------------------
    // R16: lease TTL for a silent member
    // ------------------------------------------------------------------

    /**
     * Covers R14/R16. A member that stops reading quanta keeps its share for the TTL (capacity lost, never
     * redistributed mid-window - the active member does NOT get it), then is dropped and the division
     * converges.
     */
    @Test
    void silentMemberIsDroppedAfterItsLeaseTtlAndItsShareIsNotRedistributedBefore() {
        join("a");
        join("b"); // b joins, then never reads a quantum - its lease clock starts at the join
        for (int quantum = 1; quantum <= StubResourceAllocator.MEMBERSHIP_LEASE_TTL_QUANTA; quantum++) {
            nextQuantum();
            read("a");
            assertWithMessage("quantum %s: silent b is still within its lease TTL, so a gets only its share",
                    quantum)
                    .that(credits("a")).isEqualTo(1);
        }

        nextQuantum(); // first quantum past the TTL

        read("a");
        assertThat(credits("a")).isEqualTo(2);
        assertIdentityCloses();
    }

    /**
     * Covers KTD9's query discipline: only {@link ResourceAllocator#readQuantum} renews the membership lease -
     * a member that merely polls the pure reads every quantum still lapses.
     */
    @Test
    void pureReadsDoNotRenewTheMembershipLease() {
        join("a");
        join("b");
        long lastQuantum = StubResourceAllocator.MEMBERSHIP_LEASE_TTL_QUANTA + 1;
        for (int quantum = 1; quantum <= lastQuantum; quantum++) {
            nextQuantum();
            read("a");
            // b polls every pure read every quantum - none of these may count as a lease renewal
            allocator.currentLease("b", API_X, now());
            allocator.nextCreditAt("b", API_X, now());
            allocator.localRatePerSecond("b", API_X, now());
            allocator.conservationLedger(API_X, now());
        }

        assertThat(credits("a")).isEqualTo(2); // b lapsed despite its polling
        assertIdentityCloses();
    }

    // ------------------------------------------------------------------
    // R8/R12: the minted-per-window bound, across membership transitions
    // ------------------------------------------------------------------

    /**
     * Covers R12/R8. Over a window starting at a quantum boundary, credits minted never exceed
     * rate x window + burst - asserted from the conservation counters, across joins, leaves and a TTL drop.
     */
    @Test
    void mintedPerWindowNeverExceedsRateTimesWindowPlusBurst() {
        long windowStartMinted = ledger().getMinted(); // window anchored at the quantum-0 boundary
        join("a");
        join("b");

        int quanta = 8;
        for (int quantum = 1; quantum <= quanta; quantum++) {
            nextQuantum();
            read("a");
            if (quantum <= 2) {
                read("b");
            } // then b goes silent - TTL drop mid-window
            if (quantum == 3) {
                join("c");
            }
            if (quantum >= 4) {
                read("c");
            }
            if (quantum == 6) {
                leave("c");
            }
            long mintedInWindow = ledger().getMinted() - windowStartMinted;
            long bound = 2L * (quantum + 1) + DEMO_POLICY.getBurst(); // rate x elapsed-window + burst
            assertWithMessage("quantum %s: minted-in-window %s must stay within the R8 bound %s",
                    quantum, mintedInWindow, bound)
                    .that(mintedInWindow).isAtMost(bound);
            assertIdentityCloses();
        }
    }

    // ------------------------------------------------------------------
    // R16/KTD4: membership boundaries
    // ------------------------------------------------------------------

    /**
     * Covers R16/KTD4. A constructed-but-not-joined member is excluded from the division - even one that
     * (incorrectly) calls readQuantum. Running members' per-quantum credit is unaffected.
     */
    @Test
    void memberThatNeverJoinedIsExcludedFromDivision() {
        join("a");
        nextQuantum();

        read("never-joined");
        read("a");

        assertThat(credits("a")).isEqualTo(2);
        assertThat(credits("never-joined")).isEqualTo(0);
        assertThat(allocator.localRatePerSecond("never-joined", API_X, now())).isEqualTo(0.0);
        assertIdentityCloses();
    }

    /**
     * Covers R16. A join lands at the NEXT quantum - the joiner gets nothing from the quantum it joined in,
     * and does not dilute the incumbent's share for it.
     */
    @Test
    void joinIsEffectiveNextQuantumNotImmediately() {
        join("a");
        nextQuantum();
        join("late"); // joins during quantum 1

        read("a");
        read("late");

        assertThat(credits("a")).isEqualTo(2); // quantum 1's division predates the join
        assertThat(credits("late")).isEqualTo(0);

        nextQuantum();
        read("a");
        read("late");

        assertThat(credits("a")).isEqualTo(1);
        assertThat(credits("late")).isEqualTo(1);
        assertIdentityCloses();
    }

    // ------------------------------------------------------------------
    // KTD4: integral division with rotating remainder
    // ------------------------------------------------------------------

    /**
     * Covers KTD4. Three members sharing 2 credits/quantum: floor gives everyone 0, the 2-credit remainder
     * rotates deterministically by quantum index - per quantum exactly two members get one credit each (the
     * grant is never exceeded), and over the 3-quantum rotation cycle nobody starves.
     */
    @Test
    void threeMembersSharingTwoCreditsRotateTheRemainderWithoutStarvation() {
        join("a");
        join("b");
        join("c");
        List<String> members = UniLists.of("a", "b", "c");
        Map<String, Integer> totals = new HashMap<>();
        Map<String, Integer> longestDrought = new HashMap<>();
        Map<String, Integer> currentDrought = new HashMap<>();

        int quanta = 6; // two full rotation cycles
        for (int quantum = 1; quantum <= quanta; quantum++) {
            nextQuantum();
            int mintedThisQuantum = 0;
            for (String member : members) {
                read(member);
                int got = credits(member);
                assertWithMessage("quantum %s: member %s share", quantum, member).that(got).isAtMost(1);
                mintedThisQuantum += got;
                totals.merge(member, got, Integer::sum);
                if (got == 0) {
                    int drought = currentDrought.merge(member, 1, Integer::sum);
                    longestDrought.merge(member, drought, Integer::max);
                } else {
                    currentDrought.put(member, 0);
                }
            }
            assertWithMessage("quantum %s: total minted must equal the policy grant, never exceed it", quantum)
                    .that(mintedThisQuantum).isEqualTo(2);
            assertIdentityCloses();
        }

        for (String member : members) {
            assertWithMessage("member %s total over %s quanta", member, quanta)
                    .that(totals.get(member)).isEqualTo(4); // 12 credits, rotated evenly
            assertWithMessage("member %s must not starve across the rotation", member)
                    .that(longestDrought.getOrDefault(member, 0)).isAtMost(1);
        }
    }

    // ------------------------------------------------------------------
    // R17: an idle member keeps its share
    // ------------------------------------------------------------------

    /**
     * Covers R17. A live member with no spends keeps its equal share (its control loop still reads quanta);
     * its credits expire each quantum, and the busy member's rate is unchanged - accepted v1
     * underutilization.
     */
    @Test
    void idleMemberKeepsItsShareItsCreditsExpireAndTheBusyMemberIsUnaffected() {
        join("busy");
        join("idle");
        int quanta = 3;
        for (int quantum = 1; quantum <= quanta; quantum++) {
            nextQuantum();
            read("busy");
            read("idle");
            assertWithMessage("quantum %s: busy member's share is unchanged by its neighbour's idleness",
                    quantum)
                    .that(credits("busy")).isEqualTo(1);
            assertThat(credits("idle")).isEqualTo(1);
            spend("busy");
        }
        nextQuantum();

        // every idle credit expired; every busy credit was spent
        assertThat(ledger().getExpired()).isEqualTo(quanta);
        assertThat(ledger().getSpent()).isEqualTo(quanta);
        assertIdentityCloses();
    }

    // ------------------------------------------------------------------
    // Edge: membership to zero, then a fresh joiner
    // ------------------------------------------------------------------

    /**
     * Edge case. Membership drops to zero cleanly - later quanta mint nothing - and a new joiner starts fresh
     * with no stale state.
     */
    @Test
    void membershipToZeroIsCleanAndANewJoinerStartsFresh() {
        join("a");
        nextQuantum();
        read("a");
        spend("a");
        leave("a"); // takes its 1 unspent credit with it

        nextQuantum(); // quantum 2 - no members
        assertThat(ledger().getMinted()).isEqualTo(2);
        assertThat(ledger().getSpent()).isEqualTo(1);
        assertThat(ledger().getExpired()).isEqualTo(1);
        assertThat(ledger().getOutstanding()).isEqualTo(0);
        assertIdentityCloses();

        join("d");
        nextQuantum(); // quantum 3
        read("d");

        assertThat(credits("d")).isEqualTo(2); // full grant, no stale state
        assertThat(ledger().getMinted()).isEqualTo(4);
        assertIdentityCloses();
    }

    /**
     * Covers R14/KTD4's no-re-mint rule at the leave edge: a member that left and whose control loop
     * (incorrectly) reads again in the same quantum must NOT be re-issued its share - the quantum was already
     * issued to it once, and total minted per quantum must never exceed the policy grant.
     */
    @Test
    void leaveThenReadInTheSameQuantumDoesNotReMint() {
        join("a");
        nextQuantum();
        read("a");
        assertThat(ledger().getMinted()).isEqualTo(2);

        leave("a");
        read("a"); // a straggling control-loop pass after close-entry

        assertThat(credits("a")).isEqualTo(0);
        assertThat(ledger().getMinted()).isEqualTo(2); // the quantum was already issued - never re-minted
        assertThat(ledger().getExpired()).isEqualTo(2); // the leaver's unspent credits were lost at leave
        assertIdentityCloses();
    }

    // ------------------------------------------------------------------
    // KTD1/KD10: the soft debit
    // ------------------------------------------------------------------

    /**
     * Covers KTD1/KD10. Quantum-boundary debit: a credit observed as available expires before the post-claim
     * spend lands - the debit still succeeds, as overdraft, and the identity holds. No refund, no re-mint, no
     * negative bookkeeping.
     */
    @Test
    void quantumBoundaryDebitLandsAsOverdraftAndTheIdentityHolds() {
        join("a");
        nextQuantum();
        read("a");
        assertThat(credits("a")).isEqualTo(2); // observed available...

        nextQuantum(); // ...but the quantum rolls before the spend lands
        spend("a");

        ConservationLedger snapshot = ledger();
        assertThat(snapshot.getSpent()).isEqualTo(1);
        assertThat(snapshot.getOverdraft()).isEqualTo(1);
        assertThat(snapshot.getExpired()).isEqualTo(2); // the whole stale lease expired
        assertThat(snapshot.getOutstanding()).isEqualTo(0);
        assertIdentityCloses();
    }

    /**
     * Covers R8/KTD1. Overdraft WITHIN the declared burst is the BUDGETED overshoot - the expected racing
     * debits between an eligibility read and the spend landing. The beyond-burst monitor must not move, and
     * the identity must still close.
     */
    @Test
    void overdraftWithinTheBurstBudgetIsCountedButNeverFlaggedBeyondBurst() {
        join("a");
        nextQuantum();
        read("a"); // sole member: both of the quantum's 2 credits
        spend("a");
        spend("a"); // live credit exhausted

        spend("a");
        spend("a"); // two overdrafts - exactly the demo policy's burst budget of 2

        ConservationLedger snapshot = ledger();
        assertThat(snapshot.getOverdraft()).isEqualTo(2);
        assertThat(snapshot.getOverdraftBeyondBurst()).isEqualTo(0);
        assertIdentityCloses();
    }

    /**
     * Covers R8/KTD1. A quantum's cumulative overdraft pushing BEYOND the declared burst still succeeds - no
     * clamp, no refusal, the conservation identity untouched (KTD1) - but every such debit moves the monotonic
     * beyond-burst monitor, which proves {@code spend} actually reads the contract's burst.
     */
    @Test
    void overdraftBeyondTheBurstBudgetSucceedsAndMovesTheBeyondBurstMonitor() {
        join("a");
        nextQuantum();
        read("a");
        spend("a");
        spend("a"); // the two live credits

        for (int i = 0; i < 5; i++) {
            spend("a"); // five overdrafts: 2 consume the burst budget, 3 land beyond it
        }

        ConservationLedger snapshot = ledger();
        assertThat(snapshot.getSpent()).isEqualTo(7); // every debit succeeded (KTD1's always-succeeds rule)
        assertThat(snapshot.getOverdraft()).isEqualTo(5);
        assertThat(snapshot.getOverdraftBeyondBurst()).isEqualTo(3);
        assertIdentityCloses();
    }

    /**
     * Covers R8/KTD4. The overshoot budget is PER QUANTUM: consuming the whole burst in quantum N and again in
     * quantum N+1 flags nothing - only a budget that failed to reset at the quantum roll (the bug shape) would
     * read the second quantum's within-burst overdraft as beyond-burst.
     */
    @Test
    void quantumRollResetsTheBurstBudgetSoWithinBurstOverdraftNeverFlagsTwiceOver() {
        join("a");
        nextQuantum();
        spend("a");
        spend("a"); // nothing read this quantum: two overdrafts, exactly the burst budget

        nextQuantum();
        spend("a");
        spend("a"); // fresh quantum, fresh budget - exactly the burst again

        ConservationLedger snapshot = ledger();
        assertThat(snapshot.getOverdraft()).isEqualTo(4);
        assertThat(snapshot.getOverdraftBeyondBurst()).isEqualTo(0);
        assertIdentityCloses();
    }

    /**
     * Covers R8/KTD1 against the out-of-order-instants hazard {@code trackOverdraftAgainstBurstBudget}'s javadoc
     * names: the observation instant behind a spend is read outside {@code stateLock} and the call is
     * concurrently reachable, so a straggler carrying an OLDER quantum's instant can reach the allocator AFTER a
     * later quantum's overdraft is already underway. The budget must not reset on that arrival - it must fold
     * the straggler into the CURRENT quantum's cumulative count, exactly like the allocator's membership-lease
     * renewal monotonically merges against the identical hazard. A bare {@code !=} reset (the bug shape) would
     * zero the current quantum's count back down, undercounting beyond-burst and silently re-arming the
     * once-per-quantum warn.
     */
    @Test
    void outOfOrderStragglerSpendDoesNotResetTheCurrentQuantumsBurstBudget() {
        join("a");
        nextQuantum(); // quantum index 1 ("N") - membership effective from here on
        Instant staleQuantumNInstant = now(); // captured now, delivered to the allocator only later

        nextQuantum(); // quantum index 2 ("N+1") - the CURRENT budget from here on
        spend("a");
        spend("a"); // two genuine overdrafts in quantum 2 - exactly the burst budget, not yet beyond it

        // the straggler: its instant belongs to quantum 1, but it arrives after quantum 2's budget is live
        spendAt("a", staleQuantumNInstant);

        ConservationLedger afterStraggler = ledger();
        assertThat(afterStraggler.getOverdraft()).isEqualTo(3); // every debit still succeeds (KTD1)
        assertWithMessage("the straggler's older-quantum instant must fold into quantum 2's ALREADY-LIVE budget, "
                        + "not reset it - it is the 3rd cumulative overdraft, one beyond the burst of 2")
                .that(afterStraggler.getOverdraftBeyondBurst())
                .isEqualTo(1);

        // further genuine quantum-2 debits must keep accumulating against the SAME budget - a resetting bug
        // would have regressed overdraftBudgetQuantumIndex to 1, making these look like a fresh quantum again
        spend("a");
        spend("a");

        ConservationLedger snapshot = ledger();
        assertThat(snapshot.getOverdraft()).isEqualTo(5);
        assertWithMessage("the straggler must not have re-armed a fresh within-budget allowance for quantum 2")
                .that(snapshot.getOverdraftBeyondBurst())
                .isEqualTo(3);
        assertIdentityCloses();
    }

    // ------------------------------------------------------------------
    // KTD2: conservation under arbitrary interleavings
    // ------------------------------------------------------------------

    /**
     * Covers KTD2. A seeded pseudo-random storm of joins, leaves, quantum reads, spends (including overdraft
     * debits) and clock advances - the conservation identity must hold at EVERY observation point, not just at
     * quiescence.
     */
    @Test
    void conservationIdentityHoldsAtEveryObservationPointUnderArbitraryInterleavings() {
        Random random = new Random(20260901); // seeded: deterministic on the virtual clock
        List<String> members = new ArrayList<>(UniLists.of("m0", "m1", "m2", "m3"));
        for (int step = 0; step < 500; step++) {
            String member = members.get(random.nextInt(members.size()));
            switch (random.nextInt(6)) {
                case 0 -> allocator.join(member, now());
                case 1 -> allocator.leave(member, now());
                case 2, 3 -> allocator.readQuantum(member, now());
                case 4 -> allocator.spend(member, API_X, now());
                case 5 -> clock.add(Duration.ofMillis(random.nextInt(700)));
                default -> throw new IllegalStateException("unreachable");
            }
            assertIdentityCloses();
        }
    }

    // ------------------------------------------------------------------
    // KTD5/R18: next-credit time and the rate views
    // ------------------------------------------------------------------

    /**
     * Covers KTD5. The resource-level next-credit time is the next quantum boundary; unknown resources answer
     * empty rather than inventing a time.
     */
    @Test
    void resourceLevelNextCreditAtIsTheNextQuantumBoundary() {
        clock.add(Duration.ofMillis(500));

        assertThat(allocator.nextCreditAt(API_X, now())).hasValue(Instant.ofEpochSecond(1));
        assertThat(allocator.nextCreditAt("unknown", now()).isPresent()).isFalse();
        assertThat(allocator.nextCreditAt("m", "unknown", now()).isPresent()).isFalse();
    }

    /**
     * Covers KTD5/KTD4. The member-level next-credit time skips a quantum in which rotation gives the member
     * nothing: advancing to the projected instant and reading really does yield a credit - the projection is
     * driven, not trivially satisfied.
     */
    @Test
    void memberNextCreditAtSkipsQuantaTheRotationGivesItNothingIn() {
        join("a");
        join("b");
        join("c");
        nextQuantum();
        for (String member : UniLists.of("a", "b", "c")) {
            read(member);
        }

        for (String member : UniLists.of("a", "b", "c")) {
            Instant projected = allocator.nextCreditAt(member, API_X, now()).get();
            assertThat(projected).isGreaterThan(now());
            assertWithMessage("projection for %s must land on a quantum boundary", member)
                    .that(projected.toEpochMilli() % QUANTUM.toMillis()).isEqualTo(0);
            // within one rotation cycle - three members means nobody waits more than three quanta
            assertThat(projected.getEpochSecond()).isAtMost(now().getEpochSecond() + 3);
        }

        // drive one member's projection for real: advance to it and confirm the credit arrives
        String member = "a";
        Instant projected = allocator.nextCreditAt(member, API_X, now()).get();
        clock.setInstant(projected);
        read("a");
        read("b");
        read("c");
        assertWithMessage("at its projected next-credit time the member really has a credit")
                .that(credits(member)).isEqualTo(1);
        assertIdentityCloses();
    }

    /**
     * Covers R18. The rate views: global is the declared policy rate; instance-local is the member's equal
     * share of it under current membership; unknown resources and non-members answer zero.
     */
    @Test
    void rateViewsReportPolicyRateGloballyAndEqualShareLocally() {
        assertThat(allocator.globalRatePerSecond(API_X)).isEqualTo(2.0);
        assertThat(allocator.globalRatePerSecond("unknown")).isEqualTo(0.0);

        join("a");
        join("b");
        nextQuantum();

        assertThat(allocator.localRatePerSecond("a", API_X, now())).isEqualTo(1.0);
        assertThat(allocator.localRatePerSecond("b", API_X, now())).isEqualTo(1.0);
        assertThat(allocator.localRatePerSecond("not-a-member", API_X, now())).isEqualTo(0.0);
        assertThat(allocator.localRatePerSecond("a", "unknown", now())).isEqualTo(0.0);
    }

    /**
     * Pure reads are repeatable: two consecutive ledger snapshots with stale (lazily-expired) leases in play
     * are identical - observation folds nothing and moves no counter.
     */
    @Test
    void ledgerObservationIsPureAndRepeatable() {
        join("a");
        nextQuantum();
        read("a");
        nextQuantum(); // a's whole lease is now lazily expired, but NOT yet folded by any mutating call

        ConservationLedger first = ledger();
        ConservationLedger second = ledger();

        assertThat(first).isEqualTo(second);
        assertThat(first.getExpired()).isEqualTo(2);
        assertIdentityCloses();
    }
}
