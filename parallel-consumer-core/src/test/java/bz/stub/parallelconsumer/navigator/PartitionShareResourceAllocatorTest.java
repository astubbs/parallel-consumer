package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.quantumIndexOf;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The partition-share plan's U2 on the virtual clock: {@link PartitionShareResourceAllocator} mints each
 * instance's share of a resource from the fraction of the subscription's partitions it holds (R2, KD3),
 * slotting each held partition into the in-process allocator's remainder rotation by its fleet-stable
 * ordinal so a fleet's shares sum to exactly the grant with no communication (R3, KTD1); a snapshot published
 * mid-quantum reaches the NEXT quantum (R4, KTD2); an unresolved total mints nothing (R5); and the
 * conservation identity holds at every observation point (R10, KTD2).
 * <p>
 * All time comes from field-held {@link MutableClock}s - no wall clock anywhere. Each allocator here is ONE
 * PC instance (the engine builds one per instance, KTD4), so a fleet is several allocators driven by the
 * same clock, or by offset clocks where skew is the subject. The demo policy throughout is the micro-MVP's
 * KTD7: 2 credits/sec, burst 2, one-second quantum.
 * <p>
 * The scheduler-controlled complement (concurrent reads, spends, publishes and view reads against one
 * allocator) lives in {@link PartitionShareResourceAllocatorLincheckTest}, in the opt-in {@code lincheck}
 * lane (KTD12).
 */
class PartitionShareResourceAllocatorTest {

    private static final String API_X = "api-x";

    private static final String TOPIC = "orders";

    private static final Duration QUANTUM = Duration.ofSeconds(1);

    private static final ResourceContract DEMO_POLICY = new ResourceContract(API_X, 2.0, 2, QUANTUM);

    /** The engine passes its instance id; here one allocator is one instance, so the id is a constant. */
    private static final String INSTANCE = "pc-instance";

    /** Epoch start means quantum indices count from zero and every boundary is a whole second - legible maths. */
    private final MutableClock clock = MutableClock.epochUTC();

    private Instant now() {
        return clock.instant();
    }

    private void nextQuantum() {
        clock.add(QUANTUM);
    }

    private PartitionShareResourceAllocator newInstance() {
        PartitionShareResourceAllocator allocator = new PartitionShareResourceAllocator(clock);
        allocator.register(DEMO_POLICY);
        return allocator;
    }

    private static TopicPartition partition(int index) {
        return new TopicPartition(TOPIC, index);
    }

    private static Set<TopicPartition> partitions(int... indices) {
        Set<TopicPartition> held = new HashSet<>();
        for (int index : indices) {
            held.add(partition(index));
        }
        return held;
    }

    /** A resolved snapshot over the one demo topic: {@code held} of {@code total}. */
    private static AssignmentSnapshot holding(int total, int... indices) {
        return AssignmentSnapshot.resolved(partitions(indices), UniMaps.of(TOPIC, total));
    }

    private void publish(PartitionShareResourceAllocator allocator, AssignmentSnapshot snapshot) {
        allocator.publish(snapshot, now());
    }

    private void read(PartitionShareResourceAllocator allocator) {
        allocator.readQuantum(INSTANCE, now());
    }

    private void spend(PartitionShareResourceAllocator allocator) {
        allocator.spend(INSTANCE, API_X, now());
    }

    private int credits(PartitionShareResourceAllocator allocator) {
        return allocator.currentLease(INSTANCE, API_X, now())
                .map(CapacityLease::getAvailableCredits)
                .orElse(0);
    }

    private ConservationLedger ledger(PartitionShareResourceAllocator allocator) {
        return allocator.conservationLedger(API_X, now());
    }

    private Optional<Instant> nextCreditAt(PartitionShareResourceAllocator allocator) {
        return allocator.nextCreditAt(INSTANCE, API_X, now());
    }

    /**
     * KTD2's identity, asserted the non-tautological way: outstanding DERIVED from the counters must equal the
     * independently-scanned live credits.
     */
    private void assertIdentityCloses(PartitionShareResourceAllocator allocator) {
        ConservationLedger snapshot = ledger(allocator);
        assertWithMessage("conservation identity minted + overdraft == spent + expired + outstanding (%s)",
                snapshot)
                .that(snapshot.getOutstanding())
                .isEqualTo(snapshot.getLiveCredits());
    }

    /** Advances one quantum, reads every instance, and returns what each minted for that quantum. */
    private List<Long> mintNextQuantum(List<PartitionShareResourceAllocator> fleet) {
        List<Long> before = new ArrayList<>();
        for (PartitionShareResourceAllocator instance : fleet) {
            before.add(ledger(instance).getMinted());
        }
        nextQuantum();
        List<Long> minted = new ArrayList<>();
        for (int i = 0; i < fleet.size(); i++) {
            read(fleet.get(i));
            minted.add(ledger(fleet.get(i)).getMinted() - before.get(i));
        }
        return minted;
    }

    // ------------------------------------------------------------------
    // AE1 / AE3 / R3: the share and its rotation
    // ------------------------------------------------------------------

    /**
     * Covers AE1 mechanics / R2, R6. Two instances holding one partition each of two, at 2/sec on a 1s
     * quantum: one credit each per quantum, and an unspent credit is gone the moment the next quantum starts.
     */
    @Test
    void twoInstancesHoldingOnePartitionEachMintOneCreditPerQuantum() {
        PartitionShareResourceAllocator a = newInstance();
        PartitionShareResourceAllocator b = newInstance();
        publish(a, holding(2, 0));
        publish(b, holding(2, 1));

        for (int quantum = 1; quantum <= 4; quantum++) {
            List<Long> minted = mintNextQuantum(UniLists.of(a, b));
            assertWithMessage("quantum %s", quantum).that(minted).containsExactly(1L, 1L).inOrder();
            assertThat(credits(a)).isEqualTo(1);
            assertThat(credits(b)).isEqualTo(1);
        }
        spend(a);
        assertThat(credits(a)).isEqualTo(0);
        nextQuantum();
        assertWithMessage("b's unspent credit expired at the boundary").that(credits(b)).isEqualTo(0);
        assertThat(ledger(b).getExpired()).isEqualTo(4); // every one of b's credits went unspent
        assertIdentityCloses(a);
        assertIdentityCloses(b);
    }

    /**
     * Covers AE3 mechanics / R2, R3. Four partitions three-to-one with a grant of two: the rotation period is
     * the partition total, so over every four consecutive quanta the three-partition holder mints six credits
     * and the one-partition holder two - per-quantum shares 2,2,1,1 and 0,0,1,1 - and neither share is ever
     * zero forever.
     */
    @Test
    void threeToOneSplitRotatesTheRemainderSoSharesAccrueSixAndTwoPerFourQuanta() {
        PartitionShareResourceAllocator big = newInstance();
        PartitionShareResourceAllocator small = newInstance();
        publish(big, holding(4, 0, 1, 2));
        publish(small, holding(4, 3));

        List<Long> bigPerQuantum = new ArrayList<>();
        List<Long> smallPerQuantum = new ArrayList<>();
        for (int quantum = 1; quantum <= 8; quantum++) {
            List<Long> minted = mintNextQuantum(UniLists.of(big, small));
            bigPerQuantum.add(minted.get(0));
            smallPerQuantum.add(minted.get(1));
        }
        // quanta 4..7 start on a rotation boundary: the plan's stated sequences
        assertThat(bigPerQuantum.subList(3, 7)).containsExactly(2L, 2L, 1L, 1L).inOrder();
        assertThat(smallPerQuantum.subList(3, 7)).containsExactly(0L, 0L, 1L, 1L).inOrder();
        for (int start = 0; start + 4 <= 8; start++) {
            long bigWindow = bigPerQuantum.subList(start, start + 4).stream().mapToLong(Long::longValue).sum();
            long smallWindow = smallPerQuantum.subList(start, start + 4).stream().mapToLong(Long::longValue).sum();
            assertWithMessage("four-quantum window from quantum %s", start + 1).that(bigWindow).isEqualTo(6);
            assertWithMessage("four-quantum window from quantum %s", start + 1).that(smallWindow).isEqualTo(2);
        }
        assertThat(smallPerQuantum).contains(1L); // never a zero-forever share
    }

    /**
     * Covers R3. Five instances each holding one partition of five, grant two: the per-quantum sum over every
     * slot equals EXACTLY the grant for every quantum index - a fractional share never coincides above it -
     * and every holder mints its two credits per five quanta.
     */
    @Test
    void fivePartitionHoldersMintExactlyTheGrantEveryQuantum() {
        List<PartitionShareResourceAllocator> fleet = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            PartitionShareResourceAllocator instance = newInstance();
            publish(instance, holding(5, index));
            fleet.add(instance);
        }

        long[] perHolder = new long[5];
        for (int quantum = 1; quantum <= 10; quantum++) {
            List<Long> minted = mintNextQuantum(fleet);
            long fleetSum = minted.stream().mapToLong(Long::longValue).sum();
            assertWithMessage("quantum %s fleet mint %s", quantum, minted).that(fleetSum).isEqualTo(2);
            for (int i = 0; i < 5; i++) {
                perHolder[i] += minted.get(i);
            }
        }
        for (int i = 0; i < 5; i++) {
            assertWithMessage("holder of partition %s over ten quanta", i).that(perHolder[i]).isEqualTo(4);
        }
    }

    /**
     * Covers R3 / KTD1. Two topics of unequal partition counts (two and three) held across two instances: the
     * ordinal is the cumulative count of the topics sorted by name before a partition's topic plus its own
     * index, so every ordinal 0..4 is held exactly once, the fleet sum equals the grant for every quantum, and
     * no slot goes unminted. The bare partition index would slot alpha-0 and beta-0 together and leave slots
     * 3 and 4 unheld, and this fleet sum would then miss the grant.
     */
    @Test
    void unequalTopicsAreSlottedByFleetStableOrdinalSoEverySlotIsHeldExactlyOnce() {
        Map<String, Integer> totals = UniMaps.of("alpha", 2, "beta", 3);
        AssignmentSnapshot first = AssignmentSnapshot.resolved(UniSets.of(
                new TopicPartition("alpha", 0), new TopicPartition("alpha", 1), new TopicPartition("beta", 0)),
                totals);
        AssignmentSnapshot second = AssignmentSnapshot.resolved(UniSets.of(
                new TopicPartition("beta", 1), new TopicPartition("beta", 2)), totals);

        assertThat(first.getHeldOrdinals()).containsExactly(0, 1, 2).inOrder();
        assertThat(second.getHeldOrdinals()).containsExactly(3, 4).inOrder();
        assertThat(first.getTotalPartitions()).isEqualTo(5);

        PartitionShareResourceAllocator a = newInstance();
        PartitionShareResourceAllocator b = newInstance();
        publish(a, first);
        publish(b, second);
        long[] perInstance = new long[2];
        for (int quantum = 1; quantum <= 10; quantum++) {
            List<Long> minted = mintNextQuantum(UniLists.of(a, b));
            assertWithMessage("quantum %s fleet mint %s", quantum, minted)
                    .that(minted.get(0) + minted.get(1)).isEqualTo(2);
            perInstance[0] += minted.get(0);
            perInstance[1] += minted.get(1);
        }
        // ten quanta is twenty credits over five slots, four per slot: three slots make twelve, two make eight
        assertThat(perInstance[0]).isEqualTo(12);
        assertThat(perInstance[1]).isEqualTo(8);
    }

    /**
     * Covers R14. A repeated read of an issued quantum returns the IDENTICAL grant, never a fresh or topped-up
     * one, and mints nothing more.
     */
    @Test
    void repeatedReadsOfAnIssuedQuantumReturnTheIdenticalGrantAndMintNothingMore() {
        PartitionShareResourceAllocator a = newInstance();
        publish(a, holding(2, 0, 1));
        nextQuantum();
        read(a);
        spend(a);
        long mintedAfterFirstRead = ledger(a).getMinted();

        read(a);
        read(a);

        assertThat(credits(a)).isEqualTo(1);
        assertThat(ledger(a).getMinted()).isEqualTo(mintedAfterFirstRead);
    }

    // ------------------------------------------------------------------
    // R10's fleet ledger: the exact per-index entitlement, distinct from the averaged gauge
    // ------------------------------------------------------------------

    /**
     * Covers R10 / AE7's instrument. {@link PartitionShareResourceAllocator#entitledCredits} is the index's
     * ACTUAL grant - what {@code readQuantum} mints for it - not the rotation-averaged share the gauges carry:
     * for a read index it equals the minted lease; for the unread index ahead it predicts what the read then
     * mints; over a whole rotation the per-index values sum to the averaged share while individual indexes
     * differ from it (a half share of twelve slots mints 0 in some indexes and 2 in others, never 1.0). The
     * multi-process harness sums this per observed index, so minted is checked against an exact entitlement:
     * summed against the averaged gauge instead, the churn ladder failed a rung by exactly the rotation's phase
     * deviation, with nothing over-minted.
     */
    @Test
    void entitledCreditsIsTheIndexsActualGrantNotTheAveragedGauge() {
        PartitionShareResourceAllocator a = newInstance();
        publish(a, holding(12, 6, 7, 8, 9, 10, 11)); // six of twelve: the ladder's half share, 1.0 per quantum averaged
        long firstIndex = quantumIndexOf(now(), QUANTUM) + 1;
        long predictedNext = a.entitledCredits(API_X, firstIndex);
        long rotationSum = 0;
        Set<Long> distinctGrants = new HashSet<>();
        for (int step = 0; step < 12; step++) {
            nextQuantum();
            long index = quantumIndexOf(now(), QUANTUM);
            long before = ledger(a).getMinted();
            read(a);
            long minted = ledger(a).getMinted() - before;
            assertWithMessage("index %s: the unread projection said what the read then minted", index)
                    .that(minted).isEqualTo(predictedNext);
            assertWithMessage("index %s: the read index reproduces its minted lease", index)
                    .that(a.entitledCredits(API_X, index)).isEqualTo(minted);
            assertThat(a.entitledCredits(API_X, now())).isEqualTo(minted);
            assertThat(credits(a)).isEqualTo((int) minted);
            rotationSum += minted;
            distinctGrants.add(minted);
            predictedNext = a.entitledCredits(API_X, index + 1);
        }
        double averagedOverRotation = a.localRatePerSecond(INSTANCE, API_X, now()) * QUANTUM.getSeconds() * 12;
        assertWithMessage("over a whole rotation the exact grants sum to the averaged share")
                .that((double) rotationSum).isEqualTo(averagedOverRotation);
        assertWithMessage("individual indexes carry the rotation's actual grant, not the average")
                .that(distinctGrants).containsAtLeast(0L, 2L);
        assertThat(a.entitledCredits("unknown", firstIndex)).isEqualTo(0);
        assertIdentityCloses(a);
    }

    // ------------------------------------------------------------------
    // KTD11: clock skew is priced, not assumed away
    // ------------------------------------------------------------------

    /**
     * Covers KTD11 / KD5. Two instances mint from their OWN clocks, the giver's ahead and the receiver's
     * behind; a partition moved at a boundary in the gap where their quantum indices disagree is minted by both
     * for one index. The excess over the skew-free fleet mint is exactly that partition's per-quantum share
     * for exactly one index - the skew term the ladder publishes - and no more.
     */
    @Test
    void aPartitionMovedInTheSkewGapIsMintedTwiceForExactlyOneIndex() {
        Duration offset = Duration.ofMillis(300);
        MutableClock giverClock = MutableClock.of(Instant.ofEpochSecond(5).plus(offset), clock.getZone());
        MutableClock receiverClock = MutableClock.of(Instant.ofEpochSecond(5).minus(offset), clock.getZone());
        PartitionShareResourceAllocator giver = new PartitionShareResourceAllocator(giverClock);
        PartitionShareResourceAllocator receiver = new PartitionShareResourceAllocator(receiverClock);
        giver.register(DEMO_POLICY);
        receiver.register(DEMO_POLICY);
        giver.publish(holding(2, 1), giverClock.instant());
        receiver.publish(holding(2, 0), receiverClock.instant());

        Map<Long, Long> fleetMintByIndex = new TreeMap<>();
        for (int realSecond = 6; realSecond <= 13; realSecond++) {
            giverClock.add(QUANTUM);
            receiverClock.add(QUANTUM);
            if (realSecond == 10) {
                // the move lands on real second 10.0 - the giver's clock already reads quantum 10, the
                // receiver's still reads quantum 9: the giver's revoke is effective from 11, the receiver's
                // assignment from 10, so index 10 is minted by both
                giver.publish(holding(2), giverClock.instant());
                receiver.publish(holding(2, 0, 1), receiverClock.instant());
            }
            recordMint(giver, giverClock.instant(), fleetMintByIndex);
            recordMint(receiver, receiverClock.instant(), fleetMintByIndex);
        }

        // indices both instances read: the giver reads 6..13, the receiver 5..12
        for (long index = 6; index <= 12; index++) {
            long expected = index == 10 ? 3 : 2; // the grant, plus the moved partition's one-credit share once
            assertWithMessage("fleet mint for quantum index %s (%s)", index, fleetMintByIndex)
                    .that(fleetMintByIndex.get(index)).isEqualTo(expected);
        }
    }

    private static void recordMint(PartitionShareResourceAllocator instance, Instant ownNow,
                                   Map<Long, Long> fleetMintByIndex) {
        long before = instance.conservationLedger(API_X, ownNow).getMinted();
        instance.readQuantum(INSTANCE, ownNow);
        long minted = instance.conservationLedger(API_X, ownNow).getMinted() - before;
        fleetMintByIndex.merge(quantumIndexOf(ownNow, QUANTUM), minted, Long::sum);
    }

    // ------------------------------------------------------------------
    // R2: the burst budget scales with the share, rounded up
    // ------------------------------------------------------------------

    /**
     * Covers R2. The burst budget is the contract's burst scaled by the fraction and rounded UP: with burst 4,
     * a quarter share budgets one credit and a three-quarter share budgets three; holding nothing budgets
     * nothing; a holder whose scaled budget would truncate to zero keeps one; and a zero burst stays zero.
     */
    @Test
    void burstBudgetIsTheFractionOfBurstRoundedUpToAtLeastOne() {
        ResourceContract burstFour = new ResourceContract(API_X, 4.0, 4, QUANTUM);

        assertThat(holding(4, 0).burstBudgetFor(burstFour)).isEqualTo(1);
        assertThat(holding(4, 0, 1, 2).burstBudgetFor(burstFour)).isEqualTo(3);
        assertThat(holding(4).burstBudgetFor(burstFour)).isEqualTo(0);
        assertThat(AssignmentSnapshot.unresolved(partitions(0)).burstBudgetFor(burstFour)).isEqualTo(0);
        assertThat(holding(8, 0).burstBudgetFor(DEMO_POLICY)).isEqualTo(1); // 2 x 1/8 rounds up to one
        // a contract declaring no burst budgets none here either - the same policy means the same budget
        // under both allocators, so the beyond-burst monitor flags the first overdraft under each
        assertThat(holding(2, 0).burstBudgetFor(new ResourceContract(API_X, 2.0, 0, QUANTUM))).isEqualTo(0);
    }

    /**
     * Covers R2 and the plan's settled burst-slack decision: the fleet's summed budgets are at least the
     * contract's burst and at most burst plus the number of partition-holding instances. The three-to-one
     * shape under burst 2 is the case that exercises the slack - ceil(1.5) + ceil(0.5) = 3, one over.
     */
    @Test
    void fleetBurstBudgetsSumToAtLeastBurstAndAtMostBurstPlusHolders() {
        long summed = holding(4, 0, 1, 2).burstBudgetFor(DEMO_POLICY) + holding(4, 3).burstBudgetFor(DEMO_POLICY);

        assertThat(summed).isAtLeast((long) DEMO_POLICY.getBurst());
        assertThat(summed).isAtMost(DEMO_POLICY.getBurst() + 2L);
        assertThat(summed).isEqualTo(3); // the slack, exactly
    }

    /**
     * Covers R2 / R8 / KTD1. The scaled budget is what the beyond-burst monitor watches: a quarter-share holder
     * under burst 4 budgets one overdraft per quantum, so the second overdraft in a quantum is flagged - where
     * the unscaled burst would have let three more through silently.
     */
    @Test
    void theScaledBurstBudgetIsWhatTheBeyondBurstMonitorWatches() {
        PartitionShareResourceAllocator quarter = new PartitionShareResourceAllocator(clock);
        quarter.register(new ResourceContract(API_X, 4.0, 4, QUANTUM));
        publish(quarter, holding(4, 0));
        nextQuantum();
        read(quarter);
        assertThat(quarter.burstBudget(API_X, now())).isEqualTo(1);
        assertThat(credits(quarter)).isEqualTo(1);

        spend(quarter); // the credit
        spend(quarter); // overdraft one - within the scaled budget
        assertThat(ledger(quarter).getOverdraftBeyondBurst()).isEqualTo(0);
        spend(quarter); // overdraft two - beyond it

        assertThat(ledger(quarter).getOverdraft()).isEqualTo(2);
        assertThat(ledger(quarter).getOverdraftBeyondBurst()).isEqualTo(1);
        assertIdentityCloses(quarter);
    }

    // ------------------------------------------------------------------
    // R4 / KTD2: a snapshot reaches the next quantum, never the current one
    // ------------------------------------------------------------------

    /**
     * Covers R4 / KTD2. A snapshot published mid-quantum does not change the current quantum's issued grant -
     * a re-read finds the same lease and mints nothing - and the next quantum mints from it.
     */
    @Test
    void aSnapshotPublishedMidQuantumReachesTheNextQuantumNotTheCurrentOne() {
        PartitionShareResourceAllocator a = newInstance();
        publish(a, holding(2, 0));
        nextQuantum();
        read(a);
        assertThat(credits(a)).isEqualTo(1);

        clock.add(Duration.ofMillis(400));
        publish(a, holding(2, 0, 1));
        read(a);
        assertThat(credits(a)).isEqualTo(1);
        assertThat(ledger(a).getMinted()).isEqualTo(1);
        assertThat(a.effectiveAssignment(API_X, now())).isEqualTo(holding(2, 0));

        nextQuantum();
        read(a);
        assertThat(credits(a)).isEqualTo(2);
        assertThat(a.effectiveAssignment(API_X, now())).isEqualTo(holding(2, 0, 1));
    }

    /**
     * Covers R4 / KTD2. A snapshot published mid-quantum BEFORE the quantum was first read still does not
     * reach it: the effective snapshot is the one current at the quantum's start, not at its first read - the
     * in-process allocator's next-quantum rule for a joiner, so a fleet's instances cannot double-mint an
     * index by reading it late.
     */
    @Test
    void aSnapshotPublishedBeforeTheFirstReadOfAQuantumStillWaitsForTheNextOne() {
        PartitionShareResourceAllocator a = newInstance();
        publish(a, holding(2, 0));
        nextQuantum();
        clock.add(Duration.ofMillis(400));
        publish(a, holding(2, 0, 1));

        read(a);

        assertThat(credits(a)).isEqualTo(1);
        nextQuantum();
        read(a);
        assertThat(credits(a)).isEqualTo(2);
    }

    /**
     * Covers R4 in the lease-unchanged model the plan settled: a revoke snapshot (held minus one) published
     * mid-quantum leaves the current lease untouched and moves no counter; the next quantum's lease is the
     * smaller one. The revoked partition's share is last minted for the quantum the revocation was published
     * in and excluded from the next index on.
     */
    @Test
    void aRevokeMidQuantumLeavesTheCurrentLeaseAndMovesNoCounter() {
        PartitionShareResourceAllocator a = newInstance();
        publish(a, holding(2, 0, 1));
        nextQuantum();
        read(a);
        ConservationLedger beforeRevoke = ledger(a);
        assertThat(credits(a)).isEqualTo(2);

        clock.add(Duration.ofMillis(500));
        publish(a, holding(2, 0));

        assertThat(credits(a)).isEqualTo(2);
        assertThat(ledger(a)).isEqualTo(beforeRevoke);
        nextQuantum();
        read(a);
        assertThat(credits(a)).isEqualTo(1);
        assertThat(ledger(a).getExpired()).isEqualTo(2); // the two unspent credits expired at the boundary
        assertIdentityCloses(a);
    }

    /**
     * Covers R4's eager-protocol gap: a revoke-all followed by an assign in the same quantum mints nothing
     * for the quantum between them only if a boundary falls in the gap; here both land in one quantum and the
     * next quantum mints from the assign - undershoot at most, never overshoot.
     */
    @Test
    void anEagerRevokeAllThenAssignInOneQuantumMintsFromTheAssignNextQuantum() {
        PartitionShareResourceAllocator a = newInstance();
        publish(a, holding(2, 0));
        nextQuantum();
        read(a);
        publish(a, holding(2));
        publish(a, holding(2, 0, 1));

        nextQuantum();
        read(a);

        assertThat(credits(a)).isEqualTo(2);
    }

    // ------------------------------------------------------------------
    // R5: no partitions, or no known total, means no share
    // ------------------------------------------------------------------

    /**
     * Covers R5. An unresolved-total snapshot mints nothing and has no next credit; publishing a resolved
     * total resumes at the next boundary - and the next-credit projection says so as soon as it is published.
     */
    @Test
    void anUnresolvedTotalMintsNothingAndHasNoNextCreditUntilResolved() {
        PartitionShareResourceAllocator a = newInstance();
        publish(a, AssignmentSnapshot.unresolved(partitions(0)));
        nextQuantum();
        read(a);

        assertThat(credits(a)).isEqualTo(0);
        assertThat(ledger(a).getMinted()).isEqualTo(0);
        assertThat(nextCreditAt(a).isPresent()).isFalse();
        assertThat(a.localRatePerSecond(INSTANCE, API_X, now())).isEqualTo(0.0);

        publish(a, holding(2, 0));
        assertThat(nextCreditAt(a)).hasValue(Instant.ofEpochSecond(2));
        nextQuantum();
        read(a);
        assertThat(credits(a)).isEqualTo(1);
    }

    /**
     * Covers R5 / AE5. An instance holding no partitions of a resolved total has no share: nothing mints,
     * the local rate is zero, and the next-credit projection is empty because no rotation slot is its own.
     */
    @Test
    void holdingNoPartitionsMintsNothing() {
        PartitionShareResourceAllocator a = newInstance();
        publish(a, holding(2));
        nextQuantum();
        read(a);

        assertThat(credits(a)).isEqualTo(0);
        assertThat(ledger(a).getMinted()).isEqualTo(0);
        assertThat(nextCreditAt(a).isPresent()).isFalse();
        assertThat(a.localRatePerSecond(INSTANCE, API_X, now())).isEqualTo(0.0);
    }

    /** Covers R5 at the seam: before any publish, the allocator is in the unresolved state and mints nothing. */
    @Test
    void beforeAnyPublishNothingMints() {
        PartitionShareResourceAllocator a = newInstance();
        nextQuantum();
        read(a);

        assertThat(credits(a)).isEqualTo(0);
        assertThat(a.effectiveAssignment(API_X, now())).isEqualTo(AssignmentSnapshot.none());
        assertThat(nextCreditAt(a).isPresent()).isFalse();
    }

    // ------------------------------------------------------------------
    // KTD5: the next-credit projection is bounded by the partition total
    // ------------------------------------------------------------------

    /**
     * Covers KTD5 and the feasibility review's bound: the look-ahead is bounded by the subscription's partition
     * total, not the held count - a holder of one partition among twelve with a grant of one gets its credit
     * once per twelve quanta, and the projection must reach that far rather than give up after one.
     */
    @Test
    void nextCreditAtLooksAheadUpToThePartitionTotalNotTheHeldCount() {
        PartitionShareResourceAllocator a = new PartitionShareResourceAllocator(clock);
        a.register(new ResourceContract(API_X, 1.0, 1, QUANTUM)); // grant one per quantum
        publish(a, holding(12, 5));
        nextQuantum(); // quantum 1

        // slot 5 takes the single remainder credit when floorMod(5 - index, 12) == 0, i.e. at index 5
        assertThat(nextCreditAt(a)).hasValue(Instant.ofEpochSecond(5));
        for (int quantum = 2; quantum <= 12; quantum++) {
            nextQuantum();
            read(a);
        }
        assertThat(ledger(a).getMinted()).isEqualTo(1);
    }

    /**
     * Covers KTD5. When the current quantum's share is already minted the projection skips to the first
     * FUTURE quantum with a non-zero share - the plan's rotation, projected under the effective snapshot.
     */
    @Test
    void nextCreditAtSkipsQuantaTheRotationGivesThisInstanceNothingIn() {
        PartitionShareResourceAllocator small = newInstance();
        publish(small, holding(4, 3));
        nextQuantum(); // quantum 1: slot 3's shares run 0,0,1,1 from quantum 4, so quantum 1 is 0 and 2 is 1

        assertThat(nextCreditAt(small)).hasValue(Instant.ofEpochSecond(2));
    }

    // ------------------------------------------------------------------
    // The interface's other reads and the no-op lifecycle
    // ------------------------------------------------------------------

    /**
     * Covers the plan's settled share-through-the-interface decision: the local rate is the contract rate
     * times the effective snapshot's fraction (rotation-averaged, not the current index's share), and the
     * global rate is the contract rate - the view and gauges derive fraction = local / global from these.
     */
    @Test
    void localRateIsTheContractRateTimesTheFractionAndGlobalIsTheContractRate() {
        PartitionShareResourceAllocator big = newInstance();
        publish(big, holding(4, 0, 1, 2));
        nextQuantum();

        assertThat(big.globalRatePerSecond(API_X)).isEqualTo(2.0);
        assertThat(big.localRatePerSecond(INSTANCE, API_X, now())).isEqualTo(1.5);
        assertThat(big.localRatePerSecond(INSTANCE, "api-unknown", now())).isEqualTo(0.0);
        assertThat(big.globalRatePerSecond("api-unknown")).isEqualTo(0.0);
    }

    /**
     * Covers KTD1's no-op lifecycle: the group's assignment IS the membership, so join and leave change
     * nothing - not the share, not the counters - and a member that never joined still mints its share.
     */
    @Test
    void joinAndLeaveAreNoOpsBecauseTheAssignmentIsTheMembership() {
        PartitionShareResourceAllocator a = newInstance();
        publish(a, holding(2, 0, 1));
        nextQuantum();
        read(a); // never joined - mints regardless
        assertThat(credits(a)).isEqualTo(2);
        ConservationLedger before = ledger(a);

        a.leave(INSTANCE, now());
        a.join(INSTANCE, now());

        assertThat(ledger(a)).isEqualTo(before);
        assertThat(credits(a)).isEqualTo(2);
    }

    /** Covers KTD5. The resource-level next-credit time is the next quantum boundary, whatever the assignment. */
    @Test
    void resourceLevelNextCreditAtIsTheNextQuantumBoundary() {
        PartitionShareResourceAllocator a = newInstance();
        clock.add(Duration.ofMillis(2300));

        assertThat(a.nextCreditAt(API_X, now())).hasValue(Instant.ofEpochSecond(3));
        assertThat(a.nextCreditAt("api-unknown", now()).isPresent()).isFalse();
    }

    /** Covers R7: the registry's fail-fast rules are the in-process allocator's, through the shared registry. */
    @Test
    void registryFailFastRulesAreTheInProcessAllocators() {
        PartitionShareResourceAllocator a = newInstance();

        a.register(new ResourceContract(API_X, 2.0, 2, QUANTUM)); // identical: accepted
        assertThatThrownBy(() -> a.register(new ResourceContract(API_X, 5.0, 2, QUANTUM)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(API_X);
        assertThatThrownBy(() -> a.spend(INSTANCE, "api-unknown", now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api-unknown");
        assertThat(a.lookup(API_X)).hasValue(DEMO_POLICY);
    }

    // ------------------------------------------------------------------
    // KTD2: conservation under arbitrary interleavings
    // ------------------------------------------------------------------

    /**
     * Covers R10 / KTD2. A seeded pseudo-random storm of publishes (resolved, unresolved, empty), quantum
     * reads, spends (including overdraft debits) and clock advances - the conservation identity must hold at
     * EVERY observation point, not just at quiescence.
     */
    @Test
    void conservationIdentityHoldsAtEveryObservationPointUnderArbitraryPublishesReadsAndSpends() {
        Random random = new Random(20260905); // seeded: deterministic on the virtual clock
        PartitionShareResourceAllocator a = newInstance();
        for (int step = 0; step < 600; step++) {
            switch (random.nextInt(7)) {
                case 0 -> publish(a, holding(4, randomIndices(random)));
                case 1 -> publish(a, AssignmentSnapshot.unresolved(partitions(randomIndices(random))));
                case 2, 3 -> read(a);
                case 4 -> spend(a);
                case 5, 6 -> clock.add(Duration.ofMillis(random.nextInt(700)));
                default -> throw new IllegalStateException("unreachable");
            }
            assertIdentityCloses(a);
        }
        assertWithMessage("the storm must have minted something for the identity to be a real check")
                .that(ledger(a).getMinted()).isGreaterThan(0);
    }

    private static int[] randomIndices(Random random) {
        return random.ints(0, 4).distinct().limit(random.nextInt(5)).toArray();
    }

    // ------------------------------------------------------------------
    // AssignmentSnapshot: the value type's own contract
    // ------------------------------------------------------------------

    /** Covers R5 / KTD3: a resolved snapshot that cannot describe its own numerator is refused, naming it. */
    @Test
    void aResolvedSnapshotRefusesAHeldPartitionItsTotalsCannotPlace() {
        assertThatThrownBy(() -> AssignmentSnapshot.resolved(partitions(4), UniMaps.of(TOPIC, 4)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("orders-4");
        assertThatThrownBy(() -> AssignmentSnapshot.resolved(
                UniSets.of(new TopicPartition("elsewhere", 0)), UniMaps.of(TOPIC, 4)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("elsewhere-0");
        assertThatThrownBy(() -> AssignmentSnapshot.resolved(partitions(), UniMaps.of(TOPIC, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(TOPIC);
    }

    /** The snapshot is a value: equal by held set and totals, whatever order they were given in. */
    @Test
    void snapshotsAreValuesEqualByHeldSetAndTotals() {
        assertThat(holding(4, 0, 2)).isEqualTo(holding(4, 2, 0));
        assertThat(holding(4, 0, 2)).isNotEqualTo(holding(4, 0));
        assertThat(holding(4, 0, 2)).isNotEqualTo(holding(5, 0, 2));
        assertThat(AssignmentSnapshot.unresolved(partitions(0))).isNotEqualTo(holding(1, 0));
        assertThat(holding(4, 0, 2).fraction()).isEqualTo(0.5);
        assertThat(AssignmentSnapshot.none().fraction()).isEqualTo(0.0);
    }
}
