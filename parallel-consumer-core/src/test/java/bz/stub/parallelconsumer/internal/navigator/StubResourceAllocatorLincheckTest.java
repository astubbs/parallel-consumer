package bz.stub.parallelconsumer.internal.navigator;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

/**
 * REGRESSION DETECTOR for {@link StubResourceAllocator}'s concurrency contract (KTD11): no interleaving of
 * the allocator's operations may produce a result no sequential order could. Like
 * {@link bz.stub.parallelconsumer.state.RetryQueueLincheckTest} - and unlike the violation-expecting
 * harnesses in this lane - every green run is a claim that the single-monitor discipline holds, so its teeth
 * were measured with a control arm (below) rather than assumed.
 * <p>
 * <b>The scenario.</b> One fixed instant inside quantum 1, two members joined during quantum 0 (so both are
 * in quantum 1's division: one credit each under the KTD7 demo policy). Threads concurrently pull the quantum
 * ({@code readQuantum}), spend, leave, and read the lease and conservation ledger. The properties this
 * closes:
 * <ul>
 *   <li><b>Repeated and concurrent reads of an issued quantum agree</b> (R14): two concurrent
 *       {@code pullQuantum}s of the same quantum must be explainable as a sequential pair where the second is
 *       a no-op - any interleaving that minted twice would surface as a lease or ledger value no sequential
 *       order permits.</li>
 *   <li><b>The soft debit is atomic</b> (KTD1): concurrent spends of one credit must land as one decrement
 *       plus one overdraft, never a negative lease.</li>
 *   <li><b>The conservation identity survives interleaving</b> (KTD2): {@code ledger()} returns the whole
 *       counter snapshot as a value, so a torn or lost update shows up as an inconsistent snapshot.</li>
 * </ul>
 * <b>Control arm, measured 2026-09-01.</b> Dropping {@code synchronized} from
 * {@link StubResourceAllocator#spend} (predicted red) produced an Invalid-execution-results verdict; restored
 * byte-identical afterwards and this harness runs green. So a future edit weakening the monitor is what this
 * catches first.
 * <p>
 * <b>STRESS only</b>, per the lane's settled constraint ({@code docs/testing.md}): the model checker cannot
 * run over this project's Lombok value types ({@link ConservationLedger} is one);
 * {@code LincheckSuperHashCodeProbeTest} is the tripwire that fires when that is fixed.
 * <p>
 * The clock never advances inside a scenario, deliberately: with time frozen, every membership event is
 * effective NEXT quantum and quantum 1's division is a constant, which is exactly what makes a sequential
 * specification exist for Lincheck to check against. The time-driven semantics (expiry, TTL, rotation) are
 * {@link StubResourceAllocatorMintingTest}'s job on the virtual clock.
 *
 * @author Antony Stubbs
 */
@Tag("lincheck")
@Param(name = "member", gen = IntGen.class, conf = "0:1")
public class StubResourceAllocatorLincheckTest {

    private static final String API_X = "api-x";

    /** KTD7's demo policy: 2/sec, burst 2, 1s quantum - two members means one credit each per quantum. */
    private static final ResourceContract DEMO_POLICY =
            new ResourceContract(API_X, 2.0, 2, Duration.ofSeconds(1));

    private static final String[] MEMBERS = {"a", "b"};

    /** Joins land in quantum 0, so membership is effective for quantum 1 - where the scenario runs. */
    private static final Instant JOINED_AT = Instant.EPOCH;

    /** The one frozen observation instant, inside quantum 1. */
    private static final Instant NOW = Instant.ofEpochSecond(1);

    private final StubResourceAllocator allocator = new StubResourceAllocator();

    public StubResourceAllocatorLincheckTest() {
        allocator.register(DEMO_POLICY);
        allocator.join(MEMBERS[0], JOINED_AT);
        allocator.join(MEMBERS[1], JOINED_AT);
    }

    /**
     * Control-loop thread: the per-pass quantum pull (KTD4). Concurrent pulls of the one quantum are the
     * R14 identical-grant surface.
     */
    @Operation
    public void pullQuantum(@Param(name = "member") int member) {
        allocator.readQuantum(MEMBERS[member], NOW);
    }

    /**
     * Claim-path thread: the post-claim soft debit (KTD1) - always succeeds, overdrafting when the credit is
     * gone.
     */
    @Operation
    public void spend(@Param(name = "member") int member) {
        allocator.spend(MEMBERS[member], API_X, NOW);
    }

    /**
     * Claim-path reader: the pure eligibility read. {@code -1} distinguishes "no lease" from a zero-credit
     * lease, so a torn mint is visible.
     */
    @Operation
    public int liveCredits(@Param(name = "member") int member) {
        return allocator.currentLease(MEMBERS[member], API_X, NOW)
                .map(CapacityLease::getAvailableCredits)
                .orElse(-1);
    }

    /**
     * Lifecycle thread: close-entry (R16) - folds the leaver's unspent credits and marks the quantum still
     * issued, the edge the leave-then-read re-mint fix guards.
     */
    @Operation
    public void leaveB() {
        allocator.leave(MEMBERS[1], NOW);
    }

    /**
     * Observer: the whole conservation snapshot as one value (KTD2) - a lost or torn counter update surfaces
     * as a snapshot no sequential order produces.
     */
    @Operation
    public ConservationLedger ledger() {
        return allocator.conservationLedger(API_X, NOW);
    }

    /**
     * Bounds: every iteration runs to completion (a no-violation run cannot stop early), so this is a
     * straight cost - 100 x 500 covers the operation pairings many times over while staying well inside the
     * lane's whole-lane budget. The assertion is Lincheck's own: {@code check} throws, carrying the minimal
     * interleaving, the moment a result appears that no sequential order of the same operations could
     * produce. Do not convert a red here into a wider bound or a retry - on this class it means the monitor
     * discipline has been broken.
     */
    @Test
    void stressFindsNoWayToBreakTheAllocatorsMonitorDiscipline() {
        new StressOptions()
                .threads(2)
                .actorsPerThread(3)
                .actorsBefore(0)
                .actorsAfter(0)
                .iterations(100)
                .invocationsPerIteration(500)
                .check(getClass());
    }
}
