package bz.stub.parallelconsumer.navigator;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.TopicPartition;
import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.time.Instant;

/**
 * REGRESSION DETECTOR for {@link PartitionShareResourceAllocator}'s concurrency contract (the partition-share
 * plan's KTD12, the micro-MVP's KTD11): no interleaving of the allocator's operations may produce a result no
 * sequential order could. Like {@link StubResourceAllocatorLincheckTest}, every green run is a claim that
 * the single-monitor discipline holds, so its teeth were measured with a control arm (below) rather than
 * assumed.
 * <p>
 * <b>The scenario.</b> One fixed instant inside quantum 1, a resolved snapshot published during quantum 0 (so
 * it is effective for quantum 1: one partition of two under the KTD7 demo policy, one credit). Threads
 * concurrently pull the quantum ({@code readQuantum}), spend, publish a replacement snapshot stamped in
 * quantum 0 - effective for quantum 1 at once, so the publish RACES the mint rather than waiting for a
 * boundary the frozen clock never crosses - and read the lease, the ledger and the local rate. The
 * properties this closes:
 * <ul>
 *   <li><b>Repeated and concurrent reads of an issued quantum agree</b> (R14): two concurrent pulls must be
 *       explainable as a sequential pair where the second is a no-op - an interleaving that minted twice
 *       surfaces as a lease or ledger value no sequential order permits.</li>
 *   <li><b>The publish is atomic against the mint</b> (KTD2): a pull sees the snapshot before or after a
 *       concurrent publish, never a torn one - the atomic reference of an immutable value, and the
 *       resolve-once-under-the-monitor read of it.</li>
 *   <li><b>The soft debit is atomic</b> (KTD1): concurrent spends of one credit land as one decrement plus
 *       one overdraft, never a negative lease.</li>
 *   <li><b>The conservation identity survives interleaving</b> (KTD2): {@code ledger()} returns the whole
 *       counter snapshot as a value, so a torn or lost update shows up as an inconsistent snapshot.</li>
 * </ul>
 * <b>Control arm, measured 2026-09-05, two attempts.</b> Dropping {@code synchronized} from
 * {@link PartitionShareResourceAllocator#spend} alone never reached Lincheck: Error Prone refused the build
 * with {@code [GuardedBy] This access should be guarded by 'this.stateLock', which is not currently held} on
 * both ledger calls - the annotation is the first line of defence, and it fires at compile time. Dropping the
 * monitor AND the field's {@code @GuardedBy} together (predicted red) produced an
 * {@code Invalid execution results} verdict within the stress budget: {@code spend()} threw
 * {@code ConcurrentModificationException} from {@code HashMap.computeIfAbsent} in
 * {@code CreditLedger.countersFor} while the other thread's {@code ledger()} was reading the counters map
 * under the monitor. Restored byte-identical afterwards and this harness runs green. So a future edit
 * weakening the monitor is caught by the compiler first and by this harness second.
 * <p>
 * <b>STRESS only</b>, per the lane's settled constraint ({@code docs/testing.md}): the model checker cannot
 * run over this project's Lombok value types ({@link ConservationLedger} is one);
 * {@code LincheckSuperHashCodeProbeTest} is the tripwire that fires when that is fixed.
 * <p>
 * The clock never advances inside a scenario, deliberately: with time frozen, quantum 1's grant is a
 * constant per snapshot, which is exactly what makes a sequential specification exist for Lincheck to check
 * against. The time-driven semantics (expiry, next-quantum publication, rotation, skew) are
 * {@link PartitionShareResourceAllocatorTest}'s job on the virtual clock.
 *
 * @author Antony Stubbs
 */
@Tag("lincheck")
@Param(name = "held", gen = IntGen.class, conf = "1:2")
public class PartitionShareResourceAllocatorLincheckTest {

    private static final String API_X = "api-x";

    private static final String TOPIC = "orders";

    /** KTD7's demo policy: 2/sec, burst 2, 1s quantum - one partition of two means one credit per quantum. */
    private static final ResourceContract DEMO_POLICY =
            new ResourceContract(API_X, 2.0, 2, Duration.ofSeconds(1));

    private static final String INSTANCE = "pc-instance";

    /** Publishes land in quantum 0, so they are effective for quantum 1 - where the scenario runs. */
    private static final Instant PUBLISHED_AT = Instant.EPOCH;

    /** The one frozen observation instant, inside quantum 1. */
    private static final Instant NOW = Instant.ofEpochSecond(1);

    private final PartitionShareResourceAllocator allocator = new PartitionShareResourceAllocator();

    public PartitionShareResourceAllocatorLincheckTest() {
        allocator.register(DEMO_POLICY);
        allocator.publish(holding(1), PUBLISHED_AT);
    }

    /** {@code held} of the two demo partitions, as the rebalance callbacks would publish it. */
    private static AssignmentSnapshot holding(int held) {
        return AssignmentSnapshot.resolved(
                held == 1
                        ? UniSets.of(new TopicPartition(TOPIC, 0))
                        : UniSets.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1)),
                UniMaps.of(TOPIC, 2));
    }

    /**
     * Control-loop thread: the per-pass quantum pull (KTD4). Concurrent pulls of the one quantum are the
     * R14 identical-grant surface.
     */
    @Operation
    public void pullQuantum() {
        allocator.readQuantum(INSTANCE, NOW);
    }

    /**
     * Claim-path thread: the post-claim soft debit (KTD1) - always succeeds, overdrafting when the credit is
     * gone.
     */
    @Operation
    public void spend() {
        allocator.spend(INSTANCE, API_X, NOW);
    }

    /**
     * Rebalance-callback thread: publishes a replacement snapshot stamped in quantum 0, so it is effective
     * for the frozen quantum and races the mint (KTD2's atomic handoff).
     */
    @Operation
    public void publish(@Param(name = "held") int held) {
        allocator.publish(holding(held), PUBLISHED_AT);
    }

    /**
     * Claim-path reader: the pure eligibility read. {@code -1} distinguishes "no lease" from a zero-credit
     * lease, so a torn mint is visible.
     */
    @Operation
    public int liveCredits() {
        return allocator.currentLease(INSTANCE, API_X, NOW)
                .map(CapacityLease::getAvailableCredits)
                .orElse(-1);
    }

    /** View reader: the share as the context view and the gauges read it - rate times the effective fraction. */
    @Operation
    public double localRate() {
        return allocator.localRatePerSecond(INSTANCE, API_X, NOW);
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
     * discipline, or the atomic snapshot handoff, has been broken.
     */
    @Test
    void stressFindsNoWayToBreakTheAllocatorsMonitorDisciplineOrTheSnapshotHandoff() {
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
