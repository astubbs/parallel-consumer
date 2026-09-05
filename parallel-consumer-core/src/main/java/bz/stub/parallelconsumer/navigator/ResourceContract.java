package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

import java.time.Duration;

/**
 * The policy a named, shared, rate-limited resource is registered under (R1): the resource's declared rate plus
 * burst define its overshoot bound (R8, R12), and {@link #getQuantum()} is the cadence the allocator mints credit
 * on (KTD4).
 * <p>
 * Immutable and value-equal ({@link Value} generates {@code equals}/{@code hashCode} over every field), which is
 * exactly the property {@link StubResourceAllocator#register} depends on to tell an identical re-registration
 * (accepted) apart from a policy collision (rejected, R19).
 * <p>
 * This is the {@code ResourceContract} of the seam vocabulary in
 * {@code docs/ideation/2026-08-29-hasten-compound-engineering-handoff.md} section 24. U1 keeps it a plain data
 * carrier - no validation of its own fields beyond what the {@code @Value} constructor already gives for free
 * (non-primitive fields still accept {@code null}); registration-time fail-fast (R4, R19) lives on
 * {@link ResourceAllocator#register}, not here.
 *
 * <h2>What this contract promises across a fleet</h2>
 *
 * The partition-share plan's R8, stated here because this is where the contract is documented. Under the
 * default {@link bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy#PARTITION_SHARE}
 * strategy the instances of ONE consumer group divide this contract between them - each takes the fraction of
 * its subscription's partitions it currently holds and mints that fraction locally
 * ({@link PartitionShareResourceAllocator}) - so every promise below is the fleet's, not one JVM's. Under
 * {@link bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy#IN_PROCESS} or
 * {@link bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy#CUSTOM} the contract promises
 * only what the allocator you supply implements.
 * <p>
 * <b>Credits per quantum.</b> Each quantum mints {@code floor(ratePerSecond x quantum)} credits for the whole
 * fleet - the grant - and each instance mints its own fraction of that grant. Minting is lazy and per
 * instance: nothing is minted for a quantum until that instance reads the quantum, on that instance's own
 * clock. Spent credits are gone, and unspent ones do not carry into the next quantum.
 * <p>
 * <b>Burst is one fleet-wide overdraft budget, divided - not a budget per process.</b> An instance's budget
 * is {@code ceil(burst x fraction)}, so a quarter-share of a burst of four may overdraw by one credit. The
 * rounding up is deliberate: a holder of at least one partition never ends up with no budget at all. Its
 * price is slack, and the bound below carries it - the fleet's summed budgets can exceed {@code burst} by up
 * to one credit per partition-holding instance.
 * <p>
 * <b>Under a stable assignment there is no overshoot.</b> Over any window starting on a quantum boundary,
 * while the assignment does not change, the fleet mints at most {@code ratePerSecond x window}. A share below
 * one whole credit per quantum does not round up into an extra credit: it accrues through a remainder
 * rotation over the partitions, so the per-quantum shares of every partition holder sum to exactly the grant
 * at every quantum index.
 * <p>
 * <b>Across a rebalance the overshoot is bounded, and the bound is measured rather than asserted.</b> Over
 * any quantum-aligned window in which instances join or leave, the fleet's firings stay within
 * {@code ratePerSecond x window + burst + one quantum's credits} while every instance's clock agrees about
 * the current quantum index. When clocks disagree the bound carries a further skew term (KD5, KTD11): for
 * each partition that moves between two holders whose clocks disagree about the index, that partition's whole
 * per-quantum share - both holders mint that index - rounded up to a whole credit and summed over the moves
 * the window contains. The bound's <b>domain is what the churn ladder actually ran, and the ladder's own
 * dated record states it</b> - rungs of a few instances, under both the range and the cooperative-sticky
 * assignor, with injected clock offsets of zero and of plus and minus a fraction of a quantum. Nothing is
 * claimed outside it, and where this paragraph and the record disagree about the domain the record wins,
 * because it is the run's own output. The <b>rule</b> that turns the ladder's measurements into these
 * numbers is equally fixed: on a zero-offset rung the bound holds with no
 * re-derivation and any crossing of it is a defect, and only the skew term may be re-derived - by a
 * derivation written into the ladder's dated record before the re-run that tests it. That record is written
 * by the ladder run itself and committed under {@code docs/test-hardening/} with the CI run URL that produced
 * it.
 * <p>
 * <b>Utilisation: the rate is exhausted, not merely respected - as far as demand allows.</b> No holder of at
 * least one partition starves, because the remainder rotation gives every partition's slot its turn. An
 * instance holding no partitions has no share and mints nothing, so running more instances than partitions is
 * safe rather than a stall. The cost is the other half of the same property: a share nobody has work for goes
 * unspent - unminted or expired. Unminted is the common case, because minting is lazy and an idle control
 * loop reads only a minority of its quanta. So under uneven demand the fleet runs below the declared rate by
 * the idle fraction, and the remedy is demand-weighted allocation, which is the controller rung's, not this
 * one's.
 * <p>
 * <b>The scope is one consumer group.</b> A second group whose instances tag the same resource name receives
 * the full rate again, so the effective aggregate multiplies by the number of groups. That is the documented
 * scope on this rung, not a defect; sharing a resource across applications belongs to the controller rung.
 * <p>
 * <b>The convergence clock.</b> A departed instance's share is unavailable to the survivors until the group
 * rebalances - after {@code session.timeout.ms} for a killed process, at once for one that closes cleanly,
 * and under static membership after that same timeout at its configured length - and the survivors then mint
 * the larger share from the next quantum boundary. A joining instance mints from the first quantum boundary
 * after its assignment. Within the quantum a move lands in, the lease is unchanged: a revoked partition's
 * share is last minted for the quantum in which the revocation was published, and the new holder first mints
 * it at the next boundary. A move therefore undershoots for at most one quantum rather than minting one index
 * twice.
 * <p>
 * <b>What one instance can tell you.</b> {@link NavigatorView#shareFraction} and
 * {@link NavigatorView#creditsPerQuantum} report this instance's current share of a tagged resource, and the
 * {@code pc.navigator.share.fraction} and {@code pc.navigator.share.credits.per.quantum} meters publish the
 * same two numbers - so "why am I at 1Hz" is answerable from one JVM without querying the fleet.
 */
@Value
public class ResourceContract {

    /**
     * The resource's name - what a function's {@code resourceTags} and a registration's collision check both key
     * on.
     */
    String name;

    /**
     * The declared rate, in credits (tokens) per second (R1). v1's demo policy is {@code 2.0} (KTD7).
     * <p>
     * {@code 0} is legal and intentional - the deliberate shut valve, always-blocked for every tagged member.
     * Any other non-negative rate must still mint at least one whole credit per quantum
     * ({@code floor(rate x quantum) > 0}); {@link ResourceAllocator#register} rejects a policy that cannot
     * (R19).
     */
    double ratePerSecond;

    /**
     * The burst allowance folded into the overshoot bound alongside {@link #getRatePerSecond()} (R1, R8) - v1's
     * demo policy is {@code 2} (KTD7), one quantum's worth.
     */
    int burst;

    /**
     * The cadence the allocator mints credit on (KTD4) - v1's demo policy is one second (KTD7).
     */
    Duration quantum;
}
