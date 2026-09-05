package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.errorprone.annotations.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.grantPerQuantum;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.quantumIndexOf;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.shareFor;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.startOfQuantum;

/**
 * The partition-share {@link ResourceAllocator} (the partition-share plan's KD2, KD3, KTD1): one PC instance's
 * share of a tagged resource is the fraction of the subscription's partitions it holds, minted locally per
 * quantum exactly as {@link StubResourceAllocator} mints from membership - and the consumer group's own
 * rebalance re-divides the rate when instances come and go. No control plane, nothing distributed on the
 * record path: every instance derives the same division from the assignment it already has.
 * <p>
 * <b>The slot is the partition's fleet-stable ordinal (KTD1).</b> For each resource and quantum index the
 * instance sums {@link QuantumArithmetic#shareFor shareFor(ordinal, totalPartitions, grant, index)} over the
 * partitions it holds, where {@link AssignmentSnapshot} defines the ordinal as the cumulative partition count
 * of the subscribed topics sorted by name before the partition's topic, plus its own index. Every instance
 * derives the same ordinals from the identical topic set R2 assumes, so the fleet's fractional shares sum to
 * exactly the grant with no communication (R3), and no holder of a partition starves - the remainder rotates
 * over the partition total. The burst budget is the same fraction of the contract's burst, rounded up to one
 * while any partition is held (R2).
 * <p>
 * <b>The assignment crosses threads as one immutable snapshot (KTD2).</b> The rebalance callbacks (engine
 * wiring, U3) {@link #publish} an {@link AssignmentSnapshot}; the allocator keeps the publication history in an
 * {@link AtomicReference} of an immutable list, so the callback takes no lock the control thread can hold.
 * Quantum {@code N} mints from the newest snapshot published BEFORE {@code N}'s start - a mid-quantum publish
 * affects the next index, the stub's next-quantum rule for a joiner - so a revoked partition's share is last
 * minted for the quantum its revocation was published in and excluded from the next index on, the new holder
 * first mints it at the next boundary, and no index is minted twice by one fleet on one clock. Under skewed
 * clocks a partition moved in the gap where two instances' indices disagree IS minted twice for one index;
 * that is the priced skew term (KD5), not a defect here.
 * <p>
 * <b>Lifecycle is the assignment's.</b> {@link #join} and {@link #leave} are no-ops, and there is no lease
 * TTL: the group's assignment is the membership, and an instance that stops reading quanta simply stops
 * minting. {@link #readQuantum} still mints once per quantum index (R14). The member id the interface carries
 * is kept as the ledger key only - one allocator serves ONE instance (the engine builds one per instance,
 * KTD4), and its share is the instance's whatever id reads it.
 * <p>
 * <b>Concurrency contract (KTD12).</b> The lease ledger is guarded by ONE monitor, {@link #stateLock}, and
 * carries {@code @GuardedBy} so Error Prone enforces the discipline; the publication history is an atomic
 * reference of an immutable value; the registry is a concurrent map. Conservation, overdraft and expiry are
 * {@link CreditLedger}'s - lifted from the in-process allocator, not reimplemented (KTD1).
 */
@Slf4j
public class PartitionShareResourceAllocator implements ResourceAllocator {

    /** The production time source (KTD4): taken at construction, used by the no-argument overloads. */
    private final Clock clock;

    /** Registered policies by resource name; its own thread safety is the concurrency control for that half. */
    private final ContractRegistry registry = new ContractRegistry();

    /** THE monitor (KTD12). Guards the lease ledger. */
    private final Object stateLock = new Object();

    /** The lease ledger and conservation counters (KTD2) - every call into it is made under the monitor. */
    @GuardedBy("stateLock")
    private final CreditLedger ledger = new CreditLedger(registry);

    /**
     * The publication history, oldest first, as an immutable list behind an atomic reference (KTD2): a
     * publish appends atomically and never blocks on {@link #stateLock}, a quantum resolves against it under
     * the monitor. Pruned at publish to the newest entry effective before the current quantum's start under
     * the largest registered quantum, plus everything after it - the entries any current or future quantum
     * can resolve to; nothing older can be the answer again.
     */
    private final AtomicReference<List<Publication>> publications =
            new AtomicReference<>(Collections.emptyList());

    public PartitionShareResourceAllocator() {
        this(Clock.systemUTC());
    }

    /**
     * @param clock the canonical time source (KTD4) - production wants UTC; the virtual-clock test lane
     *              passes a {@code MutableClock}
     */
    public PartitionShareResourceAllocator(Clock clock) {
        this.clock = clock;
    }

    // ------------------------------------------------------------------
    // Registry
    // ------------------------------------------------------------------

    @Override
    public void register(ResourceContract contract) {
        registry.register(contract);
    }

    @Override
    public Optional<ResourceContract> lookup(String resourceName) {
        return registry.lookup(resourceName);
    }

    // ------------------------------------------------------------------
    // The assignment handoff (KTD2) - the rebalance callbacks' entry
    // ------------------------------------------------------------------

    /**
     * Publishes the instance's current assignment as of {@code now}: effective for every quantum starting
     * AFTER {@code now}, never the one it lands in (R4, KTD2). The engine's rebalance callbacks call this with
     * held-minus-revoked on revoke or loss and held-plus-added with a refreshed total on assign; a declined
     * metadata read publishes an {@link AssignmentSnapshot#unresolved} snapshot, which R5 treats as no share.
     * Lock-free - an atomic append to an immutable history - so a callback never waits on the control loop.
     */
    public void publish(AssignmentSnapshot snapshot, Instant now) {
        Publication publication = new Publication(snapshot, now);
        List<Publication> updated = publications.updateAndGet(history -> {
            List<Publication> appended = new ArrayList<>(history.size() + 1);
            appended.addAll(history);
            appended.add(publication);
            return Collections.unmodifiableList(prune(appended, now));
        });
        log.debug("Published assignment {} at {} - effective from the next quantum; history depth {}",
                snapshot, now, updated.size());
    }

    /** {@link #publish(AssignmentSnapshot, Instant)} on the allocator's own clock - the production entry. */
    public void publish(AssignmentSnapshot snapshot) {
        publish(snapshot, clock.instant());
    }

    /**
     * Drops every entry that no current or future quantum can resolve to: the cutoff is the earliest current
     * quantum start across the registered contracts (the largest quantum wins), and the newest entry before
     * that cutoff is the last one any resolution still needs. With nothing registered the cutoff is
     * {@code now} itself.
     */
    private List<Publication> prune(List<Publication> history, Instant now) {
        Instant cutoff = now;
        for (ResourceContract contract : registry.all()) {
            Instant currentQuantumStart = startOfQuantum(quantumIndexOf(now, contract.getQuantum()),
                    contract.getQuantum());
            if (currentQuantumStart.isBefore(cutoff)) {
                cutoff = currentQuantumStart;
            }
        }
        int keepFrom = 0;
        for (int i = history.size() - 1; i >= 0; i--) {
            if (history.get(i).publishedAt.isBefore(cutoff)) {
                keepFrom = i;
                break;
            }
        }
        return new ArrayList<>(history.subList(keepFrom, history.size()));
    }

    /**
     * Pure read - the snapshot quantum {@code quantumIndex} of {@code contract} mints from: the newest one
     * published in an EARLIER quantum (KTD2), or {@link AssignmentSnapshot#none()} when nothing was.
     */
    private AssignmentSnapshot effectiveFor(ResourceContract contract, long quantumIndex) {
        return effectiveFor(publications.get(), contract, quantumIndex);
    }

    /** {@link #effectiveFor(ResourceContract, long)} over ONE read of the history, for a projection over several quanta. */
    private static AssignmentSnapshot effectiveFor(List<Publication> history, ResourceContract contract,
                                                   long quantumIndex) {
        for (int i = history.size() - 1; i >= 0; i--) {
            Publication publication = history.get(i);
            if (quantumIndexOf(publication.publishedAt, contract.getQuantum()) < quantumIndex) {
                return publication.snapshot;
            }
        }
        return AssignmentSnapshot.none();
    }

    /**
     * Pure read - the assignment the quantum containing {@code now} mints {@code resourceName} from (R18's
     * share view): the view and the share gauges read fraction and credits per quantum off this.
     */
    public AssignmentSnapshot effectiveAssignment(String resourceName, Instant now) {
        return registry.lookup(resourceName)
                .map(contract -> effectiveFor(contract, quantumIndexOf(now, contract.getQuantum())))
                .orElse(AssignmentSnapshot.none());
    }

    /**
     * Pure read - this instance's burst budget against {@code resourceName} for the quantum containing
     * {@code now} (R2): the contract's burst scaled by the effective fraction, rounded up to at least one
     * while a partition is held. {@code 0} when the resource is unknown.
     */
    public long burstBudget(String resourceName, Instant now) {
        return registry.lookup(resourceName)
                .map(contract -> effectiveFor(contract, quantumIndexOf(now, contract.getQuantum()))
                        .burstBudgetFor(contract))
                .orElse(0L);
    }

    // ------------------------------------------------------------------
    // Membership lifecycle - no-ops: the group's assignment is the membership (KTD1)
    // ------------------------------------------------------------------

    /**
     * A no-op (KTD1): membership is the consumer group's assignment, published through {@link #publish} from
     * the rebalance callbacks, so the running transition has nothing to declare here.
     */
    @Override
    public void join(String memberId, Instant now) {
        log.debug("join({}) is a no-op under partition-share - the assignment is the membership", memberId);
    }

    /**
     * A no-op (KTD1): a closing instance's partitions are revoked by the group, which is what stops its share
     * (R4). Its unspent credits expire at the boundary like any other; there is nothing to forfeit early
     * because nothing will spend them.
     */
    @Override
    public void leave(String memberId, Instant now) {
        log.debug("leave({}) is a no-op under partition-share - the assignment is the membership", memberId);
    }

    // ------------------------------------------------------------------
    // The per-pass quantum pull (KTD4) - mutating
    // ------------------------------------------------------------------

    @Override
    public void readQuantum(String memberId, Instant now) {
        synchronized (stateLock) {
            ledger.settle(now);
            for (ResourceContract contract : registry.all()) {
                mintShareIfUnissued(memberId, contract, now);
            }
        }
    }

    /** {@link #readQuantum(String, Instant)} on the allocator's own clock - the production entry point. */
    public void readQuantum(String memberId) {
        readQuantum(memberId, clock.instant());
    }

    @GuardedBy("stateLock")
    private void mintShareIfUnissued(String memberId, ResourceContract contract, Instant now) {
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        if (ledger.isIssued(memberId, contract.getName(), quantumIndex)) {
            return; // this quantum's grant is already issued - never re-mint (R14)
        }
        AssignmentSnapshot assignment = effectiveFor(contract, quantumIndex);
        long share = shareOf(assignment, contract, quantumIndex);
        ledger.mint(memberId, contract.getName(), quantumIndex, share);
    }

    /**
     * The instance's share of quantum {@code quantumIndex} under {@code assignment} (KTD1): the rotation
     * share of every held partition's ordinal, over the subscription's partition total. Zero while the total
     * is unresolved or nothing is held (R5).
     */
    private static long shareOf(AssignmentSnapshot assignment, ResourceContract contract, long quantumIndex) {
        int total = assignment.getTotalPartitions();
        if (!assignment.isResolved() || total == 0) {
            return 0;
        }
        long grant = grantPerQuantum(contract);
        long share = 0;
        for (int ordinal : assignment.getHeldOrdinals()) {
            share += shareFor(ordinal, total, grant, quantumIndex);
        }
        return share;
    }

    // ------------------------------------------------------------------
    // The soft debit (KTD1/KD10) - mutating
    // ------------------------------------------------------------------

    @Override
    public void spend(String memberId, String resourceName, Instant now) {
        ResourceContract contract = registry.require(resourceName);
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        synchronized (stateLock) {
            // the budget is read under the monitor with the debit it governs, so a publish cannot land
            // between the two and leave the debit judged against a snapshot the ledger never minted from
            long burstBudget = effectiveFor(contract, quantumIndex).burstBudgetFor(contract);
            ledger.settle(now);
            ledger.spend(memberId, contract, quantumIndex, burstBudget);
        }
    }

    /** {@link #spend(String, String, Instant)} on the allocator's own clock - the production entry point. */
    public void spend(String memberId, String resourceName) {
        spend(memberId, resourceName, clock.instant());
    }

    // ------------------------------------------------------------------
    // Pure reads (KTD1 eligibility, KTD5 wakeup, R18 views) - never mutate
    // ------------------------------------------------------------------

    @Override
    public Optional<CapacityLease> currentLease(String memberId, String resourceName, Instant now) {
        Optional<ResourceContract> contract = registry.lookup(resourceName);
        if (!contract.isPresent()) {
            return Optional.empty();
        }
        long quantumIndex = quantumIndexOf(now, contract.get().getQuantum());
        synchronized (stateLock) {
            return ledger.currentLease(memberId, contract.get(), quantumIndex);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Projected under the snapshot effective for each future quantum - so a snapshot published this quantum
     * is projected from the next one on. The look-ahead is bounded by the subscription's partition total,
     * the rotation's period: a holder of one partition among {@code P} with grant {@code G} waits up to
     * {@code ceil(P / G)} quanta for its slot. Empty when the resource is unknown, mints nothing, or no
     * effective snapshot gives this instance a slot (R5).
     */
    @Override
    public Optional<Instant> nextCreditAt(String memberId, String resourceName, Instant now) {
        Optional<ResourceContract> registered = registry.lookup(resourceName);
        if (!registered.isPresent() || grantPerQuantum(registered.get()) == 0) {
            return Optional.empty();
        }
        ResourceContract contract = registered.get();
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        List<Publication> history = publications.get(); // one read: the projection is over one history
        // the newest publication is effective from its next quantum on, so the projection settles on it
        // within one step; its total (and the current one's) bounds how far the rotation needs projecting
        int bound = Math.max(
                effectiveFor(history, contract, quantumIndex + 1).getTotalPartitions(),
                effectiveFor(history, contract, quantumIndex + 2).getTotalPartitions());
        for (int ahead = 1; ahead <= bound; ahead++) {
            AssignmentSnapshot assignment = effectiveFor(history, contract, quantumIndex + ahead);
            if (shareOf(assignment, contract, quantumIndex + ahead) > 0) {
                return Optional.of(startOfQuantum(quantumIndex + ahead, contract.getQuantum()));
            }
        }
        return Optional.empty(); // no slot is this instance's under any effective snapshot (R5)
    }

    @Override
    public Optional<Instant> nextCreditAt(String resourceName, Instant now) {
        Optional<ResourceContract> contract = registry.lookup(resourceName);
        if (!contract.isPresent() || grantPerQuantum(contract.get()) == 0) {
            return Optional.empty();
        }
        return Optional.of(startOfQuantum(quantumIndexOf(now, contract.get().getQuantum()) + 1,
                contract.get().getQuantum()));
    }

    @Override
    public double globalRatePerSecond(String resourceName) {
        return registry.lookup(resourceName).map(ResourceContract::getRatePerSecond).orElse(0.0);
    }

    /**
     * {@inheritDoc}
     * <p>
     * The contract rate times the effective snapshot's fraction - the rotation-averaged share, not the
     * current index's grant - so fraction {@code = local / global} and credits per quantum
     * {@code = local x quantum} are derivable from the two rate reads alone.
     */
    @Override
    public double localRatePerSecond(String memberId, String resourceName, Instant now) {
        return registry.lookup(resourceName)
                .map(contract -> contract.getRatePerSecond()
                        * effectiveFor(contract, quantumIndexOf(now, contract.getQuantum())).fraction())
                .orElse(0.0);
    }

    @Override
    public ConservationLedger conservationLedger(String resourceName, Instant now) {
        ResourceContract contract = registry.require(resourceName);
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        synchronized (stateLock) {
            return ledger.snapshot(resourceName, quantumIndex);
        }
    }

    /** One {@link #publish}: the snapshot and the instant it landed, effective from the quantum after it. */
    private static final class Publication {
        private final AssignmentSnapshot snapshot;
        private final Instant publishedAt;

        private Publication(AssignmentSnapshot snapshot, Instant publishedAt) {
            this.snapshot = snapshot;
            this.publishedAt = publishedAt;
        }
    }
}
