package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.grantPerQuantum;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.quantumIndexOf;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.startOfQuantum;

/**
 * What the in-process and the partition-share allocators have in common, so it is written once: a
 * {@link ContractRegistry}, a {@link CreditLedger} behind ONE monitor, and every seam method whose body is
 * "find the contract, index the quantum, act on the ledger under the monitor" - registration, the per-pass
 * quantum pull, the soft debit, the lease read, the conservation snapshot and the resource-wide reads. A
 * subclass supplies only where its division comes from: {@link #shareOf} says how many credits this member
 * is entitled to for a quantum (or that it is not a participant), {@link #burstBudgetFor} says how much of
 * the declared burst this member may overdraw, and the two hooks {@link #onSettle} and {@link #onQuantumRead}
 * let a subclass fold its own bookkeeping into the same monitor. Membership, the member-side next-credit
 * projection and the local rate stay with the subclass, because those ARE the division.
 * <p>
 * <b>Concurrency contract (KTD11, KTD12).</b> {@link #stateLock} is THE monitor: the ledger and every
 * subclass field that resolves a quantum's division are guarded by it and carry {@code @GuardedBy}, so
 * Error Prone enforces the discipline at compile time. The hooks are called under the monitor and are
 * annotated as such; a subclass override must keep the annotation.
 * <p>
 * Package-private on purpose: the two allocators are the seam's implementations, the base is their shared
 * body, and nothing outside this package should extend it.
 */
abstract class LedgerBackedResourceAllocator implements ResourceAllocator {

    /** Returned by {@link #shareOf} when the member is not a participant of that quantum: nothing is issued. */
    static final long NOT_A_PARTICIPANT = -1;

    /**
     * The production time source (KTD4): taken at construction, never an instance's module clock. Used by the
     * no-argument convenience overloads; the explicit-{@code now} methods trust their caller to have read the
     * same canonical clock.
     */
    final Clock clock;

    /** Registered policies by resource name; its own thread safety is the concurrency control for that half. */
    final ContractRegistry registry = new ContractRegistry();

    /** THE monitor (KTD11, KTD12). Guards the lease ledger and every subclass field annotated with it. */
    final Object stateLock = new Object();

    /** The lease ledger and conservation counters (KTD2) - every call into it is made under the monitor. */
    @GuardedBy("stateLock")
    final CreditLedger ledger = new CreditLedger(registry);

    LedgerBackedResourceAllocator(Clock clock) {
        this.clock = clock;
    }

    // ------------------------------------------------------------------
    // What a subclass supplies
    // ------------------------------------------------------------------

    /**
     * The credits {@code memberId} is entitled to for quantum {@code quantumIndex} of {@code contract} under
     * the division effective for that quantum - what the pull mints - or {@link #NOT_A_PARTICIPANT} when the
     * member holds no place in that quantum's division at all, in which case nothing is issued and a later
     * read of the same quantum may still find one.
     */
    @GuardedBy("stateLock")
    abstract long shareOf(String memberId, ResourceContract contract, long quantumIndex);

    /** The overdraft budget {@code memberId}'s debits in quantum {@code quantumIndex} are judged against (R8). */
    @GuardedBy("stateLock")
    abstract long burstBudgetFor(String memberId, ResourceContract contract, long quantumIndex);

    /**
     * Folds every stale lease into the expired counter, then lets the subclass prune whatever else time
     * passing invalidates ({@link #onSettle}). Called at the top of every MUTATING operation - pure reads
     * instead account for staleness lazily, so they never write.
     */
    @GuardedBy("stateLock")
    final void settle(Instant now) {
        ledger.settle(now);
        onSettle(now);
    }

    /** Runs under the monitor after the ledger has settled, for a subclass's own time-keyed bookkeeping. */
    @GuardedBy("stateLock")
    void onSettle(Instant now) {
    }

    /** Runs under the monitor at the top of every {@link #readQuantum} pull, before any share is minted. */
    @GuardedBy("stateLock")
    void onQuantumRead(String memberId, Instant now) {
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
    // The per-pass quantum pull (KTD4) - mutating
    // ------------------------------------------------------------------

    @Override
    public void readQuantum(String memberId, Instant now) {
        synchronized (stateLock) {
            settle(now);
            onQuantumRead(memberId, now);
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
            return; // this quantum's grant is already issued to this member - never re-mint (R14)
        }
        long share = shareOf(memberId, contract, quantumIndex);
        if (share == NOT_A_PARTICIPANT) {
            return;
        }
        ledger.mint(memberId, contract.getName(), quantumIndex, share);
    }

    // ------------------------------------------------------------------
    // The soft debit (KTD1, the micro-MVP plan's KD10) - mutating
    // ------------------------------------------------------------------

    @Override
    public void spend(String memberId, String resourceName, Instant now) {
        ResourceContract contract = registry.require(resourceName);
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        synchronized (stateLock) {
            settle(now);
            // the budget is read under the monitor with the debit it governs, so nothing can land between
            // the two and leave the debit judged against a division the ledger never minted from
            long burstBudget = burstBudgetFor(memberId, contract, quantumIndex);
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

    @Override
    public ConservationLedger conservationLedger(String resourceName, Instant now) {
        ResourceContract contract = registry.require(resourceName);
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        synchronized (stateLock) {
            return ledger.snapshot(resourceName, quantumIndex);
        }
    }
}
