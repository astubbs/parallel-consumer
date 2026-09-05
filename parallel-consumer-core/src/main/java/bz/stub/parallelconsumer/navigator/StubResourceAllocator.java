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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.grantPerQuantum;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.quantumIndexOf;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.shareFor;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.startOfQuantum;

/**
 * The v1 in-process {@link ResourceAllocator} (KD2): an in-JVM implementation that honours the distributed
 * semantics a later Kafka-coordinated allocator must - equal share, expiring credits, death-loses-capacity,
 * no re-mint of an issued interval - behind the same seam, so that rung swaps transport rather than moving
 * the seam. {@link PartitionShareResourceAllocator} is that rung: it shares this class's arithmetic
 * ({@link QuantumArithmetic}), registry ({@link ContractRegistry}) and bookkeeping ({@link CreditLedger}),
 * and differs only in where the division comes from - the consumer group's assignment rather than an
 * in-process membership log.
 * <p>
 * <b>Quantum-indexed lazy minting (KTD4).</b> There is no minting thread and no mutable credit pool. Time on
 * the ONE canonical clock divides into quanta anchored at the epoch; the grant for quantum {@code N} is a pure
 * function of the policy and the membership effective before {@code N}'s start. A member's
 * {@link #readQuantum} pull materialises its share of the current quantum into its lease exactly once - a
 * repeated read of an issued quantum finds the lease already minted and returns the identical grant (R14).
 * Equal share divides integrally: everyone gets the floor, and the remainder rotates deterministically by
 * quantum index over the sorted member ordering, so no member starves and the total minted per quantum never
 * exceeds the policy grant.
 * <p>
 * <b>Concurrency contract (KTD11).</b> All mutable allocator state - the membership event log, the lease
 * ledger, and the per-quantum issued-membership record - is guarded by ONE monitor, {@link #stateLock}, and
 * every such field carries {@code @GuardedBy} so Error Prone enforces the discipline at compile time.
 * Membership for quantum {@code N} is resolved once, under the monitor, from events effective before
 * {@code N}'s start, and the resolved (immutable, sorted) snapshot is recorded per quantum - so concurrent
 * lazy reads of the same quantum reproduce identical grants even when a membership event or a lease-TTL
 * transition races the resolution. The share arithmetic itself ({@link QuantumArithmetic#shareFor}) is a
 * lock-free pure function over that immutable snapshot. The conservation counters live in the ledger and
 * move only under the monitor, so ledger snapshots are consistent (KTD2).
 * <p>
 * <b>Conservation (KTD2).</b> {@code minted + overdraft == spent + expired + outstanding}, outstanding
 * derived, no clamps anywhere. Expiry is derivable rather than scheduled: a lease's credits are unusable from
 * the quantum after its own, so a mutating call folds stale leases into the expired counter and a pure ledger
 * read counts them lazily without mutating - the identity holds at every observation point.
 */
@Slf4j
public class StubResourceAllocator implements ResourceAllocator {

    /**
     * How many quanta a member may go without a {@link #readQuantum} before its membership lapses (R16).
     * Greater than 1 so one missed control-loop pass (a GC pause, a slow commit) does not cost capacity;
     * small so a dead instance's share returns to the survivors within a few quanta.
     */
    public static final int MEMBERSHIP_LEASE_TTL_QUANTA = 3;

    /**
     * The production time source (KTD4): taken at construction, never an instance's module clock. Used by the
     * no-argument convenience overloads; the explicit-{@code now} methods trust their caller to have read the
     * same canonical clock.
     */
    private final Clock clock;

    /** Registered policies by resource name; its own thread safety is the concurrency control for that half. */
    private final ContractRegistry registry = new ContractRegistry();

    /**
     * THE monitor (KTD11). Guards the membership event log, the lease ledger, the last-read renewals, and
     * the issued-membership record below.
     */
    private final Object stateLock = new Object();

    /** The lease ledger and conservation counters (KTD2) - every call into it is made under the monitor. */
    @GuardedBy("stateLock")
    private final CreditLedger ledger = new CreditLedger(registry);

    /**
     * Append-only membership event log (R16): joins and leaves, each effective from the quantum AFTER its
     * instant. Membership-at-N replays only events from quanta before N, so a quantum's division never shifts
     * under a mid-quantum join or leave.
     */
    @GuardedBy("stateLock")
    private final List<MembershipEvent> membershipEvents = new ArrayList<>();

    /**
     * Each member's most recent {@link #readQuantum} instant - its membership lease renewal (KTD4). Seeded at
     * join so a joiner that never reads lapses after the TTL like any silent member. Monotonic per member.
     */
    @GuardedBy("stateLock")
    private final Map<String, Instant> lastQuantumRead = new HashMap<>();

    /**
     * The membership each (resource, quantum) was ISSUED with: resolved once under the monitor, immutable and
     * sorted, so concurrent and repeated reads of the same quantum reproduce the identical grant (R14, KTD4)
     * even when an event or TTL transition races the first resolution. A record of a pure function's value,
     * pruned to the current quantum by {@link #settle} - not a mutable pool.
     */
    @GuardedBy("stateLock")
    private final Map<String, Map<Long, List<String>>> issuedMemberships = new HashMap<>();

    public StubResourceAllocator() {
        this(Clock.systemUTC());
    }

    /**
     * @param clock the canonical time source (KTD4) - production wants UTC; the virtual-clock test lane
     *              passes the {@code MutableClock} it shares with every participating instance
     */
    public StubResourceAllocator(Clock clock) {
        this.clock = clock;
    }

    // ------------------------------------------------------------------
    // Registry (U1)
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
    // Membership lifecycle (R16) - mutating
    // ------------------------------------------------------------------

    @Override
    public void join(String memberId, Instant now) {
        synchronized (stateLock) {
            settle(now);
            membershipEvents.add(new MembershipEvent(memberId, true, now));
            renewLease(memberId, now);
        }
    }

    /** {@link #join(String, Instant)} on the allocator's own clock - the production entry point (KTD4). */
    public void join(String memberId) {
        join(memberId, clock.instant());
    }

    @Override
    public void leave(String memberId, Instant now) {
        synchronized (stateLock) {
            settle(now);
            membershipEvents.add(new MembershipEvent(memberId, false, now));
            // Death loses capacity (KD9/R6): the leaver's live unspent credits are gone NOW - expired, never
            // redistributed mid-window. Its share re-divides from the next quantum via the event above.
            ledger.forfeit(memberId);
        }
    }

    /** {@link #leave(String, Instant)} on the allocator's own clock - the production entry point (KTD4). */
    public void leave(String memberId) {
        leave(memberId, clock.instant());
    }

    // ------------------------------------------------------------------
    // The per-pass quantum pull (KTD4) - mutating
    // ------------------------------------------------------------------

    @Override
    public void readQuantum(String memberId, Instant now) {
        synchronized (stateLock) {
            settle(now);
            renewLease(memberId, now);
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
        List<String> members = issuedMembershipFor(contract, quantumIndex);
        int position = members.indexOf(memberId);
        if (position < 0) {
            return; // not a member for this quantum (not joined, joined this quantum, left, or TTL-lapsed)
        }
        long share = shareFor(position, members.size(), grantPerQuantum(contract), quantumIndex);
        ledger.mint(memberId, contract.getName(), quantumIndex, share);
    }

    // ------------------------------------------------------------------
    // The soft debit (KTD1/KD10) - mutating
    // ------------------------------------------------------------------

    @Override
    public void spend(String memberId, String resourceName, Instant now) {
        ResourceContract contract = registry.require(resourceName);
        synchronized (stateLock) {
            settle(now);
            // every member's overdraft budget is the whole declared burst - the in-process rung has no share
            // to scale it by (the partition-share rung scales it, R2 there)
            ledger.spend(memberId, contract, quantumIndexOf(now, contract.getQuantum()), contract.getBurst());
        }
    }

    /** {@link #spend(String, String, Instant)} on the allocator's own clock - the production entry point. */
    public void spend(String memberId, String resourceName) {
        spend(memberId, resourceName, clock.instant());
    }

    // ------------------------------------------------------------------
    // Pure reads (KTD1 eligibility, KTD5 wakeup, R18 views) - never renew, never mutate
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
    public Optional<Instant> nextCreditAt(String memberId, String resourceName, Instant now) {
        Optional<ResourceContract> registered = registry.lookup(resourceName);
        if (!registered.isPresent() || grantPerQuantum(registered.get()) == 0) {
            return Optional.empty();
        }
        ResourceContract contract = registered.get();
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        synchronized (stateLock) {
            List<String> members = issuedMembershipFor(contract, quantumIndex);
            int position = members.indexOf(memberId);
            if (position < 0) {
                // Not currently a member - the earliest it could hold credit is the next quantum (a
                // projection, not a promise: it still has to be a member by then).
                return Optional.of(startOfQuantum(quantumIndex + 1, contract.getQuantum()));
            }
            // Project the rotation forward under current membership: the first future quantum whose share for
            // this position is non-zero. Bounded by the member count - the rotation cycles at that period.
            long grant = grantPerQuantum(contract);
            for (int ahead = 1; ahead <= members.size(); ahead++) {
                if (shareFor(position, members.size(), grant, quantumIndex + ahead) > 0) {
                    return Optional.of(startOfQuantum(quantumIndex + ahead, contract.getQuantum()));
                }
            }
            return Optional.empty(); // unreachable while grant > 0, kept for totality
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
    public double localRatePerSecond(String memberId, String resourceName, Instant now) {
        Optional<ResourceContract> registered = registry.lookup(resourceName);
        if (!registered.isPresent()) {
            return 0.0;
        }
        ResourceContract contract = registered.get();
        synchronized (stateLock) {
            List<String> members = issuedMembershipFor(contract, quantumIndexOf(now, contract.getQuantum()));
            if (!members.contains(memberId)) {
                return 0.0;
            }
            return contract.getRatePerSecond() / members.size();
        }
    }

    @Override
    public ConservationLedger conservationLedger(String resourceName, Instant now) {
        ResourceContract contract = registry.require(resourceName);
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        synchronized (stateLock) {
            return ledger.snapshot(resourceName, quantumIndex);
        }
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    /**
     * Folds every stale lease into the expired counter (the ledger's settle) and prunes issued-membership
     * records from passed quanta. Called at the top of every MUTATING operation - pure reads instead account
     * for staleness lazily, so they never write.
     */
    @GuardedBy("stateLock")
    private void settle(Instant now) {
        ledger.settle(now);
        for (Map.Entry<String, Map<Long, List<String>>> entry : issuedMemberships.entrySet()) {
            ResourceContract contract = registry.require(entry.getKey());
            long currentIndex = quantumIndexOf(now, contract.getQuantum());
            entry.getValue().keySet().removeIf(issuedIndex -> issuedIndex < currentIndex);
        }
    }

    /** Renews {@code memberId}'s membership lease (KTD4) - monotonic, so an out-of-order call cannot regress it. */
    @GuardedBy("stateLock")
    private void renewLease(String memberId, Instant now) {
        lastQuantumRead.merge(memberId, now, (previous, candidate) ->
                candidate.isAfter(previous) ? candidate : previous);
    }

    /**
     * The membership quantum {@code quantumIndex} of {@code contract} is (or was) issued with: resolved once
     * from events effective before the quantum's start plus the lease-TTL predicate, then recorded so every
     * later read of the same quantum - concurrent or repeated - sees the identical division (R14, KTD4).
     */
    @GuardedBy("stateLock")
    private List<String> issuedMembershipFor(ResourceContract contract, long quantumIndex) {
        return issuedMemberships
                .computeIfAbsent(contract.getName(), name -> new HashMap<>())
                .computeIfAbsent(quantumIndex, index -> resolveMembership(contract, index));
    }

    @GuardedBy("stateLock")
    private List<String> resolveMembership(ResourceContract contract, long quantumIndex) {
        Set<String> current = new LinkedHashSet<>();
        for (MembershipEvent event : membershipEvents) {
            boolean effective = quantumIndexOf(event.at, contract.getQuantum()) < quantumIndex;
            if (!effective) {
                continue; // changes land at the NEXT quantum (R16) - this quantum's division predates them
            }
            if (event.join) {
                current.add(event.memberId);
            } else {
                current.remove(event.memberId);
            }
        }
        // The lease TTL (R16): a member whose control loop stopped reading quanta lapses; its capacity is
        // lost until re-division. A member is live for this quantum if its last read is within the TTL of the
        // quantum's start.
        Instant leaseDeadline = startOfQuantum(quantumIndex, contract.getQuantum())
                .minus(contract.getQuantum().multipliedBy(MEMBERSHIP_LEASE_TTL_QUANTA));
        // an explicit loop rather than removeIf: Error Prone's GuardedBy check cannot see that a lambda body
        // runs under the enclosing method's lock, and a suppression would silence the whole method
        Iterator<String> liveness = current.iterator();
        while (liveness.hasNext()) {
            Instant lastRead = lastQuantumRead.get(liveness.next());
            if (lastRead == null || lastRead.isBefore(leaseDeadline)) {
                liveness.remove();
            }
        }
        List<String> ordered = new ArrayList<>(current);
        Collections.sort(ordered); // the stable ordering the remainder rotation is defined over (KTD4)
        return Collections.unmodifiableList(ordered);
    }

    /** A join or leave (R16), effective from the quantum after {@link #at} - appended only under the monitor. */
    private static final class MembershipEvent {
        private final String memberId;
        private final boolean join;
        private final Instant at;

        private MembershipEvent(String memberId, boolean join, Instant at) {
            this.memberId = memberId;
            this.join = join;
            this.at = at;
        }
    }
}
