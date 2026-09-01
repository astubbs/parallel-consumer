package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.errorprone.annotations.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Duration;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * The v1 in-process {@link ResourceAllocator} (KD2): an in-JVM implementation that honours the distributed
 * semantics a later Kafka-coordinated allocator must - equal share, expiring credits, death-loses-capacity,
 * no re-mint of an issued interval - behind the same seam, so that rung swaps transport rather than moving
 * the seam.
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
 * <b>Concurrency contract (KTD11).</b> All mutable allocator state - the membership event log, the per-member
 * lease ledgers, and the per-quantum issued-membership record - is guarded by ONE monitor,
 * {@link #stateLock}, and every such field carries {@code @GuardedBy} so Error Prone enforces the discipline
 * at compile time. Membership for quantum {@code N} is resolved once, under the monitor, from events
 * effective before {@code N}'s start, and the resolved (immutable, sorted) snapshot is recorded per quantum -
 * so concurrent lazy reads of the same quantum reproduce identical grants even when a membership event or a
 * lease-TTL transition races the resolution. The share arithmetic itself
 * ({@link #shareFor(int, int, long, long)}) is a lock-free pure function over that immutable snapshot. The
 * conservation counters are {@link LongAdder}s, moved only under the monitor so ledger snapshots are
 * consistent (KTD2).
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

    /**
     * Registered policies by resource name. Concurrent because several PC instances' builders may register
     * overlapping resources at construction time (KD11); the map's own thread safety is the concurrency
     * control for the registry half.
     */
    private final Map<String, ResourceContract> registry = new ConcurrentHashMap<>();

    /**
     * Per-resource conservation counters, created at registration. The {@link LongAdder}s are only ever moved
     * while {@link #stateLock} is held, so a ledger snapshot taken under the monitor is consistent (KTD2).
     */
    private final Map<String, Counters> counters = new ConcurrentHashMap<>();

    /**
     * THE monitor (KTD11). Guards the membership event log, the lease ledgers, the last-read renewals, and
     * the issued-membership record below.
     */
    private final Object stateLock = new Object();

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
     * Live lease ledgers: memberId → resourceName → the member's minted-and-partly-spent slice of one
     * quantum. A stale entry (its quantum passed) is unusable and is folded into the expired counter by the
     * next mutating call; pure reads count it lazily instead.
     */
    @GuardedBy("stateLock")
    private final Map<String, Map<String, LeaseState>> leases = new HashMap<>();

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
        if (contract.getQuantum() == null || contract.getQuantum().isZero() || contract.getQuantum().isNegative()) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' declares a non-positive quantum {} - the minting cadence must be a positive "
                            + "duration (R19 fail-fast).",
                    contract.getName(), contract.getQuantum()));
        }
        if (contract.getRatePerSecond() < 0 || contract.getBurst() < 0) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' declares a negative rate ({}) or burst ({}) - both must be non-negative "
                            + "(R19 fail-fast).",
                    contract.getName(), contract.getRatePerSecond(), contract.getBurst()));
        }
        // A positive rate that floors to zero credits per quantum is not a slow resource - it is a permanent
        // stall: grantPerQuantum == 0 means nextCreditAt returns empty, no lease ever mints, and no wakeup
        // exists to break it, so a tagged record starves silently forever. A rate of exactly 0 is the
        // deliberate shut valve and stays legal (R19).
        if (contract.getRatePerSecond() > 0 && grantPerQuantum(contract) == 0) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' declares rate {}/s over quantum {} - floor(rate x quantum) mints ZERO "
                            + "credits every quantum, so every tagged member would starve forever with no "
                            + "wakeup to break it. Raise the quantum or the rate so at least one whole credit "
                            + "mints per quantum (R19 fail-fast).",
                    contract.getName(), contract.getRatePerSecond(), contract.getQuantum()));
        }
        ResourceContract existing = registry.putIfAbsent(contract.getName(), contract);
        if (existing != null && !existing.equals(contract)) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' is already registered with policy {} - cannot re-register it with a " +
                            "DIFFERENT policy {}. Registering the identical policy again is accepted (several " +
                            "instances may each register the resources they share); changing it is a " +
                            "configuration error (R19).",
                    contract.getName(), existing, contract));
        }
        counters.putIfAbsent(contract.getName(), new Counters());
    }

    @Override
    public Optional<ResourceContract> lookup(String resourceName) {
        return Optional.ofNullable(registry.get(resourceName));
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
            // The zeroed lease record is KEPT as the quantum's issued-marker: a straggling readQuantum after
            // the leave must find the quantum already issued, never re-mint it (R14, KTD4's per-quantum
            // grant bound). The record leaves at the next settle, like any stale lease.
            Map<String, LeaseState> memberLeases = leases.get(memberId);
            if (memberLeases != null) {
                for (Map.Entry<String, LeaseState> entry : memberLeases.entrySet()) {
                    countersFor(entry.getKey()).expired.add(entry.getValue().unspent());
                    entry.getValue().spent = entry.getValue().granted;
                }
            }
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
            for (ResourceContract contract : registry.values()) {
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
        Map<String, LeaseState> memberLeases = leases.get(memberId);
        LeaseState existing = memberLeases == null ? null : memberLeases.get(contract.getName());
        if (existing != null && existing.quantumIndex >= quantumIndex) {
            return; // this quantum's grant is already issued to this member - never re-mint (R14)
        }
        List<String> members = issuedMembershipFor(contract, quantumIndex);
        int position = members.indexOf(memberId);
        if (position < 0) {
            return; // not a member for this quantum (not joined, joined this quantum, left, or TTL-lapsed)
        }
        long share = shareFor(position, members.size(), grantPerQuantum(contract), quantumIndex);
        if (share == 0) {
            return; // rotation gives this member nothing this quantum - nothing minted, nothing to expire
        }
        leases.computeIfAbsent(memberId, id -> new HashMap<>())
                .put(contract.getName(), new LeaseState(quantumIndex, share));
        countersFor(contract.getName()).minted.add(share);
    }

    // ------------------------------------------------------------------
    // The soft debit (KTD1/KD10) - mutating
    // ------------------------------------------------------------------

    @Override
    public void spend(String memberId, String resourceName, Instant now) {
        ResourceContract contract = requireRegistered(resourceName);
        synchronized (stateLock) {
            settle(now);
            Counters resourceCounters = countersFor(resourceName);
            resourceCounters.spent.add(1);
            long quantumIndex = quantumIndexOf(now, contract.getQuantum());
            Map<String, LeaseState> memberLeases = leases.get(memberId);
            LeaseState lease = memberLeases == null ? null : memberLeases.get(resourceName);
            boolean liveCreditRemains = lease != null
                    && lease.quantumIndex == quantumIndex
                    && lease.unspent() > 0;
            if (liveCreditRemains) {
                lease.spent++;
            } else {
                // The always-succeeds rule (KTD1): the credit observed at eligibility is gone - the quantum
                // rolled, or a concurrent claimer spent it. Overdraft, monotonic; never negative bookkeeping,
                // never a refund, never re-minting. R8's burst term BUDGETS exactly this - it does not cap
                // it, so the budget is watched below rather than enforced.
                resourceCounters.overdraft.add(1);
                trackOverdraftAgainstBurstBudget(resourceCounters, contract, quantumIndex);
            }
        }
    }

    /**
     * R8's overshoot budget, made observable - never enforced, because KTD1 forbids refusing the debit. The
     * contract's burst is how much overdraft one quantum is EXPECTED to accumulate: the racing debits that land
     * between an eligibility read and the spend. A debit pushing the quantum's cumulative overdraft BEYOND that
     * budget still succeeded and is already in the ordinary overdraft counter (the conservation identity is
     * untouched); additionally it moves the monotonic beyond-burst counter and, once per (resource, quantum),
     * WARNs. The single-threaded selection engine keeps debits within budget structurally, so a nonzero count
     * means concurrent direct-pull claimers - or a caller outside the engine's discipline - are outrunning the
     * declared policy.
     *
     * <p>The reset is monotonic, mirroring {@link #renewLease}'s merge against the identical hazard: the
     * observation instant behind {@code quantumIndex} is read in {@code WorkContainer.onQueueingForExecution}
     * outside {@code stateLock}, and that call is concurrently reachable under the direct-pull engine, so two
     * spends can reach this method with their timestamps - and therefore their quantum indices - inverted. A
     * bare {@code !=} reset would let a straggler carrying an OLDER quantum's index zero the CURRENT quantum's
     * cumulative overdraft, undercounting beyond-burst and re-arming the once-per-quantum warn. So the budget
     * only ever advances forward: {@code quantumIndex > overdraftBudgetQuantumIndex} rolls to a fresh budget;
     * anything else - the same quantum accumulating further, or an out-of-order straggler from an earlier one -
     * folds into whatever budget is already current, without moving the index backward.
     */
    @GuardedBy("stateLock")
    private void trackOverdraftAgainstBurstBudget(Counters resourceCounters, ResourceContract contract,
                                                  long quantumIndex) {
        if (quantumIndex > resourceCounters.overdraftBudgetQuantumIndex) {
            // a genuine advance - the quantum rolled since the last overdraft landed, so a fresh budget starts
            resourceCounters.overdraftBudgetQuantumIndex = quantumIndex;
            resourceCounters.overdraftInQuantum = 0;
        }
        // quantumIndex <= overdraftBudgetQuantumIndex: either this quantum's own accumulation, or a
        // straggler's older index - both fold into the CURRENT budget rather than resetting it
        resourceCounters.overdraftInQuantum++;
        if (resourceCounters.overdraftInQuantum > contract.getBurst()) {
            resourceCounters.overdraftBeyondBurst.add(1);
            if (resourceCounters.overdraftInQuantum == contract.getBurst() + 1L) { // the crossing: once per quantum
                log.warn("Resource '{}': quantum {}'s cumulative overdraft ({}) has exceeded the declared burst "
                                + "budget of {}. The debit still succeeded (KTD1's always-succeeds rule) and the "
                                + "conservation ledger is untouched - but spends are outrunning R8's "
                                + "rate x window + burst bound; the pc.navigator.credits.overdraft.beyond.burst "
                                + "meter counts these debits. Warning once per quantum.",
                        contract.getName(), quantumIndex, resourceCounters.overdraftInQuantum, contract.getBurst());
            }
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
        ResourceContract contract = registry.get(resourceName);
        if (contract == null) {
            return Optional.empty();
        }
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        synchronized (stateLock) {
            Map<String, LeaseState> memberLeases = leases.get(memberId);
            LeaseState lease = memberLeases == null ? null : memberLeases.get(resourceName);
            if (lease == null || lease.quantumIndex != quantumIndex) {
                return Optional.empty(); // nothing pulled, or the lease's quantum has passed - expired (R6)
            }
            return Optional.of(new CapacityLease(
                    resourceName,
                    quantumIndex,
                    Math.toIntExact(lease.unspent()),
                    startOfQuantum(quantumIndex + 1, contract.getQuantum())));
        }
    }

    @Override
    public Optional<Instant> nextCreditAt(String memberId, String resourceName, Instant now) {
        ResourceContract contract = registry.get(resourceName);
        if (contract == null || grantPerQuantum(contract) == 0) {
            return Optional.empty();
        }
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
        ResourceContract contract = registry.get(resourceName);
        if (contract == null || grantPerQuantum(contract) == 0) {
            return Optional.empty();
        }
        return Optional.of(startOfQuantum(quantumIndexOf(now, contract.getQuantum()) + 1, contract.getQuantum()));
    }

    @Override
    public double globalRatePerSecond(String resourceName) {
        ResourceContract contract = registry.get(resourceName);
        return contract == null ? 0.0 : contract.getRatePerSecond();
    }

    @Override
    public double localRatePerSecond(String memberId, String resourceName, Instant now) {
        ResourceContract contract = registry.get(resourceName);
        if (contract == null) {
            return 0.0;
        }
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
        ResourceContract contract = requireRegistered(resourceName);
        long quantumIndex = quantumIndexOf(now, contract.getQuantum());
        synchronized (stateLock) {
            Counters resourceCounters = countersFor(resourceName);
            // Expiry is derivable (KTD4's lazy minting): a stale lease's unspent credits are already expired
            // in effect, counted here WITHOUT folding - a pure read mutates nothing, and the identity holds
            // at every observation point.
            long lazilyExpired = 0;
            long liveCredits = 0;
            for (Map<String, LeaseState> memberLeases : leases.values()) {
                LeaseState lease = memberLeases.get(resourceName);
                if (lease == null) {
                    continue;
                }
                if (lease.quantumIndex < quantumIndex) {
                    lazilyExpired += lease.unspent();
                } else if (lease.quantumIndex == quantumIndex) {
                    liveCredits += lease.unspent();
                }
            }
            return new ConservationLedger(
                    resourceName,
                    resourceCounters.minted.sum(),
                    resourceCounters.spent.sum(),
                    resourceCounters.expired.sum() + lazilyExpired,
                    resourceCounters.overdraft.sum(),
                    resourceCounters.overdraftBeyondBurst.sum(),
                    liveCredits);
        }
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    /**
     * Folds every stale lease (its quantum passed) into the expired counter and prunes issued-membership
     * records from passed quanta. Called at the top of every MUTATING operation - pure reads instead account
     * for staleness lazily, so they never write.
     */
    @GuardedBy("stateLock")
    private void settle(Instant now) {
        Iterator<Map.Entry<String, Map<String, LeaseState>>> memberIterator = leases.entrySet().iterator();
        while (memberIterator.hasNext()) {
            Map<String, LeaseState> memberLeases = memberIterator.next().getValue();
            Iterator<Map.Entry<String, LeaseState>> leaseIterator = memberLeases.entrySet().iterator();
            while (leaseIterator.hasNext()) {
                Map.Entry<String, LeaseState> entry = leaseIterator.next();
                ResourceContract contract = registry.get(entry.getKey());
                if (entry.getValue().quantumIndex < quantumIndexOf(now, contract.getQuantum())) {
                    countersFor(entry.getKey()).expired.add(entry.getValue().unspent());
                    leaseIterator.remove();
                }
            }
            if (memberLeases.isEmpty()) {
                memberIterator.remove();
            }
        }
        for (Map.Entry<String, Map<Long, List<String>>> entry : issuedMemberships.entrySet()) {
            ResourceContract contract = registry.get(entry.getKey());
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

    private Counters countersFor(String resourceName) {
        return counters.get(resourceName);
    }

    private ResourceContract requireRegistered(String resourceName) {
        ResourceContract contract = registry.get(resourceName);
        if (contract == null) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' is not registered - register the contract before using the allocator "
                            + "against it (R4/R19 fail-fast).", resourceName));
        }
        return contract;
    }

    // ------------------------------------------------------------------
    // Pure arithmetic (lock-free over immutable inputs)
    // ------------------------------------------------------------------

    private static long quantumIndexOf(Instant instant, Duration quantum) {
        return Math.floorDiv(instant.toEpochMilli(), quantum.toMillis());
    }

    private static Instant startOfQuantum(long quantumIndex, Duration quantum) {
        return Instant.ofEpochMilli(Math.multiplyExact(quantumIndex, quantum.toMillis()));
    }

    /**
     * The policy grant for one quantum: {@code floor(rate x quantum)} - integral, and never above the
     * declared rate (R8's bound holds by construction; burst manifests only as the overdraft allowance,
     * KTD7).
     */
    private static long grantPerQuantum(ResourceContract contract) {
        return (long) Math.floor(contract.getRatePerSecond() * contract.getQuantum().toMillis() / 1000.0);
    }

    /**
     * Equal share, integrally (KTD4): the floor for everyone, the remainder rotating by quantum index over
     * the stable (sorted) member ordering - deterministic, starvation-free, and summing to exactly the grant.
     */
    private static long shareFor(int position, int memberCount, long grant, long quantumIndex) {
        long floorShare = grant / memberCount;
        long remainder = grant % memberCount;
        long rotated = Math.floorMod(position - quantumIndex, memberCount);
        return floorShare + (rotated < remainder ? 1 : 0);
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

    /**
     * One member's minted slice of one quantum for one resource. Accessed only under the owning allocator's
     * {@code stateLock} (it lives inside a {@code @GuardedBy} map, which is what the check enforces).
     */
    private static final class LeaseState {
        private final long quantumIndex;
        private final long granted;
        private long spent;

        private LeaseState(long quantumIndex, long granted) {
            this.quantumIndex = quantumIndex;
            this.granted = granted;
        }

        private long unspent() {
            return granted - spent;
        }
    }

    /**
     * One resource's monotonic conservation counters (KTD2), plus the burst-budget watch. {@link LongAdder}s,
     * moved only under the allocator's monitor so ledger snapshots are consistent; outstanding is always
     * derived, never stored. The plain {@code long} budget fields are accessed only under the owning
     * allocator's {@code stateLock} (the {@link LeaseState} access pattern).
     */
    private static final class Counters {
        private final LongAdder minted = new LongAdder();
        private final LongAdder spent = new LongAdder();
        private final LongAdder expired = new LongAdder();
        private final LongAdder overdraft = new LongAdder();

        /**
         * Overdraft debits that pushed their quantum's cumulative overdraft beyond the contract's burst budget
         * (R8 observed, never enforced). A subset annotation of {@link #overdraft} - deliberately NOT a term
         * of the conservation identity.
         */
        private final LongAdder overdraftBeyondBurst = new LongAdder();

        /**
         * Which quantum {@link #overdraftInQuantum} counts - advances monotonically, never backward, so an
         * out-of-order straggler (see {@link StubResourceAllocator#trackOverdraftAgainstBurstBudget}) cannot
         * zero the current quantum's count.
         */
        private long overdraftBudgetQuantumIndex = Long.MIN_VALUE;

        /** The current quantum's cumulative overdraft - the burst budget's consumption, NOT monotonic. */
        private long overdraftInQuantum;
    }
}
