package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.quantumIndexOf;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.startOfQuantum;

/**
 * The lease ledger and conservation counters every {@link ResourceAllocator} in this package keeps (KTD2):
 * who holds how much of which quantum, the soft debit with its overdraft and per-quantum burst-budget watch
 * (KTD1, R8), expiry, and the identity {@code minted + overdraft == spent + expired + outstanding} - one
 * implementation, so the partition-share allocator LIFTS the in-process allocator's bookkeeping rather than
 * re-deriving it, and a fix to either's accounting is a fix to both.
 * <p>
 * <b>Not thread-safe on its own, by design.</b> An owning allocator holds its ledger in a field annotated
 * {@code @GuardedBy} with its state monitor and calls in only under that monitor - Error Prone enforces the
 * discipline at the field, which is why nothing here is annotated or synchronised. The {@link LongAdder}
 * counters are moved only under that same monitor, so a {@link #snapshot} taken under it is consistent.
 * <p>
 * <b>No clamps anywhere</b> (the counter-clamp learning): outstanding is derived from the monotonic counters
 * by {@link ConservationLedger#getOutstanding()} and cross-checked against the independently-scanned live
 * credits, so a bookkeeping mismatch shows up as a broken identity rather than being papered over.
 */
@Slf4j
final class CreditLedger {

    private final ContractRegistry registry;

    /**
     * Live lease ledgers: memberId → resourceName → the member's minted-and-partly-spent slice of one
     * quantum. A stale entry (its quantum passed) is unusable and is folded into the expired counter by the
     * next {@link #settle}; pure reads count it lazily instead.
     */
    private final Map<String, Map<String, LeaseState>> leases = new HashMap<>();

    /** Per-resource conservation counters, created on first touch. */
    private final Map<String, Counters> counters = new HashMap<>();

    CreditLedger(ContractRegistry registry) {
        this.registry = registry;
    }

    /**
     * Folds every stale lease (its quantum passed) into the expired counter. Called at the top of every
     * MUTATING allocator operation - pure reads instead account for staleness lazily, so they never write.
     */
    void settle(Instant now) {
        Iterator<Map.Entry<String, Map<String, LeaseState>>> memberIterator = leases.entrySet().iterator();
        while (memberIterator.hasNext()) {
            Map<String, LeaseState> memberLeases = memberIterator.next().getValue();
            Iterator<Map.Entry<String, LeaseState>> leaseIterator = memberLeases.entrySet().iterator();
            while (leaseIterator.hasNext()) {
                Map.Entry<String, LeaseState> entry = leaseIterator.next();
                ResourceContract contract = registry.require(entry.getKey());
                if (entry.getValue().quantumIndex < quantumIndexOf(now, contract.getQuantum())) {
                    countersFor(entry.getKey()).expired.add(entry.getValue().unspent());
                    leaseIterator.remove();
                }
            }
            if (memberLeases.isEmpty()) {
                memberIterator.remove();
            }
        }
    }

    /**
     * Whether {@code memberId} already holds a lease against {@code resourceName} for quantum
     * {@code quantumIndex} or a later one - the never-re-mint guard (R14): an issued quantum is minted exactly
     * once, however many times or from however many threads it is read.
     */
    boolean isIssued(String memberId, String resourceName, long quantumIndex) {
        return leaseOf(memberId, resourceName)
                .map(existing -> existing.quantumIndex >= quantumIndex)
                .orElse(false);
    }

    /**
     * Materialises {@code share} credits of quantum {@code quantumIndex} into {@code memberId}'s lease against
     * {@code resourceName}, replacing any earlier quantum's lease (already settled or about to be), and moves
     * the minted counter. Callers guard with {@link #isIssued} first. A zero share mints no lease - nothing
     * minted, nothing to expire.
     */
    void mint(String memberId, String resourceName, long quantumIndex, long share) {
        if (share == 0) {
            return;
        }
        leases.computeIfAbsent(memberId, id -> new HashMap<>())
                .put(resourceName, new LeaseState(quantumIndex, share));
        countersFor(resourceName).minted.add(share);
    }

    /**
     * The soft debit (KTD1/KD10): decrements the member's live credit when one remains for
     * {@code quantumIndex}, otherwise lands as overdraft - monotonic, never negative bookkeeping, never a
     * refund, never re-minting - watched against {@code burstBudget}, the overdraft the quantum is EXPECTED to
     * accumulate (R8), which the owning allocator computes for this member.
     */
    void spend(String memberId, ResourceContract contract, long quantumIndex, long burstBudget) {
        Counters resourceCounters = countersFor(contract.getName());
        resourceCounters.spent.add(1);
        Optional<LeaseState> lease = leaseOf(memberId, contract.getName());
        boolean liveCreditRemains = lease.isPresent()
                && lease.get().quantumIndex == quantumIndex
                && lease.get().unspent() > 0;
        if (liveCreditRemains) {
            lease.get().spent++;
        } else {
            // The always-succeeds rule (KTD1): the credit observed at eligibility is gone - the quantum
            // rolled, or a concurrent claimer spent it. Overdraft, monotonic; never negative bookkeeping,
            // never a refund, never re-minting. R8's burst term BUDGETS exactly this - it does not cap
            // it, so the budget is watched below rather than enforced.
            resourceCounters.overdraft.add(1);
            trackOverdraftAgainstBurstBudget(resourceCounters, contract, quantumIndex, burstBudget);
        }
    }

    /**
     * R8's overshoot budget, made observable - never enforced, because KTD1 forbids refusing the debit.
     * {@code burstBudget} is how much overdraft one quantum is EXPECTED to accumulate: the racing debits that
     * land between an eligibility read and the spend. A debit pushing the quantum's cumulative overdraft
     * BEYOND that budget still succeeded and is already in the ordinary overdraft counter (the conservation
     * identity is untouched); additionally it moves the monotonic beyond-burst counter and, once per
     * (resource, quantum), WARNs. The single-threaded selection engine keeps debits within budget
     * structurally, so a nonzero count means concurrent direct-pull claimers - or a caller outside the
     * engine's discipline - are outrunning the declared policy.
     *
     * <p>The reset is monotonic, against the out-of-order-instants hazard: the observation instant behind
     * {@code quantumIndex} is read in {@code WorkContainer.onQueueingForExecution} outside the owning monitor,
     * and that call is concurrently reachable under the direct-pull engine, so two spends can reach this
     * method with their timestamps - and therefore their quantum indices - inverted. A bare {@code !=} reset
     * would let a straggler carrying an OLDER quantum's index zero the CURRENT quantum's cumulative overdraft,
     * undercounting beyond-burst and re-arming the once-per-quantum warn. So the budget only ever advances
     * forward: {@code quantumIndex > overdraftBudgetQuantumIndex} rolls to a fresh budget; anything else - the
     * same quantum accumulating further, or an out-of-order straggler from an earlier one - folds into
     * whatever budget is already current, without moving the index backward.
     */
    private void trackOverdraftAgainstBurstBudget(Counters resourceCounters, ResourceContract contract,
                                                  long quantumIndex, long burstBudget) {
        if (quantumIndex > resourceCounters.overdraftBudgetQuantumIndex) {
            // a genuine advance - the quantum rolled since the last overdraft landed, so a fresh budget starts
            resourceCounters.overdraftBudgetQuantumIndex = quantumIndex;
            resourceCounters.overdraftInQuantum = 0;
        }
        // quantumIndex <= overdraftBudgetQuantumIndex: either this quantum's own accumulation, or a
        // straggler's older index - both fold into the CURRENT budget rather than resetting it
        resourceCounters.overdraftInQuantum++;
        if (resourceCounters.overdraftInQuantum > burstBudget) {
            resourceCounters.overdraftBeyondBurst.add(1);
            if (resourceCounters.overdraftInQuantum == burstBudget + 1L) { // the crossing: once per quantum
                log.warn("Resource '{}': quantum {}'s cumulative overdraft ({}) has exceeded this instance's burst "
                                + "budget of {} (declared burst {}). The debit still succeeded (KTD1's "
                                + "always-succeeds rule) and the conservation ledger is untouched - but spends are "
                                + "outrunning R8's rate x window + burst bound; the "
                                + "pc.navigator.credits.overdraft.beyond.burst meter counts these debits. Warning "
                                + "once per quantum.",
                        contract.getName(), quantumIndex, resourceCounters.overdraftInQuantum, burstBudget,
                        contract.getBurst());
            }
        }
    }

    /**
     * Death loses capacity (KD9/R6): every one of {@code memberId}'s live unspent credits is expired NOW, never
     * redistributed mid-window. The zeroed lease records are KEPT as their quantum's issued-marker: a
     * straggling read after the forfeit must find the quantum already issued and never re-mint it (R14). The
     * records leave at the next {@link #settle}, like any stale lease.
     */
    void forfeit(String memberId) {
        Map<String, LeaseState> memberLeases = leases.get(memberId);
        if (memberLeases != null) {
            for (Map.Entry<String, LeaseState> entry : memberLeases.entrySet()) {
                countersFor(entry.getKey()).expired.add(entry.getValue().unspent());
                entry.getValue().spent = entry.getValue().granted;
            }
        }
    }

    /**
     * Pure read - {@code memberId}'s lease against {@code contract} as of quantum {@code quantumIndex}, empty
     * when nothing was pulled or the lease's quantum has passed (expired, R6).
     */
    Optional<CapacityLease> currentLease(String memberId, ResourceContract contract, long quantumIndex) {
        return leaseOf(memberId, contract.getName())
                .filter(lease -> lease.quantumIndex == quantumIndex)
                .map(lease -> new CapacityLease(
                        contract.getName(),
                        quantumIndex,
                        Math.toIntExact(lease.unspent()),
                        startOfQuantum(quantumIndex + 1, contract.getQuantum())));
    }

    /**
     * Pure read - the conservation snapshot as of quantum {@code quantumIndex}. Expiry is derivable (KTD4's
     * lazy minting): a stale lease's unspent credits are already expired in effect and are counted here
     * WITHOUT folding - a pure read mutates nothing, and the identity holds at every observation point.
     */
    ConservationLedger snapshot(String resourceName, long quantumIndex) {
        Counters resourceCounters = countersFor(resourceName);
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

    private Optional<LeaseState> leaseOf(String memberId, String resourceName) {
        Map<String, LeaseState> memberLeases = leases.get(memberId);
        return memberLeases == null ? Optional.empty() : Optional.ofNullable(memberLeases.get(resourceName));
    }

    private Counters countersFor(String resourceName) {
        return counters.computeIfAbsent(resourceName, name -> new Counters());
    }

    /** One member's minted slice of one quantum for one resource. */
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
     * One resource's monotonic conservation counters (KTD2), plus the burst-budget watch. Outstanding is
     * always derived, never stored.
     */
    private static final class Counters {
        private final LongAdder minted = new LongAdder();
        private final LongAdder spent = new LongAdder();
        private final LongAdder expired = new LongAdder();
        private final LongAdder overdraft = new LongAdder();

        /**
         * Overdraft debits that pushed their quantum's cumulative overdraft beyond the burst budget (R8
         * observed, never enforced). A subset annotation of {@link #overdraft} - deliberately NOT a term of
         * the conservation identity.
         */
        private final LongAdder overdraftBeyondBurst = new LongAdder();

        /**
         * Which quantum {@link #overdraftInQuantum} counts - advances monotonically, never backward, so an
         * out-of-order straggler (see {@link CreditLedger#trackOverdraftAgainstBurstBudget}) cannot zero the
         * current quantum's count.
         */
        private long overdraftBudgetQuantumIndex = Long.MIN_VALUE;

        /** The current quantum's cumulative overdraft - the burst budget's consumption, NOT monotonic. */
        private long overdraftInQuantum;
    }
}
