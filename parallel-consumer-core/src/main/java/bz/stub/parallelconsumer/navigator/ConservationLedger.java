package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

/**
 * A point-in-time snapshot of one resource's credit accounting (KTD2): the four monotonic identity counters
 * plus the beyond-burst monitor, with outstanding credit DERIVED - never maintained, never clamped - so a
 * bookkeeping mismatch shows up as a broken identity rather than being papered over (the counter-clamp
 * learning).
 * <p>
 * The identity is {@code minted + overdraft == spent + expired + outstanding} at every observation point.
 * {@link #getOutstanding()} computes the left-hand definition from the counters; {@link #getLiveCredits()} is
 * the independently-scanned sum of live lease credits, so asserting the two equal is a REAL check of the
 * ledger closing, not a tautology.
 */
@Value
public class ConservationLedger {

    /**
     * The resource this ledger accounts for - matches {@link ResourceContract#getName()}.
     */
    String resourceName;

    /**
     * Credits materialised into member leases (KTD4's lazy minting) - only shares actually pulled by a
     * {@link ResourceAllocator#readQuantum} count; a share nobody read was never minted and never expires.
     */
    long minted;

    /**
     * Debits taken by {@link ResourceAllocator#spend} - EVERY spend counts here, including the ones that
     * landed as overdraft (KTD1's always-succeeds rule).
     */
    long spent;

    /**
     * Minted credits that reached the end of their quantum unspent (R6) - includes stale leases not yet
     * folded, computed lazily at snapshot time so the identity holds at every observation point.
     */
    long expired;

    /**
     * Debits taken when no live credit remained - expiry raced the spend, or a concurrent claimer got there
     * first (KTD1, KD10). Monotonic, never "repaid". The contract's burst BUDGETS this overshoot per quantum
     * (R8) rather than bounding it - {@link #getOverdraftBeyondBurst()} counts the debits that exceeded the
     * budget.
     */
    long overdraft;

    /**
     * The subset of {@link #getOverdraft()} that landed after its quantum's cumulative overdraft had already
     * consumed the contract's declared burst (R8's overshoot budget, observed rather than enforced - KTD1
     * forbids refusing the debit). Monotonic, never clamped. Deliberately NOT a term of the conservation
     * identity: every beyond-burst debit is already counted in {@link #getOverdraft()}, so this field is a
     * monitor, not a ledger column - zero while the single-threaded selection engine keeps debits within
     * budget structurally, nonzero when concurrent direct-pull claimers outrun the declared policy.
     */
    long overdraftBeyondBurst;

    /**
     * Independently-scanned sum of live lease credits at snapshot time - the cross-check for
     * {@link #getOutstanding()}, which is derived from the counters alone.
     */
    long liveCredits;

    /**
     * Outstanding credit, DERIVED from the monotonic counters per KTD2:
     * {@code minted + overdraft - spent - expired}. Equal to {@link #getLiveCredits()} whenever the ledger
     * closes.
     */
    public long getOutstanding() {
        return minted + overdraft - spent - expired;
    }
}
