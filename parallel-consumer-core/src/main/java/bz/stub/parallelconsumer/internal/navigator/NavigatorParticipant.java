package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * One PC instance's membership of the shared {@link ResourceAllocator} (U3's engine seam): the instance's stable
 * member id, its tagged resource names, and the allocator handle, bound together once at module construction so
 * the selection path never re-derives any of them per record (R3's zero-cost resolution).
 * <p>
 * <b>Two shapes, decided once per instance.</b> An untagged instance gets the {@link #inert()} participant:
 * {@link #isActive()} is false and every caller short-circuits before touching the allocator - no lookups, no
 * clock reads, no allocation on that path (R3). A tagged instance gets {@link #activeMember}, and its methods
 * split exactly as {@link ResourceAllocator}'s do:
 * <ul>
 *   <li><b>Pure reads</b> - {@link #hasSpendableCreditForAllTags}, {@link #availableAt},
 *       {@link #earliestBlockedResourceNextCreditAt}. Safe from any thread at any frequency; the claim-path
 *       eligibility term (KTD1) and the {@code timeToBlockFor} wakeup bound (KTD5) consume these.</li>
 *   <li><b>Mutating</b> - {@link #join}, {@link #leave}, {@link #readQuantum} (lifecycle, R16/KTD4) and
 *       {@link #spendOneCreditPerTag} (the post-claim debit, KTD1). Owned by the engine's lifecycle and claim
 *       seams; never called from a pure query.</li>
 * </ul>
 * <b>Immutability is the concurrency contract</b>: every field is final and set before publication, so the
 * participant itself needs no lock ({@code @GuardedBy} has nothing to name) - all synchronisation lives inside
 * the allocator implementation (KTD11). Every {@code now} passed in must come from the one canonical clock the
 * allocator and its members share (KTD4): in production both the module clock and the allocator's
 * construction-time clock are UTC; the virtual-clock test lane shares one {@code MutableClock} across both.
 *
 * @author Antony Stubbs
 */
public final class NavigatorParticipant {

    private static final NavigatorParticipant INERT =
            new NavigatorParticipant(null, Collections.emptyList(), null);

    /** Null exactly when this participant is {@link #inert()}. */
    private final ResourceAllocator allocator;

    /** Immutable; empty exactly when this participant is {@link #inert()}. */
    private final List<String> resourceTags;

    /** Null exactly when this participant is {@link #inert()}; otherwise stable for the instance's lifetime. */
    private final String memberId;

    private NavigatorParticipant(ResourceAllocator allocator, List<String> resourceTags, String memberId) {
        this.allocator = allocator;
        this.resourceTags = resourceTags;
        this.memberId = memberId;
    }

    /** The untagged instance's participant (R3): inactive, and every method a guaranteed no-op. */
    public static NavigatorParticipant inert() {
        return INERT;
    }

    /**
     * A tagged instance's participant. The caller (the module) has already validated the tags against the
     * allocator's registry ({@code ParallelConsumerOptions#validate()}, R4/R19), so this only pins the shape.
     *
     * @throws IllegalArgumentException when the tag list is empty - an "active" participant with nothing to
     *                                  gate would silently behave as inert, which is the configuration lie
     *                                  R19 exists to prevent
     */
    public static NavigatorParticipant activeMember(ResourceAllocator allocator, List<String> resourceTags,
                                                    String memberId) {
        if (resourceTags == null || resourceTags.isEmpty()) {
            throw new IllegalArgumentException("An active navigator participant needs at least one resource tag - "
                    + "use inert() for an untagged instance");
        }
        return new NavigatorParticipant(allocator,
                Collections.unmodifiableList(new ArrayList<>(resourceTags)), memberId);
    }

    /** Whether this instance participates in the navigator at all - the R3 gate every caller checks first. */
    public boolean isActive() {
        return allocator != null;
    }

    /** The stable member id this instance is known to the allocator by. Null when {@link #inert()}. */
    public String memberId() {
        return memberId;
    }

    /** The resource names this instance's function requires (R2). Immutable; empty when {@link #inert()}. */
    public List<String> resourceTags() {
        return resourceTags;
    }

    // ------------------------------------------------------------------
    // Pure reads (KTD1 eligibility, KTD5 wakeup) - never mutate anything
    // ------------------------------------------------------------------

    /**
     * The claim's resource-eligibility term (KTD1): true when EVERY tagged resource holds a live lease with at
     * least one spendable credit for this member at {@code now}. Pure - a lease can exist with zero credits, and
     * that counts as blocked. Always true when {@link #inert()}.
     */
    public boolean hasSpendableCreditForAllTags(Instant now) {
        if (!isActive()) {
            return true;
        }
        for (String tag : resourceTags) {
            if (isBlocked(tag, now)) {
                return false;
            }
        }
        return true;
    }

    /**
     * When a record deferred NOW becomes dispatchable (R7): the LATEST of the blocking resources' next-credit
     * times - a record needing several resources cannot run until the last of them has credit, so the max, not
     * the min. Empty when nothing is blocking (or when every blocking resource's policy mints nothing, in which
     * case there is no time to name). A projection, not a promise (KD10's best-effort framing).
     */
    public Optional<Instant> availableAt(Instant now) {
        Instant latest = null;
        for (String tag : blockedTags(now)) {
            Optional<Instant> nextCredit = allocator.nextCreditAt(memberId, tag, now);
            if (nextCredit.isPresent() && (latest == null || nextCredit.get().isAfter(latest))) {
                latest = nextCredit.get();
            }
        }
        return Optional.ofNullable(latest);
    }

    /**
     * The wakeup bound's input (KTD5): the EARLIEST next-credit time over the resources currently blocking -
     * the first instant at which any deferred work could become dispatchable, so the control loop's block time
     * is capped by it rather than by the poll default. Min where {@link #availableAt} is max, deliberately: a
     * wake that finds the work still multi-resource-blocked just re-blocks (soft, R8's best-effort posture).
     * Empty when nothing is blocking.
     */
    public Optional<Instant> earliestBlockedResourceNextCreditAt(Instant now) {
        Instant earliest = null;
        for (String tag : blockedTags(now)) {
            Optional<Instant> nextCredit = allocator.nextCreditAt(memberId, tag, now);
            if (nextCredit.isPresent() && (earliest == null || nextCredit.get().isBefore(earliest))) {
                earliest = nextCredit.get();
            }
        }
        return Optional.ofNullable(earliest);
    }

    // ------------------------------------------------------------------
    // Mutating - the engine's lifecycle and post-claim seams only
    // ------------------------------------------------------------------

    /**
     * The post-claim debit (KTD1): one credit from EVERY tagged resource, called immediately after the claim
     * CAS wins and never on a lost race. Always succeeds - a credit gone between the eligibility read and this
     * call lands as overdraft in the allocator (KD10); no rollback, no refund.
     */
    public void spendOneCreditPerTag(Instant now) {
        if (!isActive()) {
            return;
        }
        for (String tag : resourceTags) {
            allocator.spend(memberId, tag, now);
        }
    }

    /** Membership join (R16) - the engine calls this once, at the running transition. No-op when inert. */
    public void join(Instant now) {
        if (isActive()) {
            allocator.join(memberId, now);
        }
    }

    /**
     * Membership leave (R16) - the engine calls this at close ENTRY, before the drain, so the share is dropped
     * at the next quantum without waiting for the lease TTL (AE2). No-op when inert.
     */
    public void leave(Instant now) {
        if (isActive()) {
            allocator.leave(memberId, now);
        }
    }

    /**
     * THE per-pass quantum pull (KTD4): renews the membership lease and materialises this quantum's share.
     * The engine calls this once per control-loop pass, beside the admission tick. No-op when inert.
     */
    public void readQuantum(Instant now) {
        if (isActive()) {
            allocator.readQuantum(memberId, now);
        }
    }

    /** Blocked = no live lease, or a live lease with zero credits left (KTD1's eligibility definition). */
    private boolean isBlocked(String tag, Instant now) {
        Optional<CapacityLease> lease = allocator.currentLease(memberId, tag, now);
        return !lease.isPresent() || lease.get().getAvailableCredits() <= 0;
    }

    private List<String> blockedTags(Instant now) {
        if (!isActive()) {
            return Collections.emptyList();
        }
        List<String> blocked = new ArrayList<>(resourceTags.size());
        for (String tag : resourceTags) {
            if (isBlocked(tag, now)) {
                blocked.add(tag);
            }
        }
        return blocked;
    }

    @Override
    public String toString() {
        return isActive()
                ? "NavigatorParticipant(memberId=" + memberId + ", resourceTags=" + resourceTags + ")"
                : "NavigatorParticipant(inert)";
    }
}
