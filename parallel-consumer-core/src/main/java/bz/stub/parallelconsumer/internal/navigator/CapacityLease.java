package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

import java.time.Instant;

/**
 * One instance's delegated slice of a {@link ResourceContract}'s capacity for one quantum (KD2, KTD4) - the
 * {@code CapacityLease} of the seam vocabulary in
 * {@code docs/ideation/2026-08-29-hasten-compound-engineering-handoff.md} section 24.
 * <p>
 * <b>U1 does not mint these.</b> This type exists so {@link ResourceAllocator}'s credit-facing signature
 * ({@link ResourceAllocator#currentLease}) reads correctly ahead of the allocator that actually issues leases;
 * the next unit implements quantum-indexed lazy minting, equal share, and the conservation counters (KTD2,
 * KTD4) behind this same shape. Kept deliberately minimal - the fields a claim-path read needs (R7, KTD1) and
 * nothing an accounting implementation would want to add later without changing them.
 */
@Value
public class CapacityLease {

    /**
     * Which resource this lease was minted against - matches {@link ResourceContract#getName()}.
     */
    String resourceName;

    /**
     * Credits remaining in this lease, available to spend (R7). Never re-minted once issued (R14) - a fresh read
     * of the same quantum returns the identical value, not a topped-up one.
     */
    int availableCredits;

    /**
     * When this lease's unspent credits expire (R6) - the moment a resource-eligibility read (KTD1) and an
     * {@code availableAt} deferral (KTD5) both key off.
     */
    Instant expiresAt;
}
