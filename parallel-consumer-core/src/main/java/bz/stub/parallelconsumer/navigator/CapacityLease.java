package bz.stub.parallelconsumer.navigator;

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
 * Minted lazily by {@link StubResourceAllocator}: the member's {@link ResourceAllocator#readQuantum} pull
 * materialises its share of quantum {@link #getQuantumIndex()} exactly once - a repeated read of the same
 * quantum returns the identical grant, never a topped-up one (R14, KTD4). Unspent credits are unusable from
 * the next quantum on ({@link #getExpiresAt()}, R6).
 */
@Value
public class CapacityLease {

    /**
     * Which resource this lease was minted against - matches {@link ResourceContract#getName()}.
     */
    String resourceName;

    /**
     * The quantum this lease belongs to - the index of the interval
     * {@code [quantumIndex * quantum, (quantumIndex + 1) * quantum)} on the allocator's canonical clock,
     * anchored at the epoch (KTD4). Identifies the grant so a re-read of an issued quantum is recognisably
     * the SAME lease, not a fresh one (R14).
     */
    long quantumIndex;

    /**
     * Credits remaining in this lease, available to spend (R7). Never re-minted once issued (R14) - a fresh read
     * of the same quantum returns the identical value, not a topped-up one.
     */
    int availableCredits;

    /**
     * When this lease's unspent credits expire (R6) - the end of its quantum, the moment a resource-eligibility
     * read (KTD1) and an {@code availableAt} deferral (KTD5) both key off.
     */
    Instant expiresAt;
}
