package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;

import java.time.Instant;
import java.util.Optional;

/**
 * The seam a {@link ParallelConsumerOptions} builder is handed (KTD3) - the application constructs ONE allocator
 * and passes the SAME instance into every PC instance's builder, the {@code meterRegistry} precedent. This is the
 * {@code ResourceAllocator} of the seam vocabulary in
 * {@code docs/ideation/2026-08-29-hasten-compound-engineering-handoff.md} section 24 (R5): an in-process
 * implementation must obey the same semantic contract a later distributed one will, so the Kafka coordination
 * rung can swap transport behind this interface without moving the seam.
 * <p>
 * <b>The calls split into two disciplines, and the engine must not mix them up:</b>
 * <ul>
 *   <li><b>Mutating</b> - {@link #join}, {@link #leave}, {@link #readQuantum}, {@link #spend}. The engine's
 *       lifecycle and control-loop seams own these: join on the running transition, leave at close-entry
 *       (R16), one {@code readQuantum} per control-loop pass (the lease-renewing pull, KTD4), one
 *       {@code spend} per tagged resource immediately after a claim CAS win (KTD1).</li>
 *   <li><b>Pure reads</b> - {@link #currentLease}, {@link #nextCreditAt}, the rate views, and
 *       {@link #conservationLedger}. Safe from any thread at any frequency: they never renew a lease, never
 *       mint, never move a counter (KTD9's query discipline). The claim-path eligibility read (KTD1) and the
 *       {@code availableAt} deferral (KTD5) consume these.</li>
 * </ul>
 * Every {@code now} is an observation instant on the ONE canonical clock the allocator and its members share
 * (KTD4) - in production the allocator's own construction-time UTC clock (implementations offer no-argument
 * overloads that read it), in the virtual-clock lane the shared {@code MutableClock}.
 */
public interface ResourceAllocator {

    /**
     * Registers {@code contract} under its {@link ResourceContract#getName()} (R1) - a construction-time act that
     * must happen before any PC instance tagging that name is built and validated (R4).
     * <p>
     * Registering the SAME name again is accepted when the policy is identical (byte-for-byte {@code equals}) -
     * the ordinary case of several instances each registering the resources they share - and REJECTED, naming
     * the collision, when the policy differs (R19): a resource's policy is fixed at first registration, never
     * silently overwritten.
     *
     * @throws IllegalArgumentException {@code contract.getName()} is already registered under a different
     *                                  {@link ResourceContract}, or the policy is unusable (non-positive
     *                                  quantum or negative rate/burst) - fail fast at registration, never
     *                                  deep in the engine (R19)
     */
    void register(ResourceContract contract);

    /**
     * The contract registered under {@code resourceName}, or {@link Optional#empty()} when nothing of that name
     * has been registered - the read {@link ParallelConsumerOptions#validate()} uses to fail fast on an unknown
     * tag, naming it (R4).
     */
    Optional<ResourceContract> lookup(String resourceName);

    /**
     * Mutating - membership lifecycle (R16). The member counts from the NEXT quantum after {@code now}: a
     * joiner must not dilute a quantum whose grants may already have been read (KTD4's reproducible reads).
     * The engine calls this on the processor's running transition. Joining also starts the member's lease
     * clock - a joiner that never reads a quantum is dropped after the lease TTL like any silent member.
     */
    void join(String memberId, Instant now);

    /**
     * Mutating - membership lifecycle (R16). The engine calls this at close-entry. The leaver's share is
     * re-divided from the NEXT quantum; its unspent live credits are lost immediately - expired, never
     * redistributed mid-window (R6, KD9's death-loses-capacity).
     */
    void leave(String memberId, Instant now);

    /**
     * Mutating - THE per-pass quantum pull (KTD4). Called once per control-loop pass, it renews the member's
     * membership lease and materialises the member's share of the current quantum into its
     * {@link CapacityLease} for every registered resource - exactly once per quantum: a repeated or concurrent
     * read of an issued quantum returns the identical grant, never a fresh one (R14). A member whose control
     * loop stops calling this lapses after the lease TTL and its capacity is lost until re-division (R16).
     */
    void readQuantum(String memberId, Instant now);

    /**
     * Mutating - the post-claim debit (KTD1), called immediately after a claim CAS win, once per tagged
     * resource. <b>Always succeeds</b>: normally it decrements the member's live credit; when no live credit
     * remains (the quantum rolled between the eligibility read and this call, or a concurrent claimer spent
     * it) the debit lands as OVERDRAFT - a monotonic counter, never negative bookkeeping, never a refund,
     * never re-minting (KD10). R8's burst term budgets exactly this overshoot.
     */
    void spend(String memberId, String resourceName, Instant now);

    /**
     * Pure read - {@code memberId}'s live lease against {@code resourceName} as of {@code now}, or
     * {@link Optional#empty()} when it holds no live credit (nothing pulled yet, its last lease's quantum has
     * passed, or the resource is unknown). The claim-path eligibility read (KTD1) consumes this; it never
     * renews the membership lease - only {@link #readQuantum} does that.
     */
    Optional<CapacityLease> currentLease(String memberId, String resourceName, Instant now);

    /**
     * Pure read - when {@code memberId} next gains spendable credit against {@code resourceName}, projected
     * from current membership: usually the next quantum boundary, later when remainder rotation (KTD4) gives
     * this member no credit in the next quantum. Empty when the resource is unknown or its policy mints
     * nothing. Feeds the {@code availableAt} deferral and its attribution (KTD5, R9). A projection, not a
     * promise - membership may change before it arrives (KD10's best-effort framing).
     */
    Optional<Instant> nextCreditAt(String memberId, String resourceName, Instant now);

    /**
     * Pure read - the resource-level next credit time: the next quantum boundary after {@code now}. Empty when
     * the resource is unknown or its policy mints nothing.
     */
    Optional<Instant> nextCreditAt(String resourceName, Instant now);

    /**
     * Pure read - the resource's globally available rate in credits per second: its declared policy rate (R18's
     * global view). {@code 0.0} when the resource is unknown.
     */
    double globalRatePerSecond(String resourceName);

    /**
     * Pure read - the rate currently available to {@code memberId} against {@code resourceName} in credits per
     * second: its equal share of the policy rate under current membership (R18's instance-local view).
     * {@code 0.0} when the member is not currently a member or the resource is unknown.
     */
    double localRatePerSecond(String memberId, String resourceName, Instant now);

    /**
     * Pure read - the resource's conservation counters as of {@code now} (KTD2). The identity
     * {@code minted + overdraft == spent + expired + outstanding} holds at every observation point; lazily
     * expired credits are included in {@link ConservationLedger#getExpired()} without mutating anything.
     *
     * @throws IllegalArgumentException {@code resourceName} is not registered
     */
    ConservationLedger conservationLedger(String resourceName, Instant now);
}
