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
 * <b>U1 wires only the registry half</b> - {@link #register} and {@link #lookup} - which is all registration-time
 * fail-fast (R4, R19) needs. {@link #currentLease} is declared here so the credit-facing shape is fixed, but its
 * only implementation in this unit ({@link StubResourceAllocator}) is a placeholder; the next unit fills in real
 * quantum-indexed minting (KD2, KTD4) behind this same method.
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
     *                                  {@link ResourceContract}
     */
    void register(ResourceContract contract);

    /**
     * The contract registered under {@code resourceName}, or {@link Optional#empty()} when nothing of that name
     * has been registered - the read {@link ParallelConsumerOptions#validate()} uses to fail fast on an unknown
     * tag, naming it (R4).
     */
    Optional<ResourceContract> lookup(String resourceName);

    /**
     * This instance's current lease against {@code resourceName} as of {@code now}, or {@link Optional#empty()}
     * when it holds no live credit (none minted yet, its last lease expired, or the resource is unknown). The
     * claim-path read (KTD1) and the {@code availableAt} deferral computation (KTD5) both consume this.
     */
    Optional<CapacityLease> currentLease(String resourceName, Instant now);
}
