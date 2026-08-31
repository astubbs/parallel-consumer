package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

import java.time.Duration;

/**
 * The policy a named, shared, rate-limited resource is registered under (R1): the resource's declared rate plus
 * burst define its overshoot bound (R8, R12), and {@link #getQuantum()} is the cadence the allocator mints credit
 * on (KTD4).
 * <p>
 * Immutable and value-equal ({@link Value} generates {@code equals}/{@code hashCode} over every field), which is
 * exactly the property {@link StubResourceAllocator#register} depends on to tell an identical re-registration
 * (accepted) apart from a policy collision (rejected, R19).
 * <p>
 * This is the {@code ResourceContract} of the seam vocabulary in
 * {@code docs/ideation/2026-08-29-hasten-compound-engineering-handoff.md} section 24. U1 keeps it a plain data
 * carrier - no validation of its own fields beyond what the {@code @Value} constructor already gives for free
 * (non-primitive fields still accept {@code null}); registration-time fail-fast (R4, R19) lives on
 * {@link ResourceAllocator#register}, not here.
 */
@Value
public class ResourceContract {

    /**
     * The resource's name - what a function's {@code resourceTags} and a registration's collision check both key
     * on.
     */
    String name;

    /**
     * The declared rate, in credits (tokens) per second (R1). v1's demo policy is {@code 2.0} (KTD7).
     */
    double ratePerSecond;

    /**
     * The burst allowance folded into the overshoot bound alongside {@link #getRatePerSecond()} (R1, R8) - v1's
     * demo policy is {@code 2} (KTD7), one quantum's worth.
     */
    int burst;

    /**
     * The cadence the allocator mints credit on (KTD4) - v1's demo policy is one second (KTD7).
     */
    Duration quantum;
}
