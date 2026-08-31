package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * The v1 in-process {@link ResourceAllocator} (KD2): an in-JVM implementation that must honour the same
 * distributed semantics a later Kafka-coordinated allocator will, so that rung swaps transport behind this
 * interface rather than moving the seam.
 * <p>
 * <b>U1 implements only the registry</b> ({@link #register}, {@link #lookup} - R1, R4, R19), backed by a
 * {@link ConcurrentHashMap} because several PC instances' builders may register overlapping resources
 * concurrently (KD11: registration rides each instance's construction-time API) - the map's own thread safety is
 * the concurrency control here, so no field on this class needs {@code @GuardedBy}.
 * <p>
 * {@link #currentLease} is a placeholder that always answers empty - the next unit replaces it with
 * quantum-indexed lazy minting, equal share, membership lifecycle, and the conservation counters (KD9, KTD2,
 * KTD4), behind this same class and the same {@link ResourceAllocator} seam.
 */
public class StubResourceAllocator implements ResourceAllocator {

    private final Map<String, ResourceContract> registry = new ConcurrentHashMap<>();

    @Override
    public void register(ResourceContract contract) {
        ResourceContract existing = registry.putIfAbsent(contract.getName(), contract);
        if (existing != null && !existing.equals(contract)) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' is already registered with policy {} - cannot re-register it with a " +
                            "DIFFERENT policy {}. Registering the identical policy again is accepted (several " +
                            "instances may each register the resources they share); changing it is a " +
                            "configuration error (R19).",
                    contract.getName(), existing, contract));
        }
    }

    @Override
    public Optional<ResourceContract> lookup(String resourceName) {
        return Optional.ofNullable(registry.get(resourceName));
    }

    @Override
    public Optional<CapacityLease> currentLease(String resourceName, Instant now) {
        // Skeletal in U1 - always empty; the next unit implements real quantum-indexed minting (KTD4).
        return Optional.empty();
    }
}
