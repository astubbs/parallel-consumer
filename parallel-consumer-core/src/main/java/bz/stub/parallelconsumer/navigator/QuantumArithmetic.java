package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;
import java.time.Instant;

/**
 * The quantum arithmetic every {@link ResourceAllocator} in this package mints by (KTD4): pure, lock-free
 * functions over immutable inputs, shared so the in-process and the partition-share allocators divide time and
 * rotate the remainder IDENTICALLY - a copy that drifted would let two allocators disagree about which quantum
 * an instant belongs to, or who the remainder credit goes to, and nothing else would catch it.
 * <p>
 * Time on the ONE canonical clock divides into quanta anchored at the epoch. The grant for a quantum is
 * {@code floor(rate x quantum)}, and a grant divides integrally among {@code slotCount} slots: everyone gets
 * the floor, and the remainder rotates deterministically by quantum index over the slot ordering, so no slot
 * starves and the shares of one quantum sum to exactly the grant. The in-process allocator's slots are its
 * sorted members; the partition-share allocator's slots are the subscription's partitions by fleet-stable
 * ordinal (the partition-share plan's KTD1).
 */
final class QuantumArithmetic {

    private QuantumArithmetic() {
    }

    /** The index of the quantum {@code instant} falls in: {@code floor(epochMillis / quantumMillis)}. */
    static long quantumIndexOf(Instant instant, Duration quantum) {
        return Math.floorDiv(instant.toEpochMilli(), quantum.toMillis());
    }

    /** The first instant of quantum {@code quantumIndex}. */
    static Instant startOfQuantum(long quantumIndex, Duration quantum) {
        return Instant.ofEpochMilli(Math.multiplyExact(quantumIndex, quantum.toMillis()));
    }

    /**
     * The policy grant for one quantum: {@code floor(rate x quantum)} - integral, and never above the
     * declared rate (R8's bound holds by construction; burst manifests only as the overdraft allowance,
     * KTD7).
     */
    static long grantPerQuantum(ResourceContract contract) {
        return (long) Math.floor(contract.getRatePerSecond() * contract.getQuantum().toMillis() / 1000.0);
    }

    /**
     * Equal share, integrally (KTD4): the floor for every slot, the remainder rotating by quantum index over
     * the stable slot ordering - deterministic, starvation-free, and summing to exactly {@code grant} over all
     * {@code slotCount} slots for every quantum index.
     *
     * @param position   the slot's position in the stable ordering, {@code 0 <= position < slotCount}
     * @param slotCount  how many slots divide the grant - must be positive
     */
    static long shareFor(int position, int slotCount, long grant, long quantumIndex) {
        long floorShare = grant / slotCount;
        long remainder = grant % slotCount;
        long rotated = Math.floorMod(position - quantumIndex, slotCount);
        return floorShare + (rotated < remainder ? 1 : 0);
    }
}
