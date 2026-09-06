package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.navigator.QuantumArithmetic.grantPerQuantum;

/**
 * The registry half of a {@link ResourceAllocator} (R1, R4, R19), shared by every allocator in this package so
 * the fail-fast rules - unusable policy, conflicting policy under one name - have ONE implementation and one
 * set of messages. Concurrent because several PC instances' builders may register overlapping resources at
 * construction time (KD11); the map's own thread safety is the whole concurrency control here, which is why
 * an allocator may hold this outside its state monitor.
 * <p>
 * Public for two callers outside this package, both of which need the rules without an allocator:
 * {@code ParallelConsumerOptions#validate()} under the partition-share strategy, where no allocator exists yet
 * (the engine builds it after the consumer does, KTD4) and the options-supplied contracts are registered into a
 * fresh registry purely to apply R19 at validation (R6, R7); and a {@code CUSTOM} {@link ResourceAllocator}
 * implementation, which may hold one rather than re-deriving the same checks and messages.
 */
public final class ContractRegistry {

    private final Map<String, ResourceContract> contracts = new ConcurrentHashMap<>();

    public ContractRegistry() {
    }

    /** {@link ResourceAllocator#register}'s contract, verbatim: validate, then accept identical or reject differing. */
    public void register(ResourceContract contract) {
        if (contract.getQuantum() == null || contract.getQuantum().isZero() || contract.getQuantum().isNegative()) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' declares a non-positive quantum {} - the minting cadence must be a positive "
                            + "duration (R19 fail-fast).",
                    contract.getName(), contract.getQuantum()));
        }
        if (contract.getRatePerSecond() < 0 || contract.getBurst() < 0) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' declares a negative rate ({}) or burst ({}) - both must be non-negative "
                            + "(R19 fail-fast).",
                    contract.getName(), contract.getRatePerSecond(), contract.getBurst()));
        }
        // A positive rate that floors to zero credits per quantum is not a slow resource - it is a permanent
        // stall: grantPerQuantum == 0 means nextCreditAt returns empty, no lease ever mints, and no wakeup
        // exists to break it, so a tagged record starves silently forever. A rate of exactly 0 is the
        // deliberate shut valve and stays legal (R19).
        if (contract.getRatePerSecond() > 0 && grantPerQuantum(contract) == 0) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' declares rate {}/s over quantum {} - floor(rate x quantum) mints ZERO "
                            + "credits every quantum, so every tagged member would starve forever with no "
                            + "wakeup to break it. Raise the quantum or the rate so at least one whole credit "
                            + "mints per quantum (R19 fail-fast).",
                    contract.getName(), contract.getRatePerSecond(), contract.getQuantum()));
        }
        ResourceContract existing = contracts.putIfAbsent(contract.getName(), contract);
        if (existing != null && !existing.equals(contract)) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' is already registered with policy {} - cannot re-register it with a " +
                            "DIFFERENT policy {}. Registering the identical policy again is accepted (several " +
                            "instances may each register the resources they share); changing it is a " +
                            "configuration error (R19).",
                    contract.getName(), existing, contract));
        }
    }

    /** {@link ResourceAllocator#lookup}'s contract, verbatim. */
    public Optional<ResourceContract> lookup(String resourceName) {
        return Optional.ofNullable(contracts.get(resourceName));
    }

    /** The registered contract, or the R4/R19 fail-fast for a name nothing registered. */
    ResourceContract require(String resourceName) {
        ResourceContract contract = contracts.get(resourceName);
        if (contract == null) {
            throw new IllegalArgumentException(msg(
                    "Resource '{}' is not registered - register the contract before using the allocator "
                            + "against it (R4/R19 fail-fast).", resourceName));
        }
        return contract;
    }

    /** A live view of every registered contract - iteration order is unspecified. */
    Collection<ResourceContract> all() {
        return contracts.values();
    }
}
