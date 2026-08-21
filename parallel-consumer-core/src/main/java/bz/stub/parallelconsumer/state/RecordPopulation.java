package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.atomic.LongAdder;

/**
 * How many records the system is currently holding, derived by conservation rather than maintained as a running
 * total:
 *
 * <pre>
 *     records in system = records admitted  -  records retired
 * </pre>
 * <p>
 * A record is <em>admitted</em> exactly when its {@link WorkContainer} is inserted into a {@link ProcessingShard}'s
 * entry map, and <em>retired</em> exactly when it is removed from it again. Those are the only two ways the
 * population changes, and both live inside {@link ProcessingShard}, which is the sole owner of that map - so the
 * figure cannot disagree with the collection it describes the way a separately-maintained counter can.
 * <p>
 * <b>Why two monotonic counters rather than one that goes up and down.</b> A single counter has to be decremented
 * on the correct condition at every removal site, and getting that condition wrong at one site drifts the value
 * permanently, in whichever direction the mistake happened to point. Here each side only ever increases, and each
 * side has exactly one caller pattern: insert, remove. There is no predicate to get wrong. The failure mode of a
 * <em>missed</em> retirement is a leak (reads high, throttles intake); there is no way to make it read low, and no
 * way for a double-count in one direction to be silently cancelled by one in the other.
 * <p>
 * <b>Why {@link LongAdder} and not {@link java.util.concurrent.atomic.AtomicLong}.</b> Admission happens on the
 * broker-poll thread and retirement mostly on the control thread; both are hot. {@link LongAdder} spreads writes
 * across striped cells so writers rarely contend on the same memory, and reduces them on read - which is the
 * access pattern here (written often, read once per control loop). It is not, on its own, a fix for drift: a
 * contention-free wrong number is still wrong. The correctness comes from conservation, not from the adder.
 *
 * @author Antony Stubbs
 * @see ProcessingShard
 * @see ShardManager#getNumberOfRecordsInShards()
 */
public class RecordPopulation {

    private final LongAdder admitted = new LongAdder();

    private final LongAdder retired = new LongAdder();

    /**
     * A record's {@link WorkContainer} has entered a shard.
     */
    void onAdmitted() {
        admitted.increment();
    }

    /**
     * A record's {@link WorkContainer} has left a shard, for whatever reason - success, revocation, or a stale
     * container being swept. It will never be selected as work again.
     */
    void onRetired() {
        retired.increment();
    }

    /**
     * @return the number of records currently held in shards
     */
    public long getInSystem() {
        // Read retired FIRST, then admitted. Both counters only ever increase, and no record can be retired
        // without having been admitted first, so a retirement observed here always has its admission already
        // committed. Reading the subtrahend first therefore makes the difference non-negative by construction,
        // with no clamp required. Reading them the other way round can transiently read a fresh retirement
        // against a stale admission total, and go negative.
        long retiredSoFar = retired.sum();
        long admittedSoFar = admitted.sum();
        return admittedSoFar - retiredSoFar;
    }

    /**
     * @return the running total of records ever admitted - useful for asserting on conservation in tests
     */
    public long getAdmittedTotal() {
        return admitted.sum();
    }

    /**
     * @return the running total of records ever retired - useful for asserting on conservation in tests
     */
    public long getRetiredTotal() {
        return retired.sum();
    }

    @Override
    public String toString() {
        long retiredSoFar = retired.sum();
        long admittedSoFar = admitted.sum();
        return "RecordPopulation(admitted=" + admittedSoFar + ", retired=" + retiredSoFar
                + ", inSystem=" + (admittedSoFar - retiredSoFar) + ")";
    }
}
