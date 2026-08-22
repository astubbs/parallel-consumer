package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * The shared bookkeeping every opt-in-engine parity suite needs, and nothing else.
 * <p>
 * Parity suites all ask the same question of a different execution path - does it agree with the shipped engine on
 * exactly-once delivery, ordering, pause and drain - so they all need the same three mechanics: a delivery record
 * that preserves duplicates, a way to produce records, and a duplicate tally. Those are here. The SCENARIOS stay in
 * the subclasses on purpose: each engine breaks these guarantees for its own reasons, and the javadoc explaining
 * which mechanism a given assertion is actually guarding is the most valuable part of a test that exists because it
 * passes.
 *
 * @see DirectPullEngineParityTest
 * @see AdaptiveConcurrencyParityTest
 */
public abstract class EngineParityTestBase extends ParallelEoSStreamProcessorTestBase {

    /**
     * Every offset delivered to the user function, in delivery order, with duplicates preserved - a set would
     * silently absorb the very defect these suites are looking for.
     */
    protected final List<Long> deliveries = Collections.synchronizedList(new ArrayList<>());

    protected void produce(int count, IntFunction<String> keyFor) {
        for (int i = 0; i < count; i++) {
            consumerSpy.addRecord(ktu.makeRecord(keyFor.apply(i), "v-" + i));
        }
    }

    /**
     * @return offsets delivered more than once, with their delivery counts
     */
    protected Map<Long, Integer> duplicates() {
        Map<Long, Integer> counts = new LinkedHashMap<>();
        synchronized (deliveries) {
            for (Long offset : deliveries) {
                counts.merge(offset, 1, Integer::sum);
            }
        }
        Map<Long, Integer> dupes = new LinkedHashMap<>();
        counts.forEach((offset, count) -> {
            if (count > 1) {
                dupes.put(offset, count);
            }
        });
        return dupes;
    }
}
