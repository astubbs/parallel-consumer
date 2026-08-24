package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.jetbrains.lincheck.datastructures.ModelCheckingOptions;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.truth.Truth.assertThat;

/**
 * RED CONTROL for the Lincheck evaluation: the smallest possible instance of the defect class, with a KNOWN
 * answer, so that a silent "no violations found" on the real classes can be told apart from a harness that is
 * not looking at anything.
 * <p>
 * The shape is deliberately the same textbook probe SpotBugs was given in the torn-read dossier and reported
 * NOTHING on, at {@code effort=Max, threshold=Medium}, including from its nominally-relevant
 * {@code AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION} detector: a {@code containsKey} followed by a
 * {@code get} on a {@code ConcurrentHashMap}, with the result dereferenced unconditionally. That is
 * {@code ShardManager.removeWorkFromShardFor}'s bug reduced to nine lines and nothing else. If Lincheck cannot
 * see it here, no result it gives on the real classes means anything.
 * <p>
 * This test asserts nothing itself. Its assertion is Lincheck's own: the strategy under test must find a
 * failing interleaving, and it fails the build if it does not (both strategies are wired to expect a
 * violation, so a Lincheck that silently stopped exploring turns this file red rather than green).
 *
 * @author Antony Stubbs
 */
@Tag("lincheck")
public class LincheckToolchainProbeTest {

    private static final String KEY = "the-one-shard";

    /**
     * A "shard map" in miniature: keyed collections, removed once empty. Concurrent by type, torn by use.
     */
    private final Map<String, List<Integer>> shards = new ConcurrentHashMap<>();

    public LincheckToolchainProbeTest() {
        List<Integer> shard = Collections.synchronizedList(new ArrayList<>());
        shard.add(0);
        shards.put(KEY, shard);
    }

    /**
     * The defect, in full. Two reads of a moving map combined as one snapshot: {@code containsKey} says the
     * shard is there, {@code get} is a separate read, and between them another thread can have emptied and
     * garbage-collected the shard. The {@code get} then returns null and the next line dereferences it.
     */
    @Operation
    public boolean removeWork(int offset) {
        if (shards.containsKey(KEY)) {
            List<Integer> shard = shards.get(KEY);
            boolean removed = shard.remove((Integer) offset);
            if (shard.isEmpty()) {
                shards.remove(KEY);
            }
            return removed;
        }
        return false;
    }

    @Test
    void modelCheckingFindsTheTear() {
        var options = new ModelCheckingOptions()
                .threads(2)
                .actorsPerThread(2)
                .iterations(30)
                .invocationsPerIteration(1_000);
        String report = LincheckHarness.runExpectingViolation("control probe / model checking", options, getClass());
        assertThat(report).contains("NullPointerException");
    }

    @Test
    void stressFindsTheTear() {
        var options = new StressOptions()
                .threads(2)
                .actorsPerThread(2)
                .iterations(30)
                .invocationsPerIteration(10_000);
        String report = LincheckHarness.runExpectingViolation("control probe / stress", options, getClass());
        assertThat(report).contains("NullPointerException");
    }
}
