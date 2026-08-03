package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import static io.confluent.parallelconsumer.offsets.OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK;
import static io.confluent.parallelconsumer.state.PartitionState.KAFKA_OFFSET_ABSENCE;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ;

/**
 * Permanent regression guard for the sparse iteration path in {@link OffsetSimultaneousEncoder#invoke()}.
 * <p>
 * When no active encoder {@link OffsetEncoder#requiresEveryOffset()} - which in production means the offset range was
 * too large for a {@link BitSetEncoder} to even be constructed, leaving only the distance-based
 * {@link RunLengthEncoder}s - {@code invoke()} visits only the offsets at which the completion state changes, instead of
 * walking the whole range (which can be ~2.1 billion offsets).
 * <p>
 * This is production offset-encoding code: a discrepancy would corrupt committed offset metadata. So rather than
 * asserting expected bytes by hand, every scenario here is encoded twice - once via the sparse walk and once via the
 * full walk - and the resulting {@link OffsetSimultaneousEncoder#getEncodingMap()} must be byte-for-byte identical.
 * The full walk is the trusted reference implementation.
 *
 * @author Antony Stubbs
 */
class OffsetSimultaneousEncoderSparseIterationTest {

    /**
     * Fixed so failures are reproducible.
     */
    private static final long SEED = 20260803L;

    /**
     * A single encode input: the base offset to commit, how many offsets the range spans, and which actual offsets
     * within that range are incomplete.
     */
    private static class Scenario {

        final String name;

        final long baseOffset;

        final long length;

        final SortedSet<Long> incompletes;

        Scenario(String name, long baseOffset, long length, SortedSet<Long> incompletes) {
            this.name = name;
            this.baseOffset = baseOffset;
            this.length = length;
            this.incompletes = incompletes;
        }

        /**
         * The range encoded is {@code [baseOffset, baseOffset + length)}, which the encoder derives from the highest
         * succeeded offset.
         */
        long highestSucceededOffset() {
            return baseOffset + length - 1;
        }

        @Override
        public String toString() {
            return name + " (base=" + baseOffset + " length=" + length + " incompletes=" + incompletes.size() + ")";
        }
    }

    private static Scenario scenario(String name, long baseOffset, long length, long... incompleteRelativeOffsets) {
        SortedSet<Long> incompletes = new TreeSet<>();
        for (long relative : incompleteRelativeOffsets) {
            incompletes.add(baseOffset + relative);
        }
        return new Scenario(name, baseOffset, length, incompletes);
    }

    private static Scenario allIncomplete(String name, long baseOffset, long length) {
        SortedSet<Long> incompletes = new TreeSet<>();
        for (long relative = 0; relative < length; relative++) {
            incompletes.add(baseOffset + relative);
        }
        return new Scenario(name, baseOffset, length, incompletes);
    }

    private static Scenario alternating(String name, long baseOffset, long length, int runWidth) {
        SortedSet<Long> incompletes = new TreeSet<>();
        for (long relative = 0; relative < length; relative++) {
            boolean incomplete = (relative / runWidth) % 2 == 0;
            if (incomplete) {
                incompletes.add(baseOffset + relative);
            }
        }
        return new Scenario(name, baseOffset, length, incompletes);
    }

    private static Scenario randomDensity(Random random, long baseOffset, long length, double density) {
        SortedSet<Long> incompletes = new TreeSet<>();
        for (long relative = 0; relative < length; relative++) {
            if (random.nextDouble() < density) {
                incompletes.add(baseOffset + relative);
            }
        }
        return new Scenario("random density " + density, baseOffset, length, incompletes);
    }

    /**
     * Deliberately covers: empty and single-offset ranges, no/all incompletes, incompletes at the very first and very
     * last offset of the range (the two positions the sparse construction has to clamp), consecutive blocks, long
     * alternating runs, and randomised sparse/dense patterns at a variety of range sizes and base offsets - including
     * the {@link io.confluent.parallelconsumer.state.PartitionState#KAFKA_OFFSET_ABSENCE} base used elsewhere in these
     * tests.
     */
    static List<Scenario> scenarios() {
        List<Scenario> scenarios = new ArrayList<>();

        // base 5 (not the offset absence sentinel), so the constructor really does derive a zero length range
        scenarios.add(scenario("zero length range", 5, 0));
        scenarios.add(scenario("single offset, complete", 0, 1));
        scenarios.add(scenario("single offset, incomplete", 0, 1, 0));
        scenarios.add(scenario("no incompletes", 100, 50));
        scenarios.add(allIncomplete("all incomplete", 100, 50));
        scenarios.add(scenario("incomplete at first offset only", 100, 50, 0));
        scenarios.add(scenario("incomplete at last offset only", 100, 50, 49));
        scenarios.add(scenario("incomplete in the middle only", 100, 50, 25));
        scenarios.add(scenario("incompletes at both ends", 100, 50, 0, 49));
        scenarios.add(scenario("adjacent incompletes at the start", 100, 50, 0, 1, 2));
        scenarios.add(scenario("adjacent incompletes at the end", 100, 50, 47, 48, 49));
        scenarios.add(scenario("two consecutive blocks", 100, 50, 5, 6, 7, 8, 30, 31, 32));
        scenarios.add(scenario("isolated incompletes with gaps of one", 100, 20, 2, 4, 6, 8, 10));
        scenarios.add(scenario("base offset zero", 0, 30, 0, 3, 29));
        scenarios.add(scenario("negative base offset (offset absence)", KAFKA_OFFSET_ABSENCE, 30, 0, 3, 29));
        scenarios.add(scenario("large base offset", 987_654_321L, 30, 0, 15, 29));

        // out of range incompletes must be ignored exactly as the full scan ignores them
        SortedSet<Long> outOfRange = new TreeSet<>();
        outOfRange.add(90L); // below the base
        outOfRange.add(105L); // in range
        outOfRange.add(1_000L); // above the end
        scenarios.add(new Scenario("incompletes outside the range", 100, 20, outOfRange));

        for (int runWidth : new int[]{1, 2, 3, 17, 500}) {
            scenarios.add(alternating("alternating runs of " + runWidth, 100, 2_000, runWidth));
        }

        Random random = new Random(SEED);
        long[] lengths = {1, 2, 3, 7, 64, 999, 5_000};
        double[] densities = {0.0, 0.01, 0.1, 0.5, 0.9, 1.0};
        for (long length : lengths) {
            for (double density : densities) {
                long baseOffset = random.nextInt(1_000_000);
                scenarios.add(randomDensity(random, baseOffset, length, density));
            }
        }

        return scenarios;
    }

    /**
     * The core guarantee: for a given input, the sparse walk and the full walk produce identical encodings.
     * <p>
     * {@link OffsetSimultaneousEncoder#dropEncodersRequiringEveryOffset()} is used so the sparse path is legitimately
     * available on small, fast ranges - it reproduces the encoder line-up that production ends up with when the range is
     * too large for a bitset.
     */
    // read lock only - we don't change compressionForced, but the result depends on it, so exclude anyone who does
    @ResourceLock(value = COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ)
    @ParameterizedTest
    @MethodSource("scenarios")
    void sparseIterationIsByteIdenticalToTheFullScan(Scenario scenario) {
        OffsetSimultaneousEncoder sparse = new OffsetSimultaneousEncoder(scenario.baseOffset, scenario.highestSucceededOffset(), scenario.incompletes);
        sparse.dropEncodersRequiringEveryOffset();
        sparse.invoke();

        OffsetSimultaneousEncoder full = new OffsetSimultaneousEncoder(scenario.baseOffset, scenario.highestSucceededOffset(), scenario.incompletes);
        full.dropEncodersRequiringEveryOffset();
        full.invoke(false);

        // guard against the test silently exercising nothing
        Truth.assertWithMessage("sparse encoder must actually have taken the sparse path")
                .that(sparse.isSparseIterationUsed()).isTrue();
        Truth.assertWithMessage("reference encoder must actually have taken the full scan")
                .that(full.isSparseIterationUsed()).isFalse();

        assertEncodingsIdentical(sparse.getEncodingMap(), full.getEncodingMap());

        // and something was actually encoded - RunLengthV2 can represent any of these scenarios
        Truth.assertWithMessage("expected at least the v2 run-length encoding to survive")
                .that(sparse.getEncodingMap().keySet()).contains(OffsetEncoding.RunLengthV2);
    }

    /**
     * Beyond "identical to the full scan", check the sparse encoding actually round-trips back to the incompletes it
     * was given - the property the rest of the system depends on.
     */
    @ResourceLock(value = COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ)
    @ParameterizedTest
    @MethodSource("scenarios")
    void sparseIterationRoundTrips(Scenario scenario) throws NoEncodingPossibleException {
        if (scenario.length < 1) {
            return; // nothing to encode
        }

        OffsetSimultaneousEncoder sparse = new OffsetSimultaneousEncoder(scenario.baseOffset, scenario.highestSucceededOffset(), scenario.incompletes);
        sparse.dropEncodersRequiringEveryOffset();
        sparse.invoke();

        byte[] packed = sparse.packSmallest();
        HighestOffsetAndIncompletes decoded = OffsetMapCodecManager.decodeCompressedOffsets(scenario.baseOffset, packed);

        SortedSet<Long> expectedIncompletes = new TreeSet<>(scenario.incompletes.subSet(scenario.baseOffset, scenario.baseOffset + scenario.length));

        Truth.assertWithMessage("decoded incompletes for %s", scenario)
                .that(decoded.getIncompleteOffsets()).containsExactlyElementsIn(expectedIncompletes);
    }

    /**
     * Safety-by-default: as long as an encoder that needs every offset is active (a {@link BitSetEncoder} here), the
     * full scan must be used, otherwise the bitset would be left full of holes.
     */
    @Test
    void fullScanIsUsedWhileAnEncoderNeedsEveryOffset() {
        SortedSet<Long> incompletes = new TreeSet<>();
        incompletes.add(2L);
        incompletes.add(7L);

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(0, 19, incompletes);
        encoder.invoke();

        Truth.assertWithMessage("BitSet encoders need every offset, so the full scan must be used")
                .that(encoder.isSparseIterationUsed()).isFalse();
        Truth.assertThat(encoder.getEncodingMap().keySet()).contains(OffsetEncoding.BitSetV2);
    }

    /**
     * The production trigger for the sparse path: a range so wide that no {@link BitSetEncoder} can be constructed, so
     * only the run-length encoders remain. Previously this walked ~2.1 billion offsets one at a time.
     */
    @Test
    void hugeRangeUsesSparseIterationAndStillDropsOverflowingEncoders() {
        final long overflowedValue = Integer.MAX_VALUE + 100L;

        SortedSet<Long> incompletes = new TreeSet<>();
        for (long incomplete : new long[]{0L, 4L, 6L, 7L, 8L, 10L, overflowedValue}) {
            incompletes.add(incomplete);
        }

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(KAFKA_OFFSET_ABSENCE, overflowedValue - 1, incompletes);
        encoder.invoke();

        Truth.assertWithMessage("no bitset encoder can be built for a ~2.1B range, so the sparse path must be taken")
                .that(encoder.isSparseIterationUsed()).isTrue();
        // the trailing run still overflows even the v2 (Integer) run-length, so every encoding is dropped
        Truth.assertThat(encoder.getEncodingMap()).isEmpty();
    }

    private void assertEncodingsIdentical(Map<OffsetEncoding, byte[]> sparse, Map<OffsetEncoding, byte[]> full) {
        Truth.assertWithMessage("the same set of encodings must survive")
                .that(sparse.keySet()).containsExactlyElementsIn(full.keySet());

        for (Map.Entry<OffsetEncoding, byte[]> entry : full.entrySet()) {
            Truth.assertWithMessage("bytes for encoding %s", entry.getKey())
                    .that(sparse.get(entry.getKey())).isEqualTo(entry.getValue());
        }
    }

}
