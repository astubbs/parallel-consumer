package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import static bz.stub.parallelconsumer.offsets.OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK;
import static bz.stub.parallelconsumer.state.PartitionState.KAFKA_OFFSET_ABSENCE;
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
     * How many hand-written scenarios {@link #scenarios()} adds before the generated ones - a sizing hint, not a
     * contract.
     */
    private static final int FIXED_SCENARIOS = 17;

    private static final int[] ALTERNATING_RUN_WIDTHS = {1, 2, 3, 17, 500};

    private static final long[] RANDOM_LENGTHS = {1, 2, 3, 7, 64, 999, 5_000};

    private static final double[] RANDOM_DENSITIES = {0.0, 0.01, 0.1, 0.5, 0.9, 1.0};

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

    /**
     * Deliberately covers: empty and single-offset ranges, no/all incompletes, incompletes at the very first and very
     * last offset of the range (the two positions the sparse construction has to clamp), consecutive blocks, long
     * alternating runs, and randomised sparse/dense patterns at a variety of range sizes and base offsets - including
     * the {@link bz.stub.parallelconsumer.state.PartitionState#KAFKA_OFFSET_ABSENCE} base used elsewhere in these
     * tests.
     */
    static List<Scenario> scenarios() {
        // sizing hint only - the exact count is whatever this method ends up adding, and ArrayList grows if it drifts
        List<Scenario> scenarios = new ArrayList<>(FIXED_SCENARIOS + ALTERNATING_RUN_WIDTHS.length + RANDOM_LENGTHS.length * RANDOM_DENSITIES.length);

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

        for (int runWidth : ALTERNATING_RUN_WIDTHS) {
            scenarios.add(alternating("alternating runs of " + runWidth, 100, 2_000, runWidth));
        }

        scenarios.addAll(randomScenarios());

        return scenarios;
    }

    /**
     * The randomised half of {@link #scenarios()}, kept in its own method so the seeding is visible in one place.
     * <p>
     * A single generator seeded from {@link #SEED} draws every base offset and every completion state, so the whole
     * block is reproducible from that constant alone.
     * <p>
     * SpotBugs reports {@code DMI_RANDOM_USED_ONLY_ONCE} here and it is a false positive: the detector matches a
     * seeded {@link Random} held in a local, which is precisely what a reproducible fixture needs. Four shapes were
     * tried - the draws split across a helper, a per-scenario generator seeded from this one, both draws inlined
     * here, and {@code nextDouble} in place of {@code nextInt} - and it fires on all of them. Left as the clearest
     * of the four rather than contorted further.
     */
    private static List<Scenario> randomScenarios() {
        List<Scenario> scenarios = new ArrayList<>(RANDOM_LENGTHS.length * RANDOM_DENSITIES.length);
        Random random = new Random(SEED);
        for (long length : RANDOM_LENGTHS) {
            for (double density : RANDOM_DENSITIES) {
                long baseOffset = random.nextInt(1_000_000);
                SortedSet<Long> incompletes = new TreeSet<>();
                for (long relative = 0; relative < length; relative++) {
                    if (random.nextDouble() < density) {
                        incompletes.add(baseOffset + relative);
                    }
                }
                scenarios.add(new Scenario("random density " + density, baseOffset, length, incompletes));
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

    /**
     * Regression guard: an offset range whose inclusive top is {@link Long#MAX_VALUE}.
     * <p>
     * The in-range filter used to compare against an <em>exclusive</em> end offset, computed as
     * {@code lowWaterMark + lengthBetweenBaseAndHighOffset} - which is {@code highestSucceededOffset + 1}, and so wraps
     * to {@link Long#MIN_VALUE} here. Every incomplete then compared as out-of-range, so none reached the visit set and
     * the whole span encoded as one completed run: incomplete offsets silently committed as complete. That run also
     * overflows even the v2 (int) run length, so the symptom was an empty encoding map - a commit with no encoding at
     * all.
     * <p>
     * None of the generated scenarios reach this shape: they are all small ranges at modest base offsets, so the
     * differential test could not have caught it. The bound is now the inclusive last offset, which reconstructs
     * {@code highestSucceededOffset} and cannot wrap.
     */
    @ResourceLock(value = COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ)
    @Test
    void anInclusiveLongMaxValueRangeTopDoesNotDropEveryIncomplete() {
        // one wider than the bitset limit, so no BitSetEncoder can be built and the sparse path is taken for real
        final long baseOffset = Long.MAX_VALUE - Integer.MAX_VALUE;
        final long midRangeIncomplete = baseOffset + (Integer.MAX_VALUE / 2);

        SortedSet<Long> incompletes = new TreeSet<>();
        incompletes.add(midRangeIncomplete);

        OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(baseOffset, Long.MAX_VALUE, incompletes);
        encoder.invoke();

        Truth.assertWithMessage("a range wider than the bitset limit must take the sparse path")
                .that(encoder.isSparseIterationUsed()).isTrue();

        // the incomplete splits the span into two runs, each comfortably inside an int, so v2 must survive. Before the
        // fix the incomplete was dropped, leaving one run too long for any run-length encoder and an empty map.
        Truth.assertWithMessage("the mid-range incomplete must survive into the encoding, splitting the span into representable runs")
                .that(encoder.getEncodingMap().keySet()).contains(OffsetEncoding.RunLengthV2);
    }

    /**
     * Regression guard: a caller-supplied {@link SortedSet} whose comparator is not the natural one.
     * <p>
     * The in-range filter used to {@code break} on the first entry above the range, which is only sound for ascending
     * iteration. The constructor is public and the {@code incompleteOffsets} field's own javadoc promises "no order
     * requirement, but SortedSet just in case", so that was an ordering assumption the class explicitly disclaims -
     * and a reverse-ordered set presenting an above-range offset first made the loop exit before reaching a perfectly
     * valid in-range incomplete. Note {@link SortedSet#subSet}, which the hand-written loop replaced, would have
     * honoured the set's own comparator.
     * <p>
     * The full scan only ever calls {@link java.util.Set#contains}, so it is order-independent and remains the trusted
     * reference here.
     */
    @ResourceLock(value = COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ)
    @Test
    void aNonNaturalComparatorDoesNotHideInRangeIncompletes() {
        final long baseOffset = 100;
        final long highestSucceededOffset = 119; // range [100, 119]

        // reverse order, so iteration presents the above-range offset BEFORE the in-range one
        SortedSet<Long> incompletes = new TreeSet<>(Comparator.reverseOrder());
        incompletes.add(1_000L); // above the range
        incompletes.add(105L); // inside it, and must not be skipped

        OffsetSimultaneousEncoder sparse = new OffsetSimultaneousEncoder(baseOffset, highestSucceededOffset, incompletes);
        sparse.dropEncodersRequiringEveryOffset();
        sparse.invoke();

        OffsetSimultaneousEncoder full = new OffsetSimultaneousEncoder(baseOffset, highestSucceededOffset, incompletes);
        full.dropEncodersRequiringEveryOffset();
        full.invoke(false);

        Truth.assertWithMessage("sparse encoder must actually have taken the sparse path")
                .that(sparse.isSparseIterationUsed()).isTrue();

        assertEncodingsIdentical(sparse.getEncodingMap(), full.getEncodingMap());

        Truth.assertWithMessage("expected at least the v2 run-length encoding to survive")
                .that(sparse.getEncodingMap().keySet()).contains(OffsetEncoding.RunLengthV2);
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
