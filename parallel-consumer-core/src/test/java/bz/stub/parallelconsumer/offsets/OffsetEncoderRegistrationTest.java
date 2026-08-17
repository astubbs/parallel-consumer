package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSet;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSetCompressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSetV2;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.BitSetV2Compressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.DeltaList;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.DeltaListCompressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLength;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLengthCompressed;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLengthV2;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.RunLengthV2Compressed;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;

/**
 * What {@link OffsetSimultaneousEncoder} registers into the competitive set: the per-encoder compressed-twin rule
 * (KTD8) and the no-regression guarantee that adding the delta-list encoder never makes a commit bigger (R11).
 *
 * @author Antony Stubbs
 * @see OffsetSimultaneousEncoder
 */
@Slf4j
// Writes OffsetSimultaneousEncoder.compressionForced - both views of the compression rule are measured here.
@ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = READ_WRITE)
class OffsetEncoderRegistrationTest {

    /** The encodings that shipped before the delta list, paired plain-with-compressed-twin. */
    private static final OffsetEncoding[][] INCUMBENT_PAIRS = {
            {BitSet, BitSetCompressed},
            {BitSetV2, BitSetV2Compressed},
            {RunLength, RunLengthCompressed},
            {RunLengthV2, RunLengthV2Compressed},
    };

    /** Representative scenarios from the density benchmark's corpus - the ones R11 names. */
    private static final String[] SCENARIOS = {
            "trailing-incomplete-run",
            "uniform-random 0.1%",
            "uniform-random 1%",
            "uniform-random 5%",
            "uniform-random 20%",
            "clustered-bursts",
            "alternating-xo",
            "all-incomplete",
    };

    private boolean compressionForcedBefore;

    @BeforeEach
    void useTheProductionCompressionRule() {
        compressionForcedBefore = OffsetSimultaneousEncoder.compressionForced;
        OffsetSimultaneousEncoder.compressionForced = false;
    }

    @AfterEach
    void restoreCompressionForced() {
        OffsetSimultaneousEncoder.compressionForced = compressionForcedBefore;
    }

    /**
     * KTD8: compressed twins are registered PER ENCODER, so one small encoding no longer suppresses another's.
     * <p>
     * The scenario is the shape that made this necessary: a long complete run with the incompletes at the tail, where
     * run-length encodes in a handful of bytes while the bitsets need one bit per offset. Before KTD8, run-length's
     * 6 bytes were {@code quiteSmall}, and the all-or-nothing gate
     * ({@code encoders.stream().noneMatch(OffsetEncoder::quiteSmall)}) therefore registered NO compressed twin for
     * anything - including the 1,255-byte bitset whose twin compresses to a fraction of that. A small encoder winning
     * that scenario hid the cost, but any scenario where it did not win paid for it in characters against the metadata
     * cap.
     */
    @Test
    void aSmallEncodingNoLongerSuppressesAnotherEncodersCompressedTwin() {
        final int rangeSize = 10_000;
        final SortedSet<Long> incompletes = new TreeSet<>();
        for (long offset = rangeSize - 500; offset < rangeSize; offset++) {
            incompletes.add(offset);
        }

        final Map<OffsetEncoding, byte[]> registered =
                new OffsetSimultaneousEncoder(0L, rangeSize - 1L, incompletes).invoke().getEncodingMap();

        assertWithMessage("run-length is tiny here, so it gets no twin - compression cannot help it")
                .that(registered.keySet()).containsNoneOf(RunLengthCompressed, RunLengthV2Compressed);
        assertWithMessage("but the large encodings keep theirs, which the all-or-nothing gate used to suppress (KTD8)")
                .that(registered.keySet()).containsAtLeast(BitSetCompressed, BitSetV2Compressed, DeltaListCompressed);

        for (final OffsetEncoding[] pair : INCUMBENT_PAIRS) {
            final byte[] plain = registered.get(pair[0]);
            if (plain == null) {
                continue;
            }
            final boolean quiteSmall = plain.length < OffsetSimultaneousEncoder.LARGE_ENCODED_SIZE_THRESHOLD_BYTES;
            assertWithMessage("%s (%s bytes plain) has a twin exactly when it is not quiteSmall", pair[0], plain.length)
                    .that(registered.containsKey(pair[1]))
                    .isEqualTo(!quiteSmall);
        }
    }

    /** {@code compressionForced} still overrides the per-encoder rule and registers every twin, small or not. */
    @Test
    void forcedCompressionStillRegistersEveryTwin() {
        final int rangeSize = 10_000;
        final SortedSet<Long> incompletes = new TreeSet<>();
        for (long offset = rangeSize - 500; offset < rangeSize; offset++) {
            incompletes.add(offset);
        }

        OffsetSimultaneousEncoder.compressionForced = true;
        final Map<OffsetEncoding, byte[]> registered =
                new OffsetSimultaneousEncoder(0L, rangeSize - 1L, incompletes).invoke().getEncodingMap();

        assertThat(registered.keySet()).containsAtLeast(
                BitSetCompressed, BitSetV2Compressed, RunLengthCompressed, RunLengthV2Compressed, DeltaListCompressed);
    }

    /**
     * R11: for every representative scenario, the payload that ships now - with the delta-list encoder registered and
     * KTD8's per-encoder twins - is no larger than what the pre-change pipeline would have committed.
     * <p>
     * "Before" is modelled from a forced-compression pass over the same input, which registers every twin: the
     * incumbent plain sizes are identical either way (forcing compression only ADDS entries), and the old
     * all-or-nothing gate is a pure function of those plain sizes. Sizes are compared as packed bytes rather than
     * metadata characters because both outer codecs are monotonic in payload length, so a byte win is never a
     * character loss.
     */
    @SneakyThrows
    @Test
    void theShippedPayloadIsNeverLargerThanBeforeTheDeltaList() {
        for (final String scenario : SCENARIOS) {
            for (final int rangeSize : new int[]{1_000, 10_000, 100_000}) {
                final SortedSet<Long> incompletes =
                        OffsetEncodingDensityBenchmarkTest.corpusIncompletes(scenario, rangeSize);

                OffsetSimultaneousEncoder.compressionForced = false;
                final OffsetSimultaneousEncoder shipped =
                        new OffsetSimultaneousEncoder(0L, rangeSize - 1L, incompletes).invoke();
                final int shippedBytes = shipped.packSmallest().length;

                OffsetSimultaneousEncoder.compressionForced = true;
                final Map<OffsetEncoding, byte[]> everything =
                        new OffsetSimultaneousEncoder(0L, rangeSize - 1L, incompletes).invoke().getEncodingMap();
                OffsetSimultaneousEncoder.compressionForced = false;

                final int before = smallestBeforeThisChange(everything);

                assertWithMessage("%s @ %s: chosen payload with the delta list and KTD8 (%s bytes) must not be larger "
                                + "than the incumbents under the old all-or-nothing gate (%s bytes)",
                        scenario, rangeSize, shippedBytes, before)
                        .that(shippedBytes).isAtMost(before);
            }
        }
    }

    /**
     * The smallest packed payload the pipeline would have chosen before this change: incumbent encodings only, with
     * compressed twins registered only if NO encoder was {@code quiteSmall}.
     */
    private static int smallestBeforeThisChange(Map<OffsetEncoding, byte[]> everything) {
        boolean twinsRegistered = true;
        for (final OffsetEncoding[] pair : INCUMBENT_PAIRS) {
            final byte[] plain = everything.get(pair[0]);
            if (plain != null && plain.length < OffsetSimultaneousEncoder.LARGE_ENCODED_SIZE_THRESHOLD_BYTES) {
                twinsRegistered = false;
            }
        }

        int smallest = Integer.MAX_VALUE;
        for (final OffsetEncoding[] pair : INCUMBENT_PAIRS) {
            final byte[] plain = everything.get(pair[0]);
            if (plain != null) {
                smallest = Math.min(smallest, plain.length + 1);
            }
            final byte[] compressed = everything.get(pair[1]);
            if (twinsRegistered && compressed != null) {
                smallest = Math.min(smallest, compressed.length + 1);
            }
        }
        assertWithMessage("at least one incumbent encoding must exist, or there is nothing to compare against")
                .that(smallest).isLessThan(Integer.MAX_VALUE);
        return smallest;
    }

    /** The smallest packed payload among the incumbent encodings that are actually registered in {@code map}. */
    private static int smallestIncumbentRegistered(Map<OffsetEncoding, byte[]> map) {
        int smallest = Integer.MAX_VALUE;
        for (final OffsetEncoding[] pair : INCUMBENT_PAIRS) {
            for (final OffsetEncoding encoding : pair) {
                final byte[] bytes = map.get(encoding);
                if (bytes != null) {
                    smallest = Math.min(smallest, bytes.length + 1);
                }
            }
        }
        assertWithMessage("at least one incumbent encoding must be registered")
                .that(smallest).isLessThan(Integer.MAX_VALUE);
        return smallest;
    }

    /**
     * R4 / the U1 verdict, held to on the scenario class it was decided on: the delta list joins the competitive set
     * and actually wins the sparse-uniform scenarios, which is the entire reason it ships.
     */
    @SneakyThrows
    @Test
    void theDeltaListWinsTheSparseUniformScenariosItShippedFor() {
        for (final int rangeSize : new int[]{10_000, 100_000}) {
            final SortedSet<Long> incompletes =
                    OffsetEncodingDensityBenchmarkTest.corpusIncompletes("uniform-random 1%", rangeSize);

            final OffsetSimultaneousEncoder encoder =
                    new OffsetSimultaneousEncoder(0L, rangeSize - 1L, incompletes).invoke();
            final Map<OffsetEncoding, byte[]> registered = encoder.getEncodingMap();

            final int chosen = encoder.packSmallest().length;
            final int deltaListBest = Math.min(
                    registered.containsKey(DeltaList) ? registered.get(DeltaList).length + 1 : Integer.MAX_VALUE,
                    registered.containsKey(DeltaListCompressed)
                            ? registered.get(DeltaListCompressed).length + 1 : Integer.MAX_VALUE);

            assertWithMessage("uniform-random 1%% @ %s: the delta list is the smallest encoding, so it is what "
                    + "packSmallest commits", rangeSize)
                    .that(deltaListBest).isEqualTo(chosen);
            assertWithMessage("uniform-random 1%% @ %s: and it beats every incumbent encoding actually registered",
                    rangeSize)
                    .that(deltaListBest).isLessThan(smallestIncumbentRegistered(registered));
        }
    }
}
