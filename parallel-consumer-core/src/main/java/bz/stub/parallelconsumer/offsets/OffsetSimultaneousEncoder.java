package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.Range;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static bz.stub.parallelconsumer.internal.utils.Range.range;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.Version.v1;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static bz.stub.parallelconsumer.state.PartitionState.KAFKA_OFFSET_ABSENCE;

/**
 * Encode with multiple strategies at the same time.
 * <p>
 * Have results in an accessible structure, easily selecting the highest compression.
 *
 * @author Antony Stubbs
 * @see #invoke()
 */
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
public class OffsetSimultaneousEncoder {

    /**
     * Size threshold in bytes after which compressing the encodings will be compared, as it seems to be typically worth
     * the extra compression step when beyond this size in the source array.
     */
    public static final int LARGE_ENCODED_SIZE_THRESHOLD_BYTES = 200;

    /**
     * Size threshold to notice particularly large input maps.
     */
    public static final int LARGE_INPUT_MAP_SIZE = 2_000;

    /**
     * The offsets which have not yet been fully completed and can't have their offset committed - only used to test
     * with {@link Set#contains} (no order requirement, but {@link SortedSet} just in case).
     */
    @Getter
    private final SortedSet<Long> incompleteOffsets;

    /**
     * The lowest committable offset
     */
    @ToString.Include
    private final long lowWaterMark;

    /**
     * The difference between the base offset (the offset to be committed) and the highest seen offset.
     */
    @ToString.Include
    private final long lengthBetweenBaseAndHighOffset;

    /**
     * Map of different encoding types for the same offset data, used for retrieving the data for the encoding type
     */
    @Getter
    Map<OffsetEncoding, byte[]> encodingMap = new EnumMap<>(OffsetEncoding.class);

    /**
     * Ordered set of the different encodings, used to quickly retrieve the most compressed encoding
     *
     * @see #packSmallest()
     */
    @Getter
    SortedSet<EncodedOffsetPair> sortedEncodings = new TreeSet<>();

    /**
     * Force the encoder to also add the compressed versions. Useful for testing.
     * <p>
     * Visible for testing.
     */
    @ToString.Include
    public static boolean compressionForced = false;

    /**
     * Used to prevent tests running in parallel that depends on setting static state in this class. Manipulation of
     * static state in tests needs to be removed to this isn't necessary.
     */
    public static final String COMPRESSION_FORCED_RESOURCE_LOCK = "Value doesn't matter, just needs a constant";

    /**
     * The encoders to run. Concurrent so we can remove encoders while traversing.
     */
    private final ConcurrentHashMap.KeySetView<OffsetEncoder, Boolean> activeEncoders;

    /**
     * Which iteration strategy {@link #invoke()} actually used - true if the sparse (transition-only) walk was taken,
     * false if every offset in the range was visited.
     * <p>
     * Visible for testing.
     *
     * @see #invoke(boolean)
     */
    @Getter(AccessLevel.PACKAGE)
    private boolean sparseIterationUsed = false;

    public OffsetSimultaneousEncoder(long baseOffsetToCommit, long highestSucceededOffset, SortedSet<Long> incompleteOffsets) {
        this.lowWaterMark = baseOffsetToCommit;
        this.incompleteOffsets = incompleteOffsets;

        //
        if (highestSucceededOffset == KAFKA_OFFSET_ABSENCE) { // nothing succeeded yet
            highestSucceededOffset = baseOffsetToCommit;
        }

        highestSucceededOffset = maybeRaiseOffsetHighestSucceeded(baseOffsetToCommit, highestSucceededOffset);

        lengthBetweenBaseAndHighOffset = highestSucceededOffset - this.lowWaterMark + 1;

        if (lengthBetweenBaseAndHighOffset < 0) {
            // sanity check
            throw new IllegalStateException(msg("Cannot have negative length encoding (calculated length: {}, base offset to commit: {}, highest succeeded offset: {})",
                    lengthBetweenBaseAndHighOffset, baseOffsetToCommit, highestSucceededOffset));
        }

        this.activeEncoders = initEncoders();
    }

    /**
     * Ensure that the {@param #highestSucceededOffset} is always at least a single offset behind the {}@param
     * baseOffsetToCommit}. Needed to allow us to jump over gaps in the partitions such as transaction markers.
     * <p>
     * Under normal operation, it is expected that the highest succeeded offset will generally always be higher than the
     * next expected offset to poll. This is because PC processes records well beyond the
     * {@link PartitionState#getOffsetHighestSequentialSucceeded()} all the time, unless operation in
     * {@link ParallelConsumerOptions.ProcessingOrder#PARTITION} order. So this situation - where the highest succeeded
     * offset is below the next offset to poll at the time of commit - will either be an incredibly rare case: only at
     * the very beginning of processing records, or where ALL records are slow enough or blocked, or in synthetically
     * created scenarios (like test cases).
     */
    private long maybeRaiseOffsetHighestSucceeded(long baseOffsetToCommit, long highestSucceededOffset) {
        long nextExpectedMinusOne = baseOffsetToCommit - 1;

        boolean gapLargerThanOne = highestSucceededOffset < nextExpectedMinusOne;
        if (gapLargerThanOne) {
            long gap = nextExpectedMinusOne - highestSucceededOffset;
            log.debug("Gap detected in partition (highest succeeded: {} while next expected poll offset: {} - gap is {}), probably tx markers. Moving highest succeeded to next expected - 1",
                    highestSucceededOffset,
                    nextExpectedMinusOne,
                    gap);
            // jump straight to the lowest incomplete - 1, allows us to jump over gaps in the partitions such as transaction markers
            highestSucceededOffset = nextExpectedMinusOne;
        }

        return highestSucceededOffset;
    }

    private ConcurrentHashMap.KeySetView<OffsetEncoder, Boolean> initEncoders() {
        ConcurrentHashMap.KeySetView<OffsetEncoder, Boolean> newEncoders = ConcurrentHashMap.newKeySet();
        if (lengthBetweenBaseAndHighOffset > LARGE_INPUT_MAP_SIZE) {
            log.trace("Relatively large input map size: {} (start: {} end: {})", lengthBetweenBaseAndHighOffset, lowWaterMark, getEndOffsetExclusive());
        }

        addBitsetEncoder(newEncoders, v1);
        addBitsetEncoder(newEncoders, v2);


        newEncoders.add(new RunLengthEncoder(this, v1));
        newEncoders.add(new RunLengthEncoder(this, v2));

        return newEncoders;
    }

    private void addBitsetEncoder(ConcurrentHashMap.KeySetView<OffsetEncoder, Boolean> newEncoders, OffsetEncoding.Version version) {
        try {
            newEncoders.add(new BitSetEncoder(lengthBetweenBaseAndHighOffset, this, version));
        } catch (BitSetEncodingNotSupportedException a) {
            log.debug("Cannot construct {} version {} : {}", BitSetEncoder.class.getSimpleName(), version, a.getMessage());
        }
    }

    /**
     * The end offset (exclusive)
     */
    private long getEndOffsetExclusive() {
        return lowWaterMark + lengthBetweenBaseAndHighOffset;
    }

    /**
     * Not enabled as byte buffer seems to always be beaten by BitSet, which makes sense
     * <p>
     * Visible for testing
     */
    void addByteBufferEncoder() {
        try {
            activeEncoders.add(new ByteBufferEncoder(lengthBetweenBaseAndHighOffset, this));
        } catch (ArithmeticException a) {
            log.warn("Cannot use {} encoder ({})", BitSetEncoder.class.getSimpleName(), a.getMessage());
        }
    }

    /**
     * Highwater mark already encoded in string - {@link OffsetMapCodecManager#makeOffsetMetadataPayload} - so encoding
     * BitSet run length may not be needed, or could be swapped
     * <p/>
     * Simultaneously encodes:
     * <ul>
     * <li>{@link OffsetEncoding#BitSet}</li>
     * <li>{@link OffsetEncoding#RunLength}</li>
     * </ul>
     * Conditionally encodes compression variants:
     * <ul>
     * <li>{@link OffsetEncoding#BitSetCompressed}</li>
     * <li>{@link OffsetEncoding#RunLengthCompressed}</li>
     * </ul>
     * Currently commented out is {@link OffsetEncoding#ByteArray} as there doesn't seem to be an advantage over
     * BitSet encoding.
     * <p>
     * TODO: optimisation - inline this into the partition iteration loop in {@link WorkManager}
     * <p>
     * TODO: optimisation - could double the run-length range from Short.MAX_VALUE (~33,000) to Short.MAX_VALUE * 2
     *  (~66,000) by using unsigned shorts instead (highest representable relative offset is Short.MAX_VALUE because each
     *  run-length entry is a Short)
     *
     * @see #buildSparseRelativeOffsetsToVisit()
     */
    public OffsetSimultaneousEncoder invoke() {
        /*
         * Decide the iteration strategy ONCE, up front, from the encoders that are actually active. Encoders which
         * write one unit of output per call (BitSet, ByteBuffer) must see every offset; if any such encoder is active
         * we have to walk the whole range anyway, so there is nothing to gain from the sparse walk.
         */
        boolean canIterateSparsely = activeEncoders.stream().noneMatch(OffsetEncoder::requiresEveryOffset);
        return invoke(canIterateSparsely);
    }

    /**
     * Visible for testing ONLY - production code must use {@link #invoke()}, which chooses the strategy itself.
     *
     * @param useSparseIteration if true, visit only the offsets needed by distance-based encoders (see
     *                           {@link #buildSparseRelativeOffsetsToVisit()}); if false, visit every offset in the
     *                           range. Only safe to pass true when no active encoder
     *                           {@link OffsetEncoder#requiresEveryOffset()}.
     */
    OffsetSimultaneousEncoder invoke(boolean useSparseIteration) {
        log.debug("Starting encode of incompletes, base offset is: {}, end offset is: {}", lowWaterMark, getEndOffsetExclusive());
        log.trace("Incompletes are: {}", this.incompleteOffsets);

        //
        log.debug("Encode loop offset start,end: [{},{}] length: {} sparse: {}", this.lowWaterMark, getEndOffsetExclusive(), lengthBetweenBaseAndHighOffset, useSparseIteration);

        this.sparseIterationUsed = useSparseIteration;
        Iterable<Long> relativeOffsetsToVisit = useSparseIteration
                ? buildSparseRelativeOffsetsToVisit()
                : range(lengthBetweenBaseAndHighOffset);

        relativeOffsetsToVisit.forEach(relativeOffset -> {
            // range index (relativeOffset) is used as we don't actually encode offsets, we encode the relative offset from the base offset
            final long actualOffset = this.lowWaterMark + relativeOffset;
            final boolean isIncomplete = this.incompleteOffsets.contains(actualOffset);
            activeEncoders.forEach(encoder -> {
                try {
                    if (isIncomplete) {
                        log.trace("Found an incomplete offset {}", actualOffset);
                        encoder.encodeIncompleteOffset(relativeOffset);
                    } else {
                        encoder.encodeCompletedOffset(relativeOffset);
                    }
                } catch (EncodingNotSupportedException e) {
                    log.debug("Error encoding offset {} with encoder {}, removing encoder", actualOffset, encoder, e);
                    activeEncoders.remove(encoder);
                }
            });
        });

        registerEncodings(activeEncoders);

        log.debug("In order: {}", this.sortedEncodings);

        return this;
    }

    /**
     * The (ascending, deduplicated) set of relative offsets that a purely distance-based encoder needs to be shown in
     * order to produce exactly the same output as walking the entire range.
     * <p>
     * <b>Why this is correct - do not "simplify" this away.</b> {@link RunLengthEncoder} accumulates the open run by the
     * <em>delta</em> to the previously seen offset ({@code currentRunLengthSize += relativeOffset - previousRangeIndex}),
     * it does not count calls. So for a maximal run of same-state offsets {@code [a,b]}:
     * <ul>
     *     <li>the call at {@code a} sees a state change, closes the previous run and opens a new one of size 1;</li>
     *     <li>the call at {@code b} adds delta {@code b - a}, giving the correct length {@code b - a + 1};</li>
     *     <li>any additional calls <em>inside</em> {@code (a,b)} are harmless - the deltas simply telescope to the
     *     same total.</li>
     * </ul>
     * Therefore it suffices to visit the first AND last offset of every maximal run. Both are needed: visiting only run
     * starts would close each run at length 1.
     * <p>
     * Because a state change can only happen at an incomplete offset or immediately after one, the run boundaries are
     * covered by the union of:
     * <ul>
     *     <li>{@code 0} (the first run always starts here, and it anchors the initial delta), and {@code length - 1}
     *     (the last run always ends here),</li>
     *     <li>for each incomplete offset in range, its relative offset and its two neighbours, clamped to the range.</li>
     * </ul>
     * A few redundant offsets are included (per the "harmless inside a run" property above) in exchange for an
     * obviously-correct construction.
     * <p>
     * Note the returned collection is sized by the number of <em>incompletes</em>, never by the range - the whole point
     * is to not touch a structure proportional to a range that can be ~2.1 billion wide.
     */
    private List<Long> buildSparseRelativeOffsetsToVisit() {
        final long lastRelativeOffset = lengthBetweenBaseAndHighOffset - 1;
        if (lastRelativeOffset < 0) {
            // empty range - the full scan would visit nothing either
            return Collections.emptyList();
        }

        SortedSet<Long> relativeOffsetsToVisit = new TreeSet<>();
        relativeOffsetsToVisit.add(0L);
        relativeOffsetsToVisit.add(lastRelativeOffset);

        // Only incompletes that actually fall inside the encoded range matter - the full scan never looks outside it.
        // Filtered by hand rather than via subSet(): this set is caller-supplied, and SortedSet#subSet throws
        // IllegalArgumentException when the bounds fall outside an already-restricted view. That would surface as a
        // failure to encode a commit, so it is not worth the risk - iteration stops early anyway because the set is
        // sorted, giving the same cost as a subSet view.
        final long endOffsetExclusive = getEndOffsetExclusive();
        for (Long incompleteOffset : this.incompleteOffsets) {
            if (incompleteOffset < lowWaterMark) {
                continue; // below the range - later entries may still be inside it
            }
            if (incompleteOffset >= endOffsetExclusive) {
                break; // sorted, so nothing beyond this point is in range either
            }
            final long relativeOffset = incompleteOffset - lowWaterMark;
            relativeOffsetsToVisit.add(Math.max(0, relativeOffset - 1));
            relativeOffsetsToVisit.add(relativeOffset);
            relativeOffsetsToVisit.add(Math.min(lastRelativeOffset, relativeOffset + 1));
        }

        log.debug("Sparse encode: visiting {} relative offsets instead of {}", relativeOffsetsToVisit.size(), lengthBetweenBaseAndHighOffset);

        // ascending order is required - the run-length delta must always be positive
        return new ArrayList<>(relativeOffsetsToVisit);
    }

    /**
     * Visible for testing ONLY. Drops every encoder that {@link OffsetEncoder#requiresEveryOffset()}, reproducing the
     * production situation in which the sparse path in {@link #invoke()} is actually taken: an offset range so large
     * that {@link BitSetEncoder} cannot even be constructed, leaving only the {@link RunLengthEncoder}s.
     * <p>
     * Lets tests exercise the sparse path over small, fast ranges.
     */
    void dropEncodersRequiringEveryOffset() {
        activeEncoders.removeIf(OffsetEncoder::requiresEveryOffset);
    }

    private void registerEncodings(final Set<? extends OffsetEncoder> encoders) {
        List<OffsetEncoder> toRemove = new ArrayList<>();
        for (OffsetEncoder encoder : encoders) {
            try {
                encoder.register();
            } catch (EncodingNotSupportedException e) {
                log.debug("Removing {} encoder, not supported ({})", encoder.getEncodingType().description(), e.getMessage());
                toRemove.add(encoder);
            }
        }
        toRemove.forEach(encoders::remove);

        // compressed versions
        // sizes over LARGE_INPUT_MAP_SIZE_THRESHOLD bytes seem to benefit from compression
        boolean noEncodingsAreSmallEnough = encoders.stream().noneMatch(OffsetEncoder::quiteSmall);
        if (noEncodingsAreSmallEnough || compressionForced) {
            encoders.forEach(OffsetEncoder::registerCompressed);
        }
    }

    /**
     * Select the smallest encoding, and pack it.
     *
     * @see #packEncoding(EncodedOffsetPair)
     */
    public byte[] packSmallest() throws NoEncodingPossibleException {
        if (sortedEncodings.isEmpty()) {
            throw new NoEncodingPossibleException("No encodings could be used");
        }
        final EncodedOffsetPair best = this.sortedEncodings.first();
        log.debug("Compression chosen is: {}", best.encoding.name());
        return packEncoding(best);
    }

    /**
     * Pack the encoded bytes into a magic byte wrapped byte array which indicates the encoding type.
     */
    byte[] packEncoding(final EncodedOffsetPair best) {
        final int magicByteSize = Byte.BYTES;
        final ByteBuffer result = ByteBuffer.allocate(magicByteSize + best.data.capacity());
        result.put(best.encoding.magicByte);
        result.put(best.data);
        return result.array();
    }

}
