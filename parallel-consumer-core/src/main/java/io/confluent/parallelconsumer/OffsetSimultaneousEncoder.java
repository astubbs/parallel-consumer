package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.*;

import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.OffsetEncoding.Version.v1;
import static io.confluent.parallelconsumer.OffsetEncoding.Version.v2;

/**
 * Encode with multiple strategies at the same time.
 * <p>
 * Have results in an accessible structure, easily selecting the highest compression.
 *
 * @see #invoke()
 */
@Slf4j
class OffsetSimultaneousEncoder implements OffsetEncoderContract {

    /**
     * Size threshold in bytes after which compressing the encodings will be compared, as it seems to be typically worth
     * the extra compression step when beyond this size in the source array.
     */
    public static final int LARGE_INPUT_MAP_SIZE_THRESHOLD = 200;

//    /**
//     * The offsets which have not yet been fully completed and can't have their offset committed
//     */
//    @Getter
//    private final Set<Long> incompleteOffsets;

    /**
     * The highest committable offset
     */
    private long baseOffset;

    /**
     * The next expected offset to be returned by the broker
     */
    private long nextExpectedOffset;

    /**
     * The difference between the base offset (the offset to be committed) and the highest seen offset
     */
    private int length;

    /**
     * Map of different encoding types for the same offset data, used for retrieving the data for the encoding type
     */
    @Getter
    Map<OffsetEncoding, byte[]> encodingMap = new EnumMap<>(OffsetEncoding.class);

    /**
     * Ordered set of the the different encodings, used to quickly retrieve the most compressed encoding
     *
     * @see #packSmallest()
     */
    @Getter
    PriorityQueue<EncodedOffsetData> sortedEncodingData = new PriorityQueue();


    /**
     * Force the encoder to also add the compressed versions. Useful for testing.
     * <p>
     * Visible for testing.
     */
    static boolean compressionForced = false;

    /**
     * The encoders to run
     */
    private Set<OffsetEncoderBase> encoders;
    private List<OffsetEncoderBase> sortedEncoders;

    /**
     * @param lowWaterMark The highest committable offset
     */
    public OffsetSimultaneousEncoder(
            long lowWaterMark
            ,
            Long nextExpectedOffset

//            ,
    ) {

//        this.incompleteOffsets = incompleteOffsets;

        initialise(lowWaterMark, nextExpectedOffset);
    }

    private int initLength(long currentBaseOffset, long currentNextExpectedOffset) {
        long longLength = currentNextExpectedOffset - currentBaseOffset;
        int intLength = (int) longLength;
        // sanity
        if (longLength != intLength)
            throw new IllegalArgumentException("Casting inconsistency");
        return intLength;
    }

    private void initialise(final long currentBaseOffset, long currentNextExpectedOffset) {
        this.baseOffset = currentBaseOffset;
        this.nextExpectedOffset = currentNextExpectedOffset;
        this.length = initLength(currentBaseOffset, currentNextExpectedOffset);

        if (length > LARGE_INPUT_MAP_SIZE_THRESHOLD) {
            log.debug("~Large input map size: {} (start: {} end: {})", length, this.baseOffset, nextExpectedOffset);
        }

        encoders = new HashSet<>();
//        sortedEncoders = new PriorityQueue<>();
        sortedEncoders = new ArrayList<>();

        try {
            encoders.add(new BitsetEncoder(baseOffset, length, this, v1));
        } catch (BitSetEncodingNotSupportedException a) {
            log.debug("Cannot use {} encoder ({})", BitsetEncoder.class.getSimpleName(), a.getMessage());
        }

        try {
            encoders.add(new BitsetEncoder(baseOffset, length, this, v2));
        } catch (BitSetEncodingNotSupportedException a) {
            log.warn("Cannot use {} encoder ({})", BitsetEncoder.class.getSimpleName(), a.getMessage());
        }

        encoders.add(new RunLengthEncoder(baseOffset, this, v1));
        encoders.add(new RunLengthEncoder(baseOffset, this, v2));

        sortedEncoders.addAll(encoders);
    }

    /**
     * Not enabled as byte buffer seems to always be beaten by BitSet, which makes sense
     * <p>
     * Visible for testing
     */
    void addByteBufferEncoder(long baseOffset) {
        encoders.add(new ByteBufferEncoder(baseOffset, length, this));
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
     * Conditionaly encodes compression variants:
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
     *  (~66,000) by using unsigned shorts instead (higest representable relative offset is Short.MAX_VALUE because each
     *  runlength entry is a Short)
     * <p>
     *  TODO VERY large offests ranges are slow (Integer.MAX_VALUE) - encoding scans could be avoided if passing in map of incompletes which should already be known
     */
    public OffsetSimultaneousEncoder invoke(Set<Long> incompleteOffsets, final long currentBaseOffset, final long nextExpectedOffsetFromBroker) {
        checkConditionsHaventChanged(currentBaseOffset, nextExpectedOffsetFromBroker);

        log.debug("Starting encode of incompletes, base offset is: {}, end offset is: {}", baseOffset, nextExpectedOffset);
        log.trace("Incompletes are: {}", incompleteOffsets);

        //
        log.debug("Encode loop offset start,end: [{},{}] length: {}", this.baseOffset, this.nextExpectedOffset, length);
        /*
         * todo refactor this loop into the encoders (or sequential vs non sequential encoders) as RunLength doesn't need
         *  to look at every offset in the range, only the ones that change from 0 to 1. BitSet however needs to iterate
         *  the entire range. So when BitSet can't be used, the encoding would be potentially a lot faster as RunLength
         *  didn't need the whole loop.
         */
        range(length).forEach(rangeIndex -> {
            final long offset = this.baseOffset + rangeIndex;
            if (incompleteOffsets.contains(offset)) {
                log.trace("Found an incomplete offset {}", offset);
                encoders.forEach(x -> {
                    x.encodeIncompleteOffset(rangeIndex);
                });
            } else {
                encoders.forEach(x -> {
                    x.encodeCompletedOffset(rangeIndex);
                });
            }
        });

        isToBeCalledRegisterEncodings(encoders);

        log.debug("In order: {}", this.sortedEncodingData);

        return this;
    }

    private void isToBeCalledRegisterEncodings(final Set<? extends OffsetEncoderBase> encoders) {
        List<OffsetEncoderBase> toRemove = new ArrayList<>();
        for (OffsetEncoderBase encoder : encoders) {
            try {
                encoder.register();
            } catch (EncodingNotSupportedException e) {
                log.debug("Removing {} encoder, not supported ({})", encoder.getEncodingType().description(), e.getMessage());
                toRemove.add(encoder);
            }
        }
        encoders.removeAll(toRemove);

        // compressed versions
        // sizes over LARGE_INPUT_MAP_SIZE_THRESHOLD bytes seem to benefit from compression
        boolean noEncodingsAreSmallEnough = encoders.stream().noneMatch(OffsetEncoderBase::quiteSmall);
        if (noEncodingsAreSmallEnough || compressionForced) {
            encoders.forEach(OffsetEncoderBase::registerCompressed);
        }
    }

    /**
     * Select the smallest encoding, and pack it.
     *
     * @see #packEncoding(EncodedOffsetData)
     */
    public byte[] packSmallest() throws EncodingNotSupportedException {
        // todo might be called multiple times, should cache?
        if (sortedEncodingData.isEmpty()) {
            throw new EncodingNotSupportedException("No encodings could be used");
        }
        final EncodedOffsetData best = this.sortedEncodingData.poll();
        log.debug("Compression chosen is: {}", best.encoding.name());
        return packEncoding(best);
    }

    /**
     * Pack the encoded bytes into a magic byte wrapped byte array which indicates the encoding type.
     */
    byte[] packEncoding(final EncodedOffsetData best) {
        final int magicByteSize = Byte.BYTES;
        final ByteBuffer result = ByteBuffer.allocate(magicByteSize + best.data.capacity());
        result.put(best.encoding.magicByte);
        result.put(best.data);
        return result.array();
    }

    @Override
    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long nextExpectedOffsetFromBroker) {
        if (preEncodeCheckCanSkip(baseOffset, relativeOffset, nextExpectedOffsetFromBroker))
            return;

        for (final OffsetEncoderBase encoder : encoders) {
            encoder.encodeIncompleteOffset(baseOffset, relativeOffset, nextExpectedOffsetFromBroker);
        }
    }

    @Override
    public void encodeCompletedOffset(final long baseOffset, final long relativeOffset, final long nextExpectedOffsetFromBroker) {
        if (preEncodeCheckCanSkip(baseOffset, relativeOffset, nextExpectedOffsetFromBroker))
            return;

        for (final OffsetEncoderBase encoder : encoders) {
            encoder.encodeIncompleteOffset(baseOffset, relativeOffset, nextExpectedOffsetFromBroker);
        }
    }

    private boolean preEncodeCheckCanSkip(final long currentBaseOffset, final long relativeOffset, final long nextExpectedOffsetFromBroker) {
        checkConditionsHaventChanged(currentBaseOffset, nextExpectedOffsetFromBroker);

        return lowWaterMarkCheck(relativeOffset);
    }

    private void checkConditionsHaventChanged(final long currentBaseOffset, final long nextExpectedOffsetFromBroker) {
        boolean reinitialise = false;

        if (this.nextExpectedOffset != nextExpectedOffsetFromBroker) {
            log.debug("Next expected offset from broker {} has moved to {} - need to reset encoders",
                    this.nextExpectedOffset, nextExpectedOffsetFromBroker);
            reinitialise = true;

        }

        if (this.baseOffset != currentBaseOffset) {
            log.debug("Base offset {} has moved to {} - new continuous blocks of successful work - need to reset encoders",
                    this.baseOffset, currentBaseOffset);
            reinitialise = true;
        }

        if (reinitialise) {
            initialise(currentBaseOffset, nextExpectedOffsetFromBroker);
        }
    }

    /**
     * todo docs
     */
    private boolean definitlyNoEncodingRequiredSoFar = true;

    private boolean lowWaterMarkCheck(final long relativeOffset) {
        // only encode if this work is above the low water mark
        definitlyNoEncodingRequiredSoFar = relativeOffset <= 0;
        return definitlyNoEncodingRequiredSoFar;
    }

    /**
     * @return the packed size of the best encoder, or 0 if no encodings have been performed / needed
     */
    @SneakyThrows
    @Override
    public int getEncodedSize() {
//        if (noEncodingRequiredSoFar) {
//            return 0;
//        } else {
//            OffsetEncoder peek = sortedEncoders.peek();
//            return peek.getEncodedSize();
//        }
        throw new InternalRuntimeError("");
    }

    @Override
    public byte[] getEncodedBytes() {
        return new byte[0];
    }

    @Override
    public int getEncodedSizeEstimate() {
        if (definitlyNoEncodingRequiredSoFar) {
            return 0;
        } else {
            Collections.sort(sortedEncoders);
            if (sortedEncoders.isEmpty())
                throw new InternalRuntimeError("No encoders");
            OffsetEncoderBase smallestEncoder = sortedEncoders.get(0);
            int smallestSizeEstimate = smallestEncoder.getEncodedSizeEstimate();
            log.debug("Currently estimated smallest codec is {}, needing {} bytes",
                    smallestEncoder.getEncodingType(), smallestSizeEstimate);
            return smallestSizeEstimate;
        }
    }

    public Object getSmallestCodec() {
        Collections.sort(sortedEncoders);
        if (sortedEncoders.isEmpty())
            throw new InternalRuntimeError("No encoders");
        return sortedEncoders.get(0);
    }
}
