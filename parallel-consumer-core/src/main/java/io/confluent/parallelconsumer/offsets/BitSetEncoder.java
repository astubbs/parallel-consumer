package io.confluent.parallelconsumer.offsets;

import io.confluent.csid.utils.MathUtils;
import io.confluent.csid.utils.StringUtils;
import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import static io.confluent.parallelconsumer.offsets.OffsetEncoding.*;
import static java.lang.Math.toIntExact;

/**
 * Encodes a range of offsets, from an incompletes collection into a BitSet.
 * <p>
 * Highly efficient when the completion status is random.
 * <p>
 * Highly inefficient when the completion status is in large blocks ({@link RunLengthEncoder} is much better)
 * <p>
 * Because our system works on manipulating INCOMPLETE offsets, it doesn't matter if the offset range we're encoding is
 * Sequential or not. Because as records are always in commit order, if we've seen a range of offsets, we know we've
 * seen all that exist (within said range). So if offset 8 is missing from the partition, we will encode it as having
 * been completed (when in fact it doesn't exist), because we only compare against known incompletes, and assume all
 * others are complete.
 * <p>
 * So, when we deserialize, the INCOMPLETES collection is then restored, and that's what's used to compare to see if a
 * record should be skipped or not. So if record 8 is recorded as completed, it will be absent from the restored
 * INCOMPLETES list, and we are assured we will never see record 8.
 *
 * @see RunLengthEncoder
 * @see OffsetBitSet
 */
@ToString(onlyExplicitlyIncluded = true, callSuper = true)
@Slf4j
public class BitSetEncoder extends OffsetEncoder {

    /**
     * {@link BitSet} only supports {@link Integer#MAX_VALUE) bits
     */
    public static final Integer MAX_LENGTH_ENCODABLE = Integer.MAX_VALUE;

    private static final Version DEFAULT_VERSION = Version.v2;

    @ToString.Include
    private int originalLength;
//    private ByteBuffer wrappedBitsetBytesBuffer;

    // todo use a different structure that can resize dynamically?
    @Getter(AccessLevel.PRIVATE)
    private final BitSetFragmentCollection bitSet;

    private Optional<byte[]> encodedBytes = Optional.empty();

    public BitSetEncoder(long baseOffset, long length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) throws BitSetEncodingNotSupportedException {
        this(baseOffset, length, offsetSimultaneousEncoder, DEFAULT_VERSION);
    }

    public BitSetEncoder(long baseOffset, long length, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) throws BitSetEncodingNotSupportedException {
        super(baseOffset, offsetSimultaneousEncoder, newVersion);

        try {
            bitSet = new BitSetFragmentCollection(baseOffset, length);
        } catch (EncodingNotSupportedException e) {
            throw new BitSetEncodingNotSupportedException(e);
        }

        // todo no op?
        reinitialise(baseOffset, length);
    }

    private ByteBuffer constructWrappedByteBuffer(long bitsetEntriesRequired, Version newVersion) throws BitSetEncodingNotSupportedException {
        return switch (newVersion) {
            case v1 -> initV1(bitsetEntriesRequired);
            case v2 -> initV2(bitsetEntriesRequired);
        };
    }

    /**
     * Switch from encoding bitset length as a short to an integer (Short.MAX_VALUE size of 32,000 was too short).
     * <p>
     * Integer.MAX_VALUE is the most we can use, as {@link BitSet} only supports {@link Integer#MAX_VALUE} bits.
     */
    // TODO refactor inivtV2 and V1 together, passing in the Short or Integer
    private ByteBuffer initV2(long bitsetEntriesRequired) throws BitSetEncodingNotSupportedException {
        if (bitsetEntriesRequired > MAX_LENGTH_ENCODABLE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new BitSetEncodingNotSupportedException(StringUtils.msg("BitSet V2 too long to encode, as length overflows Integer.MAX_VALUE. Length: {}. (max: {})", bitsetEntriesRequired, MAX_LENGTH_ENCODABLE));
        }

        int bytesRequiredForEntries = (int) (Math.ceil((double) bitsetEntriesRequired / Byte.SIZE));
        int lengthEntryWidth = Integer.BYTES;
        int wrappedBufferLength = lengthEntryWidth + bytesRequiredForEntries + 1;
        final ByteBuffer wrappedBitSetBytesBuffer = ByteBuffer.allocate(wrappedBufferLength);

        // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
        wrappedBitSetBytesBuffer.putInt(toIntExact(bitsetEntriesRequired));

        return wrappedBitSetBytesBuffer;
    }

    /**
     * This was a bit "short" sighted of me.... Encodes the capacity of the bitset as a short, which is only ~32,000
     * bits ({@link Short#MAX_VALUE}).
     */
    private ByteBuffer initV1(long bitsetEntriesRequired) throws BitSetEncodingNotSupportedException {
        if (bitsetEntriesRequired > Short.MAX_VALUE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new BitSetEncodingNotSupportedException("Input too long to encode for BitSet V1, length overflows Short.MAX_VALUE: " + bitsetEntriesRequired + ". (max: " + Short.MAX_VALUE + ")");
        }

        int bytesRequiredForEntries = (int) (Math.ceil((double) bitsetEntriesRequired / Byte.SIZE));
        int lengthEntryWidth = Short.BYTES;
        int wrappedBufferLength = lengthEntryWidth + bytesRequiredForEntries + 1;
        final ByteBuffer wrappedBitSetBytesBuffer = ByteBuffer.allocate(wrappedBufferLength);

        // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
        wrappedBitSetBytesBuffer.putShort(MathUtils.toShortExact(bitsetEntriesRequired));

        return wrappedBitSetBytesBuffer;
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return switch (version) {
            case v1 -> BitSet;
            case v2 -> BitSetV2;
        };
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return switch (version) {
            case v1 -> BitSetCompressed;
            case v2 -> BitSetV2Compressed;
        };
    }

//    @Override
//    public void encodeIncompleteOffset(final long newBaseOffset, final long relativeOffset) throws EncodingNotSupportedException {
//        // noop - bitset defaults to 0's (`unset`)
//    }
//
//    @Override
//    public void encodeCompletedOffset(final long newBaseOffset, final long relativeOffset) throws EncodingNotSupportedException {
//        log.trace("Relative offset set {}", relativeOffset);
//        try {
//            bitSet.set(Math.toIntExact(relativeOffset));
//        } catch (IndexOutOfBoundsException e) {
//            throw new BitSetEncodingNotSupportedException(StringUtils.msg("BitSetEncoder can't encode offset {} as it's too large for the bitset. (max: {})", relativeOffset, MAX_LENGTH_ENCODABLE), e);
//        }
//    }

    @SneakyThrows
    @Override
    public byte[] serialise() {
        final byte[] bitSetArray = this.bitSet.toByteArray();
        var bitsetEntriesRequired = bitSet.calculateTotalOffsetsRepresented();
        ByteBuffer wrappedBitsetBytesBuffer = constructWrappedByteBuffer(bitsetEntriesRequired, version);
        if (wrappedBitsetBytesBuffer.remaining() < bitSetArray.length)
            throw new InternalRuntimeException("Not enough space in byte array");
        try {
            // todo use ByteBuffer source instead of pre serialising the entire array
            wrappedBitsetBytesBuffer.put(bitSetArray);
        } catch (BufferOverflowException e) {
            log.error("Error writing byte array to ByteBuffer", e);
            throw e;
        }
        final byte[] array = wrappedBitsetBytesBuffer.array();
        this.encodedBytes = Optional.of(array);
        return array;
    }

    @Override
    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
        // noop - bitset defaults to 0's (`unset`)
    }

    @Override
    public void encodeCompleteOffset(final long newBaseOffset, final long relativeOffset, final long currentHighestCompleted) throws BitSetEncodingNotSupportedException {
        try {
            ensureCapacity(newBaseOffset, relativeOffset);
        } catch (EncodingNotSupportedException e) {
            throw new BitSetEncodingNotSupportedException(e);
        }

//        try {
//            // should not be rebuilding bitset every time a work container changes state
//            maybeReinitialise(newBaseOffset, currentHighestCompleted);
//        } catch (EncodingNotSupportedException e) {
//            this.disable(e);
//        }

        log.trace("Relative offset set {}", relativeOffset);
        try {
            bitSet.set(newBaseOffset, relativeOffset);
        } catch (IndexOutOfBoundsException e) {
            throw new BitSetEncodingNotSupportedException(StringUtils.msg("BitSetEncoder can't encode offset {} as it's too large for the bitset. (max: {})", relativeOffset, MAX_LENGTH_ENCODABLE), e);
        }
    }

    @Override
    public int getEncodedSize() {
        return this.encodedBytes.get().length;
    }

    @Override
    public int getEncodedSizeEstimate() {
        int logicalSize = originalLength / Byte.SIZE;
        return logicalSize + getLengthEntryBytes();
    }

    @Override
    public void ensureCapacity(final long base, final long highest) throws EncodingNotSupportedException {
        this.bitSet.ensureCapacity(base, highest);
    }

//    @Override
//    public void maybeReinitialise(final long newBaseOffset, final long currentHighestCompleted) throws EncodingNotSupportedException {
//        boolean shouldReinitialise = false;
//
//        long newLength = currentHighestCompleted - newBaseOffset;
//        if (originalLength != newLength) {
////        if (this.highestSucceeded != currentHighestCompleted) {
//            log.debug("Length of Bitset changed {} to {}",
//                    originalLength, newLength);
//            shouldReinitialise = true;
//        }
//
//        if (originalBaseOffset != newBaseOffset) {
//            log.debug("Base offset {} has moved to {} - new continuous blocks of successful work - need to shift bitset right",
//                    this.originalBaseOffset, newBaseOffset);
//            shouldReinitialise = true;
//        }
//
//        if (newBaseOffset < originalBaseOffset)
//            throw new InternalRuntimeException("Base offset moved backwards from " + originalBaseOffset + " to " + newBaseOffset);
//
//        if (shouldReinitialise) {
//            reinitialise(newBaseOffset, newLength);
//        }
//
//    }

    // todo inline?
    // todo chain bitset fragments after each other, instead of rebuilding
    private void reinitialise(final long newBaseOffset, final long newLength) throws BitSetEncodingNotSupportedException {
//        if (newLength == -1) {
//            log.debug("Nothing to encode, highest successful offset one behind out starting point");
//            bitSet = new BitSetFragmentCollection();
//            this.originalLength = toIntExact(newLength);
//        } else if (newLength < -2) {
//            throw new InternalRuntimeException("Invalid state - highest successful too far behind starting point");
//        } else {
//            long baseDelta = newBaseOffset - originalBaseOffset;
//            // truncate at new relative delta
//
//            int endIndex = toIntExact(baseDelta + originalLength + 1);
//            int startIndex = (int) baseDelta;
//            BitSet truncated = this.bitSet.get(startIndex, endIndex);
//            this.bitSet = new BitSet(toIntExact(newLength));
//            this.bitSet.or(truncated); // fill with old values
//
////        bitSet = new BitSet(length);
//
//            this.originalLength = toIntExact(newLength);
//
//            // TODO throws away whats returned ??
////            constructWrappedByteBuffer(toIntExact(newLength), this.version);
//
////            this.bitSet = new BitSet((int) newLength);
//
//
//            enable();
//        }
    }

    private int getLengthEntryBytes() {
        return switch (version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
        };
    }

    @Override
    public byte[] getEncodedBytes() {
        return this.encodedBytes.get();
    }

    /**
     * Like BitSet#stream(), but with relative offsets. Slow for large sets - useful for testing.
     *
     * @see BitSet#stream()
     */
    public List<Long> toList() {
        return this.getBitSet().toArray();
    }

    /**
     * @return boxed wrapper of the underlying bitset
     */
    public Long[] toBoxedArray() {
        return toList().toArray(new Long[0]);
    }

    /**
     * @return primitive array of the underlying bitset
     */
    public long[] toArray() {
        return getBitSet().stream().toArray();
    }
}
