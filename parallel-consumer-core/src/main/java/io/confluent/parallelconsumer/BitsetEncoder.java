package io.confluent.parallelconsumer;

import io.confluent.csid.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Optional;

import static io.confluent.parallelconsumer.OffsetEncoding.*;

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
@Slf4j
class BitsetEncoder extends OffsetEncoderBase {

    private final Version version; // default to new version

    private static final Version DEFAULT_VERSION = Version.v2;

    public static final Integer MAX_LENGTH_ENCODABLE = Integer.MAX_VALUE;
    private final int originalLength;

    private ByteBuffer wrappedBitsetBytesBuffer;
    private final BitSet bitSet;

    private Optional<byte[]> encodedBytes = Optional.empty();

    public BitsetEncoder(long baseOffset, int length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) throws BitSetEncodingNotSupportedException {
        this(baseOffset, length, offsetSimultaneousEncoder, DEFAULT_VERSION);
    }

    public BitsetEncoder(long baseOffset, int length, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) throws BitSetEncodingNotSupportedException {
        super(baseOffset, offsetSimultaneousEncoder);

        this.version = newVersion;

        switch (newVersion) {
            case v1 -> initV1(length);
            case v2 -> initV2(length);
        }
        bitSet = new BitSet(length);

        this.originalLength = length;
    }

    /**
     * Switch from encoding bitset length as a short to an integer (length of 32,000 was reasonable too short).
     * <p>
     * Integer.MAX_VALUE should always be good enough as system restricts large from being processed at once.
     */
    private void initV2(int length) throws BitSetEncodingNotSupportedException {
        if (length > MAX_LENGTH_ENCODABLE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new BitSetEncodingNotSupportedException(StringUtils.msg("Bitset V2 too long to encode, as length overflows Integer.MAX_VALUE. Length: {}. (max: {})", length, MAX_LENGTH_ENCODABLE));
        }
        // prep bit set buffer
        this.wrappedBitsetBytesBuffer = ByteBuffer.allocate(Integer.BYTES + ((length / 8) + 1));
        // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
        this.wrappedBitsetBytesBuffer.putInt(length);
    }

    /**
     * This was a bit "short" sighted of me....
     */
    private void initV1(int length) throws BitSetEncodingNotSupportedException {
        if (length > Short.MAX_VALUE) {
            // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
            throw new BitSetEncodingNotSupportedException("Bitset V1 too long to encode, bitset length overflows Short.MAX_VALUE: " + length + ". (max: " + Short.MAX_VALUE + ")");
        }
        // prep bit set buffer
        this.wrappedBitsetBytesBuffer = ByteBuffer.allocate(Short.BYTES + ((length / 8) + 1));
        // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
        this.wrappedBitsetBytesBuffer.putShort((short) length);
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

    @Override
    public void encodeIncompleteOffset(final int relativeOffset) {
        // noop - bitset defaults to 0's (`unset`)
    }

    @Override
    public void encodeCompletedOffset(final int relativeOffset) {
        log.trace("Relative offset set {}", relativeOffset);
        bitSet.set(relativeOffset);
    }

    @Override
    public byte[] serialise() {
        final byte[] bitSetArray = this.bitSet.toByteArray();
        if (wrappedBitsetBytesBuffer.capacity() < bitSetArray.length)
            throw new InternalRuntimeError("Not enough space in byte array");
        this.wrappedBitsetBytesBuffer.put(bitSetArray);
        final byte[] array = this.wrappedBitsetBytesBuffer.array();
        this.encodedBytes = Optional.of(array);
        return array;
    }
//
//    @Override
//    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset) {
//        super(baseOffset, relativeOffset);
//    }
//
//    @Override
//    public void encodeCompletedOffset(final long baseOffset, final long relativeOffset) {
//        super(baseOffset, relativeOffset);
//    }

    @Override
    public int getEncodedSize() {
        return this.encodedBytes.get().length;
    }

    @Override
    public int getEncodedSizeEstimate() {
        return bitSet.length() + standardOverhead + getLengthEntryBytes(); // logical size
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

}
