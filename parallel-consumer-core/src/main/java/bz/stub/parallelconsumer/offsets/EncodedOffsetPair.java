package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Supplier;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE;

import static io.confluent.parallelconsumer.offsets.OffsetBitSet.deserialiseBitSetWrap;
import static io.confluent.parallelconsumer.offsets.OffsetBitSet.deserialiseBitSetWrapToIncompletes;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.*;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v1;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static io.confluent.parallelconsumer.offsets.OffsetRunLength.*;
import static io.confluent.parallelconsumer.offsets.OffsetSimpleSerialisation.decompressZstd;
import static io.confluent.parallelconsumer.offsets.OffsetSimpleSerialisation.deserialiseByteArrayToBitMapString;

/**
 * Encapsulates the encoding type, and the actual encoded data, when creating an offset map encoding. Central place for
 * decoding  the data.
 *
 * @author Antony Stubbs
 * @see #unwrap
 */
@Slf4j
public final class EncodedOffsetPair implements Comparable<EncodedOffsetPair> {

    public static final Comparator<EncodedOffsetPair> SIZE_COMPARATOR = Comparator.comparingInt(x -> x.data.capacity());
    @Getter
    OffsetEncoding encoding;
    @Getter
    ByteBuffer data;

    /**
     * @see #unwrap
     */
    EncodedOffsetPair(OffsetEncoding encoding, ByteBuffer data) {
        this.encoding = encoding;
        this.data = data;
    }

    @Override
    public int compareTo(EncodedOffsetPair o) {
        return SIZE_COMPARATOR.compare(this, o);
    }

    /**
     * Used for printing out the comparative map of each encoder
     */
    @Override
    public String toString() {
        return "\n{" + encoding.name() + ", \t\t\tsize=" + data.capacity() + "}";
    }

    /**
     * Copies array out of the ByteBuffer
     */
    public byte[] readDataArrayForDebug() {
        return copyBytesOutOfBufferForDebug(data);
    }

    private static byte[] copyBytesOutOfBufferForDebug(ByteBuffer bbData) {
        bbData.position(0);
        byte[] bytes = new byte[bbData.remaining()];
        bbData.get(bytes, 0, bbData.limit());
        return bytes;
    }

    /**
     * Splits a payload into its magic byte and its body, resolving the magic byte to an encoding.
     *
     * @throws UnknownOffsetMetadataMagicException if the magic byte belongs to no encoding this build knows - which
     *                                             the caller cannot suppress. Production decoding goes through
     *                                             {@link #decodeToIncompletes} instead, which honours the user's
     *                                             {@link InvalidOffsetMetadataHandlingPolicy}
     */
    @SneakyThrows
    static EncodedOffsetPair unwrap(byte[] input) {
        ByteBuffer wrap = ByteBuffer.wrap(input).asReadOnlyBuffer();
        byte magic = wrap.get();
        OffsetEncoding decode = decode(magic);
        ByteBuffer slice = wrap.slice();

        return new EncodedOffsetPair(decode, slice);
    }

    /**
     * The production decode entry point: turns a raw metadata payload into the incompletes it represents, applying the
     * user's {@link InvalidOffsetMetadataHandlingPolicy} to <b>every</b> way the payload can turn out to be
     * undecodable by this build.
     * <p>
     * There are three such ways, and all of them must reach the policy - it exists precisely for metadata this build
     * cannot read:
     * <ol>
     *     <li>the magic byte matches no known encoding at all - the forward-compatibility case, where a newer version
     *     of Parallel Consumer wrote an encoding that did not exist when this version was built</li>
     *     <li>the encoding is known but this build has no decoder for it</li>
     *     <li>the metadata belongs to Kafka Streams (a reused consumer group)</li>
     * </ol>
     * Before this method existed, (1) was decided by {@link OffsetEncoding#decode} - upstream of anywhere the policy
     * was known - so an older consumer reading a newer consumer's commit died with a raw {@link RuntimeException} no
     * matter how the policy was configured.
     *
     * @param baseOffset  the committed offset the payload is relative to; also what we fall back to under
     *                    {@link InvalidOffsetMetadataHandlingPolicy#IGNORE}
     * @param errorPolicy what to do when this build cannot read the payload
     * @param tp          the partition the payload was committed against, for diagnosis - may be null when unknown
     */
    static HighestOffsetAndIncompletes decodeToIncompletes(byte[] input,
                                                           long baseOffset,
                                                           InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                           TopicPartition tp) {
        ByteBuffer wrap = ByteBuffer.wrap(input).asReadOnlyBuffer();
        byte magic = wrap.get();
        Optional<OffsetEncoding> encoding = OffsetEncoding.maybeDecode(magic);
        if (!encoding.isPresent()) { // Optional#isEmpty is Java 11 - this module compiles against the Java 8 API
            return handleUnreadableMetadata(baseOffset,
                    errorPolicy,
                    msg("unrecognised magic byte {} - most likely written by a newer version of Parallel Consumer", magic),
                    () -> new UnknownOffsetMetadataMagicException(magic, describeSource(tp, baseOffset)),
                    tp);
        }
        return new EncodedOffsetPair(encoding.get(), wrap.slice())
                .getDecodedIncompletes(baseOffset, errorPolicy, tp);
    }

    /**
     * Applies the user's {@link InvalidOffsetMetadataHandlingPolicy} to a payload this build cannot read.
     * <p>
     * Under {@link InvalidOffsetMetadataHandlingPolicy#IGNORE}, the metadata is treated as absent: we warn loudly with
     * everything needed to diagnose it, and continue from the committed offset - which replays anything that was
     * completed but not yet committed, and is strictly better than refusing to start.
     *
     * @param baseOffset the committed offset, which is the <b>next</b> offset expected to be polled - so the highest
     *                   offset we can claim to have seen is the one BELOW it. Getting this wrong loses a record:
     *                   {@code of(baseOffset)} would mark the committed offset itself as succeeded, so
     *                   {@link io.confluent.parallelconsumer.state.PartitionState#isRecordPreviouslyCompleted} would
     *                   skip that record and the next commit would be {@code baseOffset + 1}. Matches the
     *                   no-metadata-at-all branch of {@link OffsetMapCodecManager#decodeCompressedOffsets}, which is
     *                   the same situation - we have a committed offset and no readable map to go with it.
     * @param problem  what is wrong, in log voice
     * @param toThrow  the typed exception for the strict policy, built lazily so its (longer) advice text costs
     *                 nothing on the IGNORE path
     */
    @SneakyThrows
    private static HighestOffsetAndIncompletes handleUnreadableMetadata(long baseOffset,
                                                                        InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                                        String problem,
                                                                        Supplier<? extends EncodingNotSupportedException> toThrow,
                                                                        TopicPartition tp) {
        if (errorPolicy == IGNORE) {
            log.warn("Cannot read the committed offset metadata for partition {} at base offset {}: {}. " +
                            "invalidOffsetMetadataPolicy is IGNORE, so the metadata is being discarded and processing " +
                            "will continue from the committed offset - records that were completed but not committed " +
                            "before this point will be replayed.",
                    tp, baseOffset, problem);
            return HighestOffsetAndIncompletes.of(baseOffset - 1);
        }
        throw toThrow.get();
    }

    /**
     * Renders where a payload came from, for exception messages.
     *
     * @param tp may be null when the caller did not know the partition
     */
    static String describeSource(TopicPartition tp, long baseOffset) {
        return msg("partition: {}, base offset: {}", tp, baseOffset);
    }

    @SneakyThrows
    public String getDecodedString() {
        String binaryArrayString = switch (encoding) {
            case ByteArray -> deserialiseByteArrayToBitMapString(data);
            case ByteArrayCompressed -> deserialiseByteArrayToBitMapString(decompressZstd(data));
            case BitSet -> deserialiseBitSetWrap(data, v1);
            case BitSetCompressed -> deserialiseBitSetWrap(decompressZstd(data), v1);
            case RunLength -> runLengthDecodeToString(runLengthDeserialise(data));
            case RunLengthCompressed -> runLengthDecodeToString(runLengthDeserialise(decompressZstd(data)));
            case BitSetV2 -> deserialiseBitSetWrap(data, v2);
            case BitSetV2Compressed -> deserialiseBitSetWrap(data, v2);
            case RunLengthV2 -> deserialiseBitSetWrap(data, v2);
            case RunLengthV2Compressed -> deserialiseBitSetWrap(data, v2);
            default ->
                    throw new InternalRuntimeException("Invalid state"); // todo why is this needed? what's not covered?
        };
        return binaryArrayString;
    }

    public HighestOffsetAndIncompletes getDecodedIncompletes(long baseOffset) {
        return getDecodedIncompletes(baseOffset,  ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL);
    }

    public HighestOffsetAndIncompletes getDecodedIncompletes(long baseOffset, InvalidOffsetMetadataHandlingPolicy errorPolicy) {
        return getDecodedIncompletes(baseOffset, errorPolicy, null);
    }

    /**
     * @param tp the partition this payload was committed against, for diagnosis - may be null when unknown
     */
    @SneakyThrows
    public HighestOffsetAndIncompletes getDecodedIncompletes(long baseOffset,
                                                             InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                             TopicPartition tp) {
        HighestOffsetAndIncompletes binaryArrayString = switch (encoding) {
//            case ByteArray -> deserialiseByteArrayToBitMapString(data);
//            case ByteArrayCompressed -> deserialiseByteArrayToBitMapString(decompressZstd(data));
            case BitSet -> deserialiseBitSetWrapToIncompletes(encoding, baseOffset, data);
            case BitSetCompressed -> deserialiseBitSetWrapToIncompletes(BitSet, baseOffset, decompressZstd(data));
            case RunLength -> runLengthDecodeToIncompletes(encoding, baseOffset, data);
            case RunLengthCompressed -> runLengthDecodeToIncompletes(RunLength, baseOffset, decompressZstd(data));
            case BitSetV2 -> deserialiseBitSetWrapToIncompletes(encoding, baseOffset, data);
            case BitSetV2Compressed -> deserialiseBitSetWrapToIncompletes(BitSetV2, baseOffset, decompressZstd(data));
            case RunLengthV2 -> runLengthDecodeToIncompletes(encoding, baseOffset, data);
            case RunLengthV2Compressed -> runLengthDecodeToIncompletes(RunLengthV2, baseOffset, decompressZstd(data));
            case KafkaStreams, KafkaStreamsV2 -> handleUnreadableMetadata(baseOffset,
                    errorPolicy,
                    msg("the metadata was written by Kafka Streams ({})", encoding.description()),
                    KafkaStreamsEncodingNotSupported::new,
                    tp);
            // an encoding this build knows of but has no decoder for - same forward-compatibility hazard as an
            // unrecognised magic byte, so it gets the same policy treatment
            default -> handleUnreadableMetadata(baseOffset,
                    errorPolicy,
                    msg("no decoder for encoding: {}", encoding.description()),
                    () -> new UnsupportedOffsetEncodingException(encoding, describeSource(tp, baseOffset)),
                    tp);
        };
        return binaryArrayString;
    }
}
