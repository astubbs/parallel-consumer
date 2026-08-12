package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.offsets.OffsetEncoding.Version.v1;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.Version.v2;

/**
 * Offset encoding MagicNumbers to {@link OffsetEncoder}.
 *
 * @author Antony Stubbs
 */
@ToString
@RequiredArgsConstructor
public enum OffsetEncoding {
    ByteArray(v1, (byte) 'L'),
    ByteArrayCompressed(v1, (byte) 'î'),
    BitSet(v1, (byte) 'l'),
    BitSetCompressed(v1, (byte) 'a'),
    RunLength(v1, (byte) 'n'),
    RunLengthCompressed(v1, (byte) 'J'),
    /**
     * switch from encoding bitset length as a short to an integer (length of 32,000 was reasonable too short)
     */
    BitSetV2(v2, (byte) 'o'),
    BitSetV2Compressed(v2, (byte) 's'),
    /**
     * switch from encoding run lengths as Shorts to Integers
     */
    RunLengthV2(v2, (byte) 'e'),
    RunLengthV2Compressed(v2, (byte) 'p'),

    /**
     * Checks for pre-existing Kafka Streams metadata. Although the Kafka Streams magic numbers are annoyingly simple, ours are not, so should be safe to take this guess that they are indeed from Kafka Streams.
     * <a href="https://github.com/apache/kafka/blob/cc77a38d280657a0e3969b255f103af4d11c7914/streams/src/main/java/org/apache/kafka/streams/processor/internals/TopicPartitionMetadata.java#L33">source from Kafka Streams code</a>
     */
    KafkaStreams(v1, (byte) 1),
    KafkaStreamsV2(v2, (byte) 2);


    public enum Version {
        v1, v2
    }

    public final Version version;

    @Getter
    public final byte magicByte;

    private static final Map<Byte, OffsetEncoding> magicMap = Arrays.stream(values()).collect(Collectors.toMap(OffsetEncoding::getMagicByte, Function.identity()));

    /**
     * Resolves a magic byte to its encoding, without deciding what to do when it resolves to nothing.
     * <p>
     * An empty result is the forward-compatibility case: metadata written by a newer version of Parallel Consumer,
     * using an encoding that did not exist when this version was built. The caller is the one that knows the
     * configured {@link bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy},
     * so the caller - not this method - decides whether that is fatal.
     *
     * @param magic the leading byte of an offset metadata payload
     * @return the encoding that claims this magic byte, or empty if no encoding known to this build does
     * @see EncodedOffsetPair#decodeToIncompletes
     */
    public static Optional<OffsetEncoding> maybeDecode(byte magic) {
        return Optional.ofNullable(magicMap.get(magic));
    }

    /**
     * Resolves a magic byte to its encoding, or fails.
     * <p>
     * Deliberately keeps its original signature - no {@code throws} clause - even though
     * {@link UnknownOffsetMetadataMagicException} is checked. This method is {@code public}, and before this change it
     * threw a bare unchecked {@code RuntimeException}, so declaring the checked exception would break source
     * compatibility for any existing caller. {@code @SneakyThrows} is how the rest of this package already smuggles
     * these (see {@link EncodedOffsetPair#unwrap} and {@link EncodedOffsetPair#getDecodedIncompletes}); the caller
     * sees a strictly better exception than before, at the same signature.
     *
     * @throws UnknownOffsetMetadataMagicException if no encoding known to this build claims this magic byte
     * @see #maybeDecode for the policy-aware decode path, which production decoding uses instead
     */
    @SneakyThrows
    public static OffsetEncoding decode(byte magic) {
        Optional<OffsetEncoding> encoding = maybeDecode(magic);
        if (!encoding.isPresent()) { // Optional#isEmpty is Java 11 - this module compiles against the Java 8 API
            throw new UnknownOffsetMetadataMagicException(magic);
        }
        return encoding.get();
    }

    public String description() {
        return name() + ":" + version;
    }
}
