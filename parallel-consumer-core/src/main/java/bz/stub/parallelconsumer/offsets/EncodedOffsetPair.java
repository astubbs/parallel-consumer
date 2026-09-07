package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.InternalException;
import bz.stub.parallelconsumer.internal.PCInternalRuntimeException;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.Rider;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Supplier;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE;

import static bz.stub.parallelconsumer.offsets.OffsetBitSet.deserialiseBitSetWrap;
import static bz.stub.parallelconsumer.offsets.OffsetBitSet.deserialiseBitSetWrapToIncompletes;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.*;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.Version.v1;
import static bz.stub.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static bz.stub.parallelconsumer.offsets.OffsetRunLength.*;
import static bz.stub.parallelconsumer.offsets.OffsetSimpleSerialisation.decompressZstd;
import static bz.stub.parallelconsumer.offsets.OffsetSimpleSerialisation.deserialiseByteArrayToBitMapString;

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
        return decodeToRiderAndIncompletes(input, baseOffset, errorPolicy, tp).getOffsets();
    }

    /**
     * The same decode, carrying what the payload said about the rider slot as well as the offsets.
     * <p>
     * The envelope is unwrapped <b>here</b>, above the pair: the rider is held in a local, and the remainder - a fresh
     * array, copied out of the read-only buffer - re-enters this method, so the inner magic byte meets
     * {@link OffsetEncoding#maybeDecode} and the policy funnel by construction. Resolving it with
     * {@link OffsetEncoding#decode} instead would throw {@link OffsetDecodingError}, which
     * {@link OffsetMapCodecManager#loadPartitionStateForAssignment} swallows even under
     * {@link InvalidOffsetMetadataHandlingPolicy#FAIL}. The recursion is bounded: the envelope never nests, and
     * {@link OffsetRiderEnvelope#unwrap} rejects one that does.
     * <p>
     * The rider is merged into whatever comes back, <b>including the policy's fallback value</b> - an envelope that
     * parsed keeps its rider even when the offset map inside it did not, because the two are structurally independent.
     * The reverse case is why a discarded payload reports {@link OffsetRiderEnvelope.RiderState#UNREADABLE} rather
     * than {@code NONE}: "the metadata was thrown away" and "no rider was ever configured" must not read the same.
     */
    static OffsetMapCodecManager.DecodedMetadata decodeToRiderAndIncompletes(
            byte[] input,
            long baseOffset,
            InvalidOffsetMetadataHandlingPolicy errorPolicy,
            TopicPartition tp) {
        if (input.length == 0) {
            // Not reachable from production today: decodeCompressedOffsets branches on an empty payload before
            // calling here, because "no metadata committed" is a legitimate state rather than a corrupt one. That
            // makes this an invariant the caller happens to keep - and an invariant nothing enforces is one a future
            // caller breaks, here into an unhandled BufferUnderflowException off wrap.get(), which is exactly the
            // escape-the-policy shape this method exists to remove.
            return discarded(handleUnreadableMetadata(baseOffset,
                    errorPolicy,
                    msg("the payload is empty - not even a magic byte"),
                    () -> new CorruptOffsetMetadataException("the payload is empty", describeSource(tp, baseOffset)),
                    tp));
        }
        ByteBuffer wrap = ByteBuffer.wrap(input).asReadOnlyBuffer();
        byte magic = wrap.get();
        Optional<OffsetEncoding> encoding = OffsetEncoding.maybeDecode(magic);
        if (!encoding.isPresent()) { // Optional#isEmpty is Java 11 - this module compiles against the Java 8 API
            return discarded(handleUnreadableMetadata(baseOffset,
                    errorPolicy,
                    msg("unrecognised magic byte {} - most likely written by a newer version of Parallel Consumer", magic),
                    () -> new UnknownOffsetMetadataMagicException(magic, describeSource(tp, baseOffset)),
                    tp));
        }
        if (encoding.get() == RiderEnvelope) {
            return decodeEnvelope(input, baseOffset, errorPolicy, tp);
        }
        return new EncodedOffsetPair(encoding.get(), wrap.slice())
                .decodeWithRider(baseOffset, errorPolicy, tp);
    }

    /**
     * Unwraps a rider envelope and decodes what it carried.
     *
     * @param input the whole payload, envelope magic byte first
     */
    private static OffsetMapCodecManager.DecodedMetadata decodeEnvelope(byte[] input,
                                                                        long baseOffset,
                                                                        InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                                        TopicPartition tp) {
        OffsetRiderEnvelope.UnwrappedEnvelope envelope;
        try {
            envelope = OffsetRiderEnvelope.unwrap(input);
        } catch (CorruptOffsetMetadataException e) {
            // The envelope itself did not parse, so nothing is known about the rider slot - the same event to a user
            // as an unrecognised magic byte, and routed identically.
            return discarded(handleUnreadableMetadata(baseOffset,
                    errorPolicy,
                    msg("the rider envelope is not readable: {}", e.getMessage()),
                    () -> new CorruptOffsetMetadataException(problemOf(e), describeSource(tp, baseOffset)),
                    tp));
        }

        Rider rider = envelope.getRider();
        byte[] innerBytes = envelope.getInnerBytes();
        if (innerBytes.length == 0) {
            // A rider and no offset map: what a caught-up partition commits. Same answer as the no-metadata branch of
            // OffsetMapCodecManager#decodeCompressedOffsets and as handleUnreadableMetadata - the committed offset is
            // the NEXT one to be polled, so the highest we can claim to have seen is the one below it. One higher
            // marks the committed record as done and loses it.
            return OffsetMapCodecManager.DecodedMetadata.of(HighestOffsetAndIncompletes.of(baseOffset - 1), rider);
        }

        // Re-entry, on a copied array: the inner magic byte gets maybeDecode and the policy funnel by construction.
        // Bounded - unwrap rejects an envelope inside an envelope, so this can recurse at most once.
        var inner = decodeToRiderAndIncompletes(innerBytes, baseOffset, errorPolicy, tp);
        return OffsetMapCodecManager.DecodedMetadata.of(inner.getOffsets(), rider);
    }

    /**
     * What the rider slot reports for a payload the policy discarded: unknown, rather than "never configured".
     * <p>
     * A zero-length array or a {@code NONE} here would tell an embedder its rider was never written, which is a
     * different fact from "PC could not read this metadata at all" and leads to a different repair.
     */
    private static OffsetMapCodecManager.DecodedMetadata discarded(HighestOffsetAndIncompletes fallback) {
        return OffsetMapCodecManager.DecodedMetadata.of(fallback, Rider.unreadable());
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
     *                   {@link bz.stub.parallelconsumer.state.PartitionState#isRecordPreviouslyCompleted} would
     *                   skip that record and the next commit would be {@code baseOffset + 1}. Matches the
     *                   no-metadata-at-all branch of {@link OffsetMapCodecManager#decodeCompressedOffsets}, which is
     *                   the same situation - we have a committed offset and no readable map to go with it.
     * @param problem  what is wrong, in log voice
     * @param toThrow  the typed exception for the strict policy, built lazily so its (longer) advice text costs
     *                 nothing on the IGNORE path
     */
    @SneakyThrows
    static HighestOffsetAndIncompletes handleUnreadableMetadata(long baseOffset,
                                                                        InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                                        String problem,
                                                                        Supplier<? extends InternalException> toThrow,
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
     * The decoder's own description of what is wrong, unwrapped from its exception.
     * <p>
     * A {@link CorruptOffsetMetadataException} already carries a specific structural reason ("bitset declares N
     * bits..."), which is worth more than the class name; anything else (a truncated buffer, a bad zstd frame) has
     * only its type and message to offer.
     */
    private static String problemOf(Exception e) {
        return e instanceof CorruptOffsetMetadataException ? e.getMessage() : e.toString();
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
            case RiderEnvelope -> describeEnvelope();
            default ->
                    throw new PCInternalRuntimeException("Invalid state"); // todo why is this needed? what's not covered?
        };
        return binaryArrayString;
    }

    /**
     * Renders an envelope for a human: the rider's state and length (never its bytes - they are the embedder's, and
     * may be large), and the string of whatever encoding was inside it.
     * <p>
     * A rider-only payload has no inner encoding to render, so the rider's own length is the whole answer. This is a
     * diagnostic path reached from log lines, so an inner magic byte this build does not know is described rather
     * than thrown on: a rendering that fails turns one diagnosis into two.
     */
    private String describeEnvelope() throws CorruptOffsetMetadataException {
        ByteBuffer body = data.duplicate();
        body.rewind();
        byte[] payload = new byte[1 + body.remaining()];
        payload[0] = OffsetRiderEnvelope.MAGIC_BYTE;
        body.get(payload, 1, payload.length - 1);

        OffsetRiderEnvelope.UnwrappedEnvelope envelope = OffsetRiderEnvelope.unwrap(payload);
        byte[] innerBytes = envelope.getInnerBytes();
        if (innerBytes.length == 0) {
            return msg("{}, no inner encoding", envelope.getRider());
        }
        Optional<OffsetEncoding> innerEncoding = OffsetEncoding.maybeDecode(innerBytes[0]);
        if (!innerEncoding.isPresent()) { // Optional#isEmpty is Java 11 - this module compiles against the Java 8 API
            return msg("{}, wrapping an encoding this build does not know (magic byte {})",
                    envelope.getRider(), innerBytes[0]);
        }
        ByteBuffer innerBody = ByteBuffer.wrap(innerBytes).asReadOnlyBuffer();
        byte ignoredInnerMagic = innerBody.get(); // resolved above; consumed to leave the buffer on the body
        return msg("{}, wrapping {}", envelope.getRider(),
                new EncodedOffsetPair(innerEncoding.get(), innerBody.slice()).getDecodedString());
    }

    /**
     * Decodes under the strict {@link InvalidOffsetMetadataHandlingPolicy#FAIL} policy, for a caller with no
     * configured consumer to take a policy from.
     *
     * @see #getDecodedIncompletes(long, InvalidOffsetMetadataHandlingPolicy, TopicPartition)
     */
    public HighestOffsetAndIncompletes getDecodedIncompletes(long baseOffset) {
        return getDecodedIncompletes(baseOffset,  ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL);
    }

    /**
     * Decodes without a partition to name in diagnostics - the payload is all the caller has.
     *
     * @see #getDecodedIncompletes(long, InvalidOffsetMetadataHandlingPolicy, TopicPartition)
     */
    public HighestOffsetAndIncompletes getDecodedIncompletes(long baseOffset, InvalidOffsetMetadataHandlingPolicy errorPolicy) {
        return getDecodedIncompletes(baseOffset, errorPolicy, null);
    }

    /**
     * Turns this pair's payload into the highest offset seen and the incomplete offsets below it, applying the user's
     * policy to every way that can fail.
     * <p>
     * Three outcomes, and the point of this method is that they are <em>one</em> user-visible event - "this build
     * cannot read the committed metadata" - rather than three unrelated failures:
     * <ol>
     *     <li>the encoding is Kafka Streams', which this build never decodes;</li>
     *     <li>the encoding has a magic byte but no decoder here (the {@code ByteArray} pair);</li>
     *     <li>a decoder exists, but the bytes are not something any encoder here could have produced - see
     *     {@link CorruptOffsetMetadataException}, which is the case that used to return a fabricated offset map
     *     instead of failing.</li>
     * </ol>
     * A fourth outcome - a payload that decodes cleanly into a wrong-but-plausible map - is <b>not</b> covered, and
     * cannot be: nothing in such a payload proves it wrong.
     *
     * @param baseOffset  the committed offset the payload is relative to, and what {@code IGNORE} falls back to
     * @param errorPolicy what to do when this build cannot read the payload
     * @param tp          the partition this payload was committed against, for diagnosis - may be {@code null} when
     *                    the caller does not know it
     * @return the highest offset seen, and the incomplete offsets below it
     */
    public HighestOffsetAndIncompletes getDecodedIncompletes(long baseOffset,
                                                             InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                             TopicPartition tp) {
        return decodeWithRider(baseOffset, errorPolicy, tp).getOffsets();
    }

    /**
     * The body of {@link #getDecodedIncompletes(long, InvalidOffsetMetadataHandlingPolicy, TopicPartition)},
     * reporting what the payload said about the rider slot alongside the offsets.
     * <p>
     * Production only ever builds a pair for a payload with <b>no</b> envelope on it - {@link #decodeEnvelope}
     * unwraps the envelope above this method and substitutes its rider into the result, and a pair carrying one is
     * malformed input that the {@code RiderEnvelope} arm below routes to the policy. So the rider reported here is
     * {@link Rider#none()} when the body decodes and {@link Rider#unreadable()} when the policy discards it. Those
     * two must not read the same: "the metadata was thrown away" and "no rider was ever configured" lead to
     * different repairs.
     */
    @SneakyThrows
    OffsetMapCodecManager.DecodedMetadata decodeWithRider(long baseOffset,
                                                          InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                          TopicPartition tp) {
        switch (encoding) {
            case KafkaStreams:
            case KafkaStreamsV2:
                return OffsetMapCodecManager.DecodedMetadata.of(handleUnreadableMetadata(baseOffset,
                        errorPolicy,
                        msg("the metadata was written by Kafka Streams ({})", encoding.description()),
                        KafkaStreamsEncodingNotSupported::new,
                        tp), Rider.unreadable());
            // an encoding this build knows of but has no decoder for - same forward-compatibility hazard as an
            // unrecognised magic byte, so it gets the same policy treatment
            case ByteArray:
            case ByteArrayCompressed:
                return OffsetMapCodecManager.DecodedMetadata.of(handleUnreadableMetadata(baseOffset,
                        errorPolicy,
                        msg("no decoder for encoding: {}", encoding.description()),
                        () -> new UnsupportedOffsetEncodingException(encoding, describeSource(tp, baseOffset)),
                        tp), Rider.unreadable());
            // The envelope is unwrapped ABOVE the pair (decodeToRiderAndIncompletes), so a pair carrying it is
            // malformed input rather than something to decode - either metadata that is not what it claims, or a
            // caller that built the pair by hand. Either way it is the user's policy that decides, never
            // decodeBody's default: that default throws a bare PCInternalRuntimeException, which is exactly the
            // escape-the-policy shape this class exists to remove.
            case RiderEnvelope:
                return OffsetMapCodecManager.DecodedMetadata.of(handleUnreadableMetadata(baseOffset,
                        errorPolicy,
                        msg("a rider envelope reached the body decoder - it is unwrapped before a pair is built"),
                        () -> new CorruptOffsetMetadataException("a rider envelope is not an inner encoding",
                                describeSource(tp, baseOffset)),
                        tp), Rider.unreadable());
            // Every remaining constant has a decoder, so it belongs to decodeBody below rather than to the policy.
            // The assumption is not left implicit: decodeBody's own default throws PCInternalRuntimeException, so an
            // encoding added without a decoder AND without an arm here fails loudly instead of being decoded as
            // something else.
            default:
                break;
        }

        // A decoder exists, so the magic byte and the encoding are both fine - but the BYTES may still not be
        // something this build could have written. That is the same event to a user ("PC cannot read this metadata")
        // and it used to leave by a different door: BufferUnderflowException off the end of a truncated payload, or a
        // ZstdIOException from a body that is not a zstd frame, neither of which is an OffsetDecodingError, so
        // loadPartitionStateForAssignment's recovery never saw them and they escaped onPartitionsAssigned.
        // IllegalArgumentException and IndexOutOfBoundsException are the buffer-slicing family: a length or position
        // derived from bytes a stranger wrote, handed to ByteBuffer, raises one of them rather than
        // BufferUnderflowException. They are a backstop BEHIND the decoders' own validation, not a substitute for it -
        // the validation is what makes the diagnosis specific, and the catch is what stops a case nobody anticipated
        // escaping the policy as a bare runtime exception.
        try {
            return OffsetMapCodecManager.DecodedMetadata.of(decodeBody(baseOffset), Rider.none());
        } catch (CorruptOffsetMetadataException | BufferUnderflowException | IOException
                | IllegalArgumentException | IndexOutOfBoundsException e) {
            return OffsetMapCodecManager.DecodedMetadata.of(handleUnreadableMetadata(baseOffset,
                    errorPolicy,
                    msg("the payload is not decodable as {}: {}", encoding.description(), e.getMessage()),
                    // Always rebuild with describeSource: the decoders raise CorruptOffsetMetadataException through
                    // its one-argument constructor, which records "source unknown". Passing that instance through
                    // would discard the partition and base offset known only here - and under FAIL this exception
                    // escapes the rebalance callback inside Kafka's generic wrapper, where its message is the
                    // operator's only clue which assigned partition holds the bad metadata.
                    () -> new CorruptOffsetMetadataException(problemOf(e), describeSource(tp, baseOffset)),
                    tp), Rider.unreadable());
        }
    }

    /**
     * Decodes a payload whose encoding this build does have a decoder for.
     * <p>
     * Separate from {@link #getDecodedIncompletes(long, InvalidOffsetMetadataHandlingPolicy, TopicPartition)} so that
     * every way this can fail is caught in one place and routed through the user's policy, rather than each decoder
     * having to know about it.
     */
    private HighestOffsetAndIncompletes decodeBody(long baseOffset) throws CorruptOffsetMetadataException, IOException {
        return switch (encoding) {
            case BitSet -> deserialiseBitSetWrapToIncompletes(encoding, baseOffset, data);
            case BitSetCompressed -> deserialiseBitSetWrapToIncompletes(BitSet, baseOffset, decompressZstd(data));
            case RunLength -> runLengthDecodeToIncompletes(encoding, baseOffset, data);
            case RunLengthCompressed -> runLengthDecodeToIncompletes(RunLength, baseOffset, decompressZstd(data));
            case BitSetV2 -> deserialiseBitSetWrapToIncompletes(encoding, baseOffset, data);
            case BitSetV2Compressed -> deserialiseBitSetWrapToIncompletes(BitSetV2, baseOffset, decompressZstd(data));
            case RunLengthV2 -> runLengthDecodeToIncompletes(encoding, baseOffset, data);
            case RunLengthV2Compressed -> runLengthDecodeToIncompletes(RunLengthV2, baseOffset, decompressZstd(data));
            // Unreachable: decodeWithRider's own arm routes the envelope to the user's policy before this is called.
            // Kept as a CHECKED corruption rather than falling to the default's bare PCInternalRuntimeException so
            // that a future caller reaching it still leaves through the policy rather than past it.
            case RiderEnvelope -> throw new CorruptOffsetMetadataException(
                    "a rider envelope is not an inner encoding - it is unwrapped before a pair is built");
            default -> throw new PCInternalRuntimeException(
                    msg("no decoder for {}, and it was not routed to the policy handler", encoding));
        };
    }
}
