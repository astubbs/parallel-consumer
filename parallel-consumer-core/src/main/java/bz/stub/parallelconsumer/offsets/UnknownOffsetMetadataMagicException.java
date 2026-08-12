package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;

import static io.confluent.csid.utils.StringUtils.msg;

/**
 * Thrown when the leading magic byte of a commit's offset metadata matches no {@link OffsetEncoding} known to this
 * build.
 * <p>
 * The expected cause is <b>forward incompatibility</b>: the metadata was written by a <i>newer</i> version of Parallel
 * Consumer, using an encoding that did not exist when this version was built. It can also mean the consumer group
 * carries metadata from an unrelated application, or that the payload is corrupt.
 * <p>
 * Whether this is thrown at all is governed by
 * {@link io.confluent.parallelconsumer.ParallelConsumerOptions#getInvalidOffsetMetadataPolicy()} - under
 * {@link io.confluent.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy#IGNORE} the
 * unreadable metadata is discarded with a warning instead, and processing resumes from the committed offset.
 *
 * @author Antony Stubbs
 * @see UnsupportedOffsetEncodingException for the sibling case where the encoding IS known, but this build has no
 *         decoder for it
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class UnknownOffsetMetadataMagicException extends EncodingNotSupportedException {

    private static final String ADVICE = "This metadata was most likely written by a NEWER version of Parallel " +
            "Consumer using an encoding this version does not know, or the consumer group is being shared with " +
            "another application. Upgrade Parallel Consumer, use a consumer group unique to this application, or set " +
            "ParallelConsumerOptions#invalidOffsetMetadataPolicy to IGNORE to discard the metadata and continue from " +
            "the committed offset (replaying anything already completed but not yet committed).";

    /**
     * The magic byte that could not be resolved to an {@link OffsetEncoding}.
     */
    @Getter
    private final byte magicByte;

    public UnknownOffsetMetadataMagicException(byte magicByte) {
        this(magicByte, "source unknown");
    }

    /**
     * @param context where the payload came from, for diagnosis - see
     *                {@link EncodedOffsetPair#describeSource(org.apache.kafka.common.TopicPartition, long)}
     */
    public UnknownOffsetMetadataMagicException(byte magicByte, String context) {
        super(msg("Unrecognised offset metadata magic byte: {} ({}). {}", magicByte, context, ADVICE));
        this.magicByte = magicByte;
    }

}
