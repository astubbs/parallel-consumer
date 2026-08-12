package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;

import static io.confluent.csid.utils.StringUtils.msg;

/**
 * Thrown when a commit's offset metadata carries an {@link OffsetEncoding} this build recognises, but has no decoder
 * for.
 * <p>
 * Replaces a bare {@link UnsupportedOperationException}, so that the failure is typed like the rest of the offsets
 * package and can be routed through
 * {@link io.confluent.parallelconsumer.ParallelConsumerOptions#getInvalidOffsetMetadataPolicy()} - under
 * {@link io.confluent.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy#IGNORE} the
 * metadata is discarded with a warning instead, and processing resumes from the committed offset.
 *
 * @author Antony Stubbs
 * @see UnknownOffsetMetadataMagicException for the sibling case where the magic byte matches no known encoding at all
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class UnsupportedOffsetEncodingException extends EncodingNotSupportedException {

    /**
     * The encoding this build cannot decode.
     */
    @Getter
    private final OffsetEncoding encoding;

    public UnsupportedOffsetEncodingException(OffsetEncoding encoding, String context) {
        super(msg("Offset encoding ({}) is known to this version of Parallel Consumer but cannot be decoded by it ({})." +
                        " Upgrade Parallel Consumer, or set ParallelConsumerOptions#invalidOffsetMetadataPolicy to IGNORE to" +
                        " discard the metadata and continue from the committed offset (replaying anything already completed" +
                        " but not yet committed).",
                encoding.description(), context));
        this.encoding = encoding;
    }

}
