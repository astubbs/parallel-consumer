package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.InternalException;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Thrown when a commit's offset metadata names an {@link OffsetEncoding} this build can decode, but the bytes that
 * follow do not describe a payload that encoding could ever have produced.
 * <p>
 * <b>Why this is not merely defensive.</b> The decoders trusted their own header. A length field is read straight out
 * of the payload and then used to drive a loop, so metadata PC did not write - or wrote and something truncated -
 * produced answers rather than errors:
 * <ul>
 *     <li>a {@code BitSet} declaring 32767 bits with no bytes behind it returned <b>32767 fabricated incomplete
 *     offsets</b>, every one of which would then be re-delivered;</li>
 *     <li>a negative bitset length returned a highest-seen offset <em>below</em> the committed one;</li>
 *     <li>a run length of {@link Integer#MAX_VALUE} moved the highest-seen offset roughly two billion forward, which
 *     marks that whole range as already succeeded - so
 *     {@link bz.stub.parallelconsumer.state.PartitionState#isRecordPreviouslyCompleted} skips it and the records are
 *     silently never processed.</li>
 * </ul>
 * A wrong answer is worse than a failure here, because nothing downstream can tell it apart from a real offset map.
 * Detecting it turns it into the case
 * {@link bz.stub.parallelconsumer.ParallelConsumerOptions#getInvalidOffsetMetadataPolicy()} already governs: under
 * {@link bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy#IGNORE} the metadata is
 * discarded with a warning and processing resumes from the committed offset.
 *
 * @author Antony Stubbs
 * @see UnknownOffsetMetadataMagicException the magic byte matches no known encoding
 * @see UnsupportedOffsetEncodingException the encoding is known but has no decoder here
 */
// Hand-written ctors (not Lombok @StandardException) - see PCInternalRuntimeException for why.
public class CorruptOffsetMetadataException extends InternalException {

    public CorruptOffsetMetadataException(String problem, String context) {
        super(msg("Offset metadata is not a payload this build could have written: {} ({})." +
                        " The metadata field of a committed offset is free-form, so this is most likely not ours - set" +
                        " ParallelConsumerOptions#invalidOffsetMetadataPolicy to IGNORE to discard it and continue from the" +
                        " committed offset (replaying anything already completed but not yet committed).",
                problem, context));
    }

    public CorruptOffsetMetadataException(String problem) {
        this(problem, "source unknown");
    }

}
