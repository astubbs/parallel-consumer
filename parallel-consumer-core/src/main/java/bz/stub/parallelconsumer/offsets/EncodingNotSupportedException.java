package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.InternalException;

/**
 * Parent of the exceptions for when the {@link OffsetEncoder} cannot encode the given data, and of those for when the
 * decode side cannot read a given payload - see {@link UnknownOffsetMetadataMagicException},
 * {@link UnsupportedOffsetEncodingException} and {@link KafkaStreamsEncodingNotSupported}.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see PCInternalRuntimeException for why.
public class EncodingNotSupportedException extends InternalException {

    /**
     * The escape hatch every "this build cannot read this payload" message ends with, held once because it was
     * written three times.
     * <p>
     * It names a public option and promises a specific behaviour, so three copies are three things that have to be
     * edited together when either changes - and nothing would go red if one were missed. The duplicate-code check on
     * astubbs/parallel-consumer#207 reported the three exception classes as near-identical; most of that was Java
     * exception boilerplate, but this sentence was genuinely copy-pasted, and it is the half worth removing.
     * <p>
     * Package-private so {@link CorruptOffsetMetadataException} can use it too. That class extends
     * {@link InternalException} rather than this one - a corrupt body is not an unsupported <em>encoding</em> - but it
     * offers the user the same escape hatch, and a second copy for the sake of the hierarchy would defeat the point.
     */
    static final String IGNORE_POLICY_ADVICE = "set ParallelConsumerOptions#invalidOffsetMetadataPolicy to IGNORE to"
            + " discard the metadata and continue from the committed offset (replaying anything already completed but"
            + " not yet committed).";

    public EncodingNotSupportedException(String message) {
        super(message);
    }

    public EncodingNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

}
