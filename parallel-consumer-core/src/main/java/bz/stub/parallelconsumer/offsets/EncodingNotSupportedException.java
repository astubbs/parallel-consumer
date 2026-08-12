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
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class EncodingNotSupportedException extends InternalException {

    public EncodingNotSupportedException(String message) {
        super(message);
    }

    public EncodingNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

}
