package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */


/**
 * Thrown when Runlength V1 encoding is not supported.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class RunLengthV2EncodingNotSupported extends EncodingNotSupportedException {

    public RunLengthV2EncodingNotSupported(String message) {
        super(message);
    }

}
