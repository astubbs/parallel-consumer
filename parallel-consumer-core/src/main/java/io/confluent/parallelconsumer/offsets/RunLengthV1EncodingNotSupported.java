package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


/**
 * Thrown when Runlength V1 encoding is not supported.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class RunLengthV1EncodingNotSupported extends EncodingNotSupportedException {

    public RunLengthV1EncodingNotSupported(String message) {
        super(message);
    }

}
