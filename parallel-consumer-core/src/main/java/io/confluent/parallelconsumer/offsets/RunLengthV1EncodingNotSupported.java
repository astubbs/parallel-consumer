package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


/**
 * Thrown when Runlength V1 encoding is not supported.
 *
 * @author Antony Stubbs
 */
// Constructors are hand-written (not Lombok @StandardException) to avoid a flaky
// annotation-processing compile race - see InternalRuntimeException for the full explanation.
public class RunLengthV1EncodingNotSupported extends EncodingNotSupportedException {

    public RunLengthV1EncodingNotSupported() {
        super();
    }

    public RunLengthV1EncodingNotSupported(String message) {
        super(message);
    }

    public RunLengthV1EncodingNotSupported(String message, Throwable cause) {
        super(message, cause);
    }

    public RunLengthV1EncodingNotSupported(Throwable cause) {
        super(cause);
    }

}
