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
public class RunLengthV2EncodingNotSupported extends EncodingNotSupportedException {

    public RunLengthV2EncodingNotSupported() {
        super();
    }

    public RunLengthV2EncodingNotSupported(String message) {
        super(message);
    }

    public RunLengthV2EncodingNotSupported(String message, Throwable cause) {
        super(message, cause);
    }

    public RunLengthV2EncodingNotSupported(Throwable cause) {
        super(cause);
    }

}
