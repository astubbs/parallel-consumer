package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


/**
 * Thrown under situations where the {@link BitSetEncoder} would not be able to encode the given data.
 *
 * @author Antony Stubbs
 */
// Constructors are hand-written (not Lombok @StandardException) to avoid a flaky
// annotation-processing compile race - see InternalRuntimeException for the full explanation.
public class BitSetEncodingNotSupportedException extends EncodingNotSupportedException {

    public BitSetEncodingNotSupportedException() {
        super();
    }

    public BitSetEncodingNotSupportedException(String message) {
        super(message);
    }

    public BitSetEncodingNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public BitSetEncodingNotSupportedException(Throwable cause) {
        super(cause);
    }

}
