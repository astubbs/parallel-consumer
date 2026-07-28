package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


/**
 * Thrown under situations where the {@link BitSetEncoder} would not be able to encode the given data.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class BitSetEncodingNotSupportedException extends EncodingNotSupportedException {

    public BitSetEncodingNotSupportedException(String message) {
        super(message);
    }

    public BitSetEncodingNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

}
