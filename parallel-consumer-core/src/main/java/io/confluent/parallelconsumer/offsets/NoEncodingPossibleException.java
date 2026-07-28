package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalException;

/**
 * Throw when for whatever reason, no encoding of the offsets is possible.
 *
 * @author Antony Stubbs
 */
// Constructors are hand-written (not Lombok @StandardException) to avoid a flaky
// annotation-processing compile race - see InternalRuntimeException for the full explanation.
public class NoEncodingPossibleException extends InternalException {

    public NoEncodingPossibleException() {
        super();
    }

    public NoEncodingPossibleException(String message) {
        super(message);
    }

    public NoEncodingPossibleException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoEncodingPossibleException(Throwable cause) {
        super(cause);
    }

}
