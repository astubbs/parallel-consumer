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
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class NoEncodingPossibleException extends InternalException {

    public NoEncodingPossibleException(String message) {
        super(message);
    }

}
