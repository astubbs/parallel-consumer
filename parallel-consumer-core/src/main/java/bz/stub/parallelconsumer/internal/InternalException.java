package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */


/**
 * Generic Parallel Consumer parent exception.
 *
 * @author Antony Stubbs
 * @see InternalRuntimeException RuntimeException version
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class InternalException extends Exception {

    public InternalException(String message) {
        super(message);
    }

    public InternalException(String message, Throwable cause) {
        super(message, cause);
    }

}
