package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


/**
 * Generic Parallel Consumer parent exception.
 *
 * @author Antony Stubbs
 * @see InternalRuntimeException RuntimeException version
 */
// Constructors are hand-written (not Lombok @StandardException) to avoid a flaky
// annotation-processing compile race - see InternalRuntimeException for the full explanation.
public class InternalException extends Exception {

    public InternalException() {
        super();
    }

    public InternalException(String message) {
        super(message);
    }

    public InternalException(String message, Throwable cause) {
        super(message, cause);
    }

    public InternalException(Throwable cause) {
        super(cause);
    }

}
