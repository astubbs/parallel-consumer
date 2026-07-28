package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


/**
 * This exception is only used when there is an exception thrown from code provided by the user.
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class ExceptionInUserFunctionException extends ParallelConsumerException {

    public ExceptionInUserFunctionException(String message, Throwable cause) {
        super(message, cause);
    }

}
