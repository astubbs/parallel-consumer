package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


/**
 * This exception is only used when there is an exception thrown from code provided by the user.
 */
// Constructors are hand-written (not Lombok @StandardException) to avoid a flaky
// annotation-processing compile race - see InternalRuntimeException for the full explanation.
public class ExceptionInUserFunctionException extends ParallelConsumerException {

    public ExceptionInUserFunctionException() {
        super();
    }

    public ExceptionInUserFunctionException(String message) {
        super(message);
    }

    public ExceptionInUserFunctionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExceptionInUserFunctionException(Throwable cause) {
        super(cause);
    }

}
