package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


/**
 * Generic Parallel Consumer {@link RuntimeException} parent.
 *
 * @author Antony Stubbs
 */
// Constructors are hand-written (not Lombok @StandardException) to avoid a flaky
// annotation-processing compile race - see InternalRuntimeException for the full explanation.
public class ParallelConsumerException extends RuntimeException {

    public ParallelConsumerException() {
        super();
    }

    public ParallelConsumerException(String message) {
        super(message);
    }

    public ParallelConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParallelConsumerException(Throwable cause) {
        super(cause);
    }

}
