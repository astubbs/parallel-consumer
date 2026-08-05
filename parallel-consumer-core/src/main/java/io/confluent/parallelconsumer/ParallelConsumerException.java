package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */


/**
 * Generic Parallel Consumer {@link RuntimeException} parent.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class ParallelConsumerException extends RuntimeException {

    public ParallelConsumerException(String message) {
        super(message);
    }

    public ParallelConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

}
