package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalException;

/**
 * Parent of the exceptions for when the {@link OffsetEncoder} cannot encode the given data.
 *
 * @author Antony Stubbs
 */
// Constructors are hand-written (not Lombok @StandardException) to avoid a flaky
// annotation-processing compile race - see InternalRuntimeException for the full explanation.
public class EncodingNotSupportedException extends InternalException {

    public EncodingNotSupportedException() {
        super();
    }

    public EncodingNotSupportedException(String message) {
        super(message);
    }

    public EncodingNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public EncodingNotSupportedException(Throwable cause) {
        super(cause);
    }

}
