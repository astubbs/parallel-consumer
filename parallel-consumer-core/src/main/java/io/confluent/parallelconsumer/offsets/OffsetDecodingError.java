package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.InternalException;

/*-
 * Error decoding offsets
 *
 * TODO should extend java.lang.Error ?
 *
 * @author Antony Stubbs
 */
// Constructors are hand-written (not Lombok @StandardException) to avoid a flaky
// annotation-processing compile race - see InternalRuntimeException for the full explanation.
public class OffsetDecodingError extends InternalException {

    public OffsetDecodingError() {
        super();
    }

    public OffsetDecodingError(String message) {
        super(message);
    }

    public OffsetDecodingError(String message, Throwable cause) {
        super(message, cause);
    }

    public OffsetDecodingError(Throwable cause) {
        super(cause);
    }

}
