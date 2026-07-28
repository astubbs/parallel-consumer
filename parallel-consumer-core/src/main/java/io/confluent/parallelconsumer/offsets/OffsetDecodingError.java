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
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class OffsetDecodingError extends InternalException {

    public OffsetDecodingError(String message, Throwable cause) {
        super(message, cause);
    }

}
