package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.StandardException;

/**
 * Thrown under situations where the {@link BitSetEncoder} would not be able to encode the given data.
 *
 * @author Antony Stubbs
 */
@StandardException
public class BitSetEncodingNotSupportedException extends EncodingNotSupportedException {
}
