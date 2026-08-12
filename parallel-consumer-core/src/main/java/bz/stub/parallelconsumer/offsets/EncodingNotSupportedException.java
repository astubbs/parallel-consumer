package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.InternalException;
import lombok.experimental.StandardException;

/**
 * Parent of the exceptions for when the {@link OffsetEncoder} cannot encode the given data.
 *
 * @author Antony Stubbs
 */
@StandardException
public class EncodingNotSupportedException extends InternalException {
}
