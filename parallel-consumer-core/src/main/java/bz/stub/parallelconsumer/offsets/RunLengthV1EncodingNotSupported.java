package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.StandardException;

/**
 * Thrown when Runlength V1 encoding is not supported.
 *
 * @author Antony Stubbs
 */
@StandardException
public class RunLengthV1EncodingNotSupported extends EncodingNotSupportedException {
}
