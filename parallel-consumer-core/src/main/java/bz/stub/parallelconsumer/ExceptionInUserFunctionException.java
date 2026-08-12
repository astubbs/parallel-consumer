package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.StandardException;

/**
 * This exception is only used when there is an exception thrown from code provided by the user.
 */
@StandardException
public class ExceptionInUserFunctionException extends ParallelConsumerException {
}
