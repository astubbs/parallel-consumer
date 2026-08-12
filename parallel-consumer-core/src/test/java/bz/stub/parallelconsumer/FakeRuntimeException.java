package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.StandardException;

/**
 * Used for testing error handling - easier to identify than a plain exception.
 *
 * @author Antony Stubbs
 */
@StandardException
public class FakeRuntimeException extends PCRetriableException {
}
