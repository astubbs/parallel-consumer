package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.StandardException;

/**
 * Generic Parallel Consumer {@link RuntimeException} parent.
 *
 * @author Antony Stubbs
 */
@StandardException
public class ParallelConsumerException extends RuntimeException {
}
