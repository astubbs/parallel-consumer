package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.StandardException;

/**
 * Generic Parallel Consumer parent exception.
 *
 * @author Antony Stubbs
 * @see InternalRuntimeException RuntimeException version
 */
@StandardException
public class InternalException extends Exception {
}
