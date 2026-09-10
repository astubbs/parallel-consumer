package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The throw that parks a record: it is out of attempts (R10), its payload will never decode (R12), or its function
 * declared it hopeless (R8).
 * <p>
 * <b>It is a retriable exception on purpose.</b> The engine's only hand-back path is to fail a record, so a park
 * leaves the wrapper as a throw like any other; extending the engine's retriable exception is what keeps it out of
 * the error log, where it would read as a bug rather than as the outcome the definition asked for (KTD4). What
 * actually parks the record is not this exception but the far-future delay the retry-delay provider answers with,
 * which the wrapper recorded as its intent immediately before throwing.
 *
 * @see RetryIntents
 */
@InterfaceStability.Unstable
public class RecordParkedException extends PCRetriableException {

    private static final long serialVersionUID = 1L;

    RecordParkedException(String message, Throwable cause) {
        super(message, cause);
    }

    RecordParkedException(String message) {
        super(message);
    }
}
