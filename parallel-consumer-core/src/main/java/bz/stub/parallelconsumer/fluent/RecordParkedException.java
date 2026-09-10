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
 * <b>It is a retriable exception on purpose</b>, and that is also what carries the park. The engine's only
 * hand-back path is to fail a record, so a park leaves the wrapper as a throw like any other; extending
 * {@link PCRetriableException} keeps it out of the error log, where it would read as a bug rather than as the
 * outcome the definition asked for, and lets it say {@link PCRetriableException#park(String)} - which is what the
 * engine reads on the failure path to mark the record never due and record why (KTD14).
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
