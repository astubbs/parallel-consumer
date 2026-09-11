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

    /**
     * Fixed rather than computed. This is the engine's hand-back vehicle and is never deliberately serialised, but a
     * {@link Throwable} is serialisable whether or not that was wanted, so the identity is pinned here.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Package-private: only the facade parks a record. The cause is the failure that exhausted the record's attempts
     * or the decode failure that can never pass, and {@link ParkedRecord#failure()} reads it back out from here.
     */
    RecordParkedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * For a park with no failure underneath it: a function that returned {@link Outcome#park(String)} because it
     * judged the record hopeless, rather than one that ran out of attempts. {@link ParkedRecord#failure()} is then
     * null, which is how a reader tells the two apart.
     */
    RecordParkedException(String message) {
        super(message);
    }
}
