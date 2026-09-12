package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The throw that hands a stopping record back, leaving it incomplete so a restart delivers it again (R24, KTD6).
 * <p>
 * Retriable, for the same reason {@link RecordParkedException} is: it is the facade asking for a record back, not a
 * failure to report. It carries {@link bz.stub.parallelconsumer.PCRetriableException#park(String)}, so the record
 * is held where it is rather than re-invoked by a drain, and an operator asking why the instance stopped finds it in
 * the parked view with the stop as its reason. It also carries
 * {@link bz.stub.parallelconsumer.PCRetriableException#notAnAttempt()}, because the record reported rather than
 * failed. The parked <em>outcome counter</em> is deliberately not moved: a stop is a request about the instance, not
 * a terminal outcome of the record (R24, R7).
 */
@InterfaceStability.Unstable
public class StopRequestedException extends PCRetriableException {

    /**
     * Fixed rather than computed, for the same reason {@link RecordParkedException} pins its own: a {@link Throwable}
     * is serialisable whether or not anything here intends to serialise one.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Package-private: a stop is raised only by the dispatch wrapper, so the wording is always the facade's own.
     * There is no cause-taking form on purpose - nothing failed, the route asked.
     */
    StopRequestedException(String message) {
        super(message);
    }
}
