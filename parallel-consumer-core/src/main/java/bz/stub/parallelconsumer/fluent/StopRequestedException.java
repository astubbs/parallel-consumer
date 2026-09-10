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
 * failure to report. It carries the far-future delay so a drain does not re-invoke the record in the window before
 * the instance closes.
 */
@InterfaceStability.Unstable
public class StopRequestedException extends PCRetriableException {

    private static final long serialVersionUID = 1L;

    StopRequestedException(String message) {
        super(message);
    }
}
