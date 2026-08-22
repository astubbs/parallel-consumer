package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.Report;

/**
 * The throwable form of a worker's failure report - the exception type the plan's U6 was asked to choose.
 * <p>
 * R5's failure reason is worker-supplied <em>text</em>, while core's failure history
 * ({@code WorkContainer#onUserFunctionFailure}) records a {@link Throwable}. This class is the bridge: the
 * reported text becomes this exception's message <b>verbatim</b> on the way in
 * ({@link RecordCodec#toFailureCause(Report.Failure)}), and {@link RecordCodec} unwraps it back to text on
 * redelivery. Sanitisation (length bound, control-character strip - the plan's U9) runs on the way <em>out</em>,
 * at serialization and logging, never here - storing the text verbatim means the sanitiser can evolve without
 * already-recorded history having been lossily filtered under an older rule.
 * <p>
 * The message may embed record payload and is untrusted input per R8: never write it to an ordinary
 * application log.
 * <p>
 * Carries no stack trace: the failure happened in a foreign process, so a Java stack captured here would
 * describe the proxy's report-handling path and read as a lie.
 *
 * @author Antony Stubbs
 */
public class WorkerReportedFailureException extends RuntimeException {

    public WorkerReportedFailureException(String workerSuppliedReason) {
        // no cause, no suppression, no writable stack trace - see class javadoc
        super(workerSuppliedReason, null, false, false);
    }
}
