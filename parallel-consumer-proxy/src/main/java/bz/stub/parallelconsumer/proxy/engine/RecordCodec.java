package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import bz.stub.parallelconsumer.proxy.protocol.WireTimestamps;
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProduceRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Record;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import bz.stub.parallelconsumer.state.WorkContainer;
import com.google.protobuf.ByteString;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Optional;

/**
 * Serializes a {@link WorkContainer}'s delivery state into the wire {@link DispatchRecord}, and converts a
 * worker's failure report back into the {@link Throwable} core's retry machinery records.
 * <p>
 * The PC-derived state (attempt count, last failure time and reason - R5) is read from the container's own
 * fields rather than re-derived, per the plan's U6. Two conversions are decided here:
 * <ul>
 *     <li><b>Failure reason, text&harr;throwable:</b> worker text is wrapped as
 *     {@link WorkerReportedFailureException} on the way in and unwrapped to text on redelivery. If the recorded
 *     reason is instead core's {@link ExceptionInUserFunctionException} wrapper (the engine's own dispatch
 *     function threw), it is unwrapped to its root cause first - the wrapper names the proxy's plumbing, not
 *     the failure. Sanitisation is U9's and runs on the way out, not here.</li>
 *     <li><b>Absent-on-first-delivery:</b> {@code last_failure_at} and {@code last_failure_reason} are simply
 *     not set before a first failure - field <em>presence</em> is the wire form of "has failed before", never a
 *     zero timestamp or empty string. Note core's quirk: {@code WorkContainer#getLastFailureReason()} returns a
 *     NULL {@link Optional} before the first failure (the field has no initializer), so it is normalised here.</li>
 * </ul>
 *
 * @author Antony Stubbs
 */
@UtilityClass
public class RecordCodec {

    /**
     * Separator inside a token's {@code record_id}. Safe because Kafka topic names are restricted to
     * {@code [a-zA-Z0-9._-]}, so the id parses unambiguously even though the client never parses it.
     */
    private static final String RECORD_ID_SEPARATOR = "/";

    /**
     * The stable identity of one consumed record, the first half of the fencing token. Opaque to the client,
     * which echoes it verbatim and must never derive meaning from it.
     */
    public static String recordIdOf(WorkContainer<byte[], byte[]> wc) {
        var cr = wc.getCr();
        return cr.topic() + RECORD_ID_SEPARATOR + cr.partition() + RECORD_ID_SEPARATOR + cr.offset();
    }

    /**
     * Serializes one delivery of a record.
     *
     * @param capturedEpoch the {@link WorkContainer#getDeliveryCount()} value <b>captured at dispatch</b> by
     *                      the caller, per KTD8 - never re-read here, so a caller cannot accidentally serialize
     *                      a fresher count than the one it registered
     */
    public static DispatchRecord toDispatchRecord(WorkContainer<byte[], byte[]> wc, long capturedEpoch) {
        var cr = wc.getCr();

        var record = Record.newBuilder()
                .setTopic(cr.topic())
                .setPartition(cr.partition())
                .setOffset(cr.offset());
        // Kafka distinguishes a null key/value (e.g. a tombstone) from an empty one, so the wire does too:
        // absent field = null, present-but-empty field = zero-length
        if (cr.key() != null) {
            record.setKey(ByteString.copyFrom(cr.key()));
        }
        if (cr.value() != null) {
            record.setValue(ByteString.copyFrom(cr.value()));
        }

        var dispatch = DispatchRecord.newBuilder()
                .setToken(Token.newBuilder()
                        .setRecordId(recordIdOf(wc))
                        // int64 on the wire, matching the long WorkContainer.getDeliveryCount() it carries -
                        // widened from the provisional schema's int32 at the freeze, while widening was free
                        .setEpoch(capturedEpoch))
                .setRecord(record)
                // 1 on first delivery, 2 on the first redelivery - product data, distinct from the fencing epoch,
                // which also counts verdict-free redeliveries that consumed no attempt
                .setAttempt(wc.getNumberOfFailedAttempts() + 1);

        wc.getLastFailedAt().ifPresent(at -> dispatch.setLastFailureAt(WireTimestamps.toWire(at)));
        lastFailureReasonText(wc).ifPresent(dispatch::setLastFailureReason);

        return dispatch.build();
    }

    /**
     * The throwable core's failure history records for a worker's failure report - the wrap half of the
     * text&harr;throwable bridge.
     */
    public static Throwable toFailureCause(Report.Failure failure) {
        return new WorkerReportedFailureException(failure.hasReason() ? failure.getReason() : "");
    }

    /**
     * A success report's produce payload entry as the record the engine's own producer sends (R6) - the only
     * sanctioned route for worker output (KTD7). The absent-versus-empty distinction is preserved in reverse:
     * an unset wire field produces a {@code null} key or value, never a zero-length one.
     */
    public static ProducerRecord<byte[], byte[]> toProducerRecord(ProduceRecord produceRecord) {
        return new ProducerRecord<>(produceRecord.getTopic(),
                produceRecord.hasKey() ? produceRecord.getKey().toByteArray() : null,
                produceRecord.hasValue() ? produceRecord.getValue().toByteArray() : null);
    }

    /**
     * The unwrap half: recovers the reason text a redelivery carries. Untrusted input per R8 - U9's sanitiser
     * is this value's next stop on any path that logs or re-serializes it.
     */
    static Optional<String> lastFailureReasonText(WorkContainer<byte[], byte[]> wc) {
        // normalise the null-before-first-failure quirk documented in the class javadoc
        return Optional.ofNullable(wc.getLastFailureReason())
                .flatMap(reason -> reason)
                .map(RecordCodec::unwrap)
                .map(cause -> cause.getMessage() != null ? cause.getMessage() : cause.toString());
    }

    private static Throwable unwrap(Throwable recorded) {
        Throwable cause = recorded;
        while (cause instanceof ExceptionInUserFunctionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
