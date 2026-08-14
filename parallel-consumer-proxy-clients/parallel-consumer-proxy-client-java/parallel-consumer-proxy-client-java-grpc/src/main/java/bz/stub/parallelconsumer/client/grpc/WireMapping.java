package bz.stub.parallelconsumer.client.grpc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.InboundRecord;
import bz.stub.parallelconsumer.client.Outcome;
import bz.stub.parallelconsumer.client.OutboundRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProcessingOrder;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProduceRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import com.google.protobuf.ByteString;

import java.time.Instant;

/**
 * The wire boundary of the gRPC transport: API types in, protocol messages out, and back. Everything
 * protobuf-shaped in this module funnels through here, so the client classes above it read in API terms - and
 * so the mapping is unit-testable without a connection.
 * <p>
 * <b>The token is echoed verbatim (KTD8):</b> {@link #toReport} sets the report's token to the very message
 * object the dispatch carried - no rebuild, no field access, no interpretation - and the client stores nothing
 * about the record anywhere. A stateless client cannot have a state bug.
 *
 * @author Antony Stubbs
 */
final class WireMapping {

    private WireMapping() {
    }

    /** The connect-time {@code Configure}: the API's options carried to the sidecar, unmodified (KTD5, R39). */
    static Configure toConfigure(ClientOptions options) {
        var configure = Configure.newBuilder()
                .addAllTopics(options.topics())
                .putAllKafkaProperties(options.kafkaProperties());
        options.maxConcurrency().ifPresent(configure::setMaxConcurrency);
        options.ordering().ifPresent(ordering -> configure.setOrdering(toWireOrdering(ordering)));
        options.commitInterval().ifPresent(interval -> configure.setCommitInterval(toWireDuration(interval)));
        options.defaultMessageRetryDelay().ifPresent(delay ->
                configure.setDefaultMessageRetryDelay(toWireDuration(delay)));
        return configure.build();
    }

    /**
     * One dispatched record as the processor sees it. Absent wire fields map to the API's own absences: a
     * missing key or value is {@code null} (Kafka's tombstone distinction), missing failure state means "has
     * not failed" (R5's absence-is-the-form rule).
     */
    static InboundRecord toInboundRecord(Dispatch dispatch) {
        var record = dispatch.getRecord();
        return new InboundRecord(
                record.getTopic(),
                record.getPartition(),
                record.getOffset(),
                record.hasKey() ? record.getKey().toByteArray() : null,
                record.hasValue() ? record.getValue().toByteArray() : null,
                dispatch.hasAttempt() ? dispatch.getAttempt() : 1,
                dispatch.hasLastFailureAt()
                        ? Instant.ofEpochSecond(dispatch.getLastFailureAt().getSeconds(),
                        dispatch.getLastFailureAt().getNanos())
                        : null,
                dispatch.hasLastFailureReason() ? dispatch.getLastFailureReason() : null);
    }

    /**
     * The processor's outcome as the wire report, keyed by the dispatch's token <b>echoed verbatim</b> - the
     * same message object, byte-identical on the wire (KTD8).
     */
    static Report toReport(Token token, Outcome outcome) {
        var report = Report.newBuilder().setToken(token);
        if (outcome.isSuccess()) {
            var success = Report.Success.newBuilder();
            for (OutboundRecord outbound : outcome.produce()) {
                success.addProduce(toProduceRecord(outbound));
            }
            report.setSuccess(success);
        } else {
            var failure = Report.Failure.newBuilder();
            outcome.failureReason().ifPresent(failure::setReason);
            report.setFailure(failure);
        }
        return report.build();
    }

    private static ProduceRecord toProduceRecord(OutboundRecord outbound) {
        var produce = ProduceRecord.newBuilder().setTopic(outbound.topic());
        if (outbound.key() != null) {
            produce.setKey(ByteString.copyFrom(outbound.key()));
        }
        if (outbound.value() != null) {
            produce.setValue(ByteString.copyFrom(outbound.value()));
        }
        return produce.build();
    }

    private static ProcessingOrder toWireOrdering(bz.stub.parallelconsumer.client.ProcessingOrder ordering) {
        switch (ordering) {
            case UNORDERED:
                return ProcessingOrder.PROCESSING_ORDER_UNORDERED;
            case PARTITION:
                return ProcessingOrder.PROCESSING_ORDER_PARTITION;
            case KEY:
            default:
                return ProcessingOrder.PROCESSING_ORDER_KEY;
        }
    }

    private static com.google.protobuf.Duration toWireDuration(java.time.Duration duration) {
        // built by hand rather than with protobuf-java-util's Durations, which is not on this module's classpath
        return com.google.protobuf.Duration.newBuilder()
                .setSeconds(duration.getSeconds())
                .setNanos(duration.getNano())
                .build();
    }
}
