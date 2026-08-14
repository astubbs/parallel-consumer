package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProduceRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import bz.stub.parallelconsumer.proxy.protocol.v1.Record;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

/**
 * Serialization fidelity for the PROVISIONAL message set - see the banner in {@code proxy.proto}: this schema moves
 * until the language-proxy plan's freeze unit (astubbs#242), so these tests prove that the codegen wiring works and
 * that the wire preserves what travels over it, not that the protocol's semantics are right (the engine and spike
 * units own those).
 */
class ProxyProtocolRoundTripTest {

    /**
     * A round-trip of every provisional message preserves all fields - including the presence bits on optional
     * fields, which is why the assertions go through {@code has*()} as well as message equality.
     */
    @Test
    void everyProvisionalMessageRoundTripsAllFields() throws Exception {
        var configure = Configure.newBuilder()
                .addTopics("in-topic")
                .setMaxConcurrency(16)
                .build();
        assertThat(Configure.parseFrom(configure.toByteArray())).isEqualTo(configure);

        var configured = Configured.newBuilder()
                .addTopics("in-topic")
                .setMaxConcurrency(16)
                .build();
        assertThat(Configured.parseFrom(configured.toByteArray())).isEqualTo(configured);

        var dispatch = Dispatch.newBuilder()
                .setToken(Token.newBuilder().setRecordId("in-topic/0/42").setEpoch(2))
                .setRecord(Record.newBuilder()
                        .setTopic("in-topic")
                        .setPartition(0)
                        .setOffset(42)
                        .setKey(ByteString.copyFromUtf8("key"))
                        .setValue(ByteString.copyFromUtf8("hello")))
                .setAttempt(2)
                .setLastFailureAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).setNanos(1))
                .setLastFailureReason("previous failure text")
                .build();
        var dispatchBack = Dispatch.parseFrom(dispatch.toByteArray());
        assertThat(dispatchBack).isEqualTo(dispatch);
        assertThat(dispatchBack.hasLastFailureAt()).isTrue();
        assertThat(dispatchBack.hasLastFailureReason()).isTrue();
        assertThat(dispatchBack.getRecord().hasKey()).isTrue();
        assertThat(dispatchBack.getToken().getEpoch()).isEqualTo(2);

        var success = Report.newBuilder()
                .setToken(Token.newBuilder().setRecordId("in-topic/0/42").setEpoch(2))
                .setSuccess(Report.Success.newBuilder()
                        .addProduce(ProduceRecord.newBuilder()
                                .setTopic("out-topic")
                                .setValue(ByteString.copyFromUtf8("world"))))
                .build();
        assertThat(Report.parseFrom(success.toByteArray())).isEqualTo(success);

        var failure = Report.newBuilder()
                .setToken(Token.newBuilder().setRecordId("in-topic/0/42").setEpoch(2))
                .setFailure(Report.Failure.newBuilder().setReason("worker-supplied reason"))
                .build();
        assertThat(Report.parseFrom(failure.toByteArray())).isEqualTo(failure);

        var clientMessage = ClientMessage.newBuilder().setReport(success).build();
        assertThat(ClientMessage.parseFrom(clientMessage.toByteArray())).isEqualTo(clientMessage);

        var proxyMessage = ProxyMessage.newBuilder().setDispatch(dispatch).build();
        assertThat(ProxyMessage.parseFrom(proxyMessage.toByteArray())).isEqualTo(proxyMessage);
    }

    /**
     * Absence must survive the wire as absence: a first-delivery dispatch has no last-failure state, and a null Kafka
     * key/value (a tombstone) is not an empty one. Presence bits are the wire form of that distinction.
     */
    @Test
    void absentOptionalFieldsStayAbsent() throws Exception {
        var firstDelivery = Dispatch.newBuilder()
                .setToken(Token.newBuilder().setRecordId("in-topic/0/0").setEpoch(1))
                .setRecord(Record.newBuilder().setTopic("in-topic").setPartition(0).setOffset(0))
                .setAttempt(1)
                .build();
        var back = Dispatch.parseFrom(firstDelivery.toByteArray());
        assertThat(back.hasLastFailureAt()).isFalse();
        assertThat(back.hasLastFailureReason()).isFalse();
        assertThat(back.getRecord().hasKey()).isFalse();
        assertThat(back.getRecord().hasValue()).isFalse();
    }

    /**
     * A report carrying a field this schema does not know yet must parse, and re-serializing it must preserve the
     * unknown field byte-for-byte - that is what lets an older proxy coexist with a newer client during a protocol
     * revision, and proto3 only guarantees it when the runtime retains unknown fields (it has since protobuf 3.5).
     */
    @Test
    void unknownFutureFieldSurvivesReserialization() throws Exception {
        var report = Report.newBuilder()
                .setToken(Token.newBuilder().setRecordId("in-topic/0/7").setEpoch(1))
                .setSuccess(Report.Success.getDefaultInstance())
                .build();

        var futureField = UnknownFieldSet.newBuilder()
                .addField(999, UnknownFieldSet.Field.newBuilder().addVarint(42).build())
                .build();
        byte[] fromTheFuture = report.toBuilder().setUnknownFields(futureField).build().toByteArray();

        var parsed = Report.parseFrom(fromTheFuture);
        assertThat(parsed.getToken().getRecordId()).isEqualTo("in-topic/0/7");
        assertThat(parsed.getUnknownFields().hasField(999)).isTrue();
        assertThat(parsed.toByteArray()).isEqualTo(fromTheFuture);
    }

    /**
     * The gRPC stub generated alongside the messages compiles and names the service the .proto declares - proving the
     * protoc-gen-grpc-java half of the codegen wiring, not just the protobuf half.
     */
    @Test
    void grpcStubIsGeneratedForTheSessionStream() {
        assertThat(ProxyServiceGrpc.SERVICE_NAME).isEqualTo("parallelconsumer.proxy.v1.ProxyService");
        assertThat(ProxyServiceGrpc.getSessionMethod().getFullMethodName())
                .isEqualTo("parallelconsumer.proxy.v1.ProxyService/Session");
    }
}
