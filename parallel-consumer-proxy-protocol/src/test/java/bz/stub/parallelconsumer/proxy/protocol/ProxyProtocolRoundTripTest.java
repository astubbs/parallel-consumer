package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Drop;
import bz.stub.parallelconsumer.proxy.protocol.v1.Heartbeat;
import bz.stub.parallelconsumer.proxy.protocol.v1.Manifest;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProduceRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import bz.stub.parallelconsumer.proxy.protocol.v1.Record;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.SetExecutorCount;
import bz.stub.parallelconsumer.proxy.protocol.v1.Shutdown;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import bz.stub.parallelconsumer.proxy.protocol.v1.WorkerDied;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Serialization fidelity for the FROZEN message set - every completed message type round-trips with all its
 * fields, including the presence bits on optional fields. This proves the codegen wiring and the wire's
 * fidelity, not the protocol's semantics (the engine units own those); the cross-language byte contract is
 * pinned separately by {@link GoldenSessionBytesTest}.
 */
class ProxyProtocolRoundTripTest {

    /** Deliberately beyond int32: the epoch is int64 on the wire because its source is a Java long. */
    private static final long BEYOND_INT32_EPOCH = 5_000_000_000L;

    private static final Token TOKEN = Token.newBuilder()
            .setRecordId("in-topic/0/42")
            .setEpoch(BEYOND_INT32_EPOCH)
            .build();

    /**
     * A round-trip of every completed message preserves all fields - including the presence bits on optional
     * fields, which is why the assertions go through {@code has*()} as well as message equality.
     */
    @Test
    void everyFrozenMessageRoundTripsAllFields() throws Exception {
        var configure = Configure.newBuilder()
                .addTopics("in-topic")
                .setMaxConcurrency(16)
                .putKafkaProperties("bootstrap.servers", "localhost:9092")
                .addCapabilities("dispatch")
                .setLaunchToken("per-launch-secret")
                .setTerminalTopic("in-topic.terminal")
                .setLeaseDuration(Duration.newBuilder().setSeconds(60))
                .setHeartbeatInterval(Duration.newBuilder().setSeconds(5))
                .setReconnectWindow(Duration.newBuilder().setSeconds(30))
                .setMessageBufferSize(500)
                .setInitialLoadFactor(2)
                .setMaximumLoadFactor(100)
                .setPcInstanceTag("proxy-under-test")
                .build();
        assertThat(Configure.parseFrom(configure.toByteArray())).isEqualTo(configure);

        var configured = Configured.newBuilder()
                .addTopics("in-topic")
                .setMaxConcurrency(16)
                .setExecutorCount(16)
                .setTerminalTopic("in-topic.terminal")
                .setLeaseDuration(Duration.newBuilder().setSeconds(60))
                .setHeartbeatInterval(Duration.newBuilder().setSeconds(5))
                .setReconnectWindow(Duration.newBuilder().setSeconds(30))
                .setMessageBufferSize(500)
                .setInitialLoadFactor(2)
                .setMaximumLoadFactor(100)
                .setPcInstanceTag("proxy-under-test")
                .build();
        assertThat(Configured.parseFrom(configured.toByteArray())).isEqualTo(configured);

        var dispatch = Dispatch.newBuilder()
                .addRecords(DispatchRecord.newBuilder()
                        .setToken(TOKEN)
                        .setRecord(Record.newBuilder()
                                .setTopic("in-topic")
                                .setPartition(0)
                                .setOffset(42)
                                .setKey(ByteString.copyFromUtf8("key"))
                                .setValue(ByteString.copyFromUtf8("hello")))
                        .setAttempt(2)
                        .setLastFailureAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).setNanos(1))
                        .setLastFailureReason("previous failure text"))
                .build();
        var dispatchBack = Dispatch.parseFrom(dispatch.toByteArray());
        assertThat(dispatchBack).isEqualTo(dispatch);
        var recordBack = dispatchBack.getRecords(0);
        assertThat(recordBack.hasLastFailureAt()).isTrue();
        assertThat(recordBack.hasLastFailureReason()).isTrue();
        assertThat(recordBack.getRecord().hasKey()).isTrue();
        assertWithMessage("the epoch is int64 - a value beyond int32 must survive the wire")
                .that(recordBack.getToken().getEpoch()).isEqualTo(BEYOND_INT32_EPOCH);

        var success = Report.newBuilder()
                .setToken(TOKEN)
                .setSuccess(Report.Success.newBuilder()
                        .addProduce(ProduceRecord.newBuilder()
                                .setTopic("out-topic")
                                .setValue(ByteString.copyFromUtf8("world"))))
                .build();
        assertThat(Report.parseFrom(success.toByteArray())).isEqualTo(success);

        var failure = Report.newBuilder()
                .setToken(TOKEN)
                .setFailure(Report.Failure.newBuilder().setReason("worker-supplied reason"))
                .build();
        assertThat(Report.parseFrom(failure.toByteArray())).isEqualTo(failure);

        var terminal = Report.newBuilder()
                .setToken(TOKEN)
                .setTerminal(Report.Terminal.newBuilder().setReason("poison pill"))
                .build();
        var terminalBack = Report.parseFrom(terminal.toByteArray());
        assertThat(terminalBack).isEqualTo(terminal);
        assertThat(terminalBack.getOutcomeCase()).isEqualTo(Report.OutcomeCase.TERMINAL);

        var released = Report.newBuilder()
                .setToken(TOKEN)
                .setReleased(Report.Released.getDefaultInstance())
                .build();
        var releasedBack = Report.parseFrom(released.toByteArray());
        assertThat(releasedBack).isEqualTo(released);
        assertThat(releasedBack.getOutcomeCase()).isEqualTo(Report.OutcomeCase.RELEASED);

        var manifest = Manifest.newBuilder().addTokens(TOKEN).build();
        assertThat(Manifest.parseFrom(manifest.toByteArray())).isEqualTo(manifest);

        var workerDied = WorkerDied.newBuilder().addTokens(TOKEN).build();
        assertThat(WorkerDied.parseFrom(workerDied.toByteArray())).isEqualTo(workerDied);

        var drop = Drop.newBuilder().setToken(TOKEN).build();
        assertThat(Drop.parseFrom(drop.toByteArray())).isEqualTo(drop);

        var setExecutorCount = SetExecutorCount.newBuilder().setExecutorCount(8).build();
        var setExecutorCountBack = SetExecutorCount.parseFrom(setExecutorCount.toByteArray());
        assertThat(setExecutorCountBack).isEqualTo(setExecutorCount);
        assertThat(setExecutorCountBack.hasExecutorCount()).isTrue();

        // The envelopes must preserve WHICH message travelled, not just its bytes: session logic switches on
        // getMessageCase(), so every oneof case is wrapped and its case asserted - equality alone would pass a
        // field-renumbering that swapped the cases, because both sides of the comparison would be equally wrong.
        assertClientEnvelope(ClientMessage.newBuilder().setConfigure(configure).build(),
                ClientMessage.MessageCase.CONFIGURE);
        assertClientEnvelope(ClientMessage.newBuilder().setReport(success).build(),
                ClientMessage.MessageCase.REPORT);
        assertClientEnvelope(ClientMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build(),
                ClientMessage.MessageCase.HEARTBEAT);
        assertClientEnvelope(ClientMessage.newBuilder().setManifest(manifest).build(),
                ClientMessage.MessageCase.MANIFEST);
        assertClientEnvelope(ClientMessage.newBuilder().setWorkerDied(workerDied).build(),
                ClientMessage.MessageCase.WORKER_DIED);

        assertProxyEnvelope(ProxyMessage.newBuilder().setConfigured(configured).build(),
                ProxyMessage.MessageCase.CONFIGURED);
        assertProxyEnvelope(ProxyMessage.newBuilder().setDispatch(dispatch).build(),
                ProxyMessage.MessageCase.DISPATCH);
        assertProxyEnvelope(ProxyMessage.newBuilder().setDrop(drop).build(),
                ProxyMessage.MessageCase.DROP);
        assertProxyEnvelope(ProxyMessage.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build(),
                ProxyMessage.MessageCase.SHUTDOWN);
        assertProxyEnvelope(ProxyMessage.newBuilder().setSetExecutorCount(setExecutorCount).build(),
                ProxyMessage.MessageCase.SET_EXECUTOR_COUNT);
    }

    private static void assertClientEnvelope(ClientMessage message, ClientMessage.MessageCase expectedCase)
            throws Exception {
        var back = ClientMessage.parseFrom(message.toByteArray());
        assertThat(back).isEqualTo(message);
        assertThat(back.getMessageCase()).isEqualTo(expectedCase);
    }

    private static void assertProxyEnvelope(ProxyMessage message, ProxyMessage.MessageCase expectedCase)
            throws Exception {
        var back = ProxyMessage.parseFrom(message.toByteArray());
        assertThat(back).isEqualTo(message);
        assertThat(back.getMessageCase()).isEqualTo(expectedCase);
    }

    /**
     * Absence must survive the wire as absence: a first-delivery dispatch has no last-failure state, and a null Kafka
     * key/value (a tombstone) is not an empty one. Presence bits are the wire form of that distinction - and an
     * unset-but-scalar field (max_concurrency, executor_count) must be distinguishable from a zero one, which is why
     * every scalar in the frozen schema carries explicit presence.
     */
    @Test
    void absentOptionalFieldsStayAbsent() throws Exception {
        var firstDelivery = Dispatch.newBuilder()
                .addRecords(DispatchRecord.newBuilder()
                        .setToken(Token.newBuilder().setRecordId("in-topic/0/0").setEpoch(1))
                        .setRecord(Record.newBuilder().setTopic("in-topic").setPartition(0).setOffset(0))
                        .setAttempt(1))
                .build();
        var back = Dispatch.parseFrom(firstDelivery.toByteArray()).getRecords(0);
        assertThat(back.hasLastFailureAt()).isFalse();
        assertThat(back.hasLastFailureReason()).isFalse();
        assertThat(back.getRecord().hasKey()).isFalse();
        assertThat(back.getRecord().hasValue()).isFalse();

        var bareConfigured = Configured.parseFrom(Configured.newBuilder().addTopics("t").build().toByteArray());
        assertThat(bareConfigured.hasMaxConcurrency()).isFalse();
        assertThat(bareConfigured.hasExecutorCount()).isFalse();
        assertThat(bareConfigured.hasTerminalTopic()).isFalse();
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
