package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.CommitMode;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Drop;
import bz.stub.parallelconsumer.proxy.protocol.v1.Heartbeat;
import bz.stub.parallelconsumer.proxy.protocol.v1.InvalidOffsetMetadataPolicy;
import bz.stub.parallelconsumer.proxy.protocol.v1.Manifest;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProcessingOrder;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProduceRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Record;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.SetExecutorCount;
import bz.stub.parallelconsumer.proxy.protocol.v1.Shutdown;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import bz.stub.parallelconsumer.proxy.protocol.v1.WorkerDied;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * The canonical golden session: one scripted exchange, message by message, covering every message type in the
 * frozen schema and every field family (presence, absence, a beyond-int32 epoch, a tombstone value, a map
 * entry). Its serialized form is committed as two test resources - {@code golden-client-messages.bin} and
 * {@code golden-proxy-messages.bin}, each a stream of standard length-delimited (varint-prefixed) messages -
 * and every language's client is expected to parse those bytes to exactly these values. That is the
 * cross-language contract the same-runtime round-trip suite cannot give: a Java-only round trip proves Java
 * agrees with Java, while these bytes are the fixed point ten independently generated parsers must meet.
 * <p>
 * <b>The bytes are frozen with the schema.</b> {@link GoldenSessionBytesTest} fails on any drift between this
 * fixture and the committed resources. A legitimate protocol ADDITION extends this fixture and regenerates the
 * resources (run {@link #main} with the resources directory) in the same change, with the specification and
 * capability entry that addition requires; anything else that moves these bytes is a wire break.
 * <p>
 * Determinism notes, load-bearing: {@code kafka_properties} carries exactly ONE entry, because protobuf map
 * serialization order is unspecified across entries; and every field is set through the generated builders, so
 * serialization order is the field-number order the spec documents.
 */
final class GoldenSessionFixture {

    static final String CLIENT_RESOURCE = "golden-client-messages.bin";
    static final String PROXY_RESOURCE = "golden-proxy-messages.bin";

    /** Beyond int32 deliberately: a parser that truncates the epoch to 32 bits fails on the golden bytes. */
    static final long BEYOND_INT32_EPOCH = 5_000_000_000L;

    private static final Token TOKEN_CURRENT = token("demo-topic/0/0", 1);
    private static final Token TOKEN_REDELIVERED = token("demo-topic/0/1", BEYOND_INT32_EPOCH);
    private static final Token TOKEN_HELD_BY_DEAD_WORKER = token("demo-topic/0/2", 2);

    private GoldenSessionFixture() {
    }

    /** The client's half of the golden session, in send order. */
    static List<ClientMessage> clientMessages() {
        return List.of(
                ClientMessage.newBuilder().setConfigure(configure()).build(),
                ClientMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build(),
                ClientMessage.newBuilder().setReport(successReport()).build(),
                ClientMessage.newBuilder().setReport(failureReport()).build(),
                ClientMessage.newBuilder().setReport(terminalReport()).build(),
                ClientMessage.newBuilder().setWorkerDied(
                        WorkerDied.newBuilder().addTokens(TOKEN_HELD_BY_DEAD_WORKER).build()).build(),
                ClientMessage.newBuilder().setManifest(
                        Manifest.newBuilder().addTokens(TOKEN_CURRENT).addTokens(TOKEN_REDELIVERED).build()).build(),
                ClientMessage.newBuilder().setReport(releasedReport()).build());
    }

    /** The proxy's half of the golden session, in send order. */
    static List<ProxyMessage> proxyMessages() {
        return List.of(
                ProxyMessage.newBuilder().setConfigured(configured()).build(),
                ProxyMessage.newBuilder().setDispatch(dispatchWave()).build(),
                ProxyMessage.newBuilder().setDrop(Drop.newBuilder().setToken(TOKEN_REDELIVERED).build()).build(),
                ProxyMessage.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build(),
                // never sent by a v1 proxy (declared unused, KTD38) - in the parse corpus so every language's
                // parser is proven against every frozen message type, not only the ones a v1 session carries
                ProxyMessage.newBuilder().setSetExecutorCount(
                        SetExecutorCount.newBuilder().setExecutorCount(2).build()).build());
    }

    private static Configure configure() {
        return Configure.newBuilder()
                .addTopics("demo-topic")
                .setMaxConcurrency(2)
                // exactly one entry - map serialization order across entries is unspecified
                .putKafkaProperties("bootstrap.servers", "localhost:9092")
                .addCapabilities("dispatch")
                .setOrdering(ProcessingOrder.PROCESSING_ORDER_KEY)
                .setCommitMode(CommitMode.COMMIT_MODE_PERIODIC_CONSUMER_SYNC)
                .setCommitInterval(seconds(1))
                .setDefaultMessageRetryDelay(seconds(1))
                .setSendTimeout(seconds(10))
                .setOffsetCommitTimeout(seconds(10))
                .setShutdownTimeout(seconds(10))
                .setDrainTimeout(seconds(30))
                .setThresholdForTimeSpendInQueueWarning(seconds(10))
                .setSaslAuthenticationRetryTimeout(seconds(0))
                .setSaslAuthenticationExceptionRetryBackoff(seconds(1))
                .setMaxFailureHistory(10)
                .setInvalidOffsetMetadataPolicy(InvalidOffsetMetadataPolicy.INVALID_OFFSET_METADATA_POLICY_FAIL)
                .setLaunchToken("per-launch-token-unused-in-v1")
                .setTerminalTopic("demo-topic.terminal")
                .setLeaseDuration(seconds(60))
                .setHeartbeatInterval(seconds(5))
                .setReconnectWindow(seconds(30))
                .setMessageBufferSize(500)
                .setInitialLoadFactor(2)
                .setMaximumLoadFactor(100)
                .setPcInstanceTag("golden-session")
                .build();
    }

    private static Configured configured() {
        return Configured.newBuilder()
                .addTopics("demo-topic")
                .setMaxConcurrency(2)
                .setExecutorCount(2)
                .addCapabilities("dispatch")
                .setOrdering(ProcessingOrder.PROCESSING_ORDER_KEY)
                .setCommitMode(CommitMode.COMMIT_MODE_PERIODIC_CONSUMER_SYNC)
                .setCommitInterval(seconds(1))
                .setDefaultMessageRetryDelay(seconds(1))
                .setSendTimeout(seconds(10))
                .setOffsetCommitTimeout(seconds(10))
                .setShutdownTimeout(seconds(10))
                .setDrainTimeout(seconds(30))
                .setThresholdForTimeSpendInQueueWarning(seconds(10))
                .setSaslAuthenticationRetryTimeout(seconds(0))
                .setSaslAuthenticationExceptionRetryBackoff(seconds(1))
                .setMaxFailureHistory(10)
                .setInvalidOffsetMetadataPolicy(InvalidOffsetMetadataPolicy.INVALID_OFFSET_METADATA_POLICY_FAIL)
                .setTerminalTopic("demo-topic.terminal")
                .setLeaseDuration(seconds(60))
                .setHeartbeatInterval(seconds(5))
                .setReconnectWindow(seconds(30))
                .setMessageBufferSize(500)
                .setInitialLoadFactor(2)
                .setMaximumLoadFactor(100)
                .setPcInstanceTag("golden-session")
                .build();
    }

    private static Dispatch dispatchWave() {
        return Dispatch.newBuilder()
                // record 1: first delivery, tombstone value (present key, ABSENT value)
                .addRecords(DispatchRecord.newBuilder()
                        .setToken(TOKEN_CURRENT)
                        .setRecord(Record.newBuilder()
                                .setTopic("demo-topic")
                                .setPartition(0)
                                .setOffset(0)
                                .setKey(ByteString.copyFromUtf8("key-a")))
                        .setAttempt(1))
                // record 2: a redelivery carrying its full failure history and a beyond-int32 epoch
                .addRecords(DispatchRecord.newBuilder()
                        .setToken(TOKEN_REDELIVERED)
                        .setRecord(Record.newBuilder()
                                .setTopic("demo-topic")
                                .setPartition(0)
                                .setOffset(1)
                                .setKey(ByteString.copyFromUtf8("key-b"))
                                .setValue(ByteString.copyFromUtf8("hello")))
                        .setAttempt(2)
                        .setLastFailureAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).setNanos(1))
                        .setLastFailureReason("worker exploded"))
                .build();
    }

    private static Report successReport() {
        return Report.newBuilder()
                .setToken(TOKEN_CURRENT)
                .setSuccess(Report.Success.newBuilder()
                        .addProduce(ProduceRecord.newBuilder()
                                .setTopic("demo-topic.out")
                                .setKey(ByteString.copyFromUtf8("key-a"))
                                .setValue(ByteString.copyFromUtf8("world"))))
                .build();
    }

    private static Report failureReport() {
        return Report.newBuilder()
                .setToken(TOKEN_REDELIVERED)
                .setFailure(Report.Failure.newBuilder().setReason("worker exploded"))
                .build();
    }

    private static Report terminalReport() {
        return Report.newBuilder()
                .setToken(TOKEN_REDELIVERED)
                .setTerminal(Report.Terminal.newBuilder().setReason("poison pill"))
                .build();
    }

    private static Report releasedReport() {
        return Report.newBuilder()
                .setToken(TOKEN_HELD_BY_DEAD_WORKER)
                .setReleased(Report.Released.getDefaultInstance())
                .build();
    }

    private static Token token(String recordId, long epoch) {
        return Token.newBuilder().setRecordId(recordId).setEpoch(epoch).build();
    }

    private static Duration seconds(long seconds) {
        return Duration.newBuilder().setSeconds(seconds).build();
    }

    static byte[] delimitedClientBytes() throws IOException {
        var out = new java.io.ByteArrayOutputStream();
        for (ClientMessage message : clientMessages()) {
            message.writeDelimitedTo(out);
        }
        return out.toByteArray();
    }

    static byte[] delimitedProxyBytes() throws IOException {
        var out = new java.io.ByteArrayOutputStream();
        for (ProxyMessage message : proxyMessages()) {
            message.writeDelimitedTo(out);
        }
        return out.toByteArray();
    }

    /**
     * Regenerates the committed resources - run with the resources directory
     * ({@code src/test/resources/bz/stub/parallelconsumer/proxy/protocol}) as the argument, only as part of a
     * protocol ADDITION that extends the fixture. {@link GoldenSessionBytesTest} red is how any other
     * regeneration is caught.
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: GoldenSessionFixture <resources-directory>");
        }
        Path directory = Path.of(args[0]);
        try (OutputStream out = Files.newOutputStream(directory.resolve(CLIENT_RESOURCE))) {
            out.write(delimitedClientBytes());
        }
        try (OutputStream out = Files.newOutputStream(directory.resolve(PROXY_RESOURCE))) {
            out.write(delimitedProxyBytes());
        }
    }
}
