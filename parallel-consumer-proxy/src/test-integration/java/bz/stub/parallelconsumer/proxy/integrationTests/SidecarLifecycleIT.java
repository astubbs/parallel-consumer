package bz.stub.parallelconsumer.proxy.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest;
import bz.stub.parallelconsumer.proxy.Main;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The sidecar as an actual operating-system process, against an actual broker - the first test in this module
 * that is either. Everything else here runs the engine in-JVM on mock clients, which cannot exercise the two
 * things this unit is about: a real inherited pipe, and a real commit.
 *
 * <h2>Why a child process rather than another in-JVM run</h2>
 *
 * {@link Main}'s contract is a process contract. The port arrives on stdout because a parent reads stdout;
 * parent death is EOF on a pipe the kernel closes because a process died. Running {@code Main.run} on a
 * thread with a {@code PipedInputStream} - which the unit tests do, correctly, for the logic - proves none of
 * that, because no operating system is involved. So this spawns the real thing.
 *
 * @author Antony Stubbs
 */
@Testcontainers
@Slf4j
class SidecarLifecycleIT extends BrokerIntegrationTest<byte[], byte[]> {

    private static final Duration STARTUP_BUDGET = Duration.ofSeconds(60);

    /**
     * The whole spawning contract end to end: a real process binds a real port, accepts a real gRPC session,
     * builds real Kafka clients from the properties that travelled the wire, and dispatches a record that was
     * genuinely produced to a broker.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void aSpawnedSidecarServesARealBrokerOverTheWire() throws Exception {
        String topic = setupTopic("sidecar-lifecycle");
        getKcu().produceMessages(topic, 1);

        try (var sidecar = SidecarProcess.spawn()) {
            var channel = ManagedChannelBuilder.forAddress("127.0.0.1", sidecar.port()).usePlaintext().build();
            try {
                var responses = new LinkedBlockingQueue<ProxyMessage>();
                var streamError = new AtomicReference<Throwable>();
                var requests = openSession(channel, responses, streamError);

                requests.onNext(ClientMessage.newBuilder()
                        .setConfigure(Configure.newBuilder()
                                .addTopics(topic)
                                .setMaxConcurrency(4)
                                .putKafkaProperties(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                        kafkaContainer.getBootstrapServers())
                                .putKafkaProperties(ConsumerConfig.GROUP_ID_CONFIG, "sidecar-lifecycle-it")
                                .putKafkaProperties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
                        .build());

                var configured = take(responses, streamError).getConfigured();
                assertThat(configured.getTopicsList()).containsExactly(topic);
                assertWithMessage("the executor count travels once, in Configured (KTD38)")
                        .that(configured.getExecutorCount()).isEqualTo(4);

                var dispatched = take(responses, streamError).getDispatch().getRecords(0);
                assertWithMessage("a record produced to a real broker reached a foreign client over the wire")
                        .that(dispatched.getToken().getRecordId()).isNotEmpty();

                requests.onNext(ClientMessage.newBuilder()
                        .setReport(Report.newBuilder()
                                .setToken(dispatched.getToken())
                                .setSuccess(Report.Success.newBuilder()))
                        .build());

                sidecar.killTheParentSide();
                assertWithMessage("the sidecar outlived the parent that spawned it")
                        .that(sidecar.process().waitFor(60, TimeUnit.SECONDS)).isTrue();
                assertThat(sidecar.process().exitValue()).isEqualTo(0);
            } finally {
                channel.shutdownNow();
            }
        }
    }

    /**
     * Two applications on one host get two sidecars, not a bind race. Real processes, real ephemeral ports.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void twoSpawnedSidecarsBindDifferentPorts() throws Exception {
        try (var first = SidecarProcess.spawn(); var second = SidecarProcess.spawn()) {
            assertThat(first.port()).isNotEqualTo(second.port());

            first.killTheParentSide();
            second.killTheParentSide();
            assertThat(first.process().waitFor(60, TimeUnit.SECONDS)).isTrue();
            assertThat(second.process().waitFor(60, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static StreamObserver<ClientMessage> openSession(ManagedChannel channel,
                                                             LinkedBlockingQueue<ProxyMessage> responses,
                                                             AtomicReference<Throwable> streamError) {
        return ProxyServiceGrpc.newStub(channel).session(new StreamObserver<>() {
            @Override
            public void onNext(ProxyMessage message) {
                responses.add(message);
            }

            @Override
            public void onError(Throwable t) {
                streamError.set(t);
            }

            @Override
            public void onCompleted() {
                // the test drives completion from the client side
            }
        });
    }

    /** Fails with the stream's error rather than a bare timeout, so a broken session says why. */
    private static ProxyMessage take(LinkedBlockingQueue<ProxyMessage> responses,
                                     AtomicReference<Throwable> streamError) throws InterruptedException {
        var message = responses.poll(60, TimeUnit.SECONDS);
        if (message == null) {
            var error = streamError.get();
            throw new AssertionError("no message within 60s", error);
        }
        return message;
    }
}
