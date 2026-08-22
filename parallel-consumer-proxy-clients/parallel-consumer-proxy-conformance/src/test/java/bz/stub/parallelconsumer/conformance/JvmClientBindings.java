package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ParallelConsumerClient;
import bz.stub.parallelconsumer.client.RecordProcessor;
import bz.stub.parallelconsumer.client.direct.DirectParallelConsumerClient;
import bz.stub.parallelconsumer.client.grpc.GrpcParallelConsumerClient;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The registry of JVM client bindings: the two Java transports, driven in this JVM rather than through a
 * child process.
 * <p>
 * <b>They are separated from {@link LanguageRunners} because the two registries answer different questions.</b>
 * That one says where a foreign runner is built and where its binary lands; this one says how a client
 * object is constructed and started. Nothing in either is a policy about which bindings run - that is
 * {@link ConformanceBindings}', with the control arm.
 * <p>
 * <b>{@code java-direct} is the most interesting binding in the set</b>, because it is the one whose wire is
 * a function call. Every other binding's red run has a transport among its suspects; this one's does not, so
 * it is the control arm for the shared API itself - "does the surface behave the same way when there is
 * nothing underneath it?" A scenario that passes for {@code java-grpc} and fails here is a claim about the
 * API, not about a stream.
 *
 * @author Antony Stubbs
 * @see JvmClientBinding
 */
public final class JvmClientBindings {

    /**
     * The in-process transport: the shared client API bound straight to core, with no protocol anywhere
     * beneath it. The harness lends it the mock Kafka clients whose commit history the assertions read, and
     * the client brings its own engine - {@link ProxyHarness#startEmbeddedClient} is that lane.
     */
    public static JvmClientBinding javaDirect() {
        return new JvmClientBinding("java-direct", JvmClientBindings::startDirect);
    }

    /**
     * The reference wire client: a real gRPC session against the engine's real transport, on an ephemeral
     * loopback port, in the same JVM. Every byte crosses a genuine stream; what it does not cross is a
     * process boundary, and {@link JvmClientBinding} carries the reasoning for that.
     */
    public static JvmClientBinding javaGrpc() {
        return new JvmClientBinding("java-grpc", JvmClientBindings::startGrpc);
    }

    /** Every JVM client binding, whether or not this run selected it. */
    public static List<JvmClientBinding> all() {
        return List.of(javaDirect(), javaGrpc());
    }

    private static ParallelConsumerClient startDirect(ProxyHarness harness, ConformanceScenario scenario,
                                                      RecordProcessor processor) {
        // The client is built INSIDE the lane because it is its own rebalance listener, and the harness has
        // to hold that listener to perform the manual assignment a MockConsumer needs. The reference escapes
        // so the run can close it; nothing reads it before start returns.
        var started = new AtomicReference<DirectParallelConsumerClient>();
        harness.startEmbeddedClient((consumer, producer) -> {
            var client = DirectParallelConsumerClient.builder()
                    .options(JvmClientBinding.optionsFor(scenario))
                    .consumer(consumer)
                    .producer(producer)
                    .build();
            started.set(client);
            client.poll(processor);
            return client;
        });
        return started.get();
    }

    private static ParallelConsumerClient startGrpc(ProxyHarness harness, ConformanceScenario scenario,
                                                    RecordProcessor processor) {
        int port = harness.startEngine();
        var client = GrpcParallelConsumerClient.builder()
                .port(port)
                .options(JvmClientBinding.optionsFor(scenario))
                .build();
        // Nothing after it: the harness assigns the partition and seeds the scenario when the Configure
        // arrives, so the client's own handshake is what starts the run.
        client.poll(processor);
        return client;
    }

    private JvmClientBindings() {
    }
}
