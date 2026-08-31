package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ParallelConsumerClient;
import bz.stub.parallelconsumer.client.RecordProcessor;
import bz.stub.parallelconsumer.client.direct.DirectParallelConsumerClient;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The registry of JVM client bindings: the Java transports driven in this JVM rather than through a child
 * process.
 * <p>
 * <b>They are separated from the foreign-language registry because the two answer different questions.</b>
 * That one says where a runner is built and where its binary lands; this one says how a client object is
 * constructed and started. Nothing in either is a policy about which bindings run - that is
 * {@link ConformanceBindings}', with the control arm.
 * <p>
 * <b>{@code java-direct} is the most interesting binding in the set</b>, because it is the one whose wire is
 * a function call. Every other binding's red run has a transport among its suspects; this one's does not, so
 * it is the control arm for the shared API itself - "does the surface behave the same way when there is
 * nothing underneath it?" A scenario that passes for a wire client and fails here is a claim about the API,
 * not about a stream.
 * <p>
 * <b>{@code java-grpc} IS NOT REGISTERED HERE, AND ITS ABSENCE IS GUARDED RATHER THAN NOTED.</b> The gRPC
 * transport connects to a sidecar, and the sidecar on this stack hosts no engine - it refuses every session
 * {@code UNIMPLEMENTED} (astubbs/parallel-consumer#384). A binding for it could not be written without
 * writing the engine, and a stubbed one would make this suite's agreement between bindings worthless. So the
 * cell is left for the engine rung, and {@link TheEngineArrivingMustBringTheGrpcBindingTest} fails the build
 * the moment the engine reaches this module's classpath while this registry still has one entry.
 *
 * @author Antony Stubbs
 * @see JvmClientBinding
 */
public final class JvmClientBindings {

    /** The name the gRPC transport's binding answers to, once there is an engine for it to reach. */
    static final String JAVA_GRPC = "java-grpc";

    /**
     * The in-process transport: the shared client API bound straight to core, with no protocol anywhere
     * beneath it. The harness lends it the mock Kafka clients whose commit history the assertions read, and
     * the client brings its own engine - {@link ConformanceHarness#startEmbeddedClient} is that lane.
     */
    public static JvmClientBinding javaDirect() {
        return new JvmClientBinding("java-direct", JvmClientBindings::startDirect);
    }

    /** Every JVM client binding, whether or not this run selected it. */
    public static List<JvmClientBinding> all() {
        return List.of(javaDirect());
    }

    private static ParallelConsumerClient startDirect(ConformanceHarness harness, ConformanceScenario scenario,
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

    private JvmClientBindings() {
    }
}
