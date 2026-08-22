package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.Main;
import bz.stub.parallelconsumer.proxy.lifecycle.ParentDeathWatchdog;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Properties;

/**
 * The Streams sidecar: a second entry point beside the Parallel Consumer one.
 *
 * <p>A second entry point rather than a second service on the existing listener, and that is a decision with a
 * reason. The proxy's server takes one bindable service and constructs one connection guard per start, and it
 * deliberately compiles against no generated protocol type - hosting two services there would force a choice about
 * whether the single-connection slot is shared across them, and would breach that seam. The test-mode sidecar is
 * the established precedent for adding a differently-configured entry point instead.
 *
 * <p>From a client's point of view this behaves like the existing sidecar: it prints its port on stdout as the first
 * line, using {@link Main#PORT_LINE_PREFIX} so the two cannot drift, and it exits when its parent dies. A leaked
 * Streams instance holds state-store locks as well as group membership, so the watchdog matters more here, not less.
 */
public final class StreamsMain {

    private static final Logger log = LoggerFactory.getLogger(StreamsMain.class);

    private static final Duration PARENT_POLL_INTERVAL = Duration.ofMillis(250);

    private StreamsMain() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err, System.in));
    }

    static int run(String[] args, PrintStream out, PrintStream err, InputStream parentLifeline) {
        if (args.length > 0) {
            err.println("streams sidecar: takes no arguments; the session is configured over the protocol");
            return Main.EXIT_USAGE;
        }

        Server server;
        try {
            server = NettyServerBuilder
                    // Loopback and an ephemeral port. The surface is unauthenticated, so it must not be reachable
                    // beyond this machine, and the port is announced rather than agreed in advance.
                    .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                    .addService(new StreamsSessionService(StreamsMain::startTopology))
                    .build()
                    .start();
        } catch (IOException bindFailed) {
            err.println("streams sidecar: could not bind the listener: " + bindFailed.getMessage());
            return Main.EXIT_USAGE;
        }

        // First line on stdout, and the client scans for the prefix rather than assuming position - a log line
        // ahead of it is survivable, a different prefix is not.
        out.println(Main.PORT_LINE_PREFIX + server.getPort());
        log.info("Streams sidecar listening on loopback port {}; waiting for the client to describe a topology",
                server.getPort());

        try (var watchdog = ParentDeathWatchdog.watchingParentOf(parentLifeline, PARENT_POLL_INTERVAL)) {
            watchdog.start();
            watchdog.awaitDeath();
            log.info("Shutting down: {}", watchdog.cause());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            server.shutdown();
        }
        return 0;
    }

    private static StreamsSessionService.TopologyRun startTopology(
            org.apache.kafka.streams.Topology topology,
            bz.stub.parallelconsumer.streams.protocol.v1alpha1.Open open) {

        Properties config = new Properties();
        // The host's Kafka settings travel on the wire and are used as-is. They are never echoed back.
        config.putAll(open.getKafkaPropertiesMap());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, open.getApplicationId());
        // Records cross as bytes in both directions; the engine never deserializes what the host sent.
        config.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
        config.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();
        return streams::close;
    }
}
