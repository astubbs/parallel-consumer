package bz.stub.parallelconsumer.examples.dashboard;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.dashboard.DashboardOptions;
import bz.stub.parallelconsumer.dashboard.DashboardServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;
import java.util.Properties;

import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Turning on the embedded web dashboard.
 * <p>
 * The dashboard is opt-in and read-only. Having the module on the classpath does nothing at all - there is no
 * auto-start and no classpath-scanning activation. It serves a page only once {@link DashboardServer#startFor} is
 * called, and it binds loopback by default.
 */
@Slf4j
public class DashboardApp {

    String inputTopic = "input-topic";

    private final Map<String, String> envVars = System.getenv();

    ParallelEoSStreamProcessor<String, String> parallelConsumer;

    DashboardServer dashboard;

    /**
     * The dashboard reads what it draws from Micrometer, so this same registry has to reach both the consumer and the
     * dashboard. See {@link #setupParallelConsumer()}.
     */
    final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private Consumer<String, String> getKafkaConsumer() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envVars.getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, envVars.getOrDefault("GROUP_ID", "pc-instance"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(props);
    }

    public void run() {
        this.parallelConsumer = setupParallelConsumer();

        parallelConsumer.poll(record ->
                log.info("Concurrently processing a record: {}", record));
    }

    // tag::example[]
    ParallelEoSStreamProcessor<String, String> setupParallelConsumer() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();

        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(1000)
                .consumer(kafkaConsumer)
                .meterRegistry(meterRegistry)                        //<1>
                .build();

        var pc = new ParallelEoSStreamProcessor<>(options);           //<2>
        pc.subscribe(of(inputTopic));

        this.dashboard = DashboardServer.startFor(pc, meterRegistry); //<3>
        log.info("Dashboard: {}", dashboard.getUrl());               //<4>

        return pc;
    }
    // end::example[]

    /**
     * Binding somewhere other than loopback. Everything the page shows is then reachable by anything that can route to
     * that address - see the README's security section before using this.
     */
    // tag::exampleBind[]
    DashboardServer startOnAChosenPort(ParallelEoSStreamProcessor<String, String> pc) {
        var dashboardOptions = DashboardOptions.builder()
                .port(9100)
                .build();

        return DashboardServer.startFor(pc, meterRegistry, dashboardOptions);
    }
    // end::exampleBind[]

    public void close() {
        // The dashboard first: it samples on the consumer's control loop, so it is the thing standing on the other.
        if (dashboard != null) {
            dashboard.close();
        }
        if (parallelConsumer != null) {
            parallelConsumer.close();
        }
    }
}
