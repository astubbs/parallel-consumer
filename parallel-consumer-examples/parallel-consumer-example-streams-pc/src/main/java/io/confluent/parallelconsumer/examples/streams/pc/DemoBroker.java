package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The broker the demo brings with it, so running this needs Docker and nothing else.
 * <p>
 * <b>One container for the whole run, shared by every arm.</b> Starting a broker is by far the largest
 * fixed cost here, and paying it once per arm would multiply the demo's runtime by four for no gain. What
 * the arms must not share is state that would let one contaminate the next, so each arm gets fresh topics
 * and a fresh {@code application.id} instead.
 *
 * @author Antony Stubbs
 */
final class DemoBroker implements AutoCloseable {

    /** Confluent Platform major = Apache Kafka major + 4, so AK 3.9 means CP 7.9. */
    private static final String FALLBACK_IMAGE = "confluentinc/cp-kafka:7.9.0";

    private static final int TOPIC_CREATE_TIMEOUT_SECONDS = 60;

    private final KafkaContainer container;

    private final boolean wasAlreadyRunning;

    private DemoBroker(final KafkaContainer container, final boolean wasAlreadyRunning) {
        this.container = container;
        this.wasAlreadyRunning = wasAlreadyRunning;
    }

    static DemoBroker start() {
        String image = deriveImage();
        Console.line("  Starting a Kafka broker in Docker (%s).", image);
        Console.line("  This is the slowest part of the run. First time on a machine it also pulls the image.");

        KafkaContainer container = new KafkaContainer(DockerImageName.parse(image))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
                // Default is 3000ms. Every arm forms a new consumer group, so this is paid once per arm and
                // is pure demo latency.
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500")
                // Honoured only if the reader has opted in via ~/.testcontainers.properties. When they have,
                // a repeat run skips broker startup entirely.
                .withReuse(true);

        long startedAt = System.nanoTime();
        // Testcontainers' own progress logging is turned down (see logback.xml) because at INFO it buries
        // the report. That leaves a cold image pull looking like a hang, so the wait reports itself.
        Thread ticker = startProgressTicker(startedAt);
        try {
            container.start();
        } finally {
            ticker.interrupt();
        }
        long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

        boolean reused = elapsedMillis < 2_000;
        Console.line("  Broker up in %,dms at %s%s", elapsedMillis, container.getBootstrapServers(),
                reused ? " (reused an already-running container)" : "");
        return new DemoBroker(container, reused);
    }

    String bootstrapServers() {
        return container.getBootstrapServers();
    }

    boolean wasReused() {
        return wasAlreadyRunning;
    }

    /**
     * Creates a topic and does not return until it exists.
     * <p>
     * Blocking on the create is deliberate. A fire-and-forget create races the producer that immediately
     * follows it, and this repo has already paid for that once - see
     * {@code docs/solutions/test-issues/flaky-topic-creation-timeout-2026-07-28.md}.
     *
     * @param partitions always 1 here: stock Kafka Streams' only concurrency is per partition, so giving it
     *                   more would hand the control arm the very parallelism the comparison says it lacks
     */
    void createTopic(final String name, final int partitions) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(Collections.singletonList(new NewTopic(name, partitions, (short) 1)))
                    .all()
                    .get(TOPIC_CREATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted creating topic " + name, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Could not create topic " + name, e);
        }
    }

    KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * Reports every few seconds that the broker is still coming up, so a first run that has to pull the
     * image does not look like a hang. Daemon, so it can never hold the JVM open.
     */
    private static Thread startProgressTicker(final long startedAt) {
        Thread ticker = new Thread(() -> {
            try {
                while (true) {
                    Thread.sleep(5_000);
                    Console.line("    ... still starting (%,ds elapsed)",
                            (System.nanoTime() - startedAt) / 1_000_000_000L);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "broker-start-progress");
        ticker.setDaemon(true);
        ticker.start();
        return ticker;
    }

    private static String deriveImage() {
        String kafkaVersion = AppInfoParser.getVersion();
        try {
            String[] parts = kafkaVersion.split("-")[0].split("\\.");
            int major = Integer.parseInt(parts[0]) + 4;
            int minor = Integer.parseInt(parts[1]);
            return "confluentinc/cp-kafka:" + major + "." + minor + ".0";
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            return FALLBACK_IMAGE;
        }
    }

    @Override
    public void close() {
        container.stop();
    }
}
