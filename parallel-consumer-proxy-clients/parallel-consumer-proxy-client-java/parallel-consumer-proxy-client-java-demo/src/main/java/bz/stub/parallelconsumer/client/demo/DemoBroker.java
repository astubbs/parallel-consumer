package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The broker the demo reads from, however the reader got here.
 *
 * <h2>Two ways in, and the second one is a rule rather than a convenience</h2>
 *
 * <ul>
 *   <li><b>Nothing supplied</b> - the demo starts a real broker in a container, which is KTD40's
 *       honest default because it is what a user actually runs.</li>
 *   <li><b>An address supplied</b> - the demo uses it and starts nothing. This is how the demo
 *       runs <em>inside</em> its own container, and it is not optional there: <b>a demo container
 *       is never granted the host Docker socket</b> (plan unit U35, step 2), so it cannot start a
 *       broker even if it wanted to. It reaches a compose sibling on the demo's own network
 *       instead. A documented socket mount is root-equivalent host access taught as the normal way
 *       to run the product, which is why the rule exists rather than the shortcut.</li>
 * </ul>
 *
 * The same door serves own-cluster mode, where the address is the user's real cluster - so nothing
 * here logs or echoes it.
 *
 * @author Antony Stubbs
 */
@Slf4j
public final class DemoBroker implements AutoCloseable {

    /**
     * The key space the seeded records spread over. Ordering is UNORDERED in every arm, so this
     * changes nothing about how they run; it exists so that a KEY-ordered lane added later has more
     * than one key to shard across, rather than needing the seeding rewritten first.
     * <p>
     * It is also what makes the <b>keys</b> column of the results tables a <em>checkable</em>
     * number rather than a decorative one - see {@link #expectedUniqueKeys(int)}.
     */
    static final int KEY_SPACE = 1_000;

    private static final String FALLBACK_IMAGE = "confluentinc/cp-kafka:7.9.0";

    private final String bootstrap;

    private final KafkaContainer container;

    private DemoBroker(String bootstrap, KafkaContainer container) {
        this.bootstrap = bootstrap;
        this.container = container;
    }

    /**
     * Uses the supplied broker, or starts one when none was supplied.
     *
     * @param supplied the address from {@code --bootstrap} or the environment, or null
     */
    public static DemoBroker resolve(String supplied) {
        if (supplied != null && !supplied.trim().isEmpty()) {
            // deliberately not logged: own-cluster mode puts a real address here
            log.info("Using the broker supplied by the caller.");
            return new DemoBroker(supplied.trim(), null);
        }
        String image = brokerImage();
        log.info("No broker supplied, starting one in a container: {}", image);
        var started = new KafkaContainer(DockerImageName.parse(image))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
                // the demo forms several consumer groups in a row, one per arm, and the default
                // three-second settling delay would be charged to every one of them
                // Deliberately NOT withReuse(true): Testcontainers short-circuits stop() for a
                // reusable container, so close() below would leave a broker running after the demo
                // exits. The demo names a fresh topic every run, so reuse would save nothing anyway.
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500");
        started.start();
        return new DemoBroker(started.getBootstrapServers(), started);
    }

    /**
     * The broker image, tracking the Kafka client this build carries.
     * <p>
     * This repeats the mapping in {@code BrokerIntegrationTest#deriveCpKafkaImage} rather than
     * calling it, and the repetition is deliberate: that class starts a singleton broker in a
     * static initialiser, so merely referencing it from here would start a second container the
     * demo never uses. Keep the two in step - and note that the compose file beside this module
     * pins the same image as a literal, because a compose file cannot derive anything.
     */
    private static String brokerImage() {
        String akVersion = AppInfoParser.getVersion();
        try {
            String[] parts = akVersion.split("-")[0].split("\\.");
            // CP major = AK major + 4 (AK 3.9 -> CP 7.9), CP minor tracks AK minor
            return "confluentinc/cp-kafka:" + (Integer.parseInt(parts[0]) + 4) + "." + parts[1] + ".0";
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            log.warn("Could not read the Kafka version from '{}', falling back to {}",
                    akVersion, FALLBACK_IMAGE, e);
            return FALLBACK_IMAGE;
        }
    }

    public String bootstrap() {
        return bootstrap;
    }

    /**
     * How many distinct keys an arm must see, having replayed {@code records} seeded records.
     *
     * <h2>Why this is worth a method</h2>
     *
     * {@link #seed} lays records over the key space cyclically, so the answer is exactly
     * "the whole key space, or the backlog if it is smaller". Every arm reports its own observed
     * count in the results tables, and this is the number that count has to equal - which is what
     * turns the <b>keys</b> column from an assertion into a demonstration, and what
     * {@code ReferenceDemoIT} checks. A demo whose evidence column cannot be predicted is not
     * evidence.
     */
    public static int expectedUniqueKeys(int records) {
        return Math.min(records, KEY_SPACE);
    }

    /** Creates the demo's topic, tolerating one that a previous run already left behind. */
    public void ensureTopic(String topic, int partitions) throws InterruptedException {
        var config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(config)) {
            try {
                admin.createTopics(Collections.singletonList(new NewTopic(topic, partitions, (short) 1)))
                        .all().get();
                log.info("Created topic {} with {} partitions", topic, partitions);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new IllegalStateException("could not create the demo topic " + topic,
                            e.getCause());
                }
                // Reusing a topic silently is fine; reusing one with a DIFFERENT partition count is
                // not, because the effective-configuration block would print a --partitions value
                // that never applied - and that block is the demo's whole reproducibility promise.
                int existing = partitionsOf(admin, topic);
                if (existing != partitions) {
                    throw new IllegalStateException("topic " + topic + " already exists with "
                            + existing + " partitions, but this run asked for " + partitions
                            + " - pass --topic to name a fresh one, or --partitions " + existing);
                }
                log.info("Topic {} already exists with the requested {} partitions, reusing it",
                        topic, partitions);
            }
        }
    }

    private static int partitionsOf(Admin admin, String topic) throws InterruptedException {
        try {
            return admin.describeTopics(Collections.singletonList(topic))
                    .allTopicNames().get().get(topic).partitions().size();
        } catch (ExecutionException e) {
            throw new IllegalStateException("could not describe the existing topic " + topic,
                    e.getCause());
        }
    }

    /**
     * Produces the backlog every arm then replays.
     * <p>
     * Pre-produced rather than produced alongside the arms, and that is what makes the workload
     * closed-loop - which is in turn why no arm reports latency. A per-record timing here would be
     * flattered by however far an arm had fallen behind, so throughput is the only honest number
     * this shape can produce.
     */
    public void seed(String topic, int from, int to) {
        if (to <= from) {
            return;
        }
        var config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.LINGER_MS_CONFIG, "20");

        log.info("Producing records {} to {}...", from, to);
        // flush() does not throw for a send that failed, and a discarded Future swallows the reason,
        // so without this the demo would report a full backlog, run every arm against a short one,
        // and print numbers for a workload that never existed.
        var firstFailure = new AtomicReference<Exception>();
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            for (int i = from; i < to; i++) {
                byte[] key = ("key-" + (i % KEY_SPACE)).getBytes(StandardCharsets.UTF_8);
                byte[] value = ("record-" + i).getBytes(StandardCharsets.UTF_8);
                producer.send(new ProducerRecord<>(topic, key, value),
                        (metadata, exception) -> {
                            if (exception != null) {
                                firstFailure.compareAndSet(null, exception);
                            }
                        });
            }
            producer.flush();
        }
        if (firstFailure.get() != null) {
            throw new IllegalStateException("the demo could not seed its backlog", firstFailure.get());
        }
        log.info("Produced {} records", to - from);
    }

    /**
     * The Kafka properties every arm's consumer needs to reach this broker.
     *
     * <h2>Why {@code enable.auto.commit} is in here, and what it exposes</h2>
     *
     * Parallel Consumer owns offset commits, so it refuses a consumer with auto-commit on. The
     * sidecar forces the setting itself - {@code KafkaClientFactory} sets it false "whatever the
     * map says" - but {@code DirectParallelConsumerClient} builds its consumer straight from the
     * caller's properties and does not, and Kafka's own default is true. So the same
     * {@code ClientOptions} run over gRPC and throw over direct, which is a divergence between two
     * transports that are meant to be interchangeable.
     * <p>
     * The demo sets it so every arm runs, and says so here rather than letting it read as
     * incidental. <b>The divergence itself is recorded as a finding against the client library</b>
     * (see the branch record); when it is closed, this line becomes redundant rather than wrong.
     * It is also invisible to the direct transport's conformance suite by construction - that suite
     * injects its own mock consumer, so the code path that builds one from properties is never
     * exercised there.
     */
    public Map<String, String> consumerProperties(String groupId) {
        var properties = new LinkedHashMap<String, String>();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", "false");
        return properties;
    }

    @Override
    public void close() {
        if (container != null) {
            container.stop();
        }
    }
}
