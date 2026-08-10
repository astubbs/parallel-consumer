package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * <b>The module's first rebalance coverage.</b> Two {@code KafkaStreams} instances in one application id over
 * a multi-partition topic, with the second joining mid-run, both dispatching through Parallel Consumer.
 *
 * <h2>Why this test exists, in the words of the thing it closes</h2>
 * Every other integration proof this module has runs <b>one partition, one task, one instance</b>. Multi-task
 * and multi-instance behaviour were not imperfect, they were untested - which is a bigger limit than any item
 * on the shortcomings list, because nothing would have reported a regression in it. The lifecycle fixes in
 * U10 (astubbs#255) are all about what happens when a task changes hands, and this is the only arm in which a
 * task actually does.
 *
 * <h2>The assertions were chosen before the harness, deliberately</h2>
 * A rebalance test that asserts "nothing threw" is close to worthless: a run that silently dropped half the
 * input satisfies it. The four properties below are what make a handover correct, and each one is here
 * because a specific defect would break it and nothing else would notice.
 * <ol>
 *   <li><b>No loss.</b> Every produced key appears in the output from <em>somebody</em>. This is the property
 *       a stale partition set breaks: a task that gains a partition without PC learning of it registers the
 *       records and dispatches none of them, and the topology just looks idle.</li>
 *   <li><b>Duplicates bounded by CAPACITY, not by a fraction of throughput.</b> At-least-once permits
 *       re-processing the window between the last commit and the revocation - which is bounded by the records
 *       in flight plus one commit interval's worth, a flat number that does not grow with the run. A
 *       percentage bound would pass a run that reprocessed everything, and this repository has the lesson
 *       recorded: see {@code ProgressProbe.ledger}'s javadoc in core.</li>
 *   <li><b>The handover actually happened.</b> Without it the first two assertions are satisfiable by
 *       instance A alone, and the test would prove nothing about rebalancing at all.</li>
 *   <li><b>Ownership moved rather than being shared.</b> The keys both instances processed must sit inside
 *       the same capacity bound - two instances concurrently owning one partition would put the whole
 *       overlap outside it.</li>
 * </ol>
 *
 * <h2>The reader is scoped, and that is load-bearing</h2>
 * Assertion 3 is a before/after claim, and this repository has already shipped a vacuous one of exactly this
 * shape - a crash-restart assertion satisfied by pre-crash data, because the reader subscribed from earliest
 * (see {@code docs/solutions/test-issues/a-restart-assertion-satisfiable-by-pre-crash-data-proves-nothing.md}).
 * So the output position is captured at the instant B is started, and the "B took over" reader is
 * {@code assign}-and-{@code seek}ed past it - structurally incapable of being satisfied by anything A
 * produced beforehand, rather than relying on the assertion staying careful.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.streams.PcTaskDispatcherTest
 */
@Slf4j
// Process-wide state, twice over: PcDispatchSwitch has no seam through KafkaStreams to inject, and
// PcTaskDispatcher's ACTIVE registry is JVM-wide. Without isolation this arm would flip the switch under a
// concurrently running control arm.
@Isolated
class RebalanceUnderPcDispatchTest extends BrokerStreamsIntegrationTest {

    /** More than one, so a handover can move a proper subset and both instances can hold work at once. */
    private static final int PARTITIONS = 4;

    private static final int KEYS_BEFORE_JOIN = 120;
    private static final int KEYS_AFTER_JOIN = 120;
    private static final int TOTAL_KEYS = KEYS_BEFORE_JOIN + KEYS_AFTER_JOIN;

    private static final int POOL_SIZE = 4;

    /**
     * Short, and stated here rather than inherited, because it is one of the two terms the duplicate bound is
     * derived from - a bound computed against a commit interval the test does not control is not a bound.
     */
    private static final Duration COMMIT_INTERVAL = Duration.ofSeconds(1);

    /**
     * The at-least-once window a correct handover may re-deliver: the records that were in flight when the
     * partition was revoked, plus whatever completed since the last commit.
     * <p>
     * <b>Flat, and derived rather than tuned.</b> Per instance, PC can have at most {@code POOL_SIZE} records
     * inside the chain per task, and at most {@code PARTITIONS} tasks - so {@code POOL_SIZE * PARTITIONS} is
     * the in-flight ceiling. The commit-lag term is the records one instance can complete in one
     * {@link #COMMIT_INTERVAL}, which for this topology is bounded by the run's own size rather than by time,
     * so it is allowed a generous slice. What matters is that <b>neither term grows with the number of
     * records produced</b>: double the run and this number does not move, which is exactly the property a
     * percentage-of-throughput bound does not have.
     */
    private static final int DUPLICATE_BOUND = POOL_SIZE * PARTITIONS + 60;

    /**
     * The in-flight half of {@link #DUPLICATE_BOUND} on its own: how many records one instance can have
     * inside the processor chain when a partition is revoked, and therefore how many keys can legitimately be
     * processed by <em>both</em> instances across one handover.
     * <p>
     * <b>Separate from {@link #DUPLICATE_BOUND} because asserting the same number twice proves nothing.</b>
     * Every key processed by both instances was delivered at least twice, so it necessarily contributes at
     * least one to the total duplicate count - which makes "keys processed by both {@code <=} DUPLICATE_BOUND"
     * arithmetically implied by "duplicates {@code <=} DUPLICATE_BOUND" and unable to fail on its own. Two
     * instances concurrently owning one partition would sit inside the looser bound while breaking this one,
     * which is the whole point of asserting it.
     */
    private static final int IN_FLIGHT_BOUND = POOL_SIZE * PARTITIONS;

    private String inputTopic;
    private String outputTopic;

    private final List<KafkaStreams> started = new ArrayList<>();

    @BeforeEach
    void enableSeam() {
        PcDispatchCounters.reset();
        // Stated at the call site rather than leaned on as a default - a test whose subject is the seam must
        // say so, or a future default flip silently turns it into a stock-dispatch test.
        PcDispatchSwitch.enable(POOL_SIZE);
    }

    @AfterEach
    void stopInstances() {
        for (KafkaStreams streams : started) {
            try {
                streams.close(Duration.ofSeconds(30));
            } catch (RuntimeException e) {
                log.warn("Failed to close a Streams instance cleanly", e);
            }
        }
        started.clear();
        PcDispatchSwitch.resetToDefault();
    }

    @Test
    void aSecondInstanceJoiningMidRunTakesOverWithoutLosingOrReplayingTheRun() {
        // Deliberately NOT setupTopic(): that creates with the base class's own numPartitions field, which
        // is package-private to core's integrationTests package and therefore not settable from here - so it
        // would silently give us a ONE-partition topic, and a one-partition topic cannot rebalance between
        // two instances at all. (The first run of this test failed on exactly that, as a listOffsets timeout
        // against output partitions that did not exist.) ensureTopic is the protected entry point that takes
        // the count explicitly.
        final long unique = System.nanoTime();
        inputTopic = "pc-rebalance-in-" + unique;
        outputTopic = "pc-rebalance-out-" + unique;
        ensureTopic(inputTopic, PARTITIONS);
        ensureTopic(outputTopic, PARTITIONS);

        final String applicationId = "pc-rebalance-app-" + System.nanoTime();
        final Set<String> producedKeys = new LinkedHashSet<>();

        produceKeys(producedKeys, 0, KEYS_BEFORE_JOIN);

        // ---- instance A alone ----
        startInstance(applicationId, "A");
        awaitOutputCount(KEYS_BEFORE_JOIN / 2);

        // ---- the boundary, captured at the instant B joins ----
        // Everything below this position is A's pre-rebalance work, and the "did B take over" reader must be
        // incapable of seeing it. Captured BEFORE B starts, so B's own early output cannot land underneath it.
        final List<Long> joinBoundary = outputEndOffsets();

        // ---- B joins, and production continues across the handover ----
        startInstance(applicationId, "B");
        produceKeys(producedKeys, KEYS_BEFORE_JOIN, TOTAL_KEYS);

        awaitOutputCount(TOTAL_KEYS);

        // ---- the ledger ----
        final List<ConsumerRecord<String, String>> allOutputs = drainAll();
        final Set<String> consumedKeys = new HashSet<>();
        final Set<String> keysByA = new HashSet<>();
        final Set<String> keysByB = new HashSet<>();
        for (ConsumerRecord<String, String> record : allOutputs) {
            consumedKeys.add(record.key());
            (instanceTagOf(record.value()).equals("A") ? keysByA : keysByB).add(record.key());
        }

        // 1. No loss.
        assertThat(consumedKeys)
                .as("at-least-once: every produced record must be processed by SOMEBODY across the "
                        + "handover. A partition whose new owner never learned of it registers records and "
                        + "dispatches none, which looks exactly like an idle topology")
                .containsAll(producedKeys);

        // 2. Duplicates bounded by capacity.
        final int duplicateDeliveries = allOutputs.size() - consumedKeys.size();
        assertThat(duplicateDeliveries)
                .as("duplicates must be bounded by the in-flight window plus commit lag (%s), NOT by a "
                        + "fraction of the %s records produced - a percentage bound passes a run that "
                        + "reprocessed the lot", DUPLICATE_BOUND, allOutputs.size())
                .isLessThanOrEqualTo(DUPLICATE_BOUND);

        // 3. The handover actually happened - proven with a reader that cannot see A's earlier work.
        final List<ConsumerRecord<String, String>> afterJoin = drainFrom(joinBoundary);
        final Set<String> instancesAfterJoin = new HashSet<>();
        for (ConsumerRecord<String, String> record : afterJoin) {
            instancesAfterJoin.add(instanceTagOf(record.value()));
        }
        assertThat(instancesAfterJoin)
                .as("the second instance must have taken real work. This reader is assign+seek'd past the "
                        + "position captured when B started, so it CANNOT be satisfied by anything A "
                        + "produced beforehand - without that scoping this assertion would be vacuous")
                .contains("B");

        // 4. Ownership moved rather than being shared.
        final Set<String> processedByBoth = new HashSet<>(keysByA);
        processedByBoth.retainAll(keysByB);
        assertThat(processedByBoth.size())
                .as("a key processed by both instances is a re-delivery across the handover, bounded by what "
                        + "was IN FLIGHT when the partition was revoked (%s) - deliberately tighter than the "
                        + "overall duplicate bound, because two instances owning one partition at the same "
                        + "time would put this far outside the in-flight window while still sitting inside "
                        + "the looser one", IN_FLIGHT_BOUND)
                .isLessThanOrEqualTo(IN_FLIGHT_BOUND);

        log.info("[rebalance-ledger] produced={} uniqueConsumed={} totalOutputs={} duplicates={} bound={} "
                        + "keysByA={} keysByB={} bothInstances={}",
                producedKeys.size(), consumedKeys.size(), allOutputs.size(), duplicateDeliveries,
                DUPLICATE_BOUND, keysByA.size(), keysByB.size(), processedByBoth.size());
    }

    /**
     * One instance of the topology, tagged so its output says which instance produced it.
     * <p>
     * The tag is carried in the record <em>value</em> rather than inferred from timing, because "which
     * instance did this" is the whole subject of assertions 3 and 4 and inferring it would make them
     * assertions about the test's clock.
     */
    @SneakyThrows
    private void startInstance(final String applicationId, final String instanceTag) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);
        stream.mapValues(value -> instanceTag + "|" + value).to(outputTopic);

        Properties props = baseStreamsProps(applicationId);
        // Both instances live in ONE JVM, so they would otherwise fight over the same state directory lock
        // and the second would never reach RUNNING.
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory("pc-rebalance-" + instanceTag).toAbsolutePath().toString());
        props.put(StreamsConfig.CLIENT_ID_CONFIG, applicationId + "-" + instanceTag);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, (int) COMMIT_INTERVAL.toMillis());
        // Cooperative is Kafka Streams' own default and the mode the lifecycle fixes target - a task that
        // gains and loses partitions WITHOUT being closed is the path updateInputPartitions serves, and it
        // does not exist under eager revocation. Pinned rather than inherited so a Kafka default change
        // cannot quietly retarget this test.
        props.put(StreamsConfig.consumerPrefix("partition.assignment.strategy"),
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        started.add(startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT));
        log.info("Streams instance {} running", instanceTag);
    }

    private static String instanceTagOf(final String value) {
        return value.substring(0, value.indexOf('|'));
    }

    private void produceKeys(final Set<String> into, final int from, final int toExclusive) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = from; i < toExclusive; i++) {
                String key = "key-" + i;
                into.add(key);
                producer.send(new ProducerRecord<>(inputTopic, key, "v" + i));
            }
            producer.flush();
        }
    }

    /**
     * The output topic's end offset per partition, captured between phases so a later reader can be scoped to
     * records produced after this point.
     */
    @SneakyThrows
    private List<Long> outputEndOffsets() {
        // One batched listOffsets rather than one call per partition. Cheaper, but the reason that matters
        // here is consistency: this is the BOUNDARY the handover assertion is scoped to, and four
        // sequentially-fetched offsets are four instants rather than one.
        final Map<TopicPartition, OffsetSpec> query = new LinkedHashMap<>();
        for (TopicPartition partition : outputPartitions()) {
            query.put(partition, OffsetSpec.latest());
        }
        final ListOffsetsResult result = getKcu().getAdmin().listOffsets(query);

        final List<Long> offsets = new ArrayList<>();
        for (TopicPartition partition : outputPartitions()) {
            offsets.add(result.partitionResult(partition).get().offset());
        }
        return offsets;
    }

    private List<TopicPartition> outputPartitions() {
        List<TopicPartition> partitions = new ArrayList<>();
        for (int partition = 0; partition < PARTITIONS; partition++) {
            partitions.add(new TopicPartition(outputTopic, partition));
        }
        return partitions;
    }

    /**
     * Block until the output topic holds at least {@code atLeast} records.
     * <p>
     * <b>One consumer, polled incrementally</b> - deliberately not a `drainAll()` in an Awaitility
     * condition. That shape builds a fresh consumer and re-reads the entire topic from offset zero on every
     * check, and {@link #pollUntilQuiet} demands three consecutive empty polls before it returns, so each
     * check cost a floor of 1.5s plus a re-read that grows as the run proceeds. This is a synchronisation
     * gate, not an assertion - the ledger is still collected by a full {@link #drainAll()} afterwards, so
     * nothing about what is asserted changes.
     */
    private void awaitOutputCount(final int atLeast) {
        final List<ConsumerRecord<String, String>> seen = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            List<TopicPartition> partitions = outputPartitions();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            await().atMost(Duration.ofMinutes(3)).until(() -> {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    seen.add(record);
                }
                return seen.size() >= atLeast;
            });
        }
    }

    /** Every output record, from the beginning - the no-loss and duplicate ledger wants the whole topic. */
    private List<ConsumerRecord<String, String>> drainAll() {
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            List<TopicPartition> partitions = outputPartitions();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            pollUntilQuiet(consumer, out);
        }
        return out;
    }

    /**
     * Only records at or after the captured boundary - {@code assign} plus {@code seek}, never
     * subscribe-from-earliest, so assertion 3 can only be satisfied by output produced after B joined.
     */
    private List<ConsumerRecord<String, String>> drainFrom(final List<Long> fromOffsets) {
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            List<TopicPartition> partitions = outputPartitions();
            consumer.assign(partitions);
            for (int partition = 0; partition < PARTITIONS; partition++) {
                consumer.seek(partitions.get(partition), fromOffsets.get(partition));
            }
            pollUntilQuiet(consumer, out);
        }
        return out;
    }

    private static void pollUntilQuiet(final KafkaConsumer<String, String> consumer,
                                       final List<ConsumerRecord<String, String>> into) {
        int quiet = 0;
        // Three consecutive empty polls, not one: a single empty poll during a rebalance says nothing about
        // whether the topic is drained.
        while (quiet < 3) {
            int before = into.size();
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                into.add(record);
            }
            quiet = into.size() == before ? quiet + 1 : 0;
        }
    }
}
