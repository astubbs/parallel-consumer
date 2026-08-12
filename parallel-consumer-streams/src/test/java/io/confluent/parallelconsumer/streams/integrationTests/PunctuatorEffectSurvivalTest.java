package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import io.confluent.parallelconsumer.streams.PcTaskDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Do a punctuator's own effects survive a crash on the PC path? (astubbs#255)
 *
 * <h2>Why this test exists</h2>
 * Both the KS handover and U13's inflight entry ranked "a punctuator's effects never become
 * commit-covered" as the largest open item. Two earlier branches
 * ({@code feats/ks-streams-punctuator-commit-coverage},
 * {@code feats/ks-streams-postcommit-checkpoint-gap}) measured the commit <em>bookkeeping</em> around that
 * claim and shrank it considerably - offsets commit, {@code postCommit} runs under load. But every one of
 * those measurements was about bookkeeping. Nobody had asked the question the concern is actually about:
 * <b>if a punctuator writes to a store or forwards a record, and the process then dies, is the effect
 * there afterwards?</b>
 *
 * <h2>The first version of this class was wrong, and how it was caught</h2>
 * It punctuated, called {@link PcTaskDispatcher#abortAllActive()}, closed the client, and <em>then</em>
 * read the topics - reporting that the effects survived a crash without any {@code flush()}. Code review
 * found the hole and one probe settled it: set {@code linger.ms} to five minutes, so the producer cannot
 * possibly deliver during an eleven-second run, and <b>the old shape still passed</b>. The effects were
 * being put on the broker by {@code streams.close()} itself, which runs a clean shutdown -
 * {@code prepareCommit()} -> {@code flush()} -> {@code streamsProducer.flush()} -> commit ->
 * {@code producer.close()}, the last of which blocks until every buffered record is sent.
 * <p>
 * Two claims in that version were simply false. There was no "window with no commit in it":
 * {@code StreamThread.lastCommitMs} starts at zero, so Streams commits on its first run-loop iteration
 * rather than 30 seconds in. And nothing reached the broker "without a flush" - the measurement sat
 * downstream of one.
 *
 * <h2>What this class measures now</h2>
 * The read happens <b>before</b> {@code close()}, and a <b>negative control</b> establishes that the
 * measurement point is upstream of any flush: an otherwise identical arm holds the producer back with a
 * five-minute linger and must find <em>nothing</em>. It does. So a non-empty reading in the other arms is
 * asynchronous producer delivery, observed rather than manufactured.
 * <p>
 * Measured: punctuator forwards and store writes reach the broker on the producer's own schedule, before
 * and independently of any commit. That is a real property and it is what the three arms pin.
 *
 * <h2>What this class does NOT measure - read this before citing it</h2>
 * <b>It is not a process-death test, and "survive a crash" overstates it.</b>
 * {@code abortAllActive()} calls {@code workerPool.shutdownNow()} and nothing else: PC's worker pool dies,
 * while the producer, the {@code StreamThread}, the {@code RecordCollector} and the punctuator itself all
 * keep running. The punctuator in particular runs through {@code maybePunctuateSystemTime}, which is
 * byte-for-byte stock and never enters PC dispatch - so the effect path under observation is not
 * PC-specific at all, and markers observed here include punctuations that fired <em>after</em> the abort.
 * <p>
 * Consequently this class does <b>not</b> settle the data-loss reading of "a punctuator's effects never
 * become commit-covered" in the sense of a process actually dying. Answering that needs a real kill - a
 * forked JVM and a SIGKILL - which nothing here does.
 * <p>
 * It also does not touch what U13's inflight entry actually ranked highest, which was
 * <em>re-firing over already-covered event time on rebalance</em> - a duplication question, not a
 * durability one. This class never restarts anything.
 *
 * <h2>Non-EOS</h2>
 * Under exactly-once the forward would sit in an open transaction and a {@code read_committed} consumer
 * would not see it until a commit. EOS is refused by the supported envelope (U11) and out of scope for v6
 * (KTD7, pile E), so that is a boundary rather than a gap - but a future EOS decision inherits the
 * question rather than any answer here.
 *
 * @author Antony Stubbs
 * @see PcTaskDispatcher#abortAllActive()
 */
@Slf4j
// PcDispatchSwitch and PcDispatchCounters are process-wide, and abortAllActive() reaches every live
// dispatcher in the JVM - a concurrent test would be crashed by this one.
@Isolated
class PunctuatorEffectSurvivalTest extends BrokerStreamsIntegrationTest {

    private static final int POOL_SIZE = 4;

    private static final int INPUT_RECORDS = 3;

    private static final String STORE = "punctuator-effect-store";

    private static final String PUNCTUATED_KEY_PREFIX = "punctuated-";

    private static final Duration PUNCTUATE_INTERVAL = Duration.ofMillis(200);

    /**
     * Enough punctuations to be sure the punctuator really ran, and few enough that the crash still lands
     * far inside the 30-second commit interval.
     */
    private static final int PUNCTUATIONS_AWAITED = 3;

    /** Leave {@code linger.ms} at whatever Kafka Streams configures. */
    private static final Integer DEFAULT_LINGER = null;

    /** Longer than any run of this class, so the producer cannot deliver during it. */
    private static final Integer HELD_BACK_LINGER = (int) Duration.ofMinutes(5).toMillis();

    private static final AtomicInteger recordsProcessed = new AtomicInteger();

    private static final AtomicInteger punctuationsFired = new AtomicInteger();

    @BeforeEach
    void resetCounters() {
        recordsProcessed.set(0);
        punctuationsFired.set(0);
        PcDispatchCounters.reset();
    }

    @AfterEach
    void resetSwitch() {
        PcDispatchSwitch.resetToDefault();
    }

    /**
     * The experiment. Punctuate, abort, and read the broker <b>before</b> the client is closed.
     */
    @Test
    void punctuatorEffectsReachTheBrokerBeforeAnyCommitOnThePcPath() {
        PcDispatchSwitch.enable(POOL_SIZE);

        ArmResult result = runArm("pc-punctuator-effects", DEFAULT_LINGER);

        assertThat(result.dispatchedToPool)
                .as("premise: this arm must have gone through the PC dispatch seam")
                .isEqualTo(INPUT_RECORDS);

        assertThat(result.forwardedToOutput)
                .as("every punctuator FORWARD that fired before the abort must already be on the output "
                        + "topic, read before close. Asserted as a COUNT, not merely non-empty: losing 2 "
                        + "of 3 is the realistic shape of a crash catching part of a producer batch, and "
                        + "isNotEmpty() would call that survival.")
                .hasSizeGreaterThanOrEqualTo(result.punctuations);

        assertThat(result.writtenToChangelog)
                .as("and every punctuator STORE WRITE must already be on the changelog topic, same "
                        + "window, same count rule")
                .hasSizeGreaterThanOrEqualTo(result.punctuations);
    }

    /**
     * The negative control, and the arm that makes the one above mean anything.
     * <p>
     * Identical except the producer is given a five-minute {@code linger.ms}, so it cannot possibly have
     * delivered anything within the run. The effects must therefore be <b>absent</b> at the same
     * measurement point. If they show up anyway, something other than asynchronous producer delivery is
     * putting them on the broker before the read - which is exactly the defect that invalidated the first
     * version of this class, where the measurement sat after {@code streams.close()} and its flush.
     */
    @Test
    void withTheProducerHeldBackTheSameMeasurementFindsNothing() {
        PcDispatchSwitch.enable(POOL_SIZE);

        ArmResult result = runArm("pc-linger-negative-control", HELD_BACK_LINGER);

        assertThat(result.forwardedToOutput)
                .as("NEGATIVE CONTROL: with a five-minute linger the producer cannot have sent anything "
                        + "in this run, so the forward must NOT be on the topic at the point we measure. "
                        + "A non-empty reading here means the measurement is downstream of a flush and "
                        + "the sibling arm's result is manufactured, not observed.")
                .isEmpty();

        assertThat(result.writtenToChangelog)
                .as("NEGATIVE CONTROL: and neither must the store write")
                .isEmpty();
    }

    /**
     * The instrument check: seam off, same protocol. Proves the fixture looks where the effects actually
     * land, so that an empty reading elsewhere means loss rather than a mis-aimed reader.
     */
    @Test
    void theFixtureCanObservePunctuatorEffectsAtAll() {
        PcDispatchSwitch.disable();

        ArmResult result = runArm("stock-punctuator-effects", DEFAULT_LINGER);

        assertThat(result.dispatchedToPool)
                .as("this is the stock arm and must not have dispatched through the seam")
                .isZero();

        assertThat(result.forwardedToOutput)
                .as("INSTRUMENT CHECK: a punctuator forward must be visible on the output topic, or "
                        + "every other arm's reading is uninterpretable")
                .isNotEmpty();

        assertThat(result.writtenToChangelog)
                .as("INSTRUMENT CHECK: and a punctuator store write must be visible on the changelog")
                .isNotEmpty();
    }

    /**
     * @param lingerMs {@link #DEFAULT_LINGER} to leave the producer alone, or a value to hold it back
     */
    private ArmResult runArm(final String name, final Integer lingerMs) {
        String inputTopic = setupTopic(name + "-in");
        String outputTopic = setupTopic(name + "-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);
        String appId = name + "-" + System.nanoTime();

        produceInput(inputTopic);

        KafkaStreams streams = startTopology(appId, inputTopic, outputTopic, lingerMs);
        int punctuations;
        long dispatched;
        List<String> forwarded;
        List<String> changelog;
        String changelogTopic = appId + "-" + STORE + "-changelog";
        try {
            await().atMost(Duration.ofSeconds(60))
                    .until(() -> recordsProcessed.get() >= INPUT_RECORDS
                            && punctuationsFired.get() >= PUNCTUATIONS_AWAITED);

            punctuations = punctuationsFired.get();
            dispatched = PcDispatchCounters.getRecordsDispatchedToPool();
            log.info("=== [{}] {} punctuations fired, {} dispatched to pool - crashing now",
                    name, punctuations, dispatched);

            PcTaskDispatcher.abortAllActive();

            // READ BEFORE CLOSE. This is the whole correction: an earlier version read after
            // streams.close(), and close runs a CLEAN shutdown - prepareCommit -> flush() ->
            // streamsProducer.flush() -> commit -> producer.close(), the last of which blocks until every
            // buffered record is sent. So it delivered the effects itself and the test reported survival
            // no matter what. Proven by setting linger.ms to five minutes: the old shape still passed.
            Map<String, List<String>> drained = drainAll(outputTopic, changelogTopic);
            forwarded = punctuatedOnly(drained.get(outputTopic));
            changelog = punctuatedOnly(drained.get(changelogTopic));
            log.info("=== [{}] BEFORE CLOSE - output topic: {}, changelog punctuator records: {}",
                    name, forwarded, changelog.size());
        } finally {
            // Cleanup only. Anything it flushes lands after the measurement above.
            streams.close(Duration.ofSeconds(30));
        }

        return new ArmResult(forwarded, changelog, punctuations, dispatched);
    }

    /**
     * Keeps only the punctuator's own records. The input records flow through the same topology, so an
     * assertion on the raw topic contents would be satisfied by ordinary processing rather than by
     * anything the punctuator did.
     */
    private static List<String> punctuatedOnly(final List<String> values) {
        List<String> punctuated = new ArrayList<>();
        for (String value : values) {
            if (value != null && value.startsWith(PUNCTUATED_KEY_PREFIX)) {
                punctuated.add(value);
            }
        }
        return punctuated;
    }

    /**
     * Every record currently on each topic, read from the beginning with one fresh group across all of
     * them and bucketed by topic.
     * <p>
     * One subscription rather than one per topic: each drain pays the whole fixed poll budget, so a
     * second sequential call doubled the dominant cost of this class for no extra information.
     */
    private Map<String, List<String>> drainAll(final String... topics) {
        Map<String, List<String>> byTopic = new HashMap<>();
        for (String topic : topics) {
            byTopic.put(topic, new ArrayList<>());
        }
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.subscribe(Arrays.asList(topics));
            // Poll a fixed number of times rather than awaiting a count: the whole question is whether
            // anything is there, so a condition on "at least one" would hang for its full timeout on the
            // very result this test exists to detect.
            for (int poll = 0; poll < 10; poll++) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    byTopic.get(record.topic()).add(record.value());
                }
            }
        }
        return byTopic;
    }

    /**
     * Hand-rolled rather than {@code KafkaClientUtils.produceMessages}, which generates exactly this
     * key/value scheme and would otherwise be the right reuse. It reaches {@code ModelUtils(new
     * PCModuleTestEnv())} and so needs {@code org.threeten.extra.MutableClock}, a core test-scoped
     * dependency that is not on this module's test classpath - the swap fails with
     * {@code NoClassDefFoundError} at runtime. Using it would mean widening this module's test
     * dependencies to save six lines.
     */
    private void produceInput(final String inputTopic) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < INPUT_RECORDS; i++) {
                producer.send(new ProducerRecord<>(inputTopic, "key-" + i, "value-" + i));
            }
            producer.flush();
        }
    }

    private KafkaStreams startTopology(final String appId,
                                       final String inputTopic,
                                       final String outputTopic,
                                       final Integer lingerMs) {
        StoreBuilder<KeyValueStore<String, String>> store = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE), Serdes.String(), Serdes.String())
                .withLoggingEnabled(Collections.emptyMap())
                // Caching off, so a punctuator's put reaches the changelog when it happens rather than at
                // some later flush - the flush being exactly what this test removes.
                .withCachingDisabled();

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(store);
        builder.<String, String>stream(inputTopic)
                .process((ProcessorSupplier<String, String, String, String>) () ->
                        new Processor<String, String, String, String>() {

                            private ProcessorContext<String, String> context;

                            private KeyValueStore<String, String> kvStore;

                            @Override
                            public void init(final ProcessorContext<String, String> context) {
                                this.context = context;
                                this.kvStore = context.getStateStore(STORE);
                                // WALL_CLOCK_TIME: maybePunctuateSystemTime is byte-for-byte stock in the
                                // patched file and carries no warning, so it is the silent case.
                                context.schedule(PUNCTUATE_INTERVAL, PunctuationType.WALL_CLOCK_TIME,
                                        timestamp -> {
                                            String marker = PUNCTUATED_KEY_PREFIX
                                                    + punctuationsFired.incrementAndGet();
                                            // The two effects under test, in one punctuation.
                                            kvStore.put(marker, marker);
                                            this.context.forward(new Record<>(marker, marker, timestamp));
                                        });
                            }

                            @Override
                            public void process(final Record<String, String> record) {
                                recordsProcessed.incrementAndGet();
                            }
                        }, STORE)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        Properties props = baseStreamsProps(appId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        // Commit interval left at Kafka's default. Note this does NOT mean "no commit for 30s": Streams'
        // lastCommitMs starts at zero, so its first commit fires on the first run-loop iteration. The
        // measurement no longer depends on that either way - it is taken before close, and the negative
        // control below is what establishes that a reading means asynchronous delivery.
        if (lingerMs != null) {
            props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), lingerMs);
        }
        return startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT);
    }

    /** What one arm observed. Plain fields: the Java 8 release target rules out a record. */
    private static final class ArmResult {

        private final List<String> forwardedToOutput;

        private final List<String> writtenToChangelog;

        private final int punctuations;

        private final long dispatchedToPool;

        private ArmResult(final List<String> forwardedToOutput,
                          final List<String> writtenToChangelog,
                          final int punctuations,
                          final long dispatchedToPool) {
            this.forwardedToOutput = forwardedToOutput;
            this.writtenToChangelog = writtenToChangelog;
            this.punctuations = punctuations;
            this.dispatchedToPool = dispatchedToPool;
        }
    }
}
