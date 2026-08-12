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
 * <h2>The design, and the one thing that makes it sharp</h2>
 * The commit interval is left at Kafka's 30-second default and the crash lands roughly 600ms after the
 * punctuator starts firing. <b>No commit can occur in that window.</b> So anything found on the broker
 * afterwards got there without any {@code flush()} - which is precisely the mechanism the original concern
 * assumed was load-bearing.
 * <p>
 * The crash is {@link PcTaskDispatcher#abortAllActive()} - the same real abort
 * {@code CommitFrontierCrashRestartTest} uses. No drain, no completion feed-back, no final commit.
 * <p>
 * Two observables, both read directly out of Kafka rather than inferred:
 * <ul>
 *   <li><b>the output topic</b>, for what the punctuator {@code forward}ed;</li>
 *   <li><b>the store's changelog topic</b>, for what the punctuator {@code put}. Reading the changelog
 *       directly avoids needing interactive queries or a restart-and-probe topology to see whether a store
 *       write was durable.</li>
 * </ul>
 *
 * <h2>The stock arm is an instrument check, not a crash control - read it that way</h2>
 * {@code abortAllActive()} only reaches PC dispatchers, so there is no way to give a stock topology the
 * same in-process crash; the stock arm closes cleanly. It therefore proves only that <b>this fixture can
 * observe punctuator effects on both topics at all</b>. Without it, an empty PC reading would be
 * indistinguishable from a test looking in the wrong place. It is deliberately not claimed as evidence
 * about what stock does under a crash.
 * <p>
 * A crash result needs no comparison arm to interpret anyway: effects present is objectively fine, effects
 * missing is objectively data loss.
 *
 * <h2>The answer</h2>
 * <b>They survive.</b> Three punctuations, hard abort, no commit in the window: all three forwards were on
 * the output topic and all three store writes were on the changelog. The producer carries a punctuator's
 * effects to the broker on its own schedule; nothing about them depends on the {@code flush()} that
 * {@code prepareCommit} would have performed. So the data-loss reading of "a punctuator's effects never
 * become commit-covered" does not hold for either effect.
 * <p>
 * What is left of that concern after this run is not data loss: WALL_CLOCK_TIME punctuators fire unwarned
 * where STREAM_TIME logs, and the idle-window checkpoint tail measured on
 * {@code feats/ks-streams-postcommit-checkpoint-gap} costs a little extra changelog replay after a crash.
 *
 * <h2>The caveat that would change this, and why it is out of scope</h2>
 * <b>This is a non-EOS run.</b> Under exactly-once the forward would sit in an open transaction and a
 * {@code read_committed} consumer would not see it until a commit that, on this path, may not come - which
 * is the one configuration where the original concern would bite for real. EOS is refused by the supported
 * envelope (U11) and is out of scope for v6 (KTD7, pile E), so that is a boundary rather than a gap. It is
 * named here because a future EOS decision inherits this question rather than this answer.
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
     * The experiment. Punctuate, crash hard before any commit can happen, then look on the broker for what
     * the punctuator did.
     */
    @Test
    void punctuatorEffectsSurviveACrashOnThePcPath() {
        PcDispatchSwitch.enable(POOL_SIZE);

        ArmResult result = runArm("pc-punctuator-effects", true);

        assertThat(result.dispatchedToPool)
                .as("premise: this arm must have gone through the PC dispatch seam, or it is measuring "
                        + "stock and says nothing about the PC path")
                .isEqualTo(INPUT_RECORDS);

        assertThat(result.forwardedToOutput)
                .as("A punctuator FORWARD, after a hard abort with no drain, no final commit, and no "
                        + "commit possible in the window at all (30s interval, crash at ~600ms). If this "
                        + "is empty the forward was lost with the process - real data loss, and the "
                        + "original framing of defect 1 is right after all. If it is present, the "
                        + "producer carried it without any flush() and the concern does not apply to "
                        + "forwards.")
                .isNotEmpty();

        assertThat(result.writtenToChangelog)
                .as("and the punctuator's STORE WRITE, read straight off the changelog topic. Same "
                        + "window, same crash. Empty here means the store mutation died with the process "
                        + "and would not be restored.")
                .isNotEmpty();

        log.info("=== PUNCTUATOR EFFECTS AFTER CRASH: {} forwarded, {} changelog records, from {} "
                        + "punctuations", result.forwardedToOutput.size(),
                result.writtenToChangelog.size(), result.punctuations);
    }

    /**
     * The instrument check. Same topology, seam off, clean close: proves the fixture can see punctuator
     * effects on both topics. Not a crash control - see the class javadoc.
     */
    @Test
    void theFixtureCanObservePunctuatorEffectsAtAll() {
        PcDispatchSwitch.disable();

        ArmResult result = runArm("stock-punctuator-effects", false);

        assertThat(result.forwardedToOutput)
                .as("INSTRUMENT CHECK: a punctuator forward must be visible on the output topic for this "
                        + "fixture. If this is empty, the PC arm's readings are uninterpretable because "
                        + "the test is not looking where the effects land.")
                .isNotEmpty();

        assertThat(result.writtenToChangelog)
                .as("INSTRUMENT CHECK: and a punctuator store write must be visible on the changelog "
                        + "topic")
                .isNotEmpty();
    }

    /**
     * @param crash whether to abort the dispatchers instead of closing cleanly
     */
    private ArmResult runArm(final String name, final boolean crash) {
        String inputTopic = setupTopic(name + "-in");
        String outputTopic = setupTopic(name + "-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);
        String appId = name + "-" + System.nanoTime();

        produceInput(inputTopic);

        KafkaStreams streams = startTopology(appId, inputTopic, outputTopic);
        int punctuations;
        long dispatched;
        try {
            await().atMost(Duration.ofSeconds(60))
                    .until(() -> recordsProcessed.get() >= INPUT_RECORDS
                            && punctuationsFired.get() >= PUNCTUATIONS_AWAITED);

            punctuations = punctuationsFired.get();
            dispatched = PcDispatchCounters.getRecordsDispatchedToPool();
            log.info("=== [{}] {} punctuations fired, {} dispatched to pool - crashing now",
                    name, punctuations, dispatched);

            if (crash) {
                // A crash, not a shutdown: no drain, no completion feed-back, no final commit.
                PcTaskDispatcher.abortAllActive();
            }
        } finally {
            streams.close(Duration.ofSeconds(30));
        }

        // Read AFTER the client is gone, so nothing further can be produced while the reader runs. Both
        // topics come off ONE subscription: two sequential drains each paid the full fixed poll budget,
        // which was most of this class's runtime.
        String changelogTopic = appId + "-" + STORE + "-changelog";
        Map<String, List<String>> drained = drainAll(outputTopic, changelogTopic);
        List<String> forwarded = drained.get(outputTopic);
        List<String> changelog = drained.get(changelogTopic);
        log.info("=== [{}] output topic: {}", name, forwarded);
        log.info("=== [{}] changelog records: {}", name, changelog.size());

        return new ArmResult(punctuatedOnly(forwarded), punctuatedOnly(changelog), punctuations, dispatched);
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
                                       final String outputTopic) {
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
        // Deliberately NOT shortened. Kafka's 30s default means no commit can land between the first
        // punctuation and the crash ~600ms later, so anything on the broker afterwards got there without
        // a flush.
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
