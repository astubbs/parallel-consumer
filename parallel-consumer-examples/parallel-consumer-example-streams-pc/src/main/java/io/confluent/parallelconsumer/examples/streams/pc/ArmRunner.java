package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs one arm of a comparison end to end: produce, start the topology, wait, measure, tear down.
 * <p>
 * <b>The two arms differ in one deliberate term, and it is this class's job to keep that true.</b>
 * Everything the runner controls - the broker, the JVM, the patched classes, the topology, the record set
 * - is held identical, so a difference in the numbers has one place it could have come from. A comparison
 * against a separately-built stock project would vary the JVM and the broker state as well, and nothing
 * measured could then be attributed to the seam.
 * <p>
 * <b>One term is not controlled, and is named here rather than claimed away:</b> ORDER. The stock arm
 * always runs first, so the PC arm meets a warmer JVM and a warmer broker. That bias favours PC, and what
 * bounds it is the single-key control - the same ordering there produces PC LOSING, which it could not do
 * if warm-up were carrying the headline result.
 *
 * @author Antony Stubbs
 */
final class ArmRunner {

    /*
     * The five constants below are lifted unchanged from HeadOfLineBlockingBenchmarkTest in
     * parallel-consumer-streams, which chose them deliberately, so the demo and the regression test
     * measure the same workload. They are duplicated rather than shared because a src/main module cannot
     * import another module's test classes. Change one set and check the other; nothing else will.
     */

    /**
     * Worker threads per task, and simultaneously PC's max concurrency. Four is enough that the fast records
     * queue rather than trivially fitting, which is what makes the measurement about dispatch rather than
     * about pool size.
     */
    static final int POOL_SIZE = 4;

    /**
     * The blocker's cost. Deliberately large relative to {@link #FAST_COST}, so a null result cannot be
     * mistaken for a small one.
     */
    static final Duration SLOW_COST = Duration.ofMillis(1500);

    static final Duration FAST_COST = Duration.ofMillis(25);

    static final String SLOW_KEY = "key-slow";

    /** Identifies the blocker independently of its key, which the single-key control shares. */
    static final String BLOCKER_VALUE = "blocker";

    /** Enough to exceed the pool several times over, and enough for a percentile to mean something. */
    static final int FAST_RECORDS = 24;

    static final int TOTAL_RECORDS = FAST_RECORDS + 1;

    private static final Duration ARM_TIMEOUT = Duration.ofSeconds(180);

    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(60);

    /** Makes each arm's topics unique without depending on the clock's resolution. */
    private static final AtomicInteger TOPIC_SEQUENCE = new AtomicInteger();

    /**
     * Makes the topics unique across RUNS as well as across arms.
     * <p>
     * The sequence above restarts at zero in every JVM, so two runs against the same broker would ask for
     * the same topic names, and the second would abort on a topic that already exists. That is reachable
     * whenever the reader has opted into container reuse - exactly the path the README recommends.
     */
    private static final long RUN_ID = System.nanoTime();

    private ArmRunner() {
    }

    /**
     * @param pcDispatch whether the seam is on. Stated explicitly in BOTH arms - the switch defaults to on,
     *                   so a stock arm that merely omitted the call would not be a stock arm at all
     * @param allOneKey  put the fast records on the blocker's key, removing the key concurrency that PC
     *                   dispatch depends on. This is what makes an arm a negative control
     */
    static ArmResult runArm(final DemoBroker broker,
                            final String armName,
                            final boolean pcDispatch,
                            final boolean allOneKey) {
        if (pcDispatch) {
            PcDispatchSwitch.enable(POOL_SIZE);
        } else {
            PcDispatchSwitch.disable();
        }
        // Checked rather than assumed. If this ever disagreed, the arm would silently measure the other path
        // and the comparison would be of one thing against itself.
        if (PcDispatchSwitch.isEnabled() != pcDispatch) {
            throw new IllegalStateException("Arm " + armName + " asked for dispatch=" + pcDispatch
                    + " but PcDispatchSwitch reports " + PcDispatchSwitch.isEnabled());
        }

        // Counters are process-wide and never reset themselves, so without this the second arm would report
        // the first arm's totals as its own.
        PcDispatchCounters.reset();

        int sequence = TOPIC_SEQUENCE.incrementAndGet();
        String inputTopic = armName + "-in-" + sequence + "-" + RUN_ID;
        String outputTopic = armName + "-out-" + sequence + "-" + RUN_ID;
        // ONE partition, deliberately: stock Kafka Streams' only concurrency is across partitions, and more
        // than one would hand the control arm the very parallelism this comparison says it lacks.
        broker.createTopics(1, inputTopic, outputTopic);

        CompletionTimer timer = new CompletionTimer();
        produceRecords(broker, inputTopic, allOneKey);

        KafkaStreams streams = startTopology(broker, armName, inputTopic, outputTopic, timer, sequence);
        boolean closedCleanly;
        try {
            awaitCompletion(timer, armName);
        } finally {
            closedCleanly = streams.close(Duration.ofSeconds(30));
        }
        // The counters this arm is about to read are process-wide, and the NEXT arm resets them. A close
        // that timed out leaves this arm's StreamThread and dispatcher alive to increment them across that
        // boundary, so the following arm would attribute this arm's work to itself.
        if (!closedCleanly) {
            throw new IllegalStateException("Arm " + armName + " did not shut down within 30s. Its threads "
                    + "would still be running during the next arm, so the next arm's counters could not be "
                    + "trusted.");
        }

        List<Long> fastLatencies = timer.fastRecordLatencies();
        if (fastLatencies.size() != FAST_RECORDS) {
            throw new IllegalStateException("Arm " + armName + " timed " + fastLatencies.size()
                    + " fast records but expected " + FAST_RECORDS
                    + ". The distribution would be of something other than what this claims to measure.");
        }

        return new ArmResult(new Latencies(fastLatencies),
                timer.totalDrainMillis(),
                PcDispatchCounters.getRecordsOfferedToWorkManager(),
                PcDispatchCounters.getRecordsAcceptedByWorkManager(),
                PcDispatchCounters.getRecordsDispatchedToPool(),
                PcDispatchCounters.getRecordsCompletedSuccessfully(),
                PcDispatchCounters.getRecordsFailed());
    }

    /**
     * Everything is produced <em>before</em> the topology starts, so both arms begin with the partition
     * already full. That is what makes this a head-of-line scenario: the blocker is genuinely at the head of
     * a queue, rather than racing the producer for position.
     */
    private static void produceRecords(final DemoBroker broker, final String inputTopic,
                                       final boolean allOneKey) {
        try (KafkaProducer<String, String> producer = broker.createProducer()) {
            producer.send(new ProducerRecord<>(inputTopic, SLOW_KEY, BLOCKER_VALUE));
            for (int i = 0; i < FAST_RECORDS; i++) {
                String key = allOneKey ? SLOW_KEY : "key-fast-" + i;
                producer.send(new ProducerRecord<>(inputTopic, key, "fast-" + i));
            }
            producer.flush();
        }
    }

    private static KafkaStreams startTopology(final DemoBroker broker,
                                              final String armName,
                                              final String inputTopic,
                                              final String outputTopic,
                                              final CompletionTimer timer,
                                              final int sequence) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);

        // NOTE FOR THE READER: this is ordinary Kafka Streams. There is no Parallel Consumer API in this
        // topology, and there is none anywhere else in this module either. Taking the dependency is the
        // whole integration - your topology code does not change.
        stream.mapValues((key, value) -> {
            timer.markStarted();
            // A BLOCK, not a spin. The motivating workload is a call to a slow web service; a spin would
            // compete for cores with the other workers and measure the scheduler instead of the seam.
            //
            // Cost is chosen by VALUE, not by key. Keying it on SLOW_KEY would make the single-key control
            // vary two terms at once - every record there carries that key, so every record would become a
            // 1500ms record, and the control would be measuring a different workload as well as a different
            // cardinality.
            sleep(BLOCKER_VALUE.equals(value) ? SLOW_COST : FAST_COST);
            timer.markCompleted(key, value);
            return value;
        }).to(outputTopic);

        Properties props = new Properties();
        // A fresh application id per arm. Reusing one would resume from the previous arm's committed
        // offsets, and the second arm would sit there processing nothing.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, armName + "-" + sequence + "-" + System.nanoTime());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        // One StreamThread, so the only concurrency available to the PC arm is the one this module adds.
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        // Built AFTER the switch is set, and fresh for every arm. The dispatch decision is taken once, in
        // the StreamTask constructor, so a client constructed while the switch was off keeps the stock path
        // for its whole life however the switch moves afterwards.
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        try {
            streams.start();
            awaitRunning(streams, armName);
        } catch (RuntimeException e) {
            // Closed before rethrowing, because a KafkaStreams that never reached RUNNING still owns
            // non-daemon StreamThreads. exec:java joins non-daemon threads before it returns, so leaking
            // one turns "the arm failed to start" into a build that hangs forever with the reason already
            // scrolled off screen. The reader has to SEE the failure for the abort to be worth anything.
            streams.close(Duration.ofSeconds(30));
            throw e;
        }
        return streams;
    }

    private static void awaitRunning(final KafkaStreams streams, final String armName) {
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (streams.state() != KafkaStreams.State.RUNNING) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Arm " + armName + ": Kafka Streams never reached RUNNING, "
                        + "last state was " + streams.state());
            }
            sleep(Duration.ofMillis(50));
        }
    }

    private static void awaitCompletion(final CompletionTimer timer, final String armName) {
        long deadline = System.nanoTime() + ARM_TIMEOUT.toNanos();
        while (timer.completed() < TOTAL_RECORDS) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Arm " + armName + " processed only " + timer.completed()
                        + " of " + TOTAL_RECORDS + " records before timing out");
            }
            sleep(Duration.ofMillis(25));
        }
    }

    private static void sleep(final Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }
}
