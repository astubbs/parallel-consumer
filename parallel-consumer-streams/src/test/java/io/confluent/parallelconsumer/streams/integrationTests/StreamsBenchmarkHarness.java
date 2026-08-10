package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import io.confluent.parallelconsumer.streams.benchmark.BenchmarkWorkload;
import io.confluent.parallelconsumer.streams.benchmark.GeneratedRecord;
import io.confluent.parallelconsumer.streams.benchmark.LatencyDistribution;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import pl.tlinkowski.unij.api.UniMaps;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The machinery every benchmark in this module's second suite shares: how one arm is run, how its work is
 * simulated, and how its completion is decided.
 * <p>
 * <b>One generated list, replayed into both arms.</b> The caller generates a {@link BenchmarkWorkload} once and
 * hands the same {@code List<GeneratedRecord>} to each arm, so the arms differ in {@link PcDispatchSwitch} and
 * in nothing else - not keys, not payloads, not per-record cost, not arrival times. Generating per arm would
 * reintroduce exactly the two-term comparison this module has already been bitten by.
 *
 * <h2>Two definitions of "drained", and the second cannot be faked</h2>
 * The clock stops on <em>in-process completions</em> reaching the record count, which is the property under
 * test. Independently, the output topic's end offset must have advanced by exactly that many, read from the
 * broker with the shared admin client. The second exists because of a defect already found in this module:
 * {@code CommitFrontierCrashRestartTest} records that an earliest-reading consumer re-reads output from before
 * the phase under test, so a count assertion can be satisfied by records the phase never produced. Reading the
 * end offset rather than the records removes that failure mode by construction rather than by remembering to
 * seek.
 *
 * <h2>Blocking work is a sleep; CPU work is a real spin - and that is not the usual rule</h2>
 * {@code HeadOfLineBlockingBenchmarkTest} says a spin "would compete for cores with the other workers -
 * measuring the scheduler instead of the seam", and for its purposes that is right. Here the profile axis
 * sweeps deliberately from blocking to CPU-bound, and for the CPU end that competition <em>is</em> the
 * measurement: the question is precisely whether a worker pool helps when the threads are not blocked. So both
 * fixtures exist and the workload's blocking fraction chooses between them per record.
 *
 * @author Antony Stubbs
 * @see BacklogCatchUpBenchmarkTest
 * @see WorkloadMatrixBenchmarkTest
 * @see PaymentAuthorisationBenchmarkTest
 */
@Slf4j
abstract class StreamsBenchmarkHarness extends BrokerStreamsIntegrationTest {

    /**
     * Workers per task, and simultaneously Parallel Consumer's max concurrency. Matches the pool size the
     * existing benchmark and the proof tests use, so figures from the two suites are comparable.
     */
    static final int POOL_SIZE = 4;

    /**
     * Generous, because a deep backlog at a realistic per-record cost is genuinely slow, and because a
     * benchmark that times out mid-drain reports nothing at all. Bounded, so a topology that never makes
     * progress fails here rather than hanging the suite.
     */
    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(6);

    /**
     * Bound on a single admin metadata call, and how many times to retry it. Broker contention during a long
     * benchmark is expected and is not a measurement failure - it must not be allowed to end a run that has
     * already produced its numbers.
     */
    private static final int ADMIN_TIMEOUT_SECONDS = 30;

    private static final int ADMIN_ATTEMPTS = 3;

    /**
     * Shared, and thread-safe for reads - the processor parses on every worker thread at once.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @AfterEach
    void restoreDefaultDispatch() {
        PcDispatchSwitch.resetToDefault();
    }

    /**
     * Runs one arm end to end.
     *
     * @param arm      names the arm in every log line and in the result
     * @param workload the description, used for its metadata; the records are passed separately so both arms
     *                 provably replay one list
     * @param records  the generated records, identical between arms
     * @param seamOn   whether Parallel Consumer's dispatch is on. Set explicitly in <b>both</b> arms: the seam
     *                 defaults to on, so a stock arm that merely omitted this would not be a stock arm
     */
    ArmResult runArm(final String arm,
                     final BenchmarkWorkload workload,
                     final List<GeneratedRecord> records,
                     final boolean seamOn) {
        return runArm(arm, workload, records, seamOn, 1, 1);
    }

    /**
     * @param partitions    input partitions. One by default: stock Kafka Streams' only concurrency is per
     *                      partition, so more than one hands the control arm the very parallelism the
     *                      experiment says it lacks. Parameterised so the "just add partitions" counter-proposal
     *                      can actually be run rather than argued about
     * @param streamThreads StreamThreads. One by default, for the same reason
     */
    ArmResult runArm(final String arm,
                     final BenchmarkWorkload workload,
                     final List<GeneratedRecord> records,
                     final boolean seamOn,
                     final int partitions,
                     final int streamThreads) {
        if (seamOn) {
            PcDispatchSwitch.enable(POOL_SIZE);
        } else {
            PcDispatchSwitch.disable();
        }
        assertThat(PcDispatchSwitch.isEnabled())
                .as("%s arm must run with the seam %s - an arm that is only a control by default stops being "
                        + "one the moment the default moves", arm, seamOn ? "ON" : "OFF")
                .isEqualTo(seamOn);
        PcDispatchCounters.reset();

        log.info("=== {} | seam {} | wake-on-work {} | pool {} | partitions {} | threads {} | {}",
                arm, seamOn ? "ON" : "OFF", PcDispatchSwitch.isWakeOnWorkEnabled() ? "ON" : "OFF",
                POOL_SIZE, partitions, streamThreads, workload);

        // Created directly rather than through setupTopic, which uses the base class's own partition count -
        // a package-private field this module cannot set. Calling ensureTopic(topic, 4) AFTER setupTopic does
        // not fix it either: KafkaClientUtils.createTopic tolerates TopicExistsException, so the second call
        // is a silent no-op and the topic keeps its single partition. That left a "four partition" arm running
        // on one, and the only symptom was a thirty-second admin timeout when the end-offset read asked about
        // partitions that did not exist.
        String inputTopic = arm + "-in-" + System.nanoTime();
        String outputTopic = arm + "-out-" + System.nanoTime();
        ensureTopic(inputTopic, partitions);
        ensureTopic(outputTopic, partitions);
        assertPartitionCount(inputTopic, partitions);
        assertPartitionCount(outputTopic, partitions);

        long outputOffsetsBefore = endOffsetSum(outputTopic, partitions);

        CompletionLedger ledger = new CompletionLedger(records.size());
        KafkaStreams streams;
        long startedRunningNanos;

        if (workload.isBacklog()) {
            // The whole point of a cold start: the partition is genuinely full before anything is running, so
            // no record is ever waiting on the broker and the measurement is pure processing concurrency.
            produceAll(inputTopic, records, ledger);
            assertThat(endOffsetSum(inputTopic, partitions))
                    .as("the backlog must actually be on the broker before the topology starts, or the arm is "
                            + "racing the producer and measuring arrival rate after all")
                    .isEqualTo(records.size());
            streams = startTopology(arm, inputTopic, outputTopic, ledger, streamThreads);
            startedRunningNanos = System.nanoTime();
        } else {
            streams = startTopology(arm, inputTopic, outputTopic, ledger, streamThreads);
            startedRunningNanos = System.nanoTime();
            produceAtRate(inputTopic, records, ledger);
        }

        try {
            await().atMost(DRAIN_TIMEOUT).until(() -> ledger.completed() >= records.size());
        } finally {
            streams.close(Duration.ofSeconds(60));
        }

        long timeToDrainMillis = (ledger.lastCompletionNanos() - startedRunningNanos) / 1_000_000L;
        double sustainedRate = ArmResult.sustainedRate(ledger.completionNanos());
        long outputDelta = endOffsetSum(outputTopic, partitions) - outputOffsetsBefore;

        ArmResult result = new ArmResult(arm, seamOn, records.size(),
                BenchmarkWorkload.distinctKeys(records).size(), timeToDrainMillis, sustainedRate,
                new LatencyDistribution(arm, ledger.inChainLatencyMillis()),
                new LatencyDistribution(arm + "-e2e", ledger.endToEndLatencyMillis()),
                PcDispatchCounters.getRecordsDispatchedToPool(),
                PcDispatchCounters.getSplitPollWaits(),
                PcDispatchCounters.getWakesOnWork(),
                outputDelta);

        assertMechanismMarkersAgreeWithTheArm(result);
        log.info("=== {}", result);
        return result;
    }

    /**
     * The instrumentation-reached-the-run check, applied to the seam itself.
     * <p>
     * A seam-on arm that dispatched nothing took the stock path and its numbers are somebody else's; a seam-off
     * arm that dispatched anything was never a control. Either way the comparison is void, and it is far
     * cheaper to fail here than to publish the figure.
     */
    private static void assertMechanismMarkersAgreeWithTheArm(final ArmResult result) {
        if (result.isSeamOn()) {
            assertThat(result.getRecordsDispatchedToPool())
                    .as("%s ran with the seam ON but dispatched nothing to the worker pool - the records took "
                            + "the stock path and this arm measured stock Kafka Streams", result.getArm())
                    .isEqualTo(result.getRecordCount());
        } else {
            assertThat(result.getRecordsDispatchedToPool())
                    .as("%s is the control arm and must have dispatched nothing to the worker pool", result.getArm())
                    .isZero();
        }
        assertThat(result.getOutputEndOffsetDelta())
                .as("%s: the broker's own bookkeeping must agree with the in-process count. A disagreement means "
                        + "either records were emitted that nothing completed, or completions were counted for "
                        + "records that never reached the output topic", result.getArm())
                .isEqualTo(result.getRecordCount());
    }

    /**
     * Sum of every partition's end offset. Read with the shared {@code AdminClient} the way the rest of the
     * suite does - a whole {@code KafkaConsumer} for a stateless metadata call is construction cost for
     * nothing, and a consumer would also need a group, which is one more thing to get wrong.
     */
    /**
     * The topic really does have the partitions this arm asked for.
     * <p>
     * Cheap, and it converts a whole class of silent misconfiguration into an immediate, readable failure. A
     * multi-partition arm that quietly ran on one partition would not be the experiment it claims to be, and
     * the way that first surfaced - a thirty-second admin timeout deep inside an end-offset read - said
     * nothing at all about the cause.
     */
    @SneakyThrows
    private void assertPartitionCount(final String topic, final int expected) {
        int actual = getKcu().getAdmin()
                .describeTopics(Collections.singletonList(topic))
                .allTopicNames().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(topic).partitions().size();
        assertThat(actual)
                .as("%s must have the %d partitions this arm was configured with. A mismatch means the arm is "
                        + "measuring a different topology than the one it reports", topic, expected)
                .isEqualTo(expected);
    }

    @SneakyThrows
    private long endOffsetSum(final String topic, final int partitions) {
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        for (int partition = 0; partition < partitions; partition++) {
            request.put(new TopicPartition(topic, partition), OffsetSpec.latest());
        }

        // One call for every partition rather than one per partition, and bounded rather than open-ended.
        // A per-partition loop with an unbounded get() timed out here after a long benchmark had created
        // dozens of topics: the shared AdminClient's metadata refresh contends with the broker while the
        // arms are hammering it, and an unbounded get turns that into a test error rather than a retry.
        for (int attempt = 1; attempt <= ADMIN_ATTEMPTS; attempt++) {
            try {
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                        getKcu().getAdmin().listOffsets(request).all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                long total = 0L;
                for (ListOffsetsResult.ListOffsetsResultInfo info : offsets.values()) {
                    total += info.offset();
                }
                return total;
            } catch (ExecutionException | TimeoutException e) {
                if (attempt == ADMIN_ATTEMPTS) {
                    throw e;
                }
                log.warn("End-offset lookup for {} failed on attempt {} of {} - retrying. This is broker "
                        + "contention, not a measurement: the arm's own numbers are unaffected.",
                        topic, attempt, ADMIN_ATTEMPTS, e);
            }
        }
        throw new IllegalStateException("unreachable - the loop either returns or rethrows");
    }

    private void produceAll(final String inputTopic,
                            final List<GeneratedRecord> records,
                            final CompletionLedger ledger) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (GeneratedRecord record : records) {
                ledger.markSent(record.getIndex());
                producer.send(new ProducerRecord<>(inputTopic, record.getKey(), record.getValue()));
            }
            producer.flush();
        }
        log.info("Pre-loaded a backlog of {} records into {}", records.size(), inputTopic);
    }

    /**
     * Sends on the workload's generated arrival schedule. Deadline-based rather than sleep-per-gap, so a slow
     * send cannot make the whole stream drift later and quietly turn the offered rate into something else.
     */
    private void produceAtRate(final String inputTopic,
                               final List<GeneratedRecord> records,
                               final CompletionLedger ledger) {
        long startNanos = System.nanoTime();
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (GeneratedRecord record : records) {
                long dueNanos = startNanos + record.getArrivalOffsetNanos();
                long waitNanos = dueNanos - System.nanoTime();
                if (waitNanos > 0L) {
                    java.util.concurrent.locks.LockSupport.parkNanos(waitNanos);
                }
                ledger.markSent(record.getIndex());
                producer.send(new ProducerRecord<>(inputTopic, record.getKey(), record.getValue()));
            }
            producer.flush();
        }
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
        log.info("Offered {} records to {} over {}ms ({} records/s achieved)",
                records.size(), inputTopic, elapsedMillis,
                String.format("%.1f", records.size() / (elapsedMillis / 1000d)));
    }

    private KafkaStreams startTopology(final String arm,
                                       final String inputTopic,
                                       final String outputTopic,
                                       final CompletionLedger ledger,
                                       final int streamThreads) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);
        stream.mapValues((key, value) -> process(value, ledger)).to(outputTopic);

        Properties props = baseStreamsProps(arm + "-" + System.nanoTime());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);

        return startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT);
    }

    /**
     * The per-record work: parse, call the dependency, compute, render. A realistic mix rather than a single
     * sleep, because the ratio of framework overhead to useful work is one of the things a sceptic will ask
     * about and because the parse and render are what dilute the gain on a large payload.
     */
    @SneakyThrows
    private static String process(final String value, final CompletionLedger ledger) {
        long entryNanos = System.nanoTime();

        // A real parse of a real payload. The cost is carried inside the record, so the parse is load-bearing:
        // a benchmark whose "realistic payload" is never read is measuring a sleep with a string attached.
        JsonNode authorisation = MAPPER.readTree(value);
        long blockNanos = authorisation.get("blockNanos").asLong();
        long spinNanos = authorisation.get("spinNanos").asLong();
        int index = authorisation.get("idx").asInt();
        long amountPence = authorisation.get("amountPence").asLong();
        String country = authorisation.get("country").asText();

        blockFor(blockNanos);
        long checksum = spinFor(spinNanos, value);

        // A decision a screening stage would actually make, so the emitted record is not simply the input
        // echoed back. Cheap on purpose - the cost model is the workload's, not this line's.
        String decision = amountPence > 200_000L || !"GB".equals(country) ? "REVIEW" : "APPROVE";
        String out = "{\"idx\":" + index + ",\"decision\":\"" + decision + "\",\"score\":" + (checksum & 0xFFFF) + "}";

        ledger.markCompleted(index, entryNanos);
        return out;
    }

    /**
     * The dependency call: a thread that cannot proceed. This is the case the seam exists for.
     */
    private static void blockFor(final long nanos) throws InterruptedException {
        if (nanos <= 0L) {
            return;
        }
        TimeUnit.NANOSECONDS.sleep(nanos);
    }

    /**
     * CPU-bound work: a fixed <em>amount</em> of real computation, sized so that on an unloaded machine it
     * takes about the requested time.
     *
     * <h2>Why this is not a deadline loop, which is what it was first written as</h2>
     * The first version of this method spun {@code while (System.nanoTime() < deadline)}. That is not
     * CPU-bound work - it is a busy-wait for a fixed <em>duration</em>, and a duration is exactly what a sleep
     * also is. Four workers each waiting out their own deadline finish in the same wall-clock time whether the
     * machine has twelve spare cores or none, so contention cannot possibly show up.
     * <p>
     * The bug was caught by the negative control failing: CPU-bound work on a machine with eleven of twelve
     * cores deliberately burned still measured 3.42x, which is impossible for genuinely CPU-bound work and so
     * indicted the fixture rather than the seam. A control arm that goes the wrong way is doing its job.
     * <p>
     * The fix is to fix the instruction count instead of the clock: {@link #ITERATIONS_PER_NANOSECOND} is
     * measured once on this machine, and the loop runs that many iterations per requested nanosecond. Under
     * contention the same work now genuinely takes longer, which is the property a CPU-bound control needs.
     *
     * @return the accumulated hash, returned so nothing in here is dead code the compiler may remove
     */
    private static long spinFor(final long nanos, final String payload) {
        if (nanos <= 0L) {
            return 0L;
        }
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        long iterations = Math.max(1L, (long) (nanos * ITERATIONS_PER_NANOSECOND));
        long hash = 1125899906842597L;
        for (long i = 0; i < iterations; i++) {
            for (byte b : bytes) {
                hash = 31 * hash + b;
            }
        }
        return hash;
    }

    /**
     * How many payload-hashing passes this machine manages per nanosecond when nothing else is running.
     * <p>
     * Measured once at class load, which is before any test constructs a {@code CpuSaturator} - calibrating
     * against an already-loaded machine would size the work unit too small and quietly turn the CPU-bound
     * cells back into something cheaper than they claim to be.
     */
    private static final double ITERATIONS_PER_NANOSECOND = calibrateSpin();

    /**
     * A short timed run of exactly the loop {@link #spinFor} uses, on a representative payload.
     * <p>
     * Deliberately preceded by a discarded pass: the first few thousand iterations run interpreted, and
     * calibrating on those would overstate the cost of an iteration by an order of magnitude and make every
     * CPU-bound cell far cheaper than requested.
     */
    private static double calibrateSpin() {
        byte[] bytes = new byte[512];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ('a' + (i % 26));
        }
        hashPasses(bytes, 200_000);

        long startNanos = System.nanoTime();
        long passes = 500_000L;
        long sink = hashPasses(bytes, passes);
        long elapsedNanos = Math.max(1L, System.nanoTime() - startNanos);

        double perNano = passes / (double) elapsedNanos;
        log.info("Calibrated CPU-bound work unit: {} hashing passes per nanosecond ({} bytes/pass, sink {})",
                String.format("%.6f", perNano), bytes.length, sink & 0xFF);
        return perNano;
    }

    private static long hashPasses(final byte[] bytes, final long passes) {
        long hash = 1125899906842597L;
        for (long pass = 0; pass < passes; pass++) {
            for (byte b : bytes) {
                hash = 31 * hash + b;
            }
        }
        return hash;
    }

    /**
     * Times each record inside the chain and counts completions.
     * <p>
     * Latency is measured from entry into the processor rather than from the produce timestamp, following the
     * existing benchmark: producer batching and the topology's startup have nothing to do with the seam, and
     * including them would put noise in the distribution that moves with the broker rather than with the arm.
     */
    private static final class CompletionLedger {

        private final List<Long> completionNanos = Collections.synchronizedList(new ArrayList<>());
        private final List<Long> inChainLatencyMillis = Collections.synchronizedList(new ArrayList<>());
        private final List<Long> endToEndLatencyMillis = Collections.synchronizedList(new ArrayList<>());

        /**
         * When each record was handed to the producer, indexed by the record's position in the workload.
         * <p>
         * <b>This is what makes end-to-end latency measurable at all, and its absence hid a real effect.</b>
         * In-chain latency starts when a record enters the processor, so it cannot see a record sitting in the
         * consumer's buffer waiting for a free StreamThread - which is precisely where head-of-line blocking
         * puts it. A steady-state arm measured with in-chain latency alone reported 0.99x and looked like a
         * null result, because both arms take the same time once they are actually running. Stamping the send
         * is what lets the wait before that show up.
         */
        private final long[] sentNanos;

        private final AtomicLong lastCompletion = new AtomicLong();

        CompletionLedger(final int recordCount) {
            this.sentNanos = new long[recordCount];
        }

        void markSent(final int index) {
            sentNanos[index] = System.nanoTime();
        }

        void markCompleted(final int index, final long entryNanos) {
            long now = System.nanoTime();
            completionNanos.add(now);
            inChainLatencyMillis.add((now - entryNanos) / 1_000_000L);
            endToEndLatencyMillis.add((now - sentNanos[index]) / 1_000_000L);
            lastCompletion.accumulateAndGet(now, Math::max);
        }

        List<Long> endToEndLatencyMillis() {
            synchronized (endToEndLatencyMillis) {
                return new ArrayList<>(endToEndLatencyMillis);
            }
        }

        int completed() {
            return completionNanos.size();
        }

        long lastCompletionNanos() {
            return lastCompletion.get();
        }

        List<Long> completionNanos() {
            synchronized (completionNanos) {
                return new ArrayList<>(completionNanos);
            }
        }

        List<Long> inChainLatencyMillis() {
            synchronized (inChainLatencyMillis) {
                return new ArrayList<>(inChainLatencyMillis);
            }
        }
    }
}
