package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import bz.stub.parallelconsumer.internal.utils.ProgressBarUtils;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static java.time.Duration.ofMillis;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * The comparison demo: the same records, through a serial consumer and through Parallel Consumer in
 * each of its three orderings, with the per-record work simulated by a sleep.
 * <p>
 * This is the second half of the rescue described in
 * {@code docs/inflight/branch-classic-comparison-demo.md} - decision 10, "keep the Vert.x demo AND
 * build the core/sleep one as well". The first half is {@code Demo} in the Vert.x module, the 2021
 * class behind the asciinema cast the README links.
 *
 * <h2>Why a sleep, and no HTTP server</h2>
 *
 * The cast's headline number was 27,201 msg/s at a 2ms simulated service time, which is 54 records
 * in flight; this fork's port of that class re-measured 41. The configured ceiling was 100. Parallel
 * Consumer was sitting at <b>its own configured ceiling, never at a threading limit</b> - so the
 * non-blocking Vert.x HTTP client was not what produced the number, and 100 threads each sleeping
 * 2ms reproduces it. That is the single fact this design rests on (decision 5), and it is why this
 * demo can drop both WireMock and Vert.x and still show the same result.
 * <p>
 * Where that stops being true is {@link #MAX_CONCURRENCY_CAP}.
 *
 * <h2>What it measures, and what it deliberately does not</h2>
 *
 * <b>Throughput over a pre-produced backlog. Not latency.</b> Every record is on the topic before
 * any lane starts, so the workload is closed-loop by construction: a lane that falls behind is
 * handed its next record later, and per-record timings taken here would be flattered by exactly the
 * amount it fell behind. That is coordinated omission, and the constraint agreed across both perf
 * tracks is that latency at a user-chosen delay must be driven open-loop at a fixed arrival rate,
 * measured from intended send time. This demo does not do that, so it does not report a latency -
 * rather than reporting one that is wrong in the flattering direction.
 *
 * <h2>Running it</h2>
 *
 * <pre>./mvnw verify -pl parallel-consumer-core -am -Dit.test=ComparisonDemo \
 *     -Dfailsafe.failIfNoSpecifiedTests=false -DskipUTs=true -Dpc.demo=true</pre>
 * <p>
 * Off by default, and the gate is not decoration: this lane collects by PACKAGE PATH - failsafe
 * includes {@code **&#47;integrationTest*&#47;**&#47;*.java} - so living in this package is what
 * decides collection and the class name is irrelevant. A multi-minute measurement with no
 * assertions would otherwise run on every build.
 * <p>
 * There is no {@code main}, deliberately. The Vert.x demo has one for historical reasons and pays
 * for it: a hand-built classpath hid a real dependency, and the explicit {@code System.exit} it
 * needs is a trap next to a JUnit entry point, because inside a failsafe fork that reports as a
 * crashed VM however well the run went. Maven owns the classpath here.
 */
@Testcontainers
@Slf4j
public class ComparisonDemo extends BrokerIntegrationTest<String, String> {

    /** Set to {@code true} to run the demo; see the class javadoc for the full command. */
    static final String DEMO_ENABLED_PROPERTY = "pc.demo";

    /**
     * The four lanes of decision 8. The pairing is the point: a plain consumer is inherently
     * partition-ordered and serial, so {@link #PC_PARTITION} is the apples-to-apples lane and
     * {@link #PC_UNORDERED} is the ceiling. That is the README chart's own structure.
     * <p>
     * <b>Public because the truth generator sweeps every enum under
     * {@code bz.stub.parallelconsumer} and emits its Subject into the PARENT package</b>, which
     * cannot see a package-private type - so narrowing this fails the generated code's compile in
     * another directory, not this file's. {@code OffsetCommittingSanityTest.CheckMode} and
     * {@code ChaosConductor.ChaosAction} are public for the same reason.
     */
    public enum Lane {
        VANILLA(null),
        PC_UNORDERED(ProcessingOrder.UNORDERED),
        PC_KEY(ProcessingOrder.KEY),
        PC_PARTITION(ProcessingOrder.PARTITION);

        final ProcessingOrder ordering;

        Lane(ProcessingOrder ordering) {
            this.ordering = ordering;
        }

        boolean isParallel() {
            return ordering != null;
        }
    }

    // --- knobs (decision 9). Property convention is <concern>.<knob>, as with load.total. ---

    static final int RECORDS = Integer.getInteger("demo.records", 5_000);

    /** The simulated per-record service time. 2ms is the classic cast's value. */
    static final int DELAY_MS = Integer.getInteger("demo.delayMs", 2);

    /**
     * Key-set size as a PERCENTAGE of the record count, so the knob means the same thing at any
     * volume. 100 gives every record its own key, which is what the classic demo did - and which
     * makes keys inert.
     * <p>
     * It does <b>not</b> change the ordering mode. That was left open when the lanes were decided,
     * and running all four lanes dissolves it: the dial sets the number of independent shards under
     * {@link Lane#PC_KEY} and is inert everywhere else, so it is orthogonal to ordering rather than
     * coupled to it. {@link #describeKeyRelevance} labels that in the report instead of hiding it.
     */
    static final int KEYS_PERCENT = Integer.getInteger("demo.keysPercent", 100);

    /**
     * Percentage of records that fail their FIRST attempt and succeed on retry. A permanent failure
     * would end the run rather than exercise the retry path, and a randomly-retrying one would make
     * the lanes incomparable - so the rule is deterministic and identical in every lane.
     */
    static final int FAILURE_PERCENT = Integer.getInteger("demo.failurePercent", 0);

    /**
     * The cap of decision 16 - a demo must choose a number, there is no "let it run".
     * <p>
     * <b>1000 is where this demo's own model stops being honest</b>, which is the only defensible
     * place to put it. The simulated work is a blocking sleep, so in-flight records are threads;
     * that is free at a hundred and roughly free at a thousand, and above it the thread-per-record
     * cost starts showing up in the number being reported. That is also precisely where the Vert.x
     * demo becomes the one worth running - a non-blocking client is what earns its keep above this
     * line, and the sibling demo exists to show it.
     */
    static final int MAX_CONCURRENCY_CAP = 1_000;

    static final int MAX_CONCURRENCY = Math.min(
            Integer.getInteger("demo.maxConcurrency", 100), MAX_CONCURRENCY_CAP);

    /**
     * Lanes run one after another by default (decision 6). Sequential reproduces the classic cast's
     * shape and is the only mode whose numbers are comparable between lanes; concurrent is offered
     * so the choice is the reader's, and the report says which was used.
     */
    static final boolean SEQUENTIAL = !Boolean.getBoolean("demo.concurrent");

    static final List<Lane> LANES = parseLanes(System.getProperty("demo.lanes"));

    /** How far past a lane's ideal time the run may go before it is called a stall. */
    private static final int SLOWEST_ACCEPTABLE_FACTOR = 20;

    /** Below this, start-up and the first rebalance dominate, so the derived deadline is noise. */
    private static final Duration DEADLINE_FLOOR = Duration.ofMinutes(2);

    /**
     * Written by every lane, and in concurrent mode by several at once, so it cannot be a plain
     * list.
     */
    private final List<AutoCloseable> openResources = Collections.synchronizedList(new ArrayList<>());

    private static List<Lane> parseLanes(String property) {
        if (property == null || property.isEmpty()) {
            return Arrays.asList(Lane.values());
        }
        return Arrays.stream(property.split(","))
                .map(String::trim)
                .map(String::toUpperCase)
                .map(Lane::valueOf)
                .collect(Collectors.toList());
    }

    @Value
    static class LaneResult {
        Lane lane;
        Duration elapsed;
        int processed;

        double ratePerSecond() {
            double seconds = elapsed.toNanos() / 1_000_000_000d;
            return seconds > 0 ? processed / seconds : 0;
        }
    }

    /**
     * Closes everything every lane opened, on the success path and the failure path alike. The
     * Vert.x demo shipped without this and hung on failure instead of reporting, because the engine
     * and the consumers hold non-daemon threads.
     */
    @AfterEach
    void closeEverything() {
        for (AutoCloseable resource : openResources) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("Failed to close {} - continuing, so the remaining closes still run",
                        resource.getClass().getSimpleName(), e);
            }
        }
        openResources.clear();
    }

    @Test
    @EnabledIfSystemProperty(named = DEMO_ENABLED_PROPERTY, matches = "true")
    @SneakyThrows
    void compareLanes() {
        int uniqueKeys = Math.max(1, (int) (RECORDS * (KEYS_PERCENT / 100d)));
        String topic = setupTopic("comparison-demo-" + RandomUtils.nextInt());

        logRunFingerprint(uniqueKeys, topic);

        // Produced ONCE, read by every lane under its own group id (decision 7), so the lanes
        // process identical records rather than merely similar ones.
        log.info("Producing {} records ({} unique keys)...", format(RECORDS), format(uniqueKeys));
        getKcu().produceMessages(topic, RECORDS, "", uniqueKeys);

        List<LaneResult> results = SEQUENTIAL
                ? runSequentially(topic)
                : runConcurrently(topic);

        report(results);
    }

    private List<LaneResult> runSequentially(String topic) {
        List<LaneResult> results = new ArrayList<>();
        for (Lane lane : LANES) {
            results.add(runLane(lane, topic));
        }
        return results;
    }

    /**
     * All lanes at once. Reported separately because the lanes then contend for the same host, so
     * these numbers are comparable with each other but NOT with a sequential run's.
     */
    @SneakyThrows
    private List<LaneResult> runConcurrently(String topic) {
        Map<Lane, LaneResult> results = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(LANES.size());
        for (Lane lane : LANES) {
            Thread thread = new Thread(() -> {
                try {
                    results.put(lane, runLane(lane, topic));
                } finally {
                    done.countDown();
                }
            }, "demo-lane-" + lane);
            thread.start();
        }
        done.await();
        return LANES.stream().map(results::get).filter(r -> r != null).collect(Collectors.toList());
    }

    private LaneResult runLane(Lane lane, String topic) {
        log.info("\n=== {} starting ===", lane);
        long startedAt = System.nanoTime();
        int processed = lane.isParallel()
                ? runParallelLane(lane, topic)
                : runVanillaLane(topic);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
        log.info("=== {} finished: {} records in {}ms ===", lane, format(processed), elapsed.toMillis());
        return new LaneResult(lane, elapsed, processed);
    }

    /**
     * The serial arm: poll, then process each record to completion before the next one. Failures
     * are retried inline, because that is what a naive consumer does with a record it cannot skip -
     * dropping them here would hand the vanilla lane a discount the parallel lanes do not get.
     */
    @SneakyThrows
    private int runVanillaLane(String topic) {
        KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(GroupOption.NEW_GROUP);
        openResources.add(consumer);
        consumer.subscribe(of(topic));

        AtomicInteger processed = new AtomicInteger();
        try (ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, RECORDS)) {
            long deadline = System.nanoTime() + deadlineFor(Lane.VANILLA).toNanos();
            while (processed.get() < RECORDS) {
                failIfPastDeadline(Lane.VANILLA, processed, deadline);
                ConsumerRecords<String, String> polled = consumer.poll(ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    int attempt = 0;
                    while (true) {
                        try {
                            simulateWork(recordId(record), attempt);
                            break;
                        } catch (RuntimeException e) {
                            attempt++;
                        }
                    }
                    bar.stepTo(processed.incrementAndGet());
                }
            }
        }
        return processed.get();
    }

    @SneakyThrows
    private int runParallelLane(Lane lane, String topic) {
        Properties consumerProps = new Properties();
        KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(true, consumerProps);

        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(
                ParallelConsumerOptions.<String, String>builder()
                        .ordering(lane.ordering)
                        .consumer(consumer)
                        .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                        .maxConcurrency(MAX_CONCURRENCY)
                        .build());
        openResources.add(pc);
        pc.subscribe(of(topic));

        AtomicInteger processed = new AtomicInteger();
        try (ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, RECORDS)) {
            pc.poll(context -> {
                simulateWork(recordId(context.getSingleConsumerRecord()),
                        context.getSingleRecord().getNumberOfFailedAttempts());
                bar.stepTo(processed.incrementAndGet());
            });

            awaitCompletion(lane, processed);
        }
        pc.closeDrainFirst();
        return processed.get();
    }

    /**
     * Derived from the work the lane actually has to do, not picked: the serial arm owes
     * {@code records x delay}, a parallel arm owes that divided by its in-flight ceiling. The
     * multiplier is deliberately loose - this is a demo's "something is wrong" backstop, not a
     * performance assertion, and a tight deadline here would turn a slow host into a false failure.
     * The floor absorbs broker start-up and the first rebalance, which dominate at small volumes.
     */
    private static Duration deadlineFor(Lane lane) {
        long idealMs = (long) RECORDS * DELAY_MS;
        if (lane.isParallel()) {
            idealMs = idealMs / MAX_CONCURRENCY;
        }
        return Duration.ofMillis(Math.max(SLOWEST_ACCEPTABLE_FACTOR * idealMs, DEADLINE_FLOOR.toMillis()));
    }

    private static void awaitCompletion(Lane lane, AtomicInteger processed) {
        long deadline = System.nanoTime() + deadlineFor(lane).toNanos();
        while (processed.get() < RECORDS) {
            failIfPastDeadline(lane, processed, deadline);
            ThreadUtils.sleepQuietly(100);
        }
    }

    /**
     * A demo that hangs reports nothing, which is worse than a demo that fails - so the wait ends
     * with a message naming the lane and how far it got.
     */
    private static void failIfPastDeadline(Lane lane, AtomicInteger processed, long deadlineNanos) {
        if (System.nanoTime() > deadlineNanos) {
            throw new IllegalStateException(String.format(
                    "%s stalled: %s of %s records after %s. Nothing is asserted here, so this is a "
                            + "stall, not a slow host - check the broker and the engine's logs above.",
                    lane, format(processed.get()), format(RECORDS), deadlineFor(lane)));
        }
    }

    /**
     * The user function every lane runs: sleep for the configured service time, and fail the first
     * attempt of the selected share of records.
     *
     * @param attempt how many times this record has already failed - 0 on the first attempt
     */
    private void simulateWork(String recordId, int attempt) {
        ThreadUtils.sleepQuietly(DELAY_MS);
        if (attempt == 0 && shouldFail(recordId)) {
            throw new RuntimeException("Simulated failure for " + recordId + " (demo.failurePercent="
                    + FAILURE_PERCENT + ") - it will succeed on retry");
        }
    }

    /**
     * Deterministic, so every lane fails the SAME records. Randomly-chosen failures would differ per
     * lane, and the comparison would then be between different workloads.
     */
    private static boolean shouldFail(String recordId) {
        if (FAILURE_PERCENT <= 0) {
            return false;
        }
        return Math.floorMod(recordId.hashCode(), 100) < FAILURE_PERCENT;
    }

    private static String recordId(ConsumerRecord<String, String> record) {
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }

    /**
     * The effective-config fingerprint both perf tracks require. Printed BEFORE the run, and it
     * names what each lane actually got rather than what was asked for - the concurrency cap and the
     * derived key count both mean the requested value is not always the effective one, and a number
     * quoted without them is not reproducible.
     */
    private void logRunFingerprint(int uniqueKeys, String topic) {
        Map<String, Object> fingerprint = new HashMap<>();
        fingerprint.put("records", RECORDS);
        fingerprint.put("delayMs", DELAY_MS);
        fingerprint.put("keysPercent", KEYS_PERCENT);
        fingerprint.put("uniqueKeys", uniqueKeys);
        fingerprint.put("failurePercent", FAILURE_PERCENT);
        fingerprint.put("maxConcurrency", MAX_CONCURRENCY);
        fingerprint.put("maxConcurrencyCap", MAX_CONCURRENCY_CAP);
        fingerprint.put("commitMode", PERIODIC_CONSUMER_ASYNCHRONOUS);
        fingerprint.put("lanes", LANES);
        fingerprint.put("execution", SEQUENTIAL ? "sequential" : "concurrent");
        fingerprint.put("topic", topic);

        log.info("\nEffective configuration:\n{}", fingerprint.entrySet().stream()
                .map(e -> "  " + e.getKey() + " = " + e.getValue())
                .collect(Collectors.joining("\n")));

        if (Integer.getInteger("demo.maxConcurrency", 100) > MAX_CONCURRENCY_CAP) {
            log.warn("Requested concurrency was capped to {} - above that the blocking-sleep model "
                    + "stops being honest, and the Vert.x demo is the one to run", MAX_CONCURRENCY_CAP);
        }
        if (!SEQUENTIAL) {
            log.warn("Lanes are running CONCURRENTLY, so they contend for this host: these numbers "
                    + "compare with each other, but not with a sequential run's");
        }
    }

    private void report(List<LaneResult> results) {
        LaneResult vanilla = results.stream()
                .filter(r -> r.getLane() == Lane.VANILLA)
                .findFirst()
                .orElse(null);

        StringBuilder table = new StringBuilder("\n\nResults (" + describeKeyRelevance() + "):\n");
        table.append(String.format("  %-14s %12s %14s %10s%n", "lane", "elapsed", "msg/s", "vs vanilla"));
        for (LaneResult result : results) {
            String ratio = vanilla == null || vanilla.ratePerSecond() == 0
                    ? "-"
                    : String.format("%.1fx", result.ratePerSecond() / vanilla.ratePerSecond());
            table.append(String.format("  %-14s %11ds %14s %10s%n",
                    result.getLane(),
                    result.getElapsed().getSeconds(),
                    format((int) result.ratePerSecond()),
                    ratio));
        }
        log.info(table.toString());
    }

    /** Says where the key-set dial actually bites, rather than letting a reader assume it bites everywhere. */
    private static String describeKeyRelevance() {
        if (KEYS_PERCENT >= 100) {
            return "every record has its own key, so keys are inert in every lane";
        }
        return "the key-set dial shapes PC_KEY only; the other lanes are unaffected by it";
    }

    private static String format(int number) {
        return String.format("%,d", number);
    }
}
