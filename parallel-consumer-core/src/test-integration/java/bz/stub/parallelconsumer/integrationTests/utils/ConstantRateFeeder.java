package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * A rate-holding producer: publishes to one topic at a fixed records/second for a fixed span, so that a test
 * measures <b>the consumer under a controlled arrival process</b> rather than under whatever backlog the producer
 * happened to build. Ported from the bench lane's arrival-mode feeder (see {@code bench/Bench.java.template},
 * which is shell-rendered and not on any test classpath - hence this utility rather than an import).
 * <p>
 * The three properties that make the schedule an experiment rather than a hope, all inherited from the bench
 * feeder's design notes:
 * <ul>
 * <li><b>The schedule is absolute, never incremental.</b> Record {@code i} is due at {@code t0 + i / rate}; a
 * late send is sent immediately and does not push every later record back with it. Sleep-accumulate schedules
 * silently convert producer jitter into a lower arrival rate, which is exactly the confound being removed.</li>
 * <li><b>{@code t0} is when the consumer is demonstrably live</b>, not when the thread starts. One warmup record
 * goes first and the schedule starts only once the caller-supplied completion counter shows the consumer has
 * finished it - otherwise a slow group join hands the consumer {@code rate * joinTime} records of backlog before
 * it sees the first one, and the run begins saturated. The warmup record carries a timestamp before {@code t0},
 * which is how callers exclude it from their measures.</li>
 * <li><b>A feed that could not hold its schedule voids the run rather than annotating it.</b>
 * {@link #verdict()} is non-null when the achieved rate diverged beyond tolerance or records were handed to the
 * producer too late - the resulting numbers answer a question about the producer, and a test must fail on that
 * verdict rather than publish them.</li>
 * </ul>
 * Record timestamps are producer-set in this JVM, so {@code ConsumerRecord#timestamp()} is the record's creation
 * instant on the same clock the consumer's completion times use - end-to-end residence needs no value parsing.
 */
@Slf4j
public final class ConstantRateFeeder {

    /** Fractional divergence between requested and achieved rate that voids the run (bench default). */
    public static final double RATE_TOLERANCE = 0.05;

    /** Feed-lag p99 (ms) beyond which the schedule was not the one requested (bench default). */
    public static final long MAX_FEED_LAG_MS = 100;

    /** How long the warmup record may take to complete before the run is declared dead. */
    public static final long WARMUP_TIMEOUT_MS = 90_000;

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final double ratePerSecond;
    private final long scheduleSpanMillis;
    private final LongSupplier completedRecords;
    private final String keyPrefix;

    private final AtomicLong fedRecords = new AtomicLong();
    private final CountDownLatch scheduleStarted = new CountDownLatch(1);
    private final CountDownLatch finished = new CountDownLatch(1);
    private final List<Long> feedLagMillis = Collections.synchronizedList(new ArrayList<>());

    private volatile long scheduleStartMillis;
    private volatile double achievedRatePerSecond;
    private volatile String failure;
    private Thread thread;

    /**
     * @param producer         the producer to feed with; NOT closed by this class - the caller owns it. Configure
     *                         it with {@code linger.ms=0}: the schedule is the experiment, so nothing may sit in
     *                         an accumulator waiting for a batch to fill
     * @param topic            destination topic
     * @param ratePerSecond    the arrival rate to hold
     * @param scheduleSpanMillis how long the schedule runs, measured from {@code t0}
     * @param completedRecords live count of records the consumer has finished - the warmup barrier's signal
     * @param keyPrefix        prefix for the generated keys ({@code prefix + index}), distinct per arm so ledgers
     *                         cannot alias across arms
     */
    public ConstantRateFeeder(KafkaProducer<String, String> producer, String topic, double ratePerSecond,
                              long scheduleSpanMillis, LongSupplier completedRecords, String keyPrefix) {
        this.producer = producer;
        this.topic = topic;
        this.ratePerSecond = ratePerSecond;
        this.scheduleSpanMillis = scheduleSpanMillis;
        this.completedRecords = completedRecords;
        this.keyPrefix = keyPrefix;
    }

    /** Starts the feeder thread: warmup record, warmup barrier, then the absolute schedule. */
    public void start() {
        thread = new Thread(this::run, "constant-rate-feeder-" + keyPrefix);
        thread.setDaemon(true);
        thread.start();
    }

    private void run() {
        try {
            producer.send(new ProducerRecord<>(topic, keyPrefix + "warmup", "warmup"));
            producer.flush();
            long waitedFrom = System.currentTimeMillis();
            while (completedRecords.getAsLong() < 1) {
                if (System.currentTimeMillis() - waitedFrom > WARMUP_TIMEOUT_MS) {
                    failure = "consumer completed nothing within " + WARMUP_TIMEOUT_MS
                            + "ms of the warmup record being produced";
                    return;
                }
                sleepQuietly(5);
            }
            long t0 = System.currentTimeMillis();
            scheduleStartMillis = t0;
            scheduleStarted.countDown();
            double intervalMs = 1000.0 / ratePerSecond;
            long lastSend = t0;
            long index = 0;
            while (true) {
                long due = t0 + Math.round(index * intervalMs);
                if (due - t0 >= scheduleSpanMillis) {
                    break;
                }
                parkUntil(due);
                long at = System.currentTimeMillis();
                feedLagMillis.add(Math.max(0, at - due));
                producer.send(new ProducerRecord<>(topic, keyPrefix + index, "v"));
                fedRecords.incrementAndGet();
                lastSend = at;
                index++;
            }
            producer.flush();
            long span = Math.max(1, lastSend - t0);
            achievedRatePerSecond = fedRecords.get() * 1000.0 / span;
        } catch (Exception e) {
            failure = "feeder threw: " + e;
            log.error("Constant-rate feeder failed - the run's verdict will void it", e);
        } finally {
            finished.countDown();
        }
    }

    /** Blocks until the schedule's {@code t0} is set (the warmup barrier passed), or the timeout passes. */
    public boolean awaitScheduleStart(long timeout, TimeUnit unit) throws InterruptedException {
        return scheduleStarted.await(timeout, unit);
    }

    /** Blocks until the whole schedule has been fed (or the feeder failed). */
    public boolean awaitFinished(long timeout, TimeUnit unit) throws InterruptedException {
        return finished.await(timeout, unit);
    }

    /** The schedule's epoch {@code t0} in wall millis - 0 until the warmup barrier passes. */
    public long getScheduleStartMillis() {
        return scheduleStartMillis;
    }

    /** Records fed on the schedule so far - excludes the warmup record. */
    public long getFedRecords() {
        return fedRecords.get();
    }

    public double getAchievedRatePerSecond() {
        return achievedRatePerSecond;
    }

    /**
     * @return null when the feed delivered the schedule it was asked for; otherwise the reason the run is VOID -
     * a non-null verdict means the numbers measure the producer, and the caller must fail on it, never publish
     */
    public String verdict() {
        if (failure != null) {
            return failure;
        }
        if (achievedRatePerSecond <= 0) {
            return "the feeder never completed its schedule";
        }
        double divergence = Math.abs(achievedRatePerSecond - ratePerSecond) / ratePerSecond;
        if (divergence > RATE_TOLERANCE) {
            return String.format("achieved arrival rate %.1f/s against a requested %.1f/s (%.1f%% out, "
                            + "tolerance %.1f%%) - this run measures the producer",
                    achievedRatePerSecond, ratePerSecond, divergence * 100, RATE_TOLERANCE * 100);
        }
        long lagP99 = feedLagP99Millis();
        if (lagP99 > MAX_FEED_LAG_MS) {
            return String.format("feed-lag p99 %dms exceeds %dms - records were handed to the producer late "
                    + "enough that the arrival schedule is not the one requested", lagP99, MAX_FEED_LAG_MS);
        }
        return null;
    }

    private long feedLagP99Millis() {
        List<Long> lags;
        synchronized (feedLagMillis) {
            lags = new ArrayList<>(feedLagMillis);
        }
        if (lags.isEmpty()) {
            return 0;
        }
        Collections.sort(lags);
        return lags.get((int) Math.min(lags.size() - 1, Math.floor(lags.size() * 0.99)));
    }

    /** Parks until {@code due}; returns immediately when already late - what keeps the schedule absolute. */
    private static void parkUntil(long due) {
        long remaining;
        while ((remaining = due - System.currentTimeMillis()) > 0) {
            LockSupport.parkNanos(remaining * 1_000_000L);
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
