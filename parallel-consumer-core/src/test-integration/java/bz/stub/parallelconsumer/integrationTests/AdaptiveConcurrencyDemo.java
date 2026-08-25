package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.admission.AdmissionController;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofMinutes;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * The classic demo's question - "how fast does PC eat a backlog when every record needs an external call?" -
 * asked against downstreams whose true capacity NOBODY CONFIGURES INTO PC. Two plants:
 * <p>
 * <b>{@link #adaptiveDiscoversWhatTheClassicDemoHardCodes()}</b> - a slot-limited service:
 * {@value #DOWNSTREAM_CAPACITY_SLOTS} requests at once, {@value #SERVICE_TIME_MS}ms each (a semaphore around a
 * sleep, per the demo ledger's simulated-work rule). A HARD knee: every arm at or above
 * {@value #DOWNSTREAM_CAPACITY_SLOTS} slots saturates the same {@code slots/serviceTime} ceiling, so matching
 * throughput between such arms is arithmetic, not discovery - the columns that separate the arms are the
 * <b>avg request time</b> (extra slots buy queueing, not work) and, for the adaptive arm, the target trajectory.
 * <p>
 * <b>{@link #adaptiveBacksOffAnOverloadedCpuBoundServer()}</b> - an overloaded CPU-bound server: each request
 * counts to {@value #CPU_COUNT_TO} as fast as the machine can, so concurrent requests genuinely slow each other
 * once the spinning threads outnumber the cores. A SOFT knee at roughly the core count, which nothing
 * configures anywhere: a static 50-thread arm pays for the oversubscription in per-request time; the adaptive
 * arm has to find the knee from throughput alone. The demo logs a serial calibration of one count first, so the
 * arithmetic behind the plateau is visible in the run itself.
 * <p>
 * Off by default, same discipline as the classic {@code Demo}: this package path is failsafe-collected, so a
 * multi-minute measurement with no assertions must not run on every build. Run one plant at a time:
 * <pre>./mvnw verify -pl parallel-consumer-core -am -Dpc.demo=true \
 *     -Dit.test='AdaptiveConcurrencyDemo#adaptiveDiscoversWhatTheClassicDemoHardCodes' \
 *     -Dtest=skipall -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dfailsafe.failIfNoSpecifiedTests=false</pre>
 * (or {@code ...Demo#adaptiveBacksOffAnOverloadedCpuBoundServer}; a bare {@code -Dit.test=AdaptiveConcurrencyDemo}
 * runs both.)
 */
@Slf4j
public class AdaptiveConcurrencyDemo extends BrokerIntegrationTest<String, String> {

    /** Set to {@code true} to run the demo; mirrors the classic {@code Demo}'s flag. */
    static final String DEMO_ENABLED_PROPERTY = "pc.demo";

    /** What the slot-limited downstream can really do at once - and no arm is told it. */
    static final int DOWNSTREAM_CAPACITY_SLOTS = 32;
    /** Service time per request once a downstream slot is held. */
    static final int SERVICE_TIME_MS = 20;
    /** The library's default maxConcurrency - what you get when you never tune. */
    static final int DEFAULT_GUESS_SLOTS = 16;
    /** The classic demo's number - here it is simply over-provisioned: 3x the downstream's capacity. */
    static final int OVER_PROVISIONED_SLOTS = 100;
    /** Adaptive starts nearly serial, so the discovery is visible rather than pre-solved by the seed. */
    static final int ADAPTIVE_SEED_SLOTS = 2;
    /**
     * Enough backlog that the adaptive arm has runway PAST the knee: the first run's 60k ended with the target
     * still climbing at 39 mid-ramp, which shows a snapshot, not convergence. ~150s at the ceiling gives the
     * hold band and the descent probes time to actually settle.
     */
    static final int RECORD_COUNT = 240_000;

    /**
     * The CPU plant's per-request work: count this high with a data dependency the JIT cannot delete. At
     * ~3.8GHz this is a few milliseconds serially; the run logs its own calibration rather than trusting this
     * comment.
     */
    static final long CPU_COUNT_TO = 10_000_000L;
    /** The owner's comparison arm: PC core pinned at 50 threads, spinning on however many cores exist. */
    static final int CPU_STATIC_SLOTS = 50;
    /** Sized for roughly a minute per arm at a ~32-core plateau; adjust with CPU_COUNT_TO for other machines. */
    static final int CPU_RECORD_COUNT = 300_000;

    @Test
    @EnabledIfSystemProperty(named = DEMO_ENABLED_PROPERTY, matches = "true")
    @SneakyThrows
    void adaptiveDiscoversWhatTheClassicDemoHardCodes() {
        String topic = setupTopic(getClass().getSimpleName());
        log.info("Downstream capacity nobody configures into PC: {} concurrent, {}ms each = {} msg/s ceiling. "
                        + "NOTE the knee is HARD: every arm at or above {} slots hits the same ceiling, so equal "
                        + "throughput between those arms is arithmetic - read the avg-request and target columns.",
                DOWNSTREAM_CAPACITY_SLOTS, SERVICE_TIME_MS, 1000 / SERVICE_TIME_MS * DOWNSTREAM_CAPACITY_SLOTS,
                DOWNSTREAM_CAPACITY_SLOTS);
        log.info("Producing {} records...", String.format("%,d", RECORD_COUNT));
        getKcu().produceMessages(topic, RECORD_COUNT);

        Semaphore downstream = new Semaphore(DOWNSTREAM_CAPACITY_SLOTS);
        Runnable slotLimitedRequest = () -> {
            try {
                downstream.acquire();
                try {
                    Thread.sleep(SERVICE_TIME_MS);
                } finally {
                    downstream.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        };

        ArmResult defaultGuess = runArm(topic, "static, the default guess (16)", DEFAULT_GUESS_SLOTS, false,
                RECORD_COUNT, slotLimitedRequest);
        ArmResult overProvisioned = runArm(topic, "static, over-provisioned (100)", OVER_PROVISIONED_SLOTS, false,
                RECORD_COUNT, slotLimitedRequest);
        ArmResult adaptive = runArm(topic, "adaptive, told nothing", OVER_PROVISIONED_SLOTS, true,
                RECORD_COUNT, slotLimitedRequest);

        log.info("\n=== RESULTS: {} records each, downstream truth is {} slots x {}ms ===\n{}\n{}\n{}\n",
                String.format("%,d", RECORD_COUNT), DOWNSTREAM_CAPACITY_SLOTS, SERVICE_TIME_MS,
                defaultGuess, overProvisioned, adaptive);
        log.info("Same ceiling, same seed rules: the adaptive arm was given the over-provisioned arm's ceiling "
                + "and a seed of {} - the level it ran at, it discovered.", ADAPTIVE_SEED_SLOTS);
    }

    @Test
    @EnabledIfSystemProperty(named = DEMO_ENABLED_PROPERTY, matches = "true")
    @SneakyThrows
    void adaptiveBacksOffAnOverloadedCpuBoundServer() {
        // Calibrate serially first, so the run carries its own arithmetic: N cores / serial-time is the
        // theoretical plateau, and the knee sits near the core count - which nothing below configures.
        long calStart = System.nanoTime();
        int calRounds = 20;
        for (int i = 0; i < calRounds; i++) {
            spinCount();
        }
        double serialMs = (System.nanoTime() - calStart) / 1_000_000.0 / calRounds;
        int cores = Runtime.getRuntime().availableProcessors();
        log.info("CPU plant calibration: counting to {} takes {}ms serially; the JVM reports {} processors "
                        + "(the OS may grant spinning threads more). Theoretical plateau ~{} msg/s, knee near the "
                        + "real core count.",
                String.format("%,d", CPU_COUNT_TO), String.format(Locale.ROOT, "%.2f", serialMs), cores,
                String.format("%,.0f", cores / (serialMs / 1000)));

        String topic = setupTopic(getClass().getSimpleName() + "-cpu");
        log.info("Producing {} records...", String.format("%,d", CPU_RECORD_COUNT));
        getKcu().produceMessages(topic, CPU_RECORD_COUNT);

        Runnable cpuBoundRequest = AdaptiveConcurrencyDemo::spinCount;

        ArmResult staticFifty = runArm(topic, "static, 50 threads", CPU_STATIC_SLOTS, false,
                CPU_RECORD_COUNT, cpuBoundRequest);
        ArmResult adaptive = runArm(topic, "adaptive, told nothing", OVER_PROVISIONED_SLOTS, true,
                CPU_RECORD_COUNT, cpuBoundRequest);

        log.info("\n=== RESULTS: {} records each, CPU-bound server counting to {} per request, serial {}ms ===\n{}\n{}\n",
                String.format("%,d", CPU_RECORD_COUNT), String.format("%,d", CPU_COUNT_TO),
                String.format(Locale.ROOT, "%.2f", serialMs), staticFifty, adaptive);
        log.info("Watch the avg-request column: oversubscribed spinning threads make EVERY request slower "
                + "without adding throughput - the knee the adaptive arm had to find is the machine itself.");
    }

    // ------------------------------------------------------------------
    // The moving-downstream plant: the demo for "but my static guess was right".
    // ------------------------------------------------------------------

    /** Phase 1 and 3 capacity - and the static arm's PERFECT tuning for it. */
    static final int HEALTHY_CAPACITY_SLOTS = 48;
    /** Phase 2: the downstream degrades - failover, GC storm, noisy neighbour. Nobody edits any config. */
    static final int DEGRADED_CAPACITY_SLOTS = 8;
    /** Wall-clock length of each phase: healthy, degraded, recovered. */
    static final int PHASE_SECONDS = 75;
    /** Backlog deep enough that no arm ever drains it inside the schedule - arms are time-boxed, not drain-boxed. */
    static final int MOVING_RECORD_COUNT = 700_000;

    /**
     * The answer to "I keep guessing the right concurrency, so what is this worth?" - the previous two plants
     * hold still, so ANY guess at or above the knee looks right. This one moves: the downstream serves
     * {@value #HEALTHY_CAPACITY_SLOTS} concurrent requests, degrades to {@value #DEGRADED_CAPACITY_SLOTS}
     * mid-run, then recovers. The static arm gets the BEST POSSIBLE number - {@value #HEALTHY_CAPACITY_SLOTS},
     * exactly right on the day it was tuned - and the adaptive arm is SEEDED with that same number, so
     * discovery is out of the picture: this measures TRACKING only, value on top of perfect tuning.
     * <p>
     * What to watch in phase 2: throughput is capacity-bound for both arms (~{@value #DEGRADED_CAPACITY_SLOTS}
     * / serviceTime), but the static arm holds {@value #HEALTHY_CAPACITY_SLOTS} requests against a service
     * that can run {@value #DEGRADED_CAPACITY_SLOTS} - a 6x queue on a struggling dependency, which is how
     * outages get longer - while the adaptive target walks DOWN toward the new truth, and back UP in phase 3
     * (the recovery re-ask probe's whole purpose).
     */
    @Test
    @EnabledIfSystemProperty(named = DEMO_ENABLED_PROPERTY, matches = "true")
    @SneakyThrows
    void adaptiveTracksAMovingDownstreamThatStaticTuningCannot() {
        String topic = setupTopic(getClass().getSimpleName() + "-moving");
        log.info("Moving downstream: {} slots x {}ms, degrading to {} slots for the middle {}s of each arm. "
                        + "The static arm is tuned PERFECTLY for the healthy phase; the adaptive arm is seeded "
                        + "with that same perfect number.",
                HEALTHY_CAPACITY_SLOTS, SERVICE_TIME_MS, DEGRADED_CAPACITY_SLOTS, PHASE_SECONDS);
        log.info("Producing {} records...", String.format("%,d", MOVING_RECORD_COUNT));
        getKcu().produceMessages(topic, MOVING_RECORD_COUNT);

        // Which arms to run, so a re-measurement of one guess does not cost a rerun of all of them:
        // -Dpc.demo.arms=10,48,100,adaptive (the default runs all four).
        String[] arms = System.getProperty("pc.demo.arms", "10,48,100,adaptive").split(",");
        java.util.List<PhasedResult> results = new java.util.ArrayList<>();
        for (String arm : arms) {
            if (arm.trim().equals("adaptive")) {
                results.add(runPhasedArm(topic, "adaptive, seeded at 48", HEALTHY_CAPACITY_SLOTS * 2, true));
            } else {
                int slots = Integer.parseInt(arm.trim());
                String label = slots == HEALTHY_CAPACITY_SLOTS
                        ? "static, the lottery winner (48)"
                        : String.format("static, a guess (%d)", slots);
                results.add(runPhasedArm(topic, label, slots, false));
            }
        }

        StringBuilder table = new StringBuilder();
        for (PhasedResult r : results) {
            table.append('\n').append(r);
        }
        log.info("\n=== RESULTS: three {}s phases - healthy ({} slots) / DEGRADED ({} slots) / recovered ==={}\n",
                PHASE_SECONDS, HEALTHY_CAPACITY_SLOTS, DEGRADED_CAPACITY_SLOTS, table);
        log.info("Phase 2 is the argument: same capacity-bound throughput, but the static arm queued "
                + "{} requests against a service running {} - the adaptive arm took its pressure off.",
                HEALTHY_CAPACITY_SLOTS, DEGRADED_CAPACITY_SLOTS);
    }

    private static final class PhasedResult {
        final String name;
        final long[] completionsByPhase;
        final long[] requestMsByPhase;
        final Integer finalTarget;

        PhasedResult(String name, long[] completionsByPhase, long[] requestMsByPhase, Integer finalTarget) {
            this.name = name;
            this.completionsByPhase = completionsByPhase;
            this.requestMsByPhase = requestMsByPhase;
            this.finalTarget = finalTarget;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(String.format("%-30s", name + ":"));
            String[] labels = {"healthy", "DEGRADED", "recovered"};
            for (int p = 0; p < 3; p++) {
                double rate = completionsByPhase[p] / (double) PHASE_SECONDS;
                double avgReq = completionsByPhase[p] == 0 ? 0 : requestMsByPhase[p] / (double) completionsByPhase[p];
                sb.append(String.format(Locale.ROOT, "  %s %,5.0f msg/s @ %5.1fms", labels[p], rate, avgReq));
            }
            if (finalTarget != null) {
                sb.append(String.format("   final target %d", finalTarget));
            }
            return sb.toString();
        }
    }

    @SneakyThrows
    private PhasedResult runPhasedArm(String topic, String armName, int maxConcurrency, boolean adaptive) {
        log.info("\n=== {} (maxConcurrency {}{}) - {}s schedule ===", armName, maxConcurrency,
                adaptive ? ", ENFORCE, seed " + HEALTHY_CAPACITY_SLOTS : "", 3 * PHASE_SECONDS);

        // The downstream. Capacity moves by a thief thread hoarding permits - the service got slower;
        // nobody's configuration changed.
        Semaphore downstream = new Semaphore(HEALTHY_CAPACITY_SLOTS);
        long[] completions = new long[3];
        long[] requestMs = new long[3];
        java.util.concurrent.atomic.AtomicLongArray phaseCompletions = new java.util.concurrent.atomic.AtomicLongArray(3);
        java.util.concurrent.atomic.AtomicLongArray phaseRequestMs = new java.util.concurrent.atomic.AtomicLongArray(3);

        var builder = ParallelConsumerOptions.<String, String>builder()
                .consumer(getKcu().createNewConsumer(GroupOption.NEW_GROUP))
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .ordering(UNORDERED)
                .maxConcurrency(maxConcurrency);
        if (adaptive) {
            builder.adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                    .adaptiveConcurrencyInitialTarget(HEALTHY_CAPACITY_SLOTS);
        }
        var options = builder.build();
        PCModule<String, String> module = new PCModule<>(options);
        var pc = new ParallelEoSStreamProcessor<>(options, module);
        AdmissionController controller = adaptive ? module.admissionController() : null;
        pc.subscribe(UniSets.of(topic));

        long armStart = System.currentTimeMillis();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        // The degradation and the recovery, on the wall clock.
        scheduler.schedule(() -> {
            // One permit at a time: a BULK acquire on an unfair semaphore starves forever under constant
            // single-permit churn (there are never 40 simultaneously free), which silently cancelled the whole
            // degradation on the first run - both phase logs fired together at recovery time, net zero.
            for (int i = 0; i < HEALTHY_CAPACITY_SLOTS - DEGRADED_CAPACITY_SLOTS; i++) {
                downstream.acquireUninterruptibly();
            }
            log.info("   >>> downstream DEGRADED: {} -> {} slots", HEALTHY_CAPACITY_SLOTS, DEGRADED_CAPACITY_SLOTS);
        }, PHASE_SECONDS, TimeUnit.SECONDS);
        scheduler.schedule(() -> {
            downstream.release(HEALTHY_CAPACITY_SLOTS - DEGRADED_CAPACITY_SLOTS);
            log.info("   >>> downstream RECOVERED: {} -> {} slots", DEGRADED_CAPACITY_SLOTS, HEALTHY_CAPACITY_SLOTS);
        }, 2L * PHASE_SECONDS, TimeUnit.SECONDS);

        AtomicInteger lastTick = new AtomicInteger();
        java.util.concurrent.atomic.AtomicLong lastTickWorkMs = new java.util.concurrent.atomic.AtomicLong();
        AtomicInteger totalCompleted = new AtomicInteger();
        LongAdder totalRequestMs = new LongAdder();
        scheduler.scheduleAtFixedRate(() -> {
            int now = totalCompleted.get();
            int inWindow = now - lastTick.getAndSet(now);
            long workNow = totalRequestMs.sum();
            long workInWindow = workNow - lastTickWorkMs.getAndSet(workNow);
            log.info("   t+{}s  ~{} msg/s  avg req {}ms{}",
                    (System.currentTimeMillis() - armStart) / 1000, inWindow / 5,
                    inWindow == 0 ? "-" : String.format(Locale.ROOT, "%.1f", workInWindow / (double) inWindow),
                    controller == null ? "" : "  [target " + controller.currentTarget() + "]");
        }, 5, 5, TimeUnit.SECONDS);

        try {
            pc.poll(context -> {
                long start = System.currentTimeMillis();
                try {
                    downstream.acquire();
                    try {
                        Thread.sleep(SERVICE_TIME_MS);
                    } finally {
                        downstream.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    long dur = System.currentTimeMillis() - start;
                    int phase = (int) Math.min(2, (start - armStart) / (PHASE_SECONDS * 1000L));
                    phaseCompletions.incrementAndGet(phase);
                    phaseRequestMs.addAndGet(phase, dur);
                    totalRequestMs.add(dur);
                    totalCompleted.incrementAndGet();
                }
            });

            Thread.sleep(3L * PHASE_SECONDS * 1000);
        } finally {
            scheduler.shutdownNow();
            pc.closeDontDrainFirst();
        }

        for (int p = 0; p < 3; p++) {
            completions[p] = phaseCompletions.get(p);
            requestMs[p] = phaseRequestMs.get(p);
        }
        var result = new PhasedResult(armName, completions, requestMs,
                controller == null ? null : controller.currentTarget());
        log.info("=== {} ===", result);
        return result;
    }

    /** The "server": pure CPU, a data dependency the JIT cannot remove, no allocation. */
    private static long spinSink;

    private static void spinCount() {
        long acc = 0;
        for (long i = 0; i < CPU_COUNT_TO; i++) {
            acc += i ^ (acc >>> 7);
        }
        spinSink += acc; // published so the loop is observably live
    }

    private static final class ArmResult {
        final String name;
        final long elapsedMs;
        final double msgPerSec;
        final double avgRequestMs;
        final Integer finalTarget;

        ArmResult(String name, long elapsedMs, double msgPerSec, double avgRequestMs, Integer finalTarget) {
            this.name = name;
            this.elapsedMs = elapsedMs;
            this.msgPerSec = msgPerSec;
            this.avgRequestMs = avgRequestMs;
            this.finalTarget = finalTarget;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "%-34s %,7.0f msg/s   drained in %6.1fs   avg request %6.1fms%s",
                    name + ":", msgPerSec, elapsedMs / 1000.0, avgRequestMs,
                    finalTarget == null ? "" : String.format("   final target %d slot(s)", finalTarget));
        }
    }

    @SneakyThrows
    private ArmResult runArm(String topic, String armName, int maxConcurrency, boolean adaptive,
                             int recordCount, Runnable perRecordWork) {
        log.info("\n=== {} (maxConcurrency {}{}) - {} records ===", armName, maxConcurrency,
                adaptive ? ", ENFORCE, seed " + ADAPTIVE_SEED_SLOTS : "", String.format("%,d", recordCount));

        AtomicInteger completed = new AtomicInteger();
        LongAdder requestMs = new LongAdder();

        var builder = ParallelConsumerOptions.<String, String>builder()
                .consumer(getKcu().createNewConsumer(GroupOption.NEW_GROUP))
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .ordering(UNORDERED)
                .maxConcurrency(maxConcurrency);
        if (adaptive) {
            builder.adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                    .adaptiveConcurrencyInitialTarget(ADAPTIVE_SEED_SLOTS);
        }
        var options = builder.build();
        PCModule<String, String> module = new PCModule<>(options);
        var pc = new ParallelEoSStreamProcessor<>(options, module);
        // Resolve before poll() starts engine threads - the sibling ITs' determinism guard.
        AdmissionController controller = adaptive ? module.admissionController() : null;
        pc.subscribe(UniSets.of(topic));

        long armStart = System.currentTimeMillis();
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger lastTick = new AtomicInteger();
        java.util.concurrent.atomic.AtomicLong lastTickWorkMs = new java.util.concurrent.atomic.AtomicLong();
        ticker.scheduleAtFixedRate(() -> {
            int now = completed.get();
            int inWindow = now - lastTick.getAndSet(now);
            long workNow = requestMs.sum();
            long workInWindow = workNow - lastTickWorkMs.getAndSet(workNow);
            log.info("   t+{}s  {} done  ~{} msg/s  avg req {}ms{}",
                    (System.currentTimeMillis() - armStart) / 1000, String.format("%,d", now), inWindow / 5,
                    inWindow == 0 ? "-" : String.format(Locale.ROOT, "%.1f", workInWindow / (double) inWindow),
                    controller == null ? "" : "  [target " + controller.currentTarget() + "]");
        }, 5, 5, TimeUnit.SECONDS);

        try {
            pc.poll(context -> {
                long start = System.currentTimeMillis();
                try {
                    perRecordWork.run();
                } finally {
                    requestMs.add(System.currentTimeMillis() - start);
                    completed.incrementAndGet();
                }
            });

            waitAtMost(ofMinutes(10))
                    .pollInterval(Duration.ofSeconds(1))
                    .alias(armName + " drains the backlog")
                    .until(() -> completed.get() >= recordCount);
        } finally {
            ticker.shutdownNow();
            pc.closeDrainFirst();
        }

        long elapsed = System.currentTimeMillis() - armStart;
        var result = new ArmResult(armName, elapsed, recordCount / (elapsed / 1000.0),
                requestMs.sum() / (double) recordCount,
                controller == null ? null : controller.currentTarget());
        log.info("=== {} ===", result);
        return result;
    }
}
