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
 * asked three times against a downstream whose true capacity NOBODY CONFIGURES INTO PC: a service that can
 * handle {@value #DOWNSTREAM_CAPACITY_SLOTS} requests at once, each taking {@value #SERVICE_TIME_MS}ms
 * (modelled as a semaphore around a sleep, per the demo ledger's simulated-work rule - no HTTP server).
 * <p>
 * The three arms, identical workload, fresh consumer group each:
 * <ol>
 * <li><b>The default guess</b> - static {@code maxConcurrency} left at the library default of
 * {@value #DEFAULT_GUESS_SLOTS}. Half the downstream sits idle; the backlog drains at half speed.</li>
 * <li><b>The classic hand-tuned number</b> - static {@code maxConcurrency} {@value #HAND_TUNED_SLOTS}, the
 * classic demo's setting. Full speed, but three records queue at the downstream for every one it serves -
 * the extra 68 slots buy queue wait, not throughput. Watch the avg downstream-wait column.</li>
 * <li><b>Adaptive</b> - {@code adaptiveConcurrencyMode(ENFORCE)}, seeded at {@value #ADAPTIVE_SEED_SLOTS}
 * slots with the same {@value #HAND_TUNED_SLOTS} ceiling. Nobody tells it {@value #DOWNSTREAM_CAPACITY_SLOTS};
 * it discovers the knee from throughput alone. Watch the {@code [target n]} column climb and settle, and the
 * {@code Adaptive concurrency} INFO lines narrate every decision (probe chatter on the
 * {@code AdmissionController.probe} child logger).</li>
 * </ol>
 * <p>
 * Off by default, same discipline as the classic {@code Demo}: this package path is failsafe-collected, so a
 * multi-minute measurement with no assertions must not run on every build. Run it with:
 * <pre>./mvnw verify -pl parallel-consumer-core -am -Dit.test=AdaptiveConcurrencyDemo -Dpc.demo=true \
 *     -Dtest=skipall -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dfailsafe.failIfNoSpecifiedTests=false</pre>
 */
@Slf4j
public class AdaptiveConcurrencyDemo extends BrokerIntegrationTest<String, String> {

    /** Set to {@code true} to run the demo; mirrors the classic {@code Demo}'s flag. */
    static final String DEMO_ENABLED_PROPERTY = "pc.demo";

    /** What the downstream can really do at once. The number the whole demo is about - and no arm is told it. */
    static final int DOWNSTREAM_CAPACITY_SLOTS = 32;
    /** Service time per request once a downstream slot is held. */
    static final int SERVICE_TIME_MS = 20;
    /** The library's default maxConcurrency - what you get when you never tune. */
    static final int DEFAULT_GUESS_SLOTS = 16;
    /** The classic demo's hand-tuned setting. */
    static final int HAND_TUNED_SLOTS = 100;
    /** Adaptive starts nearly serial, so the discovery is visible rather than pre-solved by the seed. */
    static final int ADAPTIVE_SEED_SLOTS = 2;
    /** Enough backlog that every arm runs long enough to read, ~40s-2min per arm at these settings. */
    static final int RECORD_COUNT = 60_000;

    @Test
    @EnabledIfSystemProperty(named = DEMO_ENABLED_PROPERTY, matches = "true")
    @SneakyThrows
    void adaptiveDiscoversWhatTheClassicDemoHardCodes() {
        String topic = setupTopic(getClass().getSimpleName());
        log.info("Downstream capacity nobody configures into PC: {} concurrent, {}ms each = {} msg/s ceiling",
                DOWNSTREAM_CAPACITY_SLOTS, SERVICE_TIME_MS, 1000 / SERVICE_TIME_MS * DOWNSTREAM_CAPACITY_SLOTS);
        log.info("Producing {} records...", String.format("%,d", RECORD_COUNT));
        getKcu().produceMessages(topic, RECORD_COUNT);

        ArmResult defaultGuess = runArm(topic, "static, the default guess", DEFAULT_GUESS_SLOTS, false);
        ArmResult handTuned = runArm(topic, "static, the classic hand-tuned 100", HAND_TUNED_SLOTS, false);
        ArmResult adaptive = runArm(topic, "adaptive, told nothing", HAND_TUNED_SLOTS, true);

        log.info("\n=== RESULTS: {} records each, downstream truth is {} slots x {}ms ===\n{}\n{}\n{}\n",
                String.format("%,d", RECORD_COUNT), DOWNSTREAM_CAPACITY_SLOTS, SERVICE_TIME_MS,
                defaultGuess, handTuned, adaptive);
        log.info("The adaptive arm was configured with the same ceiling as the hand-tuned arm and a seed of {} - "
                        + "the concurrency it ran at, it discovered.", ADAPTIVE_SEED_SLOTS);
    }

    private static final class ArmResult {
        final String name;
        final long elapsedMs;
        final double msgPerSec;
        final double avgDownstreamWaitMs;
        final Integer finalTarget;

        ArmResult(String name, long elapsedMs, double msgPerSec, double avgDownstreamWaitMs, Integer finalTarget) {
            this.name = name;
            this.elapsedMs = elapsedMs;
            this.msgPerSec = msgPerSec;
            this.avgDownstreamWaitMs = avgDownstreamWaitMs;
            this.finalTarget = finalTarget;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "%-36s %,6.0f msg/s   drained in %5.1fs   avg downstream wait %5.1fms%s",
                    name + ":", msgPerSec, elapsedMs / 1000.0, avgDownstreamWaitMs,
                    finalTarget == null ? "" : String.format("   discovered target %d slot(s)", finalTarget));
        }
    }

    @SneakyThrows
    private ArmResult runArm(String topic, String armName, int maxConcurrency, boolean adaptive) {
        log.info("\n=== {} (maxConcurrency {}{}) - {} records ===", armName, maxConcurrency,
                adaptive ? ", ENFORCE, seed " + ADAPTIVE_SEED_SLOTS : "", String.format("%,d", RECORD_COUNT));

        // The downstream: capacity is a property of the SERVICE, so it lives outside every arm's configuration.
        Semaphore downstream = new Semaphore(DOWNSTREAM_CAPACITY_SLOTS);
        AtomicInteger completed = new AtomicInteger();
        LongAdder downstreamWaitMs = new LongAdder();

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
        ticker.scheduleAtFixedRate(() -> {
            int now = completed.get();
            int inWindow = now - lastTick.getAndSet(now);
            log.info("   t+{}s  {} done  ~{} msg/s{}",
                    (System.currentTimeMillis() - armStart) / 1000, String.format("%,d", now), inWindow / 5,
                    controller == null ? "" : "  [target " + controller.currentTarget() + "]");
        }, 5, 5, TimeUnit.SECONDS);

        try {
            pc.poll(context -> {
                long waitStart = System.currentTimeMillis();
                try {
                    downstream.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                try {
                    downstreamWaitMs.add(System.currentTimeMillis() - waitStart);
                    Thread.sleep(SERVICE_TIME_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    downstream.release();
                }
                completed.incrementAndGet();
            });

            waitAtMost(ofMinutes(6))
                    .pollInterval(Duration.ofSeconds(1))
                    .alias(armName + " drains the backlog")
                    .until(() -> completed.get() >= RECORD_COUNT);
        } finally {
            ticker.shutdownNow();
            pc.closeDrainFirst();
        }

        long elapsed = System.currentTimeMillis() - armStart;
        var result = new ArmResult(armName, elapsed, RECORD_COUNT / (elapsed / 1000.0),
                downstreamWaitMs.sum() / (double) RECORD_COUNT,
                controller == null ? null : controller.currentTarget());
        log.info("=== {} ===", result);
        return result;
    }
}
