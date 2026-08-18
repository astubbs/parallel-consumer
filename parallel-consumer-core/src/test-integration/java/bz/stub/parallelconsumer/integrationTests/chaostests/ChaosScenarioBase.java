package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest;
import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * Shared scaffolding for Chaos Pain Suite scenarios (W1 churn storm, W4 revoke-under-work, W5 key
 * order, ...): the keyed producer, the heavy-tailed NON-interruptible user function, coverage checks,
 * and fleet settling.
 * Scenario classes own their chaos shape (conductor weights/ticks, fleet size, commit mode) - this base
 * owns the mechanics every scenario shares, so scenarios can't drift apart on them.
 */
@Slf4j
abstract class ChaosScenarioBase extends BrokerIntegrationTest<String, String> {

    /**
     * A processing function with a heavy tail: every {@code heavyEvery}-th record dwells
     * {@code heavySleep} NON-interruptibly (sleep-until-deadline). PC's close path force-interrupts
     * stuck workers after ~5s (awaitTermination -> shutdownNow), which would cap every drain/stall at
     * seconds and shrink the windows the probes discriminate on. Real-world slow work often ignores
     * interrupts too (JDBC, native calls, CPU loops).
     */
    protected ManagedPCInstance newInstance(ManagedPCInstance.Config config,
                                            int heavyEvery, Duration heavySleep,
                                            AtomicLong totalConsumed, Queue<String> allConsumed) {
        KeyOrderLedger.Recorder recorder = orderRecorder();
        return new ManagedPCInstance(config, getKcu(), (incarnationId, context) -> {
            // recorded FIRST and released LAST, so the bracket is the user function's real execution
            // window - what KeyOrderLedger's overlap half is asserting about
            KeyOrderLedger.Delivery delivery = recorder == null ? null : recorder.started(incarnationId, context);
            try {
                String identity = identityOf(context);
                if (isHeavyKey(identity, heavyEvery)) {
                    long deadline = System.currentTimeMillis() + heavySleep.toMillis();
                    boolean interrupted = false;
                    long left;
                    while ((left = deadline - System.currentTimeMillis()) > 0) {
                        try {
                            Thread.sleep(Math.min(left, 1_000));
                        } catch (InterruptedException e) {
                            interrupted = true; // note it, keep dwelling until the deadline
                        }
                    }
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
                totalConsumed.incrementAndGet();
                allConsumed.add(identity);
            } finally {
                if (delivery != null) {
                    recorder.finished(delivery);
                }
            }
        });
    }

    /** identity format is "<prefix>-N"; every heavyEvery-th record is heavy. */
    protected static boolean isHeavyKey(String key, int heavyEvery) {
        int n = Integer.parseInt(key.substring(key.indexOf('-') + 1));
        return n > 0 && n % heavyEvery == 0;
    }

    /**
     * The RECORD IDENTITY the correctness ledger tracks - unique per produced record, and the value the
     * heavy tail is spaced on. It is the Kafka key by default because the shared scenarios produce a
     * unique key per record; a scenario that repeats keys (which is what makes a per-key ordering claim
     * testable at all) must move the identity into the value and override this trio together.
     *
     * @see #keyFor the Kafka record key, which for such a scenario is NOT the identity
     * @see #identityFor the produce-side half of the same mapping
     */
    protected String identityOf(PollContext<String, String> context) {
        return context.key();
    }

    /** The Kafka record key for produced record {@code i} - the shard, and so the ordering unit. */
    protected String keyFor(int i) {
        return "key-" + i;
    }

    /** The ledger identity of produced record {@code i} - must agree with {@link #identityOf}. */
    protected String identityFor(int i) {
        return keyFor(i);
    }

    /**
     * The per-key ordering recorder for this scenario, or {@code null} when the scenario makes NO
     * ordering claim - which is the correct answer for every {@code UNORDERED} scenario, where PC
     * promises nothing about per-key order and recording would only produce a history
     * {@link KeyOrderLedger#check} would rightly call vacuous.
     * <p>
     * An overriding scenario must return the SAME recorder every call (a field, not a fresh instance):
     * every fleet member records into it, and {@link #assertScenarioSlos} replays the one history.
     */
    protected KeyOrderLedger.Recorder orderRecorder() {
        return null;
    }

    /** Coverage check is expensive at scale - callers only evaluate once the counter says it's plausible. */
    protected boolean allConsumedCovers(Set<String> expectedKeys, Queue<String> allConsumed) {
        var unique = new HashSet<>(allConsumed);
        return unique.containsAll(expectedKeys);
    }

    protected void produceRange(String topic, int fromInclusive, int toExclusive, Set<String> expectedKeys) {
        try (Producer<String, String> producer = getKcu().createNewProducer(false)) {
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            for (int i = fromInclusive; i < toExclusive; i++) {
                expectedKeys.add(identityFor(i));
                sends.add(producer.send(new ProducerRecord<>(topic, keyFor(i), "v-" + i)));
            }
            for (Future<RecordMetadata> send : sends) {
                send.get();
            }
            log.info("Produced [{}..{})", fromInclusive, toExclusive);
        } catch (Exception e) {
            throw new RuntimeException("Producer failed at range [" + fromInclusive + ".." + toExclusive + ")", e);
        }
    }

    /** Close every fleet member that chaos left running, classifying (not asserting on) close errors. */
    protected void settleFleet(ChaosConductor conductor) {
        for (ManagedPCInstance pc : conductor.getFleet()) {
            try {
                // let any stopAsync background close finish first - close() below is then a no-op
                long waited = 0;
                while (pc.isClosePending() && waited < 15_000) {
                    Thread.sleep(100);
                    waited += 100;
                }
                if (pc.getParallelConsumer() != null && !pc.getParallelConsumer().isClosedOrFailed()) {
                    pc.getParallelConsumer().close();
                }
            } catch (Exception e) {
                log.warn("Settle-close of instance {}: {}", pc.getInstanceId(), e.getMessage());
            }
        }
    }

    /** Resolve the run's schedule seed: {@code -Dchaos.seed=<long>} replays a schedule; unset = random
     * seed (always logged via {@link #replayCommand}). */
    protected static long resolveSeed() {
        String seedProp = System.getProperty("chaos.seed");
        return seedProp == null ? RandomUtils.nextLong() : Long.parseLong(seedProp);
    }

    /** The FULL replay invocation, not just the seed - a raw CI log must be self-sufficient to
     * reproduce (the chaos tag is excluded by default, so the seed alone is not enough). */
    protected static String replayCommand(long seed) {
        return "./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true"
                + " -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=" + seed;
    }

    /**
     * Everything {@link #bootstrapFleet} wires up: consumption tracking, the pre-produced backlog (with
     * a background producer streaming the rest), the protected first member plus the started initial
     * fleet, and an UNSTARTED probe watching it all. Scenarios unpack what they need and wire their own
     * {@link ChaosConductor} - the chaos SHAPE stays per-scenario, the bootstrap mechanics stay shared.
     */
    @Value
    protected static class FleetBootstrap {
        ExecutorService pcExecutor;
        AtomicLong totalConsumed;
        Queue<String> allConsumed;
        Set<String> expectedKeys;
        Thread producerThread;
        ManagedPCInstance pc0;
        List<ManagedPCInstance> initialFleet;
        ProgressProbe probe;
    }

    /**
     * Shared scenario prologue: pre-produce {@code preProduceFraction} of the backlog, start the
     * protected first member and wait for first consumption, stream the remaining records from a
     * background producer, start the rest of the initial fleet, and construct the probe. The probe is
     * returned UNSTARTED - scenarios apply their per-scenario toggles first, then {@code probe.start()}.
     */
    protected FleetBootstrap bootstrapFleet(String topic, ManagedPCInstance.Config pcConfig,
                                            int expectedMessages, double preProduceFraction,
                                            int initialFleetSize, int heavyEvery, Duration heavySleep) {
        // fleet-wide consumption tracking (the probe's watermark + the ledger's evidence)
        AtomicLong totalConsumed = new AtomicLong();
        Queue<String> allConsumed = new ConcurrentLinkedQueue<>();
        ExecutorService pcExecutor = Executors.newWorkStealingPool();

        // pre-produce a backlog, keep the rest flowing in the background (mirrors the existing
        // MultiInstanceRebalanceTest shape; suite-local orchestration because runTest there is private)
        Set<String> expectedKeys = new ConcurrentSkipListSet<>();
        int preProduce = (int) (expectedMessages * preProduceFraction);
        produceRange(topic, 0, preProduce, expectedKeys);

        // protected first member - chaos never touches it, so the group always has a healthy survivor
        ManagedPCInstance pc0 = newInstance(pcConfig, heavyEvery, heavySleep, totalConsumed, allConsumed);
        pc0.start(pcExecutor);
        await().atMost(30, SECONDS).until(() -> totalConsumed.get() > 100);

        Thread producerThread = new Thread(() -> produceRange(topic, preProduce, expectedMessages, expectedKeys),
                "chaos-background-producer");
        producerThread.start();

        List<ManagedPCInstance> initialFleet = new ArrayList<>();
        initialFleet.add(pc0);
        for (int i = 1; i < initialFleetSize; i++) {
            ManagedPCInstance pc = newInstance(pcConfig, heavyEvery, heavySleep, totalConsumed, allConsumed);
            initialFleet.add(pc);
            pc.start(pcExecutor);
        }

        ProgressProbe probe = new ProgressProbe(getKcu(), getKcu().getGroupId(), topic,
                totalConsumed::get, expectedMessages);
        return new FleetBootstrap(pcExecutor, totalConsumed, allConsumed, expectedKeys,
                producerThread, pc0, initialFleet, probe);
    }

    /**
     * Pre-wires the fleet-plumbing half of a {@link ChaosConductor} (executor, instance factory,
     * protected member, initial fleet, probe observer). Scenarios chain their chaos SHAPE onto the
     * returned builder - seed, tick range, weights, join bias - and {@code build()}.
     */
    protected ChaosConductor.ChaosConductorBuilder conductorFor(FleetBootstrap fleet,
                                                                ManagedPCInstance.Config pcConfig,
                                                                int heavyEvery, Duration heavySleep,
                                                                int maxFleetSize) {
        return ChaosConductor.builder()
                .maxFleetSize(maxFleetSize)
                .pcExecutor(fleet.getPcExecutor())
                .instanceFactory(() -> newInstance(pcConfig, heavyEvery, heavySleep,
                        fleet.getTotalConsumed(), fleet.getAllConsumed()))
                .protectedInstance(fleet.getPc0())
                .initialFleet(fleet.getInitialFleet())
                .observer(fleet.getProbe());
    }

    /** Arm the probe, then unleash chaos. */
    protected void startRun(ProgressProbe probe, ChaosConductor conductor) {
        probe.start();
        conductor.start();
    }

    /** Shared finally-block epilogue: stop chaos and the probe, join the background producer, settle
     * the fleet, kill the executor, log the run summary. Runs on both the pass and fail path - it must
     * only tear down and report, never assert (asserting here would mask the primary failure). */
    protected void settleRun(ChaosConductor conductor, ProgressProbe probe, Thread producerThread,
                             ExecutorService pcExecutor, AtomicLong totalConsumed) throws InterruptedException {
        conductor.stop(); // also joins outstanding drain threads (bounded) - fleet is quiesced after
        List<String> violations = probe.stop();
        producerThread.join(10_000);
        settleFleet(conductor);
        pcExecutor.shutdownNow();
        log.info("Run summary: consumed={} (unique tracking via correctness ledger), probe violations={}",
                totalConsumed.get(), violations);
    }

    /** The suite-wide verdict, identical for every scenario by design: probes must be violation-free
     * (each violation carries its own diagnosis), every instance's terminal failure cause must be
     * classified, and the correctness ledger must balance - no loss ever, duplicates bounded per
     * disturbance, and per-key order kept where the scenario claims it ({@link #orderRecorder}). Every
     * message carries the full replay command - a raw CI log must be self-sufficient to reproduce. */
    protected void assertScenarioSlos(ProgressProbe probe, ChaosConductor conductor, String replayCmd,
                                      Set<String> expectedKeys, Queue<String> allConsumed) {
        assertWithMessage("chaos probes must be violation-free (each violation carries the diagnosis; " +
                "replay: %s)", replayCmd)
                .that(probe.getViolations()).isEmpty();

        // end-of-run canary sweep: every instance's terminal failure cause must be classified - an
        // instance stopped and never restarted would otherwise carry an unexpected error silently
        // (restart is the only other place classification happens)
        List<String> unexpectedFailures = new ArrayList<>();
        for (ManagedPCInstance pc : conductor.getFleet()) {
            var consumer = pc.getParallelConsumer();
            Exception cause = consumer == null ? null : consumer.getFailureCause();
            if (cause != null && !ManagedPCInstance.isExpectedCloseException(cause)) {
                unexpectedFailures.add("instance " + pc.getInstanceId() + ": " + cause);
            }
        }
        assertWithMessage("no instance may end the run with an unclassified failure cause (replay: %s)",
                replayCmd)
                .that(unexpectedFailures).isEmpty();

        // correctness ledger: no loss ever; duplicates bounded per disturbance; and - for a scenario that
        // makes the claim - per-key order kept inside every instance+partition+epoch window
        int disturbances = conductor.getDisturbanceCount();
        List<String> ledgerProblems = new ArrayList<>(ProgressProbe.ledger(expectedKeys, allConsumed,
                Math.max(disturbances, 1), /* perDisturbanceAllowance */ 5_000));
        ledgerProblems.addAll(KeyOrderLedger.checkIfRecording(orderRecorder()));
        assertWithMessage("correctness ledger must balance (replay: %s)", replayCmd)
                .that(ledgerProblems).isEmpty();
    }
}
