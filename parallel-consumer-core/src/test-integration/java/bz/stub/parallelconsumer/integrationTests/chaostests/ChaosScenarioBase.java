package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest;
import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.platform.commons.support.AnnotationSupport;

import java.time.Duration;
import java.time.Instant;
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
import java.util.stream.Collectors;

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
abstract class ChaosScenarioBase extends BrokerIntegrationTest<String, String> implements ChaosSeed.Holder {

    /**
     * <b>Diagnostic only - never a way to make a scenario pass.</b> Set
     * {@code -Dchaos.diagnoseStallRecovery=true} to answer the one question a gating run structurally
     * cannot: when a probe violation fires, does whatever it caught <b>ever</b> recover, or is it
     * wedged forever?
     * <p>
     * A gating run destroys that evidence at the moment of detection - {@code failFast} aborts the
     * wait on the first violation, so unbounded and merely-slow are indistinguishable from the data it
     * leaves behind. In this mode a scenario's completion wait (built by {@link #diagnosableWait})
     * does not bail on a violation and instead watches for {@link #effectiveDiagnosticQuietCap}
     * instead, with a scenario supplying per-poll consumption progress via
     * {@link #logDiagnosticProgress}. The discriminator is which way the wait ends: the backlog
     * drains (the finding was bounded) or it times out with consumption flat (unbounded - lost state,
     * a partition paused and never resumed, or a lost wakeup).
     * <p>
     * <b>It cannot turn a red run green.</b> {@code assertScenarioSlos} still asserts the probe's
     * violations are empty after the wait, whichever way the wait ended, so a run that trips the probe
     * still fails - it just fails having recorded what happened next. Off by default; the gating
     * configuration is byte-for-byte unchanged when the property is absent.
     * <p>
     * Lives here (rather than on one scenario family) so every scenario can be asked the same
     * question - it was first proven on {@code AbstractRevokeUnderWorkScenario} (where it collapsed
     * the whole {@code CLASS2_STALL} line - see that class's "Calibration status" javadoc), and moved
     * here because {@code ChaosChurnStormIT} needed the identical capability rather than a drifted copy
     * of it - that scenario's own "Calibration status" javadoc carries what the diagnostic went on
     * to establish there.
     */
    protected static final boolean DIAGNOSE_STALL_RECOVERY = Boolean.getBoolean("chaos.diagnoseStallRecovery");

    /**
     * How long {@link #DIAGNOSE_STALL_RECOVERY} may watch - owned by {@link DiagnosticQuietCap}, which
     * holds the requested cap, the teardown reserve, and the arithmetic that fits one inside the
     * scenario's {@code @Timeout}. It lives outside this hierarchy so it can be tested without booting
     * Kafka; that class's javadoc explains why that is not merely a preference.
     * <p>
     * A diagnostic that promises a watch longer than the enclosing {@code @Timeout} does not deliver a
     * shorter watch - it delivers an UNINTERPRETABLE one, because JUnit's kill is neither of the two
     * outcomes the experiment distinguishes ("drained" / "did not drain"), and a killed run reads like
     * one that ended for its own reasons. Shortening the watch to fit converts that into a real, if
     * smaller, negative result, and names the number to raise for a longer one.
     * <p>
     * Read from the annotation rather than duplicating the literal per subclass, so raising a
     * scenario's annotation raises its watch with no second edit to forget. Resolved through
     * {@link AnnotationSupport#findAnnotation}, which is what JUnit itself uses - so this seam tracks
     * the enforcer rather than approximating it, including meta-present and interface-declared cases.
     * {@code @Timeout} IS {@code @Inherited} (verified against the pinned junit-jupiter-api), so a
     * ceiling hoisted onto a shared superclass is still found.
     * <p>
     * <b>The one real gap is a METHOD-level {@code @Timeout}</b>, which overrides the class-level one
     * in JUnit and is invisible to a class lookup. No scenario uses one today - every {@code @Test}
     * here delegates straight to a driver method - but a method-level override would restore the
     * silent kill this guard exists to prevent. An absent annotation means no ceiling, so the request
     * stands.
     *
     * @param methodStart when the {@code @Timeout} clock started - the top of the {@code @Test} method,
     *                     since that is JUnit's own reference point
     */
    protected Duration effectiveDiagnosticQuietCap(Instant methodStart) {
        Duration ceiling = AnnotationSupport.findAnnotation(getClass(), Timeout.class)
                // toMillis() rather than TimeUnit.toChronoUnit(): the latter is Java 9+, and this
                // module compiles to Java 8 bytecode through Jabel.
                .map(t -> Duration.ofMillis(t.unit().toMillis(t.value())))
                .orElse(null);
        return DiagnosticQuietCap.within(ceiling, Duration.between(methodStart, Instant.now()),
                getClass().getSimpleName());
    }

    /**
     * Extra scenario-specific guidance logged once when the diagnostic engages, on top of the shared
     * boilerplate {@link #diagnosableWait} always logs. No-op by default - a scenario overrides this to
     * point at its own prior-art (e.g. a "Calibration status" javadoc recording an earlier recovery
     * diagnostic on the same shape), so a reader of THIS run's log is told whether the result is a
     * re-derivation or a first answer.
     */
    protected void logDiagnosticContext() {
    }

    /**
     * The terminal wait for a scenario's completion phase: normally capped at {@code gatedCap} with
     * {@code failFast} on any probe violation ({@code failFastDescription} names it in the awaitility
     * report) - or, under {@code -Dchaos.diagnoseStallRecovery=true}, capped by
     * {@link #effectiveDiagnosticQuietCap} instead and with NO fail-fast, so a violation does not abort
     * the wait. Watching what happens AFTER a violation fires is the entire point of that mode - see
     * {@link #DIAGNOSE_STALL_RECOVERY}.
     *
     * @param alias               the awaitility alias, surfaced in a timeout report
     * @param methodStart         when the {@code @Timeout} clock started, for
     *                            {@link #effectiveDiagnosticQuietCap}
     * @param gatedCap            how long the NORMAL (non-diagnostic) wait may run
     * @param failFastDescription the reason string attached to the NORMAL wait's {@code failFast}
     * @param probe               the running scenario's probe, polled for {@code hasViolations}
     */
    protected ConditionFactory diagnosableWait(String alias, Instant methodStart, Duration gatedCap,
                                                String failFastDescription, ProgressProbe probe) {
        ConditionFactory wait = await().alias(alias).pollInterval(Duration.ofSeconds(2));
        if (DIAGNOSE_STALL_RECOVERY) {
            Duration diagnosticCap = effectiveDiagnosticQuietCap(methodStart);
            // Deliberately no failFast: the whole point is to keep watching AFTER the violation.
            log.warn("=== chaos.diagnoseStallRecovery ACTIVE - watch cap {} and no fail-fast. This is "
                    + "a DIAGNOSTIC run: violations are still asserted at the end, so this cannot make "
                    + "the test pass. ===", diagnosticCap);
            logDiagnosticContext();
            return wait.atMost(diagnosticCap);
        }
        return wait.atMost(gatedCap).failFast(failFastDescription, probe::hasViolations);
    }

    /**
     * Per-poll diagnostic progress line for a {@link #diagnosableWait} wait - only emitted under
     * {@code -Dchaos.diagnoseStallRecovery=true}, so it costs nothing in a gating run. Both counters are
     * needed because a completion counter alone cannot tell "nothing is finishing" from "nothing is
     * happening": a fleet all sitting inside a heavy-tail dwell reads as a flat consumed line while
     * fully busy. {@code inFlight} (started-minus-consumed) is the difference that makes a flat line
     * interpretable.
     *
     * @param phaseLabel      names the wait in the log line (e.g. "quiet phase", "run")
     * @param expectedMessages the scenario's total backlog size, for the consumed/expected ratio
     */
    protected void logDiagnosticProgress(String phaseLabel, long expectedMessages, AtomicLong totalStarted,
                                          AtomicLong totalConsumed, ProgressProbe probe, boolean done) {
        if (!DIAGNOSE_STALL_RECOVERY) {
            return;
        }
        long started = totalStarted.get();
        long consumed = totalConsumed.get();
        log.info("[diagnose] {}: consumed={}/{} started={} inFlight={} violations={} observations={} done={}",
                phaseLabel, consumed, expectedMessages, started, started - consumed,
                probe.getViolations().size(), probe.getObservations().size(), done);
    }

    /**
     * A processing function with a heavy tail: every {@code heavyEvery}-th record dwells
     * {@code heavySleep} NON-interruptibly (sleep-until-deadline). PC's close path force-interrupts
     * stuck workers after ~5s (awaitTermination -> shutdownNow), which would cap every drain/stall at
     * seconds and shrink the windows the probes discriminate on. Real-world slow work often ignores
     * interrupts too (JDBC, native calls, CPU loops).
     */
    protected ManagedPCInstance newInstance(ManagedPCInstance.Config config,
                                            int heavyEvery, Duration heavySleep,
                                            AtomicLong totalConsumed, AtomicLong totalStarted,
                                            Queue<String> allConsumed) {
        KeyOrderLedger.Recorder recorder = orderRecorder();
        return new ManagedPCInstance(config, getKcu(), (incarnationId, context) -> {
            // recorded FIRST and released LAST, so the bracket is the user function's real execution
            // window - what KeyOrderLedger's overlap half is asserting about
            KeyOrderLedger.Delivery delivery = recorder == null ? null : recorder.started(incarnationId, context);
            // Counted at ENTRY, where totalConsumed is counted at exit. The pair is the measurement:
            // a completion counter alone reads a fleet busy inside HEAVY_SLEEP as a flat line, which
            // is indistinguishable from a fleet that is genuinely stuck. started-minus-consumed is
            // work in flight, and it separates "nothing is finishing" from "nothing is happening".
            totalStarted.incrementAndGet();
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

    /**
     * The seed this run is replayable from - {@code null} until {@link #resolveSeed()} runs inside the
     * test method. Held on the instance, not just logged, so {@code AmbientProbeExtension} can lift it
     * into the failure autopsy: the run-start console line carrying it does not survive a truncated CI
     * log, and the autopsy travels in the uploaded failsafe XML instead
     * ({@code docs/solutions/workflow-issues/gh-run-view-log-truncation.md}).
     */
    @Getter
    private ChaosSeed chaosSeed;

    /** Resolve AND record the run's schedule seed - see {@link #getChaosSeed()} for why recording it
     * matters. {@code -Dchaos.seed=<long>} replays a schedule; unset = random. */
    protected ChaosSeed resolveSeed() {
        this.chaosSeed = ChaosSeed.resolve();
        return chaosSeed;
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
        /** Incremented at user-function ENTRY - see {@link #newInstance} for why the pair is needed. */
        AtomicLong totalStarted;
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
        AtomicLong totalStarted = new AtomicLong();
        Queue<String> allConsumed = new ConcurrentLinkedQueue<>();
        ExecutorService pcExecutor = Executors.newWorkStealingPool();

        // pre-produce a backlog, keep the rest flowing in the background (mirrors the existing
        // MultiInstanceRebalanceTest shape; suite-local orchestration because runTest there is private)
        Set<String> expectedKeys = new ConcurrentSkipListSet<>();
        int preProduce = (int) (expectedMessages * preProduceFraction);
        produceRange(topic, 0, preProduce, expectedKeys);

        // protected first member - chaos never touches it, so the group always has a healthy survivor
        ManagedPCInstance pc0 = newInstance(pcConfig, heavyEvery, heavySleep, totalConsumed, totalStarted, allConsumed);
        pc0.start(pcExecutor);
        await().atMost(30, SECONDS).until(() -> totalConsumed.get() > 100);

        Thread producerThread = new Thread(() -> produceRange(topic, preProduce, expectedMessages, expectedKeys),
                "chaos-background-producer");
        producerThread.start();

        List<ManagedPCInstance> initialFleet = new ArrayList<>();
        initialFleet.add(pc0);
        for (int i = 1; i < initialFleetSize; i++) {
            ManagedPCInstance pc = newInstance(pcConfig, heavyEvery, heavySleep, totalConsumed, totalStarted, allConsumed);
            initialFleet.add(pc);
            pc.start(pcExecutor);
        }

        ProgressProbe probe = new ProgressProbe(getKcu(), getKcu().getGroupId(), topic,
                totalConsumed::get, expectedMessages);
        return new FleetBootstrap(pcExecutor, totalConsumed, totalStarted, allConsumed, expectedKeys,
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
                        fleet.getTotalConsumed(), fleet.getTotalStarted(), fleet.getAllConsumed()))
                .protectedInstance(fleet.getPc0())
                .initialFleet(fleet.getInitialFleet())
                .observer(fleet.getProbe());
    }

    /** Arm the probe, then unleash chaos. Wired here (not per-scenario) so every scenario's probe
     * watches per-instance progress ({@code INSTANCE_STALL/NO_WORK_COMPLETED}) over the conductor's
     * LIVE fleet view - a supplier, because JOIN_NEW grows the fleet mid-run. */
    protected void startRun(ProgressProbe probe, ChaosConductor conductor) {
        probe.withInstanceProgress(() -> conductor.getFleet().stream()
                .map(ProgressProbe.InstanceProgressView::of)
                .collect(Collectors.toList()));
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
        // Observations are printed alongside violations because they are the ONLY surface they have:
        // they never fail a run, so a green build says nothing about them unless the summary does.
        log.info("Run summary: consumed={} (unique tracking via correctness ledger), probe violations={}, "
                        + "non-gating observations={}",
                totalConsumed.get(), violations, probe.getObservations());
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
