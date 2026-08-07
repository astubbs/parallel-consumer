package io.confluent.parallelconsumer.dashboard.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.DashboardServer;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.chaostests.ChaosConductor;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.Scenario;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.ScenarioRunner;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.ScriptedFunction;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.ScriptedWorkload;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.WorkloadPublisher;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The one-command demo (R32): a broker, a topic, a fleet of Parallel Consumer instances, a workload that is
 * deliberately made to misbehave, and the dashboard serving all of it - started, driven through
 * {@link ShowcaseScenario} and shut down again, from a single entry point.
 * <p>
 * Run it via {@code bin/dashboard-demo.sh}. That script is the interface; this class is what it starts.
 *
 * <h2>Two modes, one code path</h2>
 * {@code LOOP} (the default) repeats the phase list until interrupted - the thing you watch. {@code --once}
 * performs a single sweep and exits non-zero if any phase's postcondition failed - the thing CI runs. Both go
 * through the same wiring, so the demo cannot drift away from the test that guards it.
 *
 * <h2>No new broker or client plumbing</h2>
 * The broker is {@link BrokerIntegrationTest}'s Testcontainers singleton and the clients are
 * {@link KafkaClientUtils}, reused rather than reimplemented. Topic creation in particular goes through
 * {@link KafkaClientUtils#createTopic} - a second copy of that logic is how a one-second timeout drifted in
 * and became a CI flake (AGENTS.md).
 *
 * <h2>The dashboard watches ONE instance</h2>
 * Exactly as a real deployment does: the dashboard is embedded in a single process. The protected first
 * instance carries the {@link MeterRegistry}, the {@link SnapshotPublisher} and the {@link DashboardServer};
 * the rest of the fleet exists so that joining and killing members makes that one instance rebalance.
 */
@Slf4j
public final class DemoMain {

    /** Enough partitions that the offset ribbon has rows, few enough that a rebalance is quick. */
    private static final int PARTITIONS = 8;
    /** Distinct keys the workload cycles - with KEY ordering, this is the shard count ceiling. */
    private static final int KEY_SPACE = 50;
    /** Ceiling on fleet size: the protected instance plus the joiners the membership phases add. */
    private static final int MAX_FLEET = 3;
    /** High enough that a slowed user function can visibly fill the pool. */
    private static final int MAX_CONCURRENCY = 32;
    private static final Duration PC_START_TIMEOUT = Duration.ofSeconds(60);

    private DemoMain() {
    }

    /** What the demo was asked to do. Built by {@link #parse(String[])} or directly by a test. */
    public static final class Options {
        private final ScenarioRunner.Mode mode;
        private final long seed;
        private final int port;

        public Options(ScenarioRunner.Mode mode, long seed, int port) {
            this.mode = mode == null ? ScenarioRunner.Mode.LOOP : mode;
            this.seed = seed;
            this.port = port;
        }

        public static Options once(long seed) {
            return new Options(ScenarioRunner.Mode.ONCE, seed, DashboardOptions.DEFAULT_PORT);
        }

        public ScenarioRunner.Mode getMode() {
            return mode;
        }

        public long getSeed() {
            return seed;
        }
    }

    /** What the demo did. {@link #getFailures()} empty means every phase produced what it declared. */
    public static final class Result {
        private final List<String> failures;
        private final String url;
        private final DashboardWatcher watcher;
        private final long seed;

        Result(List<String> failures, String url, DashboardWatcher watcher, long seed) {
            this.failures = Collections.unmodifiableList(new ArrayList<>(failures));
            this.url = url;
            this.watcher = watcher;
            this.seed = seed;
        }

        public List<String> getFailures() {
            return failures;
        }

        public String getUrl() {
            return url;
        }

        public DashboardWatcher getWatcher() {
            return watcher;
        }

        public long getSeed() {
            return seed;
        }
    }

    public static void main(String[] args) {
        Options options;
        try {
            options = parse(args);
        } catch (IllegalArgumentException badArgs) {
            System.err.println("dashboard-demo: " + badArgs.getMessage());
            System.err.println(usage());
            System.exit(2);
            return;
        }

        Instant startedAt = Instant.now();
        Result result;
        try {
            result = run(options);
        } catch (Exception e) {
            log.error("The dashboard demo failed to run", e);
            System.err.println("dashboard-demo: FAILED to run - " + e);
            System.exit(1);
            return;
        }

        printSummary(result, Duration.between(startedAt, Instant.now()));
        System.exit(result.getFailures().isEmpty() ? 0 : 1);
    }

    static Options parse(String[] args) {
        ScenarioRunner.Mode mode = ScenarioRunner.Mode.LOOP;
        long seed = new Random().nextLong();
        int port = DashboardOptions.DEFAULT_PORT;
        for (String arg : args) {
            if ("--once".equals(arg)) {
                mode = ScenarioRunner.Mode.ONCE;
            } else if (arg.startsWith("--seed=")) {
                seed = parseNumber(arg, "--seed=");
            } else if (arg.startsWith("--port=")) {
                port = (int) parseNumber(arg, "--port=");
            } else {
                throw new IllegalArgumentException("unrecognised argument '" + arg + "'");
            }
        }
        return new Options(mode, seed, port);
    }

    private static long parseNumber(String arg, String prefix) {
        String value = arg.substring(prefix.length());
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(prefix + " needs a number, got '" + value + "'");
        }
    }

    static String usage() {
        return "usage: dashboard-demo.sh [--once] [--seed=<long>] [--port=<int>]\n"
                + "  --once     one deterministic sweep; exits non-zero if a phase's postcondition failed\n"
                + "  --seed=N   replay a run exactly (the seed of every run is logged)\n"
                + "  --port=N   first port to try for the dashboard (it walks upward from there)";
    }

    /**
     * Start everything, run the scenario, shut everything down. Blocks until the scenario finishes ({@code
     * ONCE}) or the JVM is interrupted ({@code LOOP}).
     */
    public static Result run(Options options) {
        long seed = options.seed;
        String replayCommand = "bin/dashboard-demo.sh --seed=" + seed
                + (options.mode == ScenarioRunner.Mode.ONCE ? " --once" : "");
        log.info("=== dashboard demo: mode={} seed={} (replay: {}) ===", options.mode, seed, replayCommand);

        // The Testcontainers singleton the whole integration suite shares - referencing it starts it.
        KafkaContainer broker = BrokerIntegrationTest.kafkaContainer;
        KafkaClientUtils kcu = new KafkaClientUtils(broker);
        kcu.open();

        String topic = "dashboard-demo-" + Math.abs(new Random(seed).nextInt());
        kcu.createTopic(topic, PARTITIONS);
        log.info("Demo topic '{}' created with {} partitions on {}", topic, PARTITIONS, broker.getBootstrapServers());

        MeterRegistry registry = new SimpleMeterRegistry();
        AtomicLong consumed = new AtomicLong();
        ScriptedFunction userFunction = new ScriptedFunction(key -> consumed.incrementAndGet());
        // even before the scenario starts, the function must take SOME time or every occupancy panel reads
        // empty - see ShowcaseScenario.BASELINE_FUNCTION_DELAY for the measurement behind that
        userFunction.setDelay(ShowcaseScenario.BASELINE_FUNCTION_DELAY);

        ExecutorService pcExecutor = Executors.newWorkStealingPool();
        // Only the dashboard's own instance carries the registry: two instances publishing the same
        // topic/partition-tagged meters into one registry would collide, and the page would show a blend of
        // two processes rather than what one process is doing.
        ManagedPCInstance dashboardInstance = new ManagedPCInstance(
                instanceConfig(topic, registry), kcu, userFunction);

        WorkloadPublisher workload = new WorkloadPublisher(
                (key, value) -> kcu.getProducer().send(new ProducerRecord<>(topic, key, value), (meta, exception) -> {
                    if (exception != null) log.warn("Demo produce failed: {}", exception.toString());
                }),
                KEY_SPACE, "key-", ShowcaseScenario.STARTING_RATE);

        DashboardServer server = null;
        DashboardWatcher watcher = null;
        ChaosConductor conductor = null;
        List<String> failures = Collections.emptyList();
        String url = null;
        AtomicBoolean shutDown = new AtomicBoolean();

        try {
            dashboardInstance.start(pcExecutor);
            ParallelEoSStreamProcessor<String, String> pc = awaitStarted(dashboardInstance);

            SnapshotPublisher publisher = SnapshotPublisher.createAndRegister(pc, registry);
            server = new DashboardServer(publisher, registry,
                    DashboardOptions.builder().port(options.port).build()).start();
            url = server.getUrl();

            watcher = new DashboardWatcher(publisher::getCurrent);
            watcher.start();

            Scenario scenario = ShowcaseScenario.declare(watcher);
            announce(url, scenario, seed, options.mode, replayCommand);

            workload.start();

            conductor = ChaosConductor.builder()
                    .seed(seed)
                    .scenario(scenario)
                    .mode(options.mode)
                    .replayCommand(replayCommand)
                    .maxFleetSize(MAX_FLEET)
                    .pcExecutor(pcExecutor)
                    .instanceFactory(() -> new ManagedPCInstance(instanceConfig(topic, null), kcu, userFunction))
                    .protectedInstance(dashboardInstance)
                    .initialFleet(Collections.singletonList(dashboardInstance))
                    .workload(new ScriptedWorkload(workload, userFunction))
                    .build();

            if (options.mode == ScenarioRunner.Mode.ONCE) {
                failures = conductor.runToCompletion();
            } else {
                conductor.start();
                ChaosConductor running = conductor;
                DashboardWatcher runningWatcher = watcher;
                DashboardServer runningServer = server;
                // the hook does the WHOLE teardown, not just "stop the scenario" - once every shutdown hook
                // returns the JVM halts, so anything left for the finally below would be racing that halt and
                // Ctrl-C would leave the port bound and the clients open. The finally still runs and is a
                // no-op, because shutdown() is idempotent.
                awaitInterrupt(() -> shutdown(shutDown, running, workload, runningWatcher, runningServer,
                        pcExecutor, kcu));
            }
        } finally {
            shutdown(shutDown, conductor, workload, watcher, server, pcExecutor, kcu);
        }

        log.info("Demo finished: {} records processed by the fleet, {} postcondition failure(s)",
                consumed.get(), failures.size());
        return new Result(failures, url, watcher, seed);
    }

    private static ManagedPCInstance.Config instanceConfig(String topic, MeterRegistry registry) {
        return ManagedPCInstance.Config.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                // KEY ordering is what gives the shard view something to show: one shard per key, and a key
                // pinned to fail stalls its own shard while every other key streams past it.
                .order(ProcessingOrder.KEY)
                .inputTopic(topic)
                .maxConcurrency(MAX_CONCURRENCY)
                .meterRegistry(registry)
                .build();
    }

    private static ParallelEoSStreamProcessor<String, String> awaitStarted(ManagedPCInstance instance) {
        Instant deadline = Instant.now().plus(PC_START_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            ParallelEoSStreamProcessor<String, String> pc = instance.getParallelConsumer();
            if (pc != null) {
                return pc;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for the demo's Parallel Consumer", e);
            }
        }
        throw new IllegalStateException("the demo's Parallel Consumer did not start within " + PC_START_TIMEOUT
                + " - is the broker reachable?");
    }

    /**
     * LOOP mode's whole body: park until the JVM is asked to exit, then let the shutdown hook do the tearing
     * down. Ctrl-C therefore releases the broker, the fleet and the port instead of abandoning them.
     *
     * @param teardown the FULL shutdown, run inside the hook - see the call site for why it cannot be left to
     *                 the caller's finally block
     */
    private static void awaitInterrupt(Runnable teardown) {
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Interrupted - shutting the demo down");
            teardown.run();
            stopped.countDown();
        }, "dashboard-demo-sigint"));
        System.out.println();
        System.out.println("  Watching. Press Ctrl-C to stop.");
        System.out.println();
        try {
            stopped.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Idempotent, and every step is independently guarded: one failing close must not orphan the rest. */
    private static void shutdown(AtomicBoolean alreadyDone,
                                 ChaosConductor conductor,
                                 WorkloadPublisher workload,
                                 DashboardWatcher watcher,
                                 DashboardServer server,
                                 ExecutorService pcExecutor,
                                 KafkaClientUtils kcu) {
        if (!alreadyDone.compareAndSet(false, true)) return;
        quietly("stopping the scenario", () -> {
            if (conductor != null) conductor.stop();
        });
        quietly("stopping the workload publisher", workload::stop);
        quietly("stopping the dashboard watcher", () -> {
            if (watcher != null) watcher.close();
        });
        quietly("closing the dashboard server", () -> {
            if (server != null) server.close();
        });
        if (conductor != null) {
            for (ManagedPCInstance instance : conductor.getFleet()) {
                quietly("closing instance " + instance.getInstanceId(), () -> {
                    ParallelEoSStreamProcessor<String, String> pc = instance.getParallelConsumer();
                    if (pc != null && !pc.isClosedOrFailed()) pc.close();
                });
            }
        }
        quietly("shutting the PC executor down", pcExecutor::shutdownNow);
        quietly("closing the Kafka clients", kcu::close);
    }

    private static void quietly(String what, Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            log.warn("Demo shutdown: {} failed (continuing): {}", what, e.toString());
        }
    }

    /**
     * The banner. A demo whose URL scrolls past in a wall of Kafka client logging is a demo nobody watches, so
     * this goes to stdout, framed, with the running order underneath it.
     */
    private static void announce(String url, Scenario scenario, long seed, ScenarioRunner.Mode mode, String replay) {
        StringBuilder out = new StringBuilder();
        out.append('\n').append(rule()).append('\n');
        out.append("  PARALLEL CONSUMER DASHBOARD\n\n");
        out.append("      ").append(url).append("\n\n");
        out.append("  scenario : ").append(scenario.getName()).append("  (mode ").append(mode).append(")\n");
        out.append("  seed     : ").append(seed).append("\n");
        out.append("  replay   : ").append(replay).append('\n');
        out.append(rule()).append('\n');
        out.append("  What you are about to be shown, in order:\n");
        int n = 1;
        for (String description : ShowcaseScenario.phaseDescriptions(scenario)) {
            out.append("   ").append(n++).append(". ").append(description).append('\n');
        }
        out.append(rule()).append('\n');
        System.out.println(out);
    }

    private static void printSummary(Result result, Duration elapsed) {
        StringBuilder out = new StringBuilder();
        out.append('\n').append(rule()).append('\n');
        out.append("  DASHBOARD DEMO ").append(result.getFailures().isEmpty() ? "PASSED" : "FAILED").append('\n');
        out.append("  duration : ").append(elapsed.getSeconds()).append("s\n");
        out.append("  seed     : ").append(result.getSeed()).append("\n");
        out.append("  served at: ").append(result.getUrl()).append('\n');
        out.append(rule()).append('\n');
        if (result.getWatcher() != null) {
            for (String line : result.getWatcher().summarise()) {
                out.append("  ").append(line).append('\n');
            }
        }
        if (!result.getFailures().isEmpty()) {
            out.append(rule()).append('\n');
            out.append("  ").append(result.getFailures().size()).append(" phase postcondition(s) failed:\n");
            for (String failure : result.getFailures()) {
                out.append("   - ").append(failure).append('\n');
            }
        }
        out.append(rule()).append('\n');
        System.out.println(out);
    }

    private static String rule() {
        StringBuilder rule = new StringBuilder("  ");
        for (int i = 0; i < 76; i++) {
            rule.append('-');
        }
        return rule.toString();
    }
}
