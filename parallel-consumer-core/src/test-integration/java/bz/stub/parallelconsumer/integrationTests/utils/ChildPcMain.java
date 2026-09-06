package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.utils.TimeUtils;
import bz.stub.parallelconsumer.navigator.ConservationLedger;
import bz.stub.parallelconsumer.navigator.NavigatorView;
import bz.stub.parallelconsumer.navigator.PartitionShareResourceAllocator;
import bz.stub.parallelconsumer.navigator.ResourceContract;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;

/**
 * The child JVM's entry point: one {@link ParallelEoSStreamProcessor} on its own consumer, built from
 * {@link ChildPcOptions} parsed off the command line, whose user function produces one record per dispatch to
 * the output topic (key = instance id) so the parent counts firings on the BROKER's clock (KTD8). Launched only
 * by {@link ChildPcProcess}; never run by a lane directly.
 * <p>
 * <b>Protocol with the launcher.</b> Stdout carries {@value #READY_LINE} once the processor is polling (the
 * launcher's liveness probe), then one {@value #DASHBOARD_PREFIX} line per second for a demo to render, and
 * whatever the harness logger emits. Stdin carries {@value #STOP_COMMAND} for a graceful stop; EOF on stdin
 * (the parent died) stops the child too, so an orphan never outlives its test. SIGTERM ({@code Process#destroy})
 * runs the same shutdown through a hook; SIGKILL ({@code destroyForcibly}) emits nothing, by definition. Exit 0
 * after a clean stop, 1 on any exception (stack trace on stderr), 2 on an argument error.
 * <p>
 * <b>The clock (KTD9).</b> With a non-zero offset the module's {@link PCModule#clock()} is overridden with
 * {@link Clock#offset} - the {@code ClockedModule} pattern from the admission lifecycle unit test, re-created
 * here because {@code src/test} is invisible to this source set. Production code has no clock knob; the skew
 * reaches the allocator only through the module seam.
 * <p>
 * <b>The ledger.</b> On stop, after the processor closes, one {@link ChildLedgerRecord} per tagged resource is
 * produced to the ledger topic: the allocator's own {@link ConservationLedger} counters plus the share sampler's
 * sum (see {@link ChildLedgerRecord}). The producer is flushed before exit, so the record is on the broker
 * before the exit code is.
 * <p>
 * <b>Session timeout.</b> Defaults to the broker's floor with a matching heartbeat (KTD10), so a killed child is
 * rebalanced away as fast as the broker allows; the parent's convergence deadline is built on that number.
 *
 * @author Antony Stubbs
 * @see ChildPcProcess
 * @see FiringLedger
 */
public final class ChildPcMain {

    /** Printed once the processor is polling - the launcher's liveness probe. */
    public static final String READY_LINE = "CHILD-PC READY";

    /** Prefix of the per-second dashboard line: {@code CHILD-PC DASHBOARD t=<s> fired=<n> share=<f> credits=<c>}. */
    public static final String DASHBOARD_PREFIX = "CHILD-PC DASHBOARD";

    /** Prefix of every stdout-spam line the {@code spamStdoutLines} knob prints. */
    public static final String SPAM_PREFIX = "CHILD-PC SPAM";

    /** The stdin line that asks for a graceful stop. */
    public static final String STOP_COMMAND = "stop";

    /** The message an early-exit self-test looks for. */
    public static final String DELIBERATE_FAILURE_MESSAGE = "deliberate failure before subscribe (harness self-test)";

    /** How often the share sampler reads the view - several times per quantum, so no quantum index is missed. */
    private static final Duration SAMPLE_INTERVAL = Duration.ofMillis(200);

    /** A module whose clock carries the injected offset (KTD9); everything else real. */
    static final class OffsetClockModule extends PCModule<String, String> {
        private final Clock clock;

        OffsetClockModule(ParallelConsumerOptions<String, String> options, Duration offset) {
            super(options);
            this.clock = offset.isZero() ? TimeUtils.getClock() : Clock.offset(TimeUtils.getClock(), offset);
        }

        @Override
        public Clock clock() {
            return clock;
        }
    }

    private ChildPcMain() {
    }

    public static void main(String[] args) {
        ChildPcOptions options;
        try {
            options = ChildPcOptions.parse(args);
        } catch (RuntimeException e) {
            e.printStackTrace(System.err);
            System.exit(2);
            return;
        }
        try {
            run(options);
            System.exit(0);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void run(ChildPcOptions options) throws InterruptedException {
        if (options.getSpamStdoutLines() > 0) {
            spamStdout(options.getSpamStdoutLines());
        }
        if (options.isFailBeforeSubscribe()) {
            throw new IllegalStateException(DELIBERATE_FAILURE_MESSAGE);
        }

        Duration offset = Duration.ofMillis(options.getClockOffsetMillis());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties(options));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties(options));
        ParallelConsumerOptions<String, String> pcOptions = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .ordering(PARTITION)
                .pcInstanceTag(options.getInstanceId())
                .resourceTags(options.getResourceTags())
                .resourceContracts(options.getContracts())
                .build();
        OffsetClockModule module = new OffsetClockModule(pcOptions, offset);
        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(pcOptions, module);

        AtomicLong fired = new AtomicLong();
        ShareSampler sampler = new ShareSampler(module, options);
        AtomicBoolean stopping = new AtomicBoolean();

        pc.subscribe(new HashSet<>(options.getInputTopics()));
        pc.poll(context -> {
            fired.incrementAndGet();
            String value = context.getSingleConsumerRecord().topic() + "-" + context.getSingleConsumerRecord().partition()
                    + "@" + context.offset() + " clock=" + module.clock().instant().toEpochMilli();
            // the callback owns the outcome: a failed send is logged there, so the future itself has nothing to add
            Future<RecordMetadata> ignoredOutputSend = producer.send(
                    new ProducerRecord<>(options.getOutputTopic(), options.getInstanceId(), value),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("output produce failed: " + exception);
                        }
                    });
        });

        System.out.println(READY_LINE);
        System.out.flush();

        Thread samplerThread = daemon("child-pc-share-sampler", sampler::loop);
        samplerThread.start();
        Thread dashboardThread = daemon("child-pc-dashboard", () -> dashboardLoop(module, options, fired, stopping));
        dashboardThread.start();

        AtomicReference<RuntimeException> closeFailure = new AtomicReference<>();
        Runnable shutdown = () -> {
            if (!stopping.compareAndSet(false, true)) {
                return;
            }
            try {
                pc.close();
            } catch (RuntimeException e) {
                // still emit the ledger - it is the diagnostic - but the exit code below says the close failed,
                // so the parent's "stops cleanly" assertion fails instead of accepting a broken child's books
                closeFailure.set(e);
                System.err.println("close failed: " + e);
            }
            emitLedger(producer, module, options, sampler, fired.get());
            producer.flush();
            producer.close(Duration.ofSeconds(10));
        };
        Runtime.getRuntime().addShutdownHook(new Thread(shutdown, "child-pc-sigterm"));

        awaitStopSignal(options);
        shutdown.run();
        RuntimeException failed = closeFailure.get();
        if (failed != null) {
            throw new IllegalStateException("the processor did not close cleanly", failed);
        }
    }

    // ------------------------------------------------------------------
    // Lifetime
    // ------------------------------------------------------------------

    /**
     * Returns on a {@value #STOP_COMMAND} line, on stdin EOF (the parent is gone), or after {@code runSeconds}.
     * A daemon thread blocks in {@code readLine}, because EOF on a pipe is only ever reported by a blocking
     * read: {@code ready()} is false and {@code available()} is zero on a closed pipe with nothing buffered, so
     * a polling loop over them never learns the parent died and an orphan runs forever - the harness's own
     * self-test closes stdin without a stop line to prove this path.
     */
    private static void awaitStopSignal(ChildPcOptions options) throws InterruptedException {
        CountDownLatch stop = new CountDownLatch(1);
        Thread reader = daemon("child-pc-stdin", () -> {
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            try {
                String line;
                while ((line = stdin.readLine()) != null) {
                    if (STOP_COMMAND.equals(line.trim())) {
                        break;
                    }
                    System.err.println("ignoring unknown stdin command '" + line + "'");
                }
            } catch (IOException e) {
                // stdin closed under us - the parent is gone; fall through to the stop
            }
            stop.countDown();
        });
        reader.start();
        if (options.getRunSeconds() > 0) {
            boolean ignoredSignalled = stop.await(options.getRunSeconds(), TimeUnit.SECONDS); // either way, stop
        } else {
            stop.await();
        }
    }

    private static void emitLedger(KafkaProducer<String, String> producer, OffsetClockModule module,
                                   ChildPcOptions options, ShareSampler sampler, long fired) {
        if (options.getResourceTags().isEmpty()) {
            send(producer, options, ChildLedgerRecord.untagged(options.getInstanceId(), fired));
            return;
        }
        PartitionShareResourceAllocator allocator = module.partitionShareAllocator().orElseThrow(() ->
                new IllegalStateException("a tagged child built no partition-share allocator"));
        // The processor is closed, so nothing mints again: one synchronous sample now covers the index the
        // last read minted in, which the periodic sampler may not have reached yet - the run's end effect.
        sampler.sample();
        Instant now = module.clock().instant();
        for (String resource : options.getResourceTags()) {
            ConservationLedger ledger = allocator.conservationLedger(resource, now);
            send(producer, options, ChildLedgerRecord.of(options.getInstanceId(), ledger,
                    sampler.sharesSummed(resource), sampler.quantaObserved(resource), fired));
        }
    }

    private static void send(KafkaProducer<String, String> producer, ChildPcOptions options,
                             ChildLedgerRecord record) {
        String line = record.format();
        System.out.println("CHILD-PC LEDGER " + line);
        // the callback owns the outcome, and the caller flushes before exit - the future has nothing to add
        Future<RecordMetadata> ignoredLedgerSend = producer.send(
                new ProducerRecord<>(options.getLedgerTopic(), options.getInstanceId(), line),
                (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("ledger produce failed: " + exception);
                    }
                });
    }

    // ------------------------------------------------------------------
    // Observation threads
    // ------------------------------------------------------------------

    /**
     * Samples the child's EXACT entitlement per quantum index - {@link PartitionShareResourceAllocator#entitledCredits},
     * what the allocator mints for that index - on the module clock, so a skewed child's indexes are its own,
     * and keeps the largest value seen per index (the entitlement is fixed at the index's start, so the max
     * only guards against a sample taken astride a boundary). Not the view's {@code creditsPerQuantum}: that is
     * the rotation-AVERAGED gauge, and a conservation sum over it is off by the rotation's phase - the churn
     * ladder failed a rung by exactly that deviation before this sampler read the exact value. The ledger
     * emission takes one more sample synchronously after the processor has closed, so the index of the last
     * mint is always in the sum: before that, a child stopped inside the first 200 ms of an index could mint
     * the index and never sample it, and a full holder's index is worth the whole grant, not one credit.
     */
    private static final class ShareSampler {
        private final OffsetClockModule module;
        private final ChildPcOptions options;
        private final Map<String, Map<Long, Double>> creditsByQuantum = new ConcurrentHashMap<>();

        ShareSampler(OffsetClockModule module, ChildPcOptions options) {
            this.module = module;
            this.options = options;
        }

        void loop() {
            while (true) {
                sample();
                try {
                    TimeUnit.MILLISECONDS.sleep(SAMPLE_INTERVAL.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        void sample() {
            Optional<PartitionShareResourceAllocator> allocator = module.partitionShareAllocator();
            if (!allocator.isPresent()) {
                return; // an untagged child holds no allocator and has no share to sample
            }
            Instant now = module.clock().instant();
            for (String resource : options.getResourceTags()) {
                ResourceContract contract = options.contractNamed(resource);
                long quantumIndex = Math.floorDiv(now.toEpochMilli(), contract.getQuantum().toMillis());
                double entitled = allocator.get().entitledCredits(resource, quantumIndex);
                creditsByQuantum.computeIfAbsent(resource, ignored -> new ConcurrentHashMap<>())
                        .merge(quantumIndex, entitled, Math::max);
            }
        }

        double sharesSummed(String resource) {
            return creditsByQuantum.getOrDefault(resource, Collections.emptyMap()).values().stream().mapToDouble(d -> d).sum();
        }

        long quantaObserved(String resource) {
            return creditsByQuantum.getOrDefault(resource, Collections.emptyMap()).size();
        }
    }

    private static void dashboardLoop(OffsetClockModule module, ChildPcOptions options, AtomicLong fired,
                                      AtomicBoolean stopping) {
        long lastFired = 0;
        long second = 0;
        while (!stopping.get()) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            second++;
            long now = fired.get();
            String share = "-";
            String credits = "-";
            if (!options.getResourceTags().isEmpty()) {
                NavigatorView view = module.navigatorView();
                String resource = options.getResourceTags().get(0);
                share = view.shareFraction(resource).isPresent()
                        ? String.format(Locale.ROOT, "%.3f", view.shareFraction(resource).getAsDouble()) : "-";
                credits = view.creditsPerQuantum(resource).isPresent()
                        ? String.format(Locale.ROOT, "%.3f", view.creditsPerQuantum(resource).getAsDouble()) : "-";
            }
            System.out.println(DASHBOARD_PREFIX + " t=" + second + " fired=" + (now - lastFired)
                    + " share=" + share + " credits=" + credits);
            System.out.flush();
            lastFired = now;
        }
    }

    private static void spamStdout(int lines) {
        StringBuilder padding = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            padding.append('x');
        }
        for (int i = 0; i < lines; i++) {
            System.out.println(SPAM_PREFIX + " " + i + " " + padding);
        }
        System.out.flush();
    }

    private static Thread daemon(String name, Runnable body) {
        Thread thread = new Thread(body, name);
        thread.setDaemon(true);
        return thread;
    }

    // ------------------------------------------------------------------
    // Client configuration
    // ------------------------------------------------------------------

    private static Properties consumerProperties(ChildPcOptions options) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getGroupId());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, options.getInstanceId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, options.getAssignor().className());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, options.getSessionTimeoutMs());
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, options.getHeartbeatIntervalMs());
        return props;
    }

    private static Properties producerProperties(ChildPcOptions options) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, options.getInstanceId() + "-out");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0); // every firing leaves at once: its broker timestamp IS the firing
        return props;
    }
}
