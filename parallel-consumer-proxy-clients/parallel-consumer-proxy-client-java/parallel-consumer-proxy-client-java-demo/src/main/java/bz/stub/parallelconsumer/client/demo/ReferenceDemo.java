package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.Outcome;
import bz.stub.parallelconsumer.client.ParallelConsumerClient;
import bz.stub.parallelconsumer.client.ProcessingOrder;
import bz.stub.parallelconsumer.client.RecordProcessor;
import bz.stub.parallelconsumer.client.direct.DirectParallelConsumerClient;
import bz.stub.parallelconsumer.client.grpc.GrpcParallelConsumerClient;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import bz.stub.parallelconsumer.proxy.integrationTests.SidecarProcess;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <b>The reference demo.</b> The same records through the Apache Kafka client, through Parallel
 * Consumer in process, and through Parallel Consumer reached over the sidecar - and it is the
 * artifact every per-language demo is transcribed from (plan unit U35, governed by KTD40).
 *
 * <h2>Why Java is the seed, and what only Java can show</h2>
 *
 * For every other language the comparison is "that language's own Kafka client versus that language
 * over the sidecar", and the two arms differ in more than the engine. Java is the one place where
 * the sidecar hop can be priced with <b>engine, workload, broker and host all held identical</b>,
 * because every arm runs in this JVM against this broker with the same sleep as the user function.
 * Whatever the gap turns out to be, that is what crossing the process boundary costs - not a
 * difference in client libraries.
 *
 * <h2>The arms, and what each pair isolates</h2>
 *
 * <ul>
 *   <li><b>AK core</b> - a plain {@code KafkaConsumer}, one record at a time. "AK core" is always
 *       spelled out because bare "core" reads as {@code parallel-consumer-core}; see
 *       {@code CONCEPTS.md}. This is the arm every language has.</li>
 *   <li><b>pc-core</b> - {@code ParallelEoSStreamProcessor} directly, no client library, no
 *       sidecar. The engine as a Java application already uses it today.</li>
 *   <li><b>java-direct</b> - {@code DirectParallelConsumerClient}: the client library's surface
 *       with the engine bound in process behind it. Against pc-core this prices <b>what reaching
 *       the engine through the client library costs</b>.</li>
 *   <li><b>java-grpc</b> - {@code GrpcParallelConsumerClient} over a real sidecar this process
 *       spawns. Against java-direct - same library surface, same broker, same workload - this
 *       prices <b>what it costs to reach the engine over a socket instead of in process</b>, which
 *       is the number the whole language-proxy design turns on. Read it as the wire hop <i>plus the
 *       sidecar's own dispatch model</i>, not the wire alone: the two sides run the engine with
 *       different in-flight pipelining, so attributing the whole gap to the socket would overstate
 *       it.</li>
 *   <li><b>java-raw-grpc</b> - the protocol spoken by hand, with no client library at all. A
 *       control arm: against java-grpc it prices <b>the client library itself</b>. It is here
 *       because an earlier version of this demo was <em>only</em> this arm, which is precisely why
 *       it measured the engine and said nothing about the client. Kept as a control, not as an
 *       example - no application should write this, and no other language's demo needs it.</li>
 * </ul>
 *
 * <b>Only the first two of these are part of the contract other languages keep.</b> The rest exist
 * because one JVM can hold all five at once; a language whose only Kafka client is its own has
 * nothing to compare a wrapper or a raw wire against.
 *
 * <h2>Two replays, because they answer different questions</h2>
 *
 * The small replay puts every arm over identical records - the honest side-by-side. The big replay
 * drops the arms that do not go parallel and shows what the engine sustains once start-up stops
 * dominating; at a serial-arm-sized volume a parallel arm is already finished, so a single volume
 * can only ever report one of the two.
 *
 * <h2>Run it</h2>
 *
 * <pre>parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo/run.sh</pre>
 *
 * With no broker supplied it starts one in a container. Inside its own container it is handed a
 * compose sibling instead, because a demo container is never granted the host Docker socket.
 *
 * @author Antony Stubbs
 */
@Slf4j
public final class ReferenceDemo {

    /** No arm may take longer than this before the demo calls it stalled rather than slow. */
    private static final Duration ARM_BUDGET = Duration.ofMinutes(10);

    private static final String AK_CORE = "AK core";

    /**
     * The capability the client library declares (WireMapping's own {@code DISPATCH_CAPABILITY}).
     * Named here so the hand-written arm and the test that guards it cannot drift from each other
     * by a spelling.
     */
    static final String DISPATCH_CAPABILITY = "dispatch";

    private final DemoOptions options;

    private final DemoBroker broker;

    private final String topic;

    private ReferenceDemo(DemoOptions options, DemoBroker broker, String topic) {
        this.options = options;
        this.broker = broker;
        this.topic = topic;
    }

    public static void main(String[] args) throws Exception {
        if (DemoOptions.isHelpRequested(args)) {
            usage();
            return;
        }

        DemoOptions options;
        try {
            options = DemoOptions.parse(args, System.getenv());
        } catch (IllegalArgumentException e) {
            log.error("{}", e.getMessage());
            usage();
            // a misspelled flag must not be reported as a result for settings nobody asked for
            System.exit(2);
            return;
        }

        try (DemoBroker broker = DemoBroker.resolve(options.bootstrap().orElse(null))) {
            String topic = options.topic()
                    .orElseGet(() -> "pc-demo-" + System.nanoTime());
            runFor(options, broker, topic);
        }
    }

    private static void usage() {
        log.info("\nusage: run.sh [options]\n"
                + "  --records N        records in the comparison replay   (default 2000)\n"
                + "  --delay-ms N       simulated work per record, ms      (default 2)\n"
                + "  --concurrency N    max in-flight records              (default 100)\n"
                + "  --partitions N     partitions on the demo topic       (default 10)\n"
                + "  --replay-factor N  big replay = records x N; 1 skips  (default 20)\n"
                + "  --bootstrap ADDR   an existing broker; omit to start one\n"
                + "  --topic NAME       an existing topic; omit to create one\n"
                + "\nEvery flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.\n"
                + "Flags beat the environment beats the defaults.");
    }

    /**
     * Runs the whole demo and hands back every arm's result.
     *
     * Returns the results rather than only printing them, so that {@code ReferenceDemoIT} can
     * assert what the arms actually did. It is the single entry point for both callers, so the
     * test drives the same code the reader runs rather than a parallel path that could pass while
     * the real one is broken.
     * <p>
     * Public because this module's convention puts integration tests in their own
     * {@code integrationTests} package - the rule that stops surefire silently not collecting them -
     * and this module publishes no artifact, so the wider visibility costs nothing.
     */
    public static List<ArmResult> runFor(DemoOptions options, DemoBroker broker, String topic)
            throws Exception {
        return new ReferenceDemo(options, broker, topic).run();
    }

    private List<ArmResult> run() throws Exception {
        log.info("\nEffective configuration:\n  {}\n  topic = {}", options, topic);

        broker.ensureTopic(topic, options.partitions());
        broker.seed(topic, 0, options.records());

        var small = new ArrayList<ArmResult>();
        small.add(akCore(options.records()));
        small.add(pcCore(options.records()));
        small.add(javaDirect(options.records()));
        small.add(javaGrpc(options.records()));
        small.add(javaRawGrpc(options.records()));
        report("Small replay - every arm over the same " + options.records()
                + " records (the comparison)", small, baselineOf(small), false);

        if (!options.bigReplayWanted()) {
            log.info("\nBig replay skipped (--replay-factor {}).", options.replayFactor());
            return small;
        }

        int total = options.bigReplayRecords();
        broker.seed(topic, options.records(), total);

        // AK core is excluded here because it does not go parallel: it would need
        // total * delayMs milliseconds to finish a backlog the other arms clear in seconds, and a
        // demo that makes a reader wait that long to learn nothing new is not worth the wall clock.
        var big = new ArrayList<ArmResult>();
        big.add(pcCore(total));
        big.add(javaDirect(total));
        big.add(javaGrpc(total));
        big.add(javaRawGrpc(total));
        report("Big replay - " + total + " records, parallel arms only (AK core is serial and would"
                + " take " + (total * options.delayMs() / 1000) + "s+)", big, baselineOf(small), true);

        var everything = new ArrayList<ArmResult>(small);
        everything.addAll(big);
        return everything;
    }

    private static ArmResult baselineOf(List<ArmResult> results) {
        return results.stream().filter(r -> AK_CORE.equals(r.arm())).findFirst().orElse(null);
    }

    /** The serial arm: one record at a time, the same sleep, in this JVM. */
    private ArmResult akCore(int target) {
        log.info("\n=== {} starting over {} records ===", AK_CORE, target);
        var config = new Properties();
        config.putAll(broker.consumerProperties(groupId("ak-core")));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        int processed = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(topic));
            // The clock starts AFTER the consumer is built and stops before it closes, because this
            // arm is the denominator of every ratio in both tables and no other arm charges itself
            // for client construction or teardown.
            long startedAt = System.nanoTime();
            long deadline = startedAt + ARM_BUDGET.toNanos();
            while (processed < target) {
                // The one arm that does not wait on a latch still needs the budget ARM_BUDGET
                // promises, or a backlog shorter than the target spins here forever with no output.
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(AK_CORE + " stalled at " + processed
                            + " of " + target);
                }
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                for (var ignored : polled) {
                    ThreadUtils.sleepQuietly(options.delayMs());
                    processed++;
                }
            }
            return finished(AK_CORE, startedAt, processed);
        }
    }

    /** The engine on its own, as a Java application uses it today - no client library, no sidecar. */
    private ArmResult pcCore(int target) throws InterruptedException {
        log.info("\n=== pc-core starting over {} records ===", target);
        var config = new Properties();
        config.putAll(broker.consumerProperties(groupId("pc-core")));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        var processed = new AtomicInteger();
        var done = new CountDownLatch(1);
        var engine = new ParallelEoSStreamProcessor<byte[], byte[]>(
                ParallelConsumerOptions.<byte[], byte[]>builder()
                        .consumer(new KafkaConsumer<byte[], byte[]>(config))
                        .maxConcurrency(options.maxConcurrency())
                        .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                        .build());
        try {
            engine.subscribe(Collections.singletonList(topic));
            long startedAt = System.nanoTime();
            engine.poll(context -> {
                ThreadUtils.sleepQuietly(options.delayMs());
                countTowards(processed, target, done);
            });
            return awaited("pc-core", startedAt, processed, done, target);
        } finally {
            engine.close();
        }
    }

    /** The client library with the engine bound in process behind it. */
    private ArmResult javaDirect(int target) throws InterruptedException {
        log.info("\n=== java-direct starting over {} records ===", target);
        var processed = new AtomicInteger();
        var done = new CountDownLatch(1);
        try (ParallelConsumerClient client = DirectParallelConsumerClient.builder()
                .options(clientOptions(groupId("java-direct")))
                .build()) {
            long startedAt = System.nanoTime();
            client.poll(sleepingProcessor(processed, target, done));
            return awaited("java-direct", startedAt, processed, done, target);
        }
    }

    /**
     * The client library over a real sidecar - the arm the whole design exists for.
     * <p>
     * On this path the application does no Kafka I/O: it spawns a binary, receives records over a
     * socket, runs its own function on them, and reports outcomes back, while the sidecar owns the
     * consumer, the producer, the group membership and the offsets. That is a claim about the
     * <em>path</em>, not about this process - the same JVM seeds the topic and runs the AK core and
     * pc-core arms with ordinary Kafka clients. A genuinely foreign application carries no Kafka
     * client library at all, which is the property this arm stands in for.
     */
    private ArmResult javaGrpc(int target) throws Exception {
        log.info("\n=== java-grpc starting over {} records ===", target);
        var processed = new AtomicInteger();
        var done = new CountDownLatch(1);
        try (SidecarProcess sidecar = SidecarProcess.spawn()) {
            try (ParallelConsumerClient client = GrpcParallelConsumerClient.builder()
                    .port(sidecar.port())
                    .options(clientOptions(groupId("java-grpc")))
                    .build()) {
                long startedAt = System.nanoTime();
                client.poll(sleepingProcessor(processed, target, done));
                return awaited("java-grpc", startedAt, processed, done, target);
            }
        }
    }

    /**
     * The control arm: the same sidecar, the same work, the protocol spoken by hand.
     * <p>
     * <b>This is not an example to copy.</b> It exists so that java-grpc minus this arm is a number
     * rather than an assumption - it prices the client library itself. An application writes
     * {@link #javaGrpc}; nobody writes this.
     */
    private ArmResult javaRawGrpc(int target) throws Exception {
        log.info("\n=== java-raw-grpc starting over {} records ===", target);
        var processed = new AtomicInteger();
        var done = new CountDownLatch(1);

        try (SidecarProcess sidecar = SidecarProcess.spawn()) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("127.0.0.1", sidecar.port()).usePlaintext().build();
            var workers = Executors.newFixedThreadPool(options.maxConcurrency());
            try {
                var requests = new AtomicReference<StreamObserver<ClientMessage>>();
                // A lock of its own, because a gRPC StreamObserver is not safe for concurrent
                // onNext and every worker reports through this one. It used to be the
                // AtomicReference above that was synchronized on, which SpotBugs rightly rejects
                // (JLM_JSR166_UTILCONCURRENT_MONITORENTER): locking a java.util.concurrent object
                // reads as if the atomic were providing the mutual exclusion, and it is not.
                final Object reportLock = new Object();
                var stream = ProxyServiceGrpc.newStub(channel).session(new StreamObserver<ProxyMessage>() {
                    @Override
                    public void onNext(ProxyMessage message) {
                        if (!message.hasDispatch()) {
                            return;
                        }
                        for (var record : message.getDispatch().getRecordsList()) {
                            // The user function, run on this application's own workers - the engine
                            // never learns what it is (KTD4).
                            workers.submit(() -> {
                                ThreadUtils.sleepQuietly(options.delayMs());
                                synchronized (reportLock) {
                                    requests.get().onNext(ClientMessage.newBuilder()
                                            .setReport(Report.newBuilder()
                                                    .setToken(record.getToken())
                                                    .setSuccess(Report.Success.newBuilder()))
                                            .build());
                                }
                                countTowards(processed, target, done);
                            });
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        // The demo closes the channel as soon as the target is reached, which the
                        // stream then reports back as UNAVAILABLE. Reporting our own teardown as a
                        // failure would teach a reader to expect an error at the end of a healthy
                        // run, so it is only an error if we were not already finished.
                        if (done.getCount() > 0) {
                            log.error("Session failed", t);
                            done.countDown();
                        }
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
                requests.set(stream);

                long startedAt = System.nanoTime();
                // Ordering is set EXPLICITLY, and leaving it out is not a harmless omission: the
                // field is optional, unspecified means "take
                // parallel-consumer-core's default", and that default is KEY. This arm therefore ran key-ordered while the other four ran unordered
                // for as long as the line was missing - a hand-written protocol message gets no
                // help from the client library, which is itself part of what this control arm
                // demonstrates.
                var configure = rawConfigure(options, topic,
                        broker.consumerProperties(groupId("java-raw-grpc")));
                stream.onNext(ClientMessage.newBuilder().setConfigure(configure).build());

                return awaited("java-raw-grpc", startedAt, processed, done, target);
            } finally {
                workers.shutdownNow();
                channel.shutdownNow();
            }
        }
    }

    /**
     * The user function for the two client-library arms, identical to the sleep every other arm
     * runs so that the arms differ by transport and nothing else.
     */
    private RecordProcessor sleepingProcessor(AtomicInteger processed, int target, CountDownLatch done) {
        return record -> {
            ThreadUtils.sleepQuietly(options.delayMs());
            countTowards(processed, target, done);
            return Outcome.success();
        };
    }

    private ClientOptions clientOptions(String groupId) {
        return libraryOptions(options, topic, broker.consumerProperties(groupId));
    }

    /**
     * What the client-library arms ask for. Extracted, with {@link #rawConfigure} below, so that a
     * test can hold the two side by side - see {@code ConfigureParityTest}.
     */
    static ClientOptions libraryOptions(DemoOptions options, String topic,
                                        Map<String, String> kafkaProperties) {
        return ClientOptions.builder()
                .topics(Collections.singletonList(topic))
                .maxConcurrency(options.maxConcurrency())
                .ordering(ProcessingOrder.UNORDERED)
                .kafkaProperties(kafkaProperties)
                .build();
    }

    /**
     * The same request, written by hand for the control arm.
     *
     * <b>This method exists because getting it wrong is not hypothetical.</b> Twice now the
     * hand-written message has silently differed from what the library sends - first with
     * {@code ordering} unset, which meant that arm ran key-ordered against four unordered ones,
     * and then with the capability list omitted, which negotiated a different session. Both looked
     * exactly like a working arm and both changed what the numbers meant. Building it here rather
     * than inline is what lets a test assert the two agree.
     */
    static Configure rawConfigure(DemoOptions options, String topic,
                                  Map<String, String> kafkaProperties) {
        return Configure.newBuilder()
                .addTopics(topic)
                .setOrdering(bz.stub.parallelconsumer.proxy.protocol.v1.ProcessingOrder
                        .PROCESSING_ORDER_UNORDERED)
                .addCapabilities(DISPATCH_CAPABILITY)
                .setMaxConcurrency(options.maxConcurrency())
                .putAllKafkaProperties(kafkaProperties)
                .build();
    }

    private static void countTowards(AtomicInteger processed, int target, CountDownLatch done) {
        if (processed.incrementAndGet() >= target) {
            done.countDown();
        }
    }

    static ArmResult awaited(String arm, long startedAt, AtomicInteger processed,
                                     CountDownLatch done, int target) throws InterruptedException {
        if (!done.await(ARM_BUDGET.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException(arm + " stalled at " + processed.get() + " of " + target);
        }
        // Reaching the target is not the only thing that releases the latch: a failed or completed
        // session releases it too. Without this check a broken run prints a plausible row at a
        // plausible rate and exits 0, which is the worst thing a demo whose numbers ten other
        // languages copy can do.
        int count = processed.get();
        if (count < target) {
            throw new IllegalStateException(arm + " ended early at " + count + " of " + target);
        }
        return finished(arm, startedAt, count);
    }

    private static ArmResult finished(String arm, long startedAt, int processed) {
        var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
        log.info("=== {} finished: {} records in {}ms ===", arm, processed, elapsed.toMillis());
        return new ArmResult(arm, elapsed, processed);
    }

    /** A fresh group per arm per replay, so every arm reads the same records from the beginning. */
    private static String groupId(String arm) {
        return "pc-demo-" + arm + "-" + System.nanoTime();
    }

    private static void report(String title, List<ArmResult> results, ArmResult baseline,
                               boolean acrossReplays) {
        var table = new StringBuilder("\n\n").append(title).append('\n');
        table.append(String.format(Locale.ROOT, "  %-14s %10s %14s %14s%n", "arm", "elapsed", "msg/s",
                acrossReplays ? "vs AK core*" : "vs AK core"));
        for (ArmResult result : results) {
            String ratio = baseline == null || baseline.ratePerSecond() == 0
                    ? "-"
                    : String.format(Locale.ROOT, "%.1fx", result.ratePerSecond() / baseline.ratePerSecond());
            table.append(String.format(Locale.ROOT, "  %-14s %9.1fs %14s %14s%n",
                    result.arm(), result.elapsed().toMillis() / 1000d,
                    String.format(Locale.ROOT, "%,d", (int) result.ratePerSecond()), ratio));
        }
        if (acrossReplays) {
            table.append("\n  * against the SMALL replay's AK core arm. Across replays, so not "
                    + "like-for-like.\n");
        }
        log.info("{}", table);
    }
}
