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
import io.grpc.netty.shaded.io.netty.channel.epoll.Epoll;
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
import java.util.concurrent.Executors;
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
 *   <li><b>java-grpc-uds</b> - the same client library over the same sidecar, reached through a
 *       <b>Unix domain socket</b> instead of loopback TCP. Against java-grpc - one term changed, and
 *       every other term identical - this prices <b>the TCP/IP stack</b>. It is additive: where it
 *       cannot run, every other arm reports exactly what it reports now.</li>
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

    /**
     * <b>The first thing this demo prints, and the shape every language prints.</b>
     * <p>
     * A reader who starts a demo and is greeted by {@code java-grpc: the proxy granted 100 executor
     * threads} has been told nothing about what they are looking at. The banner names the product
     * and what is about to happen, and only the language differs between the eleven copies of it.
     */
    private static final String BANNER =
            "\n================================================================\n"
                    + "  PARALLEL CONSUMER  -  Java demo\n"
                    + "  The same records, twice: one at a time, then all at once.\n"
                    + "================================================================";

    private static final String AK_CORE = "AK core";

    /**
     * <b>What each arm actually drives.</b> "AK core" is a category, not a client - it is
     * {@code KafkaConsumer} here, {@code franz-go} in Go, {@code rdkafka} in Ruby - and a reader
     * cannot judge a row without knowing which of those produced it. So every row is printed as
     * {@code arm (client)}, and these are the second half of that.
     * <p>
     * {@link #JAVA_GRPC} is the arm every language has, and its client is spelled exactly as the
     * contract spells it - {@code this client} - because in every language that row means "the
     * client library this repository ships". Java's four extra arms name what they swap for it: a
     * different engine surface, a different socket, or no library at all. So what each pair isolates
     * is readable straight off the table, rather than only from the prose above.
     */
    private static final String AK_CORE_CLIENT = "KafkaConsumer";

    private static final String PC_CORE = "pc-core";

    private static final String PC_CORE_CLIENT = "ParallelEoSStreamProcessor";

    private static final String JAVA_DIRECT = "java-direct";

    private static final String JAVA_DIRECT_CLIENT = "this client, in process";

    private static final String JAVA_GRPC = "java-grpc";

    private static final String JAVA_GRPC_CLIENT = "this client";

    private static final String JAVA_GRPC_UDS = "java-grpc-uds";

    private static final String JAVA_GRPC_UDS_CLIENT = "this client, over UDS";

    private static final String JAVA_RAW_GRPC = "java-raw-grpc";

    private static final String JAVA_RAW_GRPC_CLIENT = "no client library";

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

        String topic = options.topic().orElseGet(() -> "pc-demo-" + System.nanoTime());
        // Banner and fingerprint BEFORE the broker is resolved, because resolving it can start a
        // container and print a paragraph about doing so - and the contract's order is the product,
        // then the settings, then the run. A reader who has to scroll back past broker chatter to
        // find out what they are running has been told what it is too late.
        announce(options, topic);

        try (DemoBroker broker = DemoBroker.resolve(options.bootstrap().orElse(null))) {
            runFor(options, broker, topic);
        }
    }

    /**
     * Names the product, then echoes every dial the run is using.
     * <p>
     * The fingerprint is not decoration: a number without its settings is not reproducible. It never
     * includes the bootstrap address, because own-cluster mode puts a user's real broker there.
     */
    static void announce(DemoOptions options, String topic) {
        log.info("{}", BANNER);
        log.info("\nEffective configuration:\n  {}\n  topic = {}", options, topic);
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
        // The banner and the fingerprint are printed by main() before the broker is resolved, not
        // here: see announce(DemoOptions, String).
        if (!domainSocketsAvailable()) {
            log.info("\nThe java-grpc-uds arm is NOT running: this JVM has no epoll domain-socket "
                    + "transport, which is expected outside Linux. Every other arm is unaffected and "
                    + "reports exactly what it always reports - the comparison is one row shorter, not "
                    + "different. To include it, run the demo in its container: demo/run.sh --docker");
        }

        broker.ensureTopic(topic, options.partitions());
        broker.seed(topic, 0, options.records());

        var small = new ArrayList<ArmResult>();
        small.add(akCore(options.records()));
        small.add(pcCore(options.records()));
        small.add(javaDirect(options.records()));
        small.add(javaGrpc(options.records()));
        if (domainSocketsAvailable()) {
            small.add(javaGrpcOverDomainSocket(options.records()));
        }
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
        if (domainSocketsAvailable()) {
            big.add(javaGrpcOverDomainSocket(total));
        }
        big.add(javaRawGrpc(total));
        // The unit is chosen so this figure is never zero. At the demo's own defaults it is 80s and
        // carries the whole argument for dropping the serial arm; at the volumes CI and the
        // conformance harness run, integer seconds printed "0s+" - which told a reader the arm was
        // dropped to save no time at all, and was the only false statement this demo made. Ten
        // languages mirrored the wart from here, so the fix belongs here first.
        long serialMillis = (long) total * options.delayMs();
        String serialCost = serialMillis >= 1000 ? (serialMillis / 1000) + "s" : serialMillis + "ms";
        report("Big replay - " + total + " records, parallel arms only (AK core is serial and would"
                + " take " + serialCost + "+)", big, baselineOf(small), true);

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

        var tally = new ArmTally(target);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(topic));
            // The clock starts AFTER the consumer is built and stops before it closes, because this
            // arm is the denominator of every ratio in both tables and no other arm charges itself
            // for client construction or teardown.
            long startedAt = System.nanoTime();
            long deadline = startedAt + ARM_BUDGET.toNanos();
            while (tally.processed() < target) {
                // The one arm that does not wait on a latch still needs the budget ARM_BUDGET
                // promises, or a backlog shorter than the target spins here forever with no output.
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(AK_CORE + " stalled at " + tally.processed()
                            + " of " + target);
                }
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                for (var record : polled) {
                    ThreadUtils.sleepQuietly(options.delayMs());
                    tally.recordProcessed(record.key());
                }
            }
            return finished(AK_CORE, AK_CORE_CLIENT, startedAt, tally);
        }
    }

    /** The engine on its own, as a Java application uses it today - no client library, no sidecar. */
    private ArmResult pcCore(int target) throws InterruptedException {
        log.info("\n=== {} starting over {} records ===", PC_CORE, target);
        var config = new Properties();
        config.putAll(broker.consumerProperties(groupId(PC_CORE)));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        var tally = new ArmTally(target);
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
                tally.recordProcessed(context.key());
            });
            return awaited(PC_CORE, PC_CORE_CLIENT, startedAt, tally);
        } finally {
            engine.close();
        }
    }

    /** The client library with the engine bound in process behind it. */
    private ArmResult javaDirect(int target) throws InterruptedException {
        log.info("\n=== {} starting over {} records ===", JAVA_DIRECT, target);
        var tally = new ArmTally(target);
        try (ParallelConsumerClient client = DirectParallelConsumerClient.builder()
                .options(clientOptions(groupId(JAVA_DIRECT)))
                .build()) {
            long startedAt = System.nanoTime();
            client.poll(sleepingProcessor(tally));
            return awaited(JAVA_DIRECT, JAVA_DIRECT_CLIENT, startedAt, tally);
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
        log.info("\n=== {} starting over {} records ===", JAVA_GRPC, target);
        var tally = new ArmTally(target);
        try (SidecarProcess sidecar = SidecarProcess.spawn()) {
            try (ParallelConsumerClient client = GrpcParallelConsumerClient.builder()
                    .port(sidecar.port())
                    .options(clientOptions(groupId(JAVA_GRPC)))
                    .build()) {
                long startedAt = System.nanoTime();
                client.poll(sleepingProcessor(tally));
                return awaited(JAVA_GRPC, JAVA_GRPC_CLIENT, startedAt, tally);
            }
        }
    }

    /**
     * The same client library and the same sidecar, over a Unix domain socket.
     *
     * <h2>What it isolates, and why it is worth a whole arm</h2>
     *
     * Against {@link #javaGrpc} exactly one term changes: the socket type. Same library, same protobuf,
     * same engine, same spawned child, same broker, same workload. So the difference is what the TCP/IP
     * stack costs, which the java-direct to java-grpc step otherwise lumps together with serialization,
     * the gRPC machinery and the process boundary.
     *
     * <h2>Where it runs</h2>
     *
     * Linux, including inside this demo's own container on any host - grpc-netty-shaded bundles the epoll
     * natives (x86_64 and aarch64) and no kqueue transport. On macOS natively it cannot run, and the demo
     * routes the whole run through its container rather than dropping the row or, worse, running this one
     * arm in a container while its comparators run on the host - two environments in one table would read
     * as a UDS penalty that is really the container.
     */
    private ArmResult javaGrpcOverDomainSocket(int target) throws Exception {
        log.info("\n=== {} starting over {} records ===", JAVA_GRPC_UDS, target);
        var tally = new ArmTally(target);
        try (SidecarProcess sidecar = SidecarProcess.spawnOnDomainSocket()) {
            try (ParallelConsumerClient client = GrpcParallelConsumerClient.builder()
                    .socketPath(sidecar.socketPath())
                    .options(clientOptions(groupId(JAVA_GRPC_UDS)))
                    .build()) {
                long startedAt = System.nanoTime();
                client.poll(sleepingProcessor(tally));
                return awaited(JAVA_GRPC_UDS, JAVA_GRPC_UDS_CLIENT, startedAt, tally);
            }
        }
    }

    /**
     * Whether this JVM can open a Unix domain socket at all, asked of the runtime rather than guessed
     * from the operating system's name - the shaded jar answers it directly.
     * <p>
     * Public for the same reason {@link #runFor} is: the integration test lives in its own
     * {@code integrationTests} package, and it has to expect exactly the arms this platform can run.
     */
    public static boolean domainSocketsAvailable() {
        return Epoll.isAvailable();
    }

    /**
     * The control arm: the same sidecar, the same work, the protocol spoken by hand.
     * <p>
     * <b>This is not an example to copy.</b> It exists so that java-grpc minus this arm is a number
     * rather than an assumption - it prices the client library itself. An application writes
     * {@link #javaGrpc}; nobody writes this.
     */
    private ArmResult javaRawGrpc(int target) throws Exception {
        log.info("\n=== {} starting over {} records ===", JAVA_RAW_GRPC, target);
        var tally = new ArmTally(target);

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
                                // The key comes off the wire here rather than out of a client
                                // library, which is the whole point of this arm - it reports the
                                // same two evidence columns as the rest with no help at all. The
                                // presence check is the library's null-key handling, done by hand:
                                // the field is optional, and an ABSENT key decodes to an EMPTY
                                // ByteString, which would count as a key rather than as no key.
                                var wireRecord = record.getRecord();
                                tally.recordProcessed(wireRecord.hasKey()
                                        ? wireRecord.getKey().toByteArray()
                                        : null);
                            });
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        // The demo closes the channel as soon as the target is reached, which the
                        // stream then reports back as UNAVAILABLE. Reporting our own teardown as a
                        // failure would teach a reader to expect an error at the end of a healthy
                        // run, so it is only an error if we were not already finished.
                        if (tally.stillRunning()) {
                            log.error("Session failed", t);
                            tally.sessionEnded();
                        }
                    }

                    @Override
                    public void onCompleted() {
                        tally.sessionEnded();
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
                        broker.consumerProperties(groupId(JAVA_RAW_GRPC)));
                stream.onNext(ClientMessage.newBuilder().setConfigure(configure).build());

                return awaited(JAVA_RAW_GRPC, JAVA_RAW_GRPC_CLIENT, startedAt, tally);
            } finally {
                workers.shutdownNow();
                channel.shutdownNow();
            }
        }
    }

    /**
     * The user function for the client-library arms, identical to the sleep every other arm
     * runs so that the arms differ by transport and nothing else.
     */
    private RecordProcessor sleepingProcessor(ArmTally tally) {
        return record -> {
            ThreadUtils.sleepQuietly(options.delayMs());
            tally.recordProcessed(record.key());
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

    static ArmResult awaited(String arm, String client, long startedAt, ArmTally tally)
            throws InterruptedException {
        if (!tally.awaitCompletion(ARM_BUDGET)) {
            throw new IllegalStateException(arm + " stalled at " + tally.processed()
                    + " of " + tally.target());
        }
        // Reaching the target is not the only thing that releases the latch: a failed or completed
        // session releases it too. Without this check a broken run prints a plausible row at a
        // plausible rate and exits 0, which is the worst thing a demo whose numbers ten other
        // languages copy can do.
        if (tally.processed() < tally.target()) {
            throw new IllegalStateException(arm + " ended early at " + tally.processed()
                    + " of " + tally.target());
        }
        return finished(arm, client, startedAt, tally);
    }

    private static ArmResult finished(String arm, String client, long startedAt, ArmTally tally) {
        var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
        log.info("=== {} finished: {} records over {} keys in {}ms ===",
                arm, tally.processed(), tally.uniqueKeys(), elapsed.toMillis());
        return new ArmResult(arm, client, elapsed, tally.processed(), tally.uniqueKeys());
    }

    /** A fresh group per arm per replay, so every arm reads the same records from the beginning. */
    private static String groupId(String arm) {
        return "pc-demo-" + arm + "-" + System.nanoTime();
    }

    /**
     * Prints one replay's table.
     *
     * <h2>Six columns, and two of them are the evidence</h2>
     *
     * {@code elapsed}, {@code msg/s} and the ratio are measurements: they depend on the machine, the
     * load on it and the language. {@code records} and {@code keys} are not - every arm, in every
     * language, replaying the same backlog must report the same pair. They are what turns a table
     * that <em>asserts</em> the work happened into one that <em>shows</em> it: a short arm is a
     * failed arm rather than a fast one, and a keys figure that collapses to 1 says the backlog was
     * never spread however good the rate looks.
     *
     * <h2>The arm column is sized to its contents</h2>
     *
     * Because the label now carries the client too, a fixed width either truncates
     * {@code pc-core (ParallelEoSStreamProcessor)} or wastes a third of the line on every other row.
     * Column width is explicitly not part of the cross-language contract for exactly this reason -
     * arm names differ in length between languages - so the widest label in the table sets it.
     */
    private static void report(String title, List<ArmResult> results, ArmResult baseline,
                               boolean acrossReplays) {
        log.info("{}", renderTable(title, results, baseline, acrossReplays));
    }

    /**
     * The table as text, split from {@link #report} for one reason: <b>column identity and order
     * are contract</b> across all eleven languages, and nothing else in this demo can assert them.
     * The C++ and Rust demos split theirs for the same reason; Java, as the seed, had no such test
     * at all until the eleven implementations returned three different column orders from one
     * document - and the seed's own order was one of the wrong ones.
     * <p>
     * Package-private rather than public: the test lives beside it, and the demo's surface is
     * {@code main}.
     */
    static String renderTable(String title, List<ArmResult> results, ArmResult baseline,
                              boolean acrossReplays) {
        int armWidth = results.stream().mapToInt(r -> r.label().length()).max().orElse(20);
        String rowFormat = "  %-" + armWidth + "s %9s %7s %9s %10s %11s%n";

        var table = new StringBuilder("\n\n").append(title).append('\n');
        table.append(String.format(Locale.ROOT, rowFormat, "arm", "records", "keys", "elapsed",
                "msg/s", acrossReplays ? "vs AK core*" : "vs AK core"));
        for (ArmResult result : results) {
            String ratio = baseline == null || baseline.ratePerSecond() == 0
                    ? "-"
                    : String.format(Locale.ROOT, "%.1fx", result.ratePerSecond() / baseline.ratePerSecond());
            table.append(String.format(Locale.ROOT, rowFormat,
                    result.label(),
                    String.format(Locale.ROOT, "%,d", result.processed()),
                    String.format(Locale.ROOT, "%,d", result.uniqueKeys()),
                    String.format(Locale.ROOT, "%.1fs", result.elapsed().toMillis() / 1000d),
                    String.format(Locale.ROOT, "%,d", (int) result.ratePerSecond()),
                    ratio));
        }
        // Phrased as a sentence rather than as `name = value` on purpose: the cross-language
        // conformance check reads any such line as a configuration dial the demo echoed.
        table.append("\n  The records and keys columns are deterministic - every arm must process "
                + "every record, over the same keys.\n");
        if (acrossReplays) {
            table.append("  * against the SMALL replay's AK core arm. Across replays, so not "
                    + "like-for-like.\n");
        }
        return table.toString();
    }
}
