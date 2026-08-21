package bz.stub.parallelconsumer.proxy.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static java.time.Duration.ofMillis;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * <b>The seed demo.</b> The same records through the Apache Kafka client and through Parallel Consumer
 * reached over the sidecar - and it is the reference every per-language demo mirrors.
 *
 * <h2>Why Java is the seed, and what only Java can show</h2>
 *
 * For every other language the comparison is "that language's own Kafka client versus that language over the
 * sidecar", and the two arms differ in more than the engine. Java is the one place where the sidecar hop can
 * be priced with <b>engine, workload, broker and host all held identical</b>, because both arms run in this
 * JVM against this broker with the same sleep as the user function. Whatever the gap turns out to be, that is
 * what crossing the process boundary costs - not a difference in client libraries.
 *
 * <h2>The arms</h2>
 *
 * <ul>
 *   <li><b>AK core</b> - a plain {@code KafkaConsumer}, one record at a time. "AK core" is always spelled out
 *       because bare "core" reads as {@code parallel-consumer-core}; see {@code CONCEPTS.md}.</li>
 *   <li><b>Sidecar</b> - this process acts as a foreign client: it spawns the sidecar binary, configures it
 *       over gRPC, and runs the same sleep on records the engine dispatches to it. This is the invisible
 *       sidecar of KTD41 - nothing is installed, deployed or operated.</li>
 * </ul>
 *
 * <h2>Two replays, because they answer different questions</h2>
 *
 * The small replay puts both arms over identical records - the honest side-by-side. The big replay drops the
 * serial arm and shows what the engine sustains once start-up stops dominating; at a serial-arm-sized volume
 * a parallel arm is already finished, so a single volume can only ever report one of the two. Same structure
 * as {@code ComparisonDemo} in core, deliberately: a reader who has seen one has seen both.
 *
 * <h2>Run it</h2>
 *
 * <pre>parallel-consumer-proxy/demo/run.sh</pre>
 *
 * @author Antony Stubbs
 */
@Testcontainers
@Slf4j
public class SidecarDemo extends BrokerIntegrationTest<byte[], byte[]> {

    static final String DEMO_ENABLED_PROPERTY = "pc.demo";

    static final int RECORDS = Integer.getInteger("demo.records", 2_000);

    static final int DELAY_MS = Integer.getInteger("demo.delayMs", 2);

    static final int MAX_CONCURRENCY = Integer.getInteger("demo.maxConcurrency", 100);

    static final int PARTITIONS = Integer.getInteger("demo.partitions", 10);

    /** The big replay, as a multiple of {@link #RECORDS}. 1 or less skips it. */
    static final int REPLAY_FACTOR = Integer.getInteger("demo.replayFactor", 20);

    private final List<LaneResult> results = new ArrayList<>();

    private static final class LaneResult {
        final String lane;
        final Duration elapsed;
        final int processed;

        LaneResult(String lane, Duration elapsed, int processed) {
            this.lane = lane;
            this.elapsed = elapsed;
            this.processed = processed;
        }

        double ratePerSecond() {
            double seconds = elapsed.toNanos() / 1_000_000_000d;
            return seconds > 0 ? processed / seconds : 0;
        }
    }

    @Test
    @EnabledIfSystemProperty(named = DEMO_ENABLED_PROPERTY, matches = "true")
    @SneakyThrows
    void compareAkCoreWithTheSidecar() {
        // ensureTopic rather than setupTopic: the partition count field is package-private to
        // BrokerIntegrationTest and this demo lives in the proxy's package, so the protected
        // topic-creation method is the supported door rather than widening core's field.
        String topic = "sidecar-demo-" + RandomUtils.nextInt();
        ensureTopic(topic, PARTITIONS);

        log.info("\nEffective configuration:\n  records = {}\n  delayMs = {}\n  maxConcurrency = {}"
                        + "\n  partitions = {}\n  replayFactor = {}\n  topic = {}",
                RECORDS, DELAY_MS, MAX_CONCURRENCY, PARTITIONS, REPLAY_FACTOR, topic);

        log.info("\nProducing {} records...", RECORDS);
        getKcu().produceMessages(topic, RECORDS);

        results.add(runAkCoreLane(topic, RECORDS));
        results.add(runSidecarLane(topic, RECORDS, "SIDECAR"));
        report("Small replay - both arms over the same " + RECORDS + " records (the comparison)", false);

        if (REPLAY_FACTOR > 1) {
            int replayTotal = RECORDS * REPLAY_FACTOR;
            log.info("\nProducing {} more records for the big replay ({} total)...",
                    replayTotal - RECORDS, replayTotal);
            getKcu().produceMessages(topic, replayTotal - RECORDS);

            var big = runSidecarLane(topic, replayTotal, "SIDECAR");
            results.add(big);
            report("Big replay - " + replayTotal + " records, sidecar only (AK core is serial and would "
                    + "take " + (replayTotal * DELAY_MS / 1000) + "s+)", true);
        }
    }

    /** The serial arm: one record at a time, the same sleep, in this JVM. */
    @SneakyThrows
    private LaneResult runAkCoreLane(String topic, int target) {
        log.info("\n=== AK core starting over {} records ===", target);
        KafkaConsumer<byte[], byte[]> consumer = getKcu().createNewConsumer(NEW_GROUP);
        consumer.subscribe(of(topic));

        int processed = 0;
        long startedAt = System.nanoTime();
        while (processed < target) {
            ConsumerRecords<byte[], byte[]> polled = consumer.poll(ofMillis(500));
            for (var ignored : polled) {
                ThreadUtils.sleepQuietly(DELAY_MS);
                processed++;
            }
        }
        var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
        consumer.close();
        log.info("=== AK core finished: {} records in {}ms ===", processed, elapsed.toMillis());
        return new LaneResult("AK_CORE", elapsed, processed);
    }

    /**
     * The foreign-client arm. This is the whole point of the sidecar: the application never touches Kafka -
     * it spawns a binary, receives records over a socket, and reports outcomes back.
     */
    @SneakyThrows
    private LaneResult runSidecarLane(String topic, int target, String laneName) {
        log.info("\n=== {} starting over {} records ===", laneName, target);
        var processed = new AtomicInteger();
        var done = new CountDownLatch(1);
        long startedAt;

        try (var sidecar = SidecarProcess.spawn()) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("127.0.0.1", sidecar.port()).usePlaintext().build();
            ExecutorService workers = Executors.newFixedThreadPool(MAX_CONCURRENCY);
            try {
                var requests = new java.util.concurrent.atomic.AtomicReference<StreamObserver<ClientMessage>>();
                var stream = ProxyServiceGrpc.newStub(channel).session(new StreamObserver<ProxyMessage>() {
                    @Override
                    public void onNext(ProxyMessage message) {
                        if (!message.hasDispatch()) {
                            return;
                        }
                        for (var record : message.getDispatch().getRecordsList()) {
                            // The user function, run on this application's own workers - the engine never
                            // learns what it is (KTD4).
                            workers.submit(() -> {
                                ThreadUtils.sleepQuietly(DELAY_MS);
                                synchronized (requests) {
                                    requests.get().onNext(ClientMessage.newBuilder()
                                            .setReport(Report.newBuilder()
                                                    .setToken(record.getToken())
                                                    .setSuccess(Report.Success.newBuilder()))
                                            .build());
                                }
                                if (processed.incrementAndGet() >= target) {
                                    done.countDown();
                                }
                            });
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Session failed", t);
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
                requests.set(stream);

                startedAt = System.nanoTime();
                stream.onNext(ClientMessage.newBuilder()
                        .setConfigure(Configure.newBuilder()
                                .addTopics(topic)
                                .setMaxConcurrency(MAX_CONCURRENCY)
                                .putKafkaProperties(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                        kafkaContainer.getBootstrapServers())
                                .putKafkaProperties(ConsumerConfig.GROUP_ID_CONFIG,
                                        "sidecar-demo-" + System.nanoTime())
                                .putKafkaProperties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
                        .build());

                if (!done.await(10, TimeUnit.MINUTES)) {
                    throw new IllegalStateException(laneName + " stalled at " + processed.get()
                            + " of " + target);
                }
                var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
                log.info("=== {} finished: {} records in {}ms ===", laneName, processed.get(),
                        elapsed.toMillis());
                return new LaneResult(laneName, elapsed, processed.get());
            } finally {
                workers.shutdownNow();
                channel.shutdownNow();
            }
        }
    }

    private void report(String title, boolean acrossReplays) {
        var akCore = results.stream().filter(r -> r.lane.equals("AK_CORE")).findFirst().orElse(null);
        var shown = acrossReplays
                ? results.subList(results.size() - 1, results.size())
                : results.subList(0, Math.min(2, results.size()));

        var table = new StringBuilder("\n\n" + title + "\n");
        table.append(String.format("  %-12s %10s %14s %12s%n", "arm", "elapsed", "msg/s",
                acrossReplays ? "vs AK core*" : "vs AK core"));
        for (var r : shown) {
            String ratio = akCore == null || akCore.ratePerSecond() == 0
                    ? "-" : String.format("%.1fx", r.ratePerSecond() / akCore.ratePerSecond());
            table.append(String.format("  %-12s %9ds %14s %12s%n", r.lane, r.elapsed.getSeconds(),
                    String.format("%,d", (int) r.ratePerSecond()), ratio));
        }
        if (acrossReplays) {
            table.append("\n  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.\n");
        }
        log.info(table.toString());
    }
}
