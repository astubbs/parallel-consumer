package bz.stub.parallelconsumer.vertx.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest;
import bz.stub.parallelconsumer.internal.RateLimiter;
import bz.stub.parallelconsumer.internal.utils.GeneralTestUtils;
import bz.stub.parallelconsumer.internal.utils.ProgressBarUtils;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import bz.stub.parallelconsumer.vertx.VertxParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import pl.tlinkowski.unij.api.UniMaps;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @see #testVertxConcurrency()
 */
@Slf4j
public class Demo extends BrokerIntegrationTest<String, String> {

    /**
     * Standalone entry point, kept for running the demo outside Maven. It has to drive the
     * lifecycle by hand - JUnit is not here to call {@link #setupWireMock()} or {@link #close()} -
     * and it has to exit EXPLICITLY: Testcontainers, WireMock and the engine all run non-daemon
     * threads, so a JVM that merely reaches the end of this method keeps running.
     * <p>
     * The status code is the point of the try/catch. Before it, only the success path exited, so a
     * failure left those threads holding the JVM open and the demo hung instead of reporting.
     */
    public static void main(String[] args) {
        Demo demo = new Demo();
        int status = 0;
        try {
            demo.getKcu().open();
            demo.setupWireMock();
            demo.testVertxConcurrency();
        } catch (Throwable t) {
            log.error("Demo failed", t);
            status = 1;
        } finally {
            demo.close();
        }
        System.exit(status);
    }

    /** Set to {@code true} to run the demo; see {@link #testVertxConcurrency()}. */
    static final String DEMO_ENABLED_PROPERTY = "pc.demo";

    //    static final int expectedMessageCount = 1_000_000;
    static final int expectedMessageCount = 5_000;
    static final int bigExpectedMessageCount = expectedMessageCount * 70;
    static final int concurrencyTarget = 100;
    static final int simulatedDelayMs = 2;

    public List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
    public AtomicInteger processedCount = new AtomicInteger(0);
    public AtomicInteger httpResponceReceivedCount = new AtomicInteger(0);

    public static VertxHttpStub stubServer;

    Demo() {

    }

    /** Groups digits, as {@code StringTestUtils.format} did before that class was deleted. */
    private static String format(int number) {
        return String.format("%,d", number);
    }

    ProgressBar bar;

    /**
     * Held as fields, not locals, so {@link #close()} can reach them on the failure path. Both keep
     * non-daemon threads alive, which is what turns an exception in the middle of a run into a
     * hang.
     */
    VertxParallelEoSStreamProcessor<String, String> pc;

    KafkaConsumer<String, String> vanillaConsumer;

    /**
     * @implNote annotated for the JUnit path AND called by hand from {@link #main(String[])}, which
     * has no JUnit to call it. It ran from {@code main} only until now, so the documented Maven
     * command reached {@code stubServer.port()} with a null stub.
     */
    @BeforeEach
    void setupWireMock() {
        bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);
        bar.pause();
        stubServer = VertxHttpStub.start(concurrencyTarget, request -> {
            bar.stepBy(1);
            ThreadUtils.sleepQuietly(simulatedDelayMs);
        });
    }

    /**
     * Releases everything that holds a non-daemon thread, on the success path and the failure path
     * alike. Each close is guarded: the first failure must not skip the rest, or one broken
     * resource still hangs the JVM.
     */
    @AfterEach
    void close() {
        closeQuietly("parallel consumer", () -> {
            if (pc != null) pc.close();
        });
        closeQuietly("vanilla consumer", () -> {
            if (vanillaConsumer != null) vanillaConsumer.close();
        });
        closeQuietly("progress bar", () -> {
            if (bar != null) bar.close();
        });
        closeQuietly("stub server", () -> {
            if (stubServer != null) stubServer.close();
        });
    }

    private static void closeQuietly(String what, Runnable close) {
        try {
            close.run();
        } catch (Exception e) {
            log.warn("Failed to close the {} - continuing, so the remaining closes still run", what, e);
        }
    }

    /**
     * The demo: a vanilla consumer and the Vert.x Parallel Consumer over the same records, against
     * a stub HTTP service with a simulated per-request delay. It measures; it does not assert.
     * <p>
     * Off by default. This lane collects by PACKAGE PATH - failsafe includes
     * {@code **&#47;integrationTest*&#47;**&#47;*.java} - so living in this package is what decides <!-- issue-refs: exempt -->
     * collection, and a multi-minute measurement with no assertions would otherwise run on every
     * build. {@code VertxConcurrencyIT} is the sibling that does assert, and it stays in the lane.
     * <p>
     * Run it with:
     * <pre>./mvnw verify -pl parallel-consumer-vertx -am -Dit.test=Demo -DfailIfNoTests=false -DskipUTs=true -Dpc.demo=true</pre>
     * {@code -am} is not optional: this module's parent is not in a single-module reactor, and the
     * enforcer's ReactorModuleConvergence rule fails the build before any test runs without it.
     * {@code -DfailIfNoTests=false} keeps {@code -Dit.test} from failing the modules that {@code -am}
     * drags in, and {@code -DskipUTs=true} stops those modules running their unit tests on the way
     * past.
     */
    @Test
    @EnabledIfSystemProperty(named = DEMO_ENABLED_PROPERTY, matches = "true")
    @SneakyThrows
    void testVertxConcurrency() {
        var commitMode = PERIODIC_CONSUMER_ASYNCHRONOUS;
        var order = ParallelConsumerOptions.ProcessingOrder.UNORDERED;

        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();

        log.info("Simulating a server side request delay of {}ms - expected ideal msg rate of {}msg/s", simulatedDelayMs, 1000 / simulatedDelayMs);

        log.info("\nProducing {} messages for test...", format(expectedMessageCount));
        expectedKeys.addAll(getKcu().produceMessages(inputName, expectedMessageCount));
        assertThat(expectedKeys).hasSize(expectedMessageCount);

        // run parallel-consumer
        log.debug("Starting test");
//        KafkaProducer<String, String> newProducer = getKcu().createNewProducer(commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER));


        Properties consumerProps = new Properties();

        {
            vanillaConsumer = getKcu().createNewConsumer(true, consumerProps);

            vanillaConsumer.subscribe(of(inputName));
            log.info("\nStarting vanilla consumer run...");
            bar.resume();
            URL uri = URI.create("http://localhost:" + stubServer.port()).toURL();

//        HttpClient client = HttpClient.newHttpClient();
            RateLimiter rateLimiter = new RateLimiter(1);
//        HttpRequest request = HttpRequest.newBuilder().GET().uri(uri).build();
            while (consumedKeys.size() + 1 < expectedMessageCount) {
                ConsumerRecords<String, String> poll = vanillaConsumer.poll(1000);
                poll.forEach(c -> {
                    consumedKeys.add(c.key());

                    Duration time = GeneralTestUtils.time(() -> blockingGet(uri));
                    rateLimiter.performIfNotLimited(() -> log.debug("Req duration: {}ms", time.toMillis()));

                });
            }
        }
        bar.close();

        log.info("\nVanilla run finished.\n");

        Thread.sleep(2000);

//        consumedKeys = new ArrayList<>();

        log.info("\nPC run starting with concurrency setting of {}...", format(concurrencyTarget));

        {
            // sanity
            KafkaConsumer<String, String> newConsumer = getKcu().createNewConsumer(true, consumerProps);
            TopicPartition tp = new TopicPartition(inputName, 0);
            Map<TopicPartition, Long> beginOffsets = newConsumer.beginningOffsets(of(tp));
            Map<TopicPartition, Long> endOffsets = newConsumer.endOffsets(of(tp));
            assertThat(endOffsets.get(tp)).isEqualTo(expectedMessageCount);
            assertThat(beginOffsets.get(tp)).isEqualTo(0L);

            pc = new VertxParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                    .ordering(order)
                    .consumer(newConsumer)
//                .producer(newProducer)
                    .commitMode(commitMode)
                    .maxConcurrency(concurrencyTarget)
                    .build());
            pc.subscribe(of(inputName));
            pc.vertxHttpReqInfo(record -> {
                        consumedKeys.add(record.key());
                        return new VertxParallelEoSStreamProcessor.RequestInfo("localhost", stubServer.port(), "/", UniMaps.of());
                    }, onSend -> {
                        processedCount.incrementAndGet();
                    }, onWebResponseAsyncResult -> {
                        httpResponceReceivedCount.incrementAndGet();
                        log.trace("Response received complete {}", onWebResponseAsyncResult);
                    }
            );
        }

        // wait for all pre-produced messages to be processed and produced
//        log.info("Waiting for {} requests in parallel on server.", expectedMessageCount);
//        Assertions.useRepresentation(new TrimListRepresentation());
//        var failureMessage = StringUtils.msg("Mock server receives {} requests in parallel from vertx engine",
//                expectedMessageCount);
//        try {
//            waitAtMost(ofSeconds(10))
//                    .pollInterval(ofSeconds(1))
//                    .alias(failureMessage)
//                    .untilAsserted(() -> {
//                        log.info("got {}/{}", parallelRequests.size(), expectedMessageCount);
//                        assertThat(parallelRequests.size()).isEqualTo(expectedMessageCount);
//                    });
//        } catch (ConditionTimeoutException e) {
//            fail(failureMessage + "\n" + e.getMessage());
//        }
//        log.info("All {} requests received in parallel by server, releasing server response lock.", expectedMessageCount);

        // all requests were received in parallel, so unlock the server to respond to all of them
//        LatchTestUtils.release(responseLock);

//        assertNumberOfThreads();

        log.info("\nWaiting for {} responses from server...\n", format(expectedMessageCount));
        bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);
        waitAtMost(ofSeconds(60))
//                .alias(failureMessage)
                .untilAsserted(() -> {
                    assertThat(httpResponceReceivedCount).hasValueGreaterThanOrEqualTo(expectedMessageCount);
                });
//        bar.stepTo(expectedMessageCount);

        // Drain rather than close: the second run reassigns pc, so this instance would otherwise
        // only be reached by close() after it had been replaced.
        pc.closeDrainFirst();
        bar.close();
        log.info("\nAll {} responses received.", format(expectedMessageCount));

        Thread.sleep(5000);
//        assertNumberOfThreads();


        log.info("\nProducing {} messages for a longer PC test...", format(bigExpectedMessageCount));
        int bigTestMessagesToProduce = bigExpectedMessageCount - expectedMessageCount;
        assertThat(getKcu().produceMessages(inputName, bigTestMessagesToProduce)).hasSize(bigTestMessagesToProduce);


        log.info("\nPC run starting with concurrency setting of {}...", format(concurrencyTarget));

        httpResponceReceivedCount.set(0);
        {
            KafkaConsumer<String, String> newConsumer = getKcu().createNewConsumer(true, consumerProps);
            pc = new VertxParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                    .ordering(order)
                    .consumer(newConsumer)
//                .producer(newProducer)
                    .commitMode(commitMode)
                    .maxConcurrency(concurrencyTarget)
                    .build());
            pc.subscribe(of(inputName));
            pc.vertxHttpReqInfo(record -> {
                        consumedKeys.add(record.key());
                        return new VertxParallelEoSStreamProcessor.RequestInfo("localhost", stubServer.port(), "/", UniMaps.of());
                    }, onSend -> {
                        processedCount.incrementAndGet();
                    }, onWebResponseAsyncResult -> {
                        httpResponceReceivedCount.incrementAndGet();
                        log.trace("Response received complete {}", onWebResponseAsyncResult);
                    }
            );
            log.info("\nWaiting for {} responses from server...\n", format(bigExpectedMessageCount));
            bar = ProgressBarUtils.getNewMessagesBar(log, bigExpectedMessageCount);
        }
        waitAtMost(ofSeconds(60))
//                .alias(failureMessage)
                .untilAsserted(() -> {
                    assertThat(httpResponceReceivedCount.get()).isEqualTo(bigExpectedMessageCount);
                });
        bar.close();

        log.info("\nAll {} responses received.", format(bigExpectedMessageCount));

        // sanity
//        assertThat(expectedMessageCount).isEqualTo(processedCount.get());
//        assertThat(responseLock.getCount()).isZero();
//        assertThat(httpResponceReceivedCount).hasValue(bigExpectedMessageCount);
        // Deliberately no System.exit here. Under Maven this method runs INSIDE the failsafe fork,
        // and exiting it reports as "the forked VM terminated without properly saying goodbye"
        // however well the demo went. Releasing the threads is close()'s job; main() owns the exit.
    }

    /**
     * One blocking HTTP round trip - the whole point of the vanilla arm, which does exactly one of
     * these at a time.
     * <p>
     * The original used {@code simplehttp}, which the build no longer carries. {@code java.net.http}
     * is not the replacement: {@code release.target} is 8, so the Java 11 client is invisible to this
     * compile - which is what the 2021 commit subject ("Java http net doesn't compile?") was already
     * running into. {@link HttpURLConnection} is blocking, dependency-free and available at that
     * target.
     */
    @SneakyThrows
    private static void blockingGet(URL url) {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            connection.setRequestMethod("GET");
            connection.getResponseCode();
        } catch (IOException e) {
            throw new RuntimeException("Vanilla arm request failed", e);
        } finally {
            connection.disconnect();
        }
    }

}
