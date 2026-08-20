package bz.stub.parallelconsumer.vertx.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Request;
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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniMaps;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static java.lang.Thread.getAllStackTraces;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @see #testVertxConcurrency()
 */
@Slf4j
public class Demo extends BrokerIntegrationTest<String, String> {

    public static void main(String[] args) {
        Demo demo = new Demo();
        demo.getKcu().open();
        demo.setupWireMock();
        demo.testVertxConcurrency();
    }

    //    static final int expectedMessageCount = 1_000_000;
    static final int expectedMessageCount = 5_000;
    static final int bigExpectedMessageCount = expectedMessageCount * 70;
    static final int concurrencyTarget = 100;
    static final int simulatedDelayMs = 2;

    public List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
    public AtomicInteger processedCount = new AtomicInteger(0);
    public AtomicInteger httpResponceReceivedCount = new AtomicInteger(0);

    public static WireMockServer stubServer;

    static CountDownLatch responseLock = new CountDownLatch(1);

    static Queue<Request> parallelRequests = new ConcurrentLinkedQueue<>();

    Demo() {

    }

    /** Groups digits, as {@code StringTestUtils.format} did before that class was deleted. */
    private static String format(int number) {
        return String.format("%,d", number);
    }

    ProgressBar bar;

    void setupWireMock() {
        int minThreadsJettyNeeds = 6;
        WireMockConfiguration options = wireMockConfig().dynamicPort()
//                .containerThreads(Math.max(concurrencyTarget, minThreadsJettyNeeds));
                .containerThreads(Math.max(concurrencyTarget, minThreadsJettyNeeds));

        stubServer = new WireMockServer(options);
        MappingBuilder mappingBuilder = get(urlPathEqualTo("/"))
                .willReturn(aResponse());

        stubServer.stubFor(mappingBuilder);

        bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);
        bar.pause();
        stubServer.addMockServiceRequestListener((request, response) -> {
//                log.debug("req: {}", request);
//                parallelRequests.add(request);
            bar.stepBy(1);
            ThreadUtils.sleepQuietly(simulatedDelayMs);
//                awaitLatch(responseLock, 30); // latch timeout should be longer than awaitility's
//                log.trace("unlocked");
        });

        stubServer.start();
    }

    /**
     * This test uses a wire mock server which blocks responding to all requests, until it has received a certain number
     * of requests in parallel. Once this count has been reached, the global latch is released, and all requests are
     * responded to.
     * <p>
     * This is used to sanity test that the PC vertx module is indeed sending the number of concurrent requests that we
     * would expect.
     */
    @SneakyThrows
    void testVertxConcurrency() {
        var commitMode = PERIODIC_CONSUMER_ASYNCHRONOUS;
        var order = ParallelConsumerOptions.ProcessingOrder.UNORDERED;

        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();

        log.info("Simulating a server side request delay of {}ms - expected ideal msg rate of {}msg/s", simulatedDelayMs, 1000 / simulatedDelayMs);

        {
            log.info("\nProducing {} messages for test...", format(expectedMessageCount));
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            try (Producer<String, String> kafkaProducer = getKcu().createNewProducer(false)) {
                for (int i = 0; i < expectedMessageCount; i++) {
                    String key = "key-" + i;
                    Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                        if (exception != null) {
                            log.error("Error sending, ", exception);
                        }
                    });
                    sends.add(send);
                    expectedKeys.add(key);
                }
                log.debug("Finished sending test data");
            }
            // make sure we finish sending before next stage
            log.debug("Waiting for broker acks");
            for (Future<RecordMetadata> send : sends) {
                send.get();
            }
            assertThat(sends).hasSize(expectedMessageCount);
        }

        // run parallel-consumer
        log.debug("Starting test");
//        KafkaProducer<String, String> newProducer = getKcu().createNewProducer(commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER));


        Properties consumerProps = new Properties();

        {
            KafkaConsumer<String, String> vanillaConsumer = getKcu().createNewConsumer(true, consumerProps);

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

        VertxParallelEoSStreamProcessor<String, String> pc;
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

        // close
        pc.closeDrainFirst();
        bar.close();
        log.info("\nAll {} responses received.", format(expectedMessageCount));

        Thread.sleep(5000);
//        assertNumberOfThreads();


        {
            log.info("\nProducing {} messages for a longer PC test...", format(bigExpectedMessageCount));
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            int bigTestMessagesToProduce = bigExpectedMessageCount - expectedMessageCount;
            try (Producer<String, String> kafkaProducer = getKcu().createNewProducer(false)) {
                for (int i = 0; i < bigTestMessagesToProduce; i++) {
                    String key = "key-" + i;
                    Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                        if (exception != null) {
                            log.error("Error sending, ", exception);
                        }
                    });
                    sends.add(send);
                    expectedKeys.add(key);
                }
                log.debug("Finished sending test data");
            }
            // make sure we finish sending before next stage
            log.debug("Waiting for broker acks");
            for (Future<RecordMetadata> send : sends) {
                send.get();
            }
            assertThat(sends).hasSize(bigTestMessagesToProduce);
        }


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
        System.exit(0);
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
