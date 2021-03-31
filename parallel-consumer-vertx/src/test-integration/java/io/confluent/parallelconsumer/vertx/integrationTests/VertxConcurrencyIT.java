package io.confluent.parallelconsumer.vertx.integrationTests;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Request;
import io.confluent.csid.utils.GeneralTestUtils;
import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.RateLimiter;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor;
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
import org.eclipse.jetty.util.ConcurrentArrayQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.tlinkowski.unij.api.UniMaps;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static java.lang.Thread.getAllStackTraces;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @see #testVertxConcurrency()
 */
@Testcontainers
@Slf4j
class VertxConcurrencyIT extends BrokerIntegrationTest {


    //    static final int expectedMessageCount = 1_000_000;
    static final int expectedMessageCount = 5_000;
    static final int concurrencyTarget = 400;
    static final int simulatedDelayMs = 2;

    public List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
    public AtomicInteger processedCount = new AtomicInteger(0);
    public AtomicInteger httpResponceReceivedCount = new AtomicInteger(0);

    public static WireMockServer stubServer;

    static CountDownLatch responseLock = new CountDownLatch(1);

    static Queue<Request> parallelRequests = new ConcurrentArrayQueue<>();

    VertxConcurrencyIT() {

    }
    ProgressBar bar;

    @BeforeEach
    void setupWireMock() {
        int minThreadsJettyNeeds = 6;
        WireMockConfiguration options = wireMockConfig().dynamicPort()
//                .containerThreads(Math.max(concurrencyTarget, minThreadsJettyNeeds));
                .containerThreads(Math.max(1000, minThreadsJettyNeeds));

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
    @Test
    @SneakyThrows
    void testVertxConcurrency() {
        var commitMode = PERIODIC_CONSUMER_ASYNCHRONOUS;
        var order = ParallelConsumerOptions.ProcessingOrder.UNORDERED;

        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();


        log.info("Producing {} messages for test...", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
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

        // run parallel-consumer
        log.debug("Starting test");
//        KafkaProducer<String, String> newProducer = kcu.createNewProducer(commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER));

        Properties consumerProps = new Properties();
        KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(true, consumerProps);
        KafkaConsumer<String, String> vanillaConsumer = kcu.createNewConsumer(true, consumerProps);



        // sanity
        TopicPartition tp = new TopicPartition(inputName, 0);
        Map<TopicPartition, Long> beginOffsets = newConsumer.beginningOffsets(of(tp));
        Map<TopicPartition, Long> endOffsets = newConsumer.endOffsets(of(tp));
        assertThat(endOffsets.get(tp)).isEqualTo(expectedMessageCount);
        assertThat(beginOffsets.get(tp)).isEqualTo(0L);

        vanillaConsumer.subscribe(of(inputName));
        HttpClient client = HttpClient.newHttpClient();
        RateLimiter rateLimiter = new RateLimiter(1);
        HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create("http://localhost:" + stubServer.port())).build();
        while (consumedKeys.size() + 1 < expectedMessageCount) {
            ConsumerRecords<String, String> poll = vanillaConsumer.poll(1000);
            poll.forEach(c -> {
                consumedKeys.add(c.key());

                Duration time = GeneralTestUtils.time(() -> {
                    try {
                        client.send(request, HttpResponse.BodyHandlers.ofString());
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
                rateLimiter.performIfNotLimited(() -> log.debug("Req duration: {}ms", time.toMillis()));

            });
        }
        log.info("Vanilla run finished.");

//        consumedKeys = new ArrayList<>();

        bar.stepTo(0);
        log.info("PC run starting...");
        var pc = new VertxParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
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

        log.info("Waiting for {} responses from server.", expectedMessageCount);
        waitAtMost(ofSeconds(60))
//                .alias(failureMessage)
                .untilAsserted(() -> {
                    assertThat(httpResponceReceivedCount).hasValueGreaterThanOrEqualTo(expectedMessageCount);
                });

        log.info("All {} responses received.", expectedMessageCount);

//        assertNumberOfThreads();

        // close
        bar.close();
        pc.closeDrainFirst();

        // sanity
        assertThat(expectedMessageCount).isEqualTo(processedCount.get());
//        assertThat(responseLock.getCount()).isZero();
        assertThat(httpResponceReceivedCount).hasValue(expectedMessageCount);
    }

    /**
     * Should be around this number of threads running - introduced to sanity check thread count looks right
     */
    private void assertNumberOfThreads() {
        Set<Thread> threadKeys = getAllStackTraces().keySet();
        String pcPrefix = "pc-";
        String wireMockPrefix = "qtp";
        long pcThreadCount = threadKeys.stream().filter(x -> x.getName().startsWith(pcPrefix)).count();
        long wireMockThreadCount = threadKeys.stream().filter(x -> x.getName().startsWith(wireMockPrefix)).count();

        int expectedPCThreads = 3;

        if (pcThreadCount > 0) // pc may not have started
        {
            log.info("Checking there are only {} PC threads running", expectedPCThreads);
            assertThat(pcThreadCount)
                    .as("Number of Parallel Consumer threads outside expected estimates")
                    .isEqualTo(expectedPCThreads);
        }

        log.info("Checking there are about ~{} wire mock threads running to process requests in parallel from vert.x", expectedMessageCount);
        assertThat(wireMockThreadCount)
                .as("Number of wiremock threads outside expected estimates")
                .isCloseTo(expectedMessageCount, withPercentage(5));

        log.info("Checking total thread count is about {} plus {}", expectedMessageCount, expectedPCThreads);
        assertThat(threadKeys.size())
                .as("Total number of threads")
                .isCloseTo(expectedMessageCount + expectedPCThreads, withPercentage(15));
    }

}
