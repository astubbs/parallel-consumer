package bz.stub.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LatchTestUtils;
import bz.stub.parallelconsumer.internal.utils.ProgressBarUtils;
import bz.stub.parallelconsumer.internal.utils.StringUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertWithMessage;
import static bz.stub.parallelconsumer.truth.LongPollingMockConsumerSubject.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
class ReactorPCTest extends ReactorUnitTestBase {

    /**
     * The percent of the max concurrency tolerance allowed
     */
    public static final Percentage MAX_CONCURRENCY_OVERFLOW_ALLOWANCE = Percentage.withPercentage(1.2);

    @BeforeEach
    public void setupData() {
        super.primeFirstRecord();
    }

    @Test
    void kickTires() {
        primeFirstRecord();
        primeFirstRecord();
        primeFirstRecord();

        ConcurrentLinkedQueue<Object> msgs = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> threads = new ConcurrentLinkedQueue<>();

        reactorPC.react((rec) -> {
            log.info("Reactor user poll function: {}", rec);
            msgs.add(rec);
            threads.add(Thread.currentThread().getName());
            return Mono.just(StringUtils.msg("result: {}:{}", rec.offset(), rec.value()));
        });

        await()
                .atMost(defaultTimeout)
                .untilAsserted(() -> {
                    assertWithMessage("Processed records collection so far")
                            .that(msgs.size())
                            .isEqualTo(4);

                    assertThat(consumerSpy)
                            .hasCommittedToPartition(topicPartition)
                            .atLeastOffset(4);

                    assertWithMessage("The user-defined function should be executed by the scheduler")
                            .that(threads.stream().allMatch(thread -> thread.startsWith("boundedElastic")))
                            .isTrue();
                });
    }

    /**
     * A user function may legitimately have nothing to emit - {@code Mono.empty()}, or a {@code null} the wrapper
     * turns into an empty sequence - and the record still has to retire.
     * <p>
     * It did not. {@code onComplete} was wired as the subscriber's ON NEXT consumer, so an empty sequence fired
     * neither it nor {@code onError}: the record never reached the mailbox, its offset was never committed, its
     * in-flight accounting leaked, and once {@link bz.stub.parallelconsumer.internal.ExternalEngine} gained a
     * dispatch ceiling, its permit leaked too - which turns a stuck record into a wedged engine.
     * <p>
     * More records than {@code maxConcurrency} is the whole point of the quantity here: a per-record leak only
     * becomes a stall once it has consumed every permit, so a handful of records would pass against the defect.
     */
    @SneakyThrows
    @Test
    void anEmptyPublisherStillRetiresTheRecord() {
        var quantity = MAX_CONCURRENCY + (MAX_CONCURRENCY / 2);
        ktu.send(consumerSpy, ktu.generateRecords(quantity - 1)); // -1 coz already has 1 record primed (all tests do)

        reactorPC.react(recordContext -> Mono.empty());

        await()
                .atMost(defaultTimeout)
                .untilAsserted(() -> assertThat(consumerSpy)
                        .hasCommittedToPartition(topicPartition)
                        .atLeastOffset(quantity));
    }

    @SneakyThrows
    @Test
    void concurrencyTest() {
        //
        var quantity = 100_000;
        var consumerRecords = ktu.generateRecords(quantity - 1); // -1 coz already has 1 record primed (all tests do)
        ktu.send(consumerSpy, consumerRecords);
        log.info("Finished priming records");

        //
        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, quantity);

        //
        ConcurrentLinkedQueue<Object> msgs = new ConcurrentLinkedQueue<>();

        var finishedCount = new AtomicInteger(0);
        var maxConcurrentRecordsSeen = new AtomicInteger(0);
        var completeOrProblem = new CountDownLatch(1);
        var maxConcurrency = MAX_CONCURRENCY;

        reactorPC.react(recordContext -> Mono.just(StringUtils.msg("result: {}:{}", recordContext.offset(), recordContext.value()))
                .doOnNext(ignore -> {
                    // add that our mono processing has started
                    log.trace("Reactor user function executing: {}", recordContext);
                    msgs.add(recordContext);
                    if (msgs.size() > maxConcurrency) {
                        log.error("More records submitted for processing than max concurrency settings ({} vs {})", msgs.size(), maxConcurrency);
                        // fail fast - test already failed
                        completeOrProblem.countDown();
                    }
                })
                // delay the Mono to simulate a slow async processing time, to cause our concurrency to be reached for sure
                .delayElement(Duration.ofMillis((int) (100 * Math.random())))
                .doOnNext(s -> {
                    log.trace("User function after delay. Records pending: {}, removing from out for processing: {}", msgs.size(), recordContext);
                    int currentConcurrentRecords = msgs.size();
                    int highestSoFar = Math.max(currentConcurrentRecords, maxConcurrentRecordsSeen.get());
                    maxConcurrentRecordsSeen.set(highestSoFar);

                    //
                    boolean removed = msgs.remove(recordContext);
                    assertWithMessage("record was present and removed")
                            .that(removed).isTrue();

                    //
                    int numberOfFinishedRecords = finishedCount.incrementAndGet();
                    boolean allExpectedRecordsAreProcessed = numberOfFinishedRecords > quantity - 1;
                    if (allExpectedRecordsAreProcessed) {
                        // release the latch to indicate processing complete
                        completeOrProblem.countDown();
                    }

                    //
                    bar.step();
                }));

        // block here until all messages processed
        LatchTestUtils.awaitLatch(completeOrProblem, defaultTimeoutSeconds);

        //
        int maxConcurrencyAllowedThreshold = (int) (maxConcurrency * MAX_CONCURRENCY_OVERFLOW_ALLOWANCE.value);
        assertWithMessage("Max concurrency should never be exceeded")
                .that(maxConcurrentRecordsSeen.get()).isLessThan(maxConcurrencyAllowedThreshold);
        log.info("Max concurrency was {}", maxConcurrentRecordsSeen.get());

        //
        await()
                // perform testing for at least some time - see fail fast
                .atMost(defaultTimeout)
                // make sure out for processing recs never exceeds max concurrency
                .failFast("Max concurrency exceeded", () -> msgs.size() > maxConcurrencyAllowedThreshold)
                .untilAsserted(() -> {
                    assertWithMessage("Number of completed messages")
                            .that(finishedCount.get()).isEqualTo(quantity);

                    assertThat(consumerSpy).hasCommittedToPartition(topicPartition).offset(quantity);
                });

        bar.close();
        log.info("Max concurrency was {}", maxConcurrentRecordsSeen.get());
    }
}
