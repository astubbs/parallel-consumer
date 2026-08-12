
/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */
package bz.stub.parallelconsumer.integrationTests;

import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Slf4j
public class RetriesTest extends BrokerIntegrationTest<String, String> {

    ParallelEoSStreamProcessor<String, String> pc;

    @BeforeEach
    void setUp() {
        pc = startPcOnNewTopic(options -> options
                .ordering(KEY)
                .maxConcurrency(100)
                .defaultMessageRetryDelay(Duration.ofMillis(200))
                .messageBufferSize(10000));
    }

    @Test
    @SneakyThrows
    void awaitingWorkContainersSizeDoesNotExceedNumberOfFailedContainersInRetryLoop() {
        String throwExceptionFlag = "throw";
        var latch = new CountDownLatch(1);

        getKcu().produceMessagesWithThrowHeader(topic, 4000);
        AtomicInteger count = new AtomicInteger(0);
        AtomicBoolean throwOnHeader = new AtomicBoolean(true);
        pc.poll((context) -> {
            ThreadUtils.sleepQuietly(7); //not multiple of retry delay or vice versa
            if (throwOnHeader.get()) {
                if (context.getSingleConsumerRecord().headers().lastHeader(throwExceptionFlag) != null) {
                    throw new PCRetriableException("THROW_EXCEPTION_FLAG_HAPPENED");
                }
            }
            count.incrementAndGet();
        });
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100)).until(count::get, is(equalTo(2000))); //wait for all successful messages to complete
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicBoolean checking = new AtomicBoolean(true);
        Thread sampler = new Thread(() -> {
            try {
                while (checking.get()) {
                    long countAwaiting = pc.getWm().getSm().getNumberOfWorkQueuedInShardsAwaitingSelection();
                    log.debug("NumberOfWorkQueuedInShardsAwaitingSelection : {}", countAwaiting);
                    assertThat(countAwaiting).isBetween(0L, 2000L);
                    ThreadUtils.sleepQuietly(RandomUtils.nextInt(1, 10));
                }
            } catch (AssertionError e) {
                failed.set(true);
                throw e;
            } finally {
                latch.countDown();
            }
        }, "retry-queue-depth-sampler");
        // daemon + the finally below: the sampler must not outlive the test. If the await times out, an
        // un-stopped sampler keeps asserting against a closed PC and reports its failure against whichever
        // test runs next in this JVM.
        sampler.setDaemon(true);
        sampler.start();
        try {
            ThreadUtils.sleepQuietly(3000); // soak the retry loop, so the sampler above gets many chances to catch a violation
            throwOnHeader.set(false);
            // keep sampling until the previously failing half has actually drained - the real end of the retry
            // loop, rather than a fixed wait in which we hope it drained
            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100)).until(count::get, is(equalTo(4000)));
        } finally {
            checking.set(false);
            latch.await();
        }
        assertThat(failed.get()).isFalse();
    }
}