
/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */
package bz.stub.parallelconsumer.integrationTests;

import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Slf4j
public class DrainCloseTest extends BrokerIntegrationTest<String, String> {

    ParallelEoSStreamProcessor<String, String> pc;

    @BeforeEach
    void setUp() {
        pc = startPcOnNewTopic(options -> options.ordering(PARTITION));
    }

    @Test
    @SneakyThrows
    void stopPollingAfterStateIsSetToDraining() {
        var recordsToProduce = 2L; // 1 in process + 1 waiting in shard queue
        var recordsToProduceAfterClose = 10L;

        var count = new AtomicLong();
        var latch = new CountDownLatch(1);

        getKcu().produceMessages(topic, recordsToProduce);
        pc.poll(recordContexts -> {
            count.getAndIncrement();
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.debug("Processed record, count now {} - offset: {}", count, recordContexts.offset());
        });
        await().untilAtomic(count, is(equalTo(1L)));

        new Thread(() -> pc.closeDrainFirst(Duration.ofSeconds(30))).start();
        sleep(2000);

        getKcu().produceMessages(topic, recordsToProduceAfterClose);
        sleep(5000);

        latch.countDown();

        await().until(() -> pc.isClosedOrFailed()
                || count.get() == recordsToProduce + recordsToProduceAfterClose);
        assertEquals(recordsToProduce, count.get());
        log.debug("Test finished");
    }
}