package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static org.awaitility.Awaitility.await;

/**
 * PARTITION ordering against a real broker with the buffer tuned the way the README's "As default buffer size is
 * calculated as" passage recommends - the liveness half of what arrived with upstream PR
 * confluentinc/parallel-consumer#682.
 * <p>
 * Its negative twin, {@code allPartitionsAreNotProcessedInParallel}, asserted the untuned starvation as an outcome of
 * the broker's fetch composition and raced it; it now lives at unit level as
 * {@code WorkManagerTest.aDefaultBufferFilledByOnePartitionsPollPausesIntakeAndStarvesTheOtherPartitions}, where the
 * gate that produces it is asserted with fixed inputs.
 *
 * @author Antony Stubbs
 */
@Slf4j
class PartitionOrderProcessingTest extends BrokerIntegrationTest<String, String> {

    Consumer<String, String> consumer;

    ParallelEoSStreamProcessor<String, String> pc;

    {
        super.numPartitions = 5;
    }

    // todo refactor move up
    @BeforeEach
    void setup() {
        setupTopic();
        consumer = getKcu().createNewConsumer(true, consumerProps());
    }

    @AfterEach
    void cleanup() {
        pc.close();
    }

    private ParallelEoSStreamProcessor<String, String> setupPC(Function<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>, ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>> optionsCustomizer) {
        ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder =
                ParallelConsumerOptions.<String, String>builder()
                        .consumer(consumer)
                        .ordering(PARTITION)
                        .maxConcurrency(5);
        return new ParallelEoSStreamProcessor<>(optionsCustomizer.apply(optionsBuilder).build());
    }

    /**
     * Check that all partitions are processed in parallel and not starved when Consumer options for max partition fetch
     * size and ParallelConsumer buffer size are tuned. Increasing ParallelConsumer buffer size improves parallel
     * processing load over the untuned default - which {@code WorkManagerTest} pins at unit level, see the class
     * javadoc - due to having enough buffer to fit 10 x polls so allows underlying Consumer to fetch from each
     * partition at least 2 times before back-pressure control pauses polling.
     * <p>
     * What this proves that the unit twin cannot: a real broker and fetcher deliver every partition's poll while the
     * gate is open. It is a bounded liveness claim, and can only go red if some partition is never served.
     */
    @SneakyThrows
    @Test
    void allPartitionsAreProcessedInParallel() {
        var numberOfRecordsToProduce = 10000L;
        Map<Integer, AtomicInteger> partitionCounts = new HashMap<>();
        IntStream.range(0, 5).forEach(part -> partitionCounts.put(part, new AtomicInteger(0)));
        pc = setupPC(options -> options.messageBufferSize(5000)); // Increasing message buffer to 10 * max partition fetch - to make sure mix of data from all partitions is available for processing
        pc.subscribe(UniSets.of(topic));

        //
        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        // consume all the messages
        pc.poll(recordContexts -> {
            partitionCounts.get(recordContexts.getSingleConsumerRecord().partition()).getAndIncrement();
            ThreadUtils.sleepQuietly(10); // introduce a bit of processing delay - to make sure polling backpressure kicks in.
        });
        // Wait for BOTH conditions: enough total messages AND all partitions represented.
        // Previously the await only checked total > 500, then the assertion checked all
        // partitions — a race, because Kafka may deliver from one partition first.
        // Moving the partition check into the await lets Awaitility retry until
        // all partitions have been reached.
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            int total = partitionCounts.values().stream().mapToInt(AtomicInteger::get).sum();
            Assertions.assertTrue(total > 500,
                    "Expect > 500 total messages processed, actual: " + total);
            Assertions.assertTrue(partitionCounts.values().stream().allMatch(v -> v.get() > 0),
                    "Expect all partitions to have some messages processed, actual partitionCounts:" + partitionCounts);
        });

    }

    //Tune consumer for smaller message polls both per partition and max poll records.
    private Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 500 * 100); //500 * ~100 byte message
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); //default
        return props;
    }

}
