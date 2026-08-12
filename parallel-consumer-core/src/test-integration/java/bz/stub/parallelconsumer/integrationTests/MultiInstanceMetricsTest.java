package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.internal.PCModule;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import java.util.stream.Stream;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.REUSE_GROUP;
import static bz.stub.parallelconsumer.metrics.PCMetricsDef.PC_INSTANCE_TAG;
import static bz.stub.parallelconsumer.metrics.PCMetricsDef.PROCESSED_RECORDS;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;

@Slf4j
class MultiInstanceMetricsTest extends BrokerIntegrationTest<String, String> {
    {
        super.numPartitions = 2;
    }

    SimpleMeterRegistry simpleMeterRegistry;
    private String outputTopic;

    @BeforeEach
    void setup() {
        setupTopic();
        simpleMeterRegistry = new SimpleMeterRegistry();
    }

    @AfterEach
    void cleanup() {
        simpleMeterRegistry.close();
    }


    @SneakyThrows
    @Test
    void twoInstancePCMetricsRecordedIndependently() {

        var numberOfRecordsToProduce = 100L;

        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        String pcInstance1Tag = UUID.randomUUID().toString();

        var pcOptions = getOptions(pcInstance1Tag, NEW_GROUP);

        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc.subscribe(UniSets.of(topic));

        String pcInstance2Tag = UUID.randomUUID().toString();
        pcOptions = getOptions(pcInstance2Tag, REUSE_GROUP);

        ParallelEoSStreamProcessor<String, String> pc2 = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc2.subscribe(UniSets.of(topic));

        AtomicInteger pc1Counter = new AtomicInteger();
        AtomicInteger pc2Counter = new AtomicInteger();
        pc.poll(record -> pc1Counter.incrementAndGet());

        pc2.poll(record -> pc2Counter.incrementAndGet());


        awaitMetric(() -> processedRecordsMetricFor(pcInstance1Tag, pcInstance2Tag), numberOfRecordsToProduce);
        assertThat(processedRecordsMetricFor(pcInstance1Tag)).isEqualTo(pc1Counter.get());
        assertThat(processedRecordsMetricFor(pcInstance2Tag)).isEqualTo(pc2Counter.get());
        pc.close();
        pc2.close();
    }

    @SneakyThrows
    @Test
    void sameRegistryCanBeReusedAfterPcInstanceClosed() {

        var numberOfRecordsToProduce = 20;

        getKcu().produceMessages(topic, numberOfRecordsToProduce);


        var pcOptions = getOptions(null, NEW_GROUP);

        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc.subscribe(UniSets.of(topic));


        AtomicInteger pc1Counter = new AtomicInteger();
        pc.poll(record -> pc1Counter.incrementAndGet());


        awaitMetric(this::processedRecordsMetricTotal, numberOfRecordsToProduce);
        pc.close();

        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        pcOptions = getOptions(null, NEW_GROUP);

        ParallelEoSStreamProcessor<String, String> pc2 = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc2.subscribe(UniSets.of(topic));
        AtomicInteger pc2Counter = new AtomicInteger();

        pc2.poll(record -> pc2Counter.incrementAndGet());
        awaitMetric(this::processedRecordsMetricTotal, numberOfRecordsToProduce * 2);

        pc2.close();
    }

    @SneakyThrows
    @Test
    void allMetersRemovedFromRegistryOnClose() {
        var numberOfRecordsToProduce = 10L;
        getKcu().produceMessages(topic, numberOfRecordsToProduce);
        String pcInstance1Tag = UUID.randomUUID().toString();

        var pcOptions = getOptions(pcInstance1Tag, NEW_GROUP);

        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc.subscribe(UniSets.of(topic));
        AtomicInteger pc1Counter = new AtomicInteger();
        pc.poll(record -> pc1Counter.incrementAndGet());
        awaitMetric(() -> processedRecordsMetricFor(pcInstance1Tag), numberOfRecordsToProduce);
        pc.close();
        assertThat(simpleMeterRegistry.getMeters().size()).isEqualTo(0);
    }


    ParallelConsumerOptions<String, String> getOptions(String pcInstanceTag, KafkaClientUtils.GroupOption consumerGroupOption) {
        return ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .consumer(getKcu().createNewConsumer(consumerGroupOption))
                .meterRegistry(simpleMeterRegistry)
                .pcInstanceTag(pcInstanceTag)
                .ordering(PARTITION) // just so we dont need to use keys
                .build();

    }

    /**
     * Current value of <b>PC's own</b> {@code PROCESSED_RECORDS} metric, summed across every PC
     * instance present in the registry.
     * <p>
     * Note this is not the same number as a counter incremented inside a poll function - see
     * {@link #awaitMetric(DoubleSupplier, double)} for why that distinction matters.
     */
    private double processedRecordsMetricTotal() {
        return sumCounters(Search.in(simpleMeterRegistry).name(PROCESSED_RECORDS.getName()));
    }

    /**
     * Current value of PC's {@code PROCESSED_RECORDS} metric, summed across <b>only</b> the given PC
     * instances. Pass at least one tag; for the whole registry use
     * {@link #processedRecordsMetricTotal()}, which says so at the call site.
     */
    private double processedRecordsMetricFor(String... pcInstanceTags) {
        if (pcInstanceTags.length == 0) {
            throw new IllegalArgumentException(
                    "No instance tags given. Silently falling back to the registry total would return a "
                            + "larger, plausible number and hide the mistake - call processedRecordsMetricTotal().");
        }
        return Stream.of(pcInstanceTags)
                .mapToDouble(tag -> sumCounters(
                        Search.in(simpleMeterRegistry).name(PROCESSED_RECORDS.getName()).tag(PC_INSTANCE_TAG, tag)))
                .sum();
    }

    private double sumCounters(Search search) {
        return search.counters().stream().mapToDouble(Counter::count).sum();
    }

    /**
     * Poll a metric until it reaches {@code expected}.
     * <p>
     * <b>Always await the metric you are about to assert - never a counter incremented inside the poll
     * function.</b> PC increments {@code PROCESSED_RECORDS} in {@code WorkManager#onSuccessResult},
     * i.e. <i>after</i> the user function has returned and the result has travelled back through the
     * control loop. A counter incremented inside that function therefore reaches its target strictly
     * earlier, so an assertion following such an await races the final record: it passes almost always,
     * and fails when the machine is loaded.
     * <p>
     * That is exactly how {@code sameRegistryCanBeReusedAfterPcInstanceClosed} failed CI with
     * {@code expected: 40.0 but was: 39.0} after six consecutive green runs. The same shape has bitten
     * this repo before - see
     * {@code docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md}.
     */
    private void awaitMetric(DoubleSupplier actual, double expected) {
        await().timeout(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(actual.getAsDouble()).isEqualTo(expected));
    }

}
