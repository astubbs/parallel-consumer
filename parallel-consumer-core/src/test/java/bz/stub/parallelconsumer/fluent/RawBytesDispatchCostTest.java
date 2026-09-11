package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * <b>A measurement, printed and not asserted</b>: what the raw-bytes door costs per record against a classic
 * consumer already typed for its payload, at zero processing time.
 *
 * <h2>Why it is not an assertion</h2>
 * The number is worth knowing and worth recording in the plan; it is not worth a red build. Wall-clock throughput
 * on a shared CI runner varies by more than the difference being measured, so a threshold here would be a flake
 * generator that told nobody anything. What the test does assert is that both arms actually processed every
 * record - a measurement of a run that did not happen is worse than no measurement.
 *
 * <h2>What is being compared, and what is not</h2>
 * Both arms run the same engine over the same mock consumer with a function that does nothing, so what separates
 * them is the facade's per-record work: the route lookup, two deserialiser calls, the ledger, the outcome. It is
 * <b>not</b> a serialisation benchmark: the classic arm's consumer is a mock that hands over objects it was given,
 * so it pays no deserialisation at all, which makes this the pessimistic reading of the facade's overhead rather
 * than the realistic one. Against a real broker both sides deserialise; the facade's extra is that it does so on
 * the worker thread rather than the poll thread, which is the point of R4 and not a cost.
 */
@Slf4j
@Timeout(300)
class RawBytesDispatchCostTest extends AbstractFluentEngineTest {

    private static final int RECORDS = 5_000;

    @Test
    void printThePerRecordCostOfTheRawBytesDoorAgainstATypedClassicConsumer() {
        long fluentNanos = timeTheFluentRoute();
        long classicNanos = timeTheClassicConsumer();

        long fluentPerRecord = fluentNanos / RECORDS;
        long classicPerRecord = classicNanos / RECORDS;
        log.info("RAW-BYTES DISPATCH COST over {} records at zero processing time: fluent route {} ns/record, "
                        + "classic typed consumer {} ns/record, difference {} ns/record",
                RECORDS, fluentPerRecord, classicPerRecord, fluentPerRecord - classicPerRecord);
        // Also on stdout: a surefire run that is not capturing this package's logs still has to show the figure,
        // since a measurement nobody reads is not a measurement.
        System.out.printf("RAW-BYTES DISPATCH COST: fluent %d ns/record, classic %d ns/record, difference %d "
                + "ns/record (%d records)%n", fluentPerRecord, classicPerRecord,
                fluentPerRecord - classicPerRecord, RECORDS);
    }

    private long timeTheFluentRoute() {
        var runtime = new RecordingClientRuntime();
        var pc = ParallelConsumer.connect(props());
        var processed = new AtomicInteger();
        pc.string(TOPIC).process(context -> {
            processed.incrementAndGet();
            return Outcome.succeeded();
        });

        ConsumerHandle handle = runtime.startAndAssign(pc, 1);
        try {
            for (int offset = 0; offset < RECORDS; offset++) {
                runtime.publish(TOPIC, 0, offset, "key-" + offset, "order-" + offset);
            }
            long startedAt = System.nanoTime();
            Awaitility.await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                    assertThat(processed.get()).isEqualTo(RECORDS));
            long elapsed = System.nanoTime() - startedAt;
            assertThat(pc.dispatcher().succeededCount()).isEqualTo(RECORDS);
            return elapsed;
        } finally {
            RecordingClientRuntime.closeWithoutDraining(handle);
        }
    }

    private long timeTheClassicConsumer() {
        var consumer = new LongPollingMockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        consumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(TOPIC, 0), 0L));
        var processed = new AtomicInteger();
        var classic = new ParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .build());
        try {
            classic.subscribe(Collections.singletonList(TOPIC));
            consumer.subscribeWithRebalanceAndAssignment(Collections.singletonList(TOPIC), 1);
            classic.poll(context -> processed.incrementAndGet());

            for (int offset = 0; offset < RECORDS; offset++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, "key-" + offset, "order-" + offset));
            }
            long startedAt = System.nanoTime();
            Awaitility.await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                    assertThat(processed.get()).isEqualTo(RECORDS));
            return System.nanoTime() - startedAt;
        } finally {
            classic.closeDontDrainFirst();
        }
    }
}
