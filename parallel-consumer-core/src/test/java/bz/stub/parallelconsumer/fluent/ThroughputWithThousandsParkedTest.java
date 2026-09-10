package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * What a large parked set costs the records that are still healthy - <b>measured and printed, not asserted</b>.
 * <p>
 * The cost is real and structural: a parked record stays in its shard, and under unordered processing the shard's
 * selection walks every entry it holds looking for one whose retry time has passed. So a partition with thousands
 * parked pays that walk on every dispatch, and this is the figure that says how much. It is printed rather than
 * asserted because it is a property of the machine it ran on; what a threshold here would catch is a slow CI
 * runner, not a regression.
 * <p>
 * The control arm is the same run with nothing parked, so the number means something on whatever machine produced
 * it. The engine change that removes the walk - skipping a record whose retry delay has not elapsed - is a
 * small-tier item; when it lands, this measurement is what says whether it worked.
 * <p>
 * <b>What it measured when it was written</b> (2026-09-10, one developer machine, mock consumer, box otherwise
 * quiet): about 8,800 records/second with nothing parked, the same within noise with three thousand parked, and
 * about 70% of that with twenty thousand. So the walk is not something a route with a few thousand parked records
 * needs to think about, and it is something a route with tens of thousands does - which is also the range where the
 * offset map's own capacity is the pressing question. Under a fully loaded box the twenty-thousand arm did not
 * finish two thousand healthy records inside two minutes, which is why it is not one of the arms below.
 * <p>
 * The figures are not asserted and will differ on other hardware; what is worth keeping is the shape.
 */
@Timeout(300)
class ThroughputWithThousandsParkedTest {

    private static final String TOPIC = "orders";

    /**
     * The sizes measured, in order: the control arm, and the "several thousand" the plan asks for.
     * <p>
     * <b>A twenty-thousand arm was measured by hand and deliberately left out of the suite</b> - see the figures on
     * this class. It is what showed that the cost exists at all, and it is also what made this test unbounded on a
     * loaded box: in a full-suite run it failed to process its two thousand healthy records inside two minutes,
     * against under a second when the box is quiet. A printed measurement must never be able to fail a build, and a
     * hundredfold spread under load is not a measurement anyway.
     */
    private static final int[] PARKED_SET_SIZES = {0, 3_000};

    private static final int HEALTHY_RECORDS = 2000;

    private ConsumerHandle handle;

    @AfterEach
    void closeTheInstance() {
        if (handle != null) {
            RecordingClientRuntime.closeWithoutDraining(handle);
            handle = null;
        }
    }

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "throughput-with-thousands-parked");
        return properties;
    }

    @Test
    void dispatchThroughputOfAHealthyRouteBesideThousandsOfParkedRecords() {
        double[] throughputs = new double[PARKED_SET_SIZES.length];
        for (int i = 0; i < PARKED_SET_SIZES.length; i++) {
            throughputs[i] = measureHealthyThroughput(PARKED_SET_SIZES[i]);
        }

        StringBuilder report = new StringBuilder(
                "Dispatch throughput of a healthy route, one partition, unordered:\n");
        double control = throughputs[0];
        for (int i = 0; i < PARKED_SET_SIZES.length; i++) {
            report.append(String.format("  %,7d parked: %,7.0f records/second (%3.0f%% of the control arm)%n",
                    PARKED_SET_SIZES[i], throughputs[i], control == 0 ? 0 : throughputs[i] / control * 100));
        }
        System.out.print(report);

        // The only assertion is that every arm ran: the figures above are the point.
        for (double throughput : throughputs) {
            assertThat(throughput).isGreaterThan(0d);
        }
    }

    /**
     * Parks {@code parkedRecords} records on one partition, then times {@link #HEALTHY_RECORDS} healthy ones
     * through the same route. A fresh instance each time, so one arm's parked set cannot reach the next.
     *
     * @return records per second for the healthy records
     */
    private double measureHealthyThroughput(int parkedRecords) {
        var runtime = new RecordingClientRuntime();
        var processed = new AtomicInteger();
        var pc = ParallelConsumer.connect(props()).defaultOrdering(ProcessingOrder.UNORDERED);
        pc.string(TOPIC).process(context -> {
            if (context.value().startsWith("park")) {
                // Declared hopeless by the function: no attempts spent, and no stack trace in the log for each of
                // twenty thousand records.
                return Outcome.park("this record is parked for the measurement");
            }
            processed.incrementAndGet();
            return Outcome.succeeded();
        });

        handle = runtime.startAndAssign(pc, 1);
        long offset = 0;
        for (int i = 0; i < parkedRecords; i++) {
            runtime.publish(TOPIC, 0, offset++, "park-" + i, "park");
        }
        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(120)).until(() ->
                dispatcher.parkedCount() == parkedRecords);

        long startedAt = System.nanoTime();
        for (int i = 0; i < HEALTHY_RECORDS; i++) {
            runtime.publish(TOPIC, 0, offset++, "healthy-" + i, "healthy");
        }
        Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> processed.get() == HEALTHY_RECORDS);
        double seconds = (System.nanoTime() - startedAt) / 1_000_000_000d;

        closeTheInstance();
        return HEALTHY_RECORDS / seconds;
    }
}
