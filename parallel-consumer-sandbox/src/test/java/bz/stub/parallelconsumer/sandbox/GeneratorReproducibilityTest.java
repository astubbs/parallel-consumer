package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.sandbox.demo.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * A seeded run is reproducible: the same seed generates the same records twice.
 * <p>
 * <b>Addressed by index, not by sequence.</b> The seed for record <em>n</em> is derived from the base seed and
 * <em>n</em>, so the record at a given index is the same whatever order the topics were served in and whatever
 * the pacing did - which is what makes "reproduce record 4173" a thing you can do without replaying the four
 * thousand before it. The alternative, one random stream consumed in order, would make reproducibility a property
 * of the thread schedule.
 * <p>
 * Two levels, because they can fail independently: the generator itself, and a whole run through it.
 */
@Timeout(60)
class GeneratorReproducibilityTest {

    private static final int RECORDS = 20;

    @Test
    void theSameSeedAndIndexGiveTheSameObject() {
        RandomObjects first = RandomObjects.seededWith(4711);
        RandomObjects second = RandomObjects.seededWith(4711);

        for (int index = 0; index < 10; index++) {
            assertWithMessage("record %s of seed 4711", index)
                    .that(first.create(Order.class, index).toString())
                    .isEqualTo(second.create(Order.class, index).toString());
        }
    }

    @Test
    void aDifferentSeedGivesDifferentObjects() {
        Order fromOneSeed = RandomObjects.seededWith(1).create(Order.class, 0);
        Order fromAnother = RandomObjects.seededWith(2).create(Order.class, 0);

        assertWithMessage("two seeds producing the same record would make the seed decorative")
                .that(fromOneSeed.toString()).isNotEqualTo(fromAnother.toString());
    }

    @Test
    void anIndexIsReachableWithoutGeneratingTheOnesBeforeIt() {
        RandomObjects inOrder = RandomObjects.seededWith(99);
        for (int index = 0; index < 7; index++) {
            Order ignoredWarmUp = inOrder.create(Order.class, index);
            assertThat(ignoredWarmUp).isNotNull();
        }
        Order seventhAfterSix = inOrder.create(Order.class, 7);

        Order seventhOnItsOwn = RandomObjects.seededWith(99).create(Order.class, 7);

        assertWithMessage("record 7 must not depend on records 0 to 6 having been generated first")
                .that(seventhAfterSix.toString()).isEqualTo(seventhOnItsOwn.toString());
    }

    /**
     * The whole run, end to end: two sandboxes of the same seed hand the same records, in the same order, to the
     * same function. Partition ordering with one partition, so that "the same order" is a property of the
     * generator rather than of the scheduler.
     */
    @Test
    void twoRunsOfTheSameSeedDeliverTheSameRecordsInTheSameOrder() {
        List<String> firstRun = runAndCollect(2026);
        List<String> secondRun = runAndCollect(2026);

        assertThat(firstRun).hasSize(RECORDS);
        assertThat(secondRun).isEqualTo(firstRun);
    }

    private static List<String> runAndCollect(long seed) {
        ConcurrentLinkedQueue<Order> seen = new ConcurrentLinkedQueue<>();
        Sandbox sandbox = Sandbox.builder()
                .perSecond(1000)
                .seed(seed)
                .bound(Bound.afterRecords(RECORDS))
                .build();

        try (ClassicSandbox<String, Order> classic = sandbox.classic(String.class, Order.class, "orders")) {
            ParallelEoSStreamProcessor<String, Order> pc = new ParallelEoSStreamProcessor<>(
                    ParallelConsumerOptions.<String, Order>builder()
                            .consumer(classic.consumer())
                            .ordering(ProcessingOrder.PARTITION)
                            .build());
            pc.subscribe(classic.topics());
            pc.poll(context -> seen.add(context.getSingleRecord().value()));
            classic.startGenerating(pc);
            assertThat(classic.awaitBound(Duration.ofSeconds(30))).isTrue();
            pc.closeDrainFirst();
        }

        List<String> asText = new ArrayList<>();
        for (Order order : seen) {
            asText.add(order.toString());
        }
        return asText;
    }
}
