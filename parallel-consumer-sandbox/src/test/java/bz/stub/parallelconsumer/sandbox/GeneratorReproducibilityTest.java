package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

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

    private static final long SEED = 2026;

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
     * The whole run, end to end - and two claims that are worth keeping apart, because only one of them is what
     * "reproducible" means.
     *
     * <ol>
     *   <li><b>What the generator produced.</b> A seeded run generates the sequence its seed and indices name,
     *       and both runs of a seed generate the same one. That is a property of the generator alone, so it is
     *       asserted against the sequence recomputed from the seed and compared without regard to the order the
     *       engine happened to deliver it in.</li>
     *   <li><b>What was consumed.</b> Every generated record reached the function exactly once - nothing dropped,
     *       nothing delivered twice.</li>
     * </ol>
     *
     * <p>This used to be one assertion comparing the two runs' delivery order element by element, which made the
     * consumption schedule part of the definition of a reproducible generator. It was also the test that caught
     * the real defect underneath (astubbs#504): under load a run delivered nineteen of twenty records, because
     * the bound closed the instance drain-first while the worker pool still held queued tasks and the close
     * cleared that queue. The bound now waits for every published record's offset to commit before it closes -
     * see {@link SandboxConsumer#awaitEveryPublishedRecordCommitted()} - and the completeness claim above is what
     * guards it.
     */
    @Test
    void aRunOfASeedGeneratesAndDeliversExactlyTheRecordsThatSeedNames() {
        List<String> expected = theSequenceTheSeedNames(SEED, RECORDS);

        List<String> firstRun = runAndCollect(SEED);
        List<String> secondRun = runAndCollect(SEED);

        assertWithMessage("what the generator produced: run one should be the sequence seed %s names, in "
                + "whatever order it was delivered", SEED)
                .that(firstRun).containsExactlyElementsIn(expected);
        assertWithMessage("what the generator produced: run two of the same seed should be the same sequence, "
                + "which is the whole of what a seed promises")
                .that(secondRun).containsExactlyElementsIn(expected);

        assertWithMessage("what was consumed: every generated record reached the function, exactly once")
                .that(firstRun).hasSize(RECORDS);
        assertWithMessage("what was consumed: every generated record reached the function, exactly once")
                .that(secondRun).hasSize(RECORDS);
    }

    /**
     * The sequence the generator will produce for a seed, derived the same way {@code ClassicSandbox.TypedFeed}
     * derives it - one topic, so the record index is the tick.
     */
    private static List<String> theSequenceTheSeedNames(long seed, int records) {
        RandomObjects random = RandomObjects.seededWith(seed);
        List<String> sequence = new ArrayList<>();
        for (int index = 0; index < records; index++) {
            sequence.add(random.create(Order.class, index).toString());
        }
        return sequence;
    }

    private static List<String> runAndCollect(long seed) {
        ConcurrentLinkedQueue<Order> seen = new ConcurrentLinkedQueue<>();
        Sandbox sandbox = Sandbox.builder()
                .perSecond(1000)
                .seed(seed)
                .bound(Bound.afterRecords(RECORDS))
                .build();

        try (ClassicSandbox<String, Order> classic = sandbox.classic(String.class, Order.class, "orders")) {
            ParallelEoSStreamProcessor<String, Order> pc = SandboxFixtures.startClassic(classic,
                    SandboxFixtures.partitionOrdered(classic),
                    context -> seen.add(context.getSingleRecord().value()));
            // The bound waits for every published record to commit and then closes this instance itself, so this
            // returning true is already the end of the run rather than the middle of it.
            assertThat(classic.awaitBound(Duration.ofSeconds(30))).isTrue();
            // Idempotent - the bound has already closed it. Kept so that a run whose bound was never reached
            // still shuts its instance down rather than leaving the threads behind.
            pc.closeDrainFirst();
        }

        List<String> asText = new ArrayList<>();
        for (Order order : seen) {
            asText.add(order.toString());
        }
        return asText;
    }
}
