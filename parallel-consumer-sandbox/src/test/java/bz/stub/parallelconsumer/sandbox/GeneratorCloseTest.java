package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * What {@link RecordGenerator#close()} may interrupt, and what it may not.
 * <p>
 * The interrupt exists to end a sleep between two ticks. Once the bound has been reached the generator thread is
 * inside the bound's own sequence instead - wait for the instance to account for every published record, then
 * close it - and an interrupt there makes
 * {@code SandboxConsumer#awaitEveryPublishedRecordCommitted} return as though it had succeeded and hands
 * {@code handle.close()} a thread carrying an interrupt, which is the state Parallel Consumer's own close path
 * warns about. The final commit can then be skipped and the run reads as a flake.
 * <p>
 * Driven at the generator rather than through a run, and with latches rather than a clock: the interesting
 * interleaving is "close lands while the bound sequence is in flight", which a real run reaches only by accident.
 */
@Timeout(60)
class GeneratorCloseTest {

    @Test
    void closingAGeneratorThatHasReachedItsBoundLetsTheBoundSequenceFinish() throws Exception {
        CountDownLatch insideTheBoundSequence = new CountDownLatch(1);
        CountDownLatch letTheBoundSequenceFinish = new CountDownLatch(1);
        AtomicBoolean interruptedInsideTheBoundSequence = new AtomicBoolean();

        RecordGenerator generator = new RecordGenerator(
                Collections.singletonList(new CountingFeed()), 1000, Bound.afterRecords(1),
                () -> {
                    insideTheBoundSequence.countDown();
                    try {
                        // Interruptible, so an interrupt delivered by close() is seen here at once rather than
                        // having to be inferred.
                        letTheBoundSequenceFinish.await();
                    } catch (InterruptedException interrupted) {
                        interruptedInsideTheBoundSequence.set(true);
                        Thread.currentThread().interrupt();
                    }
                });

        generator.start();
        assertWithMessage("the one-record bound should have been reached and its sequence entered")
                .that(insideTheBoundSequence.await(30, TimeUnit.SECONDS)).isTrue();

        // Exactly what a test exiting its try-with-resources does, on its own thread because close() joins.
        Thread closer = new Thread(generator::close, "test-closer");
        closer.start();
        // The closer sitting in join() is the observable proof that close() has already decided whether to
        // interrupt - so the assertion below is about that decision, not about who won a race.
        // The interrupt arm is in the condition too, so a close that DOES interrupt fails on the assertion below
        // rather than on this wait running out - a timeout says nothing about which of the two happened.
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() ->
                interruptedInsideTheBoundSequence.get()
                        || closer.getState() == Thread.State.TIMED_WAITING
                        || closer.getState() == Thread.State.WAITING);

        assertWithMessage("close() must not interrupt a generator that is already inside the bound's close - the "
                + "interrupt is for a generator still sleeping between ticks")
                .that(interruptedInsideTheBoundSequence.get()).isFalse();

        letTheBoundSequenceFinish.countDown();
        closer.join(TimeUnit.SECONDS.toMillis(30));
        assertThat(closer.isAlive()).isFalse();
        assertThat(generator.boundWasReached()).isTrue();
    }

    /**
     * A generator that has NOT reached its bound is still interrupted out of its sleep, which is the case the
     * interrupt exists for - so the fix above did not simply remove it.
     */
    @Test
    void closingAGeneratorThatIsStillGeneratingStillInterruptsItOutOfItsSleep() {
        CountingFeed feed = new CountingFeed();
        // One record every ten seconds: without an interrupt the close would sit out the join's full ten.
        RecordGenerator generator = new RecordGenerator(Collections.singletonList(feed), 0.1, Bound.none(),
                () -> {
                    throw new AssertionError("an unbounded generator never reaches a bound");
                });

        generator.start();
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> feed.published.get() >= 1);

        generator.close();

        assertWithMessage("close() returns once the generator thread has gone, which needs the interrupt: the "
                + "next tick is ten seconds away")
                .that(generator.awaitFinished(Duration.ofSeconds(1))).isTrue();
    }

    /**
     * A feed that publishes nothing anywhere - the generator's pacing and bound are what these tests are about,
     * and a consumer would only add a second thing that can fail.
     */
    private static final class CountingFeed implements TopicFeed {

        private final AtomicLong published = new AtomicLong();

        @Override
        public String topic() {
            return "orders";
        }

        @Override
        public boolean publish(long index) {
            published.incrementAndGet();
            return true;
        }
    }
}
