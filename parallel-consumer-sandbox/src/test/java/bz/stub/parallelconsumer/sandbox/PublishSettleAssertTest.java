package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.fluent.AfterRetries;
import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.fluent.ParkedRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The sandbox's front door: the caller publishes, waits for the run to settle, and asserts - all from its own
 * thread, inside one try-with-resources.
 *
 * <h2>Why this is the primary shape and the driver is the convenience</h2>
 * A test knows what it wants the instance to see. Handing it a rate and a stopping rule and then asserting on
 * whatever came out is a longer way round, and it makes every assertion a statement about a population rather
 * than about a record. So {@code publish} and {@code awaitSettled} are the pair, and the driver stays for the
 * cases that really are about volume: a soak, and a demo.
 *
 * <h2>Why there is a settle at all</h2>
 * The broker-free drivers of the stream-processing libraries users compare us with pipe a record and process it
 * on the caller's thread, so an assertion on the next line is already safe. This engine is concurrent by
 * construction - polled on one thread, dispatched on a worker, committed on the control thread - so a publish
 * that returned would tell a test nothing at all. That is what {@code awaitSettled} is for, and it is the whole
 * difference between the two shapes.
 *
 * <h2>What is NOT retested here</h2>
 * The accounting the settle is built on - what a committed offset means, why a parked record counts, the
 * transactional path - is {@code CommittedOffsetWaitTest}'s, driven at the consumer where it can be exercised
 * without an engine. What this file adds is the shape above it: that a caller's own records reach the real
 * routes, that the settle refuses rather than returning quietly when a run ends early, and that the driver's
 * vocabulary is refused on a sandbox nothing is driving.
 */
@Timeout(60)
class PublishSettleAssertTest {

    private static final String ORDERS_TOPIC = "orders";

    private static final TopicPartition ORDERS_0 = new TopicPartition(ORDERS_TOPIC, 0);

    /**
     * Long enough that only an ALREADY-settled sandbox can satisfy it: the wait reads the accounting once before
     * it consults its deadline, so a settle that has nothing outstanding returns on that first read and one that
     * has anything outstanding refuses. It is not a measurement of how fast the engine is.
     */
    private static final Duration NO_TIME_AT_ALL = Duration.ofMillis(1);

    /**
     * Short, because the one use of it is a wait that is meant to fail - the default budget is twenty seconds and
     * a test of the refusal should not pay it.
     */
    private static final Duration IMPATIENT = Duration.ofSeconds(2);

    @Test
    void aDefinitionConsumesTheRecordsTheCallerPublishedAndCommitsThem() {
        ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC).process(context -> {
            seen.add(context.value());
            return Outcome.succeeded();
        });

        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            assertThat(sandbox.publish(ORDERS_TOPIC, "cust-1", "first")).isEqualTo(0L);
            assertThat(sandbox.publish(ORDERS_TOPIC, "cust-2", "second")).isEqualTo(1L);
            assertThat(sandbox.publish(ORDERS_TOPIC, "cust-1", "third")).isEqualTo(2L);

            sandbox.awaitSettled();

            assertWithMessage("the route's own function should have been handed each published value, decoded "
                    + "with the format the route declared")
                    .that(seen).containsExactly("first", "second", "third");
            assertWithMessage("a committed offset is the NEXT offset to consume, so three published and all "
                    + "complete commits at three")
                    .that(SandboxFixtures.highestCommittedOffset(sandbox, ORDERS_0)).isEqualTo(3L);
            assertWithMessage("nothing failed, so nothing should be parked")
                    .that(instance.parkedAllTopics().count()).isEqualTo(0);
        }
    }

    /**
     * The same shape on the classic API, where the caller owns the instance and nothing is encoded: a mock
     * consumer holds records of the instance's own types, so the function receives the very object published.
     */
    @Test
    void aClassicInstanceConsumesTheRecordsTheCallerPublishedAndCommitsThem() {
        ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
        Sandbox sandbox = Sandbox.builder().handPublished().build();

        try (ClassicSandbox<String, String> classic =
                     sandbox.classic(String.class, String.class, ORDERS_TOPIC)) {
            ParallelEoSStreamProcessor<String, String> pc =
                    new ParallelEoSStreamProcessor<>(SandboxFixtures.partitionOrdered(classic));
            try {
                pc.subscribe(classic.topics());
                pc.poll(context -> seen.add(context.getSingleRecord().value()));
                // The classic API has no seam that sees the instance, so the assignment the fluent path gets in
                // ClientRuntime#started is said out loud here - after subscribe, or there is no rebalance
                // listener to assign to.
                classic.assignAfterSeeding();

                assertThat(classic.publish(ORDERS_TOPIC, "cust-1", "first")).isEqualTo(0L);
                assertThat(classic.publish(ORDERS_TOPIC, "cust-2", "second")).isEqualTo(1L);

                classic.awaitSettled();

                assertThat(seen).containsExactly("first", "second");
                assertThat(classic.consumer().highestCommittedOffsets()).containsEntry(ORDERS_0, 2L);
            } finally {
                // The caller built this instance, so the caller closes it: neither the classic sandbox nor a
                // bound owns an instance nobody asked them to drive.
                pc.closeDrainFirst();
            }
        }
    }

    /**
     * A settle with nothing outstanding returns on its first read of the accounting rather than waiting for
     * anything, which is what makes it cheap enough to call after every publish rather than once at the end.
     */
    @Test
    void aSettleWithNothingOutstandingReturnsWithoutWaiting() {
        ParallelConsumerDefinition definition =
                SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC).process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ParallelConsumerInstance ignoredInstance = definition.start(sandbox)) {
            var ignoredOffset = sandbox.publish(ORDERS_TOPIC, "cust-1", "first");
            sandbox.awaitSettled();

            // A budget nothing could be waited out in: only a sandbox that is already settled can satisfy this,
            // and one with anything outstanding refuses instead of passing slowly.
            sandbox.awaitSettled(NO_TIME_AT_ALL);
        }
    }

    /**
     * A function that throws is an ordinary failed attempt, and a route that declares a retry limit parks the
     * record when it runs out - which is a terminal outcome, so the run settles. The settle therefore <b>returns
     * rather than hanging</b>, and the failure the function threw is readable afterwards on the parked record.
     * <p>
     * This is the shape an assertion inside a processing function takes: it throws, and a test whose settle sat
     * out its whole budget and then blamed the engine would be the worst possible report of it.
     */
    @Test
    void aFunctionThatThrowsParksItsRecordAndTheSettleReturnsWithTheFailureReadable() {
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC)
                // No attempts after the first, and no delay before giving up: the park is what this test is about,
                // not how long the engine waits before it.
                .retryLimit(0)
                .retryDelay(Duration.ZERO)
                .process(context -> {
                    throw new IllegalStateException("the inventory service said no");
                });

        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            var ignoredOffset = sandbox.publish(ORDERS_TOPIC, "cust-1", "first");

            sandbox.awaitSettled();

            List<ParkedRecord> parked = instance.parkedAllTopics().records();
            assertWithMessage("the record ran out of attempts, so it parked - and a park is a settled record")
                    .that(parked).hasSize(1);
            assertThat(parked.get(0).topic()).isEqualTo(ORDERS_TOPIC);
            assertWithMessage("the function's own exception is what a reader needs, so it is carried rather "
                    + "than replaced by the engine's account of the attempt")
                    .that(parked.get(0).failure()).hasMessageThat().contains("the inventory service said no");
        }
    }

    /**
     * A route that reacts to exhaustion by stopping the instance is the one ending that <b>satisfies the settle's
     * own accounting and is still not a settled run</b>, which is why the settle checks for it separately.
     * <p>
     * The engine marks the stopping record never-due before the stop is raised, so it sits in the retry queue and
     * the parked view reports it - and a park counts as accounted for. Written without that check, the settle
     * returned cleanly here and the test that follows would have asserted about a run that stopped at its first
     * record. That is the regression this test exists to catch, and it is not hypothetical: it is what the first
     * version of the settle did.
     */
    @Test
    void aRouteThatStopsTheInstanceIsSurfacedByTheSettleRatherThanReadAsASettledRun() {
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ZERO)
                .afterRetries(AfterRetries.stop())
                .process(context -> {
                    throw new IllegalStateException("this deployment is wrong, not this record");
                });

        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            var ignoredOffset = sandbox.publish(ORDERS_TOPIC, "cust-1", "first");

            IllegalStateException refusal = assertThrows(IllegalStateException.class, sandbox::awaitSettled);

            assertWithMessage("a bare shortfall would send the reader to the engine for a cause the definition "
                    + "declared, so the refusal names the stop")
                    .that(refusal).hasMessageThat().contains("A route stopped the instance at orders-0@0");
            assertThat(refusal).hasMessageThat().contains("ran out of attempts");
            assertWithMessage("and the instance records it too, which is what the refusal read")
                    .that(instance.stopRequest()).isPresent();
        }
    }

    /**
     * The settle refuses when the budget runs out with a record the instance is still holding - the ordinary
     * failure, kept honest with a route that retries for ever so that nothing can settle it.
     */
    @Test
    void aRecordNothingWillEverFinishIsNamedByTheSettlesRefusal() {
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC)
                .retryForever()
                .retryDelay(Duration.ofMillis(50))
                .process(context -> {
                    throw new IllegalStateException("and again");
                });

        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ParallelConsumerInstance ignoredInstance = definition.start(sandbox)) {
            var ignoredOffset = sandbox.publish(ORDERS_TOPIC, "cust-1", "first");

            IllegalStateException refusal =
                    assertThrows(IllegalStateException.class, () -> sandbox.awaitSettled(IMPATIENT));

            assertWithMessage("the refusal has to name the partition and what it is short of, or it says no "
                    + "more than a timeout would")
                    .that(refusal).hasMessageThat()
                    .contains("{orders-0=published 1, completed 0, parked 0, so 1 unaccounted for}");
        }
    }

    @Test
    void publishingToATopicNoRouteClaimsIsRefusedNamingTheOnesThatAreRouted() {
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC).process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ParallelConsumerInstance ignoredInstance = definition.start(sandbox)) {
            IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                    () -> sandbox.publish("parcel-scans", "cust-1", "first"));

            assertWithMessage("a record published to an unrouted topic would never be delivered, and a silent "
                    + "publish would read as a function that never ran")
                    .that(refusal).hasMessageThat().contains("[orders]");
        }
    }

    @Test
    void publishingBeforeTheDefinitionHasStartedIsRefusedRatherThanFailingOnANullConsumer() {
        Sandbox sandbox = Sandbox.builder().handPublished().build();

        IllegalStateException refusal = assertThrows(IllegalStateException.class,
                () -> sandbox.publish(ORDERS_TOPIC, "cust-1", "first"));

        assertThat(refusal).hasMessageThat().contains("has not been started");
    }

    /**
     * Nothing is driving a hand-published sandbox, so a bound is a thing it can never reach. Waiting for one would
     * spend the caller's whole timeout and then answer false, which reads as a run that was too slow rather than
     * as a call that could never have been satisfied.
     */
    @Test
    void awaitingABoundOnAHandPublishedSandboxIsRefusedAndSaysWhatToCallInstead() {
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC).process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder().handPublished().bound(Bound.afterRecords(1)).build();
        try (ParallelConsumerInstance ignoredInstance = definition.start(sandbox)) {
            IllegalStateException refusal = assertThrows(IllegalStateException.class,
                    () -> sandbox.awaitBound(Duration.ofSeconds(1)));

            assertThat(refusal).hasMessageThat().contains("awaitSettled()");
        }
    }
}
