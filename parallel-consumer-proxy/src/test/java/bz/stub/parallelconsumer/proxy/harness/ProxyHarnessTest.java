package bz.stub.parallelconsumer.proxy.harness;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Proves the harness on an in-JVM stub client - including that it can FAIL, which is the property that makes
 * it worth sharing: a fixture that goes green when the thing it drives is broken would go green for ten
 * languages at once.
 * <p>
 * Everything here runs in the surefire lane: {@code MockConsumer} and {@code MockProducer} only, no broker, no
 * Docker, no Testcontainers.
 *
 * @author Antony Stubbs
 */
@Timeout(120)
class ProxyHarnessTest {

    /**
     * The negative control's budget: long enough for several commit intervals to have fired if a commit were
     * ever going to happen, short enough not to drag the suite.
     */
    static final Duration NEGATIVE_CONTROL_BUDGET = Duration.ofSeconds(3);

    static final String SIMULATED_FAILURE_REASON = "simulated client failure, for the redelivery scenario";

    /** The harness drives a trivial in-JVM client through one record and reports the offset advanced. */
    @Test
    void aProcessedRecordAdvancesTheCommittedOffset() throws Exception {
        try (var harness = new ProxyHarness(HarnessScenario.A_PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET)) {
            var processedCount = new AtomicInteger();

            harness.start(record -> processedCount.incrementAndGet());
            harness.seed();

            harness.awaitCommittedOffset(1);

            assertWithMessage("the client function ran exactly once")
                    .that(processedCount.get()).isEqualTo(1);
            assertThat(harness.deliveries()).hasSize(1);
            var delivery = harness.deliveries().get(0);
            assertThat(delivery.offset()).isEqualTo(0);
            assertThat(delivery.failedAttempts()).isEqualTo(0);
            assertWithMessage("nothing was produced - the client only consumed")
                    .that(harness.producedRecords()).isEmpty();
        }
    }

    /**
     * The negative control: a client that reports nothing leaves the harness FAILING on its convergence
     * condition rather than passing. The record demonstrably arrives (arrival-sync on the non-zero state
     * first), and the commit then never happens.
     */
    @Test
    void anUnreportedRecordHoldsBackTheCommit() {
        var harness = new ProxyHarness(HarnessScenario.AN_UNREPORTED_RECORD_HOLDS_BACK_THE_COMMIT);
        var neverReported = new CountDownLatch(1);
        try {
            harness.start(record -> neverReported.await());
            harness.seed();

            // arrival-sync: the delivery happened, so the missing commit below is the client's silence, not a
            // broken pipeline
            harness.awaitDeliveries(1);

            assertThrows(ConditionTimeoutException.class,
                    () -> harness.awaitCommittedOffset(1, NEGATIVE_CONTROL_BUDGET),
                    "a client that reports nothing must fail the harness's convergence condition");
            assertWithMessage("no offset was committed while the record went unreported")
                    .that(harness.lastCommittedOffset().isEmpty()).isTrue();
        } finally {
            // release the record before teardown, so close() is not fighting a deliberately wedged client
            neverReported.countDown();
            harness.close();
        }
    }

    /** A client that reports a failure produces a redelivery with the attempt count incremented. */
    @Test
    void aFailedRecordIsRedeliveredWithItsFailureHistory() throws Exception {
        try (var harness = new ProxyHarness(HarnessScenario.A_FAILED_RECORD_IS_REDELIVERED_WITH_ITS_FAILURE_HISTORY)) {
            var failedOnce = new AtomicBoolean(false);

            harness.start(record -> {
                if (!failedOnce.getAndSet(true)) {
                    throw new RuntimeException(SIMULATED_FAILURE_REASON);
                }
            });
            harness.seed();

            harness.awaitCommittedOffset(1);

            var deliveries = harness.deliveries();
            assertWithMessage("one failure, one redelivery").that(deliveries).hasSize(2);

            var first = deliveries.get(0);
            assertThat(first.failedAttempts()).isEqualTo(0);
            assertWithMessage("a first delivery carries no failure history")
                    .that(first.lastFailureReason().isEmpty()).isTrue();

            var second = deliveries.get(1);
            assertThat(second.offset()).isEqualTo(first.offset());
            assertWithMessage("the redelivery carries the incremented attempt count")
                    .that(second.failedAttempts()).isEqualTo(1);
            assertWithMessage("the redelivery carries the earlier failure's reason")
                    .that(ExceptionUtils.getRootCause(second.lastFailureReason().orElseThrow()).getMessage())
                    .isEqualTo(SIMULATED_FAILURE_REASON);
        }
    }

    /** Seeded records with distinct keys land on distinct shards; records sharing a key land on one. */
    @Test
    void recordsSharingAKeyShareAShardAndDistinctKeysRunConcurrently() throws Exception {
        try (var harness = new ProxyHarness(
                HarnessScenario.RECORDS_SHARING_A_KEY_SHARE_A_SHARD_DISTINCT_KEYS_RUN_CONCURRENTLY)) {
            var sharedHeadInFlight = new CountDownLatch(1);
            var releaseSharedHead = new CountDownLatch(1);

            harness.start(record -> {
                if (record.offset() == 0) { // the head of the "shared" key's shard
                    sharedHeadInFlight.countDown();
                    releaseSharedHead.await();
                }
            });
            harness.seed();

            assertWithMessage("the shared key's head record reached the client")
                    .that(sharedHeadInFlight.await(ProxyHarness.CONVERGENCE_BUDGET.toSeconds(), SECONDS))
                    .isTrue();

            // distinct key, distinct shard: offset 2 proceeds while the shared shard's head is still in flight
            Awaitility.await().atMost(ProxyHarness.CONVERGENCE_BUDGET).untilAsserted(() ->
                    assertWithMessage("the distinct key's record was delivered concurrently")
                            .that(deliveredOffsets(harness)).contains(2L));

            // shared key, one shard: its second record must not be in flight while its first still is
            assertWithMessage("the shared key's second record was held back behind its shard's head")
                    .that(deliveredOffsets(harness)).doesNotContain(1L);

            releaseSharedHead.countDown();
            harness.awaitCommittedOffset(3);

            var sharedShardOffsets = harness.deliveries().stream()
                    .filter(delivery -> delivery.key().equals("shared"))
                    .map(ProxyHarness.Delivery::offset)
                    .toList();
            assertWithMessage("the shared shard delivered in offset order")
                    .that(sharedShardOffsets).containsExactly(0L, 1L).inOrder();
        }
    }

    /**
     * The engine seam is stubbed, deliberately and loudly, until the engine units land - and the refusal names
     * them, so a client author who arrives early is routed rather than stranded.
     */
    @Test
    void engineSeamRefusesLoudlyUntilTheEngineUnitsLand() {
        try (var harness = new ProxyHarness(HarnessScenario.A_PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET)) {
            var pending = assertThrows(UnsupportedOperationException.class, harness::startEngine);
            assertThat(pending).hasMessageThat().contains("ProxyProcessor");
            assertThat(pending).hasMessageThat().contains("start(Client)");
        }
    }

    /** The scenario registry resolves every conformance scenario by its stable name, and refuses strangers. */
    @Test
    void everyConformanceScenarioIsResolvableByItsStableName() {
        for (var scenario : HarnessScenario.conformanceScenarios()) {
            assertWithMessage("scenario '%s' resolves to itself", scenario.name())
                    .that(HarnessScenario.byName(scenario.name()).orElseThrow()).isEqualTo(scenario);
        }
        assertThat(HarnessScenario.byName("no-such-scenario").isEmpty()).isTrue();
    }

    private static List<Long> deliveredOffsets(ProxyHarness harness) {
        return harness.deliveries().stream().map(ProxyHarness.Delivery::offset).toList();
    }
}
