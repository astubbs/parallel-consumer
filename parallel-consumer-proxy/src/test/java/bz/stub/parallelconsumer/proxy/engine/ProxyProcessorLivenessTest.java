package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.proxy.engine.ProxyProcessor.ReportResult;
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The liveness half of the engine (the language-proxy plan's U8): leases and heartbeats, the reconnect window,
 * manifest reconciliation, reported worker death - and the two rebalance-shaped races the engine-wave review
 * handed to this unit.
 * <p>
 * <b>Time is the fixture's clock, and overlap is a latch.</b> Every deadline scenario advances
 * {@link EngineFixture.TestClock} in one step rather than sleeping, and the two scenarios that turn on a
 * genuine interleaving stop one thread inside the registry with {@link LatchHook} rather than hoping a sleep
 * lands in the right window. Nothing here is timing-dependent.
 * <p>
 * Every scenario ends on the standing leak check: {@code getNumberRecordsOutForProcessing()} back at zero.
 * A liveness path that removes a registry entry without handing the container to the mailbox drifts that
 * counter and stalls the consumer with no exception, so it is the one assertion every path shares.
 *
 * @author Antony Stubbs
 */
@Timeout(120)
class ProxyProcessorLivenessTest {

    /** Comfortably past {@link LivenessSettings#DEFAULT_LEASE_DURATION} and the window, on the test clock. */
    private static final Duration WELL_PAST_EVERY_DEADLINE = Duration.ofMinutes(5);

    /** How long a negative control watches for a dispatch that must not arrive. */
    private static final Duration NEGATIVE_CONTROL_BUDGET = Duration.ofMillis(300);

    private final EngineFixture fixture = new EngineFixture("proxy-liveness-test");

    @AfterEach
    void closeFixture() {
        fixture.close();
    }

    /**
     * AE21 end to end: a worker whose function runs far longer than the lease keeps its record while its admin
     * heartbeats, and the moment the heartbeats stop the record returns to scheduling - with its attempt count
     * unchanged, because no verdict was ever reached.
     */
    @Test
    void aSlowWorkerKeepsItsRecordWhileHeartbeatsContinueAndLosesItWhenTheyStop() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("lone-key", "runs-for-hours");

        var first = fixture.takeDispatch();
        assertThat(first.getAttempt()).isEqualTo(1);

        for (int leasePeriod = 0; leasePeriod < 5; leasePeriod++) {
            fixture.clock.advance(LivenessSettings.DEFAULT_LEASE_DURATION.multipliedBy(2));
            fixture.processor.heartbeat();
            assertWithMessage("a heartbeating session must keep its record through lease period %s",
                    leasePeriod).that(fixture.pollDispatch(NEGATIVE_CONTROL_BUDGET)).isNull();
            assertThat(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(1);
        }

        // the heartbeats stop
        fixture.clock.advance(WELL_PAST_EVERY_DEADLINE);

        var redelivery = fixture.takeDispatch();
        assertWithMessage("an expired lease reaches no verdict, so the attempt count must not move")
                .that(redelivery.getAttempt()).isEqualTo(1);
        assertThat(redelivery.hasLastFailureReason()).isFalse();
        assertThat(redelivery.getToken().getEpoch()).isEqualTo(2);

        assertThat(fixture.reportSuccess(redelivery.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /** A session that never negotiated {@code heartbeat} sends none - and must never lose a record for it. */
    @Test
    void aSessionWithoutTheHeartbeatCapabilityNeverLosesARecordToALease() {
        fixture.startWith(options -> options.ordering(ProcessingOrder.KEY),
                ProxyProcessor.DEFAULT_COALESCING_WINDOW,
                LivenessSettings.defaults(false, fixture.clock), InFlightRegistry.Hook.NO_OP);
        fixture.seed("lone-key", "no-lease-here");

        var dispatch = fixture.takeDispatch();
        fixture.clock.advance(Duration.ofDays(1));

        assertWithMessage("no lease clock may run on a session that negotiated no heartbeats")
                .that(fixture.pollDispatch(NEGATIVE_CONTROL_BUDGET)).isNull();

        assertThat(fixture.reportSuccess(dispatch.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /**
     * R42 and R44: connection loss holds the records rather than returning them, and only the window's expiry
     * returns them - attempt counts unchanged. The held phase is asserted as a negative control, because
     * "returned immediately" is exactly the bug the window exists to prevent and it would otherwise pass.
     */
    @Test
    void connectionLossHoldsTheRecordsAndTheWindowsExpiryReturnsThem() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("key-a", "one");
        fixture.seed("key-b", "two");
        var held = takeDispatches(2);

        fixture.processor.onConnectionLost();

        fixture.clock.advance(LivenessSettings.DEFAULT_RECONNECT_WINDOW.minusSeconds(1));
        assertWithMessage("records must be HELD inside the window, not returned to scheduling")
                .that(fixture.pollDispatch(NEGATIVE_CONTROL_BUDGET)).isNull();
        assertThat(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(2);

        fixture.clock.advance(Duration.ofSeconds(2));

        var redelivered = takeDispatches(2);
        assertThat(redelivered.keySet()).containsExactlyElementsIn(held.keySet());
        for (DispatchRecord redelivery : redelivered.values()) {
            assertWithMessage("the window reached no verdict, so attempt counts must not move")
                    .that(redelivery.getAttempt()).isEqualTo(1);
        }

        redelivered.values().forEach(dispatch -> fixture.reportSuccess(dispatch.getToken()));
        fixture.awaitCommittedOffset(2);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /**
     * AE20: the primary reclaim path. A reported worker death returns the tokens that worker held immediately,
     * without waiting for the window - proven by the clock not moving at all.
     */
    @Test
    void aReportedWorkerDeathReturnsItsRecordsBeforeTheWindowElapses() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("key-a", "one");
        fixture.seed("key-b", "two");
        var held = takeDispatches(2);

        fixture.processor.onConnectionLost();
        int returned = fixture.processor.onWorkerDied(
                held.values().stream().map(DispatchRecord::getToken).collect(java.util.stream.Collectors.toList()));

        assertThat(returned).isEqualTo(2);
        var redelivered = takeDispatches(2);
        assertThat(redelivered.keySet()).containsExactlyElementsIn(held.keySet());
        for (DispatchRecord redelivery : redelivered.values()) {
            assertThat(redelivery.getAttempt()).isEqualTo(1);
        }

        redelivered.values().forEach(dispatch -> fixture.reportSuccess(dispatch.getToken()));
        fixture.awaitCommittedOffset(2);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /** A worker-death notice naming a delivery that has already ended is a stale notice, and acts on nothing. */
    @Test
    void aWorkerDeathNoticeForASupersededDeliveryIsIgnored() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("lone-key", "fails-once");

        var first = fixture.takeDispatch();
        fixture.reportFailure(first.getToken(), "transient");
        var redelivery = fixture.takeDispatch();
        assertThat(redelivery.getToken().getEpoch()).isEqualTo(2);

        int returned = fixture.processor.onWorkerDied(List.of(first.getToken()));

        assertThat(returned).isEqualTo(0);
        assertWithMessage("the live delivery must be untouched by a stale death notice")
                .that(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(1);

        fixture.reportSuccess(redelivery.getToken());
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /**
     * AE19: the connection dropped with A, B and C in flight; the client reconnects naming A at its current
     * delivery and B at a superseded one. A stays in flight untouched, B is ordered dropped, and C - which no
     * live worker holds - returns to scheduling with its attempt count unchanged.
     */
    @Test
    void aReconnectManifestKeepsDropsAndReturnsInOnePass() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("key-a", "a");
        fixture.seed("key-b", "b");
        fixture.seed("key-c", "c");
        var held = takeDispatches(3);
        var recordA = held.get(fixture.topic + "/0/0");
        var recordB = held.get(fixture.topic + "/0/1");
        var recordC = held.get(fixture.topic + "/0/2");

        fixture.processor.onConnectionLost();

        var supersededB = Token.newBuilder()
                .setRecordId(recordB.getToken().getRecordId())
                .setEpoch(recordB.getToken().getEpoch() - 1)
                .build();
        var outcome = fixture.processor.reconcileManifest(List.of(recordA.getToken(), supersededB));

        assertThat(outcome.kept()).isEqualTo(1);
        assertThat(outcome.drops()).containsExactly(supersededB);
        assertThat(outcome.returned()).isEqualTo(1);
        assertThat(outcome.unissued()).isEmpty();

        var redelivery = fixture.takeDispatch();
        assertWithMessage("only the unmanifested record may come back")
                .that(redelivery.getToken().getRecordId()).isEqualTo(recordC.getToken().getRecordId());
        assertThat(redelivery.getAttempt()).isEqualTo(1);

        // A and B were never returned: their original tokens are still the live deliveries
        assertThat(fixture.reportSuccess(recordA.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        assertThat(fixture.reportSuccess(recordB.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        assertThat(fixture.reportSuccess(redelivery.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);

        fixture.awaitCommittedOffset(3);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /** A manifest token the proxy never issued is rejected, and the record it does hold is untouched. */
    @Test
    void aManifestTokenTheProxyNeverIssuedIsRejectedWithoutDisturbingAnythingHeld() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("lone-key", "hello");
        var dispatch = fixture.takeDispatch();

        fixture.processor.onConnectionLost();
        var fabricated = Token.newBuilder().setRecordId("never/9/9").setEpoch(4).build();
        var outcome = fixture.processor.reconcileManifest(List.of(dispatch.getToken(), fabricated));

        assertThat(outcome.unissued()).containsExactly(fabricated);
        assertThat(outcome.kept()).isEqualTo(1);
        assertThat(outcome.returned()).isEqualTo(0);
        assertWithMessage("a rejected token must not have disturbed the record actually held")
                .that(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(1);

        fixture.reportSuccess(dispatch.getToken());
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /**
     * A report from the connection that dropped, for a record the window's expiry already returned and the
     * engine has redelivered, names an ended delivery: discarded, live delivery untouched (KTD8).
     */
    @Test
    void aReportFromTheOldConnectionForAnAlreadyRedeliveredRecordIsDiscarded() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("lone-key", "hello");
        var original = fixture.takeDispatch();

        fixture.processor.onConnectionLost();
        fixture.clock.advance(WELL_PAST_EVERY_DEADLINE);
        var redelivery = fixture.takeDispatch();
        assertThat(redelivery.getToken().getEpoch()).isEqualTo(2);

        assertThat(fixture.reportSuccess(original.getToken())).isEqualTo(ReportResult.SUPERSEDED_EPOCH);
        assertThat(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(1);

        fixture.reportSuccess(redelivery.getToken());
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /**
     * AE8 across a disconnect: with key ordering, the shard's second record is not dispatched while the first
     * is held through a connection loss and a reconnect that keeps it - so no two records of one key are ever
     * out at once, and the held record is never handed to a second worker either.
     */
    @Test
    void oneKeyIsNeverAtTwoWorkersAcrossADisconnectAndReconnect() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("shared-key", "first");
        fixture.seed("shared-key", "second");

        var first = fixture.takeDispatch();
        assertThat(first.getRecord().getOffset()).isEqualTo(0);

        fixture.processor.onConnectionLost();
        fixture.clock.advance(LivenessSettings.DEFAULT_RECONNECT_WINDOW.minusSeconds(1));
        var outcome = fixture.processor.reconcileManifest(List.of(first.getToken()));
        assertThat(outcome.kept()).isEqualTo(1);

        assertWithMessage("the shard is still occupied by the kept delivery: nothing else may go out")
                .that(fixture.pollDispatch(NEGATIVE_CONTROL_BUDGET)).isNull();
        assertWithMessage("the kept record must not have been dispatched a second time")
                .that(fixture.sink.dispatchCount()).isEqualTo(1);

        fixture.reportSuccess(first.getToken());
        var second = fixture.takeDispatch();
        assertThat(second.getRecord().getOffset()).isEqualTo(1);
        fixture.reportSuccess(second.getToken());

        fixture.awaitCommittedOffset(2);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /**
     * The claim race, forced rather than approximated (the plan's latch requirement). A report peeks its
     * entry, is stopped inside the registry, and while it waits the record is returned by a worker-death
     * notice and redelivered at a new epoch. The report's claim then loses - it names an entry that is no
     * longer the registered one - and the live delivery survives untouched.
     * <p>
     * Negative control: replace the conditional remove in {@code InFlightRegistry#claim} with an unconditional
     * one and this test fails - the claim destroys the epoch-2 entry, the record is never completed, and the
     * committed-offset await times out.
     */
    @Test
    void aClaimThatLosesToARedeliveryLeavesTheNewDeliveryIntact() throws Exception {
        var latch = new LatchHook();
        fixture.startWith(options -> options.ordering(ProcessingOrder.KEY),
                ProxyProcessor.DEFAULT_COALESCING_WINDOW,
                LivenessSettings.defaults(true, fixture.clock), latch);
        fixture.seed("lone-key", "hello");
        var original = fixture.takeDispatch();

        latch.blockClaimsFrom(LatchHook.REPORTER_THREAD);
        var reportResult = new java.util.concurrent.atomic.AtomicReference<ReportResult>();
        var reporter = new Thread(() -> reportResult.set(fixture.reportSuccess(original.getToken())),
                LatchHook.REPORTER_THREAD);
        reporter.start();
        latch.awaitBlocked();

        // while that report is stopped mid-claim, the record is returned and redelivered
        assertThat(fixture.processor.onWorkerDied(List.of(original.getToken()))).isEqualTo(1);
        var redelivery = fixture.takeDispatch();
        assertThat(redelivery.getToken().getEpoch()).isEqualTo(2);

        latch.release();
        reporter.join(TimeUnit.SECONDS.toMillis(30));

        assertWithMessage("a claim naming an entry that is no longer registered must win nothing")
                .that(reportResult.get()).isEqualTo(ReportResult.UNKNOWN_TOKEN);
        assertThat(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(1);

        assertThat(fixture.reportSuccess(redelivery.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /**
     * The engine-wave review's first finding, forced with the same latch: a dispatch already in progress
     * registers <em>after</em> the revocation sweep has run, stranding an entry the sweep never saw. The
     * record's redelivery then collides with it - and must replace it and carry on, not throw into core's
     * user-function catch block, where the record would error-retry forever and block its shard.
     * <p>
     * The log assertion is what makes this test about the replacement rather than about the sweep: it fails
     * if the collision never happened, so it cannot pass for the wrong reason.
     */
    @Test
    void aDispatchRegisteringAfterARevocationIsReplacedByItsRedelivery() throws Exception {
        var root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        var previousLevel = root.getLevel();
        var capture = new ListAppender<ILoggingEvent>();
        // the engine's own threads write this capture while the assertion below reads it - see
        // ConfigureHandlerTest#credentialsAppearInNoLogLineAtAnyLevel for the race a bare ArrayList allows
        capture.list = Collections.synchronizedList(new ArrayList<>());
        capture.start();
        root.addAppender(capture);
        root.setLevel(Level.WARN);
        try {
            var latch = new LatchHook();
            fixture.startWith(options -> options.ordering(ProcessingOrder.KEY),
                    ProxyProcessor.DEFAULT_COALESCING_WINDOW,
                    LivenessSettings.defaults(true, fixture.clock), latch);

            latch.blockRegistrations();
            fixture.seed("lone-key", "hello");
            latch.awaitBlocked();

            // the partition goes away while that dispatch is mid-registration: the sweep runs against a
            // registry the entry has not landed in yet
            fixture.processor.onPartitionsRevoked(List.of(fixture.topicPartition));
            latch.release();
            var stranded = fixture.takeDispatch();

            // and comes back, so the record is polled again into a fresh container - the seek is the real
            // rebalance's own move, resuming a reassigned partition from its committed offset
            fixture.processor.onPartitionsAssigned(List.of(fixture.topicPartition));
            fixture.mockConsumer.seek(fixture.topicPartition, 0);
            fixture.seedAt(0, "lone-key", "hello");

            var redelivery = fixture.takeDispatch();
            assertThat(redelivery.getToken().getRecordId()).isEqualTo(stranded.getToken().getRecordId());
            boolean sawTheReplacement;
            synchronized (capture.list) {
                sawTheReplacement = capture.list.stream().map(ILoggingEvent::getFormattedMessage)
                        .anyMatch(line -> line.contains("Replacing a stranded registration"));
            }
            assertWithMessage("the stranded entry must have been REPLACED - that is what this test is about")
                    .that(sawTheReplacement).isTrue();

            assertThat(fixture.reportSuccess(redelivery.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
            fixture.awaitCommittedOffset(1);
            fixture.awaitNoRecordsOutForProcessing();
        } finally {
            root.detachAppender(capture);
            root.setLevel(previousLevel);
        }
    }

    /**
     * The mixed run: successes, a lease expiry, a worker death, a disconnect and a reconnect in one scenario,
     * ending where every other one ends - the counter back at zero.
     */
    @Test
    void aMixedLivenessRunReturnsOutForProcessingToBaseline() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("key-a", "a");
        fixture.seed("key-b", "b");
        fixture.seed("key-c", "c");
        var held = takeDispatches(3);
        var tokens = new ArrayList<>(held.values());

        fixture.reportSuccess(tokens.get(0).getToken());
        fixture.processor.onWorkerDied(List.of(tokens.get(1).getToken()));
        var afterDeath = fixture.takeDispatch();

        fixture.processor.onConnectionLost();
        fixture.processor.reconcileManifest(List.of(afterDeath.getToken()));

        // the reconnect kept one record and returned the other; the returned one comes back
        var afterReconnect = fixture.takeDispatch();
        fixture.reportSuccess(afterDeath.getToken());
        fixture.reportSuccess(afterReconnect.getToken());

        fixture.awaitCommittedOffset(3);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /** Takes {@code count} dispatches, keyed by record id - the arrival-sync every scenario opens with. */
    private Map<String, DispatchRecord> takeDispatches(int count) {
        Map<String, DispatchRecord> byRecordId = new HashMap<>();
        for (int i = 0; i < count; i++) {
            var dispatch = fixture.takeDispatch();
            byRecordId.put(dispatch.getToken().getRecordId(), dispatch);
        }
        assertWithMessage("expected %s distinct records", count).that(byRecordId).hasSize(count);
        return byRecordId;
    }

    /**
     * The registry latch: stops one named thread inside {@code register} or {@code claim} until the test
     * releases it, so an interleaving is <b>forced</b> rather than hoped for. Only the thread the scenario
     * names is stopped, because the very act of setting the overlap up - returning a record, redelivering it -
     * goes through the same two methods on other threads.
     */
    private static final class LatchHook implements InFlightRegistry.Hook {

        static final String REPORTER_THREAD = "latched-reporter";

        private final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private volatile String claimThreadName;
        private volatile boolean blockRegistrations;

        void blockClaimsFrom(String threadName) {
            this.claimThreadName = threadName;
        }

        /** Stops the first registration, whichever thread makes it - the dispatcher's, in practice. */
        void blockRegistrations() {
            this.blockRegistrations = true;
        }

        void awaitBlocked() throws InterruptedException {
            assertWithMessage("no thread reached the latch within the budget")
                    .that(blocked.await(30, TimeUnit.SECONDS)).isTrue();
        }

        void release() {
            release.countDown();
        }

        @Override
        public void beforeRegister(String recordId) {
            if (blockRegistrations) {
                blockRegistrations = false; // one-shot: the redelivery must run straight through
                await();
            }
        }

        @Override
        public void beforeClaim(String recordId) {
            if (Thread.currentThread().getName().equals(claimThreadName)) {
                claimThreadName = null; // one-shot, for the same reason
                await();
            }
        }

        private void await() {
            blocked.countDown();
            try {
                assertWithMessage("the latch was never released").that(release.await(30, TimeUnit.SECONDS))
                        .isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted at the registry latch", e);
            }
        }
    }
}
