package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.proxy.engine.ProxyProcessor.ReportResult;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The engine's product behaviours, driven through the shared {@link EngineFixture} with the test standing in
 * for the transport on both halves of the boundary: it receives waves as the {@link DispatchSink} and answers
 * them through {@link ProxyProcessor#report} - which also makes every hand-off a deterministic rendezvous, so
 * no scenario approximates concurrency with sleeps. The liveness half of the engine's surface - leases,
 * reconnect reconciliation, worker death - is {@link ProxyProcessorLivenessTest}'s.
 * <p>
 * Every scenario ends with the standing leak check: {@code getNumberRecordsOutForProcessing()} back at zero,
 * because a path out of the in-flight registry that misses the mailbox drifts that counter and stalls the
 * consumer with no exception.
 *
 * @author Antony Stubbs
 */
class ProxyProcessorTest {

    private final EngineFixture fixture = new EngineFixture("proxy-engine-test");

    @AfterEach
    void closeFixture() {
        fixture.close();
    }

    @Test
    void aProcessedRecordAdvancesTheCommittedOffset() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("lone-key", "hello");

        var dispatch = fixture.takeDispatch();

        // first delivery carries the full R5 shape: attempt 1, and absence - not zero values - for the history
        assertThat(dispatch.getToken().getRecordId()).isEqualTo(fixture.topic + "/0/0");
        assertThat(dispatch.getToken().getEpoch()).isEqualTo(1);
        assertThat(dispatch.getAttempt()).isEqualTo(1);
        assertThat(dispatch.hasLastFailureAt()).isFalse();
        assertThat(dispatch.hasLastFailureReason()).isFalse();
        assertThat(dispatch.getRecord().getKey().toStringUtf8()).isEqualTo("lone-key");
        assertThat(dispatch.getRecord().getValue().toStringUtf8()).isEqualTo("hello");

        assertThat(fixture.reportSuccess(dispatch.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);

        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void orderingHoldsWithinAShard() {
        // covers AE1: two records share a key; the second is not dispatched until the first is reported
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("shared-key", "first");
        fixture.seed("shared-key", "second");

        var first = fixture.takeDispatch();
        assertThat(first.getRecord().getOffset()).isEqualTo(0);

        // while the first is unreported its shard is occupied, so nothing more may arrive
        assertWithMessage("second record of the shard dispatched before the first was reported")
                .that(fixture.pollDispatch(Duration.ofMillis(300))).isNull();

        fixture.reportSuccess(first.getToken());

        var second = fixture.takeDispatch();
        assertThat(second.getRecord().getOffset()).isEqualTo(1);
        fixture.reportSuccess(second.getToken());

        fixture.awaitCommittedOffset(2);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void outOfOrderCompletionCommitsCorrectly() {
        // covers AE2, rebased to this fixture's offsets: records 0, 1, 2 in flight; reporting 2 then 0 leaves
        // the committed offset at 1 - the highest sequentially succeeded offset (0) plus one - and records 2's
        // completion in the encoded metadata rather than redelivering it
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("key-a", "offset-0");
        fixture.seed("key-b", "offset-1");
        fixture.seed("key-c", "offset-2");

        Map<Long, Token> tokensByOffset = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            var dispatch = fixture.takeDispatch();
            tokensByOffset.put(dispatch.getRecord().getOffset(), dispatch.getToken());
        }

        fixture.reportSuccess(tokensByOffset.get(2L));
        fixture.reportSuccess(tokensByOffset.get(0L));

        // 1, not 3 (offset 1 is still running) and not 0 (a record already succeeded there)
        fixture.awaitCommittedOffset(1);
        assertWithMessage("offset 2's completion must ride in the commit's encoded metadata")
                .that(fixture.lastCommitted().orElseThrow().metadata()).isNotEmpty();

        fixture.reportSuccess(tokensByOffset.get(1L));
        fixture.awaitCommittedOffset(3);

        assertWithMessage("offset 2 was already succeeded and must not have been redelivered")
                .that(fixture.sink.dispatchCount()).isEqualTo(3);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void redeliveryCarriesAttemptCountAndFailureHistory() {
        // covers AE3: a failure report's reason and time come back with the redelivery, verbatim
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("lone-key", "fails-once");

        var first = fixture.takeDispatch();
        assertThat(fixture.reportFailure(first.getToken(), "worker exploded: reason text"))
                .isEqualTo(ReportResult.APPLIED_FAILURE);

        var redelivery = fixture.takeDispatch();
        assertThat(redelivery.getAttempt()).isEqualTo(2);
        assertThat(redelivery.hasLastFailureAt()).isTrue();
        assertThat(redelivery.getLastFailureReason()).isEqualTo("worker exploded: reason text");
        // the fencing epoch moved with the redelivery, distinct from the attempt count it happens to equal here
        assertThat(redelivery.getToken().getEpoch()).isEqualTo(2);

        fixture.reportSuccess(redelivery.getToken());
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void underRestrictedOrderingNoWaveContainsTwoRecordsOfOneShard() {
        // covers AE22: with key ordering and several records across few keys, every assembled wave draws from
        // distinct shards - here observed on real waves rather than asserted by the assembler's own guard
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("key-a", "a-0");
        fixture.seed("key-a", "a-1");
        fixture.seed("key-b", "b-0");
        fixture.seed("key-b", "b-1");

        // one record per shard may be out: the first of each key, then the second of each once reported
        for (int round = 0; round < 2; round++) {
            fixture.reportSuccess(fixture.takeDispatch().getToken());
            fixture.reportSuccess(fixture.takeDispatch().getToken());
        }
        fixture.awaitCommittedOffset(4);

        for (Dispatch wave : fixture.sink.waves) {
            var keysInWave = wave.getRecordsList().stream()
                    .map(dispatch -> dispatch.getRecord().getKey().toStringUtf8())
                    .collect(Collectors.toList());
            assertWithMessage("wave carries two records of one shard: %s", keysInWave)
                    .that(keysInWave).containsNoDuplicates();
        }
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void underUnorderedSeveralRecordsOfOnePartitionAreInFlightAtOnce() {
        // the configuration KTD10's assertion must NOT reject: under UNORDERED the shard is the partition and
        // many of its records - even of one key - are legitimately out at the client simultaneously
        fixture.start(ProcessingOrder.UNORDERED);
        for (int i = 0; i < 5; i++) {
            fixture.seed("same-key", "value-" + i);
        }

        var tokens = new ArrayDeque<Token>();
        for (int i = 0; i < 5; i++) {
            tokens.add(fixture.takeDispatch().getToken());
        }
        assertWithMessage("all five records of the partition should be out at once")
                .that(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(5);

        while (!tokens.isEmpty()) {
            fixture.reportSuccess(tokens.poll());
        }
        fixture.awaitCommittedOffset(5);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void aSupersededEpochReportIsDiscardedAndTheLiveDeliveryContinues() {
        // KTD8's fencing, forced deterministically: the failure report and the redelivery dispatch are hard
        // rendezvous points on the sink, so the stale report provably arrives while delivery 2 is live
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("lone-key", "fails-once");

        var first = fixture.takeDispatch();
        assertThat(first.getToken().getEpoch()).isEqualTo(1);
        fixture.reportFailure(first.getToken(), "first failure");

        var redelivery = fixture.takeDispatch();
        assertThat(redelivery.getToken().getEpoch()).isEqualTo(2);

        // a late duplicate of delivery 1's token arrives while delivery 2 is out
        var staleToken = Token.newBuilder().setRecordId(first.getToken().getRecordId()).setEpoch(1).build();
        assertThat(fixture.reportSuccess(staleToken)).isEqualTo(ReportResult.SUPERSEDED_EPOCH);

        assertWithMessage("the live delivery must be untouched by the discarded report")
                .that(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(1);

        assertThat(fixture.reportSuccess(redelivery.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        fixture.awaitCommittedOffset(1);

        assertWithMessage("the record must have been delivered exactly twice - once per epoch")
                .that(fixture.sink.dispatchCount()).isEqualTo(2);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void unknownAndMalformedTokensAreRejectedWithoutDisturbingInFlight() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("lone-key", "hello");

        var dispatch = fixture.takeDispatch();

        var unknownToken = Token.newBuilder().setRecordId(fixture.topic + "/0/999").setEpoch(1).build();
        assertThat(fixture.reportSuccess(unknownToken)).isEqualTo(ReportResult.UNKNOWN_TOKEN);

        var noToken = Report.newBuilder().setSuccess(Report.Success.newBuilder()).build();
        assertThat(fixture.processor.report(noToken)).isEqualTo(ReportResult.MALFORMED);

        var emptyRecordId = Report.newBuilder()
                .setToken(Token.newBuilder().setEpoch(1))
                .setSuccess(Report.Success.newBuilder())
                .build();
        assertThat(fixture.processor.report(emptyRecordId)).isEqualTo(ReportResult.MALFORMED);

        var noOutcome = Report.newBuilder().setToken(dispatch.getToken()).build();
        assertThat(fixture.processor.report(noOutcome)).isEqualTo(ReportResult.MALFORMED);

        assertWithMessage("rejected reports must not have disturbed the record actually in flight")
                .that(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(1);

        assertThat(fixture.reportSuccess(dispatch.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /**
     * Terminal and Released are in the frozen schema but this engine does not answer them until U9 and U8
     * land - until then they are discarded peek-only: the live delivery is untouched, so no invented verdict
     * can resolve a record the worker did not actually finish.
     */
    @Test
    void terminalAndReleasedReportsAreDiscardedAsUnsupportedWithoutDisturbingInFlight() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("lone-key", "hello");

        var dispatch = fixture.takeDispatch();

        var terminal = Report.newBuilder()
                .setToken(dispatch.getToken())
                .setTerminal(Report.Terminal.newBuilder().setReason("poison pill"))
                .build();
        assertThat(fixture.processor.report(terminal)).isEqualTo(ReportResult.UNSUPPORTED_OUTCOME);

        var released = Report.newBuilder()
                .setToken(dispatch.getToken())
                .setReleased(Report.Released.getDefaultInstance())
                .build();
        assertThat(fixture.processor.report(released)).isEqualTo(ReportResult.UNSUPPORTED_OUTCOME);

        assertWithMessage("unsupported outcomes must leave the record in flight, not resolve it")
                .that(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(1);

        assertThat(fixture.reportSuccess(dispatch.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void recordsInFlightNeverExceedMaxConcurrencyUnderVaryingReportLatency() {
        int maxConcurrency = 3;
        int records = 10;
        fixture.startWith(options -> options.ordering(ProcessingOrder.KEY).maxConcurrency(maxConcurrency),
                ProxyProcessor.DEFAULT_COALESCING_WINDOW);
        for (int i = 0; i < records; i++) {
            fixture.seed("key-" + i, "value-" + i); // distinct keys: nothing but the ceiling constrains flight
        }

        // vary the report latency structurally: hold reports until the ceiling is reached, then release from
        // alternating ends of the outstanding set, so completions land both in and out of dispatch order
        var outstanding = new ArrayDeque<Token>();
        int taken = 0;
        int step = 0;
        while (taken < records) {
            while (outstanding.size() < maxConcurrency && taken < records) {
                outstanding.add(fixture.takeDispatch().getToken());
                taken++;
            }
            var token = (step++ % 2 == 0) ? outstanding.pollLast() : outstanding.pollFirst();
            fixture.reportSuccess(token);
        }
        while (!outstanding.isEmpty()) {
            fixture.reportSuccess(outstanding.poll());
        }

        assertWithMessage("dispatched-minus-reported must never have exceeded the ceiling KTD6 derives")
                .that(fixture.sink.maxOutstanding.get()).isAtMost(maxConcurrency);
        fixture.awaitCommittedOffset(records);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void aLoneRecordIsNotHeldForTheFullCoalescingWindow() {
        // window far beyond the await budget: only the control-loop-end flush can deliver in time, so this
        // fails - rather than flaking - if that flush path breaks and the timer becomes the emission path
        fixture.startWith(options -> options.ordering(ProcessingOrder.KEY), Duration.ofMinutes(5));
        fixture.seed("lone-key", "hello");

        var dispatch = fixture.takeDispatch();

        fixture.reportSuccess(dispatch.getToken());
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void aMixedRunReturnsOutForProcessingToBaseline() {
        // the standing leak check as its own scenario: successes, a failure with redelivery, a superseded
        // report, an unknown token and a malformed report in one run - and the counter still lands on zero
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("key-1", "succeeds");
        fixture.seed("key-2", "fails-once");
        fixture.seed("key-3", "succeeds");

        Map<String, DispatchRecord> byKey = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            var dispatch = fixture.takeDispatch();
            byKey.put(dispatch.getRecord().getKey().toStringUtf8(), dispatch);
        }

        fixture.reportSuccess(byKey.get("key-1").getToken());
        fixture.reportFailure(byKey.get("key-2").getToken(), "transient failure");
        fixture.reportSuccess(byKey.get("key-3").getToken());

        var redelivery = fixture.takeDispatch();
        assertThat(redelivery.getRecord().getKey().toStringUtf8()).isEqualTo("key-2");

        // noise while the redelivery is live: none of it may touch the counter
        assertThat(fixture.reportSuccess(byKey.get("key-2").getToken())).isEqualTo(ReportResult.SUPERSEDED_EPOCH);
        var unknownToken = Token.newBuilder().setRecordId(fixture.topic + "/0/77").setEpoch(1).build();
        assertThat(fixture.reportSuccess(unknownToken)).isEqualTo(ReportResult.UNKNOWN_TOKEN);
        assertThat(fixture.processor.report(Report.newBuilder().build())).isEqualTo(ReportResult.MALFORMED);

        fixture.reportSuccess(redelivery.getToken());

        fixture.awaitCommittedOffset(3);
        fixture.awaitNoRecordsOutForProcessing();
    }

    /**
     * Close funnels through the wave assembler's teardown whichever overload starts it: the no-arg
     * {@code close()} route (DrainingCloseable's default &rarr; {@code close(DrainingMode)}) must stop the
     * wave-window timer thread just as the {@code (Duration, DrainingMode)} route does - and the teardown must
     * survive a {@code super.close} that throws, which is why it sits in a {@code finally} (F5 of the U7
     * review). Thread identity is tracked relative to a pre-test snapshot, so another engine's timer cannot
     * pollute the assertion.
     */
    @Test
    void closeStopsTheWaveWindowTimerThread() {
        var preexisting = waveWindowThreads();
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("k", "v");
        fixture.reportSuccess(fixture.takeDispatch().getToken());
        var spawned = waveWindowThreads();
        spawned.removeAll(preexisting);
        assertWithMessage("the wave-window timer must be live while the engine runs")
                .that(spawned).isNotEmpty();

        fixture.close(); // deliberately the no-arg close() route

        Awaitility.await().untilAsserted(() ->
                assertWithMessage("the wave-window timer thread must terminate on close")
                        .that(spawned.stream().anyMatch(Thread::isAlive)).isFalse());
    }

    private static Set<Thread> waveWindowThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> "pc-proxy-wave-window".equals(thread.getName()))
                .collect(Collectors.toCollection(HashSet::new));
    }

}
