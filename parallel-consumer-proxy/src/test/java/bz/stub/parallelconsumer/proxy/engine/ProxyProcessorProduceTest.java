package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.proxy.engine.ProxyProcessor.ReportResult;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * R6's produce payload: the half of the engine's success path that talks to a broker, and the half that was
 * driven by nothing until this class existed - every {@code MockProducer} in the tree auto-completed its acks,
 * so the ordering the at-least-once claim rests on, its failure branch, and the cost of waiting for an ack
 * were all unexercised.
 * <p>
 * The fixture's producer therefore hands the acks to the scenario ({@code autoCompleteProduceAcks = false}),
 * which is what makes three otherwise-invisible properties assertable:
 * <ul>
 *     <li><b>Ordering:</b> the record does not complete - and its offset cannot commit - until the produce is
 *     acked, so an input offset never becomes committable ahead of its output existing.</li>
 *     <li><b>The failure branch:</b> a produce that fails is applied as a record failure, so the record is
 *     redelivered rather than silently losing the worker's output.</li>
 *     <li><b>The lane:</b> {@link ProxyProcessor#report} is called from gRPC's single serialized inbound
 *     callback, which also carries {@code Heartbeat} - so it must return while the ack is still outstanding,
 *     and other records must keep completing behind it.</li>
 * </ul>
 *
 * @author Antony Stubbs
 */
class ProxyProcessorProduceTest {

    /** The scenario owns every ack here; nothing completes on its own. */
    private final EngineFixture fixture = new EngineFixture("proxy-produce-test", false);

    @AfterEach
    void closeFixture() {
        fixture.close();
    }

    @Test
    void theProduceIsAckedBeforeTheRecordCompletes() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("key", "hello");
        var dispatch = fixture.takeDispatch();

        assertThat(fixture.reportSuccessProducing(dispatch.getToken(), "worker-output"))
                .isEqualTo(ReportResult.ACCEPTED_PRODUCING);

        // the send has left the engine, but its ack has not come back
        Awaitility.await().atMost(EngineFixture.CONVERGENCE_BUDGET).untilAsserted(() ->
                assertThat(fixture.mockProducer.history()).hasSize(1));
        assertThat(fixture.mockProducer.history().get(0).topic()).isEqualTo(fixture.topic + "-output");
        assertWithMessage("the input record completed while its output was still unacknowledged - the "
                + "at-least-once ordering R6 states is exactly this: produce, ack, THEN the success hook")
                .that(fixture.processor.getNumberRecordsOutForProcessing()).isEqualTo(1);
        assertWithMessage("an offset was committed before the produce was acknowledged")
                .that(fixture.lastCommitted()).isEmpty();

        fixture.mockProducer.completeNext();

        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void aFailedProduceIsAppliedAsAFailureAndRedelivered() {
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("key", "hello");
        var first = fixture.takeDispatch();

        assertThat(fixture.reportSuccessProducing(first.getToken(), "worker-output"))
                .isEqualTo(ReportResult.ACCEPTED_PRODUCING);
        Awaitility.await().atMost(EngineFixture.CONVERGENCE_BUDGET).untilAsserted(() ->
                assertWithMessage("nothing was sent, so there is no ack to fail")
                        .that(fixture.mockProducer.history()).hasSize(1));

        assertThat(fixture.mockProducer.errorNext(new IllegalStateException("the broker refused the output")))
                .isTrue();

        // the worker succeeded but its output did not, so the record must come back rather than commit
        var redelivery = fixture.takeDispatch();
        assertThat(redelivery.getRecord().getOffset()).isEqualTo(first.getRecord().getOffset());
        assertWithMessage("a failed produce must consume an attempt - it is a record failure, not a "
                + "verdict-free return like a lease expiry")
                .that(redelivery.getAttempt()).isEqualTo(2);
        assertThat(redelivery.getToken().getEpoch()).isEqualTo(2);
        assertThat(redelivery.getLastFailureReason()).contains("rejected by the broker");
        assertWithMessage("the input offset was committed even though its output never landed")
                .that(fixture.lastCommitted()).isEmpty();

        // and the redelivery completes normally, so the leak check is a real result rather than a vacuous one
        assertThat(fixture.reportSuccessProducing(redelivery.getToken(), "worker-output"))
                .isEqualTo(ReportResult.ACCEPTED_PRODUCING);
        Awaitility.await().atMost(EngineFixture.CONVERGENCE_BUDGET).untilAsserted(() ->
                assertThat(fixture.mockProducer.history()).hasSize(2));
        fixture.mockProducer.completeNext();

        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void anOutstandingAckDoesNotHoldTheReportLane() {
        // The P1 this class is the regression harness for: report() runs on the session's SINGLE serialized
        // inbound gRPC callback - the lane Heartbeat arrives on - so an ack awaited there collapses the
        // client's whole configured concurrency to serial and can starve the heartbeats that hold every
        // in-flight record's lease. Two records, two shards: the second must complete while the first's ack
        // is still outstanding. Against the blocking version this test does not merely fail, it cannot even
        // reach its second report until the send timeout expires.
        fixture.start(ProcessingOrder.KEY);
        fixture.seed("first-key", "produces");
        fixture.seed("second-key", "produces-nothing");

        var producing = fixture.takeDispatch();
        var other = fixture.takeDispatch();
        if (producing.getRecord().getOffset() != 0) {
            var swap = producing;
            producing = other;
            other = swap;
        }

        assertThat(fixture.reportSuccessProducing(producing.getToken(), "worker-output"))
                .isEqualTo(ReportResult.ACCEPTED_PRODUCING);

        // the ack is still outstanding, and the lane is free: the second record's verdict applies now
        assertThat(fixture.reportSuccess(other.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        fixture.awaitRecordsOutForProcessing(1);
        assertWithMessage("the produce was never sent, so the lane never ran")
                .that(fixture.mockProducer.history()).hasSize(1);

        fixture.mockProducer.completeNext();
        fixture.awaitCommittedOffset(2);
        fixture.awaitNoRecordsOutForProcessing();
    }

    @Test
    void oneSendTimeoutBoundsTheWholePayloadRatherThanEachRecordInIt() {
        // A payload of four records whose acks arrive one every ACK_INTERVAL. The shared deadline is
        // deliberately shorter than the four together and longer than any one of them, so the two behaviours
        // give DIFFERENT VERDICTS rather than different durations: with one deadline for the payload the wait
        // runs out and the record is redelivered; with a fresh timeout per future every ack arrives inside its
        // own bound and the record commits. Slowness only makes the expected outcome more certain, which is
        // what keeps this off the flaky-timing list - the sleeps are lower bounds, so a loaded box overshoots
        // the shared deadline by more, never less.
        var ackInterval = Duration.ofMillis(200);
        var sendTimeout = Duration.ofMillis(500);
        fixture.startWith(options -> options.ordering(ProcessingOrder.KEY).sendTimeout(sendTimeout),
                ProxyProcessor.DEFAULT_COALESCING_WINDOW);
        fixture.seed("key", "hello");
        var first = fixture.takeDispatch();

        var acks = new Thread(() -> {
            for (int i = 0; i < 4; i++) {
                try {
                    Thread.sleep(ackInterval.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                fixture.mockProducer.completeNext();
            }
        }, "produce-ack-drip");
        acks.setDaemon(true);
        acks.start();

        assertThat(fixture.reportSuccessProducing(first.getToken(), "one", "two", "three", "four"))
                .isEqualTo(ReportResult.ACCEPTED_PRODUCING);

        var redelivery = fixture.takeDispatch();
        assertWithMessage("the payload's four acks took longer than the session's %s send timeout, so the "
                        + "report must have been applied as a failure - a per-record timeout would have let "
                        + "the payload run for four times what the client configured", sendTimeout)
                .that(redelivery.getAttempt()).isEqualTo(2);
        assertThat(redelivery.getLastFailureReason()).contains("shared");

        // drain the redelivery normally so the standing leak check means something
        assertThat(fixture.reportSuccess(redelivery.getToken())).isEqualTo(ReportResult.APPLIED_SUCCESS);
        fixture.awaitCommittedOffset(1);
        fixture.awaitNoRecordsOutForProcessing();
    }
}
