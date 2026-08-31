package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * R41's single-connection admission: the slot's state machine unit-tested directly (including the
 * generation-counter scenario - a late release from a superseded stream must not free the live holder's
 * slot), and the wire-level behaviour through a real server (a second concurrent stream is rejected while the
 * first is live, with the service-invocation counter proving the service method never ran for it; the slot
 * releases on stream termination, so the next stream is admitted - the seam U8's reconnection re-uses).
 */
@Timeout(value = 30)
class SingleConnectionGuardTest extends WireTestBase {

    // --- the state machine, directly ---

    @Test
    void firstAcquireWinsAndTheSlotRejectsASecondWhileHeld() {
        var guard = new SingleConnectionGuard();

        OptionalLong first = guard.tryAcquire();
        assertThat(first.isPresent()).isTrue();
        assertThat(guard.isHeld()).isTrue();

        assertWithMessage("a second acquire while the slot is held must be rejected")
                .that(guard.tryAcquire().isPresent()).isFalse();
    }

    @Test
    void releaseFreesTheSlotForReacquisitionWithANewGeneration() {
        var guard = new SingleConnectionGuard();

        long first = guard.tryAcquire().orElseThrow();
        assertThat(guard.release(first)).isTrue();
        assertThat(guard.isHeld()).isFalse();

        long second = guard.tryAcquire().orElseThrow();
        assertThat(second).isGreaterThan(first);
        assertThat(guard.isHeld()).isTrue();
    }

    /**
     * The generation-counter scenario: a late termination callback from the stream of generation N arrives
     * after generation N+1 has claimed the slot. Releasing on the stale ticket must be a no-op, or the stale
     * disconnect would open the door to a second concurrent stream under the live holder.
     */
    @Test
    void aLateReleaseFromASupersededGenerationCannotFreeTheLiveHoldersSlot() {
        var guard = new SingleConnectionGuard();

        long generationN = guard.tryAcquire().orElseThrow();
        assertThat(guard.release(generationN)).isTrue();
        long generationN1 = guard.tryAcquire().orElseThrow();

        // The late callback from generation N fires now, while N+1 holds the slot.
        assertWithMessage("a stale ticket must not release the slot")
                .that(guard.release(generationN)).isFalse();
        assertWithMessage("the slot must still be held by generation N+1")
                .that(guard.isHeld()).isTrue();
        assertWithMessage("no new stream may be admitted while N+1 holds the slot")
                .that(guard.tryAcquire().isPresent()).isFalse();

        // The live holder's own release still works.
        assertThat(guard.release(generationN1)).isTrue();
        assertThat(guard.isHeld()).isFalse();
    }

    @Test
    void releasingAnAlreadyReleasedSlotIsANoop() {
        var guard = new SingleConnectionGuard();

        long ticket = guard.tryAcquire().orElseThrow();
        assertThat(guard.release(ticket)).isTrue();
        assertThat(guard.release(ticket)).isFalse();
        assertThat(guard.isHeld()).isFalse();
    }

    // --- the wire, through a real loopback server (fixture: WireTestBase) ---

    @Test
    void aSecondConcurrentStreamIsRejectedWhileTheFirstIsLiveAndAdmittedOnceItTerminates() throws Exception {
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.port())
                .usePlaintext()
                .build();
        try {
            // First stream wins the slot, and is proven live by a Configure/Configured round trip.
            var firstResponses = new RecordingProxyMessageObserver();
            StreamObserver<ClientMessage> first = ProxyServiceGrpc.newStub(channel).session(firstResponses);
            first.onNext(configure("first"));
            await().atMost(10, SECONDS).until(() -> !firstResponses.messages.isEmpty());
            assertThat(service.serviceInvocations.get()).isEqualTo(1);

            // A second concurrent stream is rejected - and the counter proves the service method never
            // ran for it, not merely that the client saw an error.
            var secondResponses = new RecordingProxyMessageObserver();
            StreamObserver<ClientMessage> second = ProxyServiceGrpc.newStub(channel).session(secondResponses);
            try {
                second.onNext(configure("second"));
            } catch (IllegalStateException alreadyClosed) {
                // The rejection can land before the client's send; the counters below are the proof.
            }
            assertWithMessage("second stream should terminate with the rejection")
                    .that(secondResponses.terminated.await(10, SECONDS)).isTrue();
            assertThat(Status.fromThrowable(secondResponses.error.get()).getCode())
                    .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertWithMessage("the service method must not have run for the rejected stream")
                    .that(service.serviceInvocations.get()).isEqualTo(1);

            // The first stream is unharmed by the rejected second: another round trip still works.
            first.onNext(configure("first-again"));
            await().atMost(10, SECONDS).until(() -> firstResponses.messages.size() >= 2);

            // Termination releases the slot, so the next stream is admitted - the mechanism U8's
            // reconnection re-uses.
            first.onCompleted();
            assertWithMessage("first stream should complete cleanly")
                    .that(firstResponses.terminated.await(10, SECONDS)).isTrue();
            var thirdResponses = new RecordingProxyMessageObserver();
            await().atMost(10, SECONDS).untilAsserted(() -> {
                var reconnectResponses = new RecordingProxyMessageObserver();
                StreamObserver<ClientMessage> reconnect =
                        ProxyServiceGrpc.newStub(channel).session(reconnectResponses);
                reconnect.onNext(configure("reconnect"));
                await().atMost(2, SECONDS).until(() -> !reconnectResponses.messages.isEmpty());
                reconnect.onCompleted();
                thirdResponses.messages.addAll(reconnectResponses.messages);
            });
            assertThat(thirdResponses.messages).isNotEmpty();
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
