package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.BuilderCall;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Open;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Source;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsClientMessage;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsServerMessage;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * The session's state machine, driven without a broker or a gRPC server.
 *
 * <p>Everything up to describe-complete is reachable this way, which is where the protocol rules live: what the
 * first message must be, what a refusal says, and whether a handle comes back for the call that asked.
 */
class StreamsSessionServiceTest {

    /** Collects what the engine sends. gRPC serialises inbound callbacks, so a plain list is faithful here. */
    private static final class Recorder implements StreamObserver<StreamsServerMessage> {
        final List<StreamsServerMessage> sent = new ArrayList<>();
        boolean completed;

        @Override
        public void onNext(StreamsServerMessage message) {
            sent.add(message);
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    private final Recorder recorder = new Recorder();
    // Never invoked in these tests: nothing here sends describe-complete, which is the only thing that starts a
    // topology. A runner that throws makes an accidental start loud rather than silent.
    private final StreamsSessionService service = new StreamsSessionService((topology, open) -> {
        throw new AssertionError("no test here should start a topology");
    });

    @Test
    void theHandshakeIsAnsweredWithReady() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);

        toEngine.onNext(open("counts"));

        assertThat(recorder.sent).hasSize(1);
        assertThat(recorder.sent.get(0).getReady().getApplicationId()).isEqualTo("counts");
    }

    @Test
    void aSessionThatDescribesBeforeOpeningIsRefusedAndToldWhy() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);

        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder().setSource(Source.newBuilder().setTopic("in")))
                .build());

        assertThat(recorder.sent).hasSize(1);
        assertThat(recorder.sent.get(0).getFault().getReason()).contains("Open");
    }

    @Test
    void aBuilderCallIsAnsweredWithAHandleForTheCallThatAsked() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(open("counts"));

        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder()
                        .setCallId(7)
                        .setSource(Source.newBuilder().setTopic("in")))
                .build());

        var assigned = recorder.sent.get(1).getHandleAssigned();
        assertThat(assigned.getCallId()).isEqualTo(7);
        assertThat(assigned.getHandle()).isGreaterThan(0L);
    }

    @Test
    void aBuilderCallNamingNoMethodIsRefusedAndTheRefusalNamesTheCall() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(open("counts"));

        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder().setCallId(11))
                .build());

        assertThat(recorder.sent.get(1).getFault().getReason()).contains("11");
    }

    @Test
    void aCallNamingAnUnknownHandleIsRefusedRatherThanKillingTheStream() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(open("counts"));

        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder()
                        .setCallId(3)
                        .setGroupByKey(bz.stub.parallelconsumer.streams.protocol.v1alpha1.GroupByKey.newBuilder()
                                .setHandle(4242)))
                .build());

        assertThat(recorder.sent.get(1).getFault().getReason()).contains("4242");
        assertThat(recorder.completed).isFalse();
    }

    @Test
    void openingTwiceIsRefused() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(open("counts"));

        toEngine.onNext(open("counts"));

        assertThat(recorder.sent.get(1).getFault().getReason()).ignoringCase().contains("already open");
    }

    private static StreamsClientMessage open(String applicationId) {
        return StreamsClientMessage.newBuilder()
                .setOpen(Open.newBuilder().setApplicationId(applicationId))
                .build();
    }
}
