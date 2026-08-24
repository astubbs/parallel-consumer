package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.BuilderCall;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Count;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.DataType;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Describe;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.GroupByKey;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleAssigned;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleKind;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Open;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Sink;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Source;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsClientMessage;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsServerMessage;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.TopologyDescription;
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

    /**
     * A minting answer says what the handle IS. The count is the case that matters: its table of longs is a value
     * the host never supplied, and this field is the only way the host can know what it will read off the sink.
     */
    @Test
    void aMintingAnswerCarriesTheHandlesRecordedType() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(open("counts"));

        toEngine.onNext(builderCall(1, b -> b.setSource(Source.newBuilder().setTopic("in"))));
        long source = recorder.sent.get(1).getHandleAssigned().getHandle();
        toEngine.onNext(builderCall(2, b -> b.setGroupByKey(GroupByKey.newBuilder().setHandle(source))));
        long grouped = recorder.sent.get(2).getHandleAssigned().getHandle();
        toEngine.onNext(builderCall(3, b -> b.setCount(Count.newBuilder().setHandle(grouped).setStoreName("s"))));

        HandleAssigned sourceAnswer = recorder.sent.get(1).getHandleAssigned();
        assertThat(sourceAnswer.hasType()).isTrue();
        assertThat(sourceAnswer.getType().getKind()).isEqualTo(HandleKind.HANDLE_KIND_STREAM);
        HandleAssigned countAnswer = recorder.sent.get(3).getHandleAssigned();
        assertThat(countAnswer.getCallId()).isEqualTo(3);
        assertThat(countAnswer.getType().getKind()).isEqualTo(HandleKind.HANDLE_KIND_TABLE);
        assertThat(countAnswer.getType().getKeyType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(countAnswer.getType().getValueType()).isEqualTo(DataType.DATA_TYPE_LONG);
    }

    /**
     * A describe is correlated exactly as a builder call is, and for the same reason.
     *
     * <p>Without it a host holds one description slot for the whole session, so two threads asking what the
     * topology looks like are both handed whichever answer landed last - the query defect, in the shape describe
     * takes. It is answered on the description itself rather than on an envelope, as HandleAssigned carries both
     * its call id and its handle.
     */
    @Test
    void aDescribeIsAnsweredWithTheCallIdThatAskedIt() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(open("counts"));
        toEngine.onNext(builderCall(1, b -> b.setSource(Source.newBuilder().setTopic("in"))));

        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setDescribe(Describe.newBuilder().setCallId(9)).build());

        TopologyDescription described = recorder.sent.get(2).getTopologyDescription();
        assertThat(described.getCallId()).isEqualTo(9);
        assertThat(described.getText()).contains("Sub-topology");
    }

    /** An uncorrelated describe is answered without a correlation, so absence never means "lost". */
    @Test
    void anUncorrelatedDescribeIsAnsweredWithoutACorrelation() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(open("counts"));
        toEngine.onNext(builderCall(1, b -> b.setSource(Source.newBuilder().setTopic("in"))));

        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setDescribe(Describe.getDefaultInstance()).build());

        assertThat(recorder.sent.get(2).getTopologyDescription().hasCallId()).isFalse();
    }

    /** A sink mints nothing, and its answer says so with ONE signal: neither handle nor type is present. */
    @Test
    void aSinkAnswerCarriesNeitherHandleNorType() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(open("counts"));

        toEngine.onNext(builderCall(1, b -> b.setSource(Source.newBuilder().setTopic("in"))));
        long source = recorder.sent.get(1).getHandleAssigned().getHandle();
        toEngine.onNext(builderCall(2, b -> b.setSink(Sink.newBuilder().setHandle(source).setTopic("out"))));

        HandleAssigned sinkAnswer = recorder.sent.get(2).getHandleAssigned();
        assertThat(sinkAnswer.getCallId()).isEqualTo(2);
        assertThat(sinkAnswer.hasHandle()).isFalse();
        assertThat(sinkAnswer.hasType()).isFalse();
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

    private static StreamsClientMessage builderCall(
            long callId, java.util.function.UnaryOperator<BuilderCall.Builder> call) {
        return StreamsClientMessage.newBuilder()
                .setBuilderCall(call.apply(BuilderCall.newBuilder().setCallId(callId)))
                .build();
    }
}
