package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Aggregate;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.BuilderCall;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Count;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.DataType;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Describe;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Get;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.GetResult;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleAssigned;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleKind;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleType;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Invocation;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.InvocationResult;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.MapValues;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Open;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Sink;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Source;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsClientMessage;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsServerMessage;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.TimeWindowSpec;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.ToStream;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.TopologyDescription;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.WindowedBy;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

/**
 * Every message survives a serialise-and-parse round trip with its fields intact.
 *
 * <p>These are the bytes a foreign runtime writes and reads, so a field that silently fails to travel is a defect the
 * host discovers as a missing value rather than as an error.
 */
class StreamsProtocolRoundTripTest {

    @Test
    void theHandshakeCarriesItsApplicationIdAndProperties() throws InvalidProtocolBufferException {
        StreamsClientMessage sent = StreamsClientMessage.newBuilder()
                .setOpen(Open.newBuilder()
                        .setApplicationId("counts")
                        .putKafkaProperties("bootstrap.servers", "localhost:19092"))
                .build();

        StreamsClientMessage received = StreamsClientMessage.parseFrom(sent.toByteArray());

        assertThat(received.getOpen().getApplicationId()).isEqualTo("counts");
        assertThat(received.getOpen().getKafkaPropertiesMap()).containsEntry("bootstrap.servers", "localhost:19092");
    }

    @Test
    void aBuilderCallCarriesItsCallIdAndTheCallItNames() throws InvalidProtocolBufferException {
        StreamsClientMessage sent = StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder()
                        .setCallId(7)
                        .setSource(Source.newBuilder().setTopic("orders")))
                .build();

        BuilderCall received = StreamsClientMessage.parseFrom(sent.toByteArray()).getBuilderCall();

        assertThat(received.getCallId()).isEqualTo(7);
        assertThat(received.getSource().getTopic()).isEqualTo("orders");
    }

    @Test
    void aTransformCallNamesBothItsInputHandleAndItsFunction() throws InvalidProtocolBufferException {
        StreamsClientMessage sent = StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder()
                        .setCallId(2)
                        .setMapValues(MapValues.newBuilder().setHandle(1).setFunctionToken(42)))
                .build();

        MapValues received = StreamsClientMessage.parseFrom(sent.toByteArray()).getBuilderCall().getMapValues();

        assertThat(received.getHandle()).isEqualTo(1);
        assertThat(received.getFunctionToken()).isEqualTo(42);
    }

    @Test
    void theAggregationAndSinkCallsCarryTheirNames() throws InvalidProtocolBufferException {
        StreamsClientMessage count = StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder().setCount(Count.newBuilder().setHandle(3).setStoreName("s")))
                .build();
        StreamsClientMessage sink = StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder().setSink(Sink.newBuilder().setHandle(4).setTopic("out")))
                .build();

        assertThat(StreamsClientMessage.parseFrom(count.toByteArray()).getBuilderCall().getCount().getStoreName())
                .isEqualTo("s");
        assertThat(StreamsClientMessage.parseFrom(sink.toByteArray()).getBuilderCall().getSink().getTopic())
                .isEqualTo("out");
    }

    @Test
    void aMintingAnswerCarriesWhatTheHandleIs() throws InvalidProtocolBufferException {
        StreamsServerMessage sent = StreamsServerMessage.newBuilder()
                .setHandleAssigned(HandleAssigned.newBuilder()
                        .setCallId(4)
                        .setHandle(9)
                        .setType(HandleType.newBuilder()
                                .setKind(HandleKind.HANDLE_KIND_TABLE)
                                .setKeyType(DataType.DATA_TYPE_BYTES)
                                .setValueType(DataType.DATA_TYPE_LONG)))
                .build();

        HandleAssigned received = StreamsServerMessage.parseFrom(sent.toByteArray()).getHandleAssigned();

        assertThat(received.getHandle()).isEqualTo(9);
        assertThat(received.getType().getKind()).isEqualTo(HandleKind.HANDLE_KIND_TABLE);
        assertThat(received.getType().getKeyType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(received.getType().getValueType()).isEqualTo(DataType.DATA_TYPE_LONG);
    }

    /**
     * The delivery contract on the wire: a non-minting call's answer omits handle and type both, so
     * "was a handle minted" has exactly one presence signal and a reader cannot see the two disagree.
     */
    @Test
    void aNonMintingAnswerCarriesNeitherHandleNorType() throws InvalidProtocolBufferException {
        StreamsServerMessage sent = StreamsServerMessage.newBuilder()
                .setHandleAssigned(HandleAssigned.newBuilder().setCallId(5))
                .build();

        HandleAssigned received = StreamsServerMessage.parseFrom(sent.toByteArray()).getHandleAssigned();

        assertThat(received.hasHandle()).isFalse();
        assertThat(received.hasType()).isFalse();
    }

    @Test
    void anInvocationCarriesItsCorrelationTokenAndBytes() throws InvalidProtocolBufferException {
        StreamsServerMessage sent = StreamsServerMessage.newBuilder()
                .setInvocation(Invocation.newBuilder()
                        .setCorrelation(99)
                        .setFunctionToken(42)
                        .setKey(ByteString.copyFromUtf8("k"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build();

        Invocation received = StreamsServerMessage.parseFrom(sent.toByteArray()).getInvocation();

        assertThat(received.getCorrelation()).isEqualTo(99);
        assertThat(received.getFunctionToken()).isEqualTo(42);
        assertThat(received.getKey().toStringUtf8()).isEqualTo("k");
        assertThat(received.getValue().toStringUtf8()).isEqualTo("v");
    }

    @Test
    void aResultCarriesEitherAValueOrAnError() throws InvalidProtocolBufferException {
        StreamsClientMessage ok = StreamsClientMessage.newBuilder()
                .setInvocationResult(InvocationResult.newBuilder()
                        .setCorrelation(99).setValue(ByteString.copyFromUtf8("mapped")))
                .build();
        StreamsClientMessage failed = StreamsClientMessage.newBuilder()
                .setInvocationResult(InvocationResult.newBuilder().setCorrelation(100).setError("boom"))
                .build();

        InvocationResult okResult = StreamsClientMessage.parseFrom(ok.toByteArray()).getInvocationResult();
        InvocationResult failedResult = StreamsClientMessage.parseFrom(failed.toByteArray()).getInvocationResult();

        assertThat(okResult.getValue().toStringUtf8()).isEqualTo("mapped");
        assertThat(okResult.hasError()).isFalse();
        assertThat(failedResult.getError()).isEqualTo("boom");
        assertThat(failedResult.hasValue()).isFalse();
    }

    /**
     * The correlation on the query pair travels, and its absence is distinguishable from zero.
     *
     * <p>Both halves matter. A call id that failed to travel would leave every query unanswerable; a zero that
     * could not be told from an absent field would make "this engine does not correlate" and "this is call 0"
     * the same message, and a client cannot drop one while honouring the other.
     */
    @Test
    void aQueryAndItsAnswerCarryTheirCorrelation() throws InvalidProtocolBufferException {
        StreamsClientMessage asked = StreamsClientMessage.newBuilder()
                .setGet(Get.newBuilder().setStoreName("counted").setKey(ByteString.copyFromUtf8("a")).setCallId(3))
                .build();
        StreamsServerMessage answered = StreamsServerMessage.newBuilder()
                .setGetResult(GetResult.newBuilder().setCallId(3).setFound(true)
                        .setValue(ByteString.copyFromUtf8("v")).setValueType(DataType.DATA_TYPE_BYTES))
                .build();
        StreamsServerMessage uncorrelated = StreamsServerMessage.newBuilder()
                .setGetResult(GetResult.newBuilder().setFound(false))
                .build();

        assertThat(StreamsClientMessage.parseFrom(asked.toByteArray()).getGet().getCallId()).isEqualTo(3);
        assertThat(StreamsServerMessage.parseFrom(answered.toByteArray()).getGetResult().getCallId()).isEqualTo(3);
        assertThat(StreamsServerMessage.parseFrom(uncorrelated.toByteArray()).getGetResult().hasCallId()).isFalse();
    }

    /** Describe carries the same correlation, on the description that answers it. */
    @Test
    void aDescribeAndItsDescriptionCarryTheirCorrelation() throws InvalidProtocolBufferException {
        StreamsClientMessage asked = StreamsClientMessage.newBuilder()
                .setDescribe(Describe.newBuilder().setCallId(8))
                .build();
        StreamsServerMessage answered = StreamsServerMessage.newBuilder()
                .setTopologyDescription(TopologyDescription.newBuilder().setCallId(8).setText("Topologies:"))
                .build();

        assertThat(StreamsClientMessage.parseFrom(asked.toByteArray()).getDescribe().getCallId()).isEqualTo(8);
        TopologyDescription received =
                StreamsServerMessage.parseFrom(answered.toByteArray()).getTopologyDescription();
        assertThat(received.getCallId()).isEqualTo(8);
        assertThat(received.getText()).isEqualTo("Topologies:");
    }

    /**
     * The windowed type survives the wire whole: kind, both data types and all four window fields (R10, R11).
     * These are the bytes the Python client reads its window back from, so a field that failed to travel would
     * surface as a host believing a window it never described.
     */
    @Test
    void aWindowedHandleTypeCarriesItsSpecificationIntact() throws InvalidProtocolBufferException {
        TimeWindowSpec spec = TimeWindowSpec.newBuilder()
                .setSizeMs(3_600_000L).setAdvanceMs(300_000L).setGraceMs(60_000L).setRetentionMs(7_200_000L)
                .build();
        StreamsServerMessage sent = StreamsServerMessage.newBuilder()
                .setHandleAssigned(HandleAssigned.newBuilder()
                        .setCallId(6)
                        .setHandle(11)
                        .setType(HandleType.newBuilder()
                                .setKind(HandleKind.HANDLE_KIND_TIME_WINDOWED_STREAM)
                                .setKeyType(DataType.DATA_TYPE_BYTES)
                                .setValueType(DataType.DATA_TYPE_BYTES)
                                .setWindow(spec)))
                .build();

        HandleType received = StreamsServerMessage.parseFrom(sent.toByteArray()).getHandleAssigned().getType();

        assertThat(received.getKind()).isEqualTo(HandleKind.HANDLE_KIND_TIME_WINDOWED_STREAM);
        assertThat(received.getKeyType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(received.getValueType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(received.hasWindow()).isTrue();
        // All four fields distinct on purpose: a serialisation that transposed two would still pass a
        // tumbling-shaped spec, where size equals advance.
        assertThat(received.getWindow().getSizeMs()).isEqualTo(3_600_000L);
        assertThat(received.getWindow().getAdvanceMs()).isEqualTo(300_000L);
        assertThat(received.getWindow().getGraceMs()).isEqualTo(60_000L);
        assertThat(received.getWindow().getRetentionMs()).isEqualTo(7_200_000L);
    }

    /** A windowless type reads back windowless - presence is the signal, so it must not appear from nowhere. */
    @Test
    void anUnwindowedHandleTypeCarriesNoWindow() throws InvalidProtocolBufferException {
        StreamsServerMessage sent = StreamsServerMessage.newBuilder()
                .setHandleAssigned(HandleAssigned.newBuilder().setHandle(3)
                        .setType(HandleType.newBuilder().setKind(HandleKind.HANDLE_KIND_STREAM)))
                .build();

        assertThat(StreamsServerMessage.parseFrom(sent.toByteArray())
                .getHandleAssigned().getType().hasWindow()).isFalse();
    }

    @Test
    void theWindowedBuilderCallsCarryTheirFields() throws InvalidProtocolBufferException {
        StreamsClientMessage windowedBy = StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder().setWindowedBy(WindowedBy.newBuilder()
                        .setHandle(4)
                        .setWindow(TimeWindowSpec.newBuilder()
                                .setSizeMs(1000).setAdvanceMs(500).setGraceMs(100).setRetentionMs(2000))))
                .build();
        StreamsClientMessage aggregate = StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder().setAggregate(Aggregate.newBuilder()
                        .setHandle(5).setInitial(ByteString.copyFromUtf8("seed"))
                        .setFunctionToken(42).setStoreName("agg")))
                .build();
        StreamsClientMessage toStream = StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder().setToStream(ToStream.newBuilder().setHandle(6)))
                .build();

        WindowedBy parsedWindowedBy =
                StreamsClientMessage.parseFrom(windowedBy.toByteArray()).getBuilderCall().getWindowedBy();
        Aggregate parsedAggregate =
                StreamsClientMessage.parseFrom(aggregate.toByteArray()).getBuilderCall().getAggregate();

        assertThat(parsedWindowedBy.getHandle()).isEqualTo(4);
        assertThat(parsedWindowedBy.getWindow().getAdvanceMs()).isEqualTo(500);
        // The initializer's bytes are the first VALUE a builder call has ever carried; losing them would turn
        // every accumulator into empty bytes with nothing anywhere reporting it.
        assertThat(parsedAggregate.getInitial().toStringUtf8()).isEqualTo("seed");
        assertThat(parsedAggregate.getFunctionToken()).isEqualTo(42);
        assertThat(parsedAggregate.getStoreName()).isEqualTo("agg");
        assertThat(StreamsClientMessage.parseFrom(toStream.toByteArray()).getBuilderCall().getToStream()
                .getHandle()).isEqualTo(6);
    }

    @Test
    void anAbsentOptionalIsDistinguishableFromAnEmptyOne() throws InvalidProtocolBufferException {
        StreamsClientMessage absent = StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder().setSource(Source.newBuilder()))
                .build();
        StreamsClientMessage empty = StreamsClientMessage.newBuilder()
                .setBuilderCall(BuilderCall.newBuilder().setSource(Source.newBuilder().setTopic("")))
                .build();

        assertThat(StreamsClientMessage.parseFrom(absent.toByteArray()).getBuilderCall().getSource().hasTopic())
                .isFalse();
        assertThat(StreamsClientMessage.parseFrom(empty.toByteArray()).getBuilderCall().getSource().hasTopic())
                .isTrue();
    }
}
