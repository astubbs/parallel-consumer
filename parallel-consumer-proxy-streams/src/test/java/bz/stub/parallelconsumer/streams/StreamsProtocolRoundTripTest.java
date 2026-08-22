package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.BuilderCall;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Count;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Invocation;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.InvocationResult;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.MapValues;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Open;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Sink;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Source;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsClientMessage;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsServerMessage;
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
