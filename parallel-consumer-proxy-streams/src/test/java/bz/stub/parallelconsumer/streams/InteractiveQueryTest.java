package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.BuilderCall;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Count;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.DataType;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.DescribeComplete;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Get;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.GetResult;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.GroupByKey;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Open;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Reduce;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Source;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsClientMessage;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsServerMessage;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;

/**
 * The host reading engine state - the dimension of the coupling where information flows the other way.
 *
 * <p>Every other crossing so far is the engine asking the host to compute something. This is the host asking the
 * engine what it holds, and until it existed a host could build a table and never see inside it: the only window
 * onto state was whatever the topology happened to sink.
 */
class InteractiveQueryTest {

    private static final byte[] KEY = "a".getBytes(StandardCharsets.UTF_8);

    /** A stand-in store, so these tests need no broker and no running Kafka Streams. */
    private final Map<String, Object> store = new HashMap<>();
    private final List<StreamsServerMessage> sent = new ArrayList<>();
    private boolean queryThrows;

    private final StreamsSessionService service = new StreamsSessionService((topology, open) ->
            new StreamsSessionService.TopologyRun() {
                @Override
                public void close() {
                }

                @Override
                public Object get(String storeName, byte[] key) {
                    if (queryThrows) {
                        throw new IllegalStateException("the store is still restoring");
                    }
                    return store.get(new String(key, StandardCharsets.UTF_8));
                }
            });

    private final StreamObserver<StreamsServerMessage> recorder = new StreamObserver<>() {
        @Override
        public void onNext(StreamsServerMessage value) {
            sent.add(value);
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    };

    /** Opens a session and starts a reducing topology, whose store holds bytes. */
    private StreamObserver<StreamsClientMessage> runningReduceTopology() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setOpen(Open.newBuilder().setApplicationId("queries")).build());
        toEngine.onNext(call(BuilderCall.newBuilder().setCallId(1).setSource(Source.newBuilder().setTopic("in"))));
        long source = lastHandle();
        toEngine.onNext(call(BuilderCall.newBuilder().setCallId(2)
                .setGroupByKey(GroupByKey.newBuilder().setHandle(source))));
        long grouped = lastHandle();
        toEngine.onNext(call(BuilderCall.newBuilder().setCallId(3).setReduce(
                Reduce.newBuilder().setHandle(grouped).setFunctionToken(7).setStoreName("reduced"))));
        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setDescribeComplete(DescribeComplete.getDefaultInstance()).build());
        return toEngine;
    }

    private StreamsClientMessage call(BuilderCall.Builder call) {
        return StreamsClientMessage.newBuilder().setBuilderCall(call).build();
    }

    private long lastHandle() {
        return sent.get(sent.size() - 1).getHandleAssigned().getHandle();
    }

    private GetResult query(StreamObserver<StreamsClientMessage> toEngine, String storeName, byte[] key) {
        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setGet(Get.newBuilder().setStoreName(storeName).setKey(ByteString.copyFrom(key))).build());
        return sent.get(sent.size() - 1).getGetResult();
    }

    @Test
    void aPresentKeyComesBackWithItsValueAndType() {
        StreamObserver<StreamsClientMessage> toEngine = runningReduceTopology();
        store.put("a", "stored".getBytes(StandardCharsets.UTF_8));

        GetResult result = query(toEngine, "reduced", KEY);

        assertThat(result.getFound()).isTrue();
        assertThat(result.getValue().toStringUtf8()).isEqualTo("stored");
        // A reduce store holds bytes. Saying so is what lets the host decode without guessing, and it is the same
        // recorded type the sink selects its serde by - so a queried value and a sunk value cannot disagree.
        assertThat(result.getValueType()).isEqualTo(DataType.DATA_TYPE_BYTES);
    }

    @Test
    void anAbsentKeyIsNotFoundRatherThanEmpty() {
        StreamObserver<StreamsClientMessage> toEngine = runningReduceTopology();

        GetResult result = query(toEngine, "reduced", KEY);

        // The distinction the `found` flag exists for. Without it "no such key" and "a key holding empty bytes"
        // are the same answer, and a host cannot tell them apart.
        assertThat(result.getFound()).isFalse();
        assertThat(result.getValue().isEmpty()).isTrue();
        assertThat(result.getError()).isEmpty();
    }

    @Test
    void anEmptyStoredValueIsFoundAndEmpty() {
        StreamObserver<StreamsClientMessage> toEngine = runningReduceTopology();
        store.put("a", new byte[0]);

        GetResult result = query(toEngine, "reduced", KEY);

        assertThat(result.getFound()).isTrue();
        assertThat(result.getValue().isEmpty()).isTrue();
    }

    @Test
    void aQueryForAnUnknownStoreIsAnsweredNotFaulted() {
        StreamObserver<StreamsClientMessage> toEngine = runningReduceTopology();

        GetResult result = query(toEngine, "no-such-store", KEY);

        // Answered, not faulted. Tearing the stream down over one bad lookup would take the whole running
        // topology with it, which is a wildly disproportionate response to a typo in a store name.
        assertThat(result.getError()).contains("no store named no-such-store");
        assertThat(sent.stream().anyMatch(StreamsServerMessage::hasFault)).isFalse();
    }

    @Test
    void aStoreThatCannotBeReadIsAnErrorRatherThanAnAbsentKey() {
        StreamObserver<StreamsClientMessage> toEngine = runningReduceTopology();
        queryThrows = true;

        GetResult result = query(toEngine, "reduced", KEY);

        // A restoring store and a missing key mean very different things to a host deciding what to do next.
        // Collapsing the first into the second would have it conclude the data is gone.
        assertThat(result.getError()).contains("restoring");
        assertThat(result.getFound()).isFalse();
    }

    @Test
    void aQueryBeforeTheTopologyStartsIsAnsweredNotFaulted() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setOpen(Open.newBuilder().setApplicationId("queries")).build());

        GetResult result = query(toEngine, "reduced", KEY);

        assertThat(result.getError()).contains("not running");
        assertThat(sent.stream().anyMatch(StreamsServerMessage::hasFault)).isFalse();
    }

    @Test
    void aCountStoreReportsLongsAndSerialisesThemAsTheSinkWould() {
        StreamObserver<StreamsClientMessage> toEngine = service.session(recorder);
        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setOpen(Open.newBuilder().setApplicationId("queries")).build());
        toEngine.onNext(call(BuilderCall.newBuilder().setCallId(1).setSource(Source.newBuilder().setTopic("in"))));
        long source = lastHandle();
        toEngine.onNext(call(BuilderCall.newBuilder().setCallId(2)
                .setGroupByKey(GroupByKey.newBuilder().setHandle(source))));
        toEngine.onNext(call(BuilderCall.newBuilder().setCallId(3)
                .setCount(Count.newBuilder().setHandle(lastHandle()).setStoreName("counted"))));
        toEngine.onNext(StreamsClientMessage.newBuilder()
                .setDescribeComplete(DescribeComplete.getDefaultInstance()).build());
        store.put("a", 5L);

        GetResult result = query(toEngine, "counted", KEY);

        assertThat(result.getValueType()).isEqualTo(DataType.DATA_TYPE_LONG);
        // Eight big-endian bytes, exactly what the sink writes - so a host decodes a query and a sunk record the
        // same way rather than needing two rules.
        assertThat(result.getValue().size()).isEqualTo(8);
        assertThat(result.getValue().byteAt(7)).isEqualTo((byte) 5);
    }
}
