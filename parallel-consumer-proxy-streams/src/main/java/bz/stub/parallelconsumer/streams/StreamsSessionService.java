package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.BuilderCall;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Fault;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleAssigned;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Invocation;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.InvocationResult;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Open;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Ready;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsClientMessage;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsServerMessage;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.StreamsServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.BiFunction;

/**
 * One session: the handshake, the topology description, then invocations for as long as it runs.
 *
 * <p>gRPC serialises a stream's inbound callbacks, so the state machine below needs no locking. Outbound sends do:
 * several stream threads emit invocations concurrently while the transport thread may be answering a builder call.
 */
public class StreamsSessionService extends StreamsServiceGrpc.StreamsServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(StreamsSessionService.class);

    /** How long a stream thread waits for the host before failing the record. */
    static final Duration INVOCATION_TIMEOUT = Duration.ofSeconds(30);

    /** Builds the running topology. Injected so the session is testable without a broker. */
    private final BiFunction<org.apache.kafka.streams.Topology, Open, TopologyRun> runner;

    public StreamsSessionService(BiFunction<org.apache.kafka.streams.Topology, Open, TopologyRun> runner) {
        this.runner = runner;
    }

    /** A started topology, so the session can stop it without knowing how it was started. */
    public interface TopologyRun extends AutoCloseable {
        @Override
        void close();
    }

    @Override
    public StreamObserver<StreamsClientMessage> session(StreamObserver<StreamsServerMessage> toClient) {
        return new Session(toClient);
    }

    private final class Session implements StreamObserver<StreamsClientMessage> {

        private final StreamObserver<StreamsServerMessage> toClient;
        private final Object transmitLock = new Object();
        private final InvocationRegistry registry = new InvocationRegistry();
        private final TopologyAssembler assembler;

        private boolean opened;
        private Open open;
        private TopologyRun run;
        private boolean closed;

        private Session(StreamObserver<StreamsServerMessage> toClient) {
            this.toClient = toClient;
            InvocationSink sink = this::emitInvocation;
            this.assembler = new TopologyAssembler(
                    token -> new ForeignValueMapper(registry, sink, token, INVOCATION_TIMEOUT),
                    token -> new ForeignReducer(registry, sink, token, INVOCATION_TIMEOUT));
        }

        @Override
        public void onNext(StreamsClientMessage message) {
            try {
                dispatch(message);
            } catch (TopologyDescriptionException refused) {
                fault(refused.getMessage());
            } catch (RuntimeException unexpected) {
                log.error("session failed", unexpected);
                fault("the engine failed: " + unexpected);
            }
        }

        private void dispatch(StreamsClientMessage message) {
            if (!opened && message.getMessageCase() != StreamsClientMessage.MessageCase.OPEN) {
                throw new TopologyDescriptionException(
                        "the first message on a session must be Open, got " + message.getMessageCase());
            }
            switch (message.getMessageCase()) {
                case OPEN -> onOpen(message.getOpen());
                case BUILDER_CALL -> onBuilderCall(message.getBuilderCall());
                case REGISTER_FUNCTION -> log.debug("host registered function token {}",
                        message.getRegisterFunction().getToken());
                case DESCRIBE_COMPLETE -> onDescribeComplete();
                case DESCRIBE -> onDescribe();
                case INVOCATION_RESULT -> onResult(message.getInvocationResult());
                // Named rather than ignored: a foreign caller that sends something this engine does not implement
                // needs to be told which message was refused, not left waiting.
                case MESSAGE_NOT_SET -> throw new TopologyDescriptionException("an empty message is not a request");
                default -> throw new TopologyDescriptionException(
                        "this engine does not implement " + message.getMessageCase());
            }
        }

        private void onOpen(Open request) {
            if (opened) {
                throw new TopologyDescriptionException("this session is already open");
            }
            opened = true;
            open = request;
            send(StreamsServerMessage.newBuilder()
                    .setReady(Ready.newBuilder().setApplicationId(request.getApplicationId()))
                    .build());
        }

        /**
         * Performs one builder call and answers it. A minting call's answer carries the handle AND what it is -
         * the type recorded at the mint; a non-minting call (sink) is answered with neither field, so the wire has
         * exactly one presence signal for "a handle was minted". gRPC serialises this stream's inbound callbacks,
         * so the mint and the type lookup that follows are call-scoped - no other builder call can interleave.
         *
         * <p>The switch is an EXPRESSION on purpose: every arm must produce the answer it sends, so a future
         * operator cannot compile while forgetting to attach its mint - as a statement mutating a shared builder,
         * that omission would ship a "nothing was minted" answer and surface one call later as an unknown handle.
         */
        private void onBuilderCall(BuilderCall call) {
            long callId = call.getCallId();
            HandleAssigned answer = switch (call.getCallCase()) {
                case SOURCE -> minted(callId, assembler.source(call.getSource().getTopic()));
                case MAP_VALUES -> minted(callId, assembler.mapValues(
                        call.getMapValues().getHandle(), call.getMapValues().getFunctionToken()));
                case GROUP_BY_KEY -> minted(callId, assembler.groupByKey(call.getGroupByKey().getHandle()));
                case REDUCE -> minted(callId, assembler.reduce(
                        call.getReduce().getHandle(), call.getReduce().getFunctionToken(),
                        call.getReduce().getStoreName()));
                case COUNT -> minted(callId,
                        assembler.count(call.getCount().getHandle(), call.getCount().getStoreName()));
                case SINK -> {
                    assembler.sink(call.getSink().getHandle(), call.getSink().getTopic());
                    yield HandleAssigned.newBuilder().setCallId(callId).build();
                }
                // The five-method set is the contract. A sixth is the increment that tests whether the wire
                // generalises, and until it exists the refusal has to name what was asked for.
                case CALL_NOT_SET -> throw new TopologyDescriptionException(
                        "builder call " + callId + " names no method");
                default -> throw new TopologyDescriptionException(
                        "builder call " + callId + " names " + call.getCallCase()
                                + ", which is outside this engine's five-method set");
            };
            send(StreamsServerMessage.newBuilder().setHandleAssigned(answer).build());
        }

        /** A minting call's answer: the handle plus the type recorded at its mint. */
        private HandleAssigned minted(long callId, long handle) {
            return HandleAssigned.newBuilder()
                    .setCallId(callId)
                    .setHandle(handle)
                    .setType(assembler.typeOf(handle))
                    .build();
        }

        /**
          * Answers what the topology looks like, without consuming the right to start it.
          *
          * <p>Materialising the topology closes the description - no further builder call is
          * accepted afterwards - which is why this is worth saying out loud: asking is not free of
          * consequence, it just is not fatal.
          */
        private void onDescribe() {
            send(StreamsServerMessage.newBuilder()
                    .setTopologyDescription(TopologyDescriber.describe(assembler.build()))
                    .build());
        }

        private void onDescribeComplete() {
            if (run != null) {
                throw new TopologyDescriptionException("the topology is already running");
            }
            run = runner.apply(assembler.build(), open);
        }

        private void onResult(InvocationResult result) {
            if (result.hasError()) {
                registry.fail(result.getCorrelation(), result.getError());
            } else {
                registry.complete(result.getCorrelation(), result.getValue().toByteArray());
            }
        }

        /** Called from Kafka Streams threads, which is why the send is locked. */
        private void emitInvocation(long correlation, long functionToken, byte[] key, byte[] value,
                                    byte[] aggregate) {
            Invocation.Builder invocation = Invocation.newBuilder()
                    .setCorrelation(correlation)
                    .setFunctionToken(functionToken);
            if (key != null) {
                invocation.setKey(ByteString.copyFrom(key));
            }
            if (value != null) {
                invocation.setValue(ByteString.copyFrom(value));
            }
            if (aggregate != null) {
                // Set only when there IS one. Its presence is what tells the host to combine rather than map, so
                // an unconditional set would turn every mapping into a reduction against empty bytes.
                invocation.setAggregate(ByteString.copyFrom(aggregate));
            }
            send(StreamsServerMessage.newBuilder().setInvocation(invocation).build());
        }

        private void fault(String reason) {
            send(StreamsServerMessage.newBuilder().setFault(Fault.newBuilder().setReason(reason)).build());
        }

        private void send(StreamsServerMessage message) {
            synchronized (transmitLock) {
                if (!closed) {
                    toClient.onNext(message);
                }
            }
        }

        @Override
        public void onError(Throwable broken) {
            log.warn("session stream failed: {}", broken.toString());
            stop();
        }

        @Override
        public void onCompleted() {
            stop();
            synchronized (transmitLock) {
                if (!closed) {
                    closed = true;
                    toClient.onCompleted();
                }
            }
        }

        private void stop() {
            synchronized (transmitLock) {
                closed = true;
            }
            if (run != null) {
                run.close();
                run = null;
            }
        }
    }
}
