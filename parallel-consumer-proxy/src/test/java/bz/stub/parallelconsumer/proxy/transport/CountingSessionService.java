package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A stand-in session service carrying AE12's two counters: how many times the service method ran, and how
 * many application messages it received. The transport tests prove rejection <b>before the service method</b>
 * by asserting both counters unchanged across a rejected connection - a counter-based proof, per the U1 gate,
 * because a test asserting only the client-visible status can pass after the service method has already run.
 * <p>
 * On {@code Configure} it echoes a {@code Configured}, so an admitted stream can be proven live end to end.
 */
class CountingSessionService extends ProxyServiceGrpc.ProxyServiceImplBase {

    final AtomicInteger serviceInvocations = new AtomicInteger();
    final AtomicInteger applicationMessages = new AtomicInteger();

    @Override
    public StreamObserver<ClientMessage> session(StreamObserver<ProxyMessage> responseObserver) {
        serviceInvocations.incrementAndGet();
        return new StreamObserver<>() {
            @Override
            public void onNext(ClientMessage message) {
                applicationMessages.incrementAndGet();
                if (message.hasConfigure()) {
                    responseObserver.onNext(ProxyMessage.newBuilder()
                            .setConfigured(Configured.newBuilder()
                                    .addAllTopics(message.getConfigure().getTopicsList()))
                            .build());
                }
            }

            @Override
            public void onError(Throwable t) {
                // The peer went away; nothing to answer.
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
