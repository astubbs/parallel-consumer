package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Client-side observer that records what the proxy said and how the stream ended, so wire tests can await a
 * reply or a terminal status without hand-rolling latches per test.
 */
class RecordingProxyMessageObserver implements StreamObserver<ProxyMessage> {

    final List<ProxyMessage> messages = Collections.synchronizedList(new ArrayList<>());
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch terminated = new CountDownLatch(1);

    @Override
    public void onNext(ProxyMessage message) {
        messages.add(message);
    }

    @Override
    public void onError(Throwable t) {
        error.set(t);
        terminated.countDown();
    }

    @Override
    public void onCompleted() {
        terminated.countDown();
    }
}
