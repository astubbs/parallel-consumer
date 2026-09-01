package bz.stub.parallelconsumer.proxy;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * The session service a sidecar built without an engine hosts: it accepts nothing and says so.
 *
 * <p>This exists because the shell has to host <em>something</em> - the transport's contract with the thing
 * it serves is {@link io.grpc.BindableService}, and a server with no service is not a server. It is not a
 * stub of the engine and deliberately implements none of the protocol's semantics: a client that connects
 * gets {@link Status#UNIMPLEMENTED} with a description saying which piece is missing, which is the literal
 * truth of this build rather than a placeholder pretending to work. Answering a {@code Configure} with a
 * fabricated {@code Configured} would be the alternative, and it would let a client believe it had
 * configured an engine that does not exist.
 *
 * <p><b>It was replaced in production and kept as a fixture, which is the outcome the sidecar-shell rung
 * (astubbs/parallel-consumer#384) named in advance when it deferred the engine.</b>
 * {@code Main#sessionServiceFactory()} now supplies the real connect-time configuration handler, so this
 * class moved to the test tree rather than being deleted: the eight cross-language
 * {@code SidecarHandshakeTest}s and the transport's own tests need a {@link io.grpc.BindableService} that
 * hosts nothing, and {@link NoEngineMain} is the entry point they spawn to get one. Deleting it would have
 * removed their subject rather than their scaffolding.
 *
 * @author Antony Stubbs
 * @see NoEngineMain
 */
@Slf4j
class NoEngineSessionService extends ProxyServiceGrpc.ProxyServiceImplBase {

    /**
     * What a rejected client is told. Stated in full because the failure it describes is a build-time
     * omission, and a bare {@code UNIMPLEMENTED} would send the client author looking for their own bug.
     */
    static final String NO_ENGINE_DESCRIPTION =
            "this sidecar build hosts no Parallel Consumer engine: the transport, its admission rules and the "
                    + "process lifecycle are present, but connect-time configuration and record dispatch are "
                    + "not, so there is nothing for a session to do";

    @Override
    public StreamObserver<ClientMessage> session(StreamObserver<ProxyMessage> responseObserver) {
        log.warn("Refusing a session: {}", NO_ENGINE_DESCRIPTION);
        responseObserver.onError(Status.UNIMPLEMENTED.withDescription(NO_ENGINE_DESCRIPTION).asRuntimeException());
        return new StreamObserver<>() {
            @Override
            public void onNext(ClientMessage message) {
                // The call is already closed; anything still in flight from the client is discarded rather
                // than parsed, because parsing it would be the first semantic this class must not have.
            }

            @Override
            public void onError(Throwable t) {
                // The peer went away, which is the expected next thing to happen here.
            }

            @Override
            public void onCompleted() {
                // Already terminated by onError above; completing again would be an IllegalStateException.
            }
        };
    }
}
