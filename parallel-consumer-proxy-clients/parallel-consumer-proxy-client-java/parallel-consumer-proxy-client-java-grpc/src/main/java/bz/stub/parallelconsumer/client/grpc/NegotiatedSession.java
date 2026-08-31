package bz.stub.parallelconsumer.client.grpc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * What the proxy said it is actually running, from its {@code Configured} reply - the effective values, which
 * are the ones that govern. <b>Assert what came back, never what was asked for:</b> every option is a request,
 * an unset one means "take the engine's default", and only this says what the default resolved to.
 * <p>
 * It is on the gRPC transport rather than on the shared client API on purpose. Executor counts and capability
 * tokens are wire concepts - the shared surface deliberately carries no epochs, no tokens and no connection
 * state, because nine languages mirror it and it must stay expressible in all of them. A wrapper built on
 * <em>this</em> transport is already speaking the wire's language and may have them.
 *
 * @author Antony Stubbs
 * @see GrpcParallelConsumerClient#connect()
 */
public final class NegotiatedSession {

    private final int executorCount;
    private final int maxConcurrency;
    private final Set<String> capabilities;

    NegotiatedSession(int executorCount, int maxConcurrency, Set<String> capabilities) {
        this.executorCount = executorCount;
        this.maxConcurrency = maxConcurrency;
        this.capabilities = Collections.unmodifiableSet(new LinkedHashSet<>(capabilities));
    }

    /** How many executors to run. Sent once by the proxy and never revised (KTD38). */
    public int executorCount() {
        return executorCount;
    }

    /** The effective in-flight ceiling, which is also this client's dispatch-queue depth (KTD39). */
    public int maxConcurrency() {
        return maxConcurrency;
    }

    /**
     * The negotiated intersection of what the client declared and what the proxy offers - the only statement
     * of which duties exist on this session. Neither side sends a message type outside it.
     */
    public Set<String> capabilities() {
        return capabilities;
    }

    @Override
    public String toString() {
        return "NegotiatedSession(executorCount=" + executorCount + ", maxConcurrency=" + maxConcurrency
                + ", capabilities=" + capabilities + ")";
    }
}
