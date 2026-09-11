package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Set;

/**
 * One route on a running instance: what {@link ConsumerHandle#topic(String)} hands back (R28).
 * <p>
 * It exists so the parked set is retrieved <em>by the route's name</em> - {@code handle.topic("orders").parked()} -
 * rather than through an overloaded accessor that means one thing with an argument and another without. The
 * instance-wide roll-up is named apart, on the handle itself.
 * <p>
 * A route declared over a set of topics (R5) answers here under any one of them, and its view spans all of them:
 * they share one function and one policy, so they are one route.
 */
@InterfaceStability.Unstable
public final class RouteHandle {

    private final ConsumerHandle handle;

    private final Set<String> topics;

    RouteHandle(ConsumerHandle handle, Set<String> topics) {
        this.handle = handle;
        this.topics = topics;
    }

    /**
     * The topics this route binds - more than one when it was declared over a set.
     */
    public Set<String> topics() {
        return topics;
    }

    /**
     * This route's parked records, across every partition (R28). Narrow with {@link ParkedView#partition(int)}.
     */
    public ParkedView parked() {
        return handle.parkedView(describeTopics(), topics);
    }

    private String describeTopics() {
        return RouteState.describeTopics(topics);
    }

    @Override
    public String toString() {
        return "RouteHandle(" + describeTopics() + ")";
    }
}
