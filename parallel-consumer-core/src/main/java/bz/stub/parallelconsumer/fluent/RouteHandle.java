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

    /**
     * The instance this route belongs to. The handle owns the parked state, so nothing is cached here and a route
     * handle held past the instance's close answers the way the instance does rather than from a stale copy.
     */
    private final ConsumerHandle handle;

    /**
     * Every topic the route binds, not the one that was asked for: a route declared over a set answers under any of
     * them and its view spans all of them, because they share one function and one policy (R5).
     */
    private final Set<String> topics;

    /**
     * Package-private: a route handle is only minted by {@link ConsumerHandle#topic(String)}, which is what refuses a
     * topic no route claims before a handle for it can exist.
     */
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

    /**
     * Renders the route's name by delegating to the one owner of that spelling, {@code RouteState}, rather than
     * formatting a set here: a refusal and a handle's {@code toString} are read side by side and must agree.
     */
    private String describeTopics() {
        return RouteState.describeTopics(topics);
    }

    /**
     * Names the route rather than the instance - a route handle appears in a line about one route's parked set.
     */
    @Override
    public String toString() {
        return "RouteHandle(" + describeTopics() + ")";
    }
}
