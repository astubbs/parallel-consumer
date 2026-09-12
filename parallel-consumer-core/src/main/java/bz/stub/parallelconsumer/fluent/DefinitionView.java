package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A validated definition as a reader sees it - the argument the runtime seam is handed (KTD9).
 *
 * <h2>What a definition is</h2>
 * A <em>definition</em> is the complete description of one consumer, assembled by the fluent calls and then fixed:
 * the connection properties it will use, every route declared on it - each with its topics, its consumed and
 * produced formats and its processing function - the policy each of those routes carries, and the instance-wide
 * defaults a route falls back to when it declares none of its own. It describes; it does not run. Starting a
 * definition is what produces a running instance, and the same definition can start more than one.
 *
 * <h2>What the view is</h2>
 * This interface is what a <em>runtime</em> - the real Kafka clients, a broker-free sandbox, a test - may read of a
 * definition. It is enough for an implementation of {@link ClientRuntime} to build or fake the clients that
 * definition needs: which topics to subscribe to, what each route consumes and produces, the ordering and the
 * commit mode, and whether a producer is needed at all. It carries no way to change the definition, and it is
 * only ever handed out after validation has passed, so a reader never sees a definition that was going to be
 * refused.
 */
@InterfaceStability.Unstable
public interface DefinitionView {

    /**
     * Every topic the instance subscribes to: the union of the routes' topics.
     */
    Set<String> topics();

    /**
     * Every route, in the order they were declared, as an unmodifiable snapshot. A runtime that wants one topic's
     * route asks {@link #route(String)} rather than scanning this.
     */
    Collection<RouteView> routes();

    /**
     * The route bound to one topic, which is the lookup a runtime actually performs. It can answer with a single
     * route rather than a collection because a definition refuses a second route for a topic it already routes.
     *
     * @return the route bound to that topic, or null when nothing routes it
     */
    RouteView route(String topic);

    /**
     * Instance-wide: the engine has one consumer, one commit and one transaction (KD11).
     */
    CommitMode commitMode();

    /**
     * The instance default every route copies. Per-route ordering is an engine change on the shard-key seam (R6).
     */
    ProcessingOrder ordering();

    /**
     * Whether anything in this definition needs a producer: a route that declares produced types, a dead-letter
     * destination, or the transactional commit mode (R4, KTD2). When this is false the facade opens no producer and
     * starts on the plain poll flow.
     */
    boolean requiresProducer();

    /**
     * The connection properties as supplied, for a client this seam builds. Never carries a key or value
     * deserialiser: the facade reads raw bytes and each route decodes its own, and a deserialiser named here is
     * refused at definition time (R4).
     */
    Map<String, Object> connectionProperties();

    /**
     * The properties each route's <em>formats</em> are configured with - its deserialisers and, on a route that
     * produces, its serialisers. Named for the formats rather than for the clients because that is the whole
     * distinction it draws: it is everything in {@link #connectionProperties()} except the keys the facade owns and
     * hands to the Kafka clients itself - the bootstrap servers, the group id and the client serialisers. A
     * schema-registry URL reaches a route's formats; the bootstrap servers do not (R4, KTD7).
     */
    Map<String, Object> formatProperties();

    /**
     * What is known about a pre-built consumer supplied by the caller, for the message a raw-bytes cast failure
     * carries (KTD3), or null when the facade builds the consumer itself.
     */
    String preBuiltConsumerDescription();
}
