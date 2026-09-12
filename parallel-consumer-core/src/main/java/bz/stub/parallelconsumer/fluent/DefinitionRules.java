package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.List;
import java.util.Map;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Every refusal a definition makes before anything is built, in one place (AE7).
 * <p>
 * The fluent package's rule is that a setting it cannot honour is refused where the caller wrote it, at definition
 * time, rather than accepted and left inert - so these checks are a substantial part of what the package IS, not
 * incidental validation. Holding them here rather than on {@link ParallelConsumerDefinition} keeps that body of
 * rules readable as one thing, and keeps the definition readable as the fluent surface it is.
 * <p>
 * <b>No client is built by any of this.</b> That is the property the checks exist to preserve: a mistake in a
 * definition surfaces before a consumer has joined a group.
 * <p>
 * Built fresh for each {@link ParallelConsumerDefinition#validate()} call, over the definition's state as it stands
 * at that moment, because a definition is mutable until it starts and validating a snapshot taken earlier would
 * check a definition nobody wrote.
 */
final class DefinitionRules {

    /**
     * The routes in declaration order, which is the order refusals name them in.
     */
    private final List<RouteState> routes;

    /**
     * The route table, read by {@link #routedTopics()} so that a refusal about a superseded connection property can
     * name the routes that supersede it.
     */
    private final Map<String, RouteState> routesByTopic;

    /**
     * The connection properties, asked only whether a key was declared and, once, what it was set to.
     */
    private final ConnectionProperties connection;

    /**
     * The instance-wide commit mode, read by the one refusal that says the transactional id and the commit mode
     * must agree.
     */
    private final CommitMode commitMode;

    /**
     * The per-route defaults, read by the one refusal that has to know whether a policy came from the route or
     * from the instance - the message names the scope, so the author is pointed at the call they wrote.
     */
    private final InstanceDefaults defaults;

    /**
     * Whether the caller supplied a finished producer, which is what excuses a transactional definition from
     * carrying a transactional id in its properties: the id is already on the client they built.
     */
    private final boolean preBuiltProducerSupplied;

    DefinitionRules(List<RouteState> routes,
                    Map<String, RouteState> routesByTopic,
                    ConnectionProperties connection,
                    CommitMode commitMode,
                    InstanceDefaults defaults,
                    boolean preBuiltProducerSupplied) {
        this.routes = routes;
        this.routesByTopic = routesByTopic;
        this.connection = connection;
        this.commitMode = commitMode;
        this.defaults = defaults;
        this.preBuiltProducerSupplied = preBuiltProducerSupplied;
    }

    /**
     * Every check, in a fixed order - routes, then properties, then policy. The order is load-bearing twice:
     * {@link #validateRoutes()} resolves each route's defaults, so everything after it reads the values a route
     * will actually run with rather than the nulls that mean "take the instance's"; and a definition with no route
     * at all is reported as that, rather than as whichever setting the later checks happened to reach first.
     *
     * @throws IllegalArgumentException naming the offending topic or setting
     */
    void validate() {
        validateRoutes();
        validateProperties();
        validatePolicy();
    }

    /**
     * Routes first, because a definition with no route, or a topic with no function, is a mistake about the shape
     * of the definition rather than about one setting - and saying so before the property and policy checks run
     * stops those reporting on a definition that was never going to start (AE7).
     * <p>
     * Resolving each route's defaults here is what lets everything below read the values a route will actually run
     * with, rather than the nulls that mean "take the instance's".
     */
    private void validateRoutes() {
        if (routes.isEmpty()) {
            throw new IllegalArgumentException("This definition declares no routes - declare at least one topic with "
                    + "a processing function before starting");
        }
        for (RouteState route : routes) {
            if (!route.hasFunction()) {
                throw new IllegalArgumentException(msg("Topic {} has no processing function - a route is a statement "
                        + "that ends in process(...)", route.describeTopics()));
            }
            route.resolveDefaults();
        }
    }

    /**
     * The facade reads raw bytes and each route decodes its own records, so a deserialiser named in the connection
     * properties would be silently superseded. It is refused, naming the setting and the routes that supersede it
     * (R4); everything else the facade does not own is passed to each route's deserialisers.
     */
    private void validateProperties() {
        refuseDeserialiserSetting(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        refuseDeserialiserSetting(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        refuseConsumerAutoCommit();
        validateTransactionalId();

        Map<String, Object> forFormats = connection.forFormats();
        for (RouteState route : routes) {
            route.consumedKey().configure(forFormats, true);
            route.consumedValue().configure(forFormats, false);
            if (route.producesRecords()) {
                route.producedKey().configure(forFormats, true);
                route.producedValue().configure(forFormats, false);
            }
        }
    }

    /**
     * Parallel Consumer commits offsets itself, and refuses to run a consumer that auto-commits - so a definition
     * that asked for both would fail at start, from inside a client the user never built.
     * <p>
     * Kafka's own default is {@code true}, which is why {@link KafkaClientRuntime} sets it to {@code false} on the
     * consumer it constructs rather than leaving the default to fail every properties-only definition (R1). What is
     * refused here is only the explicit {@code true}: silently overriding a setting somebody typed would be the one
     * outcome worse than either.
     */
    private void refuseConsumerAutoCommit() {
        Object declared = connection.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (declared != null && Boolean.parseBoolean(String.valueOf(declared))) {
            throw new IllegalArgumentException(msg("{} is {} in the connection properties, and Parallel Consumer "
                            + "commits offsets for you - a consumer that also commits on its own would commit "
                            + "records this instance has not finished. Remove the setting; the fluent API disables "
                            + "it on the consumer it builds.",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, declared));
        }
    }

    /**
     * One wording for both deserialiser settings, so the key and the value halves of the same mistake cannot be
     * answered in two different sentences. It names the routes that supersede the setting, because "it would never
     * be used" is only actionable once the reader can see what is using its own types instead.
     *
     * @param setting the connection-properties key being refused
     */
    private void refuseDeserialiserSetting(String setting) {
        if (connection.contains(setting)) {
            throw new IllegalArgumentException(msg("{} was supplied in the connection properties, and the routes for "
                            + "{} supersede it: the fluent API consumes raw bytes and each route applies its own "
                            + "deserialisers, so a deserialiser named here would never be used. Remove it and "
                            + "declare the types on the route (R4).",
                    setting, routedTopics()));
        }
    }

    /**
     * The transactional id and the commit mode have to agree: Parallel Consumer builds its producer from these
     * properties, and Kafka's producer needs the id in that map to be transactional at all.
     */
    private void validateTransactionalId() {
        boolean idDeclared = connection.contains(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        boolean transactional = commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
        if (idDeclared && !transactional) {
            throw new IllegalArgumentException(msg("{} is in the connection properties but the commit mode is {} - a "
                            + "transactional producer under a non-transactional commit mode never opens a "
                            + "transaction. Declare withCommitMode({}) or remove the setting.",
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, commitMode,
                    CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER));
        }
        if (transactional && !idDeclared && !preBuiltProducerSupplied) {
            throw new IllegalArgumentException(msg("The commit mode is {} but there is no {} in the connection "
                            + "properties - Parallel Consumer builds the producer from these properties and Kafka "
                            + "needs the id there to make it transactional. Add it, or supply a transactional "
                            + "producer with producer(...).",
                    commitMode, ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        }
    }

    /**
     * Policy last, once every route has resolved its defaults, so each check reads the policy the route will
     * actually run with rather than the one it declared.
     * <p>
     * Everything refused here is a setting that could not be honoured - a policy no exhaustion can reach, or half
     * of a park cycle. Each refusal names its topic, because a policy mistake is a mistake about one route
     * (R27, AE7).
     */
    private void validatePolicy() {
        refuseAPolicyNothingCanTrigger();
        for (RouteState route : routes) {
            refuseHalfAParkCycle(route.afterRetries(), route.describeTopics());
        }
    }

    /**
     * An after-retries policy is the answer to "what happens when a record runs out of attempts", so retrying
     * forever leaves it nothing to react to. Exhaustion is the only thing that consults a policy, and under
     * unbounded retries no record ever reaches it: the reaction - park <em>and</em> stop alike - and the park
     * cycles go inert together. This is not a reaction that merely never fires in practice; it is a setting the
     * code can never read, which is the one thing this definition refuses to produce (R10, R27, AE7).
     * <p>
     * Checked per route and against what was actually <em>declared</em>, because either half may be declared at
     * either scope and all four pairings are the same mistake: retryForever() on the route or
     * withDefaultRetryForever() on the instance, beside an afterRetries(...) on the route or a
     * withDefaultAfterRetries(...)
     * on the instance. The refusal names the scope each half came from, so the author is pointed at the two calls
     * they wrote rather than at the route where the two happened to meet.
     * <p>
     * What is not refused is retrying forever with no policy declared anywhere. Every route resolves to
     * {@link AfterRetries#park()} when nothing is declared, and that resolved default is not a setting anybody
     * wrote - refusing it would make retryForever() unusable, which is the opposite of the point.
     */
    private void refuseAPolicyNothingCanTrigger() {
        for (RouteState route : routes) {
            if (route.retryLimit().isPresent()) {
                continue;
            }
            boolean ownPolicy = route.declaresOwnAfterRetries();
            if (!ownPolicy && defaults.afterRetries() == null) {
                // Retrying forever with nothing declared to react: the resolved park default is not a setting.
                continue;
            }
            throw new IllegalArgumentException(msg("Topic {} {}, and {} - so the policy can never fire. Running out "
                            + "of attempts is the only thing that consults an after-retries policy, and a record "
                            + "that retries forever never runs out, so retrying forever leaves the policy nothing "
                            + "to react to: the reaction and the park cycles are both inert. "
                            + "Declare {}, or drop the policy (R10, R27).",
                    route.describeTopics(),
                    route.declaresOwnRetryLimit()
                            ? "declares retryForever()"
                            : "retries forever, from the instance's withDefaultRetryForever()",
                    ownPolicy
                            ? "declares an after-retries policy of its own"
                            : "takes the instance's withDefaultAfterRetries(...)",
                    route.declaresOwnRetryLimit()
                            ? "retryLimit(...) on this route instead"
                            : "a retryLimit(...) on this route, or replace withDefaultRetryForever() with "
                                    + "withDefaultRetryLimit(...)"));
        }
    }

    /**
     * A park delay and a cycle count mean nothing apart: a delay with no cycles grants no attempt, and cycles with
     * no delay is scheduled retry with no schedule. Either would be a setting that silently does nothing, which is
     * the one thing this definition refuses to produce (R27, AE7).
     */
    private void refuseHalfAParkCycle(AfterRetries policy, String topic) {
        if (!policy.declaresAnyParkCycle()) {
            return;
        }
        if (policy.parkDelay() == null) {
            throw new IllegalArgumentException(msg("Topic {} declares forCycles({}) with no park delay - a cycle is "
                            + "a wait followed by one more attempt, so declare thenRetryAfter(...) beside it, or "
                            + "drop it and let the record park as soon as its retries run out (R27).",
                    topic, policy.parkCycles()));
        }
        if (policy.parkCycles() == 0) {
            throw new IllegalArgumentException(msg("Topic {} declares thenRetryAfter({}) with no cycle count - "
                            + "nothing would ever wait that long, because no attempt has been granted. Declare "
                            + "forCycles(...) beside it, or drop it (R27).",
                    topic, policy.parkDelay()));
        }
    }

    /**
     * The routed topics as one string for a refusal message. Naming them is the part that makes a refusal
     * actionable: it is what shows the reader which routes are superseding the setting they typed.
     */
    private String routedTopics() {
        return routesByTopic.keySet().toString();
    }
}
