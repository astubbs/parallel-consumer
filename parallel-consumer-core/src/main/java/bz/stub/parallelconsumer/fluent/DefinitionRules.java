package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.Percent;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
     * The route table, used for the one check that asks whether this instance reads a topic itself - a dead-letter
     * destination it also consumes.
     */
    private final Map<String, RouteState> routesByTopic;

    /**
     * The connection properties, asked only whether a key was declared and, once, what it was set to.
     */
    private final ConnectionProperties connection;

    /**
     * The instance-wide commit mode, which two refusals read: the transactional id must agree with it, and an
     * export destination is refused under the transactional mode until producer recovery lands.
     */
    private final CommitMode commitMode;

    /**
     * The per-route defaults, read by the one refusal that has to know whether a policy came from the route or
     * from the instance - the message names the scope, so the author is pointed at the call they wrote.
     */
    private final InstanceDefaults defaults;

    /**
     * The instance-wide export percentage as declared, or null when nothing declared one. Held only to be refused
     * by name: the trigger it would drive reads an engine accessor that does not exist yet (KTD5).
     */
    private final Percent instancePayloadPercentage;

    /**
     * Whether the caller supplied a finished producer, which is what excuses a transactional definition from
     * carrying a transactional id in its properties: the id is already on the client they built.
     */
    private final boolean preBuiltProducerSupplied;

    /**
     * What was established about that producer when it was handed over - whether it can open a transaction. Read at
     * supply time because the facade does not keep the client (KTD3); see {@link SuppliedProducer}.
     */
    private final Optional<Boolean> preBuiltProducerIsTransactional;

    DefinitionRules(List<RouteState> routes,
                    Map<String, RouteState> routesByTopic,
                    ConnectionProperties connection,
                    CommitMode commitMode,
                    InstanceDefaults defaults,
                    Percent instancePayloadPercentage,
                    boolean preBuiltProducerSupplied,
                    Optional<Boolean> preBuiltProducerIsTransactional) {
        this.routes = routes;
        this.routesByTopic = routesByTopic;
        this.connection = connection;
        this.commitMode = commitMode;
        this.defaults = defaults;
        this.instancePayloadPercentage = instancePayloadPercentage;
        this.preBuiltProducerSupplied = preBuiltProducerSupplied;
        this.preBuiltProducerIsTransactional = preBuiltProducerIsTransactional;
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
                            + "producer with withProducer(...).",
                    commitMode, ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        }
        refuseASuppliedProducerThatDisagreesWithTheCommitMode(transactional);
    }

    /**
     * A supplied producer and the commit mode have to agree, and <b>its presence is not evidence that they do</b>.
     * <p>
     * Supplying a producer used to excuse the transactional definition from every check, on the reasoning that the
     * id is already on the client the caller built. That reasoning does not verify anything: a non-transactional
     * producer under {@link CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER} passed validation and was refused later by
     * the engine's producer manager at start, and a transactional producer under a consumer commit mode did the
     * same in the other direction. Putting a {@code transactional.id} in the connection properties could even make
     * the first of those look deliberately valid, when those properties are not what built the supplied client and
     * nothing reads them. U2 asks for a definition-time refusal in both directions, and the answer is read off the
     * client itself rather than inferred from what is beside it.
     * <p>
     * <b>An unknown answer refuses nothing.</b> The probe reads client internals and can decline - see
     * {@link SuppliedProducer} - and "could not tell" is not "not transactional". Refusing on it would fail a well
     * formed definition for a fact nobody established; standing aside leaves the engine's start-time check exactly
     * where it already was.
     */
    private void refuseASuppliedProducerThatDisagreesWithTheCommitMode(boolean transactional) {
        if (!preBuiltProducerSupplied) {
            return;
        }
        boolean knownTransactional = preBuiltProducerIsTransactional.orElse(false);
        boolean knownNotTransactional = preBuiltProducerIsTransactional.isPresent() && !knownTransactional;
        if (transactional && knownNotTransactional) {
            throw new IllegalArgumentException(msg("The commit mode is {} and the producer supplied with "
                            + "withProducer(...) was not built with a {}, so it can never open a transaction. Build it "
                            + "with one, or declare a consumer commit mode.",
                    commitMode, ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        }
        if (!transactional && knownTransactional) {
            throw new IllegalArgumentException(msg("The producer supplied with withProducer(...) was built with a {}, so "
                            + "it is transactional, but the commit mode is {} - which commits offsets through the "
                            + "consumer and never opens a transaction, so that producer's transactional guarantee "
                            + "would silently not apply. Declare withCommitMode({}), or supply a producer built "
                            + "without a transactional id.",
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, commitMode,
                    CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER));
        }
    }

    /**
     * Policy last, once every route has resolved its defaults, so each check reads the policy the route will
     * actually run with rather than the one it declared.
     * <p>
     * Everything refused here is a setting that could not be honoured - a trigger with no destination, a
     * destination with no trigger, a destination this instance reads itself, or one under a commit mode that
     * cannot recover from a failed export. Each refusal names its topic, because a policy mistake is a mistake
     * about one route (R27, AE7).
     */
    private void validatePolicy() {
        if (instancePayloadPercentage != null) {
            throw refusedPercentage(instancePayloadPercentage, null);
        }
        refuseAPolicyNothingCanTrigger();
        for (RouteState route : routes) {
            AfterRetries policy = route.resolvedAfterRetries();
            String topic = route.describeTopics();
            refuseHalfAParkCycle(route);
            if (policy.payloadPercentage().isPresent()) {
                throw refusedPercentage(policy.payloadPercentage().get(), topic);
            }
            if (policy.destination() == null) {
                if (policy.isDlqImmediately()) {
                    throw noDestination("dlqImmediately", topic);
                }
                if (policy.ageBound() != null) {
                    throw noDestination("dlqOlderThan", topic);
                }
                continue;
            }
            if (!policy.hasExportTrigger()) {
                throw new IllegalArgumentException(msg("Topic {} declares the dead-letter destination {} with no "
                        + "trigger, so nothing would ever be exported to it. The payload-fraction trigger needs an "
                        + "engine accessor that does not exist yet (KTD5), so declare dlqImmediately() or "
                        + "dlqOlderThan(...), or drop the destination and let records park in place.",
                        topic, policy.destination()));
            }
            if (routesByTopic.containsKey(policy.destination())) {
                throw new IllegalArgumentException(msg("Topic {} names {} as its dead-letter destination, and this "
                        + "instance routes {} itself - it would consume its own exports. Send them to a topic this "
                        + "definition does not read (R13).",
                        topic, policy.destination(), policy.destination()));
            }
            if (commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER) {
                throw new IllegalArgumentException(msg("Topic {} declares the dead-letter destination {} under the "
                        + "{} commit mode, which is refused until producer recovery lands (astubbs#410, closing "
                        + "astubbs#225): an export send that fails inside the transaction aborts it, the instance "
                        + "terminates, and on restart the attempt counts reset - a persistently failing export would "
                        + "loop. Park in place under this commit mode, or use a consumer commit mode (R14).",
                        topic, policy.destination(), commitMode));
            }
        }
    }

    /**
     * An after-retries policy is the answer to "what happens when a record runs out of attempts", so retrying
     * forever leaves it nothing to react to. Exhaustion is the only thing that consults a policy, and under
     * unbounded retries no record ever reaches it: the reaction - park <em>and</em> stop alike - the park cycles
     * and the export triggers all go inert together. This is not a reaction that merely never fires in practice;
     * it is a setting the code can never read, which is the one thing this definition refuses to produce
     * (R10, R27, AE7).
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
            throw new IllegalArgumentException(msg("Topic {} {}, and its after-retries policy comes from {} - so "
                            + "the policy can never fire. Running out "
                            + "of attempts is the only thing that consults an after-retries policy, and a record "
                            + "that retries forever never runs out, so retrying forever leaves the policy nothing "
                            + "to react to: the reaction, the park cycles and the export triggers are all inert. "
                            + "Declare {}, or drop the policy (R10, R27).",
                    route.describeTopics(),
                    route.declaresOwnRetryLimit()
                            ? "declares retryForever()"
                            : "retries forever, from the instance's withDefaultRetryForever()",
                    policyDeclaredBy(route),
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
     * <p>
     * Takes the route rather than its resolved policy, because the refusal has to say <em>where the policy was
     * written</em> as well as which topic it lands on, and only the route knows which of the two calls declared it -
     * see {@link #policyDeclaredBy}. Read off the resolved policy, a park cycle the instance declared is
     * indistinguishable from one the route declared, and that is what these two refusals used to report it as.
     */
    private void refuseHalfAParkCycle(RouteState route) {
        AfterRetries policy = route.resolvedAfterRetries();
        if (!policy.declaresAnyParkCycle()) {
            return;
        }
        if (policy.parkDelay() == null) {
            throw new IllegalArgumentException(msg("The after-retries policy on topic {} comes from {} and declares "
                            + "forCycles({}) with no park delay - a cycle is a wait followed by one more attempt, "
                            + "so declare thenRetryAfter(...) beside it, or drop it and let the record park as soon "
                            + "as its retries run out (R27).",
                    route.describeTopics(), policyDeclaredBy(route), policy.parkCycles()));
        }
        if (policy.parkCycles() == 0) {
            throw new IllegalArgumentException(msg("The after-retries policy on topic {} comes from {} and declares "
                            + "thenRetryAfter({}) with no cycle count - nothing would ever wait that long, because "
                            + "no attempt has been granted. Declare forCycles(...) beside it, or drop it (R27).",
                    route.describeTopics(), policyDeclaredBy(route), policy.parkDelay()));
        }
    }

    /**
     * The shared wording for an export trigger declared with nothing to send to: two settings, one sentence, so
     * the two cannot drift into answering the same mistake differently (R27).
     *
     * @param setting the trigger that was declared without a destination
     * @param topic   the route's topics, named so the reader knows which route to fix
     * @return the exception to throw, so the call site reads as {@code throw noDestination(...)}
     */
    private IllegalArgumentException noDestination(String setting, String topic) {
        return new IllegalArgumentException(msg("Topic {} declares {} with no dead-letter destination - declare "
                + "dlqTo(...) beside it, or drop it and let records park in place (R27)", topic, setting));
    }

    /**
     * One refusal covering both halves of R27's rule: no explicit percentage is accepted in this version at all, and
     * a value above the ceiling would never be reached even when they are.
     */
    private IllegalArgumentException refusedPercentage(Percent percentage, String topic) {
        // Spelled per side rather than once: the same trigger is dlqAtOffsetPayload on a route's park policy and
        // withDlqAtOffsetPayload on the definition, where every setting carries the prefix (KD16), and the message
        // has to keep matching the method a user actually wrote. The sibling case is policyDeclaredBy, which asks
        // the same KD16 question about the call that handed a policy over; it is not reused here because the two
        // answer with different strings - it names a hand-in call from the with-DEFAULT family, this names the
        // setting itself, and no mechanical rule maps one pairing onto the other.
        String setting = topic == null ? "withDlqAtOffsetPayload" : "dlqAtOffsetPayload";
        String where = topic == null ? "the definition" : "topic " + topic;
        String ceiling = percentage.compareTo(AfterRetries.MAX_PAYLOAD_PERCENTAGE) > 0
                ? msg(" It is also above the ceiling of {}: the engine stops a partition taking work at {} of the "
                        + "commit-metadata cap, so a percentage at or near that is never reached.",
                AfterRetries.MAX_PAYLOAD_PERCENTAGE,
                AfterRetries.PAUSE_THRESHOLD_PERCENTAGE)
                : "";
        return new IllegalArgumentException(msg("{} ({}) on {} is not supported in this version: the trigger reads a "
                        + "partition's encoded payload length, and the engine has no accessor for it yet, so an "
                        + "explicit percentage would be a setting that never fires (KTD5). Records park in place "
                        + "until it lands; declare dlqImmediately() or dlqOlderThan(...) for an export today.{}",
                setting, percentage, where, ceiling));
    }

    /**
     * A route's after-retries policy named as the call that declared it, in the spelling valid <em>where that call
     * was written</em>: {@code afterRetries(...)} on the route, {@code withDefaultAfterRetries(...)} on the
     * definition, since every instance-wide setting carries the {@code with} prefix and its per-route twin keeps the
     * bare name (KD16).
     * <p>
     * <b>Every refusal about a policy goes through here, and that is the point.</b> A policy reaches a route by one
     * of two calls spelled differently, so a refusal that names the other one sends the author to a call they did
     * not write - and, when they declared it on the definition, to one they could not have written there at all.
     * {@link #refuseHalfAParkCycle} said "Topic X declares forCycles(3)" for a cycle count
     * {@code withDefaultAfterRetries(...)} had declared: the author went to that route, found nothing of the kind on
     * it, and the message had nothing else to offer them.
     * <p>
     * A method rather than another copy of the same ternary at each site, because choosing between two literals per
     * refusal is exactly what let a refusal drift into naming the scope its author was not on.
     */
    private static String policyDeclaredBy(RouteState route) {
        return route.declaresOwnAfterRetries()
                ? "its own afterRetries(...)"
                : "the instance's withDefaultAfterRetries(...)";
    }

    /**
     * The routed topics as one string for a refusal message. Naming them is the part that makes a refusal
     * actionable: it is what shows the reader which routes are superseding the setting they typed.
     */
    private String routedTopics() {
        return routesByTopic.keySet().toString();
    }
}
