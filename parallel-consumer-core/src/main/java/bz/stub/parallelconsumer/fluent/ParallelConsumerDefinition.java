package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * A Parallel Consumer being defined: connection properties in, one typed route per topic, policy as data, and a
 * handle out. Start it from {@link ParallelConsumer#connect(Properties)}.
 *
 * <h2>What happens when</h2>
 * <b>Defining opens nothing.</b> Every check this class makes runs before a client exists, in a fixed order - routes,
 * then properties, then policy - so a mistake surfaces as a message naming the topic or the setting rather than as a
 * connection failure or a record that fails forever (AE7). A definition that is written and never started constructs
 * no consumer and no producer at all.
 * <p>
 * <b>Starting builds exactly what the definition needs.</b> A producer is opened only when something asks for one: a
 * route that declares produced types, a dead-letter destination, or the transactional commit mode. Every other
 * definition runs on the plain poll flow with no producer (R4, KTD2).
 *
 * <h2>Instance-wide settings and per-route defaults are spelled apart</h2>
 * An instance-wide setting is plain - {@link #commitMode} - because the engine has one consumer, one commit and one
 * transaction, so those are properties of the clients rather than of the work. So are {@link #closePath}, which is
 * how this instance shuts down whoever asked it to, and {@link #meterRegistry}, which is where its meters go.
 * Everything else is a per-route value
 * with an instance default, and those carry a {@code default} prefix: {@link #defaultRetryLimit},
 * {@link #defaultConcurrency}, {@link #defaultAfterRetries}. A route that declares its own overrides its copy and
 * nobody else's (KD11, R6).
 *
 * <h2>It is closeable, and so is the handle</h2>
 * {@link #start()} hands back a {@link ConsumerHandle}, which is what the README holds in try-with-resources. This
 * definition is {@link AutoCloseable} too, for a caller who would rather hold one thing than two: closing it closes
 * the instance it started, and closes nothing at all if it never started one.
 *
 * @see ParallelConsumer#connect(Properties)
 */
@Slf4j
@InterfaceStability.Unstable
public class ParallelConsumerDefinition implements DefinitionView, AutoCloseable {

    /**
     * Connection properties the facade owns, so they are not passed on to a route's deserialisers: the two that
     * address the cluster, and the client serialisers, which the facade sets to raw bytes itself (KTD7).
     */
    private static final Set<String> FACADE_OWNED_PROPERTIES = Collections.unmodifiableSet(new LinkedHashSet<>(
            java.util.Arrays.asList(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ConsumerConfig.GROUP_ID_CONFIG,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)));

    /**
     * Producer-side keys that mean nothing to a producer and would only log an unknown-configuration warning.
     */
    private static final Set<String> CONSUMER_ONLY_PROPERTIES = Collections.unmodifiableSet(new LinkedHashSet<>(
            java.util.Arrays.asList(
                    ConsumerConfig.GROUP_ID_CONFIG,
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)));

    private final Map<String, Object> properties;

    /**
     * The options under construction. A pre-built client goes into this the moment it is supplied and is never held
     * in a field of this class, which is what keeps core's raw-client architecture rule intact (KTD3).
     */
    private final ParallelConsumerOptions.ParallelConsumerOptionsBuilder<byte[], byte[]> options =
            ParallelConsumerOptions.builder();

    private final List<RouteState> routes = new ArrayList<>();

    private final Map<String, RouteState> routesByTopic = new LinkedHashMap<>();

    private CommitMode commitMode = CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;

    private ProcessingOrder defaultOrdering = ProcessingOrder.KEY;

    private int defaultConcurrency = ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY;

    /**
     * Ten attempts after the first, then park - the default that at last gives the engine's inert failure-history
     * setting of ten a meaning (R10).
     */
    private OptionalInt defaultRetryLimit = OptionalInt.of(10);

    private Duration defaultRetryDelay = Duration.ofSeconds(1);

    private AfterRetries defaultAfterRetries;

    private CircuitBreakerPolicy defaultCircuitBreaker;

    private ParkObserver<?, ?> defaultParkObserver;

    private Integer instancePayloadPercentage;

    private boolean preBuiltConsumerSupplied;

    private String preBuiltConsumerDescription;

    private boolean preBuiltProducerSupplied;

    private boolean started;

    /**
     * How this instance shuts down, whoever asked it to (R17, R24). Draining by default, which is what makes the
     * handle a graceful shutdown.
     */
    private ClosePath closePath = ClosePath.DRAIN_FIRST;

    /**
     * The handle {@link #start} produced, so that a definition held in try-with-resources closes the instance it
     * started. Null until it starts one.
     */
    private volatile ConsumerHandle startedHandle;

    /**
     * The user's own rebalance listener, chained after the facade's (KTD2). Null when none was declared.
     */
    private ConsumerRebalanceListener usersRebalanceListener;

    /**
     * The route-dispatching wrapper. Built by {@link #buildOptions}, because the retry-delay provider registered on
     * the options is one of its methods, and reused by {@link #start(ClientRuntime)} - one owner of the route
     * table, the attempt ledger and the intent hook.
     */
    private RouteDispatcher dispatcher;

    /**
     * The same door as {@link ParallelConsumer#connect(Properties)}, which is how the documentation and the README
     * spell it: {@code connect} reads as the verb Kafka's own client uses for taking configuration now and reaching
     * the cluster later, where a constructor reads as an object being built.
     */
    public ParallelConsumerDefinition(Properties connectionProperties) {
        Objects.requireNonNull(connectionProperties, "Connection properties must be supplied");
        Map<String, Object> copy = new LinkedHashMap<>();
        for (String name : connectionProperties.stringPropertyNames()) {
            copy.put(name, connectionProperties.get(name));
        }
        // Properties may carry non-String values when built programmatically; stringPropertyNames misses those.
        for (Map.Entry<Object, Object> entry : connectionProperties.entrySet()) {
            if (entry.getKey() instanceof String) {
                copy.put((String) entry.getKey(), entry.getValue());
            }
        }
        this.properties = copy;
    }

    // ---------------------------------------------------------------- instance-wide settings

    /**
     * Instance-wide, because the engine has one consumer: one offset commit per group, and under the transactional
     * mode one producer's transaction around it (KD11).
     */
    public ParallelConsumerDefinition commitMode(CommitMode commitMode) {
        this.commitMode = Objects.requireNonNull(commitMode, "A commit mode must be supplied");
        return this;
    }

    /**
     * Refused: the seam this needs has not landed.
     * <p>
     * Shutting down or carrying on when a commit exhausts its budget is the one other instance-wide setting, by the
     * same line as {@link #commitMode} - it is a property of the one commit. It sits here once the commit-failure
     * seam (astubbs#352) lands. Until then this refuses rather than storing a value that would do nothing: an
     * accepted setting that is silently inert is worse than one that says so.
     *
     * @throws IllegalArgumentException always, until astubbs#352 lands
     */
    public ParallelConsumerDefinition commitFailure(CommitFailurePolicy policy) {
        throw new IllegalArgumentException(msg("commitFailure ({}) cannot be declared yet - the commit-failure seam "
                + "it needs (astubbs#352) has not landed, and a setting that is accepted and then does nothing is "
                + "worse than one that says so. Today a commit that exhausts its budget shuts the instance down.",
                policy));
    }

    /**
     * How this instance shuts down: whether the records already fetched are processed first (R17, R24).
     * <p>
     * Instance-wide because the stop outcome closes the instance from a thread of its own, with nobody there to
     * pass an argument - and because an instance having two answers to "what happens to the backlog", one for its
     * caller and one for itself, is a difference nobody would predict correctly (KTD6).
     */
    public ParallelConsumerDefinition closePath(ClosePath path) {
        this.closePath = Objects.requireNonNull(path, "A close path must be supplied");
        return this;
    }

    /**
     * Where this instance's meters go (R19). Without one, Parallel Consumer registers into a no-op registry and
     * nothing is published.
     * <p>
     * The fluent API's own meters - what each route did with its records, and what is parked right now - register
     * here alongside every engine meter, under the {@code routes} subsystem, and are removed when the instance
     * closes.
     */
    public ParallelConsumerDefinition meterRegistry(MeterRegistry registry) {
        Objects.requireNonNull(registry, "A meter registry must be supplied");
        options.meterRegistry(registry);
        return this;
    }

    // ---------------------------------------------------------------- per-route defaults

    /**
     * The ordering guarantee every route copies. Per-route ordering needs a change at the engine's shard-key seam
     * and is a later milestone, so in this version it is the instance default and nothing else (R6).
     */
    public ParallelConsumerDefinition defaultOrdering(ProcessingOrder ordering) {
        this.defaultOrdering = Objects.requireNonNull(ordering, "An ordering must be supplied");
        return this;
    }

    /**
     * The admission target every route copies: how many of its records may be in flight at once. Routes do not
     * compete for one shared limit, so the engine's total admission is the sum of the routes' targets (R23, KD6).
     */
    public ParallelConsumerDefinition defaultConcurrency(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException(msg("defaultConcurrency ({}) must be at least one - it is each "
                    + "route's admission target", limit));
        }
        this.defaultConcurrency = limit;
        return this;
    }

    /**
     * How many attempts after the first every route allows before its records park (R10).
     */
    public ParallelConsumerDefinition defaultRetryLimit(int attempts) {
        if (attempts < 0) {
            throw new IllegalArgumentException(msg("defaultRetryLimit ({}) cannot be negative - it counts the "
                    + "attempts after the first; use defaultRetryForever() to ask for unbounded retries", attempts));
        }
        this.defaultRetryLimit = OptionalInt.of(attempts);
        return this;
    }

    /**
     * Retry forever, as the classic API always has. Opt-in on purpose (R10).
     */
    public ParallelConsumerDefinition defaultRetryForever() {
        this.defaultRetryLimit = OptionalInt.empty();
        return this;
    }

    /**
     * How long a failed record waits before its next attempt, on every route that declares no delay of its own.
     */
    public ParallelConsumerDefinition defaultRetryDelay(Duration delay) {
        Objects.requireNonNull(delay, "A retry delay must be supplied");
        if (delay.isNegative()) {
            throw new IllegalArgumentException(msg("defaultRetryDelay ({}) cannot be negative", delay));
        }
        this.defaultRetryDelay = delay;
        return this;
    }

    /**
     * What happens to a record that runs out of attempts, on every route that declares nothing of its own (R27).
     */
    public ParallelConsumerDefinition defaultAfterRetries(AfterRetries policy) {
        this.defaultAfterRetries = Objects.requireNonNull(policy, "An after-retries policy must be supplied");
        return this;
    }

    /**
     * The breaker every route copies. No breaker state is shared between routes (R29).
     */
    public ParallelConsumerDefinition defaultCircuitBreaker(CircuitBreakerPolicy policy) {
        this.defaultCircuitBreaker = Objects.requireNonNull(policy, "A circuit breaker policy must be supplied");
        return this;
    }

    /**
     * The park observer every route copies, told once when one of its records parks (R16).
     * <p>
     * Typed {@code Object} on both sides because it is one observer over routes whose consumed types differ: an
     * instance default cannot know them. Declare it on a route with {@link Route#onParked} to see that route's own
     * types.
     */
    public ParallelConsumerDefinition defaultOnParked(ParkObserver<Object, Object> observer) {
        this.defaultParkObserver = Objects.requireNonNull(observer, "A park observer must be supplied");
        return this;
    }

    /**
     * The instance default for the export percentage a route's park policy may override (R6, R27).
     * <p>
     * <b>Refused in this version</b>, at validation, along with a percentage on any route: the accessor it reads -
     * a partition's encoded payload length - does not exist in the engine yet, so an explicit percentage would be a
     * setting that never fires (KTD5). The default of {@link AfterRetries#MAX_PAYLOAD_PERCENTAGE} applies once that
     * accessor lands.
     */
    public ParallelConsumerDefinition dlqWhenOffsetPayloadReaches(int percentage) {
        this.instancePayloadPercentage = percentage;
        return this;
    }

    // ---------------------------------------------------------------- pre-built clients (Java binding only)

    /**
     * Run against a consumer you built yourself, instead of letting the definition's properties build one. Sugar for
     * the Java binding that never reaches the wire (R18): on the wire a definition is properties only.
     * <p>
     * <b>It must be a raw-bytes consumer</b>, because the facade decodes each record with its own route's
     * deserialisers (KD9). Nothing here can check that - the type parameters are erased - so a consumer configured
     * for anything else surfaces on its first record, once, as a
     * {@link RawBytesConsumerFaultException} naming the deserialisers read off it here, and stops the instance
     * rather than retrying (KTD3, R1). It must also be unsubscribed: the engine manages the subscription and refuses
     * a consumer that is not clean.
     */
    public ParallelConsumerDefinition consumer(Consumer<byte[], byte[]> consumer) {
        Objects.requireNonNull(consumer, "A consumer must be supplied");
        this.preBuiltConsumerDescription = RawBytesConsumerFaultException.describe(consumer);
        this.preBuiltConsumerSupplied = true;
        options.consumer(consumer);
        return this;
    }

    /**
     * Run against a producer you built yourself. Sugar for the Java binding, as {@link #consumer} is.
     * <p>
     * <b>A supplied producer forgoes producer recovery</b> (astubbs#410): recovery rebuilds the producer from its
     * configuration, and an instance handed a finished producer has no configuration to rebuild from. Leave this out
     * and the definition's properties build one that can recover. Under the transactional commit mode this also
     * forgoes export (R1, R14).
     */
    public ParallelConsumerDefinition producer(Producer<byte[], byte[]> producer) {
        Objects.requireNonNull(producer, "A producer must be supplied");
        this.preBuiltProducerSupplied = true;
        options.producer(producer);
        return this;
    }

    // ---------------------------------------------------------------- routes

    /**
     * The general form: a topic whose types you declare with {@link Route#consumed}, or leave as raw bytes.
     */
    public Route<byte[], byte[], Void, Void> topic(String topic) {
        return route(Collections.singleton(topic), Formats.bytes(), Formats.bytes());
    }

    /**
     * A set of topics that share one function and one type pair, as a single route with one admission target (R5).
     */
    public Route<byte[], byte[], Void, Void> topics(Collection<String> topics) {
        return route(topics, Formats.bytes(), Formats.bytes());
    }

    /**
     * Refused: a pattern subscription is not offered in this version.
     * <p>
     * A route table is keyed by topic name, so a pattern would match topics no route claims, and the record that
     * arrives from one has nowhere to go. A default route is the addition that would make it meaningful, and it is a
     * later one (KTD2). The method exists so the refusal arrives where you reach for it.
     *
     * @throws IllegalArgumentException always
     */
    public Route<byte[], byte[], Void, Void> topics(Pattern pattern) {
        throw new IllegalArgumentException(msg("A pattern subscription ({}) is not supported in this version - the "
                + "route table is keyed by topic name, so a pattern would match topics no route claims. Name the "
                + "topics with topics(Collection), or use the classic API's subscribe(Pattern).", pattern));
    }

    /**
     * A JSON topic decoded into the given class, with a string key (R4).
     */
    public <V> Route<String, V, Void, Void> json(String topic, Class<V> valueType) {
        return route(Collections.singleton(topic), Formats.string(), Formats.json(valueType));
    }

    /**
     * A JSON topic with no class: each value is a map of field names to values, so a topic nobody has a class for
     * can be consumed and inspected by field name with nothing declared (R4).
     */
    public Route<String, Map<String, Object>, Void, Void> json(String topic) {
        return route(Collections.singleton(topic), Formats.string(), Formats.json());
    }

    /**
     * An Avro topic decoded into the given specific record type, with a string key (R4, KTD7).
     */
    public <V> Route<String, V, Void, Void> avro(String topic, Class<V> valueType) {
        return route(Collections.singleton(topic), Formats.string(), Formats.avro(valueType));
    }

    /**
     * A Protobuf topic decoded into the given message type, with a string key (R4, KTD7).
     */
    public <V> Route<String, V, Void, Void> protobuf(String topic, Class<V> valueType) {
        return route(Collections.singleton(topic), Formats.string(), Formats.protobuf(valueType));
    }

    /**
     * A topic read as raw bytes, with a string key.
     */
    public Route<String, byte[], Void, Void> bytes(String topic) {
        return route(Collections.singleton(topic), Formats.string(), Formats.bytes());
    }

    /**
     * A set of topics read as raw bytes, as one route (R5).
     */
    public Route<String, byte[], Void, Void> bytes(Collection<String> topics) {
        return route(topics, Formats.string(), Formats.bytes());
    }

    /**
     * A topic of UTF-8 strings, key and value.
     */
    public Route<String, String, Void, Void> string(String topic) {
        return route(Collections.singleton(topic), Formats.string(), Formats.string());
    }

    /**
     * Registers the route and refuses a topic that is already routed, naming it - a topic has exactly one route
     * (R2, KD11).
     */
    private <K, V> Route<K, V, Void, Void> route(Collection<String> topics, Format<K> key, Format<V> value) {
        Objects.requireNonNull(topics, "At least one topic must be supplied");
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("A route must name at least one topic");
        }
        for (String topic : topics) {
            if (topic == null || topic.trim().isEmpty()) {
                throw new IllegalArgumentException("A topic name must not be blank");
            }
            RouteState existing = routesByTopic.get(topic);
            if (existing != null) {
                throw new IllegalArgumentException(msg("Topic {} is already routed by {} - a topic has exactly one "
                                + "route, so declare the second behaviour inside that route's function or on a "
                                + "different topic (R2)",
                        topic, existing));
            }
        }
        RouteState state = new RouteState(this, new LinkedHashSet<>(topics), key, value);
        routes.add(state);
        for (String topic : topics) {
            routesByTopic.put(topic, state);
        }
        return new Route<>(state);
    }

    // ---------------------------------------------------------------- validation

    /**
     * Every check this definition makes, in a fixed order - routes, then properties, then policy - and no client is
     * built by any of them (AE7). Run automatically by {@link #start}; call it directly to fail early.
     *
     * @throws IllegalArgumentException naming the offending topic or setting
     */
    public void validate() {
        validateRoutes();
        validateProperties();
        validatePolicy();
    }

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

        Map<String, Object> passThrough = passThroughProperties();
        for (RouteState route : routes) {
            route.consumedKey().configure(passThrough, true);
            route.consumedValue().configure(passThrough, false);
            if (route.producesRecords()) {
                route.producedKey().configure(passThrough, true);
                route.producedValue().configure(passThrough, false);
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
        Object declared = properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (declared != null && Boolean.parseBoolean(String.valueOf(declared))) {
            throw new IllegalArgumentException(msg("{} is {} in the connection properties, and Parallel Consumer "
                            + "commits offsets for you - a consumer that also commits on its own would commit "
                            + "records this instance has not finished. Remove the setting; the fluent API disables "
                            + "it on the consumer it builds.",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, declared));
        }
    }

    private void refuseDeserialiserSetting(String setting) {
        if (properties.containsKey(setting)) {
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
        boolean idDeclared = properties.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        boolean transactional = commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
        if (idDeclared && !transactional) {
            throw new IllegalArgumentException(msg("{} is in the connection properties but the commit mode is {} - a "
                            + "transactional producer under a non-transactional commit mode never opens a "
                            + "transaction. Declare commitMode({}) or remove the setting.",
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

    private void validatePolicy() {
        if (instancePayloadPercentage != null) {
            throw refusedPercentage("dlqWhenOffsetPayloadReaches", instancePayloadPercentage, null);
        }
        for (RouteState route : routes) {
            AfterRetries policy = route.afterRetries();
            String topic = route.describeTopics();
            refuseHalfAParkCycle(policy, topic);
            if (policy.payloadPercentage().isPresent()) {
                throw refusedPercentage("dlqWhenOffsetPayloadReaches", policy.payloadPercentage().getAsInt(), topic);
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

    private IllegalArgumentException noDestination(String setting, String topic) {
        return new IllegalArgumentException(msg("Topic {} declares {} with no dead-letter destination - declare "
                + "dlqTo(...) beside it, or drop it and let records park in place (R27)", topic, setting));
    }

    /**
     * One refusal covering both halves of R27's rule: no explicit percentage is accepted in this version at all, and
     * a value above the ceiling would never be reached even when they are.
     */
    private IllegalArgumentException refusedPercentage(String setting, int percentage, String topic) {
        String where = topic == null ? "the definition" : "topic " + topic;
        String ceiling = percentage > AfterRetries.MAX_PAYLOAD_PERCENTAGE
                ? msg(" It is also above the ceiling of {}: the engine stops a partition taking work at {}% of the "
                        + "commit-metadata cap, so a percentage at or near that is never reached.",
                AfterRetries.MAX_PAYLOAD_PERCENTAGE,
                (int) (PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT * 100))
                : "";
        return new IllegalArgumentException(msg("{} ({}) on {} is not supported in this version: the trigger reads a "
                        + "partition's encoded payload length, and the engine has no accessor for it yet, so an "
                        + "explicit percentage would be a setting that never fires (KTD5). Records park in place "
                        + "until it lands; declare dlqImmediately() or dlqOlderThan(...) for an export today.{}",
                setting, percentage, where, ceiling));
    }

    // ---------------------------------------------------------------- start

    /**
     * Validate, build the clients this definition needs, and run (F1).
     */
    public ConsumerHandle start() {
        return start(ClientRuntime.kafka());
    }

    /**
     * Validate and run against clients from the given runtime - which is how the sandbox runs a definition with no
     * broker, changing nothing else about it (R33, KTD9).
     */
    public ConsumerHandle start(ClientRuntime runtime) {
        refuseExportUntilItLands();
        ParallelConsumerOptions<byte[], byte[]> built = buildOptions(runtime);
        // The module, not the static factory: it is what owns this instance's PCMetrics, and registering the
        // route meters through it is what puts them in the user's own registry beside every engine meter and has
        // them swept by the same close (KTD8, and core's rule that collaborators are wired through the module).
        PCModule<byte[], byte[]> module = new PCModule<>(built);
        ParallelEoSStreamProcessor<byte[], byte[]> processor = new ParallelEoSStreamProcessor<>(built, module);

        ParkedSnapshots parkedSnapshots = new ParkedSnapshots(dispatcher.parkedRecords());
        FluentMeters meters = FluentMeters.registerFor(module.pcMetrics(), topics(), parkedSnapshots);
        dispatcher.meters(meters);
        ConsumerHandle handle = new ConsumerHandle(processor, dispatcher, routeTopicsByTopic(), closePath, meters,
                parkedSnapshots);
        dispatcher.instanceControl(handle);
        this.startedHandle = handle;

        // The facade's listener first, the user's chained after it (KTD2).
        processor.subscribe(subscriptionTopics(), dispatcher.rebalanceListener(usersRebalanceListener));
        // Before the poll, so the first control loop already carries the hook rather than the second.
        handle.startObserving();
        if (requiresProducer()) {
            processor.pollAndProduceMany(dispatcher::dispatch);
        } else {
            processor.poll(dispatcher::dispatchWithoutProducing);
        }
        // After the subscription, so a fake consumer's partitions can be assigned to a listener that now exists,
        // and with the handle, so a generator with a bound can close the instance when it reaches one (KTD9).
        runtime.started(handle);
        return handle;
    }

    /**
     * Every routed topic mapped to the whole route's topics, so that asking the handle about any one topic of a
     * set-declared route answers for the route rather than for that topic alone (R5, R28).
     */
    private Map<String, Set<String>> routeTopicsByTopic() {
        Map<String, Set<String>> byTopic = new LinkedHashMap<>();
        for (Map.Entry<String, RouteState> entry : routesByTopic.entrySet()) {
            byTopic.put(entry.getKey(), entry.getValue().topics());
        }
        return Collections.unmodifiableMap(byTopic);
    }

    /**
     * Closes the instance this definition started, if it started one - so a definition may itself be held in
     * try-with-resources.
     * <p>
     * <b>A definition that was never started closes nothing</b>, and says so at debug rather than throwing: writing
     * a definition is not starting one, and a block that returns before its {@code start()} must not fail on the
     * way out.
     */
    @Override
    public void close() {
        ConsumerHandle handle = this.startedHandle;
        if (handle == null) {
            log.debug("Nothing to close: this definition was never started");
            return;
        }
        handle.close();
    }

    /**
     * Refused <b>at start</b>, not at validation: a definition with a destination is still a definition that needs
     * a producer, which is what {@link #requiresProducer()} answers and what the client-construction tests read.
     * <p>
     * The wrapper parks a record that runs out of attempts and nothing sends it on yet - export is a re-dispatch on
     * a later pass, and that unit has not landed (KTD5). Starting anyway would make {@code dlqTo} a silent no-op,
     * which is the one outcome this definition refuses to produce: every other setting it cannot honour is refused
     * at definition time for the same reason. The refusal goes away with the export unit.
     */
    private void refuseExportUntilItLands() {
        for (RouteState route : routes) {
            String destination = route.afterRetries().destination();
            if (destination != null) {
                throw new IllegalArgumentException(msg("Topic {} declares the dead-letter destination {}, and export "
                                + "does not run in this release: a record that runs out of attempts parks in place, "
                                + "and nothing copies it on yet. Starting would make dlqTo a silent no-op. Drop the "
                                + "destination and let records park - they stay incomplete in the offset map, hold "
                                + "no worker, and offsets past them still commit (R11, R27).",
                        route.describeTopics(), destination));
            }
        }
    }

    /**
     * Run this listener on every rebalance, after the facade's own (KTD2). The facade's clears the attempt ledger
     * for partitions that are no longer ours, so a listener chained here always sees a consistent state; a throw
     * from here propagates exactly as it does on the classic API.
     */
    public ParallelConsumerDefinition rebalanceListener(ConsumerRebalanceListener listener) {
        this.usersRebalanceListener = Objects.requireNonNull(listener, "A rebalance listener must be supplied");
        return this;
    }

    /**
     * Validation, the client decisions and the options - everything {@link #start(ClientRuntime)} does before the
     * engine exists. Separate so that what the definition asks for can be asserted without running anything.
     */
    ParallelConsumerOptions<byte[], byte[]> buildOptions(ClientRuntime runtime) {
        Objects.requireNonNull(runtime, "A client runtime must be supplied");
        validate();
        if (started) {
            throw new IllegalStateException("This definition has already been started - define a second one to run a "
                    + "second instance");
        }
        started = true;

        this.dispatcher = new RouteDispatcher(routesByTopic, defaultRetryDelay, preBuiltConsumerDescription);

        options.commitMode(commitMode)
                .ordering(defaultOrdering)
                .maxConcurrency(totalAdmissionTarget())
                // Park rides this hook: the wrapper records what it meant by a throw immediately before making it,
                // and the engine calls this synchronously inside the failure path to find out (KTD4).
                .retryDelayProvider(context -> dispatcher.retryDelayFor(context));
        // The engine's own defaultMessageRetryDelay is deliberately left alone. It is deprecated, and it is only
        // reached when the provider above misbehaves - which the provider is written not to do, and which
        // EngineRetryDelayProviderContractTest pins. Setting it would make the fallback look intentional.

        if (!preBuiltConsumerSupplied) {
            options.consumer(runtime.consumer(this));
        }
        if (requiresProducer() && !preBuiltProducerSupplied) {
            Optional<Producer<byte[], byte[]>> supplied = runtime.producer(this);
            if (supplied.isPresent()) {
                options.producer(supplied.get());
            } else {
                options.producerConfig(producerProperties());
            }
        }
        return options.build();
    }

    /**
     * The engine's total admission is the sum of the routes' targets, because routes do not compete for one shared
     * limit (R23). On virtual threads that costs nothing; a platform-thread user sets a lower per-route target so
     * the sum fits the pool (KD6).
     */
    private int totalAdmissionTarget() {
        int total = 0;
        for (RouteState route : routes) {
            total += route.concurrency();
        }
        return total;
    }

    /**
     * The route-dispatching wrapper this definition runs on, once {@link #buildOptions} has built it. Visible for
     * the tests that drive the wrapper without an engine.
     */
    RouteDispatcher dispatcher() {
        return dispatcher;
    }

    private List<String> subscriptionTopics() {
        return new ArrayList<>(topics());
    }

    /**
     * The producer's own configuration: the connection properties, minus the keys that mean nothing to a producer,
     * plus the raw-bytes serialisers the facade requires. Handing this to Parallel Consumer rather than a finished
     * producer is what keeps producer recovery available (R1, astubbs#410).
     */
    private Map<String, Object> producerProperties() {
        Map<String, Object> config = new LinkedHashMap<>(properties);
        config.keySet().removeAll(CONSUMER_ONLY_PROPERTIES);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return config;
    }

    private String routedTopics() {
        return routesByTopic.keySet().toString();
    }

    // ---------------------------------------------------------------- DefinitionView

    @Override
    public Set<String> topics() {
        return Collections.unmodifiableSet(routesByTopic.keySet());
    }

    @Override
    public Collection<RouteView> routes() {
        return Collections.<RouteView>unmodifiableList(new ArrayList<RouteView>(routes));
    }

    @Override
    public RouteView route(String topic) {
        return routesByTopic.get(topic);
    }

    @Override
    public CommitMode commitMode() {
        return commitMode;
    }

    @Override
    public ProcessingOrder ordering() {
        return defaultOrdering;
    }

    @Override
    public boolean requiresProducer() {
        if (commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER) {
            return true;
        }
        for (RouteState route : routes) {
            if (route.producesRecords() || route.afterRetries().destination() != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Map<String, Object> connectionProperties() {
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public Map<String, Object> passThroughProperties() {
        Map<String, Object> passThrough = new LinkedHashMap<>(properties);
        passThrough.keySet().removeAll(FACADE_OWNED_PROPERTIES);
        return Collections.unmodifiableMap(passThrough);
    }

    @Override
    public String preBuiltConsumerDescription() {
        return preBuiltConsumerDescription;
    }

    // ---------------------------------------------------------------- defaults, read by a route resolving its own

    OptionalInt defaultRetryLimitValue() {
        return defaultRetryLimit;
    }

    Duration defaultRetryDelayValue() {
        return defaultRetryDelay;
    }

    int defaultConcurrencyValue() {
        return defaultConcurrency;
    }

    ProcessingOrder defaultOrderingValue() {
        return defaultOrdering;
    }

    AfterRetries defaultAfterRetriesValue() {
        return defaultAfterRetries;
    }

    CircuitBreakerPolicy defaultCircuitBreakerValue() {
        return defaultCircuitBreaker;
    }

    ParkObserver<?, ?> defaultParkObserverValue() {
        return defaultParkObserver;
    }

    @Override
    public String toString() {
        return "ParallelConsumerDefinition(topics=" + topics() + ", commitMode=" + commitMode + ", ordering="
                + defaultOrdering + ")";
    }
}
