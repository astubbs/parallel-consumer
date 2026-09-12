package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.Percent;
import bz.stub.parallelconsumer.internal.PCModule;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.annotation.InterfaceStability;

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

import static bz.stub.parallelconsumer.internal.utils.StringUtils.isBlank;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * A Parallel Consumer being defined: connection properties in, one typed route per topic, policy as data, and a
 * running instance out. Start it from {@link ParallelConsumer#connect(Properties)}.
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
 * Every setting on this class carries a {@code with} prefix, so the settings read as one family (KD16). An
 * instance-wide setting stops there - {@link #withCommitMode} - because the engine has one consumer, one commit
 * and one transaction, so those are properties of the clients rather than of the work. So are
 * {@link #withClosePath}, which is how this instance shuts down whoever asked it to, and {@link #withMetrics},
 * which is where its meters go. Everything else is a per-route value with an instance default, and those keep
 * {@code default} inside the name, after the prefix: {@link #withDefaultRetryLimit},
 * {@link #withDefaultConcurrency}, {@link #withDefaultAfterRetries}. A route that declares its own overrides its
 * copy and nobody else's (KD11, R6).
 * <p>
 * <b>The spellings those names replaced are gone, not deprecated</b> (KD15): this package has never been released,
 * so there is no caller outside this repository to keep compiling, and a deprecated alias would have claimed a
 * compatibility that was never at stake. The day the fluent API ships, that reverses - a replaced name then keeps
 * its old spelling as a deprecated delegate, and removing it becomes a release-gated decision.
 *
 * <h2>What this class is, and what it delegates</h2>
 * It is the fluent surface and the assembly: the setters, the route helpers, {@link #start} and the
 * {@link DefinitionView} answers. Three bodies of work that had grown inside it are now their own owners, each
 * reached only from here - {@link ConnectionProperties} cuts the one property bag three ways, {@link InstanceDefaults}
 * holds the per-route defaults a route resolves against, and {@link DefinitionRules} holds every refusal
 * {@link #validate()} makes. Nothing about the public surface moved: this class still answers every call it did.
 *
 * <h2>It is closeable, and so is the instance</h2>
 * {@link #start()} hands back a {@link ParallelConsumerInstance}, which is what the README holds in
 * try-with-resources. This
 * definition is {@link AutoCloseable} too, for a caller who would rather hold one thing than two: closing it closes
 * the instance it started, and closes nothing at all if it never started one.
 *
 * @see ParallelConsumer#connect(Properties)
 */
@Slf4j
@InterfaceStability.Unstable
public class ParallelConsumerDefinition implements DefinitionView, AutoCloseable {

    /**
     * The connection properties, copied at construction rather than held, and cut three ways by their own owner:
     * what this definition validates, what a route's formats are configured with, and what a producer is built
     * from. See {@link ConnectionProperties}, which holds the two lists that decide those cuts.
     */
    private final ConnectionProperties connection;

    /**
     * The options under construction. A pre-built client goes into this the moment it is supplied and is never held
     * in a field of this class, which is what keeps core's raw-client architecture rule intact (KTD3).
     */
    private final ParallelConsumerOptions.ParallelConsumerOptionsBuilder<byte[], byte[]> options =
            ParallelConsumerOptions.builder();

    /**
     * The routes in declaration order, which is the order validation walks them in - so a definition with two
     * mistakes reports the one the author wrote first. It is also what the admission targets are summed over (R23).
     */
    private final List<RouteState> routes = new ArrayList<>();

    /**
     * Every routed topic to its route: the index that refuses a topic a second time (R2), the table the dispatch
     * wrapper is built from, and the set the instance subscribes to. A route declared over several topics appears
     * once per topic and is the same object each time (R5).
     */
    private final Map<String, RouteState> routesByTopic = new LinkedHashMap<>();

    /**
     * Instance-wide, because the engine has one consumer and one commit (KD11). Asynchronous consumer commits
     * unless the definition says otherwise, which is the only mode that needs no producer of its own.
     */
    private CommitMode commitMode = CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;

    /**
     * The per-route settings' instance-wide defaults - the {@code default}-prefixed ones, and the only settings a
     * route resolves against. One object with one reader, {@link RouteState#resolveDefaults()}, rather than six
     * fields reached through six accessors (KD11, R6).
     */
    private final InstanceDefaults defaults = new InstanceDefaults();

    /**
     * The instance-wide export percentage as declared, held only so validation can refuse it by name: the trigger
     * it would drive reads an engine accessor that does not exist yet (KTD5). Boxed so that null means nothing was
     * declared - a zero would be a value somebody typed.
     */
    private Percent instancePayloadPercentage;

    /**
     * Whether the caller supplied a finished consumer, so that start does not ask the runtime for one. A flag
     * rather than the client: the client goes straight into the options builder and is never held in a field here,
     * which is what keeps core's raw-client architecture rule intact (KTD3).
     */
    private boolean preBuiltConsumerSupplied;

    /**
     * The deserialisers read off a supplied consumer at the moment it was supplied, for the message a raw-bytes
     * fault carries later (KTD3). Read here because a consumer's configuration is not recoverable from the running
     * client. Null when the definition builds its own consumer, which is what says the fault cannot arise.
     */
    private String preBuiltConsumerDescription;

    /**
     * Whether the caller supplied a finished producer. It also settles that this instance forgoes producer
     * recovery, which rebuilds from a configuration a finished producer no longer has (astubbs#410).
     */
    private boolean preBuiltProducerSupplied;

    /**
     * One definition starts one instance. Set by {@link #buildOptions} so a second start is refused before any
     * client is built - rather than after a second consumer has already joined the group.
     */
    private boolean started;

    /**
     * How this instance shuts down, whoever asked it to (R17, R24). Draining by default, which is what makes the
     * instance a graceful shutdown.
     */
    private ClosePath closePath = ClosePath.DRAIN_FIRST;

    /**
     * What a start does about a route naming a topic the cluster does not have (owner decision, 2026-09-11).
     * Refusing by default - see {@link MissingTopic#FAIL} for the silence it replaces.
     */
    private MissingTopic whenTopicMissing = MissingTopic.FAIL;

    /**
     * The instance {@link #start} produced, so that a definition held in try-with-resources closes the instance it
     * started. Null until it starts one.
     */
    private volatile ParallelConsumerInstance startedInstance;

    /**
     * The user's own rebalance listener, chained after the facade's (KTD2). Null when none was declared.
     */
    private ConsumerRebalanceListener usersRebalanceListener;

    /**
     * The route-dispatching wrapper. Built by {@link #buildOptions}, because the retry-delay provider registered on
     * the options is one of its methods, and reused by {@link #start(ClientRuntime)} - one owner of the route
     * table.
     */
    private RouteDispatcher dispatcher;

    /**
     * The same door as {@link ParallelConsumer#connect(Properties)}, which is how the documentation and the README
     * spell it: {@code connect} reads as the verb Kafka's own client uses for taking configuration now and reaching
     * the cluster later, where a constructor reads as an object being built.
     */
    public ParallelConsumerDefinition(Properties connectionProperties) {
        this.connection = ConnectionProperties.copyOf(connectionProperties);
    }

    // ---------------------------------------------------------------- instance-wide settings

    /**
     * Instance-wide, because the engine has one consumer: one offset commit per group, and under the transactional
     * mode one producer's transaction around it (KD11).
     */
    public ParallelConsumerDefinition withCommitMode(CommitMode commitMode) {
        this.commitMode = Objects.requireNonNull(commitMode, "A commit mode must be supplied");
        return this;
    }

    /**
     * How this instance shuts down: whether the records already fetched are processed first (R17, R24).
     * <p>
     * Instance-wide because the stop outcome closes the instance from a thread of its own, with nobody there to
     * pass an argument - and because an instance having two answers to "what happens to the backlog", one for its
     * caller and one for itself, is a difference nobody would predict correctly (KTD6).
     */
    public ParallelConsumerDefinition withClosePath(ClosePath path) {
        this.closePath = Objects.requireNonNull(path, "A close path must be supplied");
        return this;
    }

    /**
     * What the start does about a route naming a topic the cluster does not have: refuse it, create it, or say so
     * and carry on. {@link MissingTopic#FAIL} unless the definition says otherwise, and
     * {@link MissingTopic} carries why.
     * <p>
     * <b>Instance-wide rather than per route with an instance default</b>, which is where KD11 would have put it
     * and where {@code docs/refactoring.md} said it would go. Directed by the owner on 2026-09-11, and the reason
     * it is the better shape: this is one question asked once of one cluster, before any route is running, and two
     * routes of one definition disagreeing about whether a missing topic is fatal describes an instance that is
     * half-started - which is not a state this API has. A route that may legitimately be absent is a definition
     * that declares {@link MissingTopic#IGNORE} and reads its route's parked view, not a per-route flag.
     */
    public ParallelConsumerDefinition withMissingTopicPolicy(MissingTopic policy) {
        this.whenTopicMissing = Objects.requireNonNull(policy, "A missing-topic policy must be supplied");
        return this;
    }

    /**
     * Where this instance's meters go (R19). Without one, Parallel Consumer registers into a no-op registry and
     * nothing is published.
     * <p>
     * The fluent API's own meters - what each route did with its records - register here alongside every engine
     * meter, under the {@code routes} subsystem, and are removed when the instance closes. The live parked figures
     * are the engine's, gauged per partition under the {@code partitions} subsystem.
     */
    public ParallelConsumerDefinition withMetrics(MeterRegistry registry) {
        Objects.requireNonNull(registry, "A meter registry must be supplied");
        options.meterRegistry(registry);
        return this;
    }

    // ---------------------------------------------------------------- per-route defaults

    /**
     * The ordering guarantee every route copies. Per-route ordering needs a change at the engine's shard-key seam
     * and is a later milestone, so in this version it is the instance default and nothing else (R6).
     */
    public ParallelConsumerDefinition withDefaultOrdering(ProcessingOrder ordering) {
        defaults.ordering(Objects.requireNonNull(ordering, "An ordering must be supplied"));
        return this;
    }

    /**
     * The admission target every route copies: how many of its records may be in flight at once. Routes do not
     * compete for one shared limit, so the engine's total admission is the sum of the routes' targets (R23, KD6).
     */
    public ParallelConsumerDefinition withDefaultConcurrency(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException(msg("withDefaultConcurrency ({}) must be at least one - it is each "
                    + "route's admission target", limit));
        }
        defaults.concurrency(limit);
        return this;
    }

    /**
     * How many attempts after the first every route allows before its records park (R10).
     */
    public ParallelConsumerDefinition withDefaultRetryLimit(int attempts) {
        if (attempts < 0) {
            throw new IllegalArgumentException(msg("withDefaultRetryLimit ({}) cannot be negative - it counts the "
                    + "attempts after the first; use withDefaultRetryForever() to ask for unbounded retries", attempts));
        }
        defaults.retryLimit(OptionalInt.of(attempts));
        return this;
    }

    /**
     * Retry forever, as the classic API always has. Opt-in on purpose (R10).
     */
    public ParallelConsumerDefinition withDefaultRetryForever() {
        defaults.retryLimit(OptionalInt.empty());
        return this;
    }

    /**
     * How long a failed record waits before its next attempt, on every route that declares no delay of its own.
     */
    public ParallelConsumerDefinition withDefaultRetryDelay(Duration delay) {
        Objects.requireNonNull(delay, "A retry delay must be supplied");
        if (delay.isNegative()) {
            throw new IllegalArgumentException(msg("withDefaultRetryDelay ({}) cannot be negative", delay));
        }
        defaults.retryDelay(delay);
        return this;
    }

    /**
     * What happens to a record that runs out of attempts, on every route that declares nothing of its own (R27).
     */
    public ParallelConsumerDefinition withDefaultAfterRetries(AfterRetries policy) {
        defaults.afterRetries(Objects.requireNonNull(policy, "An after-retries policy must be supplied"));
        return this;
    }

    /**
     * The park observer every route copies, told once when one of its records parks (R16).
     * <p>
     * Typed {@code Object} on both sides because it is one observer over routes whose consumed types differ: an
     * instance default cannot know them. Declare it on a route with {@link Route#onParked} to see that route's own
     * types.
     */
    public ParallelConsumerDefinition withDefaultOnParked(ParkObserver<Object, Object> observer) {
        defaults.parkObserver(Objects.requireNonNull(observer, "A park observer must be supplied"));
        return this;
    }

    /**
     * How full the offset payload may get before the oldest parked records are exported to the dead-letter topic to
     * make room. This is the instance default a route's own park policy may override (R6, R27).
     *
     * <h2>What the offset payload is</h2>
     * Committing progress as a single number - "everything up to offset N is done" - cannot describe a partition
     * where record 100 is parked while records 101 to 400 have all succeeded. So this library commits two things:
     * the ordinary committed offset, held back at the oldest record still incomplete, and beside it, in the small
     * metadata field every Kafka commit carries, a compact encoded map marking which records past that offset are
     * still incomplete. That map is the <em>offset payload</em>. It is what lets processing run on ahead of a record
     * that is stuck, without either losing the work done past it or re-delivering all of it after a restart.
     * <p>
     * Kafka caps how large that metadata field may be, and every parked record is one more thing the map must
     * carry - a parked record is precisely a record the committed offset cannot move past. So the more records park
     * on a partition, the closer its map comes to the cap, and a partition that reaches the cap can take on no new
     * work at all. This setting is the release valve: at the given percentage of the cap, the partition's
     * oldest-parked records are copied to the declared dead-letter topic and completed, which shortens the map
     * again. A higher percentage leaves records parked for longer and leaves less headroom; a lower one exports
     * sooner and keeps more.
     * <p>
     * <b>Refused in this version</b>, at validation, along with a percentage on any route: the accessor it reads -
     * a partition's encoded payload length - does not exist in the engine yet, so an explicit percentage would be a
     * setting that never fires (KTD5). The default of {@link AfterRetries#MAX_PAYLOAD_PERCENTAGE} applies once that
     * accessor lands.
     *
     * @param percentage a percentage of the cap - {@code percentOf(70)} is seventy percent of it - at most
     *                   {@link AfterRetries#MAX_PAYLOAD_PERCENTAGE}
     */
    public ParallelConsumerDefinition withDlqWhenOffsetPayloadReaches(Percent percentage) {
        this.instancePayloadPercentage = Objects.requireNonNull(percentage, "An export percentage must be supplied");
        return this;
    }

    /**
     * The same setting for a caller who would rather write the number than the type: {@code 70} is seventy percent of
     * the cap, the unit {@link Percent} spells out. It builds one, so a value that is not a percentage is refused
     * here and now rather than being stored and explained later as something else.
     *
     * @param percentage a percentage of the cap out of a hundred, not a fraction of one
     * @see #withDlqWhenOffsetPayloadReaches(Percent)
     */
    public ParallelConsumerDefinition withDlqWhenOffsetPayloadReaches(double percentage) {
        return withDlqWhenOffsetPayloadReaches(Percent.percentOf(percentage));
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
    public ParallelConsumerDefinition withConsumer(Consumer<byte[], byte[]> consumer) {
        Objects.requireNonNull(consumer, "A consumer must be supplied");
        this.preBuiltConsumerDescription = RawBytesConsumerFaultException.describe(consumer);
        this.preBuiltConsumerSupplied = true;
        options.consumer(consumer);
        return this;
    }

    /**
     * Run against a producer you built yourself. Sugar for the Java binding, as {@link #withConsumer} is.
     * <p>
     * <b>A supplied producer forgoes producer recovery</b> (astubbs#410): recovery rebuilds the producer from its
     * configuration, and an instance handed a finished producer has no configuration to rebuild from. Leave this out
     * and the definition's properties build one that can recover. Under the transactional commit mode this also
     * forgoes export (R1, R14).
     */
    public ParallelConsumerDefinition withProducer(Producer<byte[], byte[]> producer) {
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
            if (isBlank(topic)) {
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
     * Every refusal this definition makes, before anything is built (AE7). Run automatically by {@link #start};
     * call it directly to fail early.
     * <p>
     * The rules themselves live in {@link DefinitionRules}, built fresh here over the definition as it stands
     * right now - a definition is mutable until it starts, so a snapshot taken any earlier would check a
     * definition nobody wrote.
     *
     * @throws IllegalArgumentException naming the offending topic or setting
     */
    public void validate() {
        new DefinitionRules(routes, routesByTopic, connection, commitMode, defaults, instancePayloadPercentage,
                preBuiltProducerSupplied).validate();
    }

    // ---------------------------------------------------------------- start

    /**
     * Validate, build the clients this definition needs, and run (F1).
     */
    public ParallelConsumerInstance start() {
        return start(ClientRuntime.kafka());
    }

    /**
     * Validate and run against clients from the given runtime - which is how the sandbox runs a definition with no
     * broker, changing nothing else about it (R33, KTD9).
     */
    public ParallelConsumerInstance start(ClientRuntime runtime) {
        refuseExportUntilItLands();
        ParallelConsumerOptions<byte[], byte[]> built = buildOptions(runtime);
        // The module, not the static factory: it is what owns this instance's PCMetrics, and registering the
        // route meters through it is what puts them in the user's own registry beside every engine meter and has
        // them swept by the same close (KTD8, and core's rule that collaborators are wired through the module).
        PCModule<byte[], byte[]> module = new PCModule<>(built);
        ParallelEoSStreamProcessor<byte[], byte[]> processor = new ParallelEoSStreamProcessor<>(built, module);

        // Outcome counters only, one per routed topic per outcome: the live parked figures are the engine's, gauged
        // by the partition that owns the records (KTD8).
        FluentMeters meters = FluentMeters.registerFor(module.pcMetrics(), topics());
        dispatcher.meters(meters);
        ParallelConsumerInstance instance = new ParallelConsumerInstance(processor, dispatcher,
                routeTopicsByTopic(), closePath, meters);
        // The wrapper's two callbacks into this instance are wired by startObserving() below, with the parked view
        // and the loop-end hook - before anything polls, and so the instance need not publish them itself.
        this.startedInstance = instance;

        // The facade's own listener, with the user's chained behind it when the definition declared one (KTD2).
        // The facade still keeps no per-assignment state to clear on a revocation - the attempt count and the
        // parked set are both the engine's, and the engine already drops a revoked partition's records from both
        // (KTD14). What it does need is to know that a rebalance has LANDED, because an assignment that is empty
        // because nothing was given to this member and one that is empty because nothing has happened yet are the
        // same set, and only the first is worth warning about.
        processor.subscribe(subscriptionTopics(), instance.rebalanceListener(usersRebalanceListener));
        // Before the poll, so the first control loop already carries the hook rather than the second.
        instance.startObserving();
        if (requiresProducer()) {
            processor.pollAndProduceMany(dispatcher::dispatch);
        } else {
            processor.poll(dispatcher::dispatchWithoutProducing);
        }
        // After the subscription, so a fake consumer's partitions can be assigned to a listener that now exists,
        // and with the instance, so a generator with a bound can close it when it reaches one (KTD9).
        runtime.started(instance);
        return instance;
    }

    /**
     * Every routed topic mapped to the whole route's topics, so that asking the instance about any one topic of a
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
        ParallelConsumerInstance instance = this.startedInstance;
        if (instance == null) {
            log.debug("Nothing to close: this definition was never started");
            return;
        }
        instance.close();
    }

    /**
     * Refused <b>at start</b>, not at validation: a definition with a destination is still a definition that needs
     * a producer, which is what {@link #requiresProducer()} answers and what the client-construction tests read.
     * <p>
     * The wrapper parks a record that runs out of attempts and nothing sends it on yet - export is a re-dispatch on
     * a later pass, and that unit has not landed (KTD5). Starting anyway would make the destination a silent no-op,
     * which is the one outcome this definition refuses to produce: every other setting it cannot honour is refused
     * at definition time for the same reason. The refusal goes away with the export unit.
     * <p>
     * It covers every way of naming a destination - {@link AfterRetries#dlq(String)}, whose whole reaction is the
     * copy, as well as {@link AfterRetries#dlqTo(String)} on a parking policy - because the missing piece is the
     * same one in both cases, so the message names the destination rather than the call that carried it.
     */
    private void refuseExportUntilItLands() {
        for (RouteState route : routes) {
            String destination = route.afterRetries().destination();
            if (destination != null) {
                throw new IllegalArgumentException(msg("Topic {} declares the dead-letter destination {}, and export "
                                + "does not run in this release: a record that runs out of attempts parks in place, "
                                + "and nothing copies it on yet. Starting would make the destination a silent "
                                + "no-op. Drop it and let records park - they stay incomplete in the offset map, "
                                + "hold no worker, and offsets past them still commit (R11, R27).",
                        route.describeTopics(), destination));
            }
        }
    }

    /**
     * Run this listener on every rebalance (KTD2). It is handed to the engine as the classic API's own listener is,
     * so it sees the same callbacks in the same order and a throw from it propagates exactly as it does there.
     */
    public ParallelConsumerDefinition withRebalanceListener(ConsumerRebalanceListener listener) {
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

        // Before any client of the instance exists, and after the definition has been refused for its own faults:
        // a topic that is not there is a fault of the definition too, and the start that carries on regardless is
        // the one this replaces (owner decision, 2026-09-11).
        TopicExistenceCheck.enforce(whenTopicMissing, topics(), runtime, this);

        this.dispatcher = new RouteDispatcher(routesByTopic, defaults.retryDelay(), preBuiltConsumerDescription);

        options.commitMode(commitMode)
                .ordering(defaults.ordering())
                .maxConcurrency(totalAdmissionTarget())
                // One delay per route, answered from the topic and the record's attempt count (R6). What a throw
                // MEANT - a park, a hand-back that is not an attempt - rides on the exception instead, so this
                // stays a pure function and there is no note for it to find (KTD14). A park CYCLE's own delay is
                // the exception: the engine only reads a carried delay off a PCRetriableException, so a plain
                // throw under a park policy has to be answered here - see RouteDispatcher#retryDelayFor.
                .retryDelayProvider(context ->
                        dispatcher.retryDelayFor(context.topic(), context.getNumberOfFailedAttempts()));
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
                options.producerConfig(connection.forProducer());
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

    /**
     * What the instance subscribes to: exactly the union of the routes' topics, never more. That equality is what
     * makes a record arriving with no route an invariant break in the dispatch wrapper rather than a user error,
     * and it is why a pattern subscription is refused (R2, KTD2). A list, because that is what the engine takes.
     */
    private List<String> subscriptionTopics() {
        return new ArrayList<>(topics());
    }

    // ---------------------------------------------------------------- DefinitionView

    /**
     * The union of the routes' topics, in declaration order and read-only: a reader of a definition may see what
     * is subscribed to and may not add to it, because a topic here with no route behind it is the one thing the
     * dispatch wrapper treats as impossible.
     */
    @Override
    public Set<String> topics() {
        return Collections.unmodifiableSet(routesByTopic.keySet());
    }

    /**
     * Every route once each, however many topics it was declared over. A copy rather than a wrapper, so a route
     * declared after this call does not appear in a collection a reader is still holding.
     */
    @Override
    public Collection<RouteView> routes() {
        return Collections.<RouteView>unmodifiableList(new ArrayList<RouteView>(routes));
    }

    /**
     * Null for an unrouted topic, as the view's contract says: a reader asks this to find out <em>whether</em> a
     * topic is routed, so a refusal would make the ordinary answer an exception. The instance and the dispatch
     * wrapper are the ones that refuse, because there the question is about a topic the caller believes it owns.
     */
    @Override
    public RouteView route(String topic) {
        return routesByTopic.get(topic);
    }

    /**
     * The mode as declared, before any client exists - which is what lets the runtime seam decide whether to build
     * a transactional producer at all.
     */
    @Override
    public CommitMode commitMode() {
        return commitMode;
    }

    /**
     * The instance default, and in this version every route's ordering, so a reader need not ask each route (R6).
     */
    @Override
    public ProcessingOrder ordering() {
        return defaults.ordering();
    }

    /**
     * Derived from the routes each time rather than tracked as a flag, so it stays true however late a route that
     * produces or exports is declared. It is also read at start to choose the produce-many arm over the plain poll
     * one, so the two decisions cannot disagree (R4, KTD2).
     */
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

    /**
     * The copy this definition took, read-only: a runtime seam builds its clients from these, and a seam that
     * could edit them would be changing a definition that has already been validated.
     */
    @Override
    public Map<String, Object> connectionProperties() {
        return connection.all();
    }

    /**
     * Answered by {@link ConnectionProperties#forFormats()}, which holds the one list of what the facade owns - so
     * the same list decides both what a route's formats are configured with and what they are not: a
     * schema-registry URL reaches them, the bootstrap servers and the client serialisers do not (KTD7).
     */
    @Override
    public Map<String, Object> formatProperties() {
        return connection.forFormats();
    }

    /**
     * Null when the definition builds its own consumer, which is more than an absent string: it is what tells a
     * reader that a raw-bytes fault is unreachable on this instance at all (KTD3).
     */
    @Override
    public String preBuiltConsumerDescription() {
        return preBuiltConsumerDescription;
    }

    // ---------------------------------------------------------------- defaults, read by a route resolving its own

    /**
     * The instance-wide defaults a route falls back to, handed out whole rather than one accessor per setting -
     * {@link RouteState#resolveDefaults()} is the only reader, and it wants all of them.
     */
    InstanceDefaults defaults() {
        return defaults;
    }

    /**
     * What this definition looks like in a log line: the topics it claims and the two instance-wide settings that
     * decide which clients it opens. Deliberately not the policy - that is per route, and a definition's own
     * rendering should not read as though it were instance-wide.
     */
    @Override
    public String toString() {
        return "ParallelConsumerDefinition(topics=" + topics() + ", commitMode=" + commitMode + ", ordering="
                + defaults.ordering() + ")";
    }
}
