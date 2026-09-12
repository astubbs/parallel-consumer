package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.ClientRuntime;
import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.DefinitionView;
import bz.stub.parallelconsumer.fluent.Format;
import bz.stub.parallelconsumer.fluent.ParkedRecord;
import bz.stub.parallelconsumer.fluent.RouteView;
import bz.stub.parallelconsumer.fluent.StopRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;

/**
 * Runs a definition with no broker, against records you publish or records it makes up.
 *
 * <h2>The primary shape: pipe, settle, assert</h2>
 * A test knows its own data, so it supplies it and then waits for the instance to finish with it:
 * <pre>{@code
 * ParallelConsumerDefinition pc = ParallelConsumer.connect(props);
 * pc.json("orders", Order.class)
 *         .process(ctx -> { inventory.reserve(ctx.value()); return Outcome.succeeded(); });
 *
 * Sandbox sandbox = Sandbox.builder().handPublished().build();
 * try (ParallelConsumerInstance instance = pc.start(sandbox)) {   // against a broker, this line reads pc.start()
 *     sandbox.pipe("orders", "cust-1", new Order("o-1"));
 *     sandbox.pipe("orders", "cust-1", new Order("o-2"));
 *     sandbox.awaitSettled();
 *     assertThat(inventory.reserved()).hasSize(2);
 * }
 * }</pre>
 * {@link #pipe(String, Object, Object)} encodes with the route's own serialiser and hands the record to the
 * mock consumer from <em>your</em> thread; {@link #awaitSettled()} blocks until every record published so far is
 * accounted for - completed, or parked - and then the try-with-resources closes the instance.
 * <p>
 * <b>Why there is a settle at all.</b> Parallel Consumer is concurrent by construction: a published record is
 * dispatched on a worker thread and committed on the control thread, so nothing about {@code publish} returning
 * says the function has run. The broker-free drivers of the stream-processing libraries users compare us with
 * process a piped record synchronously, because those engines are single-threaded, and so they need no such call.
 * This one is the price of the thing being exercised being the real engine.
 *
 * <h2>Per-topic objects, on both sides</h2>
 * {@link #createInputTopic(String)} and {@link #createOutputTopic(String)} are the same two shapes the broker-free
 * drivers of the stream-processing libraries users compare us with hand out (KTD16): the first carries
 * {@code pipeInput}, for a test that pipes into one topic repeatedly rather than naming it on every line, and the
 * second carries the read verbs - {@code readValue}, {@code readRecord}, {@code readValuesToList},
 * {@code readRecordsToList}, {@code readKeyValuesToMap}, {@code queueSize} and {@code isEmpty} - over what the
 * instance produced. {@link SandboxOutputTopic} owns the two contracts a caller has to know: <b>reading
 * consumes</b>, and <b>every read refuses until the run has settled</b>.
 *
 * <h2>The convenience on top: a driver that publishes for you</h2>
 * For a soak or a demo, where the point is volume rather than particular records, the module publishes on a
 * thread of its own until a {@link Bound} is reached, then settles and closes:
 * <pre>{@code
 * Sandbox sandbox = Sandbox.builder()
 *         .perSecond(50)
 *         .bound(Bound.after(Duration.ofSeconds(10)))
 *         .build();
 * try (ParallelConsumerInstance instance = pc.start(sandbox)) {
 *     sandbox.awaitBound(Duration.ofSeconds(30));
 * }
 * }</pre>
 * This is the default, and {@link Builder#handPublished()} is what turns it off. The close is the driver's on this
 * path for a reason a caller does not have to care about but which is the whole shape of the class: a close cannot
 * run from inside the engine it closes (KTD6), and the driver's thread is the only non-engine thread the module
 * owns. When <em>you</em> publish, your thread is already outside the engine, so you do the settle and the close
 * yourself and the constraint is not there.
 *
 * <h2>The definition does not change; the start call does</h2>
 * Everything above the {@code start} line in either example is the definition a production instance runs. That is
 * the whole claim of the sandbox (R33): what it exercises is the real facade over the real engine, with the
 * clients replaced - not a simulation of either.
 *
 * <h2>What the driver publishes, and where that comes from</h2>
 * One record per source topic per tick, at the declared rate, encoded with that route's own serialiser. <b>What
 * is in the record is yours</b>: {@link Builder#feeding(String, java.util.function.LongFunction)} takes a function
 * from a record's index to its value, per topic. This class knows how to pace and stop; it has no idea what an
 * order looks like, and an artefact that makes realistic fake objects is simply one such function.
 * <p>
 * Keys come from a small pool of strings unless a topic declares its own with
 * {@link Builder#feedingKeys(String, java.util.function.LongFunction)} - they repeat, which is what gives key
 * ordering something to order.
 *
 * <h2>What it refuses, and when</h2>
 * At {@code start}, before a record exists, and both refusals name the topic: a route whose value or key format
 * cannot <em>write</em> - a hand-written deserialiser with no serialiser beside it, so there would be nothing to
 * encode with, whoever supplied the value - and, on a driven sandbox only, a routed topic that nothing has said
 * how to fill.
 *
 * <h2>The classic API too</h2>
 * {@link #classic} hands the same mock consumer, producer and driver to an options builder, so an existing
 * classic-API application or example runs broker-free with its start call changed and nothing else (R33, AE26).
 * It has both shapes as well - {@link ClassicSandbox#pipe} and {@link ClassicSandbox#awaitSettled()}, or
 * {@link ClassicSandbox#startDriving}.
 *
 * @see Bound
 * @see ClassicSandbox
 */
@Slf4j
@InterfaceStability.Unstable
public final class Sandbox implements ClientRuntime, AutoCloseable {

    /**
     * The declared rate, per source topic. Copied from the builder rather than read back through it, so a builder
     * reused after {@code build()} cannot change a running sandbox.
     */
    private final double perSecond;

    /**
     * When to stop - see {@link Bound}. Read by {@link #awaitBound(Duration)} to refuse an unbounded run, and
     * handed to the driver to be asked per record.
     */
    private final Bound bound;

    /**
     * Partitions per source topic, which is what makes key ordering observable - with one partition every key
     * lands in the same place whatever the hash said.
     */
    private final int partitionsPerTopic;

    /**
     * How many distinct keys the driver's feeds draw from, so that keys repeat and a shard has more than one
     * record to order.
     */
    private final int keyCardinality;

    /**
     * The run's seed. Logged when the run starts, because it is the one number a reader needs to reproduce what
     * they just saw, and handed to the hydration, which is the only thing here with anything to be random about.
     */
    private final long seed;

    /**
     * Value types declared by hand for topics whose route cannot name one. The escape hatch for a route with a
     * custom serde, so that "the hydration cannot fill a value for this topic" is a fixable state rather than a
     * wall. It says nothing about a topic given its own value function, which needs no type at all.
     */
    private final Map<String, Class<?>> declaredTypes;

    /**
     * What the driver publishes as the value on each topic, for the topics whose caller said. A topic that did
     * not gets the hydration, which fills the route's declared type with realistic random data.
     * <p>
     * <b>The driver knows how to pace and stop; it does not know what a record contains.</b> Keeping that a
     * function rather than a hard-wired filler is what lets an application publish its own domain objects through
     * the same driver - and it is why the hydration below is a default rather than the only answer.
     */
    private final Map<String, LongFunction<Object>> values;

    /**
     * What the driver publishes as the key on each topic, for the topics whose caller said. A topic that did not
     * gets a key of the route's own declared key type, drawn from a pool of {@link #keyCardinality} so that keys
     * repeat and a shard has something to order.
     */
    private final Map<String, LongFunction<Object>> keys;

    /**
     * Whether the caller publishes rather than the driver. True means {@link #started(ParallelConsumerInstance)}
     * assigns the partitions and stops there, so the only records this sandbox holds are the ones
     * {@link #pipe} was given.
     * <p>
     * An opt-out rather than the default, even though publishing by hand is the primary shape, because the driver
     * was here first and a demo that declared a rate and a bound must go on running: a sandbox that silently
     * stopped publishing would present as a definition whose routes never fire.
     */
    private final boolean handPublished;

    /**
     * The instance's producer, built up front rather than on demand because it is also where the transactional
     * commit mode's offsets go - so the consumer's wait has to be able to read it whether or not the definition
     * produces anything. Byte serialisers on both sides: the engine below the fluent facade produces raw bytes,
     * already encoded by the route.
     */
    private final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

    /**
     * Whether this sandbox has settled since the last record the caller piped in - false at the start, true after a
     * settle or a reached bound, false again on the next {@link #pipe}. It is what every read on a
     * {@link SandboxOutputTopic} checks first, because a read taken while work is in flight comes back empty and an
     * empty read is indistinguishable from a definition that produced nothing.
     * <p>
     * Atomic because the flag is written from whichever thread piped or waited and read from whichever thread
     * asserts, and those are not required to be the same one.
     */
    private final AtomicBoolean settled = new AtomicBoolean();

    // Null until the definition is started - which is the whole shape of this class: it is asked for clients,
    // then told the instance is running. NullAway reads a field a constructor does not set as a fault, and the
    // suppression is its own marker for a field initialised later rather than a new dependency on an annotation.
    @SuppressWarnings("NullAway.Init")
    private SandboxConsumer<byte[], byte[]> consumer;

    /**
     * The definition this sandbox was handed, kept from whichever client call came first: it is what this class
     * reads to learn the topics, the types and the serialisers.
     */
    @SuppressWarnings("NullAway.Init")
    private DefinitionView definition;

    /**
     * The driver, null until the instance starts with one - see {@link #started(ParallelConsumerInstance)}. Every
     * method that touches it
     * either null-checks it or refuses, because "this sandbox has not been started" is a better answer than a
     * null pointer.
     */
    @SuppressWarnings("NullAway.Init")
    private RecordDriver driver;

    /**
     * The running instance, from {@link #started(ParallelConsumerInstance)}. Held for two things the
     * caller-published shape needs and the driver got for free: something for {@link #close()} to close, and
     * somewhere for {@link #awaitSettled()} to look when the wait ends with records still outstanding.
     */
    @SuppressWarnings("NullAway.Init")
    private ParallelConsumerInstance instance;

    /**
     * One per routed topic, built at {@code start} from the route that claims it: what turns a key and a value
     * into an encoded record on a partition. Shared by {@link #pipe} and by the driver's own feeds, so a
     * hand-published record and a driven one reach the engine by exactly the same path.
     */
    private Map<String, RoutePublisher> publishers = Collections.emptyMap();

    /**
     * Private: a sandbox is built through {@link #builder()}, which is what documents the defaults.
     */
    private Sandbox(Builder builder) {
        this.perSecond = builder.perSecond;
        this.bound = builder.bound;
        this.partitionsPerTopic = builder.partitionsPerTopic;
        this.keyCardinality = builder.keyCardinality;
        this.seed = builder.seed;
        this.declaredTypes = new LinkedHashMap<>(builder.declaredTypes);
        this.values = new LinkedHashMap<>(builder.values);
        this.keys = new LinkedHashMap<>(builder.keys);
        this.handPublished = builder.handPublished;
    }

    /**
     * The one way to build a sandbox. Every setting is optional - see {@link Builder} for what the defaults are
     * and why they are those.
     */
    public static Builder builder() {
        return new Builder();
    }

    // ---------------------------------------------------------------- ClientRuntime

    /**
     * The mock consumer, with every route's topic seeded and nothing assigned yet. Every definition-time refusal
     * this class makes happens here, which is before the engine exists and long before a record does.
     */
    @Override
    public Consumer<byte[], byte[]> consumer(DefinitionView view) {
        this.definition = view;
        refuseRoutesTheDriverCannotFeed(view);
        this.consumer = new SandboxConsumer<>(view.topics(), partitionsPerTopic);
        this.publishers = routePublishers(view);
        // Under the transactional commit mode the offsets go to the broker through the producer and never reach
        // the consumer at all, so the bound's wait has to be able to see them there too.
        consumer.alsoCountingCommitsThrough(producer);
        return consumer;
    }

    /**
     * The mock producer. Handed over rather than declined, because there is no broker for Parallel Consumer to
     * build one against - and because a fake has no producer recovery to forgo by being an instance (R1).
     * <p>
     * Transactions need nothing extra here: {@code MockProducer} can act as either, and Parallel Consumer's
     * producer wrapper reads the definition's commit mode to decide, then calls {@code initTransactions} itself.
     * {@link MockProducer#transactionInitialized()} is therefore the honest test of whether a definition started
     * transactional.
     */
    @Override
    public Optional<Producer<byte[], byte[]>> producer(DefinitionView view) {
        this.definition = view;
        log.debug("Sandbox producer requested for {} under commit mode {}", view.topics(), view.commitMode());
        return Optional.of(producer);
    }

    /**
     * Assign the partitions and, unless the caller is publishing, start the driver - the moment neither client
     * factory method can give us: the engine has now subscribed, so there is a rebalance listener to assign to,
     * and the instance exists, so the bound has something to close.
     */
    @Override
    public void started(ParallelConsumerInstance instance) {
        Objects.requireNonNull(instance, "A instance must be supplied");
        if (consumer == null) {
            throw new IllegalStateException("This sandbox was started without being asked for a consumer - a "
                    + "definition that supplies its own consumer with consumer(...) cannot also be run in the "
                    + "sandbox, because the sandbox IS the consumer");
        }
        this.instance = instance;
        consumer.assignAfterSeeding();
        // A parked record is a terminal outcome, and its partition never commits past it - so without this the
        // bound's wait would spend its whole budget on a definition that parks by design, which the README's own
        // quickstart does (astubbs#504). The instance is the only thing that knows what is parked, and this is the
        // first moment it exists.
        consumer.countingParkedRecordsWith(() -> parkedCountsByPartition(instance));
        if (handPublished) {
            log.info("Sandbox ready, publishing by hand: {} - publish(topic, key, value), then awaitSettled()",
                    definition.topics());
            return;
        }
        driver = new RecordDriver(fluentFeeds(), perSecond, bound, () -> {
            try {
                // The bound stops the driver; the engine still has to finish and commit what it was already
                // given. A drain-first close does not do that for us - see
                // SandboxConsumer#awaitEveryPublishedRecordCommitted for what it does instead, and why a committed
                // offset or a park is what means a record is done with.
                consumer.awaitEveryPublishedRecordCommitted();
            } finally {
                // Closed either way: a instance left open outlives whatever made it, and the wait's own refusal
                // still reaches the caller through the driver's recorded failure.
                instance.close();
            }
        });
        log.info("Sandbox driving: {} at {}/s per topic, seed {}, {}",
                definition.topics(), perSecond, seed, bound);
        driver.start();
    }

    // ---------------------------------------------------------------- classic API

    /**
     * The classic API's entry (R33, AE26): a typed mock consumer and a driver behind it, for an options
     * builder rather than a definition.
     * <p>
     * The classic API has no runtime seam - it takes a finished consumer - so the wiring is explicit rather than
     * one call: build the options with {@link ClassicSandbox#consumer()}, subscribe, poll, then either
     * {@link ClassicSandbox#pipe} or {@link ClassicSandbox#startDriving}. Note that no encoding happens on
     * this path at all: the mock consumer holds records of the user's own types, so objects go in as they are.
     *
     * @param keyType   the classic instance's key type, drawn from a small pool so keys repeat - and the witness
     *                  that fixes {@code K}, since a classic application's types live in its options builder's
     *                  generics and erasure has taken them by the time anything here could look
     * @param valueType the classic instance's value type, filled with realistic random data unless the caller
     *                  supplies its own value function
     */
    public <K, V> ClassicSandbox<K, V> classic(Class<K> keyType, Class<V> valueType, String... topics) {
        return classic(keyType, valueType, Arrays.asList(topics));
    }

    /**
     * @see #classic(Class, Class, String...)
     */
    public <K, V> ClassicSandbox<K, V> classic(Class<K> keyType, Class<V> valueType, Collection<String> topics) {
        Objects.requireNonNull(keyType, "A key type must be supplied");
        Objects.requireNonNull(valueType, "A value type must be supplied");
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("A classic sandbox needs at least one topic to publish into");
        }
        return new ClassicSandbox<>(keyType, valueType, topics, partitionsPerTopic, perSecond, bound, seed,
                keyCardinality);
    }

    // ---------------------------------------------------------------- publish, settle, assert

    /**
     * Pipes one record into a routed topic, from the calling thread, encoded with that route's own serialisers
     * - the front door for a test that knows its own data.
     *
     * <h2>What it does with what you give it</h2>
     * The key and the value are serialised with the formats the route declared, exactly as a driven record is, so
     * what the engine polls is what that route's deserialiser will be asked to read back (KTD2). The partition is
     * the encoded key's hash modulo this sandbox's partition count, so one key lands on one partition and key
     * ordering means here what it means against a broker.
     * <p>
     * No serialiser argument, unlike the broker-free drivers of the stream-processing libraries users compare us
     * with: the route already declared both halves of its format, and asking for them again would let a test
     * encode a record its own definition could not read.
     *
     * <h2>It returns as soon as the record is on the queue</h2>
     * Piping a record is not processing it. The engine polls on its own thread and commits on another, so
     * {@link #awaitSettled()} is what makes an assertion afterwards meaningful.
     *
     * @param topic a topic one of this definition's routes claims
     * @param key   the key, of the type the route's key format reads - may be null for a format that encodes one
     * @param value the value, of the type the route's value format reads
     * @return the offset it was published at
     * @throws IllegalStateException    if this sandbox has not been started
     * @throws IllegalArgumentException naming the routed topics, if no route claims this one - a record nothing
     *                                  routes would never be delivered, and a silent publish would read as a
     *                                  function that never ran
     */
    public long pipe(String topic, Object key, Object value) {
        Objects.requireNonNull(topic, "A topic must be supplied");
        requireStarted();
        RoutePublisher publisher = routeFor(topic);
        // This record is now in flight, so anything read out of the run is a race until something settles again.
        settled.set(false);
        long offset = publisher.publish(key, value);
        if (offset < 0) {
            throw new IllegalStateException("This sandbox's consumer has closed, so " + topic + " can take no "
                    + "more records - the instance is no longer running. Publish inside the try-with-resources "
                    + "that holds the instance, and settle before you leave it.");
        }
        return offset;
    }

    /**
     * One topic's way in as an object, which is the shape the broker-free test drivers of the stream-processing
     * libraries users compare us with have: {@code sandbox.createInputTopic("orders").pipeInput(key, value)}.
     * <p>
     * Reach for it when a test pipes repeatedly into one topic, and for {@link #pipe(String, Object, Object)} when
     * it pipes once - both go through this sandbox's one encoding and one partition choice. No serialisers here
     * either, for the reason {@link #pipe(String, Object, Object)} gives.
     *
     * @param topic a topic one of this definition's routes claims
     * @throws IllegalStateException    if this sandbox has not been started
     * @throws IllegalArgumentException naming the routed topics, if no route claims this one
     */
    public SandboxInputTopic<Object, Object> createInputTopic(String topic) {
        Objects.requireNonNull(topic, "A topic must be supplied");
        requireStarted();
        // Called for its refusal, not its value: a topic nothing routes is named here, at the line that spells it,
        // rather than at whichever pipe happens to be first.
        RoutePublisher ignoredPublisher = routeFor(topic);
        return new SandboxInputTopic<>(topic, this::pipe);
    }

    /**
     * One topic's way out: what the instance produced onto it. {@link SandboxOutputTopic} owns the contract -
     * reading consumes, and every read refuses until the run has settled.
     * <p>
     * The topic is not checked against the definition's routes, because a definition produces wherever its function
     * says and those are output topics, which no route claims. On the fluent API what fills it is export, so until
     * export lands this reads honestly empty; the classic API's {@code pollAndProduce} fills it today.
     *
     * @throws IllegalStateException if this sandbox has not been started
     */
    public SandboxOutputTopic<byte[], byte[]> createOutputTopic(String topic) {
        Objects.requireNonNull(topic, "A topic must be supplied");
        requireStarted();
        return new SandboxOutputTopic<>(topic, producer::history, settled::get);
    }

    /**
     * The route that claims a topic, or a refusal naming what this definition does route. One place, so the
     * refusal a pipe gives and the refusal an input topic gives are the same sentence.
     */
    private RoutePublisher routeFor(String topic) {
        RoutePublisher publisher = publishers.get(topic);
        if (publisher == null) {
            throw new IllegalArgumentException("No route in this definition claims topic " + topic + " - it routes "
                    + publishers.keySet() + ". A record published to an unrouted topic would never be delivered.");
        }
        return publisher;
    }

    /**
     * Blocks until the instance has accounted for every record published so far - each one completed, or parked -
     * and then returns, so that what a test asserts next is the end of the work rather than the middle of it.
     * <p>
     * A park counts as settled: it is a terminal outcome, the record holds no worker and is never due again. The
     * arithmetic, and why a committed offset rather than a delivery count is what "completed" reads off, are on
     * {@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}, which this is a caller-facing wrapper of.
     *
     * <h2>It refuses rather than returning quietly</h2>
     * Four endings are not a settled run and all four throw here, because a test that asserted after any of them
     * would be asserting about a run that stopped early:
     * <ol>
     *   <li>the budget ran out with records outstanding;</li>
     *   <li>the instance shut down under the wait with records outstanding - which the underlying wait returns
     *       quietly for, since that is how an unbounded driven run ordinarily ends;</li>
     *   <li>the instance recorded a failure;</li>
     *   <li><b>a route stopped the instance</b> (R24) - and this one <em>satisfies</em> the accounting, which is
     *       why it is checked separately. The stopping record is marked never-due before the stop is raised, so
     *       the engine's retry queue holds it and it reads as parked, and a park is a settled record. A run that
     *       stopped at its first record would otherwise present as a run that finished.</li>
     * </ol>
     * Where the instance failed or a route stopped it, the refusal says so, because a bare shortfall would send a
     * reader looking at the engine for a cause the definition already declared.
     * <p>
     * A park on its own is not one of these: it is a record's own terminal outcome, the definition asked for it,
     * and the parked set is there to be asserted on.
     *
     * @throws IllegalStateException naming what is still outstanding, and why the instance is not going to account
     *                               for it
     */
    public void awaitSettled() {
        requireStarted();
        try {
            consumer.awaitEveryPublishedRecordCommitted();
        } catch (IllegalStateException shortfall) {
            throw endedShort(shortfall.getMessage(), shortfall);
        }
        settledOrRefuse();
        settled.set(true);
    }

    /**
     * @param budget how long to wait before refusing, for a test whose subject is the refusal - the default is
     *               {@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}'s, which is sized for a commit
     *               cadence rather than guessed at
     * @see #awaitSettled()
     */
    public void awaitSettled(Duration budget) {
        Objects.requireNonNull(budget, "A budget must be supplied");
        requireStarted();
        try {
            consumer.awaitEveryPublishedRecordCommitted(budget);
        } catch (IllegalStateException shortfall) {
            throw endedShort(shortfall.getMessage(), shortfall);
        }
        settledOrRefuse();
        settled.set(true);
    }

    /**
     * The half of the settle that runs after a wait which did not itself refuse: the wait returns quietly when the
     * consumer closes under it, so this is where a run that ended early is caught, and where a failure the
     * instance recorded is put in front of the caller instead of being left in the log.
     */
    private void settledOrRefuse() {
        Optional<String> outstanding = consumer.whatIsNotAccountedFor();
        if (outstanding.isPresent()) {
            throw endedShort("The sandbox stopped waiting before the instance accounted for what was published, "
                    + "and these partitions never got there: " + outstanding.get(), null);
        }
        Throwable failure = instanceFailure();
        if (failure != null) {
            throw new IllegalStateException("Every published record is accounted for, but the instance recorded a "
                    + "failure while the sandbox was waiting for it", failure);
        }
        Optional<StopRequest> stopped = stopRequest();
        if (stopped.isPresent()) {
            // A stopped run satisfies the accounting and is still not a settled run, which is why this check is
            // here rather than folded into the one above: the stopping record is marked never-due before the stop
            // is raised, so the engine's retry queue holds it and the parked view reports it - and a park counts
            // as accounted for. Left alone, a caller would assert on a run that stopped at its first record.
            throw new IllegalStateException("Every record published reached an end, but the run did not settle: "
                    + describe(stopped.get()) + " Whatever was in flight when the stop landed was left incomplete "
                    + "by design, and nothing published after it is processed at all. Assert on the stop itself "
                    + "through the instance rather than on the run.");
        }
    }

    /**
     * A settle that is about to refuse, told whatever the instance knows about why - the failure it recorded, or
     * the stop a route asked for. Both are ordinary states a definition can declare its way into, and neither is
     * visible in the accounting the underlying wait renders.
     *
     * @param cause what the underlying wait threw, when it threw - kept as the cause unless the instance has a
     *              better one
     */
    private IllegalStateException endedShort(String what, Throwable cause) {
        Throwable failure = instanceFailure();
        if (failure != null) {
            return new IllegalStateException(what + " The instance failed, which is why.", failure);
        }
        Optional<StopRequest> stopped = stopRequest();
        if (stopped.isPresent()) {
            return new IllegalStateException(what + " " + describe(stopped.get())
                    + " Records still in flight when a stop lands are left incomplete by design.", cause);
        }
        return new IllegalStateException(what, cause);
    }

    /**
     * The stop a route asked for, if one did - null-safe over a sandbox that has not been started, so both
     * refusal paths can ask without guarding first.
     */
    private Optional<StopRequest> stopRequest() {
        return instance == null ? Optional.empty() : instance.stopRequest();
    }

    /**
     * One sentence naming the stop, written once because both refusals carry it and a reader grepping a build log
     * should find the same words either way.
     */
    private static String describe(StopRequest stop) {
        return "A route stopped the instance at " + stop.topic() + "-" + stop.partition() + "@" + stop.offset()
                + ": " + stop.reason() + ".";
    }

    /**
     * @return the failure the instance recorded, or null - a definition fault the facade raised, or the engine's
     * own control-thread failure
     */
    private Throwable instanceFailure() {
        return instance == null ? null : instance.failureCause().orElse(null);
    }

    /**
     * Refuses every caller-facing call that needs a running instance, in one place and with one message, because
     * "this sandbox has not been started" is a better answer than a null pointer or an empty result.
     */
    private void requireStarted() {
        if (consumer == null || instance == null) {
            throw new IllegalStateException("This sandbox has not been started - pass it to the definition's "
                    + "start(...) first, and publish inside the try-with-resources that holds the instance");
        }
    }

    // ---------------------------------------------------------------- observation

    /**
     * The mock consumer this sandbox is running, or null before {@code start}. Its commit history is what a test
     * asserts offsets against.
     */
    public SandboxConsumer<byte[], byte[]> consumer() {
        return consumer;
    }

    /**
     * The raw mock producer, for a test that needs something an output topic does not expose - whether transactions
     * were initialised, the transactional offset history, every topic's records at once.
     * <p>
     * <b>To read what came out, reach for {@link #createOutputTopic(String)}</b>: that is where the read verbs live,
     * with a cursor and the settle guard, and it is where a test should normally look. This accessor is the escape
     * hatch under it, and it hands back a client rather than records - so it carries no read verb of its own.
     * <p>
     * The client a definition asks for is {@link #producer(DefinitionView)}; this is the same object, reached from a
     * test rather than from the facade.
     */
    public MockProducer<byte[], byte[]> producer() {
        return producer;
    }

    /**
     * How many records the driver has put in, across every topic. A sandbox nothing is driving has none, so this
     * reads zero however much a caller piped - {@link SandboxConsumer#publishedCounts()} is what counts that.
     */
    public long drivenRecords() {
        return driver == null ? 0 : driver.drivenRecords();
    }

    /**
     * Waits for the run to reach its bound, <b>and for the bound to finish what reaching it starts</b>: the
     * driver stops, every published record is accounted for - committed, or parked - and the instance closes.
     * So a true return means the state readable afterwards is the end of the run rather than the middle of it. An
     * unbounded run never reaches a bound, so this is the wait a test uses and a demo does not.
     * <p>
     * Give it a timeout larger than the bound's own wait
     * ({@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}), or this will time out first and report a
     * bare false where that wait would have named the partition and the shortfall.
     *
     * @return false if the bound had not been reached when the wait ran out
     * @throws IllegalStateException wrapping whatever stopped the driver or failed the bound's wait
     */
    public boolean awaitBound(Duration timeout) {
        if (handPublished) {
            throw new IllegalStateException("This sandbox publishes by hand, so nothing is driving it towards a "
                    + "bound - publish the records you want and call awaitSettled(), or drop "
                    + "Sandbox.builder().handPublished() to have the driver publish for you");
        }
        if (driver == null) {
            throw new IllegalStateException("This sandbox has not been started");
        }
        if (!bound.isBounded()) {
            throw new IllegalStateException("This sandbox is unbounded, so it will never reach a bound - declare "
                    + "one with Sandbox.builder().bound(...), or close the instance to end the run");
        }
        boolean reached = driver.awaitBound(timeout);
        if (reached) {
            // The bound waits for every driven record to be accounted for and then closes the instance, so a true
            // return is a settle by another route - and reads on an output topic are earned by it.
            settled.set(true);
        }
        return reached;
    }

    /**
     * Ends the run: stop the driver if there is one, then close the instance if this sandbox has been started.
     * <p>
     * <b>Closing the instance is what makes the sandbox usable as the single try-with-resources resource</b>, the
     * shape the broker-free drivers of the stream-processing libraries users compare us with have. It is
     * idempotent and it is second, so it changes nothing on the driven path: the bound has already closed the
     * instance by the time anything gets here, and {@link ParallelConsumerInstance#close()} called twice returns
     * once the first has finished. On the caller-published path there is no driver to stop, and without this a
     * close would do nothing at all - which is a worse trap than the wordier javadoc.
     */
    @Override
    public void close() {
        if (driver != null) {
            driver.close();
        }
        if (instance != null) {
            instance.close();
        }
    }

    // ---------------------------------------------------------------- internals

    /**
     * How many records are parked on each of the instance's partitions right now, for the bound's wait.
     * <p>
     * Counted from {@link ParallelConsumerInstance#parkedAllTopics()}'s own records rather than from its
     * {@code byPartition()} roll-out, because that groups by partition <em>number</em> across every topic - so a
     * definition with two routes would credit {@code orders-0}'s parked records to {@code parcel-scans-0} as well.
     * The record carries its topic and its partition, and a partition here is both.
     */
    private static Map<TopicPartition, Long> parkedCountsByPartition(ParallelConsumerInstance instance) {
        Map<TopicPartition, Long> counts = new LinkedHashMap<>();
        for (ParkedRecord parked : instance.parkedAllTopics().records()) {
            counts.merge(new TopicPartition(parked.topic(), parked.partition()), 1L, Long::sum);
        }
        return counts;
    }

    /**
     * One publisher per routed topic, each encoding with its own route's serialisers - the engine below the facade
     * consumes raw bytes, so a value has to become bytes the route's deserialiser will read back (KTD2).
     * <p>
     * Built at {@code start} rather than per record, and shared: {@link #pipe} and the driver's feeds both go
     * through these, so there is one encoding and one partition choice rather than two that could drift.
     */
    private Map<String, RoutePublisher> routePublishers(DefinitionView view) {
        Map<String, RoutePublisher> byTopic = new LinkedHashMap<>();
        for (RouteView route : view.routes()) {
            for (String topic : route.topics()) {
                byTopic.put(topic, new RoutePublisher(topic, route.consumedKey(), route.consumedValue(), consumer,
                        partitionsPerTopic));
            }
        }
        return byTopic;
    }

    /**
     * One feed per topic for the driver, each over the publisher that topic already has and the value function
     * that topic was given. Addressed by index rather than by sequence, so record <em>n</em> of a topic is the
     * same whatever order the topics were served in and whatever the pacing did.
     */
    private List<TopicFeed> fluentFeeds() {
        List<TopicFeed> feeds = new ArrayList<>();
        for (RouteView route : definition.routes()) {
            for (String topic : route.topics()) {
                // One filler per feed, seeded with the run's seed, so record n of this topic depends on the seed
                // and n and not on how the topics interleaved. Built even when both functions are the caller's,
                // because it costs nothing until it is asked for anything.
                RandomObjects random = RandomObjects.seededWith(seed);
                Class<?> declared = declaredTypes.get(topic);
                Class<?> valueType = declared != null ? declared : ValueTypes.of(route.consumedValue());
                Class<?> keyType = ValueTypes.of(route.consumedKey());
                int pool = keyCardinality;

                LongFunction<Object> declaredKeys = keys.get(topic);
                LongFunction<Object> declaredValues = values.get(topic);
                feeds.add(new RouteFeed(publishers.get(topic),
                        declaredKeys != null ? declaredKeys : index -> random.key(keyType, index, pool),
                        declaredValues != null ? declaredValues : index -> random.create(valueType, index)));
            }
        }
        return feeds;
    }

    /**
     * Refuses, at start-up, every route this sandbox could not feed - naming the topic and what to do
     * about it.
     * <p>
     * Up front rather than at the first record, because a feed that skipped a topic it could not fill would
     * present as a definition whose route never fires, which is a far harder thing to diagnose than a refusal
     * naming the topic.
     * <p>
     * Two separate refusals, and they apply to different sandboxes. <b>A format that cannot write</b> is refused
     * whoever supplies the value, because the record has to be encoded either way. <b>A type the hydration cannot
     * name</b> is refused only where the hydration is what would fill it: a driven topic whose caller supplied
     * neither a value function nor a declared type. The cure is on the definition - a format helper that carries
     * its type, a declared type on the builder, a value function of the caller's own, or a serialiser for a route
     * that only reads.
     */
    private void refuseRoutesTheDriverCannotFeed(DefinitionView view) {
        for (RouteView route : view.routes()) {
            for (String topic : route.topics()) {
                requireWritable(topic, route.consumedValue(), "value");
                requireWritable(topic, route.consumedKey(), "key");
                if (handPublished) {
                    continue;
                }
                if (!values.containsKey(topic) && !declaredTypes.containsKey(topic)
                        && ValueTypes.of(route.consumedValue()) == null) {
                    throw new IllegalArgumentException("The sandbox cannot fill records for topic " + topic
                            + ": its value format (" + route.consumedValue() + ") does not name a Java type, and "
                            + "the hydration has to fill an instance of one. Declare the route with a format "
                            + "helper (json/avro/protobuf/string/bytes), with Format.of(deserializer, "
                            + "serializer, YourType.class), tell the sandbox with "
                            + "Sandbox.builder().hydrating(\"" + topic + "\", YourType.class), or say what a "
                            + "record contains yourself with Sandbox.builder().feeding(\"" + topic + "\", "
                            + "index -> yourValue(index)).");
                }
                if (!keys.containsKey(topic) && ValueTypes.of(route.consumedKey()) == null) {
                    throw new IllegalArgumentException("The sandbox cannot fill keys for topic " + topic
                            + ": its key format (" + route.consumedKey() + ") does not name a Java type. Declare "
                            + "the key with a format helper, with Format.of(deserializer, serializer, "
                            + "YourType.class), or with Sandbox.builder().feedingKeys(\"" + topic + "\", "
                            + "index -> yourKey(index)).");
                }
            }
        }
    }

    /**
     * Refuses a format the sandbox could read from but not write to. The sandbox has to <em>produce</em> the
     * records the definition consumes, so a read-only format leaves it with a value it cannot put on the wire.
     *
     * @param side "key" or "value", so the refusal names which half of the record is the problem
     */
    private static void requireWritable(String topic, Format<?> format, String side) {
        if (!format.hasSerializer()) {
            throw new IllegalArgumentException("The sandbox cannot encode records for topic " + topic
                    + ": its " + side + " format (" + format + ") can only read. The sandbox has to encode "
                    + "what it makes with the same format the route decodes it with, so a route declared with a "
                    + "hand-written deserialiser needs a serialiser beside it - Consumed.with(..., "
                    + "Format.of(deserializer, serializer)) - or the route needs feeding by hand.");
        }
    }

    /**
     * One routed topic's way in: encode a key and a value with that route's own serialisers, place the record by
     * the encoded key, publish it.
     * <p>
     * Apart from the feed above it, because both callers need it and they are not the same caller: the driver's
     * feed asks for record <em>n</em> of a made-up sequence, and {@link Sandbox#pipe} hands over a value the
     * test wrote. Keeping the encoding and the placement here is what makes those two indistinguishable to the
     * engine - a test cannot accidentally exercise a different path from the one a demo does.
     */
    private static final class RoutePublisher {

        /**
         * The topic this publisher writes to. One per topic, not per route: a route with two topics gets two, so
         * that the serialiser is always called with the topic the record is actually going to.
         */
        private final String topic;

        /**
         * The route's own key format, used to encode - so what the engine polls is exactly what that route's
         * deserialiser will be asked to read back.
         */
        private final Format<?> keyFormat;

        /**
         * The route's own value format, for the same reason as {@link #keyFormat}.
         */
        private final Format<?> valueFormat;

        /**
         * Where published records go. The same consumer every publisher writes into, which is what makes the
         * published counts one ledger rather than several.
         */
        private final SandboxConsumer<byte[], byte[]> consumer;

        /**
         * Partitions on this topic, the divisor the key hash is taken modulo.
         */
        private final int partitions;

        private RoutePublisher(String topic,
                               Format<?> keyFormat,
                               Format<?> valueFormat,
                               SandboxConsumer<byte[], byte[]> consumer,
                               int partitions) {
            this.topic = topic;
            this.keyFormat = keyFormat;
            this.valueFormat = valueFormat;
            this.consumer = consumer;
            this.partitions = partitions;
        }

        /**
         * @return the offset it was published at, or -1 once the consumer has closed - which is how the driver
         * learns the run is over, and what {@link Sandbox#pipe} turns into a refusal
         */
        private long publish(Object key, Object value) {
            byte[] keyBytes = encode(keyFormat, key);
            byte[] valueBytes = encode(valueFormat, value);
            // Hashed over the ENCODED KEY BYTES, which is the only thing every key type here agrees on.
            //
            // Kafka's default partitioner also hashes the serialised key rather than the object - it murmur2s
            // those bytes - so this places a key on the same partition every time and reproduces the same
            // placement for a given record index, which is what a key-ordered sandbox run promises.
            // It does NOT put a key on the same partition a broker would; nothing here needs that, and claiming
            // it would be false.
            //
            // Hashing the key OBJECT is what this used to do, and it silently defeated both promises for every
            // key type but String: a key function that hands back a fresh byte[] per call, or a freshly built
            // POJO, overrides no hashCode - so Objects.hashCode was the IDENTITY hash, different for every record
            // of the same logical key and different between two runs of the same program.
            int partition = Math.floorMod(Arrays.hashCode(keyBytes), partitions);
            return consumer.publish(topic, partition, keyBytes, valueBytes);
        }

        /**
         * The route's serialiser applied to a key or a value. The cast is unchecked because a {@code Format<?>}
         * has lost the link between its serialiser and the type beside it; what makes it safe on the driver's path
         * is that both came from the same route and the type came from that format, and on the caller's path it is
         * the caller having written a value its own route can read - a mismatch fails here, naming the class,
         * rather than one poll later as a payload nobody can account for.
         */
        @SuppressWarnings("unchecked")
        private byte[] encode(Format<?> format, Object value) {
            Serializer<Object> serializer = (Serializer<Object>) format.serializer();
            try {
                return serializer.serialize(topic, value);
            } catch (ClassCastException wrongType) {
                throw new IllegalArgumentException("The route for topic " + topic + " cannot encode a "
                        + (value == null ? "null" : value.getClass().getName()) + " with its declared format ("
                        + format + "). Publish a value of the type the route declared, or the route's own "
                        + "deserialiser could not read it back.", wrongType);
            }
        }
    }

    /**
     * One topic of a fluent definition for the driver: ask this topic's two functions for the record at an index,
     * then hand the pair to that topic's {@link RoutePublisher}.
     * <p>
     * <b>It contains no opinion at all about what a record holds</b>, which is the whole point of the seam: the
     * functions are the caller's, and an artefact that ships realistic fake objects is one such caller.
     */
    private static final class RouteFeed implements TopicFeed {

        /**
         * The way in for this feed's topic - the encoding and the partition choice both live there, so this class
         * is only about what the record contains.
         */
        private final RoutePublisher publisher;

        /**
         * This topic's key for a given record index - the caller's, or the pooled default.
         */
        private final LongFunction<Object> keys;

        /**
         * This topic's value for a given record index. Never null: a driven topic without one is refused at
         * start, naming the topic.
         */
        private final LongFunction<Object> values;

        private RouteFeed(RoutePublisher publisher, LongFunction<Object> keys, LongFunction<Object> values) {
            this.publisher = publisher;
            this.keys = keys;
            this.values = values;
        }

        /**
         * Named in the driver's log when a publish reports the consumer closed.
         */
        @Override
        public String topic() {
            return publisher.topic;
        }

        /**
         * One record: ask for the key and the value at this index, then publish.
         *
         * @param index this feed's record index - the address of a record, so that one can be reproduced without
         *              replaying the ones before it
         * @return false once the consumer has closed, which is how the driver learns the run is over
         */
        @Override
        public boolean publish(long index) {
            return publisher.publish(keys.apply(index), values.apply(index)) >= 0;
        }
    }

    /**
     * The knobs, all optional. The defaults are a run that reads well in a console and finishes when you close
     * it: fifty records a second per topic, one partition, ten distinct keys, seed zero, no bound.
     */
    @InterfaceStability.Unstable
    public static final class Builder {

        /**
         * Fifty a second per topic: fast enough that a demo shows something immediately, slow enough that its
         * console output can be read as it goes. A driver setting, so it says nothing on a hand-published sandbox.
         */
        private double perSecond = 50;

        /**
         * No bound by default, because the default caller is a demo that stops when its instance is closed. A test
         * declares one.
         */
        private Bound bound = Bound.none();

        /**
         * One partition, which is the simplest thing that works. More is what a run demonstrating key ordering
         * across partitions asks for.
         */
        private int partitionsPerTopic = 1;

        /**
         * Ten keys: enough that keys repeat and a shard has records to order, few enough that a console reader can
         * see the repetition.
         */
        private int keyCardinality = 10;

        /**
         * Zero unless asked otherwise - a fixed default rather than a random one, so that two runs of an
         * unchanged program are the same run.
         */
        private long seed;

        /**
         * Value types declared by topic, for routes whose format cannot name one. Ordered, so a refusal listing
         * what was declared lists it the way it was written.
         */
        private final Map<String, Class<?>> declaredTypes = new LinkedHashMap<>();

        /**
         * What the driver publishes as the value on each topic. Ordered, so a refusal listing what was declared
         * lists it the way it was written.
         */
        private final Map<String, LongFunction<Object>> values = new LinkedHashMap<>();

        /**
         * What the driver publishes as the key on each topic, for the topics that said.
         */
        private final Map<String, LongFunction<Object>> keys = new LinkedHashMap<>();

        /**
         * False unless {@link #handPublished()} is called - see the field of the same name on {@link Sandbox} for
         * why the driver, not the caller, is what a sandbox does by default.
         */
        private boolean handPublished;

        /**
         * Private: reached through {@link Sandbox#builder()}.
         */
        private Builder() {
        }

        /**
         * <b>The caller pipes; nothing is driven.</b> The sandbox assigns its partitions at {@code start} and
         * then waits, so the records the instance sees are exactly the ones
         * {@link Sandbox#pipe(String, Object, Object)} was given - which is what a test that knows its own data
         * wants, and what makes {@link Sandbox#awaitSettled()} a statement about that data rather than about a
         * rate.
         * <p>
         * {@link #perSecond(double)}, {@link #bound(Bound)}, {@link #keyCardinality(int)}, {@link #seed(long)},
         * {@link #hydrating(String, Class)}, {@link #feeding(String, LongFunction)} and
         * {@link #feedingKeys(String, LongFunction)} all describe the driver, so they say nothing once this is
         * set; {@link #partitionsPerTopic(int)} still applies, because it shapes the topics rather than who
         * publishes into them.
         */
        public Builder handPublished() {
            this.handPublished = true;
            return this;
        }

        /**
         * Records per second <b>per source topic</b> - a definition that gains a route gains traffic rather than
         * dividing what it had.
         */
        public Builder perSecond(double rate) {
            this.perSecond = rate;
            return this;
        }

        /**
         * When the driver should stop publishing and close, draining first. Unbounded by default.
         */
        public Builder bound(Bound bound) {
            this.bound = Objects.requireNonNull(bound, "A bound must be supplied - use Bound.none() for none");
            return this;
        }

        /**
         * How many partitions each of the sandbox's topics has. One by default: more of them is what a partition-ordered
         * definition needs to show any parallelism at all.
         */
        public Builder partitionsPerTopic(int partitions) {
            this.partitionsPerTopic = partitions;
            return this;
        }

        /**
         * How many distinct keys a topic's default key pool draws from - it has no effect on a topic given its own
         * keys with {@link #feedingKeys(String, LongFunction)}. Keys must repeat for key ordering to mean anything,
         * and a cardinality of one puts every record on a single shard.
         */
        public Builder keyCardinality(int distinctKeys) {
            if (distinctKeys < 1) {
                throw new IllegalArgumentException("A key cardinality of " + distinctKeys + " leaves no keys to "
                        + "choose from");
            }
            this.keyCardinality = distinctKeys;
            return this;
        }

        /**
         * Two runs with the same seed fill the same records, in the same order, with the same timestamps - so a
         * sandbox failure is reproducible rather than merely likely to recur. It is the hydration's number: a
         * topic given its own value function is not random and has nothing to seed.
         */
        public Builder seed(long seed) {
            this.seed = seed;
            return this;
        }

        /**
         * Name the value type for a topic whose route cannot - a custom serde over a class the format does not
         * carry. Without this, and without {@link #feeding(String, LongFunction)}, such a route is refused at
         * start, naming the topic.
         */
        public Builder hydrating(String topic, Class<?> valueType) {
            declaredTypes.put(Objects.requireNonNull(topic, "A topic must be supplied"),
                    Objects.requireNonNull(valueType, "A value type must be supplied"));
            return this;
        }

        /**
         * <b>What the driver publishes on this topic</b>, instead of the realistic random data it would otherwise
         * fill the route's declared type with: a function from the record's index within this topic to the value
         * for that record.
         * <p>
         * Addressed by <em>index</em> rather than called in sequence, deliberately, and for the same reason
         * {@link #seed(long)} exists: the record at a given index is then the same whatever order the topics were
         * served in and whatever the pacing did, so "reproduce record 4173" is a thing a caller can do without
         * replaying the four thousand before it.
         * <p>
         * Reach for it when a run is about particular values rather than plausible ones - and note that a test
         * which wants particular <em>records</em> usually wants no driver at all, which is
         * {@link #handPublished()}. The value is encoded with the route's own serialiser, so it has to be of the
         * type that route reads; one of the wrong type is refused when it is published, naming the class.
         */
        public Builder feeding(String topic, LongFunction<?> value) {
            Objects.requireNonNull(value, "A value function must be supplied");
            // Adapted rather than cast: the caller's function is declared over its own type - an Order, a String -
            // and a LongFunction<Order> is not a LongFunction<Object> however obviously every Order is an Object.
            values.put(Objects.requireNonNull(topic, "A topic must be supplied"), value::apply);
            return this;
        }

        /**
         * The keys to go with {@link #feeding(String, LongFunction)}, for a topic that wants its own rather than
         * the pool the hydration draws from its route's declared key type.
         * <p>
         * Worth declaring when the run is <em>about</em> which records share a shard - a function returning one
         * constant puts every record of the topic on one key, which is the quickest way to see what ordering
         * costs.
         */
        public Builder feedingKeys(String topic, LongFunction<?> key) {
            Objects.requireNonNull(key, "A key function must be supplied");
            // Adapted for the same reason as in feeding(...) above.
            keys.put(Objects.requireNonNull(topic, "A topic must be supplied"), key::apply);
            return this;
        }

        /**
         * The sandbox these settings describe. The builder may be reused afterwards - the sandbox copies what it
         * needs, so a later change here does not reach a run already built.
         */
        public Sandbox build() {
            return new Sandbox(this);
        }
    }
}
