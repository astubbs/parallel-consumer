package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.ClientRuntime;
import bz.stub.parallelconsumer.fluent.ConsumerHandle;
import bz.stub.parallelconsumer.fluent.DefinitionView;
import bz.stub.parallelconsumer.fluent.Format;
import bz.stub.parallelconsumer.fluent.ParkedRecord;
import bz.stub.parallelconsumer.fluent.RouteView;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Runs a definition with no broker, against records it generates itself.
 *
 * <h2>The definition does not change; the start call does</h2>
 * <pre>{@code
 * ParallelConsumerDefinition pc = ParallelConsumer.connect(props);
 * pc.json("orders", Order.class)
 *         .process(ctx -> { inventory.reserve(ctx.value()); return Outcome.succeeded(); });
 *
 * Sandbox sandbox = Sandbox.builder()
 *         .perSecond(50)
 *         .bound(Bound.after(Duration.ofSeconds(10)))
 *         .build();
 * try (ConsumerHandle handle = pc.start(sandbox)) {   // against a broker, this line reads pc.start()
 *     handle.awaitShutdown();
 * }
 * }</pre>
 * Everything above the {@code start} line is the definition a production instance runs. That is the whole claim
 * of the sandbox (R33): what it exercises is the real facade over the real engine, with the clients replaced -
 * not a simulation of either.
 *
 * <h2>What it generates</h2>
 * One record per source topic per tick, at the declared rate, with each route's declared value type filled with
 * realistic random data ({@link RandomObjects}) and encoded with that route's own serialiser. Keys are drawn from
 * a small pool so that they repeat, which is what gives key ordering something to order.
 *
 * <h2>What it refuses, and when</h2>
 * At {@code start}, before a record exists: a route whose value format cannot <em>write</em> (a hand-written
 * deserialiser with no serialiser beside it - the generator would have nothing to encode with), and a route whose
 * Java type nothing can name (see {@link ValueTypes}). Both name the topic. A Protobuf value type is refused when
 * its first record is generated, naming the type: this version has no filler for one.
 *
 * <h2>The classic API too</h2>
 * {@link #classic} hands the same mock consumer, producer and generator to an options builder, so an existing
 * classic-API application or example runs broker-free with its start call changed and nothing else (R33, AE26).
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
     * handed to the generator to be asked per record.
     */
    private final Bound bound;

    /**
     * The run's seed. Logged when the run starts, because it is the one number a reader needs to reproduce what
     * they just saw.
     */
    private final long seed;

    /**
     * Partitions per source topic, which is what makes key ordering observable - with one partition every key
     * lands in the same place whatever the hash said.
     */
    private final int partitionsPerTopic;

    /**
     * How many distinct keys the generator draws from, so that keys repeat and a shard has more than one record
     * to order.
     */
    private final int keyCardinality;

    /**
     * Value types declared by hand for topics whose route cannot name one. The escape hatch for a route with a
     * custom serde, so that "the sandbox cannot generate for this topic" is a fixable state rather than a wall.
     */
    private final Map<String, Class<?>> declaredTypes;

    /**
     * The instance's producer, built up front rather than on demand because it is also where the transactional
     * commit mode's offsets go - so the consumer's wait has to be able to read it whether or not the definition
     * produces anything. Byte serialisers on both sides: the engine below the fluent facade produces raw bytes,
     * already encoded by the route.
     */
    private final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

    // Null until the definition is started - which is the whole shape of this class: it is asked for clients,
    // then told the instance is running. NullAway reads a field a constructor does not set as a fault, and the
    // suppression is its own marker for a field initialised later rather than a new dependency on an annotation.
    @SuppressWarnings("NullAway.Init")
    private SandboxConsumer<byte[], byte[]> consumer;

    /**
     * The definition this sandbox was handed, kept from whichever client call came first: it is what the generator
     * reads to learn the topics, the types and the serialisers.
     */
    @SuppressWarnings("NullAway.Init")
    private DefinitionView definition;

    /**
     * Null until the instance starts - see {@link #started(ConsumerHandle)}. Every public method that touches it
     * either null-checks it or refuses, because "this sandbox has not been started" is a better answer than a
     * null pointer.
     */
    @SuppressWarnings("NullAway.Init")
    private RecordGenerator generator;

    /**
     * Private: a sandbox is built through {@link #builder()}, which is what documents the defaults.
     */
    private Sandbox(Builder builder) {
        this.perSecond = builder.perSecond;
        this.bound = builder.bound;
        this.seed = builder.seed;
        this.partitionsPerTopic = builder.partitionsPerTopic;
        this.keyCardinality = builder.keyCardinality;
        this.declaredTypes = new LinkedHashMap<>(builder.declaredTypes);
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
        refuseRoutesTheGeneratorCannotFeed(view);
        this.consumer = new SandboxConsumer<>(view.topics(), partitionsPerTopic);
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
     * Assign the partitions and start generating - the moment neither client factory method can give us: the
     * engine has now subscribed, so there is a rebalance listener to assign to, and the handle exists, so the
     * bound has something to close.
     */
    @Override
    public void started(ConsumerHandle handle) {
        Objects.requireNonNull(handle, "A handle must be supplied");
        if (consumer == null) {
            throw new IllegalStateException("This sandbox was started without being asked for a consumer - a "
                    + "definition that supplies its own consumer with consumer(...) cannot also be run in the "
                    + "sandbox, because the sandbox IS the consumer");
        }
        consumer.assignAfterSeeding();
        // A parked record is a terminal outcome, and its partition never commits past it - so without this the
        // bound's wait would spend its whole budget on a definition that parks by design, which the README's own
        // quickstart does (astubbs#504). The handle is the only thing that knows what is parked, and this is the
        // first moment it exists.
        consumer.countingParkedRecordsWith(() -> parkedCountsByPartition(handle));
        generator = new RecordGenerator(fluentFeeds(), perSecond, bound, () -> {
            try {
                // The bound stops the generator; the engine still has to finish and commit what it was already
                // given. A drain-first close does not do that for us - see
                // SandboxConsumer#awaitEveryPublishedRecordCommitted for what it does instead, and why a committed
                // offset or a park is what means a record is done with.
                consumer.awaitEveryPublishedRecordCommitted();
            } finally {
                // Closed either way: a handle left open outlives whatever made it, and the wait's own refusal
                // still reaches the caller through the generator's recorded failure.
                handle.close();
            }
        });
        log.info("Sandbox running: {} at {}/s per topic, seed {}, {}",
                definition.topics(), perSecond, seed, bound);
        generator.start();
    }

    // ---------------------------------------------------------------- classic API

    /**
     * The classic API's entry (R33, AE26): a typed mock consumer and a generator behind it, for an options
     * builder rather than a definition.
     * <p>
     * The classic API has no runtime seam - it takes a finished consumer - so the wiring is explicit rather than
     * one call: build the options with {@link ClassicSandbox#consumer()}, subscribe, poll, then
     * {@link ClassicSandbox#startGenerating}. Note that no encoding happens on this path at all: the mock
     * consumer holds records of the user's own types, so the generated objects go in as they are.
     *
     * @param keyType   the classic instance's key type, generated from a small pool so keys repeat
     * @param valueType the classic instance's value type, filled with realistic random data
     */
    public <K, V> ClassicSandbox<K, V> classic(Class<K> keyType, Class<V> valueType, String... topics) {
        return classic(keyType, valueType, Arrays.asList(topics));
    }

    /**
     * @see #classic(Class, Class, String...)
     */
    public <K, V> ClassicSandbox<K, V> classic(Class<K> keyType, Class<V> valueType, Collection<String> topics) {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("A classic sandbox needs at least one topic to generate into");
        }
        return new ClassicSandbox<>(keyType, valueType, topics, partitionsPerTopic, perSecond, bound, seed,
                keyCardinality);
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
     * The mock producer, whose {@code history()} holds everything the instance produced - exported records
     * included, once export lands.
     */
    public MockProducer<byte[], byte[]> producer() {
        return producer;
    }

    /**
     * How many records have been generated across every topic.
     */
    public long generatedRecords() {
        return generator == null ? 0 : generator.generatedRecords();
    }

    /**
     * Waits for the run to reach its bound, <b>and for the bound to finish what reaching it starts</b>: the
     * generator stops, every published record is accounted for - committed, or parked - and the instance closes.
     * So a true return means the state readable afterwards is the end of the run rather than the middle of it. An
     * unbounded run never reaches a bound, so this is the wait a test uses and a demo does not.
     * <p>
     * Give it a timeout larger than the bound's own wait
     * ({@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}), or this will time out first and report a
     * bare false where that wait would have named the partition and the shortfall.
     *
     * @return false if the bound had not been reached when the wait ran out
     * @throws IllegalStateException wrapping whatever stopped the generator or failed the bound's wait
     */
    public boolean awaitBound(Duration timeout) {
        if (generator == null) {
            throw new IllegalStateException("This sandbox has not been started");
        }
        if (!bound.isBounded()) {
            throw new IllegalStateException("This sandbox is unbounded, so it will never reach a bound - declare "
                    + "one with Sandbox.builder().bound(...), or close the handle to end the run");
        }
        return generator.awaitBound(timeout);
    }

    /**
     * Stops generating. Does not close the instance - the handle owns that, and closing it is what a bound does.
     */
    @Override
    public void close() {
        if (generator != null) {
            generator.close();
        }
    }

    // ---------------------------------------------------------------- internals

    /**
     * How many records are parked on each of the instance's partitions right now, for the bound's wait.
     * <p>
     * Counted from {@link ConsumerHandle#parkedAllTopics()}'s own records rather than from its
     * {@code byPartition()} roll-out, because that groups by partition <em>number</em> across every topic - so a
     * definition with two routes would credit {@code orders-0}'s parked records to {@code parcel-scans-0} as well.
     * The record carries its topic and its partition, and a partition here is both.
     */
    private static Map<TopicPartition, Long> parkedCountsByPartition(ConsumerHandle handle) {
        Map<TopicPartition, Long> counts = new LinkedHashMap<>();
        for (ParkedRecord parked : handle.parkedAllTopics().records()) {
            counts.merge(new TopicPartition(parked.topic(), parked.partition()), 1L, Long::sum);
        }
        return counts;
    }

    /**
     * One feed per topic, each encoding with its own route's serialisers - the engine below the facade consumes
     * raw bytes, so a generated object has to become bytes the route's deserialiser will read back (KTD2).
     */
    private List<TopicFeed> fluentFeeds() {
        List<TopicFeed> feeds = new ArrayList<>();
        for (RouteView route : definition.routes()) {
            for (String topic : route.topics()) {
                feeds.add(new RouteFeed(topic, route, consumer, RandomObjects.seededWith(seed),
                        partitionsPerTopic, keyCardinality, declaredTypes.get(topic)));
            }
        }
        return feeds;
    }

    /**
     * Refuses, at start-up, every route this sandbox could not generate for - naming the topic and what to do
     * about it.
     * <p>
     * Up front rather than at the first record, because a generator that skipped a topic it could not fill would
     * present as a definition whose route never fires, which is a far harder thing to diagnose than a refusal
     * naming the topic. The cure is on the definition: a format helper that carries its type, a declared type on
     * the builder, or a serialiser for a route that only reads.
     */
    private void refuseRoutesTheGeneratorCannotFeed(DefinitionView view) {
        for (RouteView route : view.routes()) {
            for (String topic : route.topics()) {
                requireWritable(topic, route.consumedValue(), "value");
                requireWritable(topic, route.consumedKey(), "key");
                if (declaredTypes.containsKey(topic)) {
                    continue;
                }
                if (ValueTypes.of(route.consumedValue()) == null) {
                    throw new IllegalArgumentException("The sandbox cannot generate records for topic " + topic
                            + ": its value format (" + route.consumedValue() + ") does not name a Java type, and "
                            + "the generator has to fill an instance of one. Declare the route with a format "
                            + "helper (json/avro/protobuf/string/bytes), with Format.of(deserializer, "
                            + "serializer, YourType.class), or tell the sandbox with "
                            + "Sandbox.builder().generating(\"" + topic + "\", YourType.class).");
                }
                if (ValueTypes.of(route.consumedKey()) == null) {
                    throw new IllegalArgumentException("The sandbox cannot generate keys for topic " + topic
                            + ": its key format (" + route.consumedKey() + ") does not name a Java type. Declare "
                            + "the key with a format helper or with Format.of(deserializer, serializer, "
                            + "YourType.class).");
                }
            }
        }
    }

    /**
     * Refuses a format the generator could read from but not write to. The sandbox has to <em>produce</em> the
     * records the definition consumes, so a read-only format leaves it with a value it cannot put on the wire.
     *
     * @param side "key" or "value", so the refusal names which half of the record is the problem
     */
    private static void requireWritable(String topic, Format<?> format, String side) {
        if (!format.hasSerializer()) {
            throw new IllegalArgumentException("The sandbox cannot generate records for topic " + topic
                    + ": its " + side + " format (" + format + ") can only read. The generator has to encode "
                    + "what it makes with the same format the route decodes it with, so a route declared with a "
                    + "hand-written deserialiser needs a serialiser beside it - Consumed.with(..., "
                    + "Format.of(deserializer, serializer)) - or the route needs feeding by hand.");
        }
    }

    /**
     * One topic of a fluent definition: generate, encode with the route's own serialisers, publish.
     */
    private static final class RouteFeed implements TopicFeed {

        /**
         * The topic this feed publishes into. One feed per topic, not per route: a route with two topics gets two.
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
         * The Java type behind the key format, resolved once at start-up - {@link ValueTypes} owns how, and the
         * route was already refused if the answer was nothing.
         */
        private final Class<?> keyType;

        /**
         * The Java type to fill for the value: the one declared on the builder if there was one, otherwise the one
         * the format carries.
         */
        private final Class<?> valueType;

        /**
         * Where published records go. The same consumer every feed publishes into, which is what makes the
         * published counts one ledger rather than several.
         */
        private final SandboxConsumer<byte[], byte[]> consumer;

        /**
         * This feed's own generator, seeded with the run's seed, so record <em>n</em> of this topic depends on the
         * seed and <em>n</em> and not on how the topics interleaved.
         */
        private final RandomObjects random;

        /**
         * Partitions on this topic, the divisor the key hash is taken modulo.
         */
        private final int partitions;

        /**
         * The size of the key pool this feed draws from.
         */
        private final int keyCardinality;

        /**
         * @param declaredValueType the type declared on the builder for this topic, or null to use the one the
         *                          format carries
         */
        private RouteFeed(String topic,
                          RouteView route,
                          SandboxConsumer<byte[], byte[]> consumer,
                          RandomObjects random,
                          int partitions,
                          int keyCardinality,
                          Class<?> declaredValueType) {
            this.topic = topic;
            this.keyFormat = route.consumedKey();
            this.valueFormat = route.consumedValue();
            this.keyType = ValueTypes.of(keyFormat);
            this.valueType = declaredValueType != null ? declaredValueType : ValueTypes.of(valueFormat);
            this.consumer = consumer;
            this.random = random;
            this.partitions = partitions;
            this.keyCardinality = keyCardinality;
        }

        /**
         * Named in the generator's log when a publish reports the consumer closed.
         */
        @Override
        public String topic() {
            return topic;
        }

        /**
         * One record: generate the key and value, encode both with the route's own serialisers, place it by the
         * encoded key's hash, publish.
         *
         * @param index this feed's record index, which is what makes the record reproducible
         * @return false once the consumer has closed, which is how the generator learns the run is over
         */
        @Override
        public boolean publish(long index) {
            Object key = random.key(keyType, index, keyCardinality);
            Object value = random.create(valueType, index);
            byte[] keyBytes = encode(keyFormat, key);
            byte[] valueBytes = encode(valueFormat, value);
            // Hashed over the ENCODED KEY BYTES, which is the only thing every key type here agrees on.
            //
            // Kafka's default partitioner also hashes the serialised key rather than the object - it murmur2s
            // those bytes - so this places a key on the same partition every time and reproduces the same
            // placement for the same seed, which is what a key-ordered sandbox run and Builder#seed both promise.
            // It does NOT put a key on the same partition a broker would; nothing here needs that, and claiming
            // it would be false.
            //
            // Hashing the key OBJECT is what this used to do, and it silently defeated both promises for every
            // key type but String: RandomObjects.key hands back a fresh byte[] per call for Format.bytes(), and a
            // freshly instantiated POJO for anything else, neither of which overrides hashCode - so
            // Objects.hashCode was the IDENTITY hash, different for every record of the same logical key and
            // different between two runs of one seed.
            int partition = Math.floorMod(Arrays.hashCode(keyBytes), partitions);
            return consumer.publish(topic, partition, keyBytes, valueBytes) >= 0;
        }

        /**
         * The route's serialiser applied to a generated value. The cast is unchecked because a {@code Format<?>}
         * has lost the link between its serialiser and the type resolved beside it; what makes it safe is that
         * both came from the same route and the type came from that format.
         */
        @SuppressWarnings("unchecked")
        private byte[] encode(Format<?> format, Object value) {
            Serializer<Object> serializer = (Serializer<Object>) format.serializer();
            return serializer.serialize(topic, value);
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
         * console output can be read as it goes.
         */
        private double perSecond = 50;

        /**
         * No bound by default, because the default caller is a demo that stops when its handle is closed. A test
         * declares one.
         */
        private Bound bound = Bound.none();

        /**
         * Zero unless asked otherwise - a fixed default rather than a random one, so that two runs of an
         * unchanged program are the same run.
         */
        private long seed;

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
         * Value types declared by topic, for routes whose format cannot name one. Ordered, so a refusal listing
         * what was declared lists it the way it was written.
         */
        private final Map<String, Class<?>> declaredTypes = new LinkedHashMap<>();

        /**
         * Private: reached through {@link Sandbox#builder()}.
         */
        private Builder() {
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
         * When to stop generating and close, draining first. Unbounded by default.
         */
        public Builder bound(Bound bound) {
            this.bound = Objects.requireNonNull(bound, "A bound must be supplied - use Bound.none() for none");
            return this;
        }

        /**
         * Two runs with the same seed generate the same records, in the same order, with the same timestamps -
         * so a sandbox failure is reproducible rather than merely likely to recur.
         */
        public Builder seed(long seed) {
            this.seed = seed;
            return this;
        }

        /**
         * How many partitions each generated topic has. One by default: more of them is what a partition-ordered
         * definition needs to show any parallelism at all.
         */
        public Builder partitionsPerTopic(int partitions) {
            this.partitionsPerTopic = partitions;
            return this;
        }

        /**
         * How many distinct keys the generator draws from. Keys must repeat for key ordering to mean anything,
         * and a cardinality of one puts every record on a single shard - which is the quickest way to see what
         * ordering costs.
         */
        public Builder keyCardinality(int distinctKeys) {
            if (distinctKeys < 1) {
                throw new IllegalArgumentException("A key cardinality of " + distinctKeys + " leaves no keys to "
                        + "generate");
            }
            this.keyCardinality = distinctKeys;
            return this;
        }

        /**
         * Name the value type for a topic whose route cannot - a custom serde over a class the format does not
         * carry. Without this such a route is refused at start, naming the topic.
         */
        public Builder generating(String topic, Class<?> valueType) {
            declaredTypes.put(Objects.requireNonNull(topic, "A topic must be supplied"),
                    Objects.requireNonNull(valueType, "A value type must be supplied"));
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
