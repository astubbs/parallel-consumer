package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.DrainingCloseable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;

/**
 * The classic API's sandbox: the same mock consumer, producer and driver, handed to an options builder rather
 * than to a definition (R33, AE26).
 *
 * <pre>{@code
 * Sandbox sandbox = Sandbox.builder().bound(Bound.afterRecords(100)).build();
 * try (ClassicSandbox<String, Order> classic = sandbox.classic(String.class, Order.class, "orders")) {
 *     var pc = ParallelStreamProcessor.createEosStreamProcessor(
 *             ParallelConsumerOptions.<String, Order>builder()
 *                     .consumer(classic.consumer())        // .consumer(new KafkaConsumer<>(props)), against a broker
 *                     .ordering(KEY)
 *                     .build());
 *     pc.subscribe(classic.topics());
 *     pc.poll(context -> System.out.println(context.getSingleRecord().value()));
 *     classic.startDriving(pc, index -> "cust-" + (index % 10), index -> new Order("o-" + index));
 *     classic.awaitBound(Duration.ofSeconds(30));
 * }
 * }</pre>
 *
 * <h2>Why this is wiring rather than one call</h2>
 * The classic API takes a finished consumer and has no seam that sees the instance afterwards, so the two things
 * the fluent path gets for free have to be said out loud here: <b>when</b> to assign the partitions (after
 * {@code subscribe}, or there is no rebalance listener to assign to) and <b>what</b> to close when the bound is
 * reached. {@link #startDriving} is both.
 *
 * <h2>Nothing is encoded on this path</h2>
 * A mock consumer holds records of the user's own types, so the objects a feed makes go in as they are and the
 * instance's own deserialisers never run - which is exactly the difference from the fluent path, where the engine
 * reads raw bytes and every record has to be encoded with its route's serialiser first. A classic definition's
 * deserialisation is therefore <em>not</em> exercised by the sandbox, and the documentation says so rather than
 * letting a green run imply it.
 *
 * @param <K> the classic instance's key type
 * @param <V> its value type
 */
@Slf4j
@InterfaceStability.Unstable
public final class ClassicSandbox<K, V> implements AutoCloseable {

    /**
     * The key type to fill. Declared by the caller rather than read from a definition, because a classic
     * application's types live in its options builder's generics and erasure has taken them by the time anything
     * here could look.
     */
    private final Class<K> keyType;

    /**
     * The value type to fill, declared for the same reason as {@link #keyType}.
     */
    private final Class<V> valueType;

    /**
     * The topics to publish into - the ones the caller will subscribe its instance to. Unmodifiable, because the
     * consumer below was built with these exact partitions and a later addition would have nowhere to go.
     */
    private final List<String> topics;

    /**
     * Partitions per topic, which is what makes key ordering visible: with one partition every key lands in the
     * same place and {@code floorMod(anything, 1)} is zero.
     */
    private final int partitionsPerTopic;

    /**
     * The declared rate, per topic, handed to the driver when the run starts.
     */
    private final double perSecond;

    /**
     * When to stop - see {@link Bound}. Kept because {@link #awaitBound(Duration)} refuses an unbounded run rather
     * than waiting for something that will never happen.
     */
    private final Bound bound;

    /**
     * The broker. Built in the constructor rather than on demand, because the caller hands it to its options
     * builder before anything else happens and its beginning offsets have to be recorded before assignment.
     */
    private final SandboxConsumer<K, V> consumer;

    // Both stay null until asked for - a definition that produces nothing never builds a producer, and nothing
    // is driven until startDriving. See the note on the same fields in Sandbox.
    @SuppressWarnings("NullAway.Init")
    private MockProducer<K, V> producer;

    /**
     * Whether this sandbox has settled since the last record the caller piped in - see the field of the same name
     * on {@link Sandbox}, which owns the reasoning. Every read on a {@link SandboxOutputTopic} checks it.
     */
    private final AtomicBoolean settled = new AtomicBoolean();

    /**
     * Null until {@link #startDriving}, which is what {@link #awaitBound(Duration)} and
     * {@link #drivenRecords()} each check before reaching for it - a sandbox asked about a run that has not
     * started should say so.
     */
    @SuppressWarnings("NullAway.Init")
    private RecordDriver driver;

    /**
     * Package-private: a classic sandbox is built by {@link Sandbox#classic(Class, Class, String...)}, so that the
     * settings below come from one builder rather than from eight arguments at a call site.
     */
    /**
     * The run's seed, given to each feed's own {@link RandomObjects} so that two runs of a seed fill the same
     * records.
     */
    private final long seed;

    /**
     * How many distinct keys to draw from. A pool rather than a key per record, because repeating keys is what
     * makes shard behaviour something a sandbox run can show.
     */
    private final int keyCardinality;

    ClassicSandbox(Class<K> keyType,
                   Class<V> valueType,
                   Collection<String> topics,
                   int partitionsPerTopic,
                   double perSecond,
                   Bound bound,
                   long seed,
                   int keyCardinality) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.topics = Collections.unmodifiableList(new ArrayList<>(topics));
        this.partitionsPerTopic = partitionsPerTopic;
        this.perSecond = perSecond;
        this.bound = bound;
        this.seed = seed;
        this.keyCardinality = keyCardinality;
        this.consumer = new SandboxConsumer<>(this.topics, partitionsPerTopic);
    }

    /**
     * The consumer to hand to {@code ParallelConsumerOptions.builder().consumer(...)}. Already seeded, not yet
     * assigned.
     */
    public SandboxConsumer<K, V> consumer() {
        return consumer;
    }

    /**
     * The topics to subscribe the instance to - {@code pc.subscribe(classic.topics())}. The same list the
     * driver publishes into, so a subscription and a feed cannot come to disagree.
     */
    public List<String> topics() {
        return topics;
    }

    /**
     * A mock producer for a definition that produces or commits transactionally. The serialisers are the
     * instance's own, because on this path Parallel Consumer serialises what it produces - unlike the consumer
     * side, where nothing is encoded at all.
     * <p>
     * Call this before building the options, and hand the result to {@code .producer(...)}. Under the
     * transactional commit mode Parallel Consumer calls {@code initTransactions} on it itself, so
     * {@link MockProducer#transactionInitialized()} afterwards is the honest test of whether the instance
     * started transactional.
     */
    public MockProducer<K, V> producer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        Objects.requireNonNull(keySerializer, "A key serializer must be supplied");
        Objects.requireNonNull(valueSerializer, "A value serializer must be supplied");
        producer = new MockProducer<>(true, keySerializer, valueSerializer);
        // Under the transactional commit mode the offsets go to the broker through the producer and never reach
        // the consumer at all, so the bound's wait has to be able to see them there too.
        consumer.alsoCountingCommitsThrough(producer);
        return producer;
    }

    /**
     * The raw producer built by {@link #producer(Serializer, Serializer)}, for a test that needs something an output
     * topic does not expose - whether transactions were initialised, the transactional offset history, every
     * topic's records at once.
     * <p>
     * <b>To read what came out, reach for {@link #createOutputTopic(String)}</b>: that is where the read verbs live,
     * with a cursor and the settle guard, and it is where a test should normally look. This accessor is the escape
     * hatch under it, and it hands back a client rather than records.
     *
     * @return that producer, or null when this definition asked for none
     */
    public MockProducer<K, V> producer() {
        return producer;
    }

    /**
     * Assign the partitions, and nothing else - the caller-published half of this class, and the classic
     * counterpart of {@code Sandbox.builder().handPublished()}.
     * <p>
     * The instance must already have subscribed, or there is no rebalance listener to assign to. After this,
     * {@link #pipe(String, Object, Object)} and {@link #awaitSettled()} are the whole shape: pipe what the
     * test knows, wait for the instance to account for it, assert, then close the instance yourself - on this API
     * the caller built it, so the caller closes it.
     * <p>
     * There is no builder switch here, unlike the fluent path, because this API has no seam that starts anything
     * on its own: {@link #startDriving} is already an explicit call, so <em>not</em> making it is the opt-out.
     */
    public void assignAfterSeeding() {
        consumer.assignAfterSeeding();
    }

    /**
     * Pipes one record in from the calling thread. Nothing is encoded on this path - a mock consumer holds
     * records of the instance's own types - so what the function receives is the very object handed over here.
     * <p>
     * The partition is chosen by the key's value hash, the same way a driven record's is, so one key sticks to one
     * partition and a key-ordered classic instance shards in the sandbox the way it would against a broker.
     *
     * @param topic one of this sandbox's topics
     * @return the offset it was published at
     * @throws IllegalStateException    if this sandbox's partitions have not been assigned, or the consumer has
     *                                  closed
     * @throws IllegalArgumentException naming this sandbox's topics, if it does not hold the one named
     */
    public long pipe(String topic, K key, V value) {
        Objects.requireNonNull(topic, "A topic must be supplied");
        requireTopic(topic);
        int partition = Math.floorMod(valueHashOf(key), partitionsPerTopic);
        // This record is now in flight, so anything read out of the run is a race until something settles again.
        settled.set(false);
        long offset = consumer.publish(topic, partition, key, value);
        if (offset < 0) {
            throw new IllegalStateException("This classic sandbox's consumer has closed, so " + topic + " can take "
                    + "no more records - the instance is no longer running.");
        }
        return offset;
    }

    /**
     * One topic's way in as an object - {@code classic.createInputTopic("orders").pipeInput(key, value)} - the shape
     * the broker-free test drivers of the stream-processing libraries users compare us with have.
     * <p>
     * {@link #pipe(String, Object, Object)} is the flat form, and both reach the same partition choice. Nothing is
     * encoded on this path either way, so the instance's function receives the very object handed over.
     *
     * @param topic one of this sandbox's topics
     * @throws IllegalArgumentException naming this sandbox's topics, if it does not hold the one named
     */
    public SandboxInputTopic<K, V> createInputTopic(String topic) {
        Objects.requireNonNull(topic, "A topic must be supplied");
        requireTopic(topic);
        return new SandboxInputTopic<>(topic, this::pipe);
    }

    /**
     * One topic's way out: what the instance produced onto it, which on this API is what a {@code pollAndProduce}
     * definition writes. {@link SandboxOutputTopic} owns the contract - reading consumes, and every read refuses
     * until the run has settled.
     * <p>
     * The topic is not checked against this sandbox's topics: those are the ones the instance consumes, and a
     * definition produces wherever its function says.
     *
     * @throws IllegalStateException if this definition asked for no producer, because then there is no history to
     *                               read - build one with {@link #producer(Serializer, Serializer)} before the
     *                               options
     */
    public SandboxOutputTopic<K, V> createOutputTopic(String topic) {
        Objects.requireNonNull(topic, "A topic must be supplied");
        if (producer == null) {
            throw new IllegalStateException("This classic sandbox has no producer, so nothing records what the "
                    + "instance produced - build one with producer(keySerializer, valueSerializer) and hand it to "
                    + "ParallelConsumerOptions.builder().producer(...) before reading " + topic + ".");
        }
        return new SandboxOutputTopic<>(topic, producer::history, settled::get);
    }

    /**
     * Refuses a topic this sandbox does not hold, naming the ones it does. One place, so the refusal a pipe gives
     * and the refusal an input topic gives are the same sentence.
     */
    private void requireTopic(String topic) {
        if (!topics.contains(topic)) {
            throw new IllegalArgumentException("This classic sandbox does not hold topic " + topic + " - it holds "
                    + topics + ". A record for a topic the instance is not subscribed to would never be "
                    + "delivered.");
        }
    }

    /**
     * Blocks until the instance has accounted for every record published so far, each one committed - so that what
     * a test asserts next is the end of the work rather than the middle of it.
     * <p>
     * No park is counted, because this API has none: a classic record either completes or is retried for ever. The
     * arithmetic is otherwise the fluent path's, and {@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}
     * owns it, including why a committed offset rather than a delivery count is what "completed" reads off.
     * <p>
     * It refuses when the budget runs out with records outstanding, and also when the instance shut down under the
     * wait leaving records outstanding - which the underlying wait returns quietly for, that being how an
     * unbounded driven run ordinarily ends. There is no instance failure to surface here: the classic API has no
     * instance to record one on.
     *
     * @throws IllegalStateException naming the partitions that never got there and what each published, completed
     *                               and parked
     */
    public void awaitSettled() {
        consumer.awaitEveryPublishedRecordCommitted();
        refuseIfAnythingIsOutstanding();
        settled.set(true);
    }

    /**
     * @param budget how long to wait before refusing, for a test whose subject is the refusal
     * @see #awaitSettled()
     */
    public void awaitSettled(Duration budget) {
        Objects.requireNonNull(budget, "A budget must be supplied");
        consumer.awaitEveryPublishedRecordCommitted(budget);
        refuseIfAnythingIsOutstanding();
        settled.set(true);
    }

    /**
     * Catches the wait's quiet ending: it returns rather than refusing when the consumer closes under it, so a run
     * that was closed mid-flight would otherwise let a test assert on a half-finished one.
     */
    private void refuseIfAnythingIsOutstanding() {
        consumer.whatIsNotAccountedFor().ifPresent(outstanding -> {
            throw new IllegalStateException("The sandbox stopped waiting before the instance accounted for what "
                    + "was published, and these partitions never got there: " + outstanding
                    + ". The instance was closed while records were still in flight.");
        });
    }

    /**
     * Assign the partitions - the instance must already have subscribed - and start driving. When the bound is
     * reached the driver stops, waits until every record it published has been accounted for
     * ({@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}), and only then closes {@code instance} drain
     * first - so what is readable afterwards is the end of the run.
     * <p>
     * That wait also counts parked records, and this path never has any: park is the fluent API's, so nothing here
     * tells the consumer how to find them and it answers zero for every partition, which on this path is the
     * truth.
     *
     * Records are filled from the key and value types this sandbox was built with, addressed by the record's
     * index within its topic rather than by sequence, so record <em>n</em> of a topic is the same whatever order
     * the topics were served in and whatever the pacing did.
     *
     * @param instance the Parallel Consumer instance to close at the bound; every processor type implements
     *                 {@link DrainingCloseable}
     */
    public void startDriving(DrainingCloseable instance) {
        RandomObjects random = RandomObjects.seededWith(seed);
        startDriving(instance,
                index -> random.key(keyType, index, keyCardinality),
                index -> random.create(valueType, index));
    }

    /**
     * The same, with the records to publish said out loud rather than filled with realistic random data - for a
     * run that is about particular values.
     *
     * @see #startDriving(DrainingCloseable)
     */
    public void startDriving(DrainingCloseable instance, LongFunction<K> keys, LongFunction<V> values) {
        Objects.requireNonNull(instance, "The instance to close at the bound must be supplied");
        Objects.requireNonNull(keys, "A key function must be supplied");
        Objects.requireNonNull(values, "A value function must be supplied");
        if (driver != null) {
            throw new IllegalStateException("This classic sandbox is already driving");
        }
        consumer.assignAfterSeeding();

        List<TopicFeed> feeds = new ArrayList<>();
        for (String topic : topics) {
            feeds.add(new TypedFeed(topic, keys, values));
        }
        driver = new RecordDriver(feeds, perSecond, bound, () -> {
            try {
                // See SandboxConsumer#awaitEveryPublishedRecordCommitted: draining is not the same as finishing,
                // so the bound waits for the instance to account for what was published rather than trusting the
                // close to catch up. No parked-count supplier is given: the classic API has no park.
                consumer.awaitEveryPublishedRecordCommitted();
            } finally {
                instance.closeDrainFirst();
            }
        });
        log.info("Classic sandbox driving: {} at {}/s per topic, seed {}, {}", topics, perSecond, seed, bound);
        driver.start();
    }

    /**
     * How many records the driver has put in. Zero before {@link #startDriving}, rather than a refusal: a caller
     * asking what a run produced before it started has its answer.
     */
    public long drivenRecords() {
        return driver == null ? 0 : driver.drivenRecords();
    }

    /**
     * @see Sandbox#awaitBound(Duration)
     */
    public boolean awaitBound(Duration timeout) {
        if (driver == null) {
            throw new IllegalStateException("This classic sandbox has not started driving");
        }
        if (!bound.isBounded()) {
            throw new IllegalStateException("This classic sandbox is unbounded, so it will never reach a bound - "
                    + "declare one with Sandbox.builder().bound(...)");
        }
        boolean reached = driver.awaitBound(timeout);
        if (reached) {
            // A reached bound has already waited for every driven record to be accounted for, so it settles this
            // sandbox as surely as awaitSettled does - and earns the reads on an output topic.
            settled.set(true);
        }
        return reached;
    }

    /**
     * Stops driving. Does <b>not</b> close the instance - the caller built it and owns it, and on this path
     * closing it is {@code pc.close()} or the bound's own close.
     */
    @Override
    public void close() {
        if (driver != null) {
            driver.close();
        }
    }

    /**
     * One topic of a classic definition: a value per index, published as it is, no encoding.
     */
    private final class TypedFeed implements TopicFeed {

        /**
         * The topic this feed publishes into, which is also the only thing that distinguishes two feeds of a
         * classic sandbox - the types are the sandbox's, not the route's.
         */
        private final String topic;

        /**
         * This feed's key for a given record index - the caller's, shared by every topic of this sandbox because
         * a classic sandbox has one pair of types rather than one per route.
         */
        private final LongFunction<K> keys;

        /**
         * This feed's value for a given record index, the caller's for the same reason as {@link #keys}.
         */
        private final LongFunction<V> values;

        /**
         * @param topic one of the sandbox's topics
         */
        private TypedFeed(String topic, LongFunction<K> keys, LongFunction<V> values) {
            this.topic = topic;
            this.keys = keys;
            this.values = values;
        }

        /**
         * Named in the driver's log line when the run ends because the consumer closed under it.
         */
        @Override
        public String topic() {
            return topic;
        }

        /**
         * One record: ask for the key and the value at this index, and put it on the partition the key's value
         * hash names so the key sticks to it.
         *
         * @param index the record's index in this feed's sequence, which is what makes it reproducible
         * @return false once the consumer has closed, which is how the driver learns the run is over
         */
        @Override
        public boolean publish(long index) {
            K key = keys.apply(index);
            V value = values.apply(index);
            int partition = Math.floorMod(valueHashOf(key), partitionsPerTopic);
            return consumer.publish(topic, partition, key, value) >= 0;
        }
    }

    /**
     * A <b>value-based</b> hash of a key, so that one key always lands on one partition and the same run places
     * it the same way twice - which is what makes a key-ordered run in the sandbox shard the way it would against
     * a broker.
     * <p>
     * Arrays are the case {@code Objects.hashCode} gets wrong, and the case a key function manufactures: one that
     * builds a <em>fresh</em> {@code byte[]} on every call overrides no {@code hashCode}, so the identity hash
     * differs for every record of the same logical key and differs again between two runs of the same program.
     * <p>
     * This is deliberately the same treatment {@code ShardKey.KeyWithEquals} gives a key when the engine above
     * shards on it - arrays by value, everything else by its own {@code hashCode} - so the sandbox partitions a
     * key the way the engine shards one. A key type whose {@code hashCode} is the inherited identity one is
     * therefore as unusable for key ordering here as it is against a real broker.
     * <p>
     * The fluent path does not need this: it partitions on the ENCODED key bytes, which every route has because
     * the engine underneath consumes raw bytes - see {@code Sandbox.RouteFeed#publish}.
     */
    private static int valueHashOf(Object key) {
        if (key instanceof byte[]) {
            return Arrays.hashCode((byte[]) key);
        }
        if (key instanceof Object[]) {
            return Arrays.deepHashCode((Object[]) key);
        }
        return Objects.hashCode(key);
    }
}
