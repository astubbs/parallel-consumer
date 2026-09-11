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

/**
 * The classic API's sandbox: the same mock consumer, producer and generator, handed to an options builder rather
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
 *     classic.startGenerating(pc);
 *     classic.awaitBound(Duration.ofSeconds(30));
 * }
 * }</pre>
 *
 * <h2>Why this is wiring rather than one call</h2>
 * The classic API takes a finished consumer and has no seam that sees the instance afterwards, so the two things
 * the fluent path gets for free have to be said out loud here: <b>when</b> to assign the partitions (after
 * {@code subscribe}, or there is no rebalance listener to assign to) and <b>what</b> to close when the bound is
 * reached. {@link #startGenerating} is both.
 *
 * <h2>Nothing is encoded on this path</h2>
 * A mock consumer holds records of the user's own types, so the generated objects go in as they are and the
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
     * The key type to generate. Declared by the caller rather than read from a definition, because a classic
     * application's types live in its options builder's generics and erasure has taken them by the time anything
     * here could look.
     */
    private final Class<K> keyType;

    /**
     * The value type to generate, declared for the same reason as {@link #keyType}.
     */
    private final Class<V> valueType;

    /**
     * The topics to generate into - the ones the caller will subscribe its instance to. Unmodifiable, because the
     * consumer below was built with these exact partitions and a later addition would have nowhere to go.
     */
    private final List<String> topics;

    /**
     * Partitions per topic, which is what makes key ordering visible: with one partition every key lands in the
     * same place and {@code floorMod(anything, 1)} is zero.
     */
    private final int partitionsPerTopic;

    /**
     * The declared rate, per topic, handed to the generator when the run starts.
     */
    private final double perSecond;

    /**
     * When to stop - see {@link Bound}. Kept because {@link #awaitBound(Duration)} refuses an unbounded run rather
     * than waiting for something that will never happen.
     */
    private final Bound bound;

    /**
     * The run's seed, given to each feed's own {@link RandomObjects} so that two runs of a seed generate the same
     * records.
     */
    private final long seed;

    /**
     * How many distinct keys to draw from. A pool rather than a key per record, because repeating keys is what
     * makes shard behaviour something a sandbox run can show.
     */
    private final int keyCardinality;

    /**
     * The broker. Built in the constructor rather than on demand, because the caller hands it to its options
     * builder before anything else happens and its beginning offsets have to be recorded before assignment.
     */
    private final SandboxConsumer<K, V> consumer;

    // Both stay null until asked for - a definition that produces nothing never builds a producer, and nothing
    // generates until startGenerating. See the note on the same fields in Sandbox.
    @SuppressWarnings("NullAway.Init")
    private MockProducer<K, V> producer;

    /**
     * Null until {@link #startGenerating}, which is what {@link #awaitBound(Duration)} and
     * {@link #generatedRecords()} each check before reaching for it - a sandbox asked about a run that has not
     * started should say so.
     */
    @SuppressWarnings("NullAway.Init")
    private RecordGenerator generator;

    /**
     * Package-private: a classic sandbox is built by {@link Sandbox#classic(Class, Class, String...)}, so that the
     * settings below come from one builder rather than from eight arguments at a call site.
     */
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
     * generator publishes into, so a subscription and a feed cannot come to disagree.
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
     * @return the producer built by {@link #producer(Serializer, Serializer)}, or null when this definition asked
     * for none
     */
    public MockProducer<K, V> producer() {
        return producer;
    }

    /**
     * Assign the partitions - the instance must already have subscribed - and start generating. When the bound is
     * reached the generator stops, waits until every record it published has been accounted for
     * ({@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}), and only then closes {@code instance} drain
     * first - so what is readable afterwards is the end of the run.
     * <p>
     * That wait also counts parked records, and this path never has any: park is the fluent API's, so nothing here
     * tells the consumer how to find them and it answers zero for every partition, which on this path is the
     * truth.
     *
     * @param instance the Parallel Consumer instance to close at the bound; every processor type implements
     *                 {@link DrainingCloseable}
     */
    public void startGenerating(DrainingCloseable instance) {
        Objects.requireNonNull(instance, "The instance to close at the bound must be supplied");
        if (generator != null) {
            throw new IllegalStateException("This classic sandbox is already generating");
        }
        consumer.assignAfterSeeding();

        List<TopicFeed> feeds = new ArrayList<>();
        for (String topic : topics) {
            feeds.add(new TypedFeed(topic));
        }
        generator = new RecordGenerator(feeds, perSecond, bound, () -> {
            try {
                // See SandboxConsumer#awaitEveryPublishedRecordCommitted: draining is not the same as finishing,
                // so the bound waits for the instance to account for what was published rather than trusting the
                // close to catch up. No parked-count supplier is given: the classic API has no park.
                consumer.awaitEveryPublishedRecordCommitted();
            } finally {
                instance.closeDrainFirst();
            }
        });
        log.info("Classic sandbox running: {} at {}/s per topic, seed {}, {}", topics, perSecond, seed, bound);
        generator.start();
    }

    /**
     * How many records have been generated. Zero before {@link #startGenerating}, rather than a refusal: a caller
     * asking what a run produced before it started has its answer.
     */
    public long generatedRecords() {
        return generator == null ? 0 : generator.generatedRecords();
    }

    /**
     * @see Sandbox#awaitBound(Duration)
     */
    public boolean awaitBound(Duration timeout) {
        if (generator == null) {
            throw new IllegalStateException("This classic sandbox has not started generating");
        }
        if (!bound.isBounded()) {
            throw new IllegalStateException("This classic sandbox is unbounded, so it will never reach a bound - "
                    + "declare one with Sandbox.builder().bound(...)");
        }
        return generator.awaitBound(timeout);
    }

    /**
     * Stops generating. Does <b>not</b> close the instance - the caller built it and owns it, and on this path
     * closing it is {@code pc.close()} or the bound's own close.
     */
    @Override
    public void close() {
        if (generator != null) {
            generator.close();
        }
    }

    /**
     * One topic of a classic definition: generate, publish, no encoding.
     */
    private final class TypedFeed implements TopicFeed {

        /**
         * The topic this feed publishes into, which is also the only thing that distinguishes two feeds of a
         * classic sandbox - the types are the sandbox's, not the route's.
         */
        private final String topic;

        /**
         * A generator per feed, each seeded with the run's seed, so that record <em>n</em> of a topic depends on
         * the seed and <em>n</em> alone and not on how the topics interleaved.
         */
        private final RandomObjects random = RandomObjects.seededWith(seed);

        /**
         * @param topic one of the sandbox's topics
         */
        private TypedFeed(String topic) {
            this.topic = topic;
        }

        /**
         * Named in the generator's log line when the run ends because the consumer closed under it.
         */
        @Override
        public String topic() {
            return topic;
        }

        /**
         * One record: a key from the pool, a value filled from the declared type, and a partition chosen by the
         * key's value hash so the key sticks to it.
         *
         * @param index the record's index in this feed's sequence, which is what makes it reproducible
         * @return false once the consumer has closed, which is how the generator learns the run is over
         */
        @Override
        public boolean publish(long index) {
            K key = random.key(keyType, index, keyCardinality);
            V value = random.create(valueType, index);
            int partition = Math.floorMod(valueHashOf(key), partitionsPerTopic);
            return consumer.publish(topic, partition, key, value) >= 0;
        }
    }

    /**
     * A <b>value-based</b> hash of a generated key, so that one key always lands on one partition and a seed
     * reproduces the placement - which is what {@code Sandbox.Builder#seed} promises and what makes a key-ordered
     * run in the sandbox shard the way it would against a broker.
     * <p>
     * Arrays are the case {@code Objects.hashCode} gets wrong, and the case this generator manufactures:
     * {@code RandomObjects#key} builds a <em>fresh</em> {@code byte[]} on every call for a {@code byte[]} key
     * type, so the identity hash differs for every record of the same logical key and differs again between two
     * runs of one seed.
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
