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

    private final Class<K> keyType;

    private final Class<V> valueType;

    private final List<String> topics;

    private final int partitionsPerTopic;

    private final double perSecond;

    private final Bound bound;

    private final long seed;

    private final int keyCardinality;

    private final SandboxConsumer<K, V> consumer;

    // Both stay null until asked for - a definition that produces nothing never builds a producer, and nothing
    // generates until startGenerating. See the note on the same fields in Sandbox.
    @SuppressWarnings("NullAway.Init")
    private MockProducer<K, V> producer;

    @SuppressWarnings("NullAway.Init")
    private RecordGenerator generator;

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
        boolean finished = generator.awaitFinished(timeout);
        generator.rethrowAnyFailure();
        return finished && generator.boundWasReached();
    }

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

        private final String topic;

        private final RandomObjects random = RandomObjects.seededWith(seed);

        private TypedFeed(String topic) {
            this.topic = topic;
        }

        @Override
        public String topic() {
            return topic;
        }

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
