package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * One side of a route's typing: how bytes become a value, and - when it is known - how a value becomes bytes.
 * <p>
 * It is a Kafka {@link Serde} so that {@link org.apache.kafka.common.serialization.Serdes Serdes.String()} and a
 * format helper from {@link Formats} are interchangeable wherever {@link Consumed} and {@link Produced} take one.
 * The difference from a plain {@code Serde} is that the serialiser may be <em>absent</em>: a route declared with a
 * hand-written {@link Deserializer} can read a topic that nothing here can write. {@link #hasSerializer()} answers
 * that, and the sandbox - which must encode what it hydrates - refuses such a route naming its topic (KTD9).
 *
 * @param <T> the value type this format reads and, when it can, writes
 */
@Slf4j
@InterfaceStability.Unstable
public final class Format<T> implements Serde<T> {

    /**
     * Where this format's deserialisers come from, or null on a write-only format. Null is a legitimate half of this
     * type, which is the one way it differs from a plain {@link Serde} - hence {@link #hasDeserializer()} rather
     * than a cast.
     */
    private final Supplier<Deserializer<T>> deserializerSupplier;

    /**
     * Where this format's serialisers come from, or null on a read-only format - a route may be declared with a
     * hand-written {@link Deserializer} for a topic that nothing here can write (KTD9).
     */
    private final Supplier<Serializer<T>> serializerSupplier;

    /**
     * The deserialiser each thread is using, made for it on first use. <b>Keyed by thread rather than held in a
     * {@link ThreadLocal}</b>, and the reason is {@link #close()}: a thread-local cannot be enumerated, so the
     * instances belonging to other threads could never be closed, and a registry-backed deserialiser holds an HTTP
     * client and a schema cache that must be given back. This map is both the lookup and that registry.
     * <p>
     * It is bounded by the engine's worker pool, which is fixed for the life of an instance, and it is emptied by
     * {@link #close()} - so an entry for a thread that has since died is held only until shutdown.
     */
    private final Map<Thread, Deserializer<T>> deserializerPerThread = new ConcurrentHashMap<>();

    /**
     * The serialiser each thread is using, on the same terms as {@link #deserializerPerThread}.
     */
    private final Map<Thread, Serializer<T>> serializerPerThread = new ConcurrentHashMap<>();

    /**
     * What {@link #configure} was given, or null until it has been called - <b>volatile, and it is the publication
     * edge for every instance this format makes</b>. It is written once on the thread that validates the definition
     * and read by every worker thread that later makes an instance of its own, which must see the configuration
     * rather than configure with nothing. Held as one object so that the properties and the key flag are published
     * together and no reader can see half of a configuration.
     */
    private volatile Configuration configuration;


    /**
     * What {@link #configure} was told, kept together so one volatile write publishes both halves.
     */
    private static final class Configuration {

        private final Map<String, ?> configs;

        private final boolean isKey;

        private Configuration(Map<String, ?> configs, boolean isKey) {
            this.configs = configs;
            this.isKey = isKey;
        }
    }

    /**
     * What this format calls itself in a message. Held rather than derived on demand because a format is named in
     * refusals about the route that declared it, and a deserialiser's class name is all there is to go on once a
     * lambda or an anonymous class has been handed in.
     */
    private final String description;

    /**
     * The Java type this format reads into, or null when nothing told it. Nothing in the facade reads this; it is
     * carried for the sandbox, which cannot hydrate a record for a class it cannot name.
     *
     * @see #type()
     */
    private final Class<T> type;

    /**
     * Private, so that every format arrives through a factory which has already decided which halves it has. The
     * four arguments are not independent: at least one of the two serialisers must be present, and the factories
     * are where that is enforced.
     */
    private Format(Supplier<Deserializer<T>> deserializerSupplier,
                   Supplier<Serializer<T>> serializerSupplier,
                   String description,
                   Class<T> type) {
        this.deserializerSupplier = deserializerSupplier;
        this.serializerSupplier = serializerSupplier;
        this.description = description;
        this.type = type;
    }

    /**
     * A supplier that answers with the one instance it was given, for every factory that takes a finished
     * serialiser: those formats share it, which is what their own javadoc says.
     * <p>
     * <b>A null instance yields a null supplier, not a supplier of null</b>, because an absent half is a real state
     * of this type and {@link #hasSerializer()} answers from the supplier. A format helper whose serialiser is not
     * on the classpath passes null deliberately - see {@code Formats}, where the serialiser is documented as
     * optional - and wrapping that in a supplier would make the format claim it can write and then fail with a
     * {@code NullPointerException} at the first produced record, instead of being refused by {@link Produced} the
     * way an unwritable format is.
     */
    private static <S> Supplier<S> shared(S instance) {
        return instance == null ? null : () -> instance;
    }

    /**
     * A format that can only read, from one deserialiser.
     * <p>
     * <b>That instance is shared by every worker thread</b>, so it must be thread-safe. Kafka's own stock
     * deserialisers are; a stateful one of your own is not, and {@link #readingPerWorker(Supplier)} is what gives
     * each worker its own.
     */
    public static <T> Format<T> reading(Deserializer<T> deserializer) {
        Objects.requireNonNull(deserializer, "A deserializer must be supplied");
        return new Format<>(shared(deserializer), null, deserializer.getClass().getSimpleName(), null);
    }

    /**
     * A format that can only read, giving <b>each worker thread its own deserialiser</b> - the declaration for a
     * deserialiser that holds state and is therefore only safe on one thread, which is all Kafka's interface
     * promises.
     * <p>
     * The supplier is called once per worker thread and its instance reused for every record that thread decodes,
     * never once per record. It is also called once here, so that a supplier which cannot produce an instance fails
     * at definition time and so that the format has something to name itself after; that instance is configured and
     * closed with the rest.
     *
     * @param deserializers called once per worker thread, and must return a fresh instance each time - a supplier
     *                      that returns the same object every time is the shared case with extra steps
     */
    public static <T> Format<T> readingPerWorker(Supplier<Deserializer<T>> deserializers) {
        Objects.requireNonNull(deserializers, "A deserializer supplier must be supplied");
        return new Format<>(deserializers, null, describeFirst(deserializers, "deserializer"), null);
    }

    /**
     * A format that can only write - the produced side of a route whose values nothing here needs to read back.
     * <p>
     * <b>That instance is shared by every worker thread</b> and must be thread-safe; see {@link #reading} for the
     * whole of that argument, and {@link #writingPerWorker(Supplier)} for the isolated form.
     */
    public static <T> Format<T> writing(Serializer<T> serializer) {
        Objects.requireNonNull(serializer, "A serializer must be supplied");
        return new Format<>(null, shared(serializer), serializer.getClass().getSimpleName(), null);
    }

    /**
     * A write-only format giving <b>each worker thread its own serialiser</b>, the mirror of
     * {@link #readingPerWorker(Supplier)} - a produce path runs on the same worker threads a decode does, so a
     * stateful serialiser needs the same isolation.
     */
    public static <T> Format<T> writingPerWorker(Supplier<Serializer<T>> serializers) {
        Objects.requireNonNull(serializers, "A serializer supplier must be supplied");
        return new Format<>(null, serializers, describeFirst(serializers, "serializer"), null);
    }

    /**
     * A format that can read and write. <b>Both instances are shared across workers</b> and must be thread-safe -
     * {@link #perWorker(Supplier, Supplier)} is the isolated form.
     */
    public static <T> Format<T> of(Deserializer<T> deserializer, Serializer<T> serializer) {
        Objects.requireNonNull(deserializer, "A deserializer must be supplied");
        Objects.requireNonNull(serializer, "A serializer must be supplied");
        return new Format<>(shared(deserializer), shared(serializer), deserializer.getClass().getSimpleName(), null);
    }

    /**
     * A read-and-write format giving <b>each worker thread its own pair</b>.
     *
     * @see #readingPerWorker(Supplier)
     */
    public static <T> Format<T> perWorker(Supplier<Deserializer<T>> deserializers,
                                          Supplier<Serializer<T>> serializers) {
        Objects.requireNonNull(deserializers, "A deserializer supplier must be supplied");
        Objects.requireNonNull(serializers, "A serializer supplier must be supplied");
        return new Format<>(deserializers, serializers, describeFirst(deserializers, "deserializer"), null);
    }

    /**
     * A format that can read and write, naming the Java type it carries - which is what lets the sandbox feed a
     * route declared with hand-written serialisers.
     *
     * @see #type()
     */
    public static <T> Format<T> of(Deserializer<T> deserializer, Serializer<T> serializer, Class<T> type) {
        Objects.requireNonNull(deserializer, "A deserializer must be supplied");
        Objects.requireNonNull(serializer, "A serializer must be supplied");
        return new Format<>(shared(deserializer), shared(serializer), deserializer.getClass().getSimpleName(), type);
    }

    /**
     * The per-worker form of {@link #of(Deserializer, Serializer, Class)}, naming the Java type as that does.
     *
     * @see #readingPerWorker(Supplier)
     */
    public static <T> Format<T> perWorker(Supplier<Deserializer<T>> deserializers,
                                          Supplier<Serializer<T>> serializers,
                                          Class<T> type) {
        Objects.requireNonNull(deserializers, "A deserializer supplier must be supplied");
        Objects.requireNonNull(serializers, "A serializer supplier must be supplied");
        return new Format<>(deserializers, serializers, describeFirst(deserializers, "deserializer"), type);
    }

    /**
     * A format from a Kafka {@link Serde}, which always has both halves.
     * <p>
     * <b>A serde hands out one instance of each half, so both are shared across workers</b> and must be thread-safe.
     * There is no per-worker form taking a serde, because a serde is the wrong shape to ask twice: supply the two
     * suppliers to {@link #perWorker(Supplier, Supplier)} instead.
     */
    public static <T> Format<T> of(Serde<T> serde) {
        if (serde instanceof Format) {
            @SuppressWarnings("unchecked") Format<T> already = (Format<T>) serde;
            return already;
        }
        Objects.requireNonNull(serde, "A serde must be supplied");
        return new Format<>(shared(serde.deserializer()), shared(serde.serializer()),
                serde.getClass().getSimpleName(), null);
    }

    /**
     * The factory for this package's own format helpers, which are the only callers that can name a format better
     * than its deserialiser's class does - {@code json(Order.class)} reads as itself in a refusal, where the public
     * factories can only report whatever class the user handed in. Package-private for that reason.
     */
    static <T> Format<T> named(Deserializer<T> deserializer,
                               Serializer<T> serializer,
                               String description,
                               Class<T> type) {
        return new Format<>(shared(deserializer), shared(serializer), description, type);
    }

    /**
     * The per-worker form of {@link #named}, for a helper whose serialisers are not safe to share.
     */
    static <T> Format<T> namedPerWorker(Supplier<Deserializer<T>> deserializers,
                                        Supplier<Serializer<T>> serializers,
                                        String description,
                                        Class<T> type) {
        return new Format<>(deserializers, serializers, description, type);
    }

    /**
     * Names a per-worker format after an instance its supplier makes, and refuses a supplier that makes none.
     * <p>
     * A format is named inside refusals about the route that declared it, and a supplier has no name worth printing -
     * a lambda's class name is noise - so one instance is made here purely to read a class name off, and
     * <b>closed again immediately</b>. It is deliberately not kept: it belongs to whichever thread happened to
     * declare the route, which is not a worker, so keeping it would hold a deserialiser nothing will ever decode
     * with until shutdown. It is closed rather than dropped because constructing a registry-backed deserialiser may
     * already have opened an HTTP client, and it has not been configured, which is the one state every Kafka serde
     * tolerates being closed in.
     * <p>
     * The real value is the refusal: a supplier that returns null, or throws, fails here at definition time rather
     * than on the first record to reach some worker thread.
     */
    private static <S> String describeFirst(Supplier<S> supplier, String what) {
        S first = supplier.get();
        if (first == null) {
            throw new IllegalArgumentException("The " + what + " supplier returned null - it is called once per "
                    + "worker thread and must return an instance every time");
        }
        String name = first.getClass().getSimpleName();
        closeQuietly(first, "the instance made to name a per-worker format");
        return name;
    }

    /**
     * Closes one serialiser or deserialiser, reporting a failure rather than propagating it.
     * <p>
     * Every caller is either shutting down or giving back something it only made to look at, and in both cases one
     * instance refusing to close must not stop the others being closed or replace the reason the caller was already
     * there.
     */
    private static void closeQuietly(Object closeable, String what) {
        try {
            if (closeable instanceof Deserializer) {
                ((Deserializer<?>) closeable).close();
            } else if (closeable instanceof Serializer) {
                ((Serializer<?>) closeable).close();
            }
        } catch (RuntimeException closeFailed) {
            log.warn("Closing {} threw; carrying on", what, closeFailed);
        }
    }

    /**
     * How this format reads bytes into its type. Handed to the dispatch path rather than used here, so that
     * decoding runs inside the record's own attempt and a bad payload is a record outcome, never a poll-thread
     * failure.
     *
     * @return the deserialiser, or null when this format can only write - test with {@link #hasDeserializer()} first
     */
    @Override
    public Deserializer<T> deserializer() {
        if (deserializerSupplier == null) {
            return null;
        }
        // computeIfAbsent, so two workers arriving at once cannot each make one and one of them lose it - and each
        // thread only ever computes its own key, so there is nothing here for them to contend on.
        return deserializerPerThread.computeIfAbsent(Thread.currentThread(), thread -> configured(
                Objects.requireNonNull(deserializerSupplier.get(), "A deserializer supplier returned null"),
                Deserializer::configure));
    }

    /**
     * Whether this format can read as well as write. Every consumed side of a route needs this to be true.
     */
    public boolean hasDeserializer() {
        return deserializerSupplier != null;
    }

    /**
     * How this format writes its type back to bytes, for the produce path and for anything that must feed a route
     * with records it would accept.
     *
     * @return the serialiser, or null when this format can only read - test with {@link #hasSerializer()} first
     */
    @Override
    public Serializer<T> serializer() {
        if (serializerSupplier == null) {
            return null;
        }
        return serializerPerThread.computeIfAbsent(Thread.currentThread(), thread -> configured(
                Objects.requireNonNull(serializerSupplier.get(), "A serializer supplier returned null"),
                Serializer::configure));
    }

    /**
     * Applies whatever {@link #configure} was told to an instance just made, so that a worker's own instance is
     * configured exactly as the one validation configured was.
     * <p>
     * Reads the volatile configuration once: it is written before the engine starts and never after, so a worker
     * either sees a configuration or sees that there was none, and never a half-written one.
     *
     * @param applyConfiguration how to configure this kind of instance - the two kinds share no interface that
     *                           declares {@code configure}, which is why it arrives as a function
     */
    private <S> S configured(S instance, ConfigureStep<S> applyConfiguration) {
        Configuration declared = configuration;
        if (declared != null) {
            applyConfiguration.configure(instance, declared.configs, declared.isKey);
        }
        return instance;
    }

    /**
     * {@code configure} on a serialiser or a deserialiser. Kafka declares that method on both and on no shared
     * supertype, so this is the seam that lets one piece of code configure either.
     */
    private interface ConfigureStep<S> {

        void configure(S instance, Map<String, ?> configs, boolean isKey);
    }

    /**
     * Whether this format can write as well as read. A route that produces, and a route the sandbox must feed,
     * needs this to be true.
     */
    public boolean hasSerializer() {
        return serializerSupplier != null;
    }

    /**
     * The Java type this format reads into, when it is known, and null when it is not.
     * <p>
     * A {@link Formats} helper always knows - {@code json(Order.class)} was told. A format built from a bare
     * {@link Deserializer} or a Kafka {@link Serde} does not: the type is erased and nothing here can recover it.
     * <p>
     * Nothing in the facade needs this. <b>The sandbox does</b>, because hydrating a record means filling an
     * instance of a class, and a route whose type it cannot name is refused there naming the topic (KTD9) - the
     * cure being either a format helper or the {@code Class}-taking factories above.
     */
    public Class<T> type() {
        return type;
    }

    /**
     * Passes the definition's remaining connection properties to both halves, as Kafka clients do for their own
     * serialisers (R4, KTD7). The facade's own keys have already been removed by the caller.
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Recorded first, so that every instance made after this - one per worker thread - is configured with it.
        this.configuration = new Configuration(configs, isKey);
        // Then any instance that already exists, which is the one made to name a per-worker format's own supplier
        // and, on a repeated configure, whatever this thread is holding.
        for (Deserializer<T> existing : deserializerPerThread.values()) {
            existing.configure(configs, isKey);
        }
        for (Serializer<T> existing : serializerPerThread.values()) {
            existing.configure(configs, isKey);
        }
        // And finally one instance of each half this format has, on this thread, so that a configuration the
        // serialiser refuses fails HERE - at definition time, naming the definition - rather than on the first
        // record to reach a worker. It is the property this method had when it held one instance, and the reason
        // validation calls it at all.
        if (hasDeserializer()) {
            Deserializer<T> ignoredProbe = deserializer();
        }
        if (hasSerializer()) {
            Serializer<T> ignoredProbe = serializer();
        }
    }

    /**
     * Closes every instance this format has handed out, on every thread, satisfying {@link Serde#close()} for a type
     * that has as many instances as there were workers.
     * <p>
     * <b>This is why the instances are held in a map rather than a {@link ThreadLocal}.</b> A thread-local can only
     * be read by the thread that owns it, so the worker instances could never be reached from the thread that closes
     * the instance - and each one may hold an HTTP client and a schema cache. The map is emptied as it goes, so a
     * second close finds nothing rather than closing a closed serialiser twice.
     * <p>
     * One instance failing to close does not stop the others: this runs during shutdown, where the remaining
     * instances are exactly as owed as the one that threw.
     */
    @Override
    public void close() {
        closeAndForget(deserializerPerThread);
        closeAndForget(serializerPerThread);
    }

    /**
     * Closes and removes every instance in one of the per-thread maps. Removing as it goes is what makes a second
     * call a no-op rather than a double close.
     * <p>
     * <b>Deduplicated by identity, because not every format has one instance per thread.</b> A format built from a
     * finished serialiser shares it, so every thread's entry holds the same object - and closing it once per worker
     * would call {@code close()} several times on a serde that is contracted to be closed once. The set is keyed on
     * identity rather than equality for the same reason the instance map is: two distinct serialisers that compare
     * equal are still two things to close.
     */
    private static void closeAndForget(Map<Thread, ?> instances) {
        Set<Object> closed = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        for (Map.Entry<Thread, ?> each : instances.entrySet()) {
            if (instances.remove(each.getKey(), each.getValue()) && closed.add(each.getValue())) {
                closeQuietly(each.getValue(), "a per-worker instance of a route format");
            }
        }
    }

    /**
     * The description, alone: a format is printed inside refusals that already name the topic and the side, so
     * anything more here would repeat what surrounds it.
     */
    @Override
    public String toString() {
        return description;
    }
}
