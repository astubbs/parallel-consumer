package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * The format helpers: a route's types named by wire format rather than by deserialiser class (R4).
 * <p>
 * <b>How the library is found.</b> Registry deserialisers are not on Maven Central and most users need none of them,
 * so {@link #avro} and {@link #protobuf} resolve the deserialiser class <em>by name on your classpath</em> and refuse
 * here, naming the missing library, rather than failing at the first record (KTD7). Only the JSON helpers have a
 * declared dependency, Jackson, and it is optional in core. Every refusal happens while the definition is being
 * written - before {@code start}, before any client exists.
 *
 * @see Consumed
 * @see Produced
 */
@InterfaceStability.Unstable
public final class Formats {

    /**
     * Named rather than imported, and that is the point of this whole class: the registry deserialisers are not on
     * Maven Central, so core cannot depend on them and {@link #avro} resolves this by name at definition time
     * instead (KTD7). A string here costs a user who wants none of it nothing at all.
     */
    private static final String AVRO_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    /**
     * The write half of {@link #AVRO_DESERIALIZER}, resolved the same way and treated as optional: a route that
     * only consumes never needs it.
     */
    private static final String AVRO_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer";

    /**
     * As {@link #AVRO_DESERIALIZER}, for {@link #protobuf}.
     */
    private static final String PROTOBUF_DESERIALIZER =
            "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer";

    /**
     * As {@link #AVRO_SERIALIZER}, for {@link #protobuf}, and optional for the same reason.
     */
    private static final String PROTOBUF_SERIALIZER = "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer";

    /**
     * The one class whose presence proves Jackson is on the classpath. Jackson is a declared but <em>optional</em>
     * dependency of core, so this is a presence check rather than a resolution: the JSON helpers use the real types
     * through {@link JacksonFormats}, which is only loaded once this check has passed.
     */
    private static final String JACKSON_OBJECT_MAPPER = "com.fasterxml.jackson.databind.ObjectMapper";

    /**
     * No instances: this is a set of static helpers a route names, not something a user holds.
     */
    private Formats() {
    }

    /**
     * Raw bytes: no decoding at all. Always available.
     */
    public static Format<byte[]> bytes() {
        return Format.named(Serdes.ByteArray().deserializer(), Serdes.ByteArray().serializer(), "bytes()",
                byte[].class);
    }

    /**
     * UTF-8 strings. Always available, and the default key type of every format-named route.
     */
    public static Format<String> string() {
        return Format.named(Serdes.String().deserializer(), Serdes.String().serializer(), "string()", String.class);
    }

    /**
     * JSON decoded into the given class, through Jackson.
     *
     * @throws IllegalStateException at definition time, naming Jackson, when it is not on the classpath
     */
    public static <T> Format<T> json(Class<T> type) {
        Objects.requireNonNull(type, "A type must be supplied - use json() for the class-free map form");
        requireOnClasspath(JACKSON_OBJECT_MAPPER, "json(" + type.getSimpleName() + ")", "Jackson",
                "com.fasterxml.jackson.core:jackson-databind");
        return JacksonFormats.of(type);
    }

    /**
     * JSON with no class: the payload is a map of field names to values, so a topic nobody has a class for can be
     * consumed and inspected by field name with nothing declared (R4).
     *
     * @throws IllegalStateException at definition time, naming Jackson, when it is not on the classpath
     */
    public static Format<Map<String, Object>> json() {
        requireOnClasspath(JACKSON_OBJECT_MAPPER, "json()", "Jackson", "com.fasterxml.jackson.core:jackson-databind");
        return JacksonFormats.ofMap();
    }

    /**
     * Avro through the Confluent Avro deserialiser already on your classpath, reading the specific record type
     * given.
     *
     * @throws IllegalStateException at definition time, naming the library, when it is not on the classpath
     */
    public static <T> Format<T> avro(Class<T> type) {
        Objects.requireNonNull(type, "A type must be supplied");
        String label = "avro(" + type.getSimpleName() + ")";
        requireOnClasspath(AVRO_DESERIALIZER, label, "the Confluent Avro serialiser",
                "io.confluent:kafka-avro-serializer (Confluent's repository, not Maven Central)");
        Map<String, Object> extra = new HashMap<>();
        extra.put("specific.avro.reader", true);
        return reflective(label, AVRO_DESERIALIZER, AVRO_SERIALIZER, extra, type);
    }

    /**
     * Protobuf through the Confluent Protobuf deserialiser already on your classpath, reading the message type
     * given.
     *
     * @throws IllegalStateException at definition time, naming the library, when it is not on the classpath
     */
    public static <T> Format<T> protobuf(Class<T> type) {
        Objects.requireNonNull(type, "A type must be supplied");
        String label = "protobuf(" + type.getSimpleName() + ")";
        requireOnClasspath(PROTOBUF_DESERIALIZER, label, "the Confluent Protobuf serialiser",
                "io.confluent:kafka-protobuf-serializer (Confluent's repository, not Maven Central)");
        Map<String, Object> extra = new HashMap<>();
        extra.put("specific.protobuf.value.type", type.getName());
        extra.put("derive.type", true);
        return reflective(label, PROTOBUF_DESERIALIZER, PROTOBUF_SERIALIZER, extra, type);
    }

    /**
     * Tell permanent decode failures from transient ones, which a stock deserialiser cannot (R12).
     * <p>
     * The classifier sees every exception the wrapped deserialiser throws and answers with
     * {@link Decode#permanentFailure} - park this record now, spending none of its attempts - or
     * {@link Decode#transientFailure}, which is an ordinary failed attempt. Without this wrapper every decode failure
     * is transient, because a deserialiser that throws cannot tell a corrupt payload from a registry outage.
     */
    public static <T> Format<T> classifyDecodeFailures(Serde<T> inner, Function<Exception, Decode<T>> classifier) {
        Objects.requireNonNull(inner, "A format to wrap must be supplied");
        Objects.requireNonNull(classifier, "A classifier must be supplied");
        Format<T> wrapped = Format.of(inner);
        Deserializer<T> classifying = new ClassifyingDeserializer<>(wrapped.deserializer(), classifier);
        return Format.named(classifying, wrapped.serializer(), "classified(" + wrapped + ")", wrapped.type());
    }

    /**
     * Resolves a deserialiser, and its serialiser when the library ships one, by name. The serialiser is optional:
     * a route that only consumes needs none, and a route that produces is refused by {@link Produced} when it is
     * missing.
     */
    private static <T> Format<T> reflective(String label,
                                            String deserializerClass,
                                            String serializerClass,
                                            Map<String, Object> extraConfig,
                                            Class<T> type) {
        @SuppressWarnings("unchecked")
        Deserializer<T> deserializer = (Deserializer<T>) instantiate(deserializerClass, Deserializer.class, label);
        Serializer<T> serializer = null;
        if (isOnClasspath(serializerClass)) {
            @SuppressWarnings("unchecked")
            Serializer<T> resolved = (Serializer<T>) instantiate(serializerClass, Serializer.class, label);
            serializer = new ConfigMergingSerializer<>(resolved, extraConfig);
        }
        return Format.named(new ConfigMergingDeserializer<>(deserializer, extraConfig), serializer, label, type);
    }

    /**
     * Builds one of the resolved classes through its no-argument constructor, and checks it really is what the
     * helper expected before handing it out. The type check matters because the class was named as a string: a
     * classpath holding something else under that name would otherwise fail as a cast at the first record, a long
     * way from the definition that asked for it.
     *
     * @param label the helper's own spelling, such as {@code avro(Order)}, so a refusal names what the user wrote
     *              rather than the class it went looking for
     */
    private static Object instantiate(String className, Class<?> expected, String label) {
        try {
            Object instance = Class.forName(className).getDeclaredConstructor().newInstance();
            if (!expected.isInstance(instance)) {
                throw new IllegalStateException(msg("{} resolved {}, which is not a {}", label, className,
                        expected.getSimpleName()));
            }
            return instance;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(msg("The {} format helper found {} on the classpath but could not "
                    + "construct it - declare the route's deserialisers directly with Consumed.with(...) instead",
                    label, className), e);
        }
    }

    /**
     * Whether a class is there, without initialising it - the {@code false} argument to {@code Class.forName}, so
     * a probe never runs a static initialiser for a library the user may end up not using at all.
     * <p>
     * {@link LinkageError} is caught beside {@link ClassNotFoundException} because a half-present library - the
     * class there, something it extends missing - is as unusable as an absent one and should be reported the same
     * way, at definition time, rather than thrown out of a probe.
     */
    private static boolean isOnClasspath(String className) {
        try {
            Class.forName(className, false, Formats.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /**
     * Refuses <b>here</b>, while the definition is being written, when the library a helper needs is absent - the
     * alternative being a first record that fails at a client that has already been built and has already joined a
     * group (KTD7).
     *
     * @param library     the human name of what is missing, and {@code coordinates} where to get it: a message that
     *                    names only a class leaves the reader to work out which artifact carries it
     * @param coordinates the dependency to add, including the repository when it is not Maven Central
     */
    private static void requireOnClasspath(String className, String label, String library, String coordinates) {
        if (!isOnClasspath(className)) {
            throw new IllegalStateException(msg("The {} format helper needs {} on the classpath and {} is not there "
                            + "- add {}, or declare this route's deserialisers directly with Consumed.with(...)",
                    label, library, className, coordinates));
        }
    }

    /**
     * Adds the helper's own settings on top of the definition's pass-through properties (R4), so a helper that needs
     * a fixed setting - {@code specific.avro.reader}, say - does not make the user repeat it.
     */
    private static Map<String, Object> merge(Map<String, ?> configs, Map<String, Object> extra) {
        Map<String, Object> merged = new HashMap<>(configs);
        merged.putAll(extra);
        return Collections.unmodifiableMap(merged);
    }

    /**
     * Carries a helper's own fixed settings into whatever the definition configures the route with - see
     * {@link #merge}. Everything else is straight delegation.
     */
    private static final class ConfigMergingDeserializer<T> implements Deserializer<T> {

        /**
         * The resolved deserialiser this exists to configure. Held rather than extended because it was built by
         * name and its type is not known here.
         */
        private final Deserializer<T> inner;

        /**
         * The helper's fixed settings, which win over the user's on the way through - they are what makes the
         * helper the format it claims to be, such as the specific-reader flag that decides what Avro hands back.
         */
        private final Map<String, Object> extra;

        /**
         * Built only by {@link #reflective}, which is the one place that knows a resolved deserialiser needs
         * settings the user was not asked for.
         */
        private ConfigMergingDeserializer(Deserializer<T> inner, Map<String, Object> extra) {
            this.inner = inner;
            this.extra = extra;
        }

        /**
         * The one method this wrapper exists for: the inner deserialiser is configured with the definition's
         * pass-through properties plus the helper's own, rather than with either alone.
         */
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            inner.configure(merge(configs, extra), isKey);
        }

        /**
         * Plain delegation - the settings were merged at configure time, so there is nothing left to do per record.
         */
        @Override
        public T deserialize(String topic, byte[] data) {
            return inner.deserialize(topic, data);
        }

        /**
         * Delegates <b>with the headers</b>, which the interface's default form would drop.
         * <p>
         * {@link Deserializer}'s default 3-arg method discards the headers and calls the 2-arg one, so a wrapper
         * that overrides only the 2-arg form silently hides them from the deserialiser it wraps - and the dispatch
         * wrapper calls the 3-arg form for every record. A registry deserialiser that resolves its schema from a
         * header would have seen none.
         */
        @Override
        public T deserialize(String topic, Headers headers, byte[] data) {
            return inner.deserialize(topic, headers, data);
        }

        /**
         * Closes the wrapped deserialiser: this wrapper holds nothing of its own, but a registry deserialiser holds
         * an HTTP client and a cache, and dropping the call would leak them for the life of the process.
         */
        @Override
        public void close() {
            inner.close();
        }
    }

    /**
     * The write-side twin of {@link ConfigMergingDeserializer}, for the routes that also produce.
     */
    private static final class ConfigMergingSerializer<T> implements Serializer<T> {

        /**
         * The resolved serialiser this exists to configure, held for the same reason its read-side twin holds one.
         */
        private final Serializer<T> inner;

        /**
         * The helper's fixed settings, which win over the user's on the way through.
         */
        private final Map<String, Object> extra;

        /**
         * Built only by {@link #reflective}, and only when the library turned out to ship a serialiser at all.
         */
        private ConfigMergingSerializer(Serializer<T> inner, Map<String, Object> extra) {
            this.inner = inner;
            this.extra = extra;
        }

        /**
         * The one method this wrapper exists for - the definition's pass-through properties plus the helper's own.
         */
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            inner.configure(merge(configs, extra), isKey);
        }

        /**
         * Plain delegation - the settings were merged at configure time.
         */
        @Override
        public byte[] serialize(String topic, T data) {
            return inner.serialize(topic, data);
        }

        /**
         * Delegates <b>with the headers</b>, for the same reason the deserialiser above does: the interface's
         * default 3-arg form drops them, and the dispatch wrapper serialises every produced record through it.
         */
        @Override
        public byte[] serialize(String topic, Headers headers, T data) {
            return inner.serialize(topic, headers, data);
        }

        /**
         * Closes the wrapped serialiser, for the reason its read-side twin's close gives.
         */
        @Override
        public void close() {
            inner.close();
        }
    }

    /**
     * @see #classifyDecodeFailures
     */
    private static final class ClassifyingDeserializer<T> implements Deserializer<T> {

        /**
         * The deserialiser whose failures are being classified. It is unchanged by this wrapper: what it throws, it
         * throws, and the only thing decided here is whether that throw is worth retrying.
         */
        private final Deserializer<T> inner;

        /**
         * The user's verdict function. It is the only thing in the library that can tell a corrupt payload from a
         * registry outage, because only the route's author knows what the wrapped deserialiser throws for each
         * (R12).
         */
        private final Function<Exception, Decode<T>> classifier;

        /**
         * Built only by {@link #classifyDecodeFailures}, which has already null-checked both arguments.
         */
        private ClassifyingDeserializer(Deserializer<T> inner, Function<Exception, Decode<T>> classifier) {
            this.inner = inner;
            this.classifier = classifier;
        }

        /**
         * Passes the configuration straight through - unlike the config-merging wrappers above, this one adds no
         * settings of its own, because a classifier changes what a failure means rather than how decoding works.
         */
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            inner.configure(configs, isKey);
        }

        /**
         * Classifies what the wrapped deserialiser throws, and rethrows the original untouched when the classifier
         * does not call the payload permanently undecodable - so an unclassified failure behaves exactly as it did
         * before this wrapper was put round it.
         */
        @Override
        public T deserialize(String topic, byte[] data) {
            try {
                return inner.deserialize(topic, data);
            } catch (Exception decodeFailure) {
                PermanentDecodeFailureException permanent = permanentIfClassifierSaysSo(topic, decodeFailure);
                if (permanent != null) {
                    throw permanent;
                }
                throw decodeFailure;
            }
        }

        /**
         * Classifies exactly as the 2-arg form does, and delegates <b>with the headers</b>.
         * <p>
         * This is the form the dispatch wrapper actually calls. Left to the interface's default it would drop the
         * headers on the way in, so a classifier wrapping a header-driven deserialiser would have been classifying
         * a failure the headers themselves caused.
         */
        @Override
        public T deserialize(String topic, Headers headers, byte[] data) {
            try {
                return inner.deserialize(topic, headers, data);
            } catch (Exception decodeFailure) {
                PermanentDecodeFailureException permanent = permanentIfClassifierSaysSo(topic, decodeFailure);
                if (permanent != null) {
                    throw permanent;
                }
                throw decodeFailure;
            }
        }

        /**
         * Asks the user's classifier whether this decode failure is one no retry could fix, and turns a yes into
         * the exception the dispatch path reads as a permanent park. The verdict is read in this one place so the
         * wrapping cannot drift between the key side and the value side (R12).
         *
         * @return the exception to throw when the classifier called this payload permanently undecodable, or null
         * when it did not - in which case the caller rethrows the original, which is an ordinary failed attempt
         * (R12)
         */
        private PermanentDecodeFailureException permanentIfClassifierSaysSo(String topic, Exception decodeFailure) {
            Decode<T> verdict = classifier.apply(decodeFailure);
            if (verdict == null || !verdict.isPermanent()) {
                return null;
            }
            return new PermanentDecodeFailureException(msg("Permanently undecodable payload on topic {}", topic),
                    decodeFailure);
        }

        /**
         * Closes the wrapped deserialiser. The classifier is the user's function and is not this wrapper's to close.
         */
        @Override
        public void close() {
            inner.close();
        }
    }
}
