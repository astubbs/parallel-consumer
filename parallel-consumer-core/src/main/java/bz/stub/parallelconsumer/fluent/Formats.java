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

    private static final String AVRO_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    private static final String AVRO_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer";

    private static final String PROTOBUF_DESERIALIZER =
            "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer";

    private static final String PROTOBUF_SERIALIZER = "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer";

    private static final String JACKSON_OBJECT_MAPPER = "com.fasterxml.jackson.databind.ObjectMapper";

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

    private static boolean isOnClasspath(String className) {
        try {
            Class.forName(className, false, Formats.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

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

    private static final class ConfigMergingDeserializer<T> implements Deserializer<T> {

        private final Deserializer<T> inner;

        private final Map<String, Object> extra;

        private ConfigMergingDeserializer(Deserializer<T> inner, Map<String, Object> extra) {
            this.inner = inner;
            this.extra = extra;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            inner.configure(merge(configs, extra), isKey);
        }

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

        @Override
        public void close() {
            inner.close();
        }
    }

    private static final class ConfigMergingSerializer<T> implements Serializer<T> {

        private final Serializer<T> inner;

        private final Map<String, Object> extra;

        private ConfigMergingSerializer(Serializer<T> inner, Map<String, Object> extra) {
            this.inner = inner;
            this.extra = extra;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            inner.configure(merge(configs, extra), isKey);
        }

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

        @Override
        public void close() {
            inner.close();
        }
    }

    /**
     * @see #classifyDecodeFailures
     */
    private static final class ClassifyingDeserializer<T> implements Deserializer<T> {

        private final Deserializer<T> inner;

        private final Function<Exception, Decode<T>> classifier;

        private ClassifyingDeserializer(Deserializer<T> inner, Function<Exception, Decode<T>> classifier) {
            this.inner = inner;
            this.classifier = classifier;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            inner.configure(configs, isKey);
        }

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

        @Override
        public void close() {
            inner.close();
        }
    }
}
