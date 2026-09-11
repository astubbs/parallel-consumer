package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * A pre-built consumer handed to the fluent API was not configured for raw bytes, and the first record proved it.
 *
 * <h2>Why this is not caught at definition time</h2>
 * The facade consumes raw bytes and each route applies its own deserialisers (KD9, KTD2), so a supplied consumer
 * must be a {@code Consumer<byte[], byte[]>}. Its type parameters are erased, so nothing at definition time can tell
 * a byte-array consumer from a string one - the two are the same class with different constructor arguments. The
 * first record is where the difference appears, as a {@link ClassCastException} on the key or the value.
 * <p>
 * That cast failure is a <b>definition fault, never a retry</b> (KTD3, R1). Retrying it would fail identically for
 * every record on the topic, forever, while the attempt counters climbed and records parked for a reason that has
 * nothing to do with them. So the dispatch wrapper classifies it with {@link #isRawBytesCastFailure} and stops the
 * instance through the stop path with this exception, whose message names the deserialisers the consumer was built
 * with - read off the consumer once, at start, before it went into the options builder.
 *
 * <h2>The seam</h2>
 * {@link #describe(Consumer)} is called by {@link ParallelConsumerDefinition#consumer} and its result kept as a
 * string, not as a client reference (KTD3). {@link #isRawBytesCastFailure} and {@link #from} are what the wrapper
 * calls when a cast fails.
 */
@InterfaceStability.Unstable
public class RawBytesConsumerFaultException extends ParallelConsumerException {

    /**
     * Pinned so that adding a field later does not change the serialised form - this travels as the cause of a stop,
     * and a stop is reported to a caller that may not be running the same build.
     */
    private static final long serialVersionUID = 1L;

    /**
     * How deep {@link #describe} follows client-internal references looking for the deserialisers. Kafka's consumer
     * reaches them through a delegate and a holder object, so two would be enough today; three leaves room without
     * turning a diagnostic into a graph walk.
     */
    private static final int DESCRIBE_MAX_DEPTH = 3;

    /**
     * Package-private, so this fault can only be raised through {@link #from(ClassCastException, String)} and
     * therefore always carries the one message that explains the diagnosis. A fault the user could construct would
     * be a fault nobody had established, on a path whose only reaction is to stop the instance.
     */
    RawBytesConsumerFaultException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Build the fault for a cast failure the wrapper has already decided is this one.
     *
     * @param consumerDescription {@link DefinitionView#preBuiltConsumerDescription()}
     */
    public static RawBytesConsumerFaultException from(ClassCastException cause, String consumerDescription) {
        return new RawBytesConsumerFaultException(msg("The consumer supplied to this definition is not configured "
                        + "for raw bytes, so the fluent API cannot decode records with each route's own "
                        + "deserialisers. It was built with {}; it must use ByteArrayDeserializer for both the key "
                        + "and the value, or be left out so that the definition's properties build it. This is a "
                        + "definition fault, not a failed record: retrying it would fail identically forever.",
                consumerDescription == null ? "deserialisers this API could not read off it" : consumerDescription),
                cause);
    }

    /**
     * Whether a {@link ClassCastException} came from a record's key or value not being a {@code byte[]}.
     * <p>
     * Matched on the message rather than on a type, because the cast is a synthetic checkcast the compiler inserted
     * in the facade's own generic code, so there is nothing else to look at. A false negative here costs the clear
     * message and leaves the ordinary retry path; a false positive would stop an instance, so the test is narrow: a
     * byte array must be named as the expected type.
     */
    public static boolean isRawBytesCastFailure(ClassCastException cause) {
        String message = cause.getMessage();
        return message != null && (message.contains("[B") || message.contains("byte[]"));
    }

    /**
     * Read the deserialiser class names off a consumer, once, so a later cast failure can name them. Best effort by
     * construction: it walks client internals, which are not API, and answers null when it cannot see them - a
     * diagnostic that guesses would be worse than one that admits it does not know.
     */
    public static String describe(Consumer<?, ?> consumer) {
        try {
            List<String> found = new ArrayList<>();
            collectDeserialisers(consumer, 0, new IdentityHashMap<>(), found);
            if (found.isEmpty()) {
                return null;
            }
            return found.toString();
        } catch (RuntimeException | LinkageError probeFailure) {
            // A diagnostic must never be the reason a definition fails to start.
            return null;
        }
    }

    /**
     * Walks a client's fields for the two deserialisers, stopping at the first of four bounds: a null, the depth
     * limit, two found, or an object already visited. Each bound is there because this runs over internals that are
     * not API and may hold cycles or an arbitrary object graph, and a diagnostic must cost less than the fault it
     * explains.
     *
     * @param seen  identity-keyed, because client internals are not required to define {@code equals} and two
     *              distinct objects that compare equal would silently truncate the walk
     * @param found accumulates in field-declaration order, which for a consumer is key then value; two is the whole
     *              answer, so the walk stops there rather than continuing to collect
     */
    private static void collectDeserialisers(Object target, int depth, Map<Object, Boolean> seen, List<String> found) {
        if (target == null || depth > DESCRIBE_MAX_DEPTH || found.size() >= 2
                || seen.put(target, Boolean.TRUE) != null) {
            return;
        }
        for (Class<?> type = target.getClass(); type != null && type != Object.class; type = type.getSuperclass()) {
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                Object value = read(field, target);
                if (value instanceof Deserializer) {
                    found.add(value.getClass().getSimpleName());
                } else if (value != null && isWorthFollowing(value)) {
                    collectDeserialisers(value, depth + 1, seen, found);
                }
            }
        }
    }

    /**
     * One field, or null if it cannot be reached. Every way of failing here - a module that does not open, a
     * security manager, a field that is not there in this client version - is the same answer to the caller: this
     * field tells us nothing, carry on with the next one.
     */
    private static Object read(Field field, Object target) {
        try {
            field.setAccessible(true);
            return field.get(target);
        } catch (RuntimeException | ReflectiveOperationException inaccessible) {
            return null;
        }
    }

    /**
     * Only Kafka's own client internals are followed - anything else is either a JDK type or the user's, and neither
     * holds the consumer's deserialisers.
     */
    private static boolean isWorthFollowing(Object value) {
        String name = value.getClass().getName();
        return name.startsWith("org.apache.kafka.");
    }
}
