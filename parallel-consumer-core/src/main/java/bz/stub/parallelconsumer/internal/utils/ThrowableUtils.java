package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;

import java.util.Collections;
import java.util.IdentityHashMap;

/**
 * Describing a throwable for a human, when the throwable came from somewhere we do not control.
 * <p>
 * Nothing novel: this is {@code ExceptionUtils.getRootCauseMessage} from commons-lang3, which the project already
 * uses - but at <b>test</b> scope. Core's compile classpath is deliberately small (kafka-clients, slf4j-api,
 * micrometer, UniJ), and this is a library, so promoting commons-lang3 to compile scope would push a transitive
 * runtime dependency onto every consumer to improve one log line. Guava, which has {@code Throwables.getRootCause},
 * is not a core dependency at all.
 * <p>
 * The other classpath options are worse, not better: reaching into {@code kafka-clients} or {@code micrometer} for a
 * general-purpose helper borrows a class from a dependency that owes us no such API and can withdraw it in a patch
 * release. Reimplementing it here is the smaller debt. <b>If commons-lang3 or Guava ever becomes a compile
 * dependency, delete this class and call theirs.</b>
 *
 * @author Antony Stubbs
 */
@UtilityClass
public class ThrowableUtils {

    /**
     * The throwable's own message, plus its root cause's, because the immediate message is routinely the least
     * informative one available.
     * <p>
     * Two ways that bites in practice: an exception thrown from user code carries a null message often enough that
     * {@code "Error: " + e.getMessage()} reads {@code "Error: null"}, and anything routed through a wrapper reports
     * the wrapper's constant while the sentence a human needs sits one level down.
     * <p>
     * <b>Never throws.</b> Both {@code getCause} and {@code getMessage} are overridable, so reading a throwable is
     * running its author's code - and callers use this on the failure path, where an exception escaping the
     * <em>description</em> of a failure would prevent handling the failure itself. On any trouble it falls back to
     * the type name, which is always available and is still more than the null it replaced.
     *
     * @param t the throwable to describe; must not be null
     * @return a human-readable description, never null
     */
    public static String describeWithRootCause(Throwable t) {
        try {
            Throwable root = rootCauseOf(t);
            return root == t
                    ? String.valueOf(t.getMessage())
                    : t.getMessage() + " - caused by " + root.getClass().getSimpleName() + ": " + root.getMessage();
        } catch (Throwable describingItFailed) {
            return t.getClass().getName();
        }
    }

    /**
     * Whether the throwable, or anything in its cause chain, is of the given type.
     * <p>
     * The chain, not the top - because whether an exception arrives wrapped is decided by whatever passed it on, not
     * by what it means. A {@code PCRetriableException} says "expected, retry me" whether user code threw it directly,
     * a wrapper caught and re-threw it, or a reactive framework repackaged it on the way out. Testing only the
     * outermost object makes the answer depend on the plumbing, which is how an expected failure ends up logged as an
     * error.
     * <p>
     * <b>Never throws</b>, for the same reason as {@link #describeWithRootCause}: callers use this on the failure
     * path. An unreadable chain answers {@code false} rather than replacing one failure with another.
     *
     * @param t    the throwable to search; null answers false
     * @param type the type to look for
     */
    public static boolean hasCauseOfType(Throwable t, Class<? extends Throwable> type) {
        try {
            var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
            for (var current = t; current != null && seen.add(current); current = current.getCause()) {
                if (type.isInstance(current)) {
                    return true;
                }
            }
            return false;
        } catch (Throwable searchingItFailed) {
            return false;
        }
    }

    /**
     * The deepest cause, stopping on a repeat.
     * <p>
     * An identity set rather than a self-reference check, because a cause chain can cycle without any link pointing
     * at itself: {@code initCause} refuses self-causation, so {@code A -> A} cannot be built and a self-check looks
     * sufficient - but {@code A -> B -> A} can, and defeats it. A chain restored by deserialization carries no such
     * guard at all. The JDK's own {@code printStackTrace} keeps an identity set for this reason.
     */
    private static Throwable rootCauseOf(Throwable t) {
        var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        var root = t;
        seen.add(root);
        for (var cause = root.getCause(); cause != null && seen.add(cause); cause = cause.getCause()) {
            root = cause;
        }
        return root;
    }
}
