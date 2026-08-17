package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.function.Predicate;

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
     * How far either walk will follow {@code getCause} before giving up.
     * <p>
     * The identity set below stops a chain that loops back to something already seen. It cannot stop one that never
     * repeats: {@code getCause()} is overridable, so a throwable may return a <b>freshly allocated</b> cause on every
     * call. Each is a new identity, so nothing is ever "seen" twice, and the walk allocates a throwable - with its own
     * captured stack trace - per hop until the heap is gone. Both guards are needed because they stop different
     * shapes, and neither implies the other.
     * <p>
     * A real chain is nowhere near this deep, so truncating is only ever reached by something pathological; reaching
     * it means answering from what was walked rather than spending the control thread's shutdown on the rest.
     */
    private static final int MAX_CAUSE_DEPTH = 100;

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
     * @param t the throwable to describe; null answers {@code "null"} rather than throwing
     * @return a human-readable description, never null
     */
    public static String describeWithRootCause(Throwable t) {
        if (t == null) {
            // degrades rather than throwing, as hasCauseOfType does for the same argument. A null here is a caller
            // bug, but this method's whole contract is that it does not add a second failure to the one being
            // described - and the fallback below dereferences t, so it cannot be the null handler.
            return "null";
        }
        try {
            Throwable root = rootCauseOf(t);
            return root == t
                    ? describeOne(t)
                    : describeOne(t) + " - caused by " + describeWithType(root);
        } catch (Throwable describingItFailed) {
            return t.getClass().getName();
        }
    }

    /**
     * The message, or the type when there is no message.
     * <p>
     * A throwable with neither a message nor a cause was the one shape still reaching the caller as the literal
     * string {@code "null"} - which is the {@code "Error: null"} this method exists to replace, surviving in the
     * branch where there is no cause to fall back to.
     */
    private static String describeOne(Throwable t) {
        String message = t.getMessage();
        return message != null ? message : t.getClass().getSimpleName();
    }

    /**
     * As {@link #describeOne}, but always naming the type - the root cause's type is the useful half when its
     * message is the uninformative one, and repeating the type when there is no message would read as
     * {@code "NullPointerException: NullPointerException"}.
     */
    private static String describeWithType(Throwable t) {
        String message = t.getMessage();
        String type = t.getClass().getSimpleName();
        return message != null ? type + ": " + message : type;
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
     * <p>
     * <b>The assumption this rests on, for whoever adds the next caller.</b> Searching the whole chain means a match
     * anywhere decides the answer for the entire failure - so if an unrelated exception of this type were ever
     * chained beneath a genuinely fatal one, the fatal one would be classified by the buried match. That is safe for
     * the current retriable-logging callers because nothing chains one failure onto another:
     * {@code WorkContainer.onUserFunctionFailure} replaces the stored cause per attempt rather than appending, and
     * the reactive engines pass on only what their framework hands back. A change that starts aggregating failures
     * into one chain would invalidate that, and the symptom would be quiet: a real error logged at debug.
     *
     * @param t    the throwable to search; null answers false
     * @param type the type to look for
     */
    public static boolean hasCauseOfType(Throwable t, Class<? extends Throwable> type) {
        try {
            var found = new boolean[1];
            walkCauseChain(t, link -> {
                found[0] = type.isInstance(link);
                return !found[0]; // stop at the first match
            });
            return found[0];
        } catch (Throwable searchingItFailed) {
            return false;
        }
    }

    /**
     * The deepest cause, stopping on a repeat or at {@link #MAX_CAUSE_DEPTH}.
     * <p>
     * An identity set rather than a self-reference check, because a cause chain can cycle without any link pointing
     * at itself: {@code initCause} refuses self-causation, so {@code A -> A} cannot be built and a self-check looks
     * sufficient - but {@code A -> B -> A} can, and defeats it. A chain restored by deserialization carries no such
     * guard at all. The JDK's own {@code printStackTrace} keeps an identity set for this reason.
     */
    private static Throwable rootCauseOf(Throwable t) {
        var root = new Throwable[]{t};
        walkCauseChain(t, link -> {
            root[0] = link;
            return true; // every link, so the last one reached is the deepest
        });
        return root[0];
    }

    /**
     * Walks {@code t} and its causes, applying both guards, until {@code visitor} returns {@code false} or the chain
     * ends. One walk rather than two, because both guards have to hold in both places and a second copy is a second
     * chance to omit one.
     *
     * @param visitor called per link; returns {@code true} to continue
     */
    private static void walkCauseChain(Throwable t, Predicate<Throwable> visitor) {
        var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        var current = t;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH && seen.add(current); depth++) {
            if (!visitor.test(current)) {
                return;
            }
            current = current.getCause();
        }
    }
}
