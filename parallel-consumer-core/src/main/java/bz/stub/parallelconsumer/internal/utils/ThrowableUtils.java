package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import bz.stub.parallelconsumer.internal.InternalRuntimeException;
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
        // the two halves degrade INDEPENDENTLY. One try around both would let an unreadable cause chain discard a
        // perfectly readable top-level message - which is how a hostile getCause() turned "the thing that actually
        // went wrong" back into a bare class name, the exact uselessness this method exists to remove.
        String own = describeSafely(t);
        Throwable root;
        try {
            root = rootCauseOf(t);
        } catch (Throwable chainUnreadable) {
            return own; // as much as could be read, which is more than the type name alone
        }
        return root == t ? own : own + " - caused by " + describeSafely(root, ThrowableUtils::describeWithType);
    }

    /**
     * Runs a log call that renders {@code reported}, guaranteeing it cannot become the failure.
     * <p>
     * Handing a throwable to the logger passes it to a binding that walks the cause chain to build a stack trace -
     * Logback's {@code ThrowableProxy} constructor calls {@code getCause} directly - and both {@code getCause} and
     * {@code getMessage} are overridable by whoever threw. So on any path that logs a user-supplied throwable and
     * then does something that must happen - rethrowing it, shutting down, marking work complete - the log call is
     * user code running before the part that matters.
     * <p>
     * The failure is silent and specific: the caller sees whatever the logger threw INSTEAD of the failure it was
     * trying to report, so the diagnosis is replaced by a stack trace from inside the logging framework.
     *
     * @param reported the throwable being logged; a logging failure is attached to it as suppressed rather than
     *                 logged, because logging is the thing that just failed
     * @param logCall  the log statement, which may render {@code reported}
     */
    public static void logWithoutEscaping(Throwable reported, Runnable logCall) {
        try {
            logCall.run();
        } catch (Throwable loggingItFailed) {
            if (reported != null && reported != loggingItFailed) {
                try {
                    reported.addSuppressed(loggingItFailed);
                } catch (Throwable evenThatFailed) {
                    // addSuppressed runs no user code, but a throwable built with suppression disabled ignores it
                    // and a subclass may override it. Nothing left to do but not make it worse.
                }
            }
        }
    }

    private static String describeSafely(Throwable t) {
        return describeSafely(t, ThrowableUtils::describeOne);
    }

    /**
     * Applies a describer, falling back to the type name when reading the throwable throws - {@code getMessage} is
     * overridable, so even naming a single link runs its author's code.
     */
    private static String describeSafely(Throwable t, java.util.function.Function<Throwable, String> describer) {
        try {
            return describer.apply(t);
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
        return message != null ? message : typeName(t);
    }

    /**
     * The simple name, except for the shapes that do not have one.
     * <p>
     * {@code getSimpleName()} returns the <b>empty string</b> for an anonymous class, so a message-less anonymous
     * throwable - an ad-hoc signal exception, which a lambda or a framework may well construct - described itself as
     * nothing at all. Same uselessness the null fallback was added to remove, one shape over.
     */
    private static String typeName(Throwable t) {
        String simple = t.getClass().getSimpleName();
        return simple.isEmpty() ? t.getClass().getName() : simple;
    }

    /**
     * As {@link #describeOne}, but always naming the type - the root cause's type is the useful half when its
     * message is the uninformative one, and repeating the type when there is no message would read as
     * {@code "NullPointerException: NullPointerException"}.
     */
    private static String describeWithType(Throwable t) {
        String message = t.getMessage();
        String type = typeName(t);
        return message != null ? type + ": " + message : type;
    }

    /**
     * Whether the throwable, or anything in its cause chain, is of the given type.
     * <p>
     * Presence anywhere in the chain - useful for "did this happen at all", not for "what is this failure".
     * <p>
     * <b>Never throws</b>, for the same reason as {@link #describeWithRootCause}: callers use this on the failure
     * path. An unreadable chain answers {@code false} rather than replacing one failure with another.
     * <p>
     * <b>A match anywhere decides the answer for the whole failure</b>, so this is the wrong question when the caller
     * means "what IS this failure" - a genuinely different exception carrying this type further down would be
     * classified by the buried match. Callers wanting identity rather than presence use
     * {@link #unwrapTransparentWrappers} instead; {@code PCRetriableException.isPresentIn} does.
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
     * Peels the wrappers PC itself adds, and stops at the first thing that is not one.
     * <p>
     * Not a search of the whole chain: the question a caller asks of the result is "what IS this failure", and a
     * wrapper answers that only for wrappers that add nothing but a name. Peeling further would let an unrelated
     * exception buried under a genuinely different failure speak for the whole thing.
     * <p>
     * Only PC's own pass-through wrappers are peeled here, because those are the only ones core can name. A
     * framework that repackages exceptions on the way out - Reactor does - is the caller's to unwrap first, using
     * that framework's own helper; core cannot see those types.
     */
    public static Throwable unwrapTransparentWrappers(Throwable t) {
        try {
            var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
            var current = t;
            // identity, not just a self-reference check - the same guard walkCauseChain uses, for the same reason:
            // two wrappers can point at each other, and the depth bound alone would spend 100 hops on a 2-cycle
            for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH && seen.add(current)
                    && isTransparentWrapper(current); depth++) {
                Throwable cause = current.getCause();
                if (cause == null) {
                    break;
                }
                current = cause;
            }
            return current;
        } catch (Throwable unwrappingItFailed) {
            return t;
        }
    }

    /**
     * A wrapper that means "something below this threw", and nothing else - so the failure it carries is the failure.
     * <p>
     * {@link ExceptionInUserFunctionException} is the only one. It is documented as used <em>only</em> when user code
     * threw, and <b>every</b> construction site wraps user code and nothing else: the three in
     * {@code UserFunctions.carefullyRun}, plus the user's rebalance listener in
     * {@code AbstractParallelEoSStreamProcessor.onPartitionsRevoked}. So it adds a name and no failure semantics of
     * its own. (Grep {@code new ExceptionInUserFunctionException} before trusting this - the claim is about all
     * sites, so one new site that wraps something else falsifies it.)
     * <p>
     * <b>{@link InternalRuntimeException} deliberately does NOT qualify</b>, though it reads like a wrapper. Its
     * message is how callers tell distinct internal failures apart - {@code "Error encoding offsets"},
     * {@code "Error producing result message"}, {@code "Too many attempts taking commit responses"} - so peeling it
     * would let a retriable cause speak for a failure that is not retriable at all, and an offset-encoding error
     * carrying one would be demoted to DEBUG. That is the same "a buried match decides the whole failure" mistake
     * {@link #hasCauseOfType} warns about, which is what this method exists to avoid. Its one cause-only site wraps a
     * {@code ProducerFencedException}, which is likewise a different failure rather than a pass-through.
     */
    private static boolean isTransparentWrapper(Throwable t) {
        return t instanceof ExceptionInUserFunctionException;
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
            try {
                current = current.getCause();
            } catch (Throwable chainEndsHere) {
                // stop where the chain became unreadable, KEEPING what was already walked. Letting this escape
                // discarded the whole walk, so one hostile link buried under a wrapper cost the caller every link
                // above it too - including the one carrying the message a human needs.
                return;
            }
        }
    }
}
