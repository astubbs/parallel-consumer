package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;
import org.slf4j.MDC;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Records what the SLF4J {@link MDC} looks like on the far side of an engine's thread boundary, so that each engine
 * module's propagation test does not have to restate the same bookkeeping and assertions.
 * <p>
 * Lives in the core test-jar because the vert.x, Reactor and Mutiny modules each depend on it (the same route
 * {@link TestConventionRules} and {@code WireMockUtils} take). Used by <em>composition</em> rather than a shared base
 * class: each engine test already extends its own module's unit-test base, so inheritance is not available.
 * <p>
 * The engine-specific parts stay in the tests - how many observations to expect, and which thread names the engine is
 * supposed to use - because those are the assertions that stop a test silently degrading into a re-test of the core
 * worker-pool boundary, and they differ per engine.
 *
 * @author Antony Stubbs
 * @see bz.stub.parallelconsumer.internal.MdcPropagation
 */
public class MdcBoundaryProbe {

    /**
     * A realistic caller key - the kind of thing a tracing library would have put in the MDC before calling
     * {@code poll*}.
     */
    public static final String CALLER_KEY = "trace_id";

    public static final String CALLER_VALUE = "caller-trace-abc";

    /**
     * What one invocation on the far side of the boundary saw. Thread and value are kept together in ONE object, added
     * to ONE collection, deliberately: a test awaits {@link #observations()} and then asserts on both fields, so if
     * they lived in two collections the first would <em>lead</em> the awaited one and the assertions would race a
     * still-arriving invocation. That is the defect fixed in {@code MultiInstanceMetricsTest} (await the value you are
     * about to assert, never a proxy that leads it); this shape makes it unrepresentable rather than merely absent.
     */
    @Value
    public static class Observation {
        String threadName;
        String callerValueSeen;
    }

    private final ConcurrentLinkedQueue<Observation> observations = new ConcurrentLinkedQueue<>();

    /**
     * Puts the caller's context on the <em>current</em> thread. Call this on the test thread, before starting Parallel
     * Consumer - that is the thread whose context is supposed to be propagated.
     */
    public void establishCallersContext() {
        MDC.put(CALLER_KEY, CALLER_VALUE);
    }

    /**
     * JUnit reuses its runner thread across tests, so context set by one test must not be inherited by the next.
     */
    public void clearCallersContext() {
        MDC.clear();
    }

    /**
     * Records the thread, and what it can see of the caller's context. Call from inside the user function or hook - the
     * code that runs on the far side of the boundary under test.
     */
    public void observeCurrentThread() {
        // single add - see the note on Observation for why this must not become two collections
        observations.add(new Observation(Thread.currentThread().getName(), String.valueOf(MDC.get(CALLER_KEY))));
    }

    /**
     * @return one entry per invocation observed. Awaiting this bounds <em>everything</em> the assertions below read, so
     *         a test that awaits it is not racing a still-arriving invocation.
     */
    public Collection<Observation> observations() {
        return observations;
    }

    /**
     * Asserts every thread that observed satisfies {@code engineThreadName} - i.e. that the work really did cross onto
     * the engine's own threads. Without this a test could pass while only ever exercising the PC worker pool, which the
     * core module already covers.
     *
     * @param engineThreadDescription how to describe the expected thread in the failure message, e.g. "Reactor
     *                                scheduler"
     * @param engineThreadName        the property the thread name must have
     */
    public void assertObservedOnlyOn(String engineThreadDescription, Predicate<String> engineThreadName) {
        assertWithMessage("the observing code must run on the %s, not the PC worker thread - threads seen: %s",
                engineThreadDescription, threadNamesSeen())
                .that(observations.stream().map(Observation::getThreadName).allMatch(engineThreadName))
                .isTrue();
    }

    private List<String> threadNamesSeen() {
        return observations.stream().map(Observation::getThreadName).collect(Collectors.toList());
    }

    private List<String> callerValuesSeen() {
        return observations.stream().map(Observation::getCallerValueSeen).collect(Collectors.toList());
    }

    /**
     * Asserts the caller's context crossed the boundary intact - visible, with the caller's value, on every
     * observation.
     *
     * @param engineThreadDescription how to describe the thread in the failure message
     */
    public void assertCallersContextWasVisible(String engineThreadDescription) {
        assertWithMessage("the caller's diagnostic context must be visible on the %s - values seen: %s",
                engineThreadDescription, callerValuesSeen())
                .that(observations.stream().map(Observation::getCallerValueSeen).allMatch(CALLER_VALUE::equals))
                .isTrue();
    }

}
