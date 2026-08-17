package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Name-to-action registry: the "registry instead of an enum" half of the generalisation. Its job is
 * name resolution - reading a scripted plan back from a log line, or rendering a timeline entry - not
 * discovery. Scenarios reference action constants directly and never need to consult it.
 * <p>
 * Registration happens when the declaring type initialises: {@link MembershipAction} and each
 * {@link WorkloadActions} factory register themselves. A lookup for an action whose declaring class has
 * not been loaded therefore returns empty rather than a wrong answer - {@link #byName} is
 * {@link Optional} for that reason.
 */
public final class ScenarioActions {

    /** Insertion-ordered so {@link #registered()} reads in declaration order in logs. */
    private static final Map<String, ScenarioAction> REGISTRY = Collections.synchronizedMap(new LinkedHashMap<>());

    private ScenarioActions() {
    }

    /**
     * Register an action under its {@link ScenarioAction#name()}. Idempotent for an identical
     * re-registration; a DIFFERENT action claiming an already-registered name is a programming error and
     * throws, because silently shadowing one would make a scripted plan resolve to the wrong behaviour.
     */
    public static <A extends ScenarioAction> A register(A action) {
        ScenarioAction existing = REGISTRY.putIfAbsent(action.name(), action);
        if (existing != null && existing != action) {
            throw new IllegalStateException("scenario action name '" + action.name() + "' is already registered to "
                    + existing + " - names must be unique across the registry");
        }
        return action;
    }

    public static void registerAll(ScenarioAction... actions) {
        for (ScenarioAction action : actions) {
            register(action);
        }
    }

    public static Optional<ScenarioAction> byName(String name) {
        return Optional.ofNullable(REGISTRY.get(name));
    }

    public static Collection<ScenarioAction> registered() {
        synchronized (REGISTRY) {
            return Collections.unmodifiableCollection(new LinkedHashMap<>(REGISTRY).values());
        }
    }
}
