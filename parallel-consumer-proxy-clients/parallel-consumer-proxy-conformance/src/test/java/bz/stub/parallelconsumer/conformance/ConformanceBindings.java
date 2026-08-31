package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Everything a scenario runs against, and the one sanctioned way to run fewer of them.
 * <p>
 * <b>The control arm is not selectable away.</b> {@link CoreBinding} is in every selection, because its
 * whole job is to answer "is this scenario wrong?" in the same run that a client went red - an answer that
 * arrives in a different CI job, hours later, is not the answer anybody needed. It costs a few in-process
 * seconds and it is what stops an afternoon being spent on an innocent client.
 * <p>
 * <b>A selector that matches nothing FAILS.</b> CI runs one language per matrix row, so the selector is on
 * the hot path of every conformance run there is, and a typo that silently evaluated zero scenarios would
 * report green having tested nothing - the exact shape this repository has seven recorded instances of. So
 * an unregistered name is an error naming what is registered, never an empty selection.
 *
 * <b>A deferred cell is named by the failure, not merely absent from it.</b> Ten languages have client
 * libraries and conformance runners in this tree ({@link LanguageRunners}) and none of them can be driven
 * here, because driving one needs an engine behind the sidecar and there is none. Selecting one therefore
 * fails saying <em>that</em>, rather than "unknown name" - a CI row that names {@code go} has not made a
 * typo, and telling it so would send its operator looking for a misspelling.
 * <p>
 * <b>The foreign bindings are concatenated into {@link #selectable()} beside the JVM clients</b>, which is
 * the only place either registry has to know the other exists - and today that concatenation contributes
 * nothing, which {@link TheEngineArrivingMustBringTheForeignCellsTest} is what stops being permanent.
 *
 * @author Antony Stubbs
 * @see JvmClientBindings
 */
public final class ConformanceBindings {

    /**
     * The selector: {@code -Dpc.conformance.language=python}, or a comma-separated list. Absent means every
     * registered binding. <b>The name is fixed</b> - the {@code clients} workflow's per-language matrix rows
     * are written against it, so changing it silently unselects every row.
     */
    public static final String LANGUAGE_PROPERTY = "pc.conformance.language";

    /**
     * The name this property had while Go was the only language, kept only to be REJECTED. A run that still
     * passes it would otherwise be told nothing and quietly select everything, which in a one-toolchain CI
     * row fails somewhere far from the cause.
     */
    static final String FORMER_LANGUAGE_PROPERTY = "pc.conformance.languages";

    /** The engine itself: the control arm, first in every selection. */
    private static final CoreBinding CORE = new CoreBinding();

    /**
     * The cells this rung knows about and cannot run - every one of them waiting on the same thing, an
     * engine behind the sidecar. Derived from {@link LanguageRunners#all()} rather than listed, so a
     * language added to the registry cannot be forgotten here, and it goes away by itself: a name that
     * becomes selectable stops being deferred because {@link #select} checks the registered set first.
     */
    static List<String> deferredUntilTheEngineArrives() {
        var deferred = new ArrayList<String>();
        deferred.add(JvmClientBindings.JAVA_GRPC);
        LanguageRunners.all().forEach(runner -> deferred.add(runner.language()));
        return List.copyOf(deferred);
    }

    /** Every binding this run drives: the core control arm, plus the languages the selector kept. */
    public static List<ConformanceBinding> selected() {
        var former = System.getProperty(FORMER_LANGUAGE_PROPERTY);
        if (former != null) {
            throw new IllegalArgumentException("-D" + FORMER_LANGUAGE_PROPERTY + " was renamed to -D"
                    + LANGUAGE_PROPERTY + " when the core binding joined the languages; it is rejected rather "
                    + "than ignored, because ignoring it would select every binding and fail in a CI row that "
                    + "installed one toolchain.");
        }
        return select(System.getProperty(LANGUAGE_PROPERTY));
    }

    /**
     * The selection itself, as a pure function of what was asked for - so the failure that matters most here
     * can be tested without a JVM-wide property.
     *
     * @param requested the property's value: {@code null} or blank for everything, else a comma-separated
     *                  list of binding names
     * @throws IllegalArgumentException if any name is not registered. Failing is the whole point: a
     *                                  selection nobody matched must never be reported as a clean run
     */
    static List<ConformanceBinding> select(String requested) {
        var selectable = selectable();
        if (requested == null || requested.isBlank()) {
            var all = new ArrayList<ConformanceBinding>(List.of(CORE));
            all.addAll(selectable);
            return List.copyOf(all);
        }

        var wanted = Arrays.stream(requested.split(",")).map(String::trim).filter(name -> !name.isEmpty())
                .toList();
        var known = new ArrayList<String>(List.of(CORE.name()));
        selectable.forEach(binding -> known.add(binding.name()));
        var unknown = wanted.stream().filter(name -> !known.contains(name)).toList();
        if (!unknown.isEmpty()) {
            var deferred = deferredUntilTheEngineArrives();
            var waiting = unknown.stream().filter(deferred::contains).toList();
            // Both cases FAIL - what differs is what the reader is told to do next. Reporting a deferred
            // cell as an unrecognised name would send a CI row's operator hunting a typo that is not there.
            throw new IllegalArgumentException("-D" + LANGUAGE_PROPERTY + " names bindings this suite does "
                    + "not register: " + unknown + " (registered: " + known + "). A typo here would otherwise "
                    + "run nothing and read as a pass."
                    + (waiting.isEmpty() ? "" : " Of those, " + waiting + " are deferred rather than "
                    + "misspelled: their clients exist, and driving one needs an engine behind the sidecar, "
                    + "which this build has none of. " + TheEngineArrivingMustBringTheForeignCellsTest.class
                    .getSimpleName() + " and " + TheEngineArrivingMustBringTheGrpcBindingTest.class
                    .getSimpleName() + " go red the day that changes."));
        }

        var selected = new ArrayList<ConformanceBinding>(List.of(CORE));
        selectable.stream().filter(binding -> wanted.contains(binding.name())).forEach(selected::add);
        return List.copyOf(selected);
    }

    /**
     * Every binding a selector may name, in matrix order: the JVM clients this suite drives in-process, then
     * the languages whose runners it spawns. The control arm is not among them - it is added to every
     * selection rather than chosen.
     * <p>
     * <b>It is a concatenation rather than one list</b> because the registries hold different facts - a client
     * object's construction against a runner binary's build - and are edited by different waves. This is the
     * only place either has to know the other exists.
     */
    private static List<ConformanceBinding> selectable() {
        var selectable = new ArrayList<ConformanceBinding>(JvmClientBindings.all());
        selectable.addAll(foreignBindings());
        return List.copyOf(selectable);
    }

    /**
     * The languages driven through their own conformance runners - <b>empty on this rung, and empty for one
     * reason that is checked rather than described.</b>
     * <p>
     * Spawning a runner means giving it a sidecar to connect to and an engine behind that sidecar to dispatch
     * records; the sidecar here hosts no engine and refuses every session {@code UNIMPLEMENTED}
     * (astubbs/parallel-consumer#384). A binding that spawned a runner at it would watch ten clients fail
     * identically and learn nothing about any of them, and a stand-in engine would make agreement between
     * bindings a statement about the stand-in. So the registry of runners is complete
     * ({@link LanguageRunners}) and the list of bindings over it is empty, and
     * {@link TheEngineArrivingMustBringTheForeignCellsTest} fails the build the moment the engine reaches
     * this module's classpath while that is still so.
     */
    private static List<ConformanceBinding> foreignBindings() {
        return List.of();
    }

    private ConformanceBindings() {
    }
}
