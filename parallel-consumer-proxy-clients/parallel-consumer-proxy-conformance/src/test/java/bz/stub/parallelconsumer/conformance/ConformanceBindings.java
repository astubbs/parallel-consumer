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
 * @author Antony Stubbs
 * @see LanguageRunners
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
            throw new IllegalArgumentException("-D" + LANGUAGE_PROPERTY + " names bindings this suite does "
                    + "not register: " + unknown + " (registered: " + known + "). A typo here would otherwise "
                    + "run nothing and read as a pass.");
        }

        var selected = new ArrayList<ConformanceBinding>(List.of(CORE));
        selectable.stream().filter(binding -> wanted.contains(binding.name())).forEach(selected::add);
        return List.copyOf(selected);
    }

    /**
     * Every binding a selector may name, in matrix order: the JVM clients this suite drives in-process,
     * then the languages whose runners it spawns. The control arm is not among them - it is added to every
     * selection rather than chosen.
     * <p>
     * Two registries rather than one because they hold different facts - a client object's construction
     * against a runner binary's build - and because they are edited by different waves. Concatenating them
     * here is the only place either has to know the other exists.
     */
    private static List<ConformanceBinding> selectable() {
        var all = new ArrayList<ConformanceBinding>(JvmClientBindings.all());
        all.addAll(LanguageRunners.all());
        return List.copyOf(all);
    }

    /**
     * A binding for a test that needs one concrete subject rather than the matrix - the parallelism proof.
     * <p>
     * <b>A spawning runner is preferred over an in-process binding</b>, because what that proof is about is
     * two child processes genuinely in flight at once; an in-JVM binding beside a subprocess would still
     * overlap, but it would stop demonstrating the thing the property is named for. Failing that, any
     * non-core binding, and failing that the control arm - because "no language was selected" must not
     * become a test that quietly does not run.
     */
    public static ConformanceBinding aSelectedBinding() {
        var selected = selected();
        return selected.stream().filter(LanguageRunner.class::isInstance).findFirst()
                .or(() -> selected.stream().filter(binding -> !CORE.name().equals(binding.name())).findFirst())
                .orElse(selected.get(0));
    }

    private ConformanceBindings() {
    }
}
