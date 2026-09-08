package bz.stub.parallelconsumer.archfixture;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.state.WorkContainer;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A defect on purpose: a {@link WorkContainer} used as the element or key of a sorted collection, in each of the
 * four declaration positions {@code WorkContainerIsNeverInASortedCollectionArchTest} inspects.
 * <p>
 * This class is <b>not production code and must never be referenced by any</b>. It exists so
 * {@code WorkContainerSortedCollectionRuleControlTest} can point the real rule at declarations it is supposed to
 * report and fail if it does not - the positive control for a rule whose whole job is to be green.
 * <p>
 * <b>{@link #containersAsValues} is the other half of the control and is the one that must NOT be reported.</b> A
 * container as a sorted map's VALUE is correct - the sorted contracts constrain keys, not values - and it is what
 * {@code ProcessingShard}'s own {@code workMap} does. A rule that widened to every type argument would report the
 * shard's map, go red on arrival on correct code, and be deleted; this field is what goes red instead.
 * <p>
 * <b>Do not "tidy" any of these into local variables.</b> A sorted collection built inside a method body and
 * never stored declares nothing, which is a stated gap of the rule - written that way this fixture stops
 * controlling for anything.
 *
 * @author Antony Stubbs
 */
public class SortedCollectionOfWorkContainers {

    /** Reported: a {@link TreeSet} is a {@code NavigableSet}, and its element is the container itself. */
    private final TreeSet<WorkContainer<?, ?>> orderedByTheContainerItself;

    /** NOT reported, deliberately: the container is a map VALUE, which no sorted contract constrains. */
    private final NavigableMap<Long, WorkContainer<?, ?>> containersAsValues = new ConcurrentSkipListMap<>();

    /**
     * Reported: a constructor parameter, the position a field-only rule would miss.
     * <p>
     * The body reads {@link PriorityQueue#comparator()} rather than treating the argument as a plain
     * {@code Collection}, and that is not incidental: the DECLARED type is the whole of what this fixture presents
     * to the rule, and fb-contrib's {@code OCP_OVERLY_CONCRETE_PARAMETER} correctly asks for the widest type the
     * body needs. Widening it to satisfy that finding would delete the control, so the body uses the concrete
     * type's own method and the finding does not arise - which beats suppressing a detector that is right.
     */
    public SortedCollectionOfWorkContainers(PriorityQueue<WorkContainer<?, ?>> byRetryTime) {
        this.orderedByTheContainerItself = new TreeSet<>(byRetryTime.comparator());
    }

    /** Reported: a return type. The {@code TreeMap} in the body is not - only the declared type is visible. */
    public SortedMap<WorkContainer<?, ?>, String> descriptionsByContainer() {
        return new TreeMap<>();
    }

    /** Reported: a method parameter. */
    public boolean holdsAllOf(NavigableSet<WorkContainer<?, ?>> containers) {
        return orderedByTheContainerItself.containsAll(containers);
    }

    /** Reads {@link #containersAsValues}, so the negative control is a live field rather than dead weight. */
    public int valueCount() {
        return containersAsValues.size();
    }
}
