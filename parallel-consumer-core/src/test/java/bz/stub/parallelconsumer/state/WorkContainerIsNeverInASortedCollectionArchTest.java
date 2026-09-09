package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaParameterizedType;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.PriorityBlockingQueue;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

/**
 * A raw {@link WorkContainer} may not be the element or the key of a sorted collection.
 * <p>
 * <b>The contract this pins is stated in {@link WorkContainer}'s own class javadoc, and it is a deliberate
 * inconsistency rather than an oversight.</b> Equality is reference identity - two containers can exist for one
 * offset, a stale one and the fresh replacement, and every value-conditional collection operation has to be able
 * to tell them apart - while {@link WorkContainer#compareTo} orders by topic, partition and offset. {@link
 * Comparable} only <em>recommends</em> that the two agree, but {@link SortedSet} and {@link SortedMap}
 * <em>require</em> it: a sorted collection decides "is this already here" by the comparator, so a container would
 * be found under the coordinates of a different flight of the same record. That is the same defect from the other
 * side as the one astubbs/parallel-consumer#468 fixed.
 * <p>
 * <b>Nothing violates it today and the argument for the rule is that nothing goes red when someone does.</b>
 * {@link RetryQueue} sorts a {@link NavigableMap} keyed on its own {@code WorkContainerSortKey} and holds
 * containers only as map VALUES, which the sorted contracts do not constrain; {@link ProcessingShard}'s
 * {@code workMap} is a {@code ConcurrentSkipListMap} keyed on the offset. Both are correct and both are one
 * refactor away from being written the other way round, at which point the loss is a record rather than a
 * compile error.
 *
 * <h2>What the rule looks at</h2>
 * Every field, method return type, method parameter and constructor parameter in main code whose declared type
 * erases to something assignable to {@link SortedSet}, {@link NavigableSet}, {@link SortedMap}, {@link
 * NavigableMap}, {@link PriorityQueue}, {@link ConcurrentSkipListSet} or {@link PriorityBlockingQueue} - which
 * covers {@code TreeSet}, {@code TreeMap} and {@code ConcurrentSkipListMap} without naming them - and whose FIRST
 * declared type argument erases to {@link WorkContainer}.
 * <p>
 * <b>The first argument is the element-or-key position for every one of those types</b>, which is why one index
 * serves both the set-shaped and the map-shaped ones, and why {@code NavigableMap<Long, WorkContainer<K, V>>}
 * passes. Checking every argument instead would report the shard's own map, whose container is a value - and a
 * rule that fires on the correct code is a rule people delete.
 *
 * <h2>What it cannot see - three gaps, none of them theoretical</h2>
 * <ul>
 *     <li><b>A sorted collection built inside a method body and never stored in a typed field or signature is
 *     invisible.</b> ArchUnit's model is declarations and accesses; {@code var seen = new TreeSet<WorkContainer<K,
 *     V>>()} used and discarded within one method declares nothing this rule reads. The rule covers the shapes
 *     that outlive a stack frame, which is where a wrong lookup does its damage, and says so rather than being
 *     read as "no sorted collection of containers exists".</li>
 *     <li><b>It cannot see the comparator.</b> A {@code Comparator}-taking constructor makes a sorted collection
 *     of containers legitimate - but only if that comparator is NOT {@link WorkContainer#compareTo} or anything
 *     equivalent to it, since the requirement is that the ordering agrees with equality and equality is identity.
 *     The rule reports the declaration either way. That is deliberate: the exemption is a judgement, so it should
 *     cost a conversation rather than pass silently.</li>
 *     <li><b>It runs where core's test classpath reaches</b>, which is core's own main code plus anything else in
 *     the {@code bz.stub.parallelconsumer} namespace on it. The other modules' main code is not on that classpath
 *     - core is the module they depend on, not the other way round - so covering them needs a wrapper per module
 *     in the {@code TestConventionRules} shape. Measured before deciding not to: no main source outside core
 *     declares any of the seven types at all ({@code grep -rE 'TreeSet|TreeMap|SortedSet|SortedMap|NavigableSet
 *     |NavigableMap|PriorityQueue|ConcurrentSkipList|PriorityBlockingQueue'} over {@code src/main}), and the
 *     three non-core files that name {@code WorkContainer} pass it to a user function.</li>
 * </ul>
 *
 * <h2>Names</h2>
 * Every target is a {@code Class} literal rather than a string, so the trap recorded in {@code
 * docs/inflight/static-archunit-main-code-rules.md} - a rule naming a member by string, matching nothing after a
 * rename, and passing having asserted nothing - cannot arise here; a rename that breaks it breaks the build.
 * {@link WorkContainerSortedCollectionRuleControlTest} is the standing proof that it can still fail.
 *
 * @author Antony Stubbs
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer", importOptions = ImportOption.DoNotIncludeTests.class)
class WorkContainerIsNeverInASortedCollectionArchTest {

    /**
     * The collection types whose contract constrains their elements or keys, so a container in that position is
     * ordered by an ordering inconsistent with its equality.
     * <p>
     * Assignability rather than equality is what makes this list short: {@code TreeSet} and {@code
     * ConcurrentSkipListSet} arrive through {@link NavigableSet}, {@code TreeMap} and {@code ConcurrentSkipListMap}
     * through {@link NavigableMap}. {@link ConcurrentSkipListSet} and {@link PriorityBlockingQueue} are named
     * anyway because neither is redundant in the other direction - the first documents the concurrent shape a
     * shard is most likely to reach for, and {@link PriorityBlockingQueue} is a {@code BlockingQueue}, not a
     * {@link PriorityQueue}, so nothing else in this list covers it.
     */
    private static final List<Class<?>> ORDERED_BY_THEIR_ELEMENTS_OR_KEYS = Arrays.asList(
            SortedSet.class,
            NavigableSet.class,
            SortedMap.class,
            NavigableMap.class,
            PriorityQueue.class,
            ConcurrentSkipListSet.class,
            PriorityBlockingQueue.class);

    @ArchTest
    static final ArchRule no_sorted_collection_is_ordered_by_a_raw_work_container =
            classes()
                    .should(notDeclareASortedCollectionOfWorkContainers())
                    .as("no field, return type or parameter may be a sorted collection whose elements or keys are "
                            + "raw WorkContainers")
                    .because("WorkContainer's equality is identity while its compareTo orders by topic, partition "
                            + "and offset - SortedSet and SortedMap REQUIRE those to agree, so such a collection "
                            + "would find one flight of a record under another's coordinates. Key it on a "
                            + "dedicated type, as RetryQueue does with WorkContainerSortKey, or hold the "
                            + "container as a map value.");

    private static ArchCondition<JavaClass> notDeclareASortedCollectionOfWorkContainers() {
        return new ArchCondition<>("not declare a sorted collection ordered by a raw WorkContainer") {
            @Override
            public void check(JavaClass javaClass, ConditionEvents events) {
                for (JavaField field : javaClass.getFields()) {
                    inspect(field.getType(), "field " + field.getFullName(), field, events);
                }
                for (JavaMethod method : javaClass.getMethods()) {
                    inspect(method.getReturnType(), "return type of " + method.getFullName(), method, events);
                    inspectParameters(method, events);
                }
                for (JavaConstructor constructor : javaClass.getConstructors()) {
                    inspectParameters(constructor, events);
                }
            }
        };
    }

    private static void inspectParameters(JavaCodeUnit codeUnit, ConditionEvents events) {
        List<JavaType> parameterTypes = codeUnit.getParameterTypes();
        for (int i = 0; i < parameterTypes.size(); i++) {
            inspect(parameterTypes.get(i),
                    "parameter " + i + " of " + codeUnit.getFullName(), codeUnit, events);
        }
    }

    /**
     * One declared type.
     * <p>
     * The parameterized check comes first and is the reason a RAW {@code TreeSet} field is not reported: it
     * carries no type arguments, so nothing here can say what it holds. That is a narrower miss than it sounds -
     * a raw sorted collection is already a compiler warning under this build's {@code -Xlint:all} - but it is a
     * miss, and it belongs with the gaps in the class javadoc rather than being discovered later.
     *
     * @param declared the declared type, which must be parameterized for the element or key to be visible at all
     * @param where    human-readable position, for a message a reader can act on without opening the class
     */
    private static void inspect(JavaType declared, String where, Object owner, ConditionEvents events) {
        if (!(declared instanceof JavaParameterizedType)) {
            return;
        }
        JavaClass erasure = declared.toErasure();
        if (ORDERED_BY_THEIR_ELEMENTS_OR_KEYS.stream().noneMatch(erasure::isAssignableTo)) {
            return;
        }
        List<JavaType> typeArguments = ((JavaParameterizedType) declared).getActualTypeArguments();
        if (typeArguments.isEmpty()) {
            return;
        }
        // Argument 0 is the ELEMENT for the set- and queue-shaped types and the KEY for the map-shaped ones, which
        // is exactly the position whose contract requires ordering to agree with equality. The value position of a
        // sorted map is unconstrained, and ProcessingShard.workMap and RetryQueue.sorted both use it.
        JavaClass elementOrKey = typeArguments.get(0).toErasure();
        if (!elementOrKey.getName().equals(WorkContainer.class.getName())) {
            return;
        }
        events.add(SimpleConditionEvent.violated(owner,
                where + " is a " + erasure.getName() + " ordered by a raw " + WorkContainer.class.getSimpleName()
                        + " - its compareTo orders by topic, partition and offset while its equality is identity, "
                        + "so a sorted collection would treat two flights of one record as the same element. "
                        + "Key it on a dedicated type (RetryQueue uses WorkContainerSortKey), or hold the "
                        + "container as a map value."));
    }
}
