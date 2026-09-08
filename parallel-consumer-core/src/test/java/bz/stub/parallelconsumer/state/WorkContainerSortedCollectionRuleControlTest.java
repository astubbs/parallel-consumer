package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.archfixture.SortedCollectionOfWorkContainers;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The positive control for {@link WorkContainerIsNeverInASortedCollectionArchTest}: proof that it can still FAIL,
 * and on each of the four positions it claims to inspect.
 * <p>
 * <b>A rule that is green because the code complies and a rule that is green because it cannot see are the same
 * green</b>, and this rule is designed to be green forever - nothing in the tree violates it today, so a run of
 * the production suite distinguishes those two states not at all. That is the state
 * {@code docs/inflight/static-archunit-main-code-rules.md} records as the recurring failure of ArchUnit rules
 * here: one naming a member by string went on passing after the member was renamed, having asserted nothing.
 * <p>
 * <b>Why the fixture is imported by hand rather than by {@code @AnalyzeClasses}.</b> The rule's own import carries
 * {@link com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests}, so it cannot see the test tree -
 * which is what keeps a deliberate violation from turning the production rule permanently red. The control
 * imports the fixture package itself and hands those classes to
 * {@link WorkContainerIsNeverInASortedCollectionArchTest#no_sorted_collection_is_ordered_by_a_raw_work_container},
 * the same rule object the build evaluates, not a copy - because a copied rule controls for a copy.
 *
 * @author Antony Stubbs
 * @see SortedCollectionOfWorkContainers
 */
class WorkContainerSortedCollectionRuleControlTest {

    private static AssertionError checkFixtureAndCaptureTheReport() {
        JavaClasses fixture = new ClassFileImporter().importPackagesOf(SortedCollectionOfWorkContainers.class);

        return assertThrows(AssertionError.class,
                () -> WorkContainerIsNeverInASortedCollectionArchTest
                        .no_sorted_collection_is_ordered_by_a_raw_work_container.check(fixture),
                "the rule saw a TreeSet, a SortedMap and a NavigableSet keyed on a raw WorkContainer and "
                        + "reported nothing - which is the false green this control exists to catch");
    }

    /**
     * All four positions in one report, because asserting one of them would leave the other three free to be
     * dropped silently - the walk is four separate loops and nothing else would notice one going missing.
     */
    @Test
    void theRuleReportsAWorkContainerAsTheElementOrKeyInEveryDeclarationPositionItInspects() {
        AssertionError violation = checkFixtureAndCaptureTheReport();

        assertThat(violation).hasMessageThat().contains("orderedByTheContainerItself");
        assertThat(violation).hasMessageThat().contains("descriptionsByContainer");
        assertThat(violation).hasMessageThat().contains("holdsAllOf");
        // The constructor parameter. ArchUnit names a constructor <init>, so this is the whole of its identity
        // in the report - and it is the position a fields-and-methods walk would miss without going red.
        assertThat(violation).hasMessageThat()
                .contains(SortedCollectionOfWorkContainers.class.getName() + ".<init>");
    }

    /**
     * The negative half, and the more important one: a container in a sorted map's VALUE position is correct, and
     * a rule that reported it would be red on arrival against {@code ProcessingShard.workMap} and would be
     * deleted rather than obeyed. If someone widens the check from type argument 0 to all of them, this is what
     * says so.
     */
    @Test
    void theRuleDoesNotReportAWorkContainerHeldAsASortedMapValue() {
        AssertionError violation = checkFixtureAndCaptureTheReport();

        assertThat(violation).hasMessageThat().doesNotContain("containersAsValues");
    }
}
