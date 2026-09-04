package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static com.google.common.truth.Truth.assertWithMessage;
import static pl.tlinkowski.unij.api.UniSets.of;

/**
 * {@link RunTagFilter} decides whether a claim proof this run never selected is allowed to count as coverage, so
 * getting its two JUnit rules right is what stands between the register and a report over tests that did not run.
 */
class RunTagFilterTest {

    private static final Set<String> UNTAGGED = Collections.emptySet();

    private static RunTagFilter excluding(String excludedCsv) {
        return new RunTagFilter("", excludedCsv);
    }

    @Test
    void anExcludedTagDeselects() {
        RunTagFilter filter = excluding("performance,chaos,quarantined,lincheck");

        assertThat(filter.selects(of("transactions"))).isTrue();
        assertThat(filter.selects(UNTAGGED)).isTrue();
        assertThat(filter.selects(of("quarantined"))).isFalse();
        assertThat(filter.selects(of("transactions", "chaos"))).isFalse();
    }

    @Test
    void anIncludeFilterDeselectsEverythingItDoesNotName() {
        RunTagFilter filter = new RunTagFilter("chaos", "");

        assertThat(filter.selects(of("chaos"))).isTrue();
        assertThat(filter.selects(of("transactions"))).isFalse();
        assertWithMessage("an untagged test carries no tag the include names, so an include filter drops it - "
                + "which is why -Dincluded.groups=performance runs no untagged unit test")
                .that(filter.selects(UNTAGGED))
                .isFalse();
    }

    /**
     * The rule {@code bin/lincheck-test.sh} exists to work around: its include alone selects nothing, because
     * {@code lincheck} is in the pom's default exclusions and exclusion wins.
     */
    @Test
    void exclusionBeatsInclusion() {
        RunTagFilter filter = new RunTagFilter("lincheck", "performance,chaos,quarantined,lincheck");

        assertWithMessage("naming a tag in BOTH lists must deselect - the reason -Dincluded.groups=lincheck has to "
                + "be paired with -Dexcluded.groups=")
                .that(filter.selects(of("lincheck")))
                .isFalse();
    }

    /**
     * Empty and absent arrive here as different values - surefire forwards an empty {@code ${included.groups}} as
     * an empty string - and both must mean "no filter of this kind" rather than "a filter matching nothing", which
     * would deselect the whole suite.
     */
    @Test
    void anEmptyOrAbsentFilterFiltersNothing() {
        for (RunTagFilter filter : of(new RunTagFilter("", ""), new RunTagFilter(null, null),
                new RunTagFilter("  ", " , "))) {
            assertThat(filter.selects(of("quarantined"))).isTrue();
            assertThat(filter.selects(UNTAGGED)).isTrue();
        }
    }

    @Test
    void whitespaceAroundATagIsIgnored() {
        assertThat(excluding(" performance , chaos ").selects(of("chaos"))).isFalse();
    }

    @Test
    void theReasonNamesTheTagThatDeselected() {
        assertThat(excluding("performance,chaos").whyNotSelected(of("transactions", "chaos")))
                .isEqualTo("this run excludes chaos");
        assertThat(new RunTagFilter("chaos", "").whyNotSelected(of("transactions")))
                .isEqualTo("this run includes only chaos and it carries transactions");
        assertWithMessage("a selected test has no reason to give, and an empty string is what the caller uses to "
                + "tell the two apart")
                .that(excluding("performance").whyNotSelected(of("transactions")))
                .isEmpty();
    }

    /**
     * The guard on the pom forwarding itself: a Maven run that cannot read its own filters must say so rather than
     * conclude that nothing was filtered.
     * <p>
     * This is the whole reason {@link RunTagFilter#read} does not simply default to an empty filter. The gates
     * built on it exist to catch a report issued over tests that never ran, and quietly reading a dropped
     * {@code systemPropertyVariables} block as "no exclusions" would make them issue exactly that report about
     * themselves. Nothing in the pom can cover this path - it fires only when the pom is wrong.
     */
    @Test
    void aMavenRunThatCannotSeeItsOwnFiltersRefusesToGuess() {
        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> RunTagFilter.read(null, null, true));
        assertWithMessage("the message has to name the file to edit - whoever hits this is not the person who "
                + "deleted the block")
                .that(thrown).hasMessageThat().contains("pom.xml");

        assertThrows(IllegalStateException.class, () -> RunTagFilter.read("", null, true));
        assertThrows(IllegalStateException.class, () -> RunTagFilter.read(null, "", true));
    }

    /**
     * Outside a Maven test run there is no Maven filter to lose, and absent genuinely means unfiltered - an IDE
     * launch, and pitest's minion JVMs, which run {@code bz.stub.parallelconsumer.*} with neither the forwarded
     * properties nor surefire's marker. Erroring there would break the mutation lane over a filter that was never
     * applied to it.
     */
    @Test
    void outsideMavenAnAbsentFilterIsSimplyUnfiltered() {
        assertThat(RunTagFilter.read(null, null, false).selects(of("quarantined"))).isTrue();
    }

    /**
     * BOTH test plugins must forward the properties, and only the pom can say so.
     * <p>
     * {@link RunTagFilter#read} catches the block vanishing entirely, because a run that cannot read its filters
     * raises. It cannot catch the block being present on only ONE plugin: surefire runs would still read their
     * filters and pass, and the gate that needs this today is a surefire test, so nothing would go red until a
     * failsafe gate started reading a filter that was not there. That is the same asymmetry
     * {@code QuarantinedAnnotationContractTest#bothSurefireAndFailsafeBindTheGroupProperties} exists for - a P1
     * where only failsafe was wired and unit-lane tag filtering was a silent no-op - so it gets the same guard.
     * <p>
     * Asserted per plugin block rather than by counting occurrences in the whole file. A review pointed out that a
     * bare count of two is also satisfied by one plugin carrying the block twice, which is the failure this test
     * names in its own message - a gate that cannot tell the state it forbids from the state it requires is the
     * defect this whole PR is about.
     */
    @Test
    void bothSurefireAndFailsafeForwardTheFiltersToTheTestJvm() throws IOException {
        String pom = new String(Files.readAllBytes(RepoRoot.find().resolve("pom.xml")), StandardCharsets.UTF_8);

        for (String plugin : of("maven-surefire-plugin", "maven-failsafe-plugin")) {
            String block = configurationBlockOf(pom, plugin);
            for (String forwarding : of("<pc.run.includedGroups>${included.groups}</pc.run.includedGroups>",
                    "<pc.run.excludedGroups>${excluded.groups}</pc.run.excludedGroups>")) {
                assertWithMessage(plugin + " must forward " + forwarding + " - with it on only one plugin, a gate "
                        + "reading the filters in the other lane would see nothing and could not tell that from an "
                        + "unfiltered run")
                        .that(block)
                        .contains(forwarding);
            }
        }
    }

    /**
     * The text of one plugin declaration, from its {@code artifactId} to the end of that declaration - so a
     * containment check below cannot be satisfied by something sitting in a different plugin.
     */
    private static String configurationBlockOf(String pom, String artifactId) {
        int start = pom.indexOf("<artifactId>" + artifactId + "</artifactId>");
        assertWithMessage("the root pom must declare " + artifactId + " for this gate to have anything to check")
                .that(start).isGreaterThan(-1);
        int end = pom.indexOf("</plugin>", start);
        assertWithMessage(artifactId + "'s declaration must be closed").that(end).isGreaterThan(start);
        return pom.substring(start, end);
    }

    /**
     * The filters go into the assertion message of every gate using this class, so a run that fails one can be
     * diagnosed from CI output alone rather than by guessing which invocation produced it.
     */
    @Test
    void itDescribesBothFiltersForTheFailureMessage() {
        assertThat(new RunTagFilter("chaos", "performance,quarantined").toString())
                .isEqualTo("included.groups=[chaos] excluded.groups=[performance,quarantined]");
    }
}
