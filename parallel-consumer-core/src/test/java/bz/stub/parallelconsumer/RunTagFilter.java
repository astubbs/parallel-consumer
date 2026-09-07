package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The JUnit Platform tag filters the CURRENT run was launched with, readable from inside the test JVM.
 * <p>
 * A gate that reasons about whether some other test will run has to know what this run actually selected, and a
 * test JVM cannot see its own launcher's filters: surefire and failsafe pass {@code groups}/{@code excludedGroups}
 * to the platform, not to the tests. So {@code pom.xml} forwards the two properties behind them through
 * {@code systemPropertyVariables}, and this class is the reader. Both plugin configurations set them, and both from
 * the same {@code ${included.groups}}/{@code ${excluded.groups}} pair the filters themselves come from, so the
 * answer here cannot drift from the filtering that is really in force.
 * <p>
 * <b>An absent property is treated as a defect, not as "nothing excluded".</b> Silently reading a missing filter as
 * an empty one would turn any future pom edit that drops the forwarding into a gate that passes without having
 * checked anything - the exact failure the gates using this class exist to catch. Under Maven the properties are
 * therefore required; outside it (an IDE, a bare JUnit launcher) there is no Maven filter to report and an absent
 * value genuinely does mean "unfiltered", which is what a plain IDE launch does.
 */
final class RunTagFilter {

    /**
     * Forwarded from {@code ${included.groups}}. Empty means "no include filter", which is the pom's default and is
     * NOT the same as absent - surefire passes an empty value through as an empty string.
     */
    static final String INCLUDED_PROPERTY = "pc.run.includedGroups";

    /**
     * Forwarded from {@code ${excluded.groups}}. {@code pom.xml} owns its default and the help text explaining
     * each group, so the list is deliberately not repeated here.
     */
    static final String EXCLUDED_PROPERTY = "pc.run.excludedGroups";

    /**
     * Set by the surefire booter itself - which failsafe shares - rather than by this repo's configuration, so it
     * still says "a Maven test run" in exactly the case that matters here: the one where the pom stopped forwarding
     * the two properties above. A marker this repo set would vanish along with them and prove nothing.
     */
    private static final String MAVEN_FORK_MARKER = "surefire.real.class.path";

    private final Set<String> included;

    private final Set<String> excluded;

    RunTagFilter(String includedCsv, String excludedCsv) {
        this.included = parse(includedCsv);
        this.excluded = parse(excludedCsv);
    }

    static RunTagFilter ofCurrentRun() {
        return read(System.getProperty(INCLUDED_PROPERTY),
                System.getProperty(EXCLUDED_PROPERTY),
                System.getProperty(MAVEN_FORK_MARKER) != null);
    }

    /**
     * The decision {@link #ofCurrentRun()} makes, with the three ambient readings passed in rather than taken from
     * the JVM - so the missing-property branch can be tested without a test mutating global state, and so this
     * class behaves the same wherever it is loaded. It is loaded in more places than the gating lanes: pitest runs
     * {@code bz.stub.parallelconsumer.*} in minion JVMs of its own, which carry neither the forwarded properties
     * nor surefire's marker, and correctly land on the unfiltered reading below.
     */
    static RunTagFilter read(String includedCsv, String excludedCsv, boolean mavenTestRun) {
        if ((includedCsv == null || excludedCsv == null) && mavenTestRun) {
            throw new IllegalStateException("This is a Maven test run, but " + INCLUDED_PROPERTY + "/"
                    + EXCLUDED_PROPERTY + " did not reach the test JVM, so no gate here can tell which tests this "
                    + "run selected. Restore the systemPropertyVariables block in pom.xml that forwards "
                    + "${included.groups} and ${excluded.groups} to surefire AND failsafe - without it these gates "
                    + "would pass without having checked anything");
        }
        return new RunTagFilter(includedCsv, excludedCsv);
    }

    private static Set<String> parse(String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return Collections.emptySet();
        }
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(tag -> !tag.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Whether a test carrying these tags is selected by this run.
     * <p>
     * Mirrors JUnit Platform's two rules: an include filter, when present, requires at least one matching tag, and
     * <b>exclusion beats inclusion</b> - which is why {@code bin/lincheck-test.sh} has to pass
     * {@code -Dexcluded.groups=} alongside its include rather than the include alone.
     */
    boolean selects(Collection<String> tags) {
        if (!included.isEmpty() && Collections.disjoint(tags, included)) {
            return false;
        }
        return Collections.disjoint(tags, excluded);
    }

    /**
     * @return the reason this run would not select a test carrying these tags, or empty if it would
     */
    String whyNotSelected(Collection<String> tags) {
        Set<String> hitting = new LinkedHashSet<>(tags);
        hitting.retainAll(excluded);
        if (!hitting.isEmpty()) {
            return "this run excludes " + String.join(", ", hitting);
        }
        if (!included.isEmpty() && Collections.disjoint(tags, included)) {
            return "this run includes only " + String.join(", ", included)
                    + " and it carries " + (tags.isEmpty() ? "no tags" : String.join(", ", tags));
        }
        return "";
    }

    @Override
    public String toString() {
        return "included.groups=[" + String.join(",", included) + "] excluded.groups=[" + String.join(",", excluded) + "]";
    }
}
