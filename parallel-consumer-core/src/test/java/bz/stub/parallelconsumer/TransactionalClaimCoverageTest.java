package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Guards the {@link TransactionalClaim} register in both directions.
 * <p>
 * Coverage rot and content rot are different failures and only one of them is obvious. A claim losing its last
 * test is at least visible in a diff; a claim's sentence being edited in the javadoc while the register keeps
 * asserting the old wording is not visible anywhere, and leaves the register quietly describing a promise the
 * project no longer makes. Both fail here.
 * <p>
 * Deliberately broker-free and untagged, so it gates every default build - the same reasoning as
 * {@code ProgressProbeLedgerIT}: a register that is only checked when someone remembers to run a slow lane is
 * not a gate.
 * <p>
 * Being untagged is also what makes {@link #everyCoveredClaimMustHaveAProofThisRunCanSelect()} possible: this
 * class keeps running in runs that deselect the proofs, which is precisely when someone needs to be told that the
 * report below cannot mean what it appears to mean. Tagging it would buy silence in that case instead of an
 * explanation, and would take the two source-drift checks - which depend on no test running at all - down with it.
 */
class TransactionalClaimCoverageTest {

    /**
     * The compiled test classes, imported once and shared by every method here.
     * <p>
     * Scans the compiled test classes rather than the whole classpath: both the unit lane and the integration
     * lane compile into this one output directory (the root pom adds {@code src/test-integration/java} as a test
     * source root via build-helper), so one import sees {@link ProvesClaim} annotations from both. That shared
     * output is what makes a single register able to span the two lanes at all.
     * <p>
     * Held statically because importing that directory is the expensive part of this class and two of the tests
     * below need it - the classes cannot change while the JVM is running, so a second import could only ever
     * produce the same answer at the same cost.
     */
    private static JavaClasses compiledTestClasses;

    @BeforeAll
    static void importCompiledTestClasses() {
        try {
            Path testClasses = Paths.get(TransactionalClaimCoverageTest.class
                    .getProtectionDomain().getCodeSource().getLocation().toURI());
            compiledTestClasses = new ClassFileImporter().importPath(testClasses);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("could not locate the compiled test classes to scan for @ProvesClaim", e);
        }
    }

    private static List<JavaMethod> claimProvingMethods() {
        return compiledTestClasses.stream()
                .flatMap(javaClass -> javaClass.getMethods().stream())
                .filter(method -> method.isAnnotatedWith(ProvesClaim.class))
                .collect(java.util.stream.Collectors.toList());
    }

    private static Map<TransactionalClaim, List<String>> referencesByClaim() {
        Map<TransactionalClaim, List<String>> references = new EnumMap<>(TransactionalClaim.class);
        for (JavaMethod method : claimProvingMethods()) {
            String where = method.getOwner().getName() + "#" + method.getName();
            for (TransactionalClaim claim : method.getAnnotationOfType(ProvesClaim.class).value()) {
                references.computeIfAbsent(claim, ignored -> new ArrayList<>()).add(where);
            }
        }
        return references;
    }

    /**
     * Every claim whose status asserts coverage must actually be referenced by a test.
     * <p>
     * The statuses that are <em>not</em> enforced are the honest escape hatches: a claim Kafka owns, and a claim
     * a later phase owns. Neither is allowed to be silent - see {@link #parkedClaimsMustSayWhoOwnsThem()}.
     */
    @Test
    void everyClaimWeSayIsCoveredHasATestReferencingIt() {
        Map<TransactionalClaim, List<String>> references = referencesByClaim();

        Set<TransactionalClaim> uncovered = new LinkedHashSet<>();
        for (TransactionalClaim claim : TransactionalClaim.values()) {
            if (claim.getStatus().isCoverageEnforced() && !references.containsKey(claim)) {
                uncovered.add(claim);
            }
        }

        assertWithMessage("These claims are recorded as covered but no test references them. Either annotate the "
                + "proving test with @ProvesClaim, or move the claim to NOT_YET_COVERED with a reason naming its "
                + "owner - do not leave the register asserting coverage that does not exist")
                .that(uncovered)
                .isEmpty();
    }

    /**
     * The other direction: a claim's recorded sentence must still be present, verbatim, in the file it was taken
     * from.
     * <p>
     * This is the check that makes the register a gate rather than a snapshot. Without it, editing or softening a
     * guarantee in the javadoc leaves the suite green and the register stale, which is precisely the silent rot
     * the register exists to prevent.
     */
    @Test
    void everyRecordedSentenceStillAppearsInItsSource() {
        Map<TransactionalClaim.Source, String> publishedText = new EnumMap<>(TransactionalClaim.Source.class);
        for (TransactionalClaim.Source source : TransactionalClaim.Source.values()) {
            publishedText.put(source, source.readPublishedText());
        }

        List<String> drifted = new ArrayList<>();
        for (TransactionalClaim claim : TransactionalClaim.values()) {
            String expected = TransactionalClaim.normalise(claim.getDocumentedSentence());
            if (!publishedText.get(claim.getSource()).contains(expected)) {
                drifted.add(claim.name() + " no longer appears in " + claim.getSource().getRepoRelativePath()
                        + "\n    register has: " + expected);
            }
        }

        assertWithMessage("The documentation moved and this register did not follow. For each claim below, either "
                + "restore the sentence in the source file, or update the register to the new wording - and if the "
                + "guarantee itself changed, that is a product decision, not a test fix")
                .that(drifted)
                .isEmpty();
    }

    /**
     * A claim may only sit in a non-enforced status if it says who owns it. Without this, {@code NOT_YET_COVERED}
     * becomes a place to park anything inconvenient and the register stops meaning anything.
     */
    @Test
    void parkedClaimsMustSayWhoOwnsThem() {
        Set<TransactionalClaim.Status> nonEnforced = EnumSet.noneOf(TransactionalClaim.Status.class);
        for (TransactionalClaim.Status status : TransactionalClaim.Status.values()) {
            if (!status.isCoverageEnforced()) {
                nonEnforced.add(status);
            }
        }

        List<String> unexplained = new ArrayList<>();
        for (TransactionalClaim claim : TransactionalClaim.values()) {
            if (nonEnforced.contains(claim.getStatus())
                    && (claim.getNote() == null || claim.getNote().trim().isEmpty())) {
                unexplained.add(claim.name() + " (" + claim.getStatus() + ")");
            }
        }

        assertWithMessage("A claim in a non-enforced status must record why - which unit owns it, or why it is not "
                + "ours to prove")
                .that(unexplained)
                .isEmpty();
    }

    private static final String DISABLED = "org.junit.jupiter.api.Disabled";

    private static final String QUARANTINED = Quarantined.class.getName();

    /**
     * The JUnit annotations that actually make a method an executable test. A {@link ProvesClaim} on a method
     * carrying none of them is a private helper, however well named.
     */
    private static final List<String> EXECUTABLE_TEST_ANNOTATIONS = java.util.Arrays.asList(
            "org.junit.jupiter.api.Test",
            "org.junit.jupiter.params.ParameterizedTest",
            "org.junit.jupiter.api.RepeatedTest");

    /**
     * A {@link ProvesClaim} on a method no test runner would ever execute is coverage on paper only.
     * <p>
     * Surefire selects the unit lane by class <em>name</em> and failsafe selects the integration lane by
     * <em>package</em>, so both routes count - a rule that knew only surefire's naming patterns would reject
     * every integration-lane claim proof, which is most of the visibility half of this register. Mirrors
     * {@link TestConventionRules#test_classes_must_be_named_so_surefire_collects_them} and its integration-test
     * exemption.
     * <p>
     * Where the class lives is only half of "will it run". A method the runner <em>collects</em> but never
     * <em>executes</em> is the same failure wearing a better disguise, and it fails in the direction that keeps the
     * register green: the claim stays recorded as covered while nothing exercises it. Three ways that happens, all
     * rejected here - no executable JUnit annotation at all, {@code @Disabled}, and {@link Quarantined}. The last
     * is not hypothetical: quarantining is a sanctioned move in this repo and the default {@code excluded.groups}
     * drops the tag from the gating lanes, so an ordinary, correct quarantine would silently hollow out a claim.
     * The annotation on the owning class counts too - it disables or excludes every method in it.
     * <p>
     * Every rule here is about the code as WRITTEN, and so gives the same answer in every run - including the
     * {@link Quarantined} one, which is a standing policy ("a claim proof may not be quarantined, because the
     * gating lanes drop that tag") rather than a reading of the run in hand.
     * {@link #everyCoveredClaimMustHaveAProofThisRunCanSelect()} asks the other question - what THIS run
     * selected - so the two overlap on the default build and neither one subsumes the other. Do not collapse them.
     */
    @Test
    void claimProofsMustLiveWhereATestRunnerWillFindThem() {
        List<String> unreachable = new ArrayList<>();
        for (JavaMethod method : claimProvingMethods()) {
            String className = method.getOwner().getName();
            String simpleName = method.getOwner().getSimpleName();
            String where = className + "#" + method.getName();

            boolean failsafeCollects = className.contains(".integrationTest")
                    || className.contains(".integrationTests");
            // TestConventionRules owns this rule, so the two gates cannot drift apart
            boolean surefireCollects = TestConventionRules.surefireCollects(simpleName);
            boolean nested = method.getOwner().isAnnotatedWith("org.junit.jupiter.api.Nested");

            if (!failsafeCollects && !surefireCollects && !nested) {
                unreachable.add(where + " - its class is collected by neither surefire "
                        + "(Test*/*Test/*Tests/*TestCase) nor failsafe (an integrationTest(s) package)");
            }

            if (EXECUTABLE_TEST_ANNOTATIONS.stream().noneMatch(annotation -> method.isAnnotatedWith(annotation))) {
                unreachable.add(where + " - carries no @Test, @ParameterizedTest or @RepeatedTest, so no runner "
                        + "will ever execute it");
            }

            if (method.isAnnotatedWith(DISABLED)) {
                unreachable.add(where + " - is @Disabled");
            } else if (method.getOwner().isAnnotatedWith(DISABLED)) {
                unreachable.add(where + " - its class " + simpleName + " is @Disabled");
            }

            if (method.isAnnotatedWith(QUARANTINED)) {
                unreachable.add(where + " - is @Quarantined, so the gating lanes exclude it");
            } else if (method.getOwner().isAnnotatedWith(QUARANTINED)) {
                unreachable.add(where + " - its class " + simpleName + " is @Quarantined, so the gating lanes "
                        + "exclude it");
            }
        }

        assertWithMessage("These methods claim to prove a documented guarantee but will not actually run - either "
                + "nothing collects them, or nothing executes them. A claim whose only proof is disabled, "
                + "quarantined or unannotated is covered on paper only: move the claim to NOT_YET_COVERED with a "
                + "reason naming its owner until a test that really runs proves it again, rather than leaving the "
                + "register asserting coverage nothing exercises")
                .that(unreachable)
                .isEmpty();
    }

    /**
     * The claim-coverage check again, but asking what THIS run selected rather than what the code says.
     * <p>
     * {@link #everyClaimWeSayIsCoveredHasATestReferencingIt()} and the checks around it read the code as written,
     * so they give the same answer in every run; this one reads the run in hand, and it exists because the two can
     * disagree while both stay green. {@code pom.xml} documents overriding {@code -Dexcluded.groups}, and nearly
     * every claim proof carries {@code @Tag("transactions")}: run
     * {@code -Dexcluded.groups=transactions,performance,chaos} and no proof is ever selected, while this class -
     * broker-free, untagged, and reading compiled annotations rather than results - still reports every claim
     * covered, every parked claim explained and every sentence intact. A fully green register over a run that
     * proved nothing, because the register's selection criteria and the proofs' selection criteria were disjoint.
     * <p>
     * Judged per CLAIM rather than per method, deliberately: a claim with two proofs, one of them tagged, is still
     * proven in a run that drops the tagged one, and failing there would be the register reporting a gap it does
     * not have. A claim with no proof at all is the sibling check's finding, not this one's, so it is left alone
     * here rather than reported twice.
     * <p>
     * <b>What this can and cannot see.</b> Tag filters are the half that is shared: surefire and failsafe are both
     * configured from the same {@code ${included.groups}}/{@code ${excluded.groups}} pair, so a tag verdict reached
     * here holds for a proof in either lane. Lane selection is the half that is not - {@code bin/ci-unit-test.sh}
     * passes {@code -DskipITs} and runs no integration proof at all - and checking that would contradict the
     * register's design, which spans the two lanes deliberately (see {@link #compiledTestClasses}). So this asks
     * only whether the tags would let a proof run, never whether this particular lane got to it.
     */
    @Test
    void everyCoveredClaimMustHaveAProofThisRunCanSelect() {
        RunTagFilter filter = RunTagFilter.ofCurrentRun();

        Set<TransactionalClaim> selectable = EnumSet.noneOf(TransactionalClaim.class);
        Map<TransactionalClaim, List<String>> deselected = new EnumMap<>(TransactionalClaim.class);
        for (JavaMethod method : claimProvingMethods()) {
            Set<String> tags = effectiveTagsOf(method);
            boolean runWillSelect = filter.selects(tags);
            String where = method.getOwner().getName() + "#" + method.getName();
            for (TransactionalClaim claim : method.getAnnotationOfType(ProvesClaim.class).value()) {
                if (runWillSelect) {
                    selectable.add(claim);
                } else {
                    deselected.computeIfAbsent(claim, ignored -> new ArrayList<>())
                            .add(where + " (" + filter.whyNotSelected(tags) + ")");
                }
            }
        }

        List<String> unexercised = new ArrayList<>();
        for (TransactionalClaim claim : TransactionalClaim.values()) {
            List<String> proofsThisRunDropped = deselected.get(claim);
            // a claim with no proof AT ALL is everyClaimWeSayIsCoveredHasATestReferencingIt's finding, not this
            // one's - null here means exactly that, so it is passed over rather than reported twice
            if (claim.getStatus().isCoverageEnforced()
                    && !selectable.contains(claim)
                    && proofsThisRunDropped != null) {
                unexercised.add(claim.name() + " - every proof deselected: "
                        + String.join(", ", proofsThisRunDropped));
            }
        }

        assertWithMessage("This run's tag filters deselected every proof of the claims below, so the coverage this "
                + "register reports was not exercised by anything here (" + filter + "). Either stop excluding the "
                + "tags that carry the claim proofs, or accept that the register cannot certify this run - it is "
                + "reporting on tests that did not execute")
                .that(unexercised)
                .isEmpty();
    }

    /**
     * The tags JUnit would apply to this method - its own, plus its declaring class's.
     * <p>
     * Resolved with JUnit's own {@link AnnotationSupport} rather than by matching {@code @Tag} annotations by hand,
     * so the meta-annotated and inherited cases come out exactly as the launcher computes them - notably
     * {@link Quarantined}, which is a {@code @Tag("quarantined")} CARRIER rather than a {@code @Tag} itself, and
     * the tags a proof inherits from a superclass.
     */
    private static Set<String> effectiveTagsOf(JavaMethod method) {
        Method reflected = method.reflect();
        List<Tag> onTheMethod = AnnotationSupport.findRepeatableAnnotations(reflected, Tag.class);
        List<Tag> onTheClass = AnnotationSupport.findRepeatableAnnotations(reflected.getDeclaringClass(), Tag.class);

        Set<String> tags = new LinkedHashSet<>(onTheMethod.size() + onTheClass.size());
        for (Tag tag : onTheMethod) {
            tags.add(tag.value().trim());
        }
        for (Tag tag : onTheClass) {
            tags.add(tag.value().trim());
        }
        return tags;
    }
}
