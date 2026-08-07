package io.confluent.parallelconsumer;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

    /**
     * A {@link ProvesClaim} on a method no test runner would ever execute is coverage on paper only.
     * <p>
     * Surefire selects the unit lane by class <em>name</em> and failsafe selects the integration lane by
     * <em>package</em>, so both routes count - a rule that knew only surefire's naming patterns would reject
     * every integration-lane claim proof, which is most of the visibility half of this register. Mirrors
     * {@link TestConventionRules#test_classes_must_be_named_so_surefire_collects_them} and its integration-test
     * exemption.
     */
    @Test
    void claimProofsMustLiveWhereATestRunnerWillFindThem() {
        List<String> unreachable = new ArrayList<>();
        for (JavaMethod method : claimProvingMethods()) {
            String className = method.getOwner().getName();
            String simpleName = method.getOwner().getSimpleName();

            boolean failsafeCollects = className.contains(".integrationTest")
                    || className.contains(".integrationTests");
            boolean surefireCollects = simpleName.startsWith("Test")
                    || simpleName.endsWith("Test")
                    || simpleName.endsWith("Tests")
                    || simpleName.endsWith("TestCase");
            boolean nested = method.getOwner().isAnnotatedWith("org.junit.jupiter.api.Nested");

            if (!failsafeCollects && !surefireCollects && !nested) {
                unreachable.add(className + "#" + method.getName());
            }
        }

        assertWithMessage("These methods claim to prove a documented guarantee but sit in a class neither surefire "
                + "(Test*/*Test/*Tests/*TestCase) nor failsafe (an integrationTest(s) package) would collect, so "
                + "they never run and the claim is covered on paper only")
                .that(unreachable)
                .isEmpty();
    }
}
