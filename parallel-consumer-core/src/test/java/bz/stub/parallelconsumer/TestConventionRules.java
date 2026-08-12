package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Shared test-architecture conventions, defined ONCE here and evaluated per-module.
 * <p>
 * Each module has a tiny {@code TestConventionsArchTest} that points ArchUnit at its own packages (via
 * {@code @AnalyzeClasses}) and pulls in these rules with {@code ArchTests.in(TestConventionRules.class)} - so
 * the rule logic lives in one place (this class, shipped in the core test-jar) instead of being copy-pasted
 * into every module. The per-module class only supplies the packages to scan (that part is irreducible: a
 * module can only see its own classes on its own test classpath).
 * <p>
 * The rules are name-based (no compile-time coupling to core types), so they apply unchanged to any module.
 * This class has no {@code @AnalyzeClasses}, so it is a rule <em>library</em> - never executed standalone,
 * only referenced.
 */
public class TestConventionRules {

    /**
     * Integration tests must NOT live in the unit (surefire) suite: a test that uses Testcontainers or extends
     * {@code BrokerIntegrationTest} needs Docker and is slow, so it must live in an {@code integrationTest}
     * package (run by failsafe), never a surefire-scanned one - otherwise it silently inflates the fast unit
     * suite.
     */
    @ArchTest
    static final ArchRule integration_tests_must_live_in_an_integrationTest_package =
            noClasses()
                    .that().resideOutsideOfPackages("..integrationTest..", "..integrationTests..")
                    .should().beAssignableTo("bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest")
                    .orShould().dependOnClassesThat().resideInAnyPackage("org.testcontainers.containers..", "org.testcontainers.junit..")
                    .because("integration tests (extend BrokerIntegrationTest or use Testcontainers) must live in "
                            + "an 'integrationTest' package so failsafe runs them (with Docker), not surefire");

    /**
     * Never use the SHADED third-party libraries that Testcontainers bundles under
     * {@code org.testcontainers.shaded.*} (e.g. a shaded Awaitility or Hamcrest). Use the real
     * {@code org.awaitility} / {@code org.hamcrest} instead, so there is exactly one copy of each library - and
     * one set of defaults (e.g. Awaitility's default timeout) - on the classpath. Mixing the shaded and real
     * copies is confusing and has bitten us: a shaded Awaitility import silently changed which default timeout
     * applied, and cross-typed the {@code untilAtomic(..)} Hamcrest matchers.
     */
    @ArchTest
    static final ArchRule tests_must_not_use_shaded_libraries =
            noClasses()
                    .should().dependOnClassesThat().resideInAnyPackage("org.testcontainers.shaded..")
                    .because("use the real org.awaitility / org.hamcrest, not Testcontainers' shaded copies, so "
                            + "there is a single copy (and a single set of defaults) of each library on the classpath");
}
