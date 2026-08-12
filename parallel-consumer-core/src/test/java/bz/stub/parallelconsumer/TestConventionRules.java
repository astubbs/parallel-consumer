package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import java.util.Arrays;
import java.util.List;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
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
                    // org.testcontainers.kafka is where the modern module-specific containers live
                    // (KafkaContainer for apache/kafka, ConfluentKafkaContainer for the vendor images);
                    // org.testcontainers.containers holds GenericContainer and the deprecated originals.
                    // Both need naming, or a test can reach Docker from the unit suite through the new package.
                    .orShould().dependOnClassesThat().resideInAnyPackage("org.testcontainers.containers..", "org.testcontainers.kafka..", "org.testcontainers.junit..")
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

    private static final List<String> TEST_METHOD_ANNOTATIONS = Arrays.asList(
            "org.junit.jupiter.api.Test",
            "org.junit.jupiter.api.RepeatedTest",
            "org.junit.jupiter.api.TestFactory",
            "org.junit.jupiter.api.TestTemplate",
            "org.junit.jupiter.params.ParameterizedTest");

    private static final DescribedPredicate<JavaClass> HAVE_A_TEST_METHOD =
            new DescribedPredicate<JavaClass>("have at least one JUnit test method") {
                @Override
                public boolean test(JavaClass javaClass) {
                    return javaClass.getMethods().stream()
                            .flatMap(method -> method.getAnnotations().stream())
                            .anyMatch(annotation -> TEST_METHOD_ANNOTATIONS.contains(annotation.getRawType().getName()));
                }
            };

    private static final ArchCondition<JavaClass> HAVE_A_NAME_SUREFIRE_COLLECTS =
            new ArchCondition<JavaClass>("be named so surefire collects it") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents events) {
                    String name = javaClass.getSimpleName();
                    // mirrors surefire's default includes
                    boolean collected = name.startsWith("Test")
                            || name.endsWith("Test")
                            || name.endsWith("Tests")
                            || name.endsWith("TestCase");
                    if (!collected) {
                        events.add(SimpleConditionEvent.violated(javaClass, javaClass.getName()
                                + " has test methods but its name matches none of surefire's default includes "
                                + "(Test*, *Test, *Tests, *TestCase), so it is never executed - rename it"));
                    }
                }
            };

    /**
     * A class holding test methods must be NAMED so that surefire actually collects it.
     * <p>
     * This repo declares no {@code <includes>} for surefire, so it uses the defaults:
     * {@code Test*.java}, {@code *Test.java}, {@code *Tests.java}, {@code *TestCase.java}. A test class named
     * anything else is never run - and, worse, never reported as missing: the suite is simply green without it.
     * That is exactly how {@code MockConsumerTestWith{CommitTimeout,SaslAuthentication}Exception} and
     * {@code MockConsumerTestWithEarlyClose} sat dormant, and how a regression test added in PR astubbs#100 would have
     * been a permanent no-op had this rule existed to catch it (it was renamed instead).
     * <p>
     * Exempt: {@code @Nested} classes (JUnit finds them via their enclosing class, so the patterns do not
     * apply), interfaces and abstract bases (their implementors/subclasses are what run), and integration
     * tests - failsafe selects those by <em>package</em>
     * ({@code **}{@code /integrationTest*}{@code /**}{@code /*.java}), not by class name, so an {@code *IT} name
     * is correct there.
     */
    @ArchTest
    static final ArchRule test_classes_must_be_named_so_surefire_collects_them =
            classes()
                    .that(HAVE_A_TEST_METHOD)
                    .and().areNotInterfaces()
                    .and().doNotHaveModifier(JavaModifier.ABSTRACT)
                    // @Nested classes are discovered THROUGH their (correctly named) enclosing class, so
                    // surefire never selects them by name and the naming patterns simply don't apply. Without
                    // this, the first @Nested test added anywhere would fail the build, and the violation
                    // message would tell you to rename it - actively wrong advice. Note this exempts only the
                    // annotation, not inner classes generally: a plain inner class holding @Test methods really
                    // is uncollected, and should still be flagged.
                    .and().areNotAnnotatedWith("org.junit.jupiter.api.Nested")
                    .and().resideOutsideOfPackages("..integrationTest..", "..integrationTests..")
                    .should(HAVE_A_NAME_SUREFIRE_COLLECTS)
                    .because("surefire only collects Test*.java / *Test.java / *Tests.java / *TestCase.java - a "
                            + "test class named anything else silently never runs, and the suite stays green "
                            + "without it")
                    // Several modules legitimately match nothing: the example modules keep their tests in
                    // integrationTests packages (exempted above, since failsafe selects those by package), so
                    // this rule evaluates against an empty set there. That is a pass, not a violation -
                    // without this, ArchUnit's verifyNoEmptyShouldIfEnabled fails those modules.
                    .allowEmptyShould(true);

}
