package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.TestConventionRules;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/**
 * Applies the shared {@link TestConventionRules} to this module's test classes - the rule logic lives once in
 * {@link TestConventionRules} (core test-jar); this only points ArchUnit at this module's packages.
 * <p>
 * Every sibling module under the Java client carries one of these, and a demo is not exempt: the naming rule
 * it enforces is what stops a test class from being silently uncollected by surefire, which would leave this
 * module reporting green having run nothing.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer.client.demo", importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
