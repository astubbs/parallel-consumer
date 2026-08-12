package bz.stub.parallelconsumer.examples.reactor;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;
import bz.stub.parallelconsumer.TestConventionRules;

/**
 * Applies the shared {@link TestConventionRules} to this module's test classes - the rule logic lives once in
 * {@link TestConventionRules} (core test-jar); this only points ArchUnit at this module's packages.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer.examples.reactor", importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
