package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/**
 * Applies the shared {@link TestConventionRules} to this module's test classes - the rule logic lives once in
 * {@link TestConventionRules}; this only points ArchUnit at this module's packages.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer", importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
