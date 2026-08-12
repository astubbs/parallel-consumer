package bz.stub.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;
import bz.stub.parallelconsumer.TestConventionRules;

/** Applies the repository's shared test conventions only to this module's fork-original tests. */
@AnalyzeClasses(
        packages = "bz.stub.parallelconsumer.connect",
        importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
