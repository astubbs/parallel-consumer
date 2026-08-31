package bz.stub.parallelconsumer.client.grpc;
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
 * Same reasoning as the direct sibling: the conformance suite arrives by subclassing, and a subclass surefire
 * does not collect is a suite that silently never runs.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer.client.grpc", importOptions = ImportOption.OnlyIncludeTests.class)
class TestConventionsArchTest {

    @ArchTest
    static final ArchTests conventions = ArchTests.in(TestConventionRules.class);
}
