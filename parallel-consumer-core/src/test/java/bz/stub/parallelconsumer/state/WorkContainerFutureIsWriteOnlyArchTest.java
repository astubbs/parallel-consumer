package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * {@link WorkContainer#getFuture()} must have no production callers.
 * <p>
 * This is not a style preference. The future is set only when a batch is dispatched to the worker pool, so
 * it is null for every container that has not been dispatched - including every batch
 * {@code AbstractParallelEoSStreamProcessor#submitWorkToPool} declines to dispatch and drops. A production
 * reader would see null on those paths and have no way to tell "never ran" from "finished".
 * <p>
 * The precondition used to live in a comment beside the discard. A comment does not fail a build, so it
 * would have gone stale the first time somebody added a caller and nothing would have said so. This rule is
 * the enforcement. If it fails, do not delete it: drop the new caller, or ask the question through a named
 * predicate on {@link WorkContainer} that reads the field, rather than through this getter.
 * <p>
 * Tests may read it - {@code SubmitWorkToPoolShutdownRaceTest} asserts on it precisely to pin that a dropped
 * batch was never dispatched - so this analyses main classes only.
 */
@AnalyzeClasses(packages = "bz.stub.parallelconsumer", importOptions = ImportOption.DoNotIncludeTests.class)
class WorkContainerFutureIsWriteOnlyArchTest {

    @ArchTest
    static final ArchRule work_container_future_has_no_production_readers =
            noClasses()
                    .should().callMethod(WorkContainer.class, "getFuture")
                    .because("the future is set only by dispatch, so a production reader would observe null for "
                            + "any container that has not been dispatched - including every batch dropped "
                            + "undispatched - and could not tell that from completion.");

    /**
     * The rule above names {@code getFuture} as a string, and ArchUnit does not check that the target exists: rename
     * or delete the getter and the rule matches nothing and passes, having asserted nothing. That is the silent
     * false-green this repo keeps finding, so pin the name the rule depends on.
     */
    @Test
    void theMethodTheRuleNamesStillExists() throws NoSuchMethodException {
        assertThat(WorkContainer.class.getDeclaredMethod("getFuture")).isNotNull();
    }
}
