package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.archfixture.BlockingReachThroughAMethodReference;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The positive control for {@code ArchitectureTest.rebalanceCallbacksMustNotBlock}: proof that it can still
 * FAIL, and specifically that it fails on the shape it was blind to.
 * <p>
 * <b>A rule that is green because the code complies and a rule that is green because it cannot see is the same
 * green.</b> That is not hypothetical here - it is what happened. Until 2026-09-03 the walk followed only
 * {@code getMethodCallsFromSelf()}, so {@code ShardManager.removeStaleContainers}'s
 * {@code .map(retryQueue::remove)} reached {@code RetryQueue.remove}'s unbounded write-lock acquire from all
 * three rebalance callbacks while the rule reported nothing on {@code onPartitionsAssigned} at all. Measured
 * again on 2026-09-03 before this test was written: restoring that method reference to the production tree
 * left the un-widened rule green at 3 tests / 0 failures.
 * <p>
 * So the rule now carries a fixture that reaches a deny-listed acquire through a method reference, and this
 * test asserts the rule reports it. Widen the deny list, refactor the walk, swap the ArchUnit version - if any
 * of that quietly stops the reference hop from being followed, this goes red instead of the whole rule going
 * quiet.
 * <p>
 * <b>Why the fixture is imported by hand rather than by {@code @AnalyzeClasses}.</b> The rule's own import
 * carries {@link com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests}, so it cannot see anything
 * in the test tree - which is exactly what keeps a deliberate violation from turning the production rule
 * permanently red. The control therefore imports the fixture package itself and hands those classes to
 * {@link ArchitectureTest#rebalanceCallbacksMustNotBlock}, the same rule object the build evaluates - not a
 * copy of it, because a copied rule controls for a copy.
 *
 * @author Antony Stubbs
 * @see BlockingReachThroughAMethodReference
 */
class RebalanceCallbackRuleControlTest {

    @Test
    void theRuleReportsABlockingAcquireReachedThroughAMethodReference() {
        JavaClasses fixture = new ClassFileImporter()
                .importPackagesOf(BlockingReachThroughAMethodReference.class);

        AssertionError violation = assertThrows(AssertionError.class,
                () -> ArchitectureTest.rebalanceCallbacksMustNotBlock.check(fixture),
                "the rule saw a rebalance callback reach a blocking write-lock acquire through a method "
                        + "reference and reported nothing - which is the false green this control exists to "
                        + "catch, and the state the rule was actually in until 2026-09-03");

        assertThat(violation).hasMessageThat()
                .contains(BlockingReachThroughAMethodReference.class.getName() + ".onPartitionsRevoked");
        assertThat(violation).hasMessageThat()
                .contains("java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock()");
        assertThat(violation).hasMessageThat()
                .contains("reaches blocking method reference");
    }
}
