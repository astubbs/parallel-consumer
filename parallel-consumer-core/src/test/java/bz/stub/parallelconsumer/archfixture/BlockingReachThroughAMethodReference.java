package bz.stub.parallelconsumer.archfixture;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Collection;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A defect on purpose: the exact shape that walked past
 * {@code ArchitectureTest.rebalanceCallbacksMustNotBlock} for as long as that rule followed only method CALLS.
 * <p>
 * This class is <b>not production code and must never be referenced by any</b>. It exists so
 * {@code RebalanceCallbackRuleControlTest} can point the real rule at a reach it is supposed to report, and
 * fail if it does not - the positive control for a rule whose whole job is to be green.
 * <p>
 * It mirrors {@code ShardManager.removeStaleContainers} as it stood before 2026-09-03: a method named like a
 * Kafka rebalance callback, a helper one hop away, and inside that helper a stream stage whose operation is a
 * METHOD REFERENCE to a blocking lock acquire. Nothing here is a method call to a name in the rule's deny
 * list, which is why the un-widened walk saw nothing.
 * <p>
 * <b>Do not "tidy" the method reference into a lambda.</b> A lambda body compiles to a synthetic method whose
 * accesses the old walk DID follow, so the same code written that way is reported either way and this fixture
 * stops controlling for anything.
 *
 * @author Antony Stubbs
 */
public class BlockingReachThroughAMethodReference {

    /** Fair, like {@code RetryQueue}'s, though nothing here depends on that - only on the acquire being a wait. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /**
     * Named so the rule's own {@code areRebalanceCallbacks()} predicate selects it. The signature does not
     * matter to the predicate - only the name and the owning package do.
     */
    public void onPartitionsRevoked(Collection<String> partitions) {
        sweep(partitions);
    }

    /**
     * One hop from the callback, so the control exercises the transitive walk and not just the root's own
     * accesses.
     */
    private void sweep(Collection<String> partitions) {
        partitions.stream()
                .map(ignoredPartition -> lock.writeLock())
                .forEach(ReentrantReadWriteLock.WriteLock::lock);
    }
}
