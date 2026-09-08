package bz.stub.parallelconsumer.archfixture;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.state.ControllerThreadOnly;

import java.util.Collection;

/**
 * A defect on purpose, the sibling of {@link BlockingReachThroughAMethodReference}: a rebalance callback
 * reaching a method the codebase itself declares {@link ControllerThreadOnly}, once by CALL and once by METHOD
 * REFERENCE.
 * <p>
 * This class is <b>not production code and must never be referenced by any</b>. It exists so
 * {@code RebalanceCallbackRuleControlTest} can point the real rule at a reach it is supposed to report, and
 * fail if it does not.
 * <p>
 * <b>Nothing here waits, and that is the point.</b> The annotated method's body is a field write - it contains
 * no call in {@code ArchitectureTest.BLOCKING_CALLS} and no monitor, so the JDK deny-list has nothing to match
 * and the only thing that can report this reach is the annotation. That is exactly the shape the annotation was
 * added for: {@code RetryQueue.remove} is reported through {@code WriteLock.lock()} either way, whereas a
 * declared contract has to hold for a method whose wait the walk cannot name.
 * <p>
 * <b>Do not "tidy" the method reference into a lambda</b>, for the reason
 * {@link BlockingReachThroughAMethodReference} gives: a lambda body compiles to a synthetic method the walk
 * follows as an ordinary call, and this half of the control then controls for nothing.
 *
 * @author Antony Stubbs
 */
public class ControllerThreadOnlyReachFromARebalanceCallback {

    private String lastSwept = "";

    /** Read only so the field is not a write-only one, which the analysers would rightly flag. */
    public String getLastSwept() {
        return lastSwept;
    }

    /**
     * Named so the rule's own {@code areRebalanceCallbacks()} predicate selects it. Deliberately a DIFFERENT
     * callback name from {@link BlockingReachThroughAMethodReference}'s, so a report naming this class can only
     * have come from this fixture.
     */
    public void onPartitionsAssigned(Collection<String> partitions) {
        sweep(partitions);
    }

    /**
     * One hop from the callback, so the control exercises the transitive walk rather than only the root's own
     * accesses - and it reaches the same annotated method both ways, so the two reported reaches differ only in
     * the kind of access that produced them.
     */
    private void sweep(Collection<String> partitions) {
        retireOnTheControllerThread("the direct call");
        partitions.forEach(this::retireOnTheControllerThread);
    }

    /**
     * Declares the contract without implementing a wait - see the class javadoc for why that is deliberate.
     */
    @ControllerThreadOnly
    private void retireOnTheControllerThread(String partition) {
        this.lastSwept = partition;
    }
}
