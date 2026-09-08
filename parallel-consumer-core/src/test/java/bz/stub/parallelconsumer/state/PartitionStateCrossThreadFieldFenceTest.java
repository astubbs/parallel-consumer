package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the shape of the fences on {@link PartitionState}'s cross-thread fields, because the behaviour they
 * protect cannot be asserted in a unit test: these are memory-model visibility effects measured between
 * 1e-7 and 1e-5 per raced pair on hardware, far below anything a JUnit run can observe. A modifier is
 * exactly the kind of thing an unrelated refactor - or a Lombok annotation change - drops silently: nothing
 * goes red, the field still compiles, and the fix is gone. This is the tripwire.
 * <p>
 * The behavioural evidence lives in the re-runnable {@code jcstress-poc/} module, and what a probe's zero is
 * worth is docs/solutions/best-practices/a-stress-probe-is-an-instrument-you-built-not-a-test.md. Each
 * field's own javadoc carries its figures and its arm name.
 * <p>
 * <b>This was {@code PartitionStateDirtyFlagFenceTest}</b>, pinning {@code volatile} on a {@code dirty}
 * flag that astubbs/parallel-consumer#469 replaced with the counter pair below. Its own failure message
 * asked for the guard to be moved rather than deleted when the fencing was redesigned; this is that move.
 * The lost-update half of that redesign is behaviour rather than a modifier, and is pinned deterministically
 * by {@link PartitionStateCommitWindowSeamTest}.
 *
 * @author Antony Stubbs
 * @see PartitionStateCommitWindowSeamTest the behavioural half of the same protocol
 */
class PartitionStateCrossThreadFieldFenceTest {

    @Test
    void theBackPressureFlagIsVolatile() throws NoSuchFieldException {
        Field allowedMoreRecords = PartitionState.class.getDeclaredField("allowedMoreRecords");
        assertWithMessage("PartitionState.allowedMoreRecords must be volatile - it is written on the broker-poll "
                + "thread inside the commit path's encode and read on the control thread when work is taken, with "
                + "no other happens-before edge, and the commit-count fence does not cover that direction. Removing "
                + "the modifier reopens the measured stale-read window (see the field's own javadoc for the "
                + "jcstress figures); if this failed because the field was renamed or the fencing redesigned, move "
                + "the guard, do not delete it.")
                .that(Modifier.isVolatile(allowedMoreRecords.getModifiers()))
                .isTrue();
    }

    @Test
    void theCompletionCountIsAFinalAtomic() throws NoSuchFieldException {
        Field completionCount = PartitionState.class.getDeclaredField("completionCount");
        assertWithMessage("PartitionState.completionCount must stay an AtomicLong - its incrementAndGet is the "
                + "release that publishes the offsets a completion just wrote, and the acquire the commit path "
                + "pairs with. A plain long silently reopens exactly the window astubbs/parallel-consumer#349 "
                + "measured and fenced.")
                .that(completionCount.getType())
                .isEqualTo(AtomicLong.class);
        assertWithMessage("PartitionState.completionCount must stay final - the protocol's whole guarantee is that "
                + "the count only ever moves forward, and replacing the reference discards it")
                .that(Modifier.isFinal(completionCount.getModifiers()))
                .isTrue();
    }

    @Test
    void theCommittedCompletionCountIsVolatile() throws NoSuchFieldException {
        Field committed = PartitionState.class.getDeclaredField("completionCountCommitted");
        assertWithMessage("PartitionState.completionCountCommitted must be volatile - the committer thread "
                + "publishes it and the control thread reads it on the commit gate. Without the modifier the "
                + "control thread can keep seeing a partition as dirty (an extra commit cycle) or, once the "
                + "counters are compared the other way round, as clean.")
                .that(Modifier.isVolatile(committed.getModifiers()))
                .isTrue();
    }
}
