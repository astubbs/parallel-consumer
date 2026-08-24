package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the {@code volatile} modifier on {@link PartitionState}'s {@code dirty} flag - the shape of the fix, because
 * the behaviour cannot be asserted in a unit test: the anomaly it prevents is a memory-model visibility effect
 * measured at ~1.4e-7 per sample on hardware, far below anything a JUnit run can observe. The behavioural evidence
 * lives in the jcstress probe module (docs/plans/2026-08-25-002-test-jcstress-poc-plain-long-visibility.md): as a
 * plain field the commit path could observe {@code dirty} set while the offsets it publishes were stale; with the
 * flag volatile the anomalous outcome was 0 in 4.29e9 samples, declared FORBIDDEN.
 * <p>
 * A modifier is exactly the kind of thing an unrelated refactor (or a Lombok annotation change) drops silently -
 * nothing goes red, the field still compiles, and the fix is gone. This is the tripwire.
 *
 * @author Antony Stubbs
 */
class PartitionStateDirtyFlagFenceTest {

    @Test
    void theDirtyFlagIsVolatile() throws NoSuchFieldException {
        Field dirty = PartitionState.class.getDeclaredField("dirty");
        assertWithMessage("PartitionState.dirty must be volatile - it is written on the control thread and read by "
                + "the broker-poll commit path with no other happens-before edge. Removing the modifier reopens the "
                + "measured stale-read window (see the field's own javadoc for the jcstress figures); if this failed "
                + "because the field was renamed or the fencing redesigned, move the guard, do not delete it.")
                .that(Modifier.isVolatile(dirty.getModifiers()))
                .isTrue();
    }
}
