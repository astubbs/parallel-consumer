package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * {@code AbstractParallelEoSStreamProcessor.lastCommitTime} is volatile, and this is the tripwire that keeps it
 * so - the same shape and the same reasoning as {@code PartitionStateDirtyFlagFenceTest}, which pins
 * {@code PartitionState.dirty}.
 * <p>
 * <b>Why a modifier needs a test at all.</b> It is exactly the kind of thing an unrelated refactor drops
 * silently: nothing goes red, the field still compiles, and the edge is gone. Here the loss is quieter still,
 * because the effect of the race is a redundant commit rather than a wrong one, so no functional test can see
 * it either.
 * <p>
 * <b>Why the field is shared at all</b>, which is the part that is easy to get wrong: the obvious reading is
 * that {@code commitOffsetsThatAreReady()} writes it and {@code isTimeToCommitNow()} reads it, both on the
 * control thread, and every record of the field said so. {@code tryCommitOffsetsOnRevoke()} writes it
 * too, from inside {@code onPartitionsRevoked}, which the broker POLL thread runs. That is why the field is not
 * {@code @ThreadConfined} - declaring it confined would have been a false declaration RacerD would then have
 * believed. It is also why a runtime confinement guard is not the answer here, the way
 * {@code RetryQueue.RetryQueueIterator.assertOnOwningThread} is for a confined object: there is no single
 * thread to assert against, only an edge to establish.
 *
 * @author Antony Stubbs
 */
class LastCommitTimeFenceTest {

    @Test
    void theLastCommitTimeIsVolatile() throws NoSuchFieldException {
        Field lastCommitTime = AbstractParallelEoSStreamProcessor.class.getDeclaredField("lastCommitTime");
        assertWithMessage("AbstractParallelEoSStreamProcessor.lastCommitTime must be volatile - it is written on "
                + "the control thread by commitOffsetsThatAreReady() AND on the broker-poll thread by "
                + "tryCommitOffsetsOnRevoke(), and read by isTimeToCommitNow() outside commitLock, so the "
                + "modifier is the only happens-before edge. Removing it lets the control thread miss a commit "
                + "that happened and commit again immediately. If this failed because the field was renamed or "
                + "the revoke-path commit moved off the poll thread, move the guard, do not delete it.")
                .that(Modifier.isVolatile(lastCommitTime.getModifiers()))
                .isTrue();
    }
}
