package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PollContextInternal;

import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * Acquires the produce lock against the <em>real</em> context and hands it to that context, exactly as
 * {@link bz.stub.parallelconsumer.ParallelEoSStreamProcessor#pollAndProduce} does.
 * <p>
 * <b>This is load-bearing, not tidying.</b> The lock must not be released until the work has reached the
 * controller's inbound queue - see {@link AbstractParallelEoSStreamProcessor#cleanUpContext}, which states the
 * invariant and is the <em>only</em> sanctioned release point since astubbs#257 removed the per-record release that
 * used to run from {@code addToMailbox}. Releasing it inside the user function opens a window in which the
 * controller can take the commit lock, drain a mailbox that does not yet contain this work, and commit an offset one
 * behind - the ~1-in-6 flake written up in
 * {@code docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md} §11.
 * <p>
 * <b>Shared because three tests had written it out separately</b> - {@code ProducerManagerTest},
 * {@code TransactionalBulkCommitTest}, and the since-merged {@code ProduceLockReleaseTest}. A test that hand-rolls
 * this again is not just duplicating five lines; it is duplicating the reasoning above, which is the part that must
 * not drift - and that drift is what {@code docs/inflight/core-produce-lock-lifecycle-has-no-owner.md} records.
 *
 * @author Antony Stubbs
 */
final class ProduceLockHandover {

    private ProduceLockHandover() {
    }

    /**
     * @param producerManager the manager to acquire from
     * @param context         the context to hand the lock to - the same one the acquisition is made against, so the
     *                        lock's own logging identifies the work it belongs to
     */
    static void acquireInto(ProducerManager<String, String> producerManager,
                            PollContextInternal<String, String> context) {
        try {
            context.setProducingLock(Optional.of(producerManager.beginProducing(context)));
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
