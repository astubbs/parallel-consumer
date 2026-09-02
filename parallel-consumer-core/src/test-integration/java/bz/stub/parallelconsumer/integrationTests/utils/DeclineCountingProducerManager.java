package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.ConsumerManager;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.ProducerManager;
import bz.stub.parallelconsumer.internal.ProducerWrapper;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link ProducerManager} that counts the revocations which found the producer transaction lock held and
 * <b>declined</b> it - the astubbs#44 (confluentinc#803) fix path executing.
 * <p>
 * This is what makes a green revoke-under-commit run mean anything, and it is the lesson the confluentinc#857
 * revoke-path cluster decomposition plan paid for on the sibling defect: <i>"A clean fixed arm with a zero
 * skip-count would be indistinguishable from a probe that never opened the window, which is exactly how this fix
 * looked unproven for four months."</i> Before the fix the outcome variable carried the proof itself - a revoke that
 * waited 79s had self-evidently overlapped a commit. After the fix the callback returns in milliseconds precisely
 * <em>because</em> it declined, so "it was fast" no longer distinguishes a working fix from a window that never
 * opened. The count does.
 * <p>
 * Shared by {@code Revoke857TransactionalWaitProbeIT}, which additionally dwells inside the lock to force the window,
 * and {@code RebalanceEoSDeadlockTest}, which forces the same window from the processor side. Both read
 * {@link #revocationDeclines()} through {@link DeclineCountingModule#manager()}.
 */
@Slf4j
public class DeclineCountingProducerManager<K, V> extends ProducerManager<K, V> {

    private final AtomicLong revocationDeclines = new AtomicLong();

    public DeclineCountingProducerManager(ProducerWrapper<K, V> producerWrapper,
                                          ConsumerManager<K, V> consumerManager,
                                          WorkManager<K, V> workManager,
                                          ParallelConsumerOptions<K, V> options) {
        super(producerWrapper, consumerManager, workManager, options);
    }

    /** Revocations that found the producer transaction lock held and declined the commit. Per instance. */
    public long revocationDeclines() {
        return revocationDeclines.get();
    }

    @Override
    public boolean tryAcquireCommitLockForRevocation() {
        boolean acquired = super.tryAcquireCommitLockForRevocation();
        if (!acquired) {
            long declines = revocationDeclines.incrementAndGet();
            log.info("Revocation #{} DECLINED the producer transaction lock - the astubbs#44 fix path executed",
                    declines);
        }
        return acquired;
    }

    /**
     * Hands a {@link DeclineCountingProducerManager} to PC in place of the real one. The components are read here
     * rather than inside the manager because {@code PCModule}'s accessors are protected: reachable through
     * {@code this} in a subclass, not through another instance from a different package.
     */
    public static class DeclineCountingModule<K, V> extends PCModule<K, V> {

        private DeclineCountingProducerManager<K, V> manager;

        public DeclineCountingModule(ParallelConsumerOptions<K, V> options) {
            super(options);
        }

        @Override
        protected ProducerManager<K, V> producerManager() {
            if (manager == null) {
                manager = new DeclineCountingProducerManager<>(producerWrap(), consumerManager(), workManager(), options());
            }
            return manager;
        }

        /** Null until PC first asks for the producer manager, which it does during construction. */
        public DeclineCountingProducerManager<K, V> manager() {
            return manager;
        }
    }
}
