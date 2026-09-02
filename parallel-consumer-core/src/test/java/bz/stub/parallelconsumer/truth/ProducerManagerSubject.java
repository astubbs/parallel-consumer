package bz.stub.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.FailureMetadata;
import bz.stub.parallelconsumer.internal.ProducerManager;
import bz.stub.parallelconsumer.internal.ProducerManagerChildSubject;
import bz.stub.parallelconsumer.internal.ProducerManagerParentSubject;
import bz.stub.parallelconsumer.internal.ProducerWrapper;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedMiddleSubject;
import io.stubbs.truth.generator.UserManagedSubject;

/**
 * Main Subject for the class under test.
 *
 * @author Antony Stubbs
 * @see ProducerManager
 * @see ProducerManagerParentSubject
 * @see ProducerManagerChildSubject
 */
@UserManagedSubject(ProducerManager.class)
public class ProducerManagerSubject extends ProducerManagerParentSubject implements UserManagedMiddleSubject {

    protected ProducerManagerSubject(FailureMetadata failureMetadata, ProducerManager actual) {
        super(failureMetadata, actual);
    }

    /**
     * Returns an assertion builder for a {@link ProducerManager} class.
     */
    @SubjectFactoryMethod
    public static Factory<ProducerManagerSubject, ProducerManager> producerManagers() {
        return ProducerManagerSubject::new;
    }

    public void transactionNotOpen() {
        check("isTransactionOpen()").that(actual.getProducerWrapper().isTransactionOpen()).isFalse();
    }

    public void transactionOpen() {
        check("isTransactionOpen()").that(actual.getProducerWrapper().isTransactionOpen()).isTrue();
    }

    public void stateIs(ProducerWrapper.ProducerState targetState) {
        var producerWrap = actual.getProducerWrapper();
        var producerState = producerWrap.getProducerState();
        check("getProducerState()").that(producerState).isEqualTo(targetState);
    }

    /**
     * How many produce locks are held right now, across all threads.
     * <p>
     * The produce lock is the read side of {@code producerTransactionLock}, so this is the number of workers
     * currently inside a produce section - which is exactly what the commit lock has to wait to reach zero. Named
     * as an assertion so tests can state the invariant instead of reaching into
     * {@link java.util.concurrent.locks.ReentrantReadWriteLock#getReadLockCount()} themselves.
     *
     * @param expected the number of produce-lock holders expected
     */
    public void hasProduceLockHoldCount(int expected) {
        check("getProducerTransactionLock().getReadLockCount()")
                .that(actual.getProducerTransactionLock().getReadLockCount())
                .isEqualTo(expected);
    }

    /**
     * No worker is inside a produce section - the precondition for a transaction commit to be allowed to gather
     * its offsets.
     */
    public void hasNoProduceLockHolders() {
        hasProduceLockHoldCount(0);
    }

    /**
     * The commit lock is held by nobody, so producing is free to proceed.
     */
    public void commitLockNotHeld() {
        check("getProducerTransactionLock().isWriteLocked()")
                .that(actual.getProducerTransactionLock().isWriteLocked())
                .isFalse();
    }

}
