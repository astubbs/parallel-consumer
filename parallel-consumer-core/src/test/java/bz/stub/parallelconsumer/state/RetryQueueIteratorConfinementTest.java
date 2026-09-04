package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ForeignThread;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link RetryQueue.RetryQueueIterator} is confined to the thread that opened it, and this asserts that it says
 * so at runtime rather than only in the {@code @ThreadConfined} declaration RacerD reads.
 * <p>
 * <b>Why the confinement is real and not a convention.</b> {@link RetryQueue#iterator()} takes the queue's READ
 * LOCK on the calling thread and the iterator's {@code close()} releases it. A
 * {@link java.util.concurrent.locks.ReentrantReadWriteLock} read lock may only be released by the thread that
 * acquired it, so an iterator that escaped to another thread could not be closed there at all: the unlock would
 * throw {@link IllegalMonitorStateException} and strand the read lock, blocking every writer for the life of
 * the process. The confinement is therefore not a style choice - the lock discipline already requires it - and
 * what these tests buy is moving the failure from an opaque monitor error at close time to a named refusal at
 * the first foreign call.
 * <p>
 * <b>What it buys the infer lane.</b> Four {@code THREAD_SAFETY_VIOLATION} identities on the one {@code closed}
 * boolean sat in {@code config/infer-known-findings.txt}, reported because RacerD saw an unsynchronised
 * read/write pair on a class it had no reason to believe was confined. Declaring the confinement retires them;
 * this test is what makes the declaration a claim the build can fail on rather than a comment the analyser
 * happens to parse. See {@code docs/inflight/static-infer-threadsafe-is-blocked-by-third-party-interfaces.md}.
 *
 * @author Antony Stubbs
 */
class RetryQueueIteratorConfinementTest extends RetryQueueTestBase {

    private final ForeignThread foreign = new ForeignThread();

    @AfterEach
    void closeForeignThread() {
        foreign.close();
    }

    @Test
    void hasNextFromAnotherThreadIsRefusedByName() {
        retryQueue.add(workFor(0));

        try (RetryQueue.RetryQueueIterator iterator = retryQueue.iterator()) {
            // named rather than discarded, per the repo's return-value rule and Error Prone's ReturnValueIgnored
            Throwable thrown = foreign.catching(() -> {
                boolean ignoredHasNext = iterator.hasNext(); // never reached - the guard throws first
            });

            assertThat(thrown).isInstanceOf(IllegalStateException.class);
            assertThat(thrown).hasMessageThat().contains("confined");
            assertThat(thrown).hasMessageThat().contains(ForeignThread.DEFAULT_NAME);
        }
    }

    @Test
    void nextFromAnotherThreadIsRefusedByName() {
        retryQueue.add(workFor(0));

        try (RetryQueue.RetryQueueIterator iterator = retryQueue.iterator()) {
            Throwable thrown = foreign.catching(() -> {
                WorkContainer<?, ?> ignoredNext = iterator.next(); // never reached - the guard throws first
            });

            assertThat(thrown).isInstanceOf(IllegalStateException.class);
            assertThat(thrown).hasMessageThat().contains("confined");
        }
    }

    /**
     * The decisive one. A foreign close has to be refused BEFORE it reaches the unlock, so the read lock stays
     * with its owner and the owner's own close still releases it. The {@code clear()} afterwards is the positive
     * half: it needs the WRITE lock, and it runs on the foreign thread because a write lock taken from this
     * thread would be refused for a different reason - {@code ReentrantReadWriteLock} does not upgrade - and a
     * still-held read lock would make it hang rather than fail, which {@link ForeignThread#catching} reports as
     * a timeout naming the fixture.
     */
    @Test
    void closeFromAnotherThreadIsRefusedAndLeavesTheLockWithItsOwner() {
        retryQueue.add(workFor(0));

        Throwable thrown;
        try (RetryQueue.RetryQueueIterator iterator = retryQueue.iterator()) {
            thrown = foreign.catching(iterator::close);
        }

        assertThat(thrown).isInstanceOf(IllegalStateException.class);
        assertThat(thrown).hasMessageThat().contains("confined");
        assertThat(foreign.catching(retryQueue::clear)).isNull();
        assertThat(retryQueue.size()).isEqualTo(0);
    }

    @Test
    void theOwningThreadIteratesAndClosesUnaffected() {
        retryQueue.add(workFor(0));
        retryQueue.add(workFor(1));

        int seen = 0;
        try (RetryQueue.RetryQueueIterator iterator = retryQueue.iterator()) {
            while (iterator.hasNext()) {
                assertThat(iterator.next()).isNotNull();
                seen++;
            }
        }

        assertThat(seen).isEqualTo(2);
    }

    /**
     * The pre-existing closed-iterator contract, kept green: reuse after close is still refused, and still names
     * closure rather than confinement - the two guards are separate claims and neither may swallow the other.
     */
    @Test
    void useAfterCloseIsStillRefusedAsClosed() {
        retryQueue.add(workFor(0));

        RetryQueue.RetryQueueIterator iterator = retryQueue.iterator();
        iterator.close();

        IllegalStateException thrown = assertThrows(IllegalStateException.class, iterator::hasNext);
        assertThat(thrown).hasMessageThat().contains("closed");
    }
}
