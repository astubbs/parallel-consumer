package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A record that cannot be returned to the mailbox terminates PC.
 * <p>
 * Operator ruling on astubbs#267: this is PC's own bookkeeping, so a failure here means PC can no longer account for
 * the record - it is neither in flight nor completed. Continuing risks committing past work that was never done, and
 * a silent skip is not a state PC may keep running in.
 * <p>
 * <b>What is asserted here is the contract the three call sites depend on</b>, because it is the part with behaviour
 * rather than delegation: the reason is recorded, the state moves so the control thread shuts down, the escalation
 * never throws, and the FIRST cause is the one kept. The alternative - driving a throwing {@code addToMailbox} through
 * a running engine - proves the same contract through three engines' worth of timing, and would assert less about the
 * ordering while being far easier to make flaky.
 */
@Slf4j
class UnmailboxableRecordIsFatalTest {

    /**
     * Not a mock: the escalation reads {@code wc} only to render it into the failure message, and a mock whose
     * {@code toString} is exercised on a failure path is a second thing that can throw there.
     */
    private static final WorkContainer<String, String> NO_CONTAINER = null;

    private ParallelEoSStreamProcessor<String, String> pc() {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        return new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .build());
    }

    @Test
    void recordsTheCauseAndMovesTheStateSoTheControlThreadShutsDown() {
        var pc = pc();

        var mailboxingThrew = new IllegalStateException("mailbox rejected the record");
        pc.failFatallyOnUnmailboxableRecord(NO_CONTAINER, mailboxingThrew);

        assertThat(pc.getFailureCause()).isNotNull();
        assertThat(pc.getFailureCause().getCause()).isSameInstanceAs(mailboxingThrew);
        // The state write is what publishes the reason to the control thread, so it is asserted rather than assumed.
        //
        // CLOSED rather than CLOSING, and that is the code being right rather than the assertion being loose:
        // transitionToClosing collapses UNUSED straight to CLOSED, and this fixture's PC was never started. On a
        // RUNNING instance the same call lands on CLOSING and the control thread performs the shutdown. Both are
        // "no longer accepting work", which is the property; the pair below says so without pinning either.
        assertThat(pc.getState()).isAnyOf(State.CLOSING, State.CLOSED);
        assertThat(pc.getState()).isNotEqualTo(State.UNUSED);
    }

    /**
     * The escalation runs inside loops whose remaining containers still have to be returned, and inside vert.x's
     * listener array, which contains nothing for us. An escape would strand exactly the records the shutdown is being
     * raised to protect - so "never throws" is the property, not an implementation detail.
     */
    @Test
    void neverThrows() {
        var pc = pc();

        // A throwable whose own rendering fails: the escalation logs the cause, and logging a throwable runs its
        // author's code. This is the shape that turned a report into a second failure elsewhere in this PR.
        var hostile = new RuntimeException("rendering me fails") {
            @Override
            public synchronized Throwable getCause() {
                throw new UnsupportedOperationException("rendering me fails");
            }
        };

        assertThatCode(() -> pc.failFatallyOnUnmailboxableRecord(NO_CONTAINER, hostile))
                .doesNotThrowAnyException();
    }

    /**
     * A batch can fail to mailbox more than one record, and the later ones are consequences of the first. Keeping the
     * first is what leaves a diagnosis rather than a symptom.
     */
    @Test
    void theFirstCauseIsKept() {
        var pc = pc();

        var first = new IllegalStateException("the one that started it");
        var second = new IllegalStateException("a consequence of the first");

        pc.failFatallyOnUnmailboxableRecord(NO_CONTAINER, first);
        pc.failFatallyOnUnmailboxableRecord(NO_CONTAINER, second);

        assertThat(pc.getFailureCause().getCause()).isSameInstanceAs(first);
    }
}
