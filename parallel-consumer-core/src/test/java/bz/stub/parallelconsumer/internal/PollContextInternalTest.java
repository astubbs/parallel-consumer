package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.ModelUtils;
import com.google.common.truth.Truth;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Optional;

import static bz.stub.parallelconsumer.ManagedTruth.assertWithMessage;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Exactly one produce lock is owed per {@link PollContextInternal}, and
 * {@link PollContextInternal#setProducingLock} is where that is enforced rather than merely intended.
 * <p>
 * <b>What makes this worth a test is the failure mode of the guard itself.</b> Acquisition happens before the
 * hand-over - each call site in {@code ParallelEoSStreamProcessor#processAndProduceResults} calls
 * {@code beginProducing} and passes the result straight to the setter - so the second read hold is already taken by
 * the time the setter can refuse it. Nothing releases that hold on the throw path, and {@code cleanUpContext}
 * releases only the lock the context kept. A guard that merely throws therefore keeps the first hold and orphans the
 * second: the read count stays at 2 and the next commit's write-lock acquisition blocks forever - the guard causing
 * the exact hang it advertises preventing, while correctly reporting the misuse. Found by Codex review on
 * astubbs#262.
 * <p>
 * The assertion carrying that is the hold COUNT after the rejection, not the exception. Delete the release, keep the
 * throw, and every other assertion here stays green.
 *
 * @author Antony Stubbs
 */
@Slf4j
@Tag("transactions")
@Timeout(30)
class PollContextInternalTest {

    private PCModuleTestEnv module;
    private ProducerManager<String, String> producerManager;
    private ModelUtils mu;

    @BeforeEach
    void setup() {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build());
        mu = new ModelUtils(module);
        producerManager = module.producerManager();
    }

    private PollContextInternal<String, String> aContext() {
        return new PollContextInternal<>(UniLists.of(mu.createWorkFor(0)));
    }

    @SneakyThrows
    @Test
    void refusingASecondProduceLockReleasesTheOneItRefused() {
        var context = aContext();

        var first = producerManager.beginProducing(context);
        context.setProducingLock(Optional.of(first));
        assertWithMessage("one acquisition, one hold")
                .that(producerManager)
                .hasProduceLockHoldCount(1);

        // the shape both call sites have - acquire, THEN hand over - so the refusal happens with the hold taken
        var second = producerManager.beginProducing(context);
        assertWithMessage("two acquisitions before the refusal, so the hold the guard must release is real")
                .that(producerManager)
                .hasProduceLockHoldCount(2);

        var thrown = assertThrows(PCInternalRuntimeException.class,
                () -> context.setProducingLock(Optional.of(second)));
        Truth.assertWithMessage("the refusal must still be attributable, not silent")
                .that(thrown.getMessage())
                .contains("Produce lock already held");

        assertWithMessage("THE POINT: the refused hold must be released rather than orphaned - a count of 2 here is "
                        + "the permanent block on the next commit that this guard exists to prevent")
                .that(producerManager)
                .hasProduceLockHoldCount(1);

        Truth.assertWithMessage("the FIRST lock is the one the context keeps - identity, not merely presence: a "
                        + "guard that released the refused hold and then stored it anyway would leave the context "
                        + "pointing at an already-released lock, and isPresent() cannot tell the two apart")
                .that(context.getProducingLock().get())
                .isSameInstanceAs(first);

        producerManager.finishProducing(context.takeProducingLock().get());
        assertWithMessage("and releasing the one the context owned leaves nobody holding it")
                .that(producerManager)
                .hasNoProduceLockHolders();
    }

    /**
     * The hazard the FIRST fix introduced, and the reason the release is conditional on identity.
     * <p>
     * {@link ProducerManager.ProducingLock} is not a token - every instance wraps the one
     * {@code ReadLock} of {@code producerTransactionLock}, so {@code unlock()} drops one of the calling thread's
     * holds whichever instance it is called on. Handed the SAME instance back there is only one acquisition, and an
     * unconditional release would take the count to zero while the worker is still producing, freeing the commit
     * thread's write lock to gather offsets mid-send. The refusal must still happen; the release must not.
     */
    @SneakyThrows
    @Test
    void refusingTheSameLockInstanceRefusesWithoutReleasingIt() {
        var context = aContext();

        var only = producerManager.beginProducing(context);
        context.setProducingLock(Optional.of(only));

        assertThrows(PCInternalRuntimeException.class, () -> context.setProducingLock(Optional.of(only)));

        assertWithMessage("one acquisition means one hold, and re-handing the SAME lock is not a second one - "
                        + "releasing here would drop the count to zero while the worker is still inside its "
                        + "produce section, and the commit thread could then gather offsets mid-send")
                .that(producerManager)
                .hasProduceLockHoldCount(1);

        producerManager.finishProducing(context.takeProducingLock().get());
        assertWithMessage("still exactly one release owed, and it worked")
                .that(producerManager)
                .hasNoProduceLockHolders();
    }

    /**
     * Clearing by assignment is refused too. Testing the INCOMING optional as well as the held one would let
     * {@code setProducingLock(empty())} fall through and overwrite a live lock with nothing - dropping the only
     * reference to a read hold, silently, which is the failure this guard exists to make loud.
     * {@code takeProducingLock()} is the sanctioned way to clear.
     */
    @SneakyThrows
    @Test
    void anEmptyAssignmentCannotSilentlyDropAHeldLock() {
        var context = aContext();

        var held = producerManager.beginProducing(context);
        context.setProducingLock(Optional.of(held));

        assertThrows(PCInternalRuntimeException.class, () -> context.setProducingLock(Optional.empty()));

        Truth.assertWithMessage("the lock is still owned by the context, so it can still be released")
                .that(context.getProducingLock().get())
                .isSameInstanceAs(held);
        assertWithMessage("and nothing was orphaned")
                .that(producerManager)
                .hasProduceLockHoldCount(1);

        producerManager.finishProducing(context.takeProducingLock().get());
        assertWithMessage("released")
                .that(producerManager)
                .hasNoProduceLockHolders();
    }

    @SneakyThrows
    @Test
    void theOrdinaryHandOverTakesExactlyOneHoldAndGivesItBack() {
        var context = aContext();

        var lock = producerManager.beginProducing(context);
        context.setProducingLock(Optional.of(lock));

        Truth.assertWithMessage("the context owns the lock it was handed")
                .that(context.getProducingLock().isPresent())
                .isTrue();
        assertWithMessage("the ordinary path still takes exactly one hold")
                .that(producerManager)
                .hasProduceLockHoldCount(1);

        producerManager.finishProducing(context.takeProducingLock().get());
        assertWithMessage("released")
                .that(producerManager)
                .hasNoProduceLockHolders();
        Truth.assertWithMessage("a taken lock leaves the context empty, so a second release is a no-op")
                .that(context.getProducingLock().isPresent())
                .isFalse();
    }
}
