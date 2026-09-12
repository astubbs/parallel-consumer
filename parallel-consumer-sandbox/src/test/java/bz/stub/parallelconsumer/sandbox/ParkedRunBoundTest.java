package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import ch.qos.logback.classic.Level;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * A bounded run whose records park ends at its bound, rather than waiting out the wait's whole budget and then
 * refusing (astubbs#504).
 *
 * <h2>What this guards, and why the timing is the assertion</h2>
 * A parked record is a terminal outcome: it holds no worker, it is never retried, and its partition's committed
 * offset stays below it for good. The first version of the bound's wait therefore could not be satisfied by such a
 * run, and refused after twenty seconds - which the README's own quickstart hit on every build, taking about
 * thirty-six seconds for a ten-second run and logging an error while it did.
 * <p>
 * <b>One assertion carries this, and the other is a backstop - they are not independent, whatever the earlier
 * version of this paragraph said.</b> The wait that runs out refuses, the driver records that refusal
 * <em>and</em> logs it at error, and {@code awaitBound} rethrows a recorded failure - so the {@code awaitBound}
 * line below throws before the log assertion is ever reached. What that line pins is the timing:
 * {@link #WELL_INSIDE_THE_BUDGET} is comfortably under the budget-plus-close a run that ignored parked records
 * would need, and comfortably over what this one takes. The log assertion cannot fail while the rethrow stands;
 * it is kept because it is the half that would still have teeth if the rethrow were ever loosened, and because a
 * driver that logged an error without recording it is a shape nothing else here would catch.
 * <p>
 * <b>What this run still spends, and what it is not.</b> Most of the wall clock here is the drain-first close
 * afterwards, which sits out its drain timeout on parked work it can never take - the engine's behaviour, not the
 * wait's, and the reason the deadline is not tighter still. The wait itself ends as soon as the last scan appears
 * in the parked view.
 *
 * <h2>Key ordering, and why the key pool is not cosmetic</h2>
 * A parked record holds its key under key ordering (R11), so a second record on the same key would queue behind it
 * and never be accounted for at all. A key each models a customer each, and is what makes every driven scan
 * reach its own park rather than the first one blocking the rest.
 */
@Timeout(120)
class ParkedRunBoundTest {

    private static final String ORDERS = "orders";

    private static final String SCANS = "parcel-scans";

    private static final long RECORDS = 20;

    /**
     * The deadline for the whole bound sequence - the wait <b>and</b> the close it starts.
     * <p>
     * A run that ignored parked records spends {@code SandboxConsumer}'s twenty-second budget before its close even
     * begins, so it cannot come in under this however fast the machine is; this one measured about thirteen
     * seconds, nearly all of it the close. Twenty-five leaves both margins wide rather than splitting the
     * difference, because the log assertion beside it is what actually pins the behaviour.
     */
    private static final Duration WELL_INSIDE_THE_BUDGET = Duration.ofSeconds(25);

    @Test
    void aBoundedRunWhoseRecordsParkEndsAtItsBoundRatherThanWaitingOutTheBudget() {
        ParallelConsumerDefinition definition = SandboxFixtures.succeedingStringRoute(
                SandboxFixtures.definition().withDefaultOrdering(KEY), ORDERS);
        definition.string(SCANS)
                .retryLimit(1)
                .retryDelay(Duration.ofMillis(50))
                .process(context -> {
                    throw new IllegalStateException("the parcel-tracking service is not reachable");
                });

        Sandbox sandbox = Sandbox.builder()
                .perSecond(100)
                .bound(Bound.afterRecords(RECORDS))
                .keyCardinality((int) RECORDS)
                .feeding(ORDERS, SandboxFixtures.countedValues(ORDERS))
                .feeding(SCANS, SandboxFixtures.countedValues(SCANS))
                .build();

        ParallelConsumerInstance instance;
        try (LogCapture driverLog = LogCapture.of(RecordDriver.class, Level.WARN)) {
            instance = definition.start(sandbox);
            assertWithMessage("the bound has to finish inside %s: every scan parks, so the only thing that could "
                    + "hold the wait open is a wait that does not count a parked record as accounted for",
                    WELL_INSIDE_THE_BUDGET)
                    .that(sandbox.awaitBound(WELL_INSIDE_THE_BUDGET)).isTrue();
            instance.awaitShutdown();

            assertWithMessage("the bound's wait refusing is logged as an error by the driver, so a run that "
                    + "ends cleanly logs none: %s", driverLog.messagesAt(Level.ERROR))
                    .that(driverLog.messagesAt(Level.ERROR)).isEmpty();
        }

        assertWithMessage("the scans route's downstream always throws, so its records are what the run left "
                + "parked - and they are what the wait counted")
                .that(instance.parkedAllTopics().count()).isAtLeast(1);
        assertWithMessage("the succeeding route still commits, so the run is the mixed case: one partition "
                + "accounted for by its commit, the other by its parked records")
                .that(SandboxFixtures.highestCommittedOffset(sandbox, new TopicPartition(ORDERS, 0)))
                .isGreaterThan(0L);
        assertThat(sandbox.drivenRecords()).isEqualTo(RECORDS);

        instance.close();
    }
}
