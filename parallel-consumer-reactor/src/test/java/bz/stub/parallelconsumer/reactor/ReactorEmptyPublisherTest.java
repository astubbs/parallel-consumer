package bz.stub.parallelconsumer.reactor;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.truth.LongPollingMockConsumerSubject.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * A record must be completed when its publisher <em>terminates</em>, not when it happens to emit something.
 * <p>
 * {@link ReactorProcessor#react(Function)} accepts any {@link Publisher}, and plenty of perfectly ordinary user
 * functions never emit an item: {@link Mono#empty()}, a {@code Mono<Void>} from {@link Mono#fromRunnable(Runnable)},
 * a {@link Flux} that filters everything away. If completion is only wired to the <em>onNext</em> signal, every one
 * of those records is left in flight forever - and because in-flight work is capped at
 * {@link ParallelConsumerOptions#getMaxConcurrency()}, the consumer silently stops selecting new work once the cap
 * fills. No exception, no failed record, nothing in the log: it just stops.
 * <p>
 * That is why these tests assert on <b>two</b> numbers and not just the committed offset. The commit tells you
 * nothing was completed; the user-function invocation count is what tells you the cap has <em>bound</em>, and it is
 * the whole difference between "slow" and "stalled". With {@link #MAX_CONCURRENCY} of {@value #MAX_CONCURRENCY} and
 * {@link #RECORD_COUNT} of {@value #RECORD_COUNT} records, a stall shows up as exactly
 * {@value #MAX_CONCURRENCY} invocations that never come back, with the last record never dispatched at all.
 * <p>
 * The counterexample that makes this Reactor's problem and not the shared {@code ExternalEngine}'s is the Mutiny
 * engine, which subscribes with an explicit completion callback and maps an absent item to an empty stream - so it
 * already treats "produced no value" as success. The two engines are offered to users as interchangeable, so they
 * must not disagree about that.
 */
@Slf4j
class ReactorEmptyPublisherTest extends ReactorUnitTestBase {

    /**
     * Deliberately tiny, and smaller than {@link #RECORD_COUNT}: a leaked in-flight slot is only observable once the
     * cap binds. With the default cap of 1000 every record would still be dispatched and the stall would look like
     * nothing more than a missing commit.
     */
    private static final int MAX_CONCURRENCY = 4;

    /**
     * One more record than the in-flight cap, so a single leaked slot per record is enough to strand the last one.
     */
    private static final int RECORD_COUNT = 5;

    private final AtomicInteger userFunctionInvocations = new AtomicInteger();

    @Override
    protected int getMaxConcurrency() {
        return MAX_CONCURRENCY;
    }

    @Override
    protected ParallelConsumerOptions.CommitMode getCommitMode() {
        return PERIODIC_CONSUMER_ASYNCHRONOUS;
    }

    @BeforeEach
    void seedRecords() {
        primeFirstRecord(); // offset 0
        ktu.send(consumerSpy, ktu.generateRecords(RECORD_COUNT - 1)); // offsets 1..RECORD_COUNT-1
    }

    @Test
    void anEmptyMonoCompletesItsRecord() {
        reactorPC.react(pollContext -> {
            userFunctionInvocations.incrementAndGet();
            return Mono.empty();
        });

        assertEveryRecordWasProcessedAndCommitted();
    }

    /**
     * The shape a user reaches for when the work has no result to hand back - {@code Mono<Void>}, which by
     * construction can only ever terminate empty.
     */
    @Test
    void aMonoVoidFromARunnableCompletesItsRecord() {
        reactorPC.react(pollContext -> Mono.fromRunnable(() -> {
            log.debug("Side-effect-only user function for {}", pollContext);
            userFunctionInvocations.incrementAndGet();
        }));

        assertEveryRecordWasProcessedAndCommitted();
    }

    /**
     * The multi-record equivalent: a {@link Flux} that terminates without ever emitting, e.g. because a filter
     * removed everything.
     */
    @Test
    void anEmptyFluxCompletesItsRecord() {
        reactorPC.react(pollContext -> {
            userFunctionInvocations.incrementAndGet();
            return Flux.just(1, 2, 3).filter(value -> false);
        });

        assertEveryRecordWasProcessedAndCommitted();
    }

    /**
     * Control arm. A publisher that emits exactly one item is the case that already worked, and must keep working -
     * without it, a "fix" that broke completion outright would look identical to a fix that worked.
     */
    @Test
    void aSingleItemPublisherStillCompletesItsRecord() {
        reactorPC.react(pollContext -> {
            userFunctionInvocations.incrementAndGet();
            return Mono.just("result: " + pollContext.offset());
        });

        assertEveryRecordWasProcessedAndCommitted();
    }

    /**
     * A publisher emitting several items must complete its record exactly ONCE, at the terminal signal - so this
     * asserts the in-flight counter as well as the commit.
     * <p>
     * Completing per-item hands the same {@code WorkContainer} to the controller once per item, and each pass
     * decrements {@code WorkManager#numberRecordsOutForProcessing}. The counter is what the engine throttles
     * against, so it drifting below zero means the cap silently stops capping. The commit alone cannot see this:
     * over-completion still commits every offset, which is exactly why it went unnoticed.
     */
    @Test
    void aMultiItemPublisherCompletesItsRecordExactlyOnce() {
        reactorPC.react(pollContext -> {
            userFunctionInvocations.incrementAndGet();
            return Flux.just(1, 2, 3);
        });

        assertEveryRecordWasProcessedAndCommitted();

        assertWithMessage("Records still counted as in-flight once every record has been committed. One completion "
                + "per record leaves this at zero; completing per emitted item decrements it once per item, so it "
                + "drifts negative and the in-flight cap stops binding")
                .that(reactorPC.getWm().getNumberRecordsOutForProcessing())
                .isEqualTo(0);
    }

    /**
     * The error path must be untouched: a publisher that fails still routes to the failure hook, so the record is
     * retried rather than completed. Each record fails once and then succeeds, which both proves the failure was
     * seen (a completed-on-error record would never be retried) and lets the test terminate.
     * <p>
     * Unlike its siblings this is a control arm, green before the fix as well as after - the retry deliberately
     * emits an item rather than completing empty, so the only thing it can be measuring is the error signal. Wiring
     * completion to a terminal signal must not start swallowing failures: Reactor guarantees onError and onComplete
     * are mutually exclusive, and this is what holds that guarantee to account.
     */
    @Test
    void anErroringPublisherFailsItsRecordAndIsRetried() {
        Map<Long, AtomicInteger> attemptsByOffset = new ConcurrentHashMap<>();

        reactorPC.react(pollContext -> {
            userFunctionInvocations.incrementAndGet();
            int attempt = attemptsByOffset
                    .computeIfAbsent(pollContext.offset(), offset -> new AtomicInteger())
                    .incrementAndGet();
            if (attempt == 1) {
                return Mono.error(new PCRetriableException("Deliberate first-attempt failure for offset " + pollContext.offset()));
            }
            return Mono.just("retry succeeded for offset " + pollContext.offset());
        });

        await().atMost(defaultTimeout).untilAsserted(() -> {
            assertWithMessage("Offsets seen by the user function")
                    .that(attemptsByOffset.keySet())
                    .hasSize(RECORD_COUNT);

            assertWithMessage("Every record must have been attempted more than once - a record whose publisher "
                    + "errored must go back to the retry queue, not be marked successful")
                    .that(attemptsByOffset.values().stream().allMatch(attempts -> attempts.get() > 1))
                    .isTrue();

            assertThat(consumerSpy).hasCommittedToPartition(topicPartition).offset(RECORD_COUNT);
        });
    }

    /**
     * Both halves of the stall signature.
     * <p>
     * The invocation count is checked FIRST because it is the one that distinguishes a stall from a slow run: when
     * completion never fires, the in-flight cap fills and the last record is never dispatched, so the count pins at
     * {@link #MAX_CONCURRENCY} and stays there for as long as you are willing to wait.
     */
    private void assertEveryRecordWasProcessedAndCommitted() {
        await().atMost(defaultTimeout).untilAsserted(() -> {
            assertWithMessage("User function invocations - pinning at the in-flight cap (%s) instead of reaching %s "
                    + "means completed records are never being released, so no further work is ever selected",
                    MAX_CONCURRENCY, RECORD_COUNT)
                    .that(userFunctionInvocations.get())
                    .isEqualTo(RECORD_COUNT);

            assertThat(consumerSpy).hasCommittedToPartition(topicPartition).offset(RECORD_COUNT);
        });
    }

}
