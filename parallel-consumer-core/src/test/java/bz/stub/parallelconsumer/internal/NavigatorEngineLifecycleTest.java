package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.navigator.StubResourceAllocator;
import bz.stub.parallelconsumer.internal.navigator.ResourceContract;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The navigator wired through a REAL control loop (the plan's U3, AE5's honest half): a resource-deferred
 * record's user function is GENUINELY NOT INVOKED - the engine runs freely for hundreds of passes and
 * dispatches nothing - until the virtual clock passes the record's {@code availableAt}, at which point the
 * loop's own per-pass quantum read (KTD4) mints the credit and the record runs. Then close-entry drops the
 * membership share at the next quantum without waiting for the lease TTL (R16, AE2's engine half).
 * <p>
 * Wiring: a real {@link ParallelEoSStreamProcessor} over a {@link PCModuleTestEnv} whose {@link MutableClock}
 * is SHARED with the {@link StubResourceAllocator} (KTD4's one canonical clock), fed by a hand-rebalanced
 * {@link MockConsumer} - the {@code MockConsumerTestBase} dance, reproduced here because that base cannot
 * inject a module. The control loop itself runs on wall time (its passes are real); only the CREDIT clock is
 * virtual, which is exactly the split that makes "it had every chance to dispatch and did not" a strong
 * negative rather than a race.
 *
 * @author Antony Stubbs
 */
@Timeout(60)
class NavigatorEngineLifecycleTest {

    static final String TOPIC = NavigatorEngineLifecycleTest.class.getSimpleName();
    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    static final String API_X = "api-x";
    static final String MEMBER = "pc-lifecycle-member";
    static final Duration QUANTUM = Duration.ofSeconds(1);

    final MutableClock clock = MutableClock.epochUTC();
    final StubResourceAllocator allocator = new StubResourceAllocator(clock);
    final AtomicInteger invocations = new AtomicInteger();

    MockConsumer<String, String> mockConsumer;
    ParallelEoSStreamProcessor<String, String> pc;

    @AfterEach
    void tearDown() {
        Awaitility.reset();
        if (pc != null) {
            pc.close();
        }
    }

    /**
     * One resource at 1 credit/sec: with a single member the equal share is one credit per quantum, so two
     * buffered records dispatch strictly one quantum apart - the deferral AE5 needs, with no second instance.
     */
    @Test
    void aDeferredRecordsFunctionIsNotInvokedBeforeAvailableAtAndCloseDropsTheShare() {
        allocator.register(new ResourceContract(API_X, 1.0, 1, QUANTUM));
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceTags(UniLists.of(API_X))
                .resourceAllocator(allocator)
                .build();
        var module = new PCModuleTestEnv(options, clock);
        pc = new ParallelEoSStreamProcessor<>(options, module);

        // the MockConsumerTestBase rebalance dance: MockConsumer assigns nothing on subscribe, so the
        // partition is rebalanced in by hand and PC told separately; beginning offsets BEFORE the rebalance
        pc.subscribe(UniLists.of(TOPIC));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        mockConsumer.rebalance(Collections.singletonList(tp));
        pc.onPartitionsAssigned(UniLists.of(tp));

        // poll() is the running transition: the navigator join lands here, at virtual epoch (quantum 0),
        // so membership is effective from quantum 1 (R16) and NOTHING can dispatch while the clock holds
        pc.poll(context -> invocations.incrementAndGet());
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "key-0", "value-0"));
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "key-1", "value-1"));

        // AE5, the honest negative: the engine loops on wall time with both records buffered, and invokes
        // NEITHER - no credit exists at quantum 0. Hundreds of real dispatch opportunities, zero invocations.
        Awaitility.await("no function invocation while no credit exists")
                .during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(10))
                .until(() -> invocations.get() == 0);

        // quantum 1: the control loop's own per-pass readQuantum mints ONE credit - exactly one record runs
        clock.add(QUANTUM);
        Awaitility.await("first credit dispatches exactly one record")
                .atMost(Duration.ofSeconds(10)).until(() -> invocations.get() == 1);
        Awaitility.await("the second record is genuinely withheld - one credit cannot dispatch two")
                .during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(10))
                .until(() -> invocations.get() == 1);
        assertThat(allocator.conservationLedger(API_X, clock.instant()).getSpent()).isEqualTo(1);

        // quantum 2: the deferred record's availableAt has passed - it dispatches on the loop's next mint
        clock.add(QUANTUM);
        Awaitility.await("the deferred record dispatches once its availableAt passes")
                .atMost(Duration.ofSeconds(10)).until(() -> invocations.get() == 2);

        // close ENTRY calls leave (R16): the share is gone from the NEXT quantum, NOT after the lease TTL -
        // the membership lease was renewed by the loop moments ago, so TTL alone (3 quanta) would still count
        // this member at quantum 3; only the close-entry leave explains a zero share there
        pc.closeDontDrainFirst();
        Instant nextQuantumAfterClose = clock.instant().plus(QUANTUM);
        assertWithMessage("explicit close must drop the share at the next quantum without waiting for the TTL")
                .that(allocator.localRatePerSecond(MEMBER, API_X, nextQuantumAfterClose)).isEqualTo(0.0);

        var ledger = allocator.conservationLedger(API_X, clock.instant());
        assertWithMessage("both dispatches spent, nothing over-spent (%s)", ledger)
                .that(ledger.getSpent()).isEqualTo(2);
        assertThat(ledger.getOverdraft()).isEqualTo(0);
        assertWithMessage("conservation identity closes at end of life (%s)", ledger)
                .that(ledger.getOutstanding()).isEqualTo(ledger.getLiveCredits());
    }
}
