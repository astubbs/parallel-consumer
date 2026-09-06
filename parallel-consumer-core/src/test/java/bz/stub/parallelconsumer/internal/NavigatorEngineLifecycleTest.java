package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.navigator.StubResourceAllocator;
import bz.stub.parallelconsumer.navigator.ResourceContract;
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

import static bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy.IN_PROCESS;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The navigator wired through a REAL control loop (the plan's U3, AE5's honest half): a resource-deferred
 * record's user function is GENUINELY NOT INVOKED - the engine runs freely for hundreds of passes and
 * dispatches nothing - until the virtual clock passes the record's {@code availableAt}, at which point the
 * loop's own per-pass quantum read (KTD4) mints the credit and the record runs. Then the CLOSING transition
 * drops the membership share at the next quantum without waiting for the lease TTL (R16, AE2's engine half) -
 * and a close(DRAIN) keeps the share, and so the credit supply, for the whole drain tail.
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
                .allocationStrategy(IN_PROCESS)
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

        // the CLOSING transition calls leave (R16, revised): DONT_DRAIN transitions on the caller's thread at
        // close entry, so the share is gone from the NEXT quantum, NOT after the lease TTL - the membership
        // lease was renewed by the loop moments ago, so TTL alone (3 quanta) would still count this member at
        // quantum 3; only the explicit leave explains a zero share there
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

    /**
     * The DRAIN half of the lifecycle (the close-entry-leave starvation fix): close(DRAIN) with a
     * resource-deferred backlog must keep the membership - and so the credit supply - for the whole drain
     * tail. Leaving at close ENTRY expired the live credits immediately and a left member never re-mints,
     * while the quantum tick was gated out of DRAINING: no new credit could ever mint, the deferred records
     * never became eligible, and close stalled until the drain timeout. Now the tick runs while DRAINING and
     * the leave fires at the drain-complete CLOSING transition - exactly once.
     * <p>
     * Four records against one credit per quantum: at most one more could dispatch before the state leaves
     * RUNNING, so completing the drain REQUIRES credits minted while DRAINING - a ticker thread advances the
     * shared virtual clock a quantum at a time while close(DRAIN) blocks the test thread.
     */
    /**
     * The partition-share plan's U3 integration scenario, through the REAL selection path under the DEFAULT
     * strategy: the first assignment's metadata read declines (the stock {@link MockConsumer} knows no
     * partitions for the topic until told), so the total is unresolved and a tagged record is deferred across
     * quantum boundaries - R5, minting nothing is not the same as minting at a boundary - until a resolving
     * assignment lands, after which the loop's own quantum pull at the next boundary mints the share and the
     * record dispatches. The view reports the resolved share alongside (R9).
     */
    @Test
    void underPartitionShareATaggedRecordIsDeferredWhileTheTotalIsUnresolvedAndDispatchesAfterItResolves() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceTags(UniLists.of(API_X))
                .resourceContracts(UniLists.of(new ResourceContract(API_X, 1.0, 1, QUANTUM)))
                .build();
        var module = new PCModuleTestEnv(options, clock);
        pc = new ParallelEoSStreamProcessor<>(options, module);
        var view = module.navigatorView();

        pc.subscribe(UniLists.of(TOPIC));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        mockConsumer.rebalance(Collections.singletonList(tp));
        // the mock knows no partitions for TOPIC yet: the timed read returns empty, a decline - unresolved (KTD3)
        pc.onPartitionsAssigned(UniLists.of(tp));

        pc.poll(context -> invocations.incrementAndGet());
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "key-0", "value-0"));

        // R5: two full quantum boundaries pass with the record buffered, and nothing mints because the
        // denominator is unknown - hundreds of dispatch opportunities, zero invocations
        clock.add(QUANTUM);
        clock.add(QUANTUM);
        Awaitility.await("no function invocation while the partition total is unresolved")
                .during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(10))
                .until(() -> invocations.get() == 0);
        assertThat(view.shareFraction(API_X).getAsDouble()).isEqualTo(0.0);

        // the resolving assignment: the metadata now names the topic's one partition, and a (cooperative-shape,
        // empty) assign re-reads it - published from this thread the way the poll thread would, lock-free
        mockConsumer.updatePartitions(TOPIC, UniLists.of(
                new org.apache.kafka.common.PartitionInfo(TOPIC, 0, null, null, null)));
        pc.onPartitionsAssigned(Collections.emptyList());

        // still nothing until the NEXT boundary (R4's next-quantum rule)...
        Awaitility.await("nothing dispatches inside the quantum the resolution landed in")
                .during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(10))
                .until(() -> invocations.get() == 0);
        // ...and at it, the loop's own per-pass read mints the whole share (1 of 1) and the record runs
        clock.add(QUANTUM);
        Awaitility.await("the deferred record dispatches once the total resolves")
                .atMost(Duration.ofSeconds(10)).until(() -> invocations.get() == 1);
        assertThat(view.shareFraction(API_X).getAsDouble()).isEqualTo(1.0);
        assertThat(view.creditsPerQuantum(API_X).getAsDouble()).isEqualTo(1.0);
    }

    @Test
    void closeWithDrainCompletesAResourceDeferredBacklogAndLeavesExactlyOnce() {
        var leaveCalls = new AtomicInteger();
        var countingAllocator = new StubResourceAllocator(clock) {
            @Override
            public void leave(String memberId, Instant now) {
                leaveCalls.incrementAndGet();
                super.leave(memberId, now);
            }
        };
        countingAllocator.register(new ResourceContract(API_X, 1.0, 1, QUANTUM));
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceTags(UniLists.of(API_X))
                .allocationStrategy(IN_PROCESS)
                .resourceAllocator(countingAllocator)
                .drainTimeout(Duration.ofSeconds(20))
                .build();
        var module = new PCModuleTestEnv(options, clock);
        pc = new ParallelEoSStreamProcessor<>(options, module);

        pc.subscribe(UniLists.of(TOPIC));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        mockConsumer.rebalance(Collections.singletonList(tp));
        pc.onPartitionsAssigned(UniLists.of(tp));

        pc.poll(context -> invocations.incrementAndGet());
        int totalRecords = 4;
        for (int offset = 0; offset < totalRecords; offset++) {
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, "key-" + offset, "value-" + offset));
        }

        // quantum 1: one credit, one dispatch - the remaining three are genuinely resource-deferred
        clock.add(QUANTUM);
        Awaitility.await("the first credit dispatches exactly one record")
                .atMost(Duration.ofSeconds(10)).until(() -> invocations.get() == 1);

        // while close(DRAIN) blocks this thread, the ticker advances the SHARED virtual clock; each dispatch
        // needs a fresh credit, so the backlog draining at all proves quanta were read while DRAINING
        var ticker = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
                clock.add(QUANTUM);
            }
        }, "virtual-clock-ticker");
        ticker.start();
        try {
            pc.close(Duration.ofSeconds(10), DrainingCloseable.DrainingMode.DRAIN);
        } finally {
            ticker.interrupt();
        }

        assertWithMessage("the whole deferred backlog must have drained before close completed")
                .that(invocations.get()).isEqualTo(totalRecords);
        assertWithMessage("every dispatch spent a credit the drain tail minted")
                .that(countingAllocator.conservationLedger(API_X, clock.instant()).getSpent())
                .isEqualTo(totalRecords);
        assertWithMessage("membership must leave exactly once, at the drain-complete CLOSING transition")
                .that(leaveCalls.get()).isEqualTo(1);
    }

    /**
     * The engine-side DRAIN bound: a backlog that can NEVER drain - here a rate-0 resource, the documented
     * shut valve, so no credit ever mints for anyone - must not wedge close(DRAIN) forever. Before the bound,
     * drain() had no exit but an empty shard, waitForClose could only throw on the caller's thread (never
     * transitioning the engine), and the DRAINING gate in tickNavigatorQuantumRead kept renewing the lease -
     * so neither the explicit leave nor the lease TTL could ever release the wedged instance's resource
     * share. Now the drainDeadline (stamped from {@code drainTimeout} on the shared virtual clock) passes,
     * drain() warns and transitions to CLOSING anyway, and the membership leaves exactly once. The record
     * itself is never invoked - undrained means uncommitted, redelivered after rebalance.
     */
    @Test
    void closeWithDrainIsBoundedWhenTheBacklogCanNeverDrain() {
        var leaveCalls = new AtomicInteger();
        var countingAllocator = new StubResourceAllocator(clock) {
            @Override
            public void leave(String memberId, Instant now) {
                leaveCalls.incrementAndGet();
                super.leave(memberId, now);
            }
        };
        countingAllocator.register(new ResourceContract(API_X, 0.0, 1, QUANTUM));
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .pcInstanceTag(MEMBER)
                .resourceTags(UniLists.of(API_X))
                .allocationStrategy(IN_PROCESS)
                .resourceAllocator(countingAllocator)
                .drainTimeout(Duration.ofSeconds(2))
                .build();
        var module = new PCModuleTestEnv(options, clock);
        pc = new ParallelEoSStreamProcessor<>(options, module);

        pc.subscribe(UniLists.of(TOPIC));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        mockConsumer.rebalance(Collections.singletonList(tp));
        pc.onPartitionsAssigned(UniLists.of(tp));

        pc.poll(context -> invocations.incrementAndGet());
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "key-0", "value-0"));

        // the shut valve: quanta pass and the record stays genuinely undispatchable - zero rate, zero credits
        clock.add(QUANTUM);
        Awaitility.await("rate 0 must never dispatch")
                .during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(10))
                .until(() -> invocations.get() == 0);

        // the ticker walks the SHARED virtual clock past the drainDeadline while close(DRAIN) blocks; the
        // caller-side waitForClose budget (drainTimeout + shutdownTimeout + grace, WALL time) stays untouched,
        // so a completed close here can only mean the ENGINE cut the drain short at its deadline
        var ticker = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
                clock.add(QUANTUM);
            }
        }, "virtual-clock-ticker");
        ticker.start();
        try {
            pc.close(Duration.ofSeconds(10), DrainingCloseable.DrainingMode.DRAIN);
        } finally {
            ticker.interrupt();
        }

        assertWithMessage("the wedged record must never have been invoked - it redelivers after rebalance")
                .that(invocations.get()).isEqualTo(0);
        // the allocator hearing the leave IS the share-release proof: localRatePerSecond cannot distinguish
        // "left" from "rate-0 member" (a zero share reads 0.0 either way), so it would assert nothing here
        assertWithMessage("membership must leave exactly once, at the deadline-forced CLOSING transition")
                .that(leaveCalls.get()).isEqualTo(1);
        assertWithMessage("the engine must have genuinely reached CLOSED, not merely returned")
                .that(pc.isClosedOrFailed()).isTrue();
    }
}
