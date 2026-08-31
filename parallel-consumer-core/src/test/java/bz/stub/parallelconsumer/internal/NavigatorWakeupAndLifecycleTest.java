package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.navigator.ResourceAllocator;
import bz.stub.parallelconsumer.internal.navigator.ResourceContract;
import bz.stub.parallelconsumer.internal.navigator.StubResourceAllocator;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The navigator's control-loop seams on the virtual clock (the plan's U3): the {@code timeToBlockFor}
 * resource-keyed wakeup bound (KTD5) - its own branch, not floored by the retry delay - and the lifecycle
 * hooks' per-call contracts ({@code joinNavigatorOnRunning}, {@code tickNavigatorQuantumRead},
 * {@code leaveNavigatorOnClosingTransition}; R16/KTD4), driven directly the way {@code AdmissionSeamTest} and
 * {@code AdmissionLifecycleTest} drive their seams, without a real control loop. The proof that a REAL control
 * loop fires these hooks at the right moments is {@link NavigatorEngineLifecycleTest}.
 *
 * @author Antony Stubbs
 */
class NavigatorWakeupAndLifecycleTest {

    static final String TOPIC = "navigator-wakeup-topic";
    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    static final String API_X = "api-x";
    static final String MEMBER = "wakeup-member";

    /** Far longer than any next-credit time here, so the commit branch never wins the minimum by accident. */
    static final Duration HUGE_COMMIT_INTERVAL = Duration.ofHours(1);

    MutableClock clock;
    StubResourceAllocator allocator;
    PCModuleTestEnv module;
    TestParallelEoSStreamProcessor<String, String> pc;
    WorkManager<String, String> wm;
    long nextOffset = 0;

    @AfterEach
    void tearDown() {
        if (pc != null) {
            // these tests never start the control loop, so the normal close handshake does not apply
            pc.setState(State.CLOSED);
            pc.close();
            pc.workerThreadPool.get().shutdownNow();
        }
    }

    void buildTaggedHarness() {
        clock = MutableClock.epochUTC();
        allocator = new StubResourceAllocator(clock);
        allocator.register(new ResourceContract(API_X, 1.0, 1, Duration.ofSeconds(1)));
        buildHarness(optionsBuilder()
                .resourceTags(UniLists.of(API_X))
                .resourceAllocator(allocator)
                .build());
    }

    void buildUntaggedHarness() {
        clock = MutableClock.epochUTC();
        buildHarness(optionsBuilder().build());
    }

    void buildHarness(ParallelConsumerOptions<String, String> options) {
        module = new PCModuleTestEnv(options, clock);
        pc = new TestParallelEoSStreamProcessor<>(options, module);
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        pc.setWm(wm);
    }

    ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pcInstanceTag(MEMBER)
                .commitInterval(HUGE_COMMIT_INTERVAL)
                .useVirtualThreads(false);
    }

    /** Registers {@code count} records, left sitting in the shards - waiting work for the bound's guard. */
    void registerWork(int count) {
        var records = new ArrayList<ConsumerRecord<String, String>>();
        for (int i = 0; i < count; i++) {
            long offset = nextOffset++;
            records.add(new ConsumerRecord<>(TOPIC, 0, offset, "key-" + offset, "value"));
        }
        wm.registerWork(new EpochAndRecordsMap<>(
                new ConsumerRecords<>(UniMaps.of(tp, records)), wm.getPm()));
    }

    // -----------------------------------------------------------------------------------------------------
    // KTD5: the wakeup bound
    // -----------------------------------------------------------------------------------------------------

    /**
     * With all waiting work resource-deferred (no credit was ever pulled), the computed block time is bounded
     * by the earliest next-credit time - NOT the default poll/commit interval. At t=200ms into a one-second
     * quantum the next credit is 800ms away, against a commit branch of about an hour.
     */
    @Test
    void blockTimeIsBoundedByTheEarliestNextCreditTimeNotThePollDefault() {
        buildTaggedHarness();
        pc.setState(State.RUNNING); // the commit branch reads ZERO outside idling/running, which would mask the bound
        registerWork(2);
        clock.add(Duration.ofMillis(200));

        Duration blockFor = pc.timeToBlockFor();

        assertWithMessage("the block time must be capped at the next quantum boundary, where credit arrives")
                .that(blockFor).isEqualTo(Duration.ofMillis(800));
    }

    /**
     * The retry-branch floor must not swallow the resource bound (the KTD5 own-branch requirement): with
     * failed work scheduled 30s out - which the retry branch floors at the default retry delay and would
     * return as tens of seconds - a credit due in 800ms still caps the block time at 800ms.
     */
    @Test
    void theRetryDelayFloorDoesNotSwallowASubSecondNextCreditTime() {
        buildTaggedHarness();
        pc.setState(State.RUNNING);
        clock.add(Duration.ofMillis(200));
        WorkManager<String, String> mockWm = Mockito.mock(WorkManager.class);
        Mockito.when(mockWm.isWorkInFlightMeetingTarget()).thenReturn(false);
        Mockito.when(mockWm.getLowestRetryTime()).thenReturn(Optional.of(Duration.ofSeconds(30)));
        Mockito.when(mockWm.getNumberOfWorkQueuedInShardsAwaitingSelection()).thenReturn(5L);
        pc.setWm(mockWm);

        Duration blockFor = pc.timeToBlockFor();

        assertWithMessage("a sub-second next-credit time must win over the retry branch's floored result")
                .that(blockFor).isEqualTo(Duration.ofMillis(800));
    }

    /** The control arm: an untagged instance's block arithmetic is byte-for-byte today's (R3). */
    @Test
    void untaggedInstanceKeepsTodaysBlockArithmetic() {
        buildUntaggedHarness();
        pc.setState(State.RUNNING);
        registerWork(2);
        clock.add(Duration.ofMillis(200));

        Duration blockFor = pc.timeToBlockFor();

        assertWithMessage("no resource term exists for an untagged instance - the commit branch must stand")
                .that(blockFor).isGreaterThan(Duration.ofMinutes(30));
    }

    /** No waiting work means nothing can be resource-deferred, so the bound must not fire (soft guard). */
    @Test
    void withNoWaitingWorkTheResourceBoundDoesNotFire() {
        buildTaggedHarness();
        pc.setState(State.RUNNING);
        clock.add(Duration.ofMillis(200));

        Duration blockFor = pc.timeToBlockFor();

        assertWithMessage("an idle tagged instance must not wake early for credits nothing is waiting on")
                .that(blockFor).isGreaterThan(Duration.ofMinutes(30));
    }

    // -----------------------------------------------------------------------------------------------------
    // R16/KTD4: the lifecycle hooks' per-call contracts
    // -----------------------------------------------------------------------------------------------------

    /**
     * The three hooks drive the allocator's membership the way the plan's R16 anchors demand: join makes the
     * member count from the next quantum, the per-pass tick mints its share, and leave at the CLOSING
     * transition expires its live credits and drops its share from the next quantum - WITHOUT waiting for the
     * lease TTL (AE2's engine half; the TTL-only path is allocator-covered).
     */
    @Test
    void joinTickAndLeaveDriveTheAllocatorMembershipLifecycle() {
        buildTaggedHarness();

        pc.joinNavigatorOnRunning(); // at epoch: member from quantum 1
        clock.add(Duration.ofSeconds(1));
        pc.setState(State.RUNNING);
        pc.tickNavigatorQuantumRead();

        var lease = allocator.currentLease(MEMBER, API_X, clock.instant());
        assertWithMessage("the per-pass tick must have pulled this quantum's share into a live lease")
                .that(lease.isPresent()).isTrue();
        assertThat(lease.get().getAvailableCredits()).isEqualTo(1);

        pc.leaveNavigatorOnClosingTransition();

        assertWithMessage("leave must expire the live unspent credits immediately (death loses capacity)")
                .that(allocator.conservationLedger(API_X, clock.instant()).getExpired()).isEqualTo(1);
        assertWithMessage("the share must be gone from the NEXT quantum - explicit close does not wait for "
                + "the lease TTL, which (at %s quanta) would still count this member",
                StubResourceAllocator.MEMBERSHIP_LEASE_TTL_QUANTA)
                .that(allocator.localRatePerSecond(MEMBER, API_X, Instant.ofEpochSecond(2))).isEqualTo(0.0);
    }

    /**
     * The tick's state gate: PAUSED keeps membership alive (an idle-but-live instance keeps its share, R17;
     * pause is a credit no-op, KTD10), and DRAINING still ticks - the membership only leaves at the CLOSING
     * transition, so the drain tail keeps earning the credits a resource-deferred backlog needs to ever
     * drain (a close-entry leave stalled close(DRAIN) until timeout). CLOSING, which follows that leave,
     * ticks nothing.
     */
    @Test
    void theQuantumTickRunsWhilePausedAndDrainingButNotWhileClosing() {
        buildTaggedHarness();
        pc.joinNavigatorOnRunning();
        clock.add(Duration.ofSeconds(1));

        pc.setState(State.DRAINING);
        pc.tickNavigatorQuantumRead();
        assertWithMessage("a draining instance is still dispatching its backlog - the tick must keep minting")
                .that(allocator.currentLease(MEMBER, API_X, clock.instant()).isPresent()).isTrue();

        pc.setState(State.PAUSED);
        pc.tickNavigatorQuantumRead();
        assertWithMessage("a paused instance is alive and keeps pulling its share (R17)")
                .that(allocator.currentLease(MEMBER, API_X, clock.instant()).isPresent()).isTrue();

        pc.leaveNavigatorOnClosingTransition();
        pc.setState(State.CLOSING);
        clock.add(Duration.ofSeconds(1));
        pc.tickNavigatorQuantumRead();
        assertWithMessage("a closing instance has already left - the tick must not renew or mint")
                .that(allocator.currentLease(MEMBER, API_X, clock.instant()).isPresent()).isFalse();
    }

    /**
     * The exactly-once guard on the leave: every route into CLOSING calls
     * {@code leaveNavigatorOnClosingTransition} - the caller's thread via either close() mode, the control
     * thread at drain-complete or on the worker-pool-death self-close - and however many of them fire, the
     * allocator must hear exactly ONE leave (a second membership event would be noise in its append-only log).
     */
    @Test
    void repeatedClosingTransitionsProduceExactlyOneAllocatorLeave() {
        clock = MutableClock.epochUTC();
        var leaveCalls = new AtomicInteger();
        allocator = new StubResourceAllocator(clock) {
            @Override
            public void leave(String memberId, Instant now) {
                leaveCalls.incrementAndGet();
                super.leave(memberId, now);
            }
        };
        allocator.register(new ResourceContract(API_X, 1.0, 1, Duration.ofSeconds(1)));
        buildHarness(optionsBuilder()
                .resourceTags(UniLists.of(API_X))
                .resourceAllocator(allocator)
                .build());
        pc.joinNavigatorOnRunning();

        pc.leaveNavigatorOnClosingTransition();
        pc.leaveNavigatorOnClosingTransition();

        assertWithMessage("racing CLOSING transitions must collapse to one allocator leave")
                .that(leaveCalls.get()).isEqualTo(1);
    }

    /** R3 on the control-loop seams: an untagged instance's hooks reach no allocator at all. */
    @Test
    void untaggedLifecycleHooksTouchNoAllocator() {
        clock = MutableClock.epochUTC();
        ResourceAllocator untouched = Mockito.mock(ResourceAllocator.class);
        buildHarness(optionsBuilder()
                .resourceAllocator(untouched) // present but untagged - the zero-cost path is decided by TAGS
                .build());
        pc.setState(State.RUNNING);

        pc.joinNavigatorOnRunning();
        pc.tickNavigatorQuantumRead();
        pc.timeToBlockFor();
        pc.leaveNavigatorOnClosingTransition();

        Mockito.verifyNoInteractions(untouched);
    }

    // -----------------------------------------------------------------------------------------------------
    // Startup failure: close() on an engine whose control loop was never submitted
    // -----------------------------------------------------------------------------------------------------

    /**
     * supervisorLoop sets state=RUNNING and then runs startup code (the navigator join, the broker-poll
     * start) BEFORE submitting the control task - a throw in that window strands the instance at RUNNING
     * with an empty {@code controlThreadFuture}. A later close() must surface that as a clear
     * IllegalStateException from waitForClose, not the unrelated NoSuchElementException an unguarded
     * {@code Optional#get} used to produce (which none of waitForClose's catch clauses name).
     */
    @Test
    void closeAfterAStrandedStartupSurfacesAStartupFailureNotAnEmptyOptional() {
        buildUntaggedHarness();
        pc.setState(State.RUNNING); // the stranded shape: state advanced, control task never submitted

        var thrown = assertThrows(IllegalStateException.class, () -> pc.closeDontDrainFirst());

        assertThat(thrown).hasMessageThat().contains("Control loop was never started");
    }
}
