package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.*;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.ProducerManager;
import bz.stub.parallelconsumer.internal.State;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static bz.stub.parallelconsumer.internal.utils.GeneralTestUtils.time;
import static bz.stub.parallelconsumer.internal.utils.KafkaTestUtils.checkExactOrdering;
import static bz.stub.parallelconsumer.internal.utils.KafkaUtils.toTopicPartition;
import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.awaitLatch;
import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.constructLatches;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.*;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static pl.tlinkowski.unij.api.UniLists.of;

@Timeout(value = 3, unit = MINUTES)
@Slf4j
public class ParallelEoSStreamProcessorTest extends ParallelEoSStreamProcessorTestBase {

    public static class MyAction implements Function<ConsumerRecord<String, String>, String> {

        @Override
        public String apply(ConsumerRecord<String, String> record) {
            log.info("User client function - consuming a record... {}", record.key());
            return "my-result";
        }
    }

    @BeforeEach()
    public void setupData() {
        primeFirstRecord();
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    public void failingActionNothingCommitted(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        parallelConsumer.poll((ignore) -> {
            throw new FakeRuntimeException("My user's function error");
        });

        // let it process
        awaitForSomeLoopCycles(3);

        parallelConsumer.close();

        //
        assertCommits(of(), "All erroring, so nothing committed except initial");
    }

    @Test
    @SneakyThrows
    public void closePCWhenInvalidPidMappingException() {
        setupParallelConsumerInstance(PERIODIC_CONSUMER_ASYNCHRONOUS);

        MockitoAnnotations.openMocks(this);
        final ParallelEoSStreamProcessor<String, String> pcSpy = spy(parallelConsumer);
        final InvalidPidMappingException invalidPidMappingException = new InvalidPidMappingException("InvalidPidMappingException exception");

        // use mocked producer manager
        ProducerManager<String, String> producerManager = mock(ProducerManager.class);
        Field producerManagerField = AbstractParallelEoSStreamProcessor.class.getDeclaredField("producerManager");
        producerManagerField.setAccessible(true);
        producerManagerField.set(pcSpy, Optional.of(producerManager));

        when(producerManager.beginProducing(any())).thenReturn(mock(ProducerManager.ProducingLock.class));
        when(producerManager.produceMessages(any())).thenThrow(invalidPidMappingException);

        CountDownLatch latch = new CountDownLatch(1);
        // count down latch if close is invoked
        Mockito.doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            latch.countDown();
            return result;
        }).when(pcSpy).close(any());

        pcSpy.pollAndProduceMany((record) -> of(new ProducerRecord<>("outputTopic", record.key(), record.value())));

        latch.await();

        //check failure cause
        Assertions.assertThat(pcSpy.getFailureCause().equals(invalidPidMappingException)).isTrue();

        //check state is CLOSED
        Field state = AbstractParallelEoSStreamProcessor.class.getDeclaredField("state");
        state.setAccessible(true);
        Assertions.assertThat(State.CLOSED.equals(state.get(pcSpy))).isTrue();

    }


    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    public void executorThreadsInterruptedOnShutdownTimeout(CommitMode commitMode) {
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        setupParallelConsumerInstance(getBaseOptionsKeyOrdered(commitMode, Duration.ofSeconds(1)));
        primeFirstRecord();

        parallelConsumer.poll((ignore) -> {
            try {
                latch.await();
            } catch (InterruptedException interruptedException) {
                interrupted.set(true);
                Thread.interrupted(); //reset interrupted flag.
                throw new RuntimeException(interruptedException);
            }
        });

        // let it process
        awaitForSomeLoopCycles(2);

        parallelConsumer.close();

        //
        assertCommits(of(), "All erroring, so nothing committed except initial");
        assertThat(interrupted).isTrue();
    }


    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    public void inFlightMessagesCommittedIfProcessedDuringShutdown(CommitMode commitMode) {
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        setupParallelConsumerInstance(getBaseOptionsKeyOrdered(commitMode, Duration.ofSeconds(1)));
        primeFirstRecord();

        parallelConsumer.poll((ignore) -> {
            try {
                latch.await();
                ThreadUtils.sleepQuietly(100);
            } catch (InterruptedException interruptedException) {
                interrupted.set(true);
                Thread.interrupted(); //reset interrupted flag.
            }
        });

        // let it process
        awaitForSomeLoopCycles(2);

        latch.countDown();
        parallelConsumer.close();

        //
        assertCommits(of(1), "1 record completed during shutdown");
        assertThat(interrupted).isFalse();
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    public void queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown(CommitMode commitMode) {
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        // Short commit interval so the primed record's commit lands while v1 is still blocked, at a point
        // the test can deterministically wait for. With the 5s default, the only commit before the latch
        // released was whichever one the close sequence happened to emit first - see the await below.
        setupParallelConsumerInstance(getBaseOptionsKeyOrdered(commitMode, Duration.ofSeconds(1), Duration.ofMillis(100)));

        primeFirstRecord();

        consumerSpy.addRecord(ktu.makeRecord("0", "v1"));
        consumerSpy.addRecord(ktu.makeRecord("0", "v2"));
        consumerSpy.addRecord(ktu.makeRecord("1", "v3"));
        consumerSpy.addRecord(ktu.makeRecord("0", "v4"));
        AtomicBoolean gotK0 = new AtomicBoolean(false);
        parallelConsumer.poll((record) -> {
            if (record.getSingleConsumerRecord().value().equals("v1")) {
                gotK0.set(true);
                try {
                    latch.await();
                    ThreadUtils.sleepQuietly(100);
                } catch (InterruptedException interruptedException) {
                    interrupted.set(true);
                    Thread.interrupted(); //reset interrupted flag.
                }
            }
        });
        // Wait for the blocking record to actually be in flight - only then is v1 latched and v2 queued
        // behind it (KEY ordering), which is the state this test is about.
        awaitUntilTrue(gotK0::get);

        // Then wait for the PRECONDITION the final assertion depends on: offset 1 committed, i.e. the
        // primed record committed while v1 is still blocked.
        //
        // This used to wait on loop CYCLES (`awaitForSomeLoopCycles(2)`), but commits are driven by
        // wall-clock, not by cycle count. Nothing guaranteed a commit had happened before the latch was
        // released, after which v1 completes (it sleeps 100ms) and the next commit covers offset 2 - so
        // offset 1 was never committed on its own and the set-wise assertion saw only [2, 2]. Waiting on
        // the commit itself makes the precondition explicit instead of probable. Full triage in
        // astubbs#101, which diagnosed and fixed this; the ledger entry it came from was in
        // docs/inflight.md, the single file that became docs/inflight/ and was deleted in 0de96fc
        // (git show 0de96fc^:docs/inflight.md, grep this test's name).
        awaitForCommit(1);

        latch.countDown();
        parallelConsumer.close();

        //
        assertCommits(of(1, 2), "primed record and first key=0 record completed only, followup key 0 records skipped");
        assertCommits().encodedIncomplete(2); //first blocked/skipped key 0 record (value v2).
        assertThat(interrupted).isFalse();
    }

    /**
     * Deterministic version of the race that made
     * {@link #queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown(CommitMode)} flake.
     * <p>
     * Under KEY ordering the key="1" record sits on a shard independent of the blocked key="0" shard, so it
     * completes while v1 is still latched. Its completion marks the partition dirty, but cannot advance the
     * committable offset - {@link bz.stub.parallelconsumer.state.PartitionState#getOffsetToCommit()} is
     * the highest <em>sequentially</em> succeeded offset plus one, and offset 1 is still in flight. PC
     * therefore commits base offset 1 a second time, carrying updated incomplete-offset encoding.
     * <p>
     * That repeat is correct behaviour - it persists the key="1" completion so a crash does not reprocess it
     * - so every commit mode must tolerate it. Holding the key="1" record until after the first commit has
     * landed forces the repeat every run, instead of leaving it to where the wall-clock commit ticks fall.
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    public void repeatCommitOfSameBaseOffsetToleratedWhenIndependentKeyCompletesDuringBlock(CommitMode commitMode) {
        CountDownLatch blockedKeyLatch = new CountDownLatch(1);
        CountDownLatch independentKeyLatch = new CountDownLatch(1);
        setupParallelConsumerInstance(getBaseOptionsKeyOrdered(commitMode, Duration.ofSeconds(1), Duration.ofMillis(100)));

        primeFirstRecord();

        consumerSpy.addRecord(ktu.makeRecord("0", "v1"));
        consumerSpy.addRecord(ktu.makeRecord("0", "v2"));
        consumerSpy.addRecord(ktu.makeRecord("1", "v3"));
        AtomicBoolean gotK0 = new AtomicBoolean(false);
        parallelConsumer.poll((record) -> {
            String value = record.getSingleConsumerRecord().value();
            if (value.equals("v1")) {
                gotK0.set(true);
                awaitLatch(blockedKeyLatch);
            } else if (value.equals("v3")) {
                awaitLatch(independentKeyLatch);
            }
        });
        awaitUntilTrue(gotK0::get);

        // the primed record's commit lands first, encoding only itself
        awaitForCommit(1);
        int commitsBeforeIndependentKey = getCommitHistoryFlattened().size();

        // only now let the independent shard finish, so its completion cannot be folded into that commit
        independentKeyLatch.countDown();
        awaitForCommittedOffsetCount(commitsBeforeIndependentKey + 1);

        // v1 is still blocked, so the second commit cannot have advanced the base offset - it must be a
        // repeat of offset 1 carrying new encoding. Assert the repeat is really there: without this the test
        // would still pass if the scenario stopped reproducing, and would then be guarding nothing.
        assertThat(getCommitHistoryFlattened())
                .as("the scenario under test must actually produce a repeat commit of base offset 1")
                .filteredOn(committed -> committed == 1)
                .hasSizeGreaterThanOrEqualTo(2);

        // Asserted before the latch is released, so the shutdown sequence cannot add commits and blur what is
        // being checked. This is the assertion that fails without the repeat-tolerance in the commit helper.
        assertCommits(of(1), "base offset 1 committed twice: once for the primed record, once carrying the "
                + "key=1 completion, which cannot advance the base offset while v1 is in flight");

        blockedKeyLatch.countDown();
        parallelConsumer.close();
    }

    /**
     * Checks that - for messages that are currently undergoing processing, that no offsets for them are committed
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    void offsetsAreNeverCommittedForMessagesStillInFlightSimplest(CommitMode commitMode) {
        var options = getBaseOptions(commitMode).toBuilder()
                .ordering(UNORDERED)
                .build();
        setupParallelConsumerInstance(options);
        parallelConsumer.setTimeBetweenCommits(ofSeconds(1));

        primeFirstRecord();
        sendSecondRecord(consumerSpy);

        // sanity
        assertThat(parallelConsumer.getWm().getOptions().getOrdering()).isEqualTo(UNORDERED);

        // setup
        var locks = constructLatches(2);
        var processedStates = new LinkedHashMap<Integer, Boolean>();
        var startBarrierLatch = new CountDownLatch(1);

        // finish processing only msg 1
        parallelConsumer.poll(context -> {
            log.debug("msg: {}", context);
            startBarrierLatch.countDown();
            int offset = (int) context.offset();
            LatchTestUtils.awaitLatch(locks, offset);
            processedStates.put(offset, true);
        });

        //
        awaitLatch(startBarrierLatch);

        // zero records waiting, 2 out for processing
        assertThat(parallelConsumer.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection()).isZero();
        assertThat(parallelConsumer.getWm().getNumberRecordsOutForProcessing()).isEqualTo(2);

        // finish processing 1
        releaseAndWait(locks, 1);

        // make sure offset 0 is committed (next expected), while the rest are not
        parallelConsumer.requestCommitAsap();
        awaitForCommitExact(0);

        // make sure no offsets are committed
        assertCommits(of(), "Partition is blocked");

        // test complete

        // So it's data is setup can be used in other tests, finish offset 0 as well
        releaseAndWait(locks, 0);

        parallelConsumer.requestCommitAsap();

        awaitForCommitExact(2);

        log.debug("Closing...");
        parallelConsumer.closeDrainFirst();

        assertThat(processedStates)
                .as("sanity - all expected messages are processed")
                .containsValues(true, true);
    }

    private void setupParallelConsumerInstance(final CommitMode commitMode) {
        setupParallelConsumerInstance(getBaseOptions(commitMode));
        // created a new client above, so have to send the prime record again
        primeFirstRecord();
    }

    private ParallelConsumerOptions getBaseOptions(final CommitMode commitMode) {
        return ParallelConsumerOptions.<String, String>builder()
                .commitMode(commitMode)
                .consumer(consumerSpy)
                .producer(producerSpy)
                .build();
    }

    private ParallelConsumerOptions getBaseOptionsKeyOrdered(final CommitMode commitMode, final Duration shutdownDuration) {
        return getBaseOptionsKeyOrdered(commitMode, shutdownDuration, null);
    }

    /**
     * @param commitInterval null to keep the default. Pass a short interval when the test needs a periodic
     *                       commit to actually land at a point it can wait for, rather than hoping one
     *                       coincides with the close sequence.
     */
    private ParallelConsumerOptions getBaseOptionsKeyOrdered(final CommitMode commitMode, final Duration shutdownDuration,
                                                             final Duration commitInterval) {
        var builder = ParallelConsumerOptions.<String, String>builder()
                .commitMode(commitMode)
                .consumer(consumerSpy)
                .producer(producerSpy)
                .shutdownTimeout(shutdownDuration)
                .ordering(KEY);
        if (commitInterval != null) {
            builder.commitInterval(commitInterval);
        }
        return builder.build();
    }

    /**
     * {@link #offsetsAreNeverCommittedForMessagesStillInFlightSimplest(CommitMode)} doesn't check the final offsets -
     * that's what this test does.
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void offsetsAreNeverCommittedForMessagesStillInFlightShort(CommitMode commitMode) {
        offsetsAreNeverCommittedForMessagesStillInFlightSimplest(commitMode);
        log.info("Test start");

        // next expected offset is now 2
        await().untilAsserted(() ->
                assertCommits(of(2), "Only one of the two offsets committed, as they were coalesced for efficiency"));
    }

    @Disabled
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    void offsetsAreNeverCommittedForMessagesStillInFlightLong(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        sendSecondRecord(consumerSpy);

        // send three messages - 0, 1, 2
        consumerSpy.addRecord(ktu.makeRecord("0", "v2"));
        consumerSpy.addRecord(ktu.makeRecord("0", "v3"));
        consumerSpy.addRecord(ktu.makeRecord("0", "v4"));
        consumerSpy.addRecord(ktu.makeRecord("0", "v5"));

        List<CountDownLatch> locks = constructLatches(6);

        CountDownLatch startLatch = new CountDownLatch(1);

        parallelConsumer.poll((context) -> {
            int offset = (int) context.offset();
            CountDownLatch latchForMsg = locks.get(offset);
            try {
                startLatch.countDown();
                latchForMsg.await();
            } catch (InterruptedException e) {
                // ignore
            }
        });

        startLatch.countDown();

        // finish processing 1
        releaseAndWait(locks, 1);

        awaitForSomeLoopCycles(1);

        // make sure no offsets are committed
        verify(producerSpy, after(verificationWaitDelay).never()).commitTransaction();

        // finish 2
        releaseAndWait(locks, 2);

        //
        awaitForSomeLoopCycles(1);

        // make sure no offsets are committed
        verify(producerSpy, after(verificationWaitDelay).never()).commitTransaction();

        // finish 0
        releaseAndWait(locks, 0);
        awaitForOneLoopCycle();

        // make sure offset 2, not 0 or 1 is committed
        verify(producerSpy, after(verificationWaitDelay).times(1)).commitTransaction();
        var maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSize(1);
        OffsetAndMetadata offsets = maps.get(0).get(CONSUMER_GROUP_ID).get(toTopicPartition(firstRecord));
        assertThat(offsets.offset()).isEqualTo(2);

        // finish 3
        releaseAndWait(locks, 3);

        // 3 committed
        verify(producerSpy, after(verificationWaitDelay).times(2)).commitTransaction();
        maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSize(2);
        offsets = maps.get(1).get(CONSUMER_GROUP_ID).get(toTopicPartition(firstRecord));
        assertThat(offsets.offset()).isEqualTo(3);

        // finish 4,5
        releaseAndWait(locks, of(4, 5));

        // 5 committed
        verify(producerSpy, after(verificationWaitDelay).atLeast(3)).commitTransaction();
        maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSizeGreaterThanOrEqualTo(3);
        offsets = maps.get(2).get(CONSUMER_GROUP_ID).get(toTopicPartition(firstRecord));
        assertThat(offsets.offset()).isEqualTo(5);
        assertCommits(of(2, 3, 5));

        // close
        parallelConsumer.close();
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void offsetCommitsAreIsolatedPerPartition(CommitMode commitMode) {
        // Disable this test for vert.x for now
        Assumptions.assumeThat(parallelConsumer)
                .as("Should only test on core PC - this test is very complicated to get to work with vert.x " +
                        "thread system, as the event and locking system needed is quite different")
                .isExactlyInstanceOf(AbstractParallelEoSStreamProcessor.class);

        setupParallelConsumerInstance(getBaseOptions(commitMode).toBuilder()
                .ordering(UNORDERED)
                .build());
        primeFirstRecord();

        sendSecondRecord(consumerSpy);

        // send messages - 0,1, to one partition and 3,4 to another partition petitions
        consumerSpy.addRecord(ktu.makeRecord(1, "0", "v2"));
        consumerSpy.addRecord(ktu.makeRecord(1, "0", "v3"));

        var msg0Lock = new CountDownLatch(1);
        var msg1Lock = new CountDownLatch(1);
        var msg2Lock = new CountDownLatch(1);
        var msg3Lock = new CountDownLatch(1);

        List<CountDownLatch> locks = of(msg0Lock, msg1Lock, msg2Lock, msg3Lock);

        parallelConsumer.poll((ignore) -> {
            int offset = (int) ignore.offset();
            CountDownLatch latchForMsg = locks.get(offset);
            try {
                latchForMsg.await();
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
        });

        // finish processing 1
        releaseAndWait(locks, 1);

        parallelConsumer.requestCommitAsap();

        awaitForSomeLoopCycles(50); // async commit can be slow - todo change this to event based

        // make sure only base offsets are committed for partition (next expected = 0 and 2 respectively)
//        assertCommits(of(2));
        assertCommitLists(of(of(), of(2)));

        // finish 2
        releaseAndWait(locks, 2);
        parallelConsumer.requestCommitAsap();

        // make sure only 2 on it's partition is committed
//        assertCommits(of(2, 3));
        await().untilAsserted(() ->
                assertCommitLists(of(of(), of(2, 3))));

        // finish 0
        releaseAndWait(locks, 0);

        parallelConsumer.requestCommitAsap();

        awaitForOneLoopCycle();
        if (isUsingAsyncCommits())
            awaitForSomeLoopCycles(3); // async commit can be slow - todo change this to event based

        // make sure offset 0 and 1 is committed
        assertCommitLists(of(of(2), of(2, 3)));

        // finish 3
        releaseAndWait(locks, 3);

        // async consumer is slower to execute the commit. We could just wait, or we could add an event to the async consumer commit cycle
        if (isUsingAsyncCommits())
            awaitForSomeLoopCycles(3); // async commit can be slow - todo change this to event based

        //
        await().untilAsserted(() ->
                assertCommitLists(of(of(2), of(2, 3, 4))));
    }

    @ParameterizedTest
    @EnumSource(CommitMode.class)
    void controlFlowException(CommitMode commitMode) {
        // setup again manually to use subscribe instead of assign (for revoke testing)
        instantiateConsumerProducer();
        parentParallelConsumer = initPollingAsyncConsumer(getBaseOptions(commitMode));
        subscribeParallelConsumerAndMockConsumerTo(INPUT_TOPIC);
        setupData();

        // cause a control loop error
        parallelConsumer.addLoopEndCallBack(() -> {
            throw new FakeRuntimeException("My fake control loop error");
        });

        //
        parallelConsumer.poll((ignore) -> {
            log.info("Ignoring {}", ignore);
        });

        // close and retrieve exception in control loop
        assertThatThrownBy(() -> {
            parallelConsumer.closeDrainFirst(ofSeconds(10));
        }).hasMessageContainingAll("Error", "poll", "thread", "fake control");
    }

    /**
     * Registering a loop-end callback from a thread other than the control thread must not stop the consumer.
     * <p>
     * {@link AbstractParallelEoSStreamProcessor#addLoopEndCallBack} is public and documents no threading restriction,
     * yet the control loop iterates that same list every cycle. A registration landing mid-iteration therefore breaks
     * the iteration, and the failure escapes the control loop and takes the consumer down with it.
     * <p>
     * How the race is made deterministic, and why the assertion is on the outcome rather than the exception type, is
     * documented on {@link #assertRegisteringFromAnotherThreadDoesNotStopTheConsumer}.
     *
     * @see <a href="https://github.com/astubbs/parallel-consumer/issues/252">astubbs#252 - the same defect class in
     *         PartitionStateManager, where a plain HashMap of partition state was streamed while another thread
     *         mutated it. Fixed there by making the collection concurrent; this field was missed.</a>
     */
    @Test
    @SneakyThrows
    void loopEndCallBackCanBeRegisteredFromAnotherThread() {
        assertRegisteringFromAnotherThreadDoesNotStopTheConsumer(
                "off-control-thread-registrar",
                parallelConsumer::addLoopEndCallBack);
    }

    /**
     * Shared harness for the two registration races - the subsystems differ, the race and the outcome that matters do
     * not.
     * <p>
     * The latches make the race deterministic rather than hoping to land inside a window a few instructions wide: the
     * first callback parks the iterating thread mid-iteration until the off-thread registration has provably landed.
     * <p>
     * <b>The assertion is on the observable outcome - the consumer keeps running and closes cleanly - not on the
     * exception type.</b> What a user reports is "it stopped after a while"; a change that swapped one exception for
     * another would still be that bug, and this test would still fail, which is the point.
     *
     * @param registrarThreadName names the registering thread, so a thread dump taken during a hang says which race it
     *                            was - the stack of the parked control thread does not
     * @param register            registers the given body with the subsystem under test, adapting it to whatever
     *                            listener type that subsystem takes. Called twice: once for the callback that parks
     *                            the iterating thread, then again off that thread while the iteration is held open
     */
    @SneakyThrows
    private void assertRegisteringFromAnotherThreadDoesNotStopTheConsumer(String registrarThreadName,
                                                                         Consumer<Runnable> register) {
        var iterationIsInProgress = new CountDownLatch(1);
        var registrationLanded = new CountDownLatch(1);
        var parkedOnce = new AtomicBoolean(false);

        register.accept(() -> {
            if (parkedOnce.compareAndSet(false, true)) {
                iterationIsInProgress.countDown();
                awaitLatch(registrationLanded);
            }
        });

        var registrar = new Thread(() -> {
            awaitLatch(iterationIsInProgress);
            register.accept(() -> log.trace("Registered off the iterating thread"));
            registrationLanded.countDown();
        }, registrarThreadName);
        registrar.start();

        parallelConsumer.poll(context -> log.debug("Processing {}", context.getSingleRecord().offset()));

        // the registrar's last act is to count this down, so awaiting it is awaiting the thread - and it fails with
        // the latch's remaining count rather than a bare "expected 0 but was 1"
        awaitLatch(registrationLanded);

        // the outcome that matters: the loop kept turning after the concurrent registration, rather than dying on it
        awaitForOneLoopCycle();
        assertThat(parallelConsumer.isClosedOrFailed()).as("consumer still running").isFalse();

        parallelConsumer.closeDrainFirst(ofSeconds(defaultTimeoutSeconds));
    }

    /**
     * The same defect class as {@link #loopEndCallBackCanBeRegisteredFromAnotherThread()}, one subsystem over.
     * <p>
     * {@link bz.stub.parallelconsumer.state.WorkManager#addSuccessfulWorkListener
     * WorkManager.addSuccessfulWorkListener} can be called from any thread, while
     * {@code WorkManager.onSuccessResult} iterates the listeners on the control thread. Plain-list iteration breaks
     * when a registration lands mid-notify.
     * <p>
     * This registration used to be spelled {@code getSuccessfulWorkListeners().add(..)}, against a list handed out
     * whole by a Lombok {@code @Getter(PUBLIC)} - which is exactly why the bug stayed hidden. Searching for the field
     * name found the declaration and the iteration but never a mutation, so it read as dead code. The accessor is now
     * a real method, so the next such search finds its callers.
     */
    @Test
    @SneakyThrows
    void successfulWorkListenerCanBeRegisteredFromAnotherThread() {
        var wm = parallelConsumer.getWm();

        assertRegisteringFromAnotherThreadDoesNotStopTheConsumer(
                "off-thread-success-listener-registrar",
                parkTheNotifyingThread -> wm.addSuccessfulWorkListener(work -> parkTheNotifyingThread.run()));
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void testVoidPollMethod(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        int expected = 1;
        var msgCompleteBarrier = new CountDownLatch(expected);
        parallelConsumer.poll(context -> {
            log.debug("Processing test context...");
            var singleRecord = context.getSingleConsumerRecord();
            myRecordProcessingAction.apply(singleRecord);
            msgCompleteBarrier.countDown();
        });

        awaitLatch(msgCompleteBarrier);

        awaitForSomeLoopCycles(2);

        parallelConsumer.close();

        assertCommits(of(1));

        verify(myRecordProcessingAction, times(expected)).apply(any());

        // assert internal methods - shouldn't really need this as we already check the commit history above through the
        // spy, so can leave in for the old producer style
        if (commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)) {
            verify(producerSpy, atLeastOnce()).commitTransaction();
            verify(producerSpy, atLeastOnce()).sendOffsetsToTransaction(anyMap(), ArgumentMatchers.<ConsumerGroupMetadata>any());
        }
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    @Disabled
    public void processInKeyOrder(CommitMode commitMode) {
        setupParallelConsumerInstance(ParallelConsumerOptions.builder()
                .commitMode(commitMode)
                .ordering(KEY)
                .build());
        // created a new client above, so have to send the prime record again
        primeFirstRecord();

        // sanity check
        assertThat(parallelConsumer.getWm().getOptions().getOrdering()).isEqualTo(KEY);

        sendSecondRecord(consumerSpy);

        // 0,1 previously sent to partition 0
        // send two more to part 0 - 2,3,
        consumerSpy.addRecord(ktu.makeRecord("key-1", "v2")); // 2
        consumerSpy.addRecord(ktu.makeRecord("key-1", "v3")); // 3

        // and 3,4 to another partition
        consumerSpy.addRecord(ktu.makeRecord(1, "key-2", "v4")); // 4
        consumerSpy.addRecord(ktu.makeRecord(1, "key-3", "v5")); // 5
        consumerSpy.addRecord(ktu.makeRecord(1, "key-3", "v6")); // 6
        consumerSpy.addRecord(ktu.makeRecord(1, "key-3", "v7")); // 7
        consumerSpy.addRecord(ktu.makeRecord(1, "key-4", "v8")); // 8 - 8 must not get committed before 7 does

        // so 3 and 4 will block each other only
        // and 0,1,2,3 will all block each other (part 0)

        // if we're going to block 8 threads, need a big enough executor pool
        var msg0Lock = new CountDownLatch(1);
        var msg1Lock = new CountDownLatch(1);
        var msg2Lock = new CountDownLatch(1);
        var msg3Lock = new CountDownLatch(1);
        var msg4Lock = new CountDownLatch(1);
        var msg5Lock = new CountDownLatch(1);
        var msg6Lock = new CountDownLatch(1);
        var msg7Lock = new CountDownLatch(1);
        var msg8Lock = new CountDownLatch(1);

        final var processedState = new HashMap<Integer, Boolean>();
        for (Long msgIndex : Range.range(8)) {
            processedState.put(msgIndex.intValue(), false);
        }

        List<CountDownLatch> locks = of(msg0Lock, msg1Lock, msg2Lock, msg3Lock, msg4Lock, msg5Lock, msg6Lock, msg7Lock, msg8Lock);

        final List polled = new ArrayList();
        Mockito.doAnswer(x -> {
            ConsumerRecords o = (ConsumerRecords) x.callRealMethod();
            for (Object o1 : o) {
                polled.add(o1);
            }
            return o;
        }).when(consumerSpy).poll(any());

        parallelConsumer.poll((ignore) -> {
            int offset = (int) ignore.offset();
            CountDownLatch latchForMsg = locks.get(offset);
            try {
                log.debug("Started msg {} processing, locking on latch to simulate long process times...", offset);
                latchForMsg.await();
            } catch (InterruptedException e) {
                // ignore
            }
            log.debug("Finished msg {} processing after waking...", offset);
            processedState.put(offset, true);
        });

        // Finish these immediately
        msg6Lock.countDown();
        msg8Lock.countDown();

        // unlock 1
        log.debug("Unlocking 1...");
        msg1Lock.countDown();

        // wait cycles to make sure
        awaitForOneLoopCycle();

        //
        assertThat(polled).as("sanity check input data").hasSameSizeAs(locks);

        //
        assertThat(processedState.get(1))
                .as("blocked by 0 (1 shouldn't be run until 0 is complete, due to key order processing)")
                .isFalse();

        // make sure no offsets are committed
        assertCommits(of());

        // finish 2 process clear, but commit blocked by 0
        log.debug("Unlocking 2...");
        msg2Lock.countDown();
        awaitForSomeLoopCycles(2);
        assertThat(processedState.get(2)).isTrue();


        // still nothing - 0 blocks 1 and 2 (partition 0)
        verify(producerSpy, after(verificationWaitDelay).never()).commitTransaction(); // todo remove all wait nevers in favour of triggers as it slows down test
        awaitForOneLoopCycle();
        assertCommits(of());

        // finish 0 - releases pending (1,2)
        log.debug("Unlocking 0...");
        msg0Lock.countDown();

        // 0 gets comitted by itself
        awaitForCommitExact(0, 0);

        // make sure offset 0 is committed. 1 is now free to be processed (same key as 0), which as 2 was processed previously, frees up offset 2 to commit
        awaitForCommitExact(0, 2);
        assertCommits(of(0, 2));

        // unlock 3 - should get committed
        log.debug("Unlocking 3...");
        msg3Lock.countDown();

        // unlock 5 - commit blocked by 4, but should finish processing and clear 6 and then 7 (in 2 loops) for processing
        log.debug("Unlocking 5...");
        msg5Lock.countDown();
        awaitUntilTrue(() -> processedState.get(5));
        assertThat(processedState.get(5)).as("5 should processed").isTrue();

        awaitForCommitExact(0, 3);
        assertCommits(of(0, 2, 3));

        // unlock 4 - clears 5 for offset commit - 7 not processed yet (5,6,7 same key), 8 was never locked
        log.debug("Unlocking 4...");
        msg4Lock.countDown();

        // 6 should have been processed, unblocked by 5 (same key)
        awaitUntilTrue(() -> processedState.get(6));
        assertThat(processedState.get(6)).as("6 should processed").isTrue();

        // 5 and 6 finished, same key, coalesced commit to 6
        awaitForSomeLoopCycles(1);
        awaitForCommitExact(1, 6);
        assertCommits(of(0, 2, 3, 6));

        // unlock 7 (same key as 6), unblocks 8 for commit
        assertThat(processedState.get(7)).isFalse();
        assertThat(processedState.get(8)).isTrue();
        //
        releaseAndWait(locks, 7);
        awaitForCommitExact(1, 8);
        assertCommits(of(0, 2, 3, 6, 8));
    }

    /**
     * Check that when processing in key order, when work is not completed or taking a long time, that the commit system
     * doesn't break.
     */
    @SneakyThrows
    @Test
    void processInKeyOrderWorkNotReturnedDoesntBreakCommits() {
        ParallelConsumerOptions options = ParallelConsumerOptions.builder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .ordering(KEY)
                .build();
        setupParallelConsumerInstance(options);
        primeFirstRecord();

        sendSecondRecord(consumerSpy);

        // sanity check
        assertThat(parallelConsumer.getWm().getOptions().getOrdering()).isEqualTo(KEY);

        // 0,1 previously sent to partition 0
        // send one more, with same key of 1
        consumerSpy.addRecord(ktu.makeRecord("key-1", "v2")); // 2

        CountDownLatch msg1latch = new CountDownLatch(1);
        HashMap<Integer, CountDownLatch> locks = new HashMap<>();
        locks.put(1, msg1latch);

        CountDownLatch twoLoopLatch = new CountDownLatch(2);
        CountDownLatch fourLoopLatch = new CountDownLatch(4);
        parallelConsumer.addLoopEndCallBack(() -> {
            log.trace("Control loop cycle - {}, {}", twoLoopLatch.getCount(), fourLoopLatch.getCount());
            twoLoopLatch.countDown();
            fourLoopLatch.countDown();
        });

        var polled = new ArrayList<>();
        doAnswer(x -> {
            var records = (ConsumerRecords<String, String>) x.callRealMethod();
            for (var record : records) {
                polled.add(record);
            }
            return records;
        }).when(consumerSpy).poll(any());

        parallelConsumer.poll((ignore) -> {
            int offset = (int) ignore.offset();
            CountDownLatch countDownLatch = locks.get(offset);
            if (countDownLatch != null) try {
                countDownLatch.await();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            log.debug("Message offset {} processed...", offset);
        });

        await().untilAsserted(() ->
                assertThat(polled)
                        .as("sanity check - the records have been polled")
                        .hasSize(3)
        );

        //
        awaitLatch(twoLoopLatch);
        awaitForOneLoopCycle();

        //
        await().untilAsserted(() -> {
            try {
                // simpler way of making the bootstrap commit optional in the results, than adding the required barrier
                // locks to ensure it's existence, which has been tested else where
                assertCommits(of(0, 1), "Only 0 should be committed, as even though 2 is also finished, 1 should be " +
                        "blocking the partition");
            } catch (AssertionError e) {
                assertCommits(of(1), "Bootstrap commit is optional. See msg in code above");
            }
        });

        //
        msg1latch.countDown(); // release remaining processing lock

        //
        awaitLatch(fourLoopLatch); // wait for some loops

        // one more step
        awaitForOneLoopCycle();

        await().untilAsserted(() -> {
            //
            try { // see above
                assertCommits(of(0, 1, 3), "Remaining two records should be committed as a single offset");
            } catch (AssertionError e) {
                assertCommits(of(1, 3), "Bootstrap commit is optional. See msg in code above");
            }
        });
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    public void closeAfterSingleMessageShouldBeEventBasedFast(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        Duration timeBetweenCommits = parallelConsumer.getTimeBetweenCommits();

        var msgCompleteBarrier = new CountDownLatch(1);

        parallelConsumer.poll((ignore) -> {
            log.info("Message processed: {} - noop", ignore.offset());
            msgCompleteBarrier.countDown();
        });

        awaitLatch(msgCompleteBarrier);

        // allow for offset to be committed
        awaitForOneLoopCycle();

        parallelConsumer.requestCommitAsap();

        awaitForOneLoopCycle();

        await().untilAsserted(() ->
                assertCommits(of(1)));

        // close
        Duration durationOfCloseOperation = time(() -> {
            parallelConsumer.close();
        });

        //
        Duration expectedDurationOfClose = JavaUtils.max(timeBetweenCommits, ofSeconds(2)); // wait at least 1 second
        assertThat(durationOfCloseOperation).as("Should be fast").isLessThan(expectedDurationOfClose);
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    public void closeWithoutRunningShouldBeEventBasedFast(CommitMode commitMode) {
        setupParallelConsumerInstance(getBaseOptions(commitMode));

        parallelConsumer.closeDontDrainFirst();
    }

    @Test
    public void ensureLibraryCantBeUsedTwice() {
        parallelConsumer.poll(ignore -> {
        });
        assertThatIllegalStateException().isThrownBy(() -> {
            parallelConsumer.poll(ignore -> {
            });
        });
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    void consumeFlowDoesntRequireProducer(CommitMode commitMode) {
        setupClients();

        var optionsWithClients = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerSpy)
                .commitMode(commitMode)
                .build();

        if (commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)) {
            assertThatThrownBy(() -> parallelConsumer = initPollingAsyncConsumer(optionsWithClients))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContainingAll("Producer", "Transaction");
        } else {
            parallelConsumer = initPollingAsyncConsumer(optionsWithClients);
            attachLoopCounter(parallelConsumer);

            subscribeParallelConsumerAndMockConsumerTo(INPUT_TOPIC);
            setupData();

            parallelConsumer.poll((ignore) -> {
                log.debug("Test record processor - rec: {}", ignore);
            });

            //
            parallelConsumer.requestCommitAsap();
            awaitForCommitExact(1);

            parallelConsumer.closeDrainFirst();

            //
            assertCommits(of(1));
        }
    }

    @Test
    void optionsProduceMessageFlowRequiresProducer() {
        setupClients();

        var optionsWithClients = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerSpy)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        assertThatThrownBy(() -> parallelConsumer = initPollingAsyncConsumer(optionsWithClients))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Producer", "Transaction");
    }


    @Test
    void optionsGroupIdRequiredAndAutoCommitDisabled() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Deserializer<String> deserializer = Serdes.String().deserializer();
        var realConsumer = new KafkaConsumer<>(properties, deserializer, deserializer);

        var optionsBuilder = ParallelConsumerOptions.<String, String>builder()
                .consumer(realConsumer)
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS);
        var optionsWithClients = optionsBuilder
                .build();

        // fail
        assertThatThrownBy(() -> parallelConsumer = initPollingAsyncConsumer(optionsWithClients))
                .as("Should error on missing group id")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Consumer", "GroupId");

        // add missing group id, now auto commit should fail
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "dummy-group");
        optionsBuilder.consumer(new KafkaConsumer<>(properties, deserializer, deserializer));
        assertThat(catchThrowable(() -> parallelConsumer = initPollingAsyncConsumer(optionsBuilder.build())))
                .as("Should error on auto commit enabled by default")
                .isInstanceOf(ParallelConsumerException.class)
                .hasMessageContainingAll("auto", "commit", "disabled");

        // fail auto commit disabled
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        optionsBuilder.consumer(new KafkaConsumer<>(properties, deserializer, deserializer));
        assertThat(catchThrowable(() -> parallelConsumer = initPollingAsyncConsumer(optionsBuilder.build())))
                .as("Should error on auto commit enabled")
                .isInstanceOf(ParallelConsumerException.class)
                .hasMessageContainingAll("auto", "commit", "disabled");

        // set missing auto commit
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        optionsBuilder.consumer(new KafkaConsumer<>(properties, deserializer, deserializer));
        assertThatNoException().isThrownBy(() -> parallelConsumer = initPollingAsyncConsumer(optionsBuilder.build()));
    }


    @Test
    void cantUseProduceFlowWithWrongOptions() throws InterruptedException {
        setupClients();

        // forget to supply producer
        var optionsWithClients = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerSpy)
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build();

        setupParallelConsumerInstance(optionsWithClients);

        subscribeParallelConsumerAndMockConsumerTo(INPUT_TOPIC);

        setupData();

        var parallel = initPollingAsyncConsumer(optionsWithClients);

        assertThatThrownBy(() -> parallel.pollAndProduce((record) ->
                new ProducerRecord<>(INPUT_TOPIC, "hi there")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Producer", "options");
    }

    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    void produceMessageFlow(CommitMode commitMode) {
        setupParallelConsumerInstance(commitMode);

        parallelConsumer.pollAndProduce((ignore) -> new ProducerRecord<>("Hello", "there"));

        // let it process
        awaitForSomeLoopCycles(2);

        parallelConsumer.requestCommitAsap();

        //
        await().untilAsserted(() ->
                assertCommits(of(1)));

        parallelConsumer.closeDrainFirst();


        assertThat(producerSpy.history()).hasSize(1);
    }

    /**
     * Explicit check for situation where thread size is much larger than key set size.
     * <p>
     * See <a href="https://github.com/confluentinc/parallel-consumer/issues/433">Different computational results
     * obtained with different max concurrency configurations for the same parallel consumer confluentinc#433</a>
     */
    @Test
    void lessKeysThanThreads() {
        setupParallelConsumerInstance(ParallelConsumerOptions.<String, String>builder()
                .ordering(KEY)
                // use many more threads than keys
                .maxConcurrency(100)
                .build());

        // use a small set of keys, over a large set of records
        final int keySetSize = 4;
        var keys = Range.range(keySetSize).listAsIntegers();
        final int total = 100_000;
        log.debug("Generating {} records against {} keys...", total, keySetSize);
        var records = ktu.generateRecords(keys, total);
        records.entrySet().forEach(x -> log.debug("Key {} has {} records", x.getKey(), x.getValue().size()));
        log.debug("Sending...");
        ktu.send(consumerSpy, records);

        var bar = ProgressBarUtils.getNewMessagesBar(log, total);

        // run
        log.debug("Consuming...");
        var results = new ConcurrentHashMap<String, Queue<PollContext<String, String>>>();
        AtomicLong counter = new AtomicLong();
        parallelConsumer.poll(recordContexts -> {
            counter.incrementAndGet();
            bar.step();
            log.trace("Consumed {}", recordContexts);
            results.computeIfAbsent(recordContexts.key(), ignore -> new ConcurrentLinkedQueue<>())
                    .add(recordContexts);
        });

        // count how many we've received so far
        await().atMost(5, MINUTES)
                .untilAsserted(() ->
                        assertThat(counter.get()).isEqualTo(total));

        parallelConsumer.closeDrainFirst();
        bar.close();

        // check ordering is exact - remove sequenceSize?
        var sequenceSize = Math.max(total / keySetSize, 1); // if we have more keys than records, then we'll have a sequence size of 1, so round up
        log.debug("Testing...");
        checkExactOrdering(results, records);
    }

}

