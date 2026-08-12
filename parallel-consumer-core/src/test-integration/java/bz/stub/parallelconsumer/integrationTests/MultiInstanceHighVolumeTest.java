package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ProgressBarUtils;
import bz.stub.parallelconsumer.internal.utils.StringUtils;
import bz.stub.parallelconsumer.internal.utils.TrimListRepresentation;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.util.Lists.list;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
@Tag("performance")
class MultiInstanceHighVolumeTest extends BrokerIntegrationTest<String, String> {

    public List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
    public List<String> producedKeysAcknowledged = Collections.synchronizedList(new ArrayList<>());
    public AtomicInteger processedCount = new AtomicInteger(0);
    public AtomicInteger producedCount = new AtomicInteger(0);

    int maxPoll = 500; // 500 is the kafka default

    CommitMode commitMode = CommitMode.PERIODIC_CONSUMER_SYNC;
    ProcessingOrder order = ProcessingOrder.KEY;


    static final int GATING_VOLUME = 3_000_000;

    /**
     * The volume this test runs at. Unlike the other recovered sites, the {@code //10_000_000} that
     * sat above this line <em>was</em> live - at {@code 04cd4d81} (2020-12-14) - and was commented
     * out at {@code ad3636a5} (2021-07-02) when the value was reduced. Git holds that history, so
     * the comment was residue and stays deleted.
     * <p>
     * What was not residue is the reason for the reduction. The wait below was hard-coded at 60
     * seconds, so the higher volume could not be met regardless of whether the run was healthy -
     * the volume was lowered to fit a deadline rather than because 10M was wrong. With the deadline
     * scaling, the original volume is reachable again:
     *
     * <pre>./mvnw verify -Pci -Dmultiinstance.messages=10000000</pre>
     */
    private static int volume() {
        return Integer.getInteger("multiinstance.messages", GATING_VOLUME);
    }

    /** The deadline this test has always had at its own volume. */
    private static final Duration GATING_CEILING = ofSeconds(60);

    private static Duration ceilingFor(int messages) {
        return completionCeiling(messages, GATING_VOLUME, GATING_CEILING);
    }

    // todo multi commit mode, multi partition count, multi instance count? 2,3,10,100? more instances than partitions, more partitions than instances
    @SneakyThrows
    @Test
    void multiInstance() {
        numPartitions = 12;
        String inputTopicName = setupTopic(this.getClass().getSimpleName() + "-input");

        int expectedMessageCount = volume();
        log.info("Producing {} messages before starting test", expectedMessageCount);

        List<String> expectedKeys = getKcu().produceMessages(inputTopicName, expectedMessageCount);

        // setup
        ParallelEoSStreamProcessor<String, String> pcOne = buildPc(inputTopicName, maxPoll, order, commitMode);
        ParallelEoSStreamProcessor<String, String> pcTwo = buildPc(inputTopicName, maxPoll, order, commitMode);
        ParallelEoSStreamProcessor<String, String> pcThree = buildPc(inputTopicName, maxPoll, order, commitMode);

        // run
        var consumedByOne = Collections.synchronizedList(new ArrayList<ConsumerRecord<?, ?>>());
        var consumedByTwo = Collections.synchronizedList(new ArrayList<ConsumerRecord<?, ?>>());
        var consumedByThree = Collections.synchronizedList(new ArrayList<ConsumerRecord<?, ?>>());
        List<ProgressBar> bars = list();
        bars.add(run(expectedMessageCount / 3, pcOne, consumedByOne));
        bars.add(run(expectedMessageCount / 3, pcTwo, consumedByTwo));
        bars.add(run(expectedMessageCount / 3, pcThree, consumedByThree));

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = StringUtils.msg("All keys sent to input-topic should be processed and produced, within time " +
                        "(expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        try {
            waitAtMost(ceilingFor(expectedMessageCount))
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    // .failFast( () -> pcThree.getFailureCause(), () -> pcThree.isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .failFast("PC died - check logs", () -> pcThree.isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .alias(failureMessage)
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").hasSameSizeAs(expectedKeys);
//                        all.assertThat(new ArrayList<>(producedKeysAcknowledged)).as("all consumed are produced ok ").hasSameSizeAs(expectedKeys);
                        all.assertAll();
                    });
        } catch (ConditionTimeoutException e) {
            fail(failureMessage + "\n" + e.getMessage());
        }

        assertThat(processedCount.get())
                .as("messages processed and produced by parallel-consumer should be equal")
                .isEqualTo(expectedMessageCount);

        // sanity
        assertThat(expectedMessageCount).isEqualTo(processedCount.get());

        bars.forEach(ProgressBar::close);
    }

    private ParallelEoSStreamProcessor<String, String> buildPc(String inputTopicName, int maxPoll, ProcessingOrder order, CommitMode commitMode) {
        var pc = getKcu().buildPc(order, commitMode, maxPoll);
        pc.subscribe(of(inputTopicName));
        return pc;
    }

    Integer barId = 0;

    private ProgressBar run(final int expectedMessageCount, final ParallelEoSStreamProcessor<String, String> pc, List<ConsumerRecord<?, ?>> consumed) {
        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);
        bar.setExtraMessage("#" + barId);
        pc.setMyId(Optional.of("id: " + barId));
        barId++;
        pc.poll(record -> {
                    processRecord(bar, record.getSingleConsumerRecord(), consumed);
                }
//                , consumeProduceResult -> {
//                    callBack(consumeProduceResult);
//                }
        );
        return bar;
    }

    @SneakyThrows
    private void processRecord(final ProgressBar bar,
                               final ConsumerRecord<String, String> record,
                               List<ConsumerRecord<?, ?>> consumed) {
//        try {
        // 1/5 chance of taking a long time
//        int chance = 10;
//        int dice = RandomUtils.nextInt(0, chance);
//        if (dice == 0) {
//            Thread.sleep(100);
//        } else {
//            Thread.sleep(RandomUtils.nextInt(3, 20));
//        }
        bar.stepBy(1);
        consumedKeys.add(record.key());
        processedCount.incrementAndGet();
        consumed.add(record);
//        return new ProducerRecord<>(outputName, record.key(), "data");
    }

}
