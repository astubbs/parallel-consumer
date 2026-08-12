package bz.stub.parallelconsumer.integrationTests;
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ProgressBarUtils;
import bz.stub.parallelconsumer.internal.utils.Range;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static bz.stub.parallelconsumer.internal.utils.GeneralTestUtils.time;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static me.tongfei.progressbar.ProgressBar.wrap;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@Slf4j
public class LoadTest extends DbTest {

    /**
     * The gating volume. Untagged, so this runs in the gating integration lane, and it is a listed
     * member of the load-tightness flake family at exactly this volume
     * (docs/inflight/test-load-tightness-flakes.md). Classify before raising it - that family is
     * where the confluentinc#857 deadlock was hiding.
     */
    static final int GATING_TOTAL = 4_000;

    /**
     * The range this test was actually run at by hand. Authored in {@code af1fa5de} (2020-06-17) as
     * four commented-out alternatives beside the live value, and deleted in {@code e67d8b89} on the
     * grounds that they were dead - but none of them was ever live, so the ladder was the only
     * record of which volumes anyone had exercised. Reachable now without editing the file:
     *
     * <pre>./mvnw verify -Pci -Dload.total=400000</pre>
     *
     * The same commit also parked {@code 8}, which is not a volume rung at all - it is the
     * fast-iteration setting for working on this harness itself, and it was deleted as though it
     * were one of the volumes. It is {@code -Dload.total=8}, and it works now: as parked it could
     * never have run, because the key range is derived as {@code volume / 100} and eight records
     * gave zero keys. {@link #setupTestData} floors that at one.
     * <p>
     * Property convention is {@code <concern>.<knob>}, matching the existing {@code chaos.seed} and
     * {@code ambient.probe}.
     */
    static final int[] RECOVERED_VOLUMES = {40_000, 80_000, 400_000};

    /**
     * The volume the high-volume case runs at when the performance lane selects it. The smallest
     * recovered rung, so the lane gets a genuine high-volume run without a multi-hour default.
     */
    static final int HIGH_VOLUME_TOTAL = RECOVERED_VOLUMES[0];

    static int total = Integer.getInteger("load.total", GATING_TOTAL);

    /** The deadline the gating volume has always had. */
    private static final Duration GATING_CEILING = ofSeconds(60);

    private static Duration ceilingFor(int volume) {
        return completionCeiling(volume, GATING_TOTAL, GATING_CEILING);
    }

    @SneakyThrows
    public void setupTestData(int volume) {
        setupTopic();

        // One key per hundred records, but never zero. Below 100 records the original `volume / 100`
        // produced an empty key list and publishMessages threw IndexOutOfBounds on the first send -
        // which is why the parked `8` could not have run as written, in af1fa5de or since.
        publishMessages(Math.max(1, volume / 100), volume, topic);
    }

    @SneakyThrows
    @Test
    void timedNormalKafkaConsumerTest() {
        setupTestData(total);

        // subscribe in advance, it can be a few seconds
        getKcu().getConsumer().subscribe(UniLists.of(topic));

        readRecordsPlainConsumer(total, topic);
    }

    @SneakyThrows
    @Test
    void asyncConsumeAndProcess() {
        asyncConsumeAndProcess(total);
    }

    /**
     * The same scenario at one of the {@link #RECOVERED_VOLUMES}. Tagged, so the gating lane never
     * runs it and the default excluded groups keep it out; select it with
     * {@code -Dincluded.groups=performance}, and reach the top of the recovered range by adding
     * {@code -Dload.total=400000}.
     */
    @SneakyThrows
    @Test
    @Tag("performance")
    void asyncConsumeAndProcessAtVolume() {
        asyncConsumeAndProcess(Integer.getInteger("load.total", HIGH_VOLUME_TOTAL));
    }

    @SneakyThrows
    private void asyncConsumeAndProcess(int volume) {
        setupTestData(volume);

        KafkaConsumer<String, String> newConsumer = getKcu().createNewConsumer();
        //
        boolean tx = true;
        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .producer(getKcu().createNewProducer(tx))
                .consumer(newConsumer)
                .maxConcurrency(3)
                .build();

        ParallelEoSStreamProcessor<String, String> async = new ParallelEoSStreamProcessor<>(options);
        async.subscribe(Pattern.compile(topic));

        AtomicInteger msgCount = new AtomicInteger(0);

        ProgressBar pb = ProgressBarUtils.getNewMessagesBar(log, volume);

        try (pb) {
            async.poll(r -> {
                // message processing function
                sleepABit();
                // db isn interesting but not a great performance test, as the db quickly becomes the bottleneck, need to test against a db cluster that can scale better
                // save to db
//                savePayload(r.key(), r.value());
                //
                msgCount.getAndIncrement();
            });

            // keep checking how many message's we've processed
            await().atMost(ceilingFor(volume)).until(() -> {
                // log.debug("msg count: {}", msgCount.get());
                pb.stepTo(msgCount.get());
                return msgCount.get() >= volume;
            });
        }
        async.close();
    }

    private void sleepABit() {
        int simulatedCPUMessageProcessingDelay = nextInt(0, 5); // random delay between 0,5
        try {
            Thread.sleep(simulatedCPUMessageProcessingDelay); // simulate json parsing overhead and network calls
        } catch (Exception ignore) {
        }
    }

    private void readRecordsPlainConsumer(int total, String topic) {
        // read
        log.info("Starting to read back");
        final List<ConsumerRecord<String, String>> allRecords = Lists.newArrayList();
        AtomicInteger count = new AtomicInteger();
        time(() -> {
            ProgressBar pb = ProgressBarUtils.getNewMessagesBar(log, total);

            Executors.newCachedThreadPool().submit(() -> {
                while (allRecords.size() < total) {
                    ConsumerRecords<String, String> poll = getKcu().getConsumer().poll(ofMillis(500));
                    log.info("Polled batch of {} messages", poll.count());

                    //save
                    Iterable<ConsumerRecord<String, String>> records = poll.records(topic);
                    records.forEach(x -> {
                        // log.trace(x.toString());
                        sleepABit();
                        // db isn interesting but not a great performance test, as the db quickly becomes the bottleneck, need to test against a db cluster that can scale better
//                        savePayload(x.key(), x.value());
                        pb.step();
                        // log.debug(testDataEbean.toString());
                    });

                    //
                    ArrayList<ConsumerRecord<String, String>> c = Lists.newArrayList(records);
                    allRecords.addAll(c);
                    count.getAndAdd(c.size());
                }
            });

            try (pb) {
                await().atMost(ceilingFor(total)).untilAsserted(() -> {
                    assertThat(count).hasValue(total);
                });
            }

        });

        assertThat(allRecords).hasSize(total);
    }

    @SneakyThrows
    private void publishMessages(int keyRange, int total, String topic) {

        // produce data
        var keys = Range.listOfIntegers(keyRange);
        var integers = Lists.newArrayList(IntStream.range(0, total).iterator());

        // publish
        var futureMetadataResultsFromPublishing = new LinkedList<Future<RecordMetadata>>();
        log.info("Start publishing...");
        time(() -> {
            for (var x : wrap(integers, "Publishing async")) {
                String key = keys.get(RandomUtils.nextInt(0, keys.size())).toString();
                int messageSizeInBytes = 500;
                String value = RandomStringUtils.randomAlphabetic(messageSizeInBytes);
                var producerRecord = new ProducerRecord<>(topic, key, value);
                try {
                    var meta = getKcu().getProducer().send(producerRecord);
                    futureMetadataResultsFromPublishing.add(meta);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // joining
        Set<Integer> usedPartitions = new HashSet<>();
        for (var meta : wrap(futureMetadataResultsFromPublishing, "Joining")) {
            RecordMetadata recordMetadata = meta.get();
            int partition = recordMetadata.partition();
            usedPartitions.add(partition);
        }
        // has a certain chance of passing, if number of messages is ~large compared to numPartitions
        if (numPartitions > 100_000) {
            assertThat(usedPartitions.stream().distinct()).as("All partitions are made use of").hasSize(numPartitions);
        }
    }

}
