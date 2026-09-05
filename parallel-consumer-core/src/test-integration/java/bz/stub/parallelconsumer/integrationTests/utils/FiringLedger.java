package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The parent's view of the fleet, read from the broker: every child's firings on the BROKER's clock, and every
 * child's end-of-run conservation record (KTD8, the fleet-conservation decision). One consumer on a daemon
 * thread tails the output topic and the ledger topic from their beginnings and files what it reads; every count
 * here is over log-append timestamps, so a child's clock offset is invisible by construction.
 * <p>
 * <b>Both topics have ONE partition, and the ledger relies on it.</b> {@link #brokerNow()} produces a marker
 * record and reads back the timestamp the broker stamped on it - the fleet's one clock - and
 * {@link #awaitBrokerTimePast} then waits until the consumer has read past that marker's offset. With a single
 * partition the marker is a total-order fence: every firing appended before it has been read, so any window
 * ending at or before the marker's timestamp is final. Two partitions would break that ordering.
 * <p>
 * <b>Windows are anchored to observed group stability, not launch.</b> The lane waits for the admin client to
 * report the group stable, then takes {@link #brokerNow()} as the anchor; the inter-rung barrier
 * ({@link #awaitTailQuiet}) waits for the departed members' tail on the same clock before the next window opens.
 * <p>
 * The static {@link #countIn} is the anchored-window primitive shared with the in-process navigator lane and demo
 * through {@code BrokerIntegrationTest#countIn} - insertion order is irrelevant, only the timestamps count.
 *
 * @author Antony Stubbs
 * @see ChildPcProcess
 * @see ChildLedgerRecord
 */
@Slf4j
public final class FiringLedger implements AutoCloseable {

    /** The key of the parent's own broker-time marker records; never an instance id. */
    public static final String MARKER_KEY = "_ledger-marker";

    /** A record on the output topic: one dispatch by one child, on the broker's clock. */
    @Value
    public static class Firing {
        String instanceId;
        /** Log-append time - the broker's clock. */
        Instant brokerTime;
        /** The child's own (possibly offset) clock at dispatch, as it reported it. */
        Instant childClock;
        long offset;
    }

    /** The aggregated fleet ledger: every child's record, with the fleet identity's two sides per resource. */
    @Value
    public static class FleetLedger {
        List<ChildLedgerRecord> records;

        public Set<String> instances() {
            return records.stream().map(ChildLedgerRecord::getInstanceId).collect(Collectors.toCollection(TreeSet::new));
        }

        public List<ChildLedgerRecord> forResource(String resource) {
            return records.stream().filter(r -> r.getResource().equals(resource)).collect(Collectors.toList());
        }

        public long mintedTotal(String resource) {
            return forResource(resource).stream().mapToLong(ChildLedgerRecord::getMinted).sum();
        }

        public long overdraftTotal(String resource) {
            return forResource(resource).stream().mapToLong(ChildLedgerRecord::getOverdraft).sum();
        }

        public double sharesSummedTotal(String resource) {
            return forResource(resource).stream().mapToDouble(ChildLedgerRecord::getSharesSummed).sum();
        }

        /** Every child's own identity closes (R10's per-instance half, read back from the broker). */
        public void assertEachIdentityBalances() {
            for (ChildLedgerRecord record : records) {
                assertThat(record.identityBalances())
                        .as("conservation identity of %s/%s: minted %s + overdraft %s == spent %s + expired %s "
                                        + "+ outstanding %s", record.getInstanceId(), record.getResource(),
                                record.getMinted(), record.getOverdraft(), record.getSpent(), record.getExpired(),
                                record.getOutstanding())
                        .isTrue();
            }
        }
    }

    private final String outputTopic;
    private final String ledgerTopic;
    private final KafkaProducer<String, String> markerProducer;
    private final Thread tailer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<String, ConcurrentLinkedQueue<Firing>> firingsByInstance = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<ChildLedgerRecord> ledgerRecords = new ConcurrentLinkedQueue<>();
    /** The highest output-topic offset the tailer has read, -1 before any. */
    private final AtomicLong outputHighWater = new AtomicLong(-1);
    private final AtomicReference<Throwable> tailerFailure = new AtomicReference<>();

    public FiringLedger(String bootstrapServers, String outputTopic, String ledgerTopic) {
        this.outputTopic = outputTopic;
        this.ledgerTopic = ledgerTopic;
        this.markerProducer = new KafkaProducer<>(producerProperties(bootstrapServers));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties(bootstrapServers));
        this.tailer = new Thread(() -> tail(consumer), "firing-ledger-tailer");
        tailer.setDaemon(true);
        tailer.start();
    }

    // ------------------------------------------------------------------
    // The broker's clock
    // ------------------------------------------------------------------

    /**
     * The broker's current time, as the timestamp it stamps on a marker record produced now. Also the fence
     * {@link #awaitBrokerTimePast} waits behind.
     */
    public Instant brokerNow() {
        return marker().brokerTime;
    }

    private Marker marker() {
        try {
            RecordMetadata metadata = markerProducer
                    .send(new ProducerRecord<>(outputTopic, MARKER_KEY, "marker")).get(30, TimeUnit.SECONDS);
            return new Marker(Instant.ofEpochMilli(metadata.timestamp()), metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted producing a broker-time marker", e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("could not produce a broker-time marker to " + outputTopic, e);
        }
    }

    private static final class Marker {
        final Instant brokerTime;
        final long offset;

        Marker(Instant brokerTime, long offset) {
            this.brokerTime = brokerTime;
            this.offset = offset;
        }
    }

    /**
     * Blocks until the broker's clock has passed {@code instant} AND the tailer has read every record appended
     * before that moment. After it returns, any window ending at or before {@code instant} is final.
     *
     * @return the broker time at which the fence landed
     */
    public Instant awaitBrokerTimePast(Instant instant, Duration budget) {
        Instant deadline = Instant.now().plus(budget);
        while (true) {
            Marker marker = marker();
            if (!marker.brokerTime.isBefore(instant)) {
                awaitReadPast(marker.offset, Duration.between(Instant.now(), deadline));
                return marker.brokerTime;
            }
            if (Instant.now().isAfter(deadline)) {
                throw new IllegalStateException("broker time " + marker.brokerTime + " did not pass " + instant
                        + " within " + budget);
            }
            sleepQuietly(Duration.between(marker.brokerTime, instant).toMillis() + 20);
        }
    }

    /** Produces a marker and waits until the tailer has read past it, so every earlier firing is filed. */
    public Instant anchorNow() {
        Marker marker = marker();
        awaitReadPast(marker.offset, Duration.ofSeconds(60));
        return marker.brokerTime;
    }

    private void awaitReadPast(long offset, Duration budget) {
        Awaitility.await("the ledger tailer reads past output offset " + offset)
                .atMost(budget.isNegative() ? Duration.ZERO : budget)
                .pollInterval(Duration.ofMillis(20))
                .failFast("the ledger tailer died", () -> tailerFailure.get() != null)
                .until(() -> outputHighWater.get() >= offset);
    }

    // ------------------------------------------------------------------
    // Counting on the broker's clock
    // ------------------------------------------------------------------

    /** Firings by one child in {@code [start, end)} of broker time. */
    public long countIn(String instanceId, Instant start, Instant end) {
        return countIn(brokerTimesOf(instanceId), start, end);
    }

    /** Firings by every child in {@code [start, end)} of broker time. */
    public long countAll(Instant start, Instant end) {
        long total = 0;
        for (String instanceId : firingsByInstance.keySet()) {
            total += countIn(instanceId, start, end);
        }
        return total;
    }

    /**
     * Count of timestamps in {@code [start, end)} - the anchored-window measurement primitive shared by the
     * navigator lanes and demos. Insertion order is irrelevant, only the timestamps count.
     */
    public static long countIn(Collection<Instant> firings, Instant start, Instant end) {
        return firings.stream().filter(firing -> !firing.isBefore(start) && firing.isBefore(end)).count();
    }

    /** Every firing of one child so far, in the order read. */
    public List<Firing> firingsOf(String instanceId) {
        return new ArrayList<>(firingsByInstance.getOrDefault(instanceId, new ConcurrentLinkedQueue<>()));
    }

    /** The broker timestamps of one child's firings so far. */
    public List<Instant> brokerTimesOf(String instanceId) {
        return firingsOf(instanceId).stream().map(Firing::getBrokerTime).collect(Collectors.toList());
    }

    /** Every instance id that has fired at least once. */
    public Set<String> instancesSeen() {
        return new TreeSet<>(firingsByInstance.keySet());
    }

    /** The latest broker timestamp among one child's firings, if any. */
    public Optional<Instant> latestFiringOf(String instanceId) {
        return brokerTimesOf(instanceId).stream().max(Instant::compareTo);
    }

    /**
     * The earliest firing of one child at or after {@code floor} on the broker's clock - awaited, so a window's
     * anchor is a real observed event (the in-process lane's {@code awaitFirstFiringAtOrAfter}).
     */
    public Instant awaitFiringAtOrAfter(String instanceId, Instant floor, Duration budget) {
        Awaitility.await("a firing by " + instanceId + " at or after " + floor)
                .atMost(budget)
                .pollInterval(Duration.ofMillis(100))
                .failFast("the ledger tailer died", () -> tailerFailure.get() != null)
                .until(() -> brokerTimesOf(instanceId).stream().anyMatch(firing -> !firing.isBefore(floor)));
        return brokerTimesOf(instanceId).stream().filter(firing -> !firing.isBefore(floor)).min(Instant::compareTo)
                .orElseThrow(IllegalStateException::new);
    }

    /**
     * The inter-rung barrier's tail wait (KTD8): blocks until, on the broker's clock, {@code settle} has passed
     * since the latest firing by any of the {@code departed} instances - and the tailer has read everything up to
     * that moment - so the previous rung's tail cannot land inside the next rung's window.
     *
     * @return the broker time at which the tail was declared quiet; open the next window at or after it
     */
    public Instant awaitTailQuiet(Set<String> departed, Duration settle, Duration budget) {
        Instant deadline = Instant.now().plus(budget);
        while (true) {
            Instant now = anchorNow();
            Optional<Instant> latestTail = departed.stream()
                    .map(this::latestFiringOf)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .max(Instant::compareTo);
            Instant quietFrom = latestTail.map(tail -> tail.plus(settle)).orElse(now);
            if (!now.isBefore(quietFrom)) {
                log.info("rung barrier: departed {} tail quiet - latest tail firing {}, settle {}, broker now {}",
                        departed, latestTail.map(Instant::toString).orElse("none"), settle, now);
                return now;
            }
            if (Instant.now().isAfter(deadline)) {
                throw new IllegalStateException("the tail of departed " + departed + " did not go quiet for "
                        + settle + " within " + budget + " - latest tail firing " + latestTail);
            }
            sleepQuietly(Duration.between(now, quietFrom).toMillis() + 20);
        }
    }

    // ------------------------------------------------------------------
    // The fleet ledger
    // ------------------------------------------------------------------

    /** Waits until every named instance's end-of-run record has arrived (at least one record per instance). */
    public FleetLedger awaitLedgerRecords(Set<String> instances, Duration budget) {
        Awaitility.await("end-of-run ledger records from " + instances)
                .atMost(budget)
                .pollInterval(Duration.ofMillis(100))
                .failFast("the ledger tailer died", () -> tailerFailure.get() != null)
                .untilAsserted(() -> assertThat(fleetLedger().instances())
                        .as("instances whose ledger record has arrived")
                        .containsAll(instances));
        return fleetLedger();
    }

    /** Every ledger record read so far. */
    public FleetLedger fleetLedger() {
        return new FleetLedger(new ArrayList<>(ledgerRecords));
    }

    // ------------------------------------------------------------------
    // The tailer
    // ------------------------------------------------------------------

    private void tail(KafkaConsumer<String, String> consumer) {
        try {
            List<TopicPartition> partitions = UniLists.of(new TopicPartition(outputTopic, 0),
                    new TopicPartition(ledgerTopic, 0));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    file(record);
                }
            }
        } catch (Throwable t) {
            if (!closed.get()) {
                tailerFailure.set(t);
                log.error("the firing ledger tailer died", t);
            }
        } finally {
            consumer.close();
        }
    }

    private void file(ConsumerRecord<String, String> record) {
        if (record.topic().equals(ledgerTopic)) {
            ledgerRecords.add(ChildLedgerRecord.parse(record.value()));
            return;
        }
        if (!MARKER_KEY.equals(record.key())) {
            firingsByInstance.computeIfAbsent(record.key(), ignored -> new ConcurrentLinkedQueue<>())
                    .add(new Firing(record.key(), Instant.ofEpochMilli(record.timestamp()),
                            childClockOf(record.value()), record.offset()));
        }
        outputHighWater.accumulateAndGet(record.offset(), Math::max);
    }

    /** The child's own clock reading, from the {@code clock=<millis>} field {@link ChildPcMain} writes. */
    private static Instant childClockOf(String value) {
        int at = value.indexOf("clock=");
        if (at < 0) {
            throw new IllegalArgumentException("output record without a clock field: '" + value + "'");
        }
        return Instant.ofEpochMilli(Long.parseLong(value.substring(at + "clock=".length()).trim()));
    }

    @Override
    public void close() {
        closed.set(true);
        try {
            tailer.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        markerProducer.close(Duration.ofSeconds(10));
    }

    private static void sleepQuietly(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(Math.max(millis, 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted waiting on the broker's clock", e);
        }
    }

    private static Properties consumerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "firing-ledger-tailer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "firing-ledger-marker");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        return props;
    }
}
