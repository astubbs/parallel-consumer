package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.fluent.ParkedRecord;
import bz.stub.parallelconsumer.fluent.ParkedView;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * The README quickstart against a real broker - the single exception to the plan's no-new-broker-tests rule
 * (R36, Success Criteria).
 *
 * <h2>What only a broker can prove</h2>
 * The sandbox run in the example module proves the definition's behaviour: routes, types, outcomes, the parked set.
 * It cannot prove the two things that are the broker's own. <b>Properties to clients</b>: a definition is given
 * connection properties and nothing else, so whether it can actually build a consumer, join a group and be assigned
 * partitions is only answered here. <b>The commit metadata</b>: park in place is only as good as the offset map
 * that carries it, and the claim that offsets past a parked record commit is a claim about what a <em>second</em>
 * instance is given when it joins the same group afterwards - which is what the second half of this test does.
 *
 * <h2>The shape of the run</h2>
 * The quickstart's shape, with the example module's types replaced by what core can already read: a JSON route
 * without a class (the payload as a map of field names to values) and a string route. One scan on the string route
 * is poison and can never be processed; every other record succeeds or is filtered. After the close, a second
 * definition joins the same group and is given exactly the poison record and nothing else.
 *
 * @see bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition
 */
@Slf4j
@Timeout(300)
class FluentQuickstartIT extends BrokerIntegrationTest<String, String> {

    private static final int RECORDS_PER_TOPIC = 10;

    /**
     * The offset on the scans topic whose record can never be processed. Nothing about the offset matters except
     * that records exist on both sides of it, so that "offsets past it commit" has something to be about.
     */
    private static final int POISON_OFFSET = 3;

    private static final String POISON_PREFIX = "POISON";

    private static final String RETURNED = "RETURNED";

    // Named in @Test, before anything reads them. Empty rather than uninitialised so that the null checker has an
    // answer, and so that a helper called out of order names an empty topic rather than throwing on a null.
    private String ordersTopic = "";

    private String scansTopic = "";

    /**
     * Every record's own key, so that no record queues behind another. Under key ordering a parked record holds its
     * key (R11), and this test is about what the partition commits rather than about what a shard holds.
     */
    private static String keyFor(String topic, int index) {
        return topic + '-' + index;
    }

    /**
     * Connection properties and nothing else - no client is constructed by this test, which is R1 and the first
     * thing the README's quickstart claims.
     * <p>
     * Built here rather than through {@code KafkaClientUtils#setupConsumerProps}, because those name the
     * deserialisers and a fluent definition refuses that: each route applies its own, so a deserialiser in the
     * connection properties would silently never be used.
     */
    private Properties connectionProperties(String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    @Test
    void theQuickstartConsumesRealRecordsParksThePoisonOneAndCommitsPastIt() {
        ordersTopic = setupTopic("quickstart-orders");
        scansTopic = setupTopic("quickstart-scans");
        produceRecords();

        String group = "quickstart-" + ordersTopic;
        var ordersSeen = new ConcurrentLinkedQueue<String>();
        var scansSeen = new ConcurrentLinkedQueue<String>();

        ParallelConsumerInstance handle = quickstartShaped(group, ordersSeen, scansSeen).start();
        try {
            // Every order, and every scan except the poison one. Both routes ran, each over its own topic's
            // records decoded into its own type, which is the typed-routes half of the quickstart.
            await().alias("both routes to process everything they can")
                    .atMost(Duration.ofMinutes(1))
                    .untilAsserted(() -> {
                        assertThat(ordersSeen).hasSize(RECORDS_PER_TOPIC - 1);   // one order is RETURNED, so filtered
                        assertThat(scansSeen).hasSize(RECORDS_PER_TOPIC - 1);    // one scan is poison, so parked
                    });

            await().alias("the poison scan to run out of attempts and park")
                    .atMost(Duration.ofMinutes(1))
                    .until(() -> handle.topic(scansTopic).parked().count() == 1);

            assertParkedRecord(handle);
        } finally {
            handle.close();
        }

        assertOnlyTheParkedRecordIsRedelivered(group);
    }

    /**
     * The quickstart's definition, in the types core can read without another dependency: JSON as a map on one
     * route, strings on the other. Everything else - the filtered outcome, the retry limit and delay, the park
     * observer - is what {@code FluentQuickstartApp} declares.
     */
    private ParallelConsumerDefinition quickstartShaped(String group,
                                                        ConcurrentLinkedQueue<String> ordersSeen,
                                                        ConcurrentLinkedQueue<String> scansSeen) {
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties(group));

        pc.json(ordersTopic)
                .process(context -> {
                    Map<String, Object> order = context.value();
                    if (RETURNED.equals(order.get("status"))) {
                        return Outcome.filtered();
                    }
                    ordersSeen.add(String.valueOf(order.get("orderId")));
                    return Outcome.succeeded();
                });

        pc.string(scansTopic)
                .retryLimit(2)
                .retryDelay(Duration.ofMillis(200))
                .onParked((record, failure, attempts) ->
                        log.warn("Parked scan at offset {} after {} attempts", record.offset(), attempts, failure))
                .process(context -> {
                    if (context.value().startsWith(POISON_PREFIX)) {
                        throw new IllegalStateException("the parcel-tracking service cannot read " + context.value());
                    }
                    scansSeen.add(context.value());
                    return Outcome.succeeded();
                });

        return pc;
    }

    /**
     * The parked set over real records: the offset it names is the one that was produced poison, and its attempt
     * count is what the route's retry limit allows and no more.
     */
    private void assertParkedRecord(ParallelConsumerInstance handle) {
        ParkedView parked = handle.topic(scansTopic).parked();
        assertThat(parked.count()).isEqualTo(1);
        ParkedRecord record = parked.records().get(0);
        assertThat(record.topic()).isEqualTo(scansTopic);
        assertThat(record.offset()).isEqualTo(POISON_OFFSET);
        assertThat(record.key()).isEqualTo(keyFor(scansTopic, POISON_OFFSET));
        // A retry limit of two allows two attempts after the first, so the function ran three times.
        assertThat(record.attempts()).isEqualTo(3);
        assertThat(record.failure()).isInstanceOf(IllegalStateException.class);
        assertThat(parked.oldestAge().isPresent()).isTrue();
    }

    /**
     * <b>The claim only a broker can settle.</b> A second definition joins the same consumer group after the first
     * has closed, and is given exactly the record that parked - so every offset past the parked one committed, and
     * the parked one did not. Under a commit frontier the group would have had to choose between replaying seven
     * good records and losing the poison one; the offset map does neither.
     * <p>
     * Its own route succeeds on everything, so what it reports is what the broker handed it rather than what it
     * decided.
     */
    private void assertOnlyTheParkedRecordIsRedelivered(String group) {
        var redelivered = new ConcurrentLinkedQueue<String>();
        ParallelConsumerDefinition second = ParallelConsumer.connect(connectionProperties(group));
        second.string(scansTopic).process(context -> {
            redelivered.add(context.value());
            return Outcome.succeeded();
        });
        // The orders route has to be declared as well: this instance joins the same group, so it is assigned both
        // topics, and a definition that routed only one of them would leave the other's records unclaimed.
        second.json(ordersTopic).process(context -> Outcome.succeeded());

        // The handle is named rather than anonymous only so that the close is visibly the try's, not a leak.
        try (ParallelConsumerInstance secondInstance = second.start()) {
            log.info("Second instance of group started: {}", secondInstance);
            await().alias("the parked record to be delivered again to a new member of the same group")
                    .atMost(Duration.ofMinutes(1))
                    .untilAsserted(() -> assertWithMessage("only the record that parked should be delivered again; "
                            + "everything either side of it committed")
                            .that(redelivered).containsExactly(poisonValue()));
        }
    }

    private static String poisonValue() {
        return POISON_PREFIX + "-scan-" + POISON_OFFSET;
    }

    /**
     * Sent one at a time and awaited, so the offsets are the indices: this test names an offset, and a producer
     * with records in flight is free to give them any order it likes.
     */
    @SneakyThrows
    private void produceRecords() {
        try (KafkaProducer<String, String> producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < RECORDS_PER_TOPIC; i++) {
                RecordMetadata order = producer
                        .send(new ProducerRecord<>(ordersTopic, keyFor(ordersTopic, i), orderJson(i))).get();
                RecordMetadata scan = producer
                        .send(new ProducerRecord<>(scansTopic, keyFor(scansTopic, i), scanValue(i))).get();
                assertWithMessage("this test names offsets, so each record must land where it is expected")
                        .that(new long[]{order.offset(), scan.offset()}).isEqualTo(new long[]{i, i});
            }
        }
    }

    /**
     * One order in six is {@code RETURNED} in the quickstart's generated data; here exactly one is, so the filtered
     * outcome fires exactly once and the expected counts are numbers rather than bands.
     */
    private static String orderJson(int index) {
        String status = index == 0 ? RETURNED : "IN_TRANSIT";
        return "{\"orderId\":\"order-" + index + "\",\"customerId\":\"customer-" + index
                + "\",\"destinationCity\":\"Wellington\",\"parcelCount\":" + (index + 1)
                + ",\"status\":\"" + status + "\"}";
    }

    private static String scanValue(int index) {
        return index == POISON_OFFSET ? poisonValue() : "scan-" + index;
    }
}
