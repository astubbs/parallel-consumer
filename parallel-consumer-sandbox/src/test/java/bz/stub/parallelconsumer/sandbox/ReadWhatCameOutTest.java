package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The read side: a per-topic object over what the instance produced, with the verbs of the broker-free test drivers
 * of the stream-processing libraries users compare us with (KTD16).
 *
 * <h2>Why the evidence is gathered through the classic API</h2>
 * A definition has to <em>produce</em> something for there to be anything to read, and on the fluent API that is
 * export, which has not landed. The classic API's {@code pollAndProduce} produces today, through the very
 * {@code MockProducer} the sandbox hands it, so it is what exercises the read side end to end. The fluent half of
 * this file is therefore about the surface being reachable and honest rather than about records coming back, and
 * the assertion that pins it says which line changes when export lands.
 *
 * <h2>The two behaviours worth a test each</h2>
 * <b>Reading consumes.</b> A test that reads twice expecting two records and gets the same one twice is the failure
 * this choice causes, so a read of each shape is followed by a read of the next record rather than by a re-read.
 * <p>
 * <b>A read before the run settles refuses.</b> Processing is concurrent, so an early read comes back empty - and an
 * empty read looks exactly like a definition that produced nothing, which is a test that passes for the wrong reason
 * and goes on passing. The refusal, not the emptiness, is what this file asserts.
 */
@Timeout(60)
class ReadWhatCameOutTest {

    private static final String ORDERS_TOPIC = "orders";

    /**
     * Where the classic definition below produces. Not one of the sandbox's own topics deliberately: an output
     * topic is not checked against what the instance consumes, because a definition produces wherever it likes.
     */
    private static final String SHIPMENTS_TOPIC = "shipments";

    /**
     * A second output topic, for the one test about a read seeing only its own topic's records.
     */
    private static final String AUDIT_TOPIC = "audit";

    /**
     * An end-to-end read of everything the output topic offers, in the order a test would reach for it, with each
     * read followed by one that proves the previous record is gone rather than re-readable.
     */
    @Test
    void everyReadVerbHandsBackTheNextRecordRatherThanTheSameOneAgain() {
        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ClassicSandbox<String, String> classic =
                     sandbox.classic(String.class, String.class, ORDERS_TOPIC)) {
            MockProducer<String, String> producer = classic.producer(new StringSerializer(), new StringSerializer());
            ParallelEoSStreamProcessor<String, String> pc = producing(classic, producer);
            try {
                pc.subscribe(classic.topics());
                pc.pollAndProduce(context -> new ProducerRecord<>(SHIPMENTS_TOPIC,
                        context.getSingleRecord().key(),
                        "shipped-" + context.getSingleRecord().value()));
                classic.assignAfterSeeding();

                classic.pipe(ORDERS_TOPIC, "cust-1", "first");
                classic.pipe(ORDERS_TOPIC, "cust-1", "second");
                classic.pipe(ORDERS_TOPIC, "cust-1", "third");
                classic.awaitSettled();

                SandboxOutputTopic<String, String> shipments = classic.createOutputTopic(SHIPMENTS_TOPIC);
                assertThat(shipments.topic()).isEqualTo(SHIPMENTS_TOPIC);
                assertThat(shipments.isEmpty()).isFalse();
                assertThat(shipments.queueSize()).isEqualTo(3);

                assertThat(shipments.readValue()).isEqualTo("shipped-first");
                assertWithMessage("reading consumes, so the queue is one shorter and the next read is the next "
                        + "record").that(shipments.queueSize()).isEqualTo(2);

                ProducerRecord<String, String> second = shipments.readRecord();
                assertThat(second.value()).isEqualTo("shipped-second");
                assertWithMessage("a record read as a whole record is consumed too, and it carries the key the "
                        + "definition produced it with").that(second.key()).isEqualTo("cust-1");

                assertWithMessage("a drain takes what is left rather than everything that ever arrived")
                        .that(shipments.readValuesToList()).containsExactly("shipped-third");
                assertThat(shipments.isEmpty()).isTrue();
                assertThat(shipments.queueSize()).isEqualTo(0);

                NoSuchElementException exhausted = assertThrows(NoSuchElementException.class, shipments::readValue);
                assertWithMessage("the refusal names the topic and says the records were read rather than never "
                        + "produced, which are the two things a reader here needs told apart")
                        .that(exhausted).hasMessageThat().contains(SHIPMENTS_TOPIC);
            } finally {
                pc.closeDrainFirst();
            }
        }
    }

    /**
     * The false-pass shape, pinned: every read refuses until the run has settled, including the two that would
     * otherwise answer "nothing came out" - {@code isEmpty} and {@code queueSize} - because that is the answer a
     * test would believe.
     */
    @Test
    void aReadTakenBeforeTheRunSettlesIsRefusedRatherThanReadingAsThoughNothingWasProduced() {
        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ClassicSandbox<String, String> classic =
                     sandbox.classic(String.class, String.class, ORDERS_TOPIC)) {
            MockProducer<String, String> producer = classic.producer(new StringSerializer(), new StringSerializer());
            ParallelEoSStreamProcessor<String, String> pc = producing(classic, producer);
            try {
                pc.subscribe(classic.topics());
                pc.pollAndProduce(context -> new ProducerRecord<>(SHIPMENTS_TOPIC,
                        context.getSingleRecord().key(),
                        "shipped-" + context.getSingleRecord().value()));
                classic.assignAfterSeeding();

                SandboxOutputTopic<String, String> shipments = classic.createOutputTopic(SHIPMENTS_TOPIC);
                classic.pipe(ORDERS_TOPIC, "cust-1", "first");

                IllegalStateException tooEarly =
                        assertThrows(IllegalStateException.class, shipments::readRecordsToList);
                assertWithMessage("the refusal has to say which call earns the read, or a caller cannot act on it")
                        .that(tooEarly).hasMessageThat().contains("awaitSettled()");
                assertWithMessage("an empty list is what a definition that produced nothing looks like, so the "
                        + "emptiness questions refuse on the same terms as the reads")
                        .that(assertThrows(IllegalStateException.class, shipments::isEmpty))
                        .hasMessageThat().contains(SHIPMENTS_TOPIC);
                IllegalStateException sizeTooEarly =
                        assertThrows(IllegalStateException.class, shipments::queueSize);
                assertThat(sizeTooEarly).hasMessageThat().contains("awaitSettled()");

                classic.awaitSettled();
                assertWithMessage("the settle is what earns the read, and the record was there all along")
                        .that(shipments.readValuesToList()).containsExactly("shipped-first");

                // And a further pipe takes the earned read away again, because that record is now in flight.
                classic.pipe(ORDERS_TOPIC, "cust-1", "second");
                assertThrows(IllegalStateException.class, shipments::readValue);
                classic.awaitSettled();
                assertThat(shipments.readValuesToList()).containsExactly("shipped-second");
            } finally {
                pc.closeDrainFirst();
            }
        }
    }

    /**
     * The map read, and the one thing it cannot say: a key produced twice keeps its last value, which is why a test
     * that cares about both reaches for the record list instead.
     */
    @Test
    void aKeyProducedTwiceKeepsItsLastValueInTheMapAndBothRecordsInTheList() {
        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ClassicSandbox<String, String> classic =
                     sandbox.classic(String.class, String.class, ORDERS_TOPIC)) {
            MockProducer<String, String> producer = classic.producer(new StringSerializer(), new StringSerializer());
            ParallelEoSStreamProcessor<String, String> pc = producing(classic, producer);
            try {
                pc.subscribe(classic.topics());
                pc.pollAndProduce(context -> new ProducerRecord<>(SHIPMENTS_TOPIC,
                        context.getSingleRecord().key(),
                        "shipped-" + context.getSingleRecord().value()));
                classic.assignAfterSeeding();

                // One key, so the map has one entry and the list has two - and one partition, so the second record
                // is processed after the first and "last" is a fact rather than a race.
                classic.pipe(ORDERS_TOPIC, "cust-1", "first");
                classic.pipe(ORDERS_TOPIC, "cust-1", "second");
                classic.awaitSettled();

                SandboxOutputTopic<String, String> shipments = classic.createOutputTopic(SHIPMENTS_TOPIC);
                List<ProducerRecord<String, String>> records = shipments.readRecordsToList();
                assertThat(records).hasSize(2);

                SandboxOutputTopic<String, String> freshCursor = classic.createOutputTopic(SHIPMENTS_TOPIC);
                Map<String, String> byKey = freshCursor.readKeyValuesToMap();
                assertWithMessage("a map cannot hold two values for a key and does not pretend to")
                        .that(byKey).containsExactly("cust-1", "shipped-second");
                assertWithMessage("the cursor belongs to the object, so a second output topic over the same topic "
                        + "reads the whole of it").that(freshCursor.isEmpty()).isTrue();
            } finally {
                pc.closeDrainFirst();
            }
        }
    }

    /**
     * Two output topics over one run, each reading only its own records - the definition produces onto both from
     * the same function.
     */
    @Test
    void anOutputTopicReadsOnlyTheRecordsProducedOntoItsOwnTopic() {
        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ClassicSandbox<String, String> classic =
                     sandbox.classic(String.class, String.class, ORDERS_TOPIC)) {
            MockProducer<String, String> producer = classic.producer(new StringSerializer(), new StringSerializer());
            ParallelEoSStreamProcessor<String, String> pc = producing(classic, producer);
            try {
                pc.subscribe(classic.topics());
                pc.pollAndProduceMany(context -> Arrays.asList(
                        new ProducerRecord<>(SHIPMENTS_TOPIC, context.getSingleRecord().key(),
                                "shipped-" + context.getSingleRecord().value()),
                        new ProducerRecord<>(AUDIT_TOPIC, context.getSingleRecord().key(),
                                "audited-" + context.getSingleRecord().value())));
                classic.assignAfterSeeding();

                classic.pipe(ORDERS_TOPIC, "cust-1", "first");
                classic.awaitSettled();

                assertThat(classic.createOutputTopic(SHIPMENTS_TOPIC).readValuesToList())
                        .containsExactly("shipped-first");
                assertThat(classic.createOutputTopic(AUDIT_TOPIC).readValuesToList())
                        .containsExactly("audited-first");
            } finally {
                pc.closeDrainFirst();
            }
        }
    }

    /**
     * A classic definition that asked for no producer has nothing recording what it produced, and the refusal says
     * how to give it one rather than handing back an output topic that would read as empty for ever.
     */
    @Test
    void anOutputTopicOnAClassicSandboxWithNoProducerSaysHowToBuildOne() {
        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ClassicSandbox<String, String> classic =
                     sandbox.classic(String.class, String.class, ORDERS_TOPIC)) {
            IllegalStateException refusal = assertThrows(IllegalStateException.class,
                    () -> classic.createOutputTopic(SHIPMENTS_TOPIC));
            assertThat(refusal).hasMessageThat().contains("producer(keySerializer, valueSerializer)");
        }
    }

    /**
     * The input object and the flat pipe are one way in reached two ways: the records arrive in the order they were
     * piped, at ascending offsets, and the route sees both without knowing which call made them.
     */
    @Test
    void theInputTopicObjectAndTheFlatPipeGoDownTheSameRoute() {
        ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC).process(context -> {
            seen.add(context.value());
            return Outcome.succeeded();
        });

        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            SandboxInputTopic<Object, Object> orders = sandbox.createInputTopic(ORDERS_TOPIC);
            assertThat(orders.topic()).isEqualTo(ORDERS_TOPIC);
            assertThat(orders.pipeInput("cust-1", "through the object")).isEqualTo(0L);
            assertWithMessage("the flat form writes to the same topic at the next offset, so neither call has a "
                    + "queue of its own").that(sandbox.pipe(ORDERS_TOPIC, "cust-1", "flat")).isEqualTo(1L);

            sandbox.awaitSettled();
            assertThat(seen).containsExactly("through the object", "flat");
            assertThat(instance.failureCause()).isEmpty();
        }
    }

    /**
     * An input topic for a topic nothing routes is refused where it is named, rather than at whichever pipe happens
     * to be first - the same refusal the flat pipe gives, naming what the definition does route.
     */
    @Test
    void anInputTopicForATopicNoRouteClaimsIsRefusedWhereItIsNamed() {
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC).process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                    () -> sandbox.createInputTopic("parcel-scans"));
            assertThat(refusal).hasMessageThat().contains(ORDERS_TOPIC);
            assertWithMessage("a refused input topic is a refusal to the caller, not a fault the instance records")
                    .that(instance.failureCause()).isEmpty();
        }
    }

    /**
     * The fluent read side is reachable and honest: it refuses before the settle like any other, and afterwards it
     * is empty, because a fluent definition produces nothing until export lands.
     * <p>
     * <b>When export lands, the second assertion here is the one to change</b> - it pins today's truth so that the
     * surface cannot quietly start reading something nobody wired up.
     */
    @Test
    void aFluentSandboxesOutputTopicIsReachableAndEmptyUntilExportLands() {
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC).process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder().handPublished().build();
        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            SandboxOutputTopic<byte[], byte[]> exports = sandbox.createOutputTopic("exports");
            long ignoredOffset = sandbox.pipe(ORDERS_TOPIC, "cust-1", "first");
            assertThrows(IllegalStateException.class, exports::readRecordsToList);

            sandbox.awaitSettled();
            assertWithMessage("a fluent definition has nothing that produces yet, so its output topic is empty "
                    + "rather than unreachable").that(exports.readRecordsToList()).isEmpty();
            assertThat(exports.isEmpty()).isTrue();
            assertWithMessage("and it is empty because nothing produces, not because the run went wrong")
                    .that(instance.failureCause()).isEmpty();
        }
    }

    /**
     * The options every test here builds: this sandbox's consumer and producer, partition ordering so that a test
     * asserting on the order records came out in is asserting about the definition rather than about the pool.
     */
    private static ParallelEoSStreamProcessor<String, String> producing(ClassicSandbox<String, String> classic,
                                                                        MockProducer<String, String> producer) {
        return new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(classic.consumer())
                .producer(producer)
                .ordering(ProcessingOrder.PARTITION)
                .build());
    }
}
