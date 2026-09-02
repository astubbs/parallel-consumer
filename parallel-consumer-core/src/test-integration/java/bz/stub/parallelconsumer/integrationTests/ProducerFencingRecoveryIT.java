package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.ProducerFactory;
import bz.stub.parallelconsumer.ProvesClaim;
import bz.stub.parallelconsumer.TransactionalClaim;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.internal.RecoverableProducerCondition;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Recovery against a real transaction coordinator (U7 of astubbs#225): a rogue producer initialised under PC's own
 * derived {@code transactional.id} fences PC's producer the way a rebalance race under KIP-447 does, and PC must
 * replace it and carry on, with every source record's output visible exactly once at {@code read_committed}.
 * <p>
 * Two controls make a green run mean something. The rogue's next transactional call must itself be fenced - that
 * is the replacement re-initialising under the same id, which is the whole mechanism. And the recovery counter
 * must have moved, so a run in which the fence never landed cannot pass.
 */
@Timeout(600)
@Tag("transactions")
@Slf4j
class ProducerFencingRecoveryIT extends BrokerIntegrationTest<String, String> {

    private static final int RECORDS_PER_PHASE = 5;
    private static final int FENCES = 3;
    private static final Duration SETTLE = ofSeconds(60);

    private String outputTopic;
    private ParallelEoSStreamProcessor<String, String> pc;
    private Consumer<String, String> verifier;
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final Map<String, List<String>> resultsByKey = new HashMap<>();
    private final Set<String> keysProcessed = ConcurrentHashMap.newKeySet();
    private final List<KafkaProducer<String, String>> rogues = new ArrayList<>();
    private volatile String derivedTransactionalId;

    @BeforeEach
    void setUp() {
        setupTopic(ProducerFencingRecoveryIT.class.getSimpleName());
        outputTopic = getTopic() + "-output";
        getKcu().createTopic(outputTopic, numPartitions);

        var pcConsumer = getKcu().<String, String>createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);
        ProducerFactory<String, String> capturingFactory = config -> {
            derivedTransactionalId = (String) config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            return new KafkaProducer<>(config);
        };
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(pcConsumer)
                .producerConfig(getKcu().transactionalProducerConfig(new Properties()))
                .producerFactory(capturingFactory)
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .batchSize(1)
                .commitInterval(ofMillis(500))
                .defaultMessageRetryDelay(ofMillis(500))
                .meterRegistry(registry)
                .build();
        pc = new ParallelEoSStreamProcessor<>(options);
        pc.subscribe(UniLists.of(getTopic()));

        verifier = getKcu().createNewConsumer("fencing-recovery-verifier-" + nextInt());
        verifier.subscribe(UniLists.of(outputTopic));
    }

    @AfterEach
    void tearDown() {
        if (pc != null && !pc.isClosedOrFailed()) {
            pc.closeDontDrainFirst(ofSeconds(30));
        }
        if (verifier != null) {
            verifier.close();
        }
        for (var rogue : rogues) {
            try {
                rogue.close(ofSeconds(5));
            } catch (RuntimeException fencedClose) {
                log.debug("Closing a fenced rogue threw, as expected: {}", fencedClose.toString());
            }
        }
    }

    /**
     * Covers AE1 and AE3 on the wire, and the recovery guarantee the README states.
     */
    @Test
    @ProvesClaim(TransactionalClaim.PRODUCER_INVALIDATION_RECOVERED)
    void aFencedProducerIsReplacedAndEveryResultIsStillVisibleExactlyOnce() {
        pc.pollAndProduceMany(context -> {
            keysProcessed.add(context.key());
            return UniLists.of(new ProducerRecord<>(outputTopic, context.key(), "result-for-" + context.key()));
        });
        List<String> allKeys = new ArrayList<>();

        // phase 0: healthy traffic read back at read_committed - the non-vacuity anchor
        allKeys.addAll(sendPhase(0));
        awaitResultsFor(allKeys);
        assertWithMessage("the id PC derived has the documented shape pc-<L>-<group.id>-<uuid>")
                .that(derivedTransactionalId).matches("pc-\\d+-.+-[0-9a-f-]{36}");
        log.info("Phase 0 read back; PC's transactional.id is {}", derivedTransactionalId);

        for (int fence = 1; fence <= FENCES; fence++) {
            double recoveriesBefore = recoveries();
            // a rogue under PC's id: its initTransactions bumps the epoch, and PC's producer is fenced from then on
            KafkaProducer<String, String> rogue = rogueUnder(derivedTransactionalId);
            rogue.initTransactions();
            log.info("Fence {}: rogue initialised under {}", fence, derivedTransactionalId);

            allKeys.addAll(sendPhase(fence));
            await("recovery " + fence + " to complete").atMost(SETTLE).until(() -> recoveries() >= recoveriesBefore + 1);
            awaitResultsFor(allKeys);

            // control: the replacement re-initialised under the same id, so the rogue is the fenced one now. A fenced
            // producer learns it on its first network call, not on beginTransaction (a local state transition), so
            // the control sends: the send must fail with the fence, and - the other half of the control - a rogue
            // that was NOT fenced would land a record in the output topic and fail the exact-keys assertion below.
            rogue.beginTransaction();
            var rogueSend = rogue.send(new ProducerRecord<>(outputTopic, "rogue-" + fence, "must never be visible"));
            var fencedRogue = assertThrows(Exception.class, () -> rogueSend.get(30, java.util.concurrent.TimeUnit.SECONDS));
            assertWithMessage("the rogue's send fails with the fence, not with something else: %s", fencedRogue)
                    .that(RecoverableProducerCondition.find(fencedRogue).map(Object::getClass).orElse(null))
                    .isAnyOf(ProducerFencedException.class, InvalidProducerEpochException.class);
            log.info("Fence {} control held: the rogue is fenced in turn ({})", fence, fencedRogue.getMessage());
        }

        assertThat(pc.isClosedOrFailed()).isFalse();
        assertThat(recoveries()).isEqualTo(FENCES);
        for (String key : allKeys) {
            assertWithMessage("result for %s at read_committed", key).that(resultsByKey.get(key)).hasSize(1);
        }
        assertWithMessage("only the keys this test sent produced results").that(resultsByKey.keySet()).containsExactlyElementsIn(allKeys);
    }

    private double recoveries() {
        return registry.find("pc.producer.recoveries").counters().stream().mapToDouble(c -> c.count()).sum();
    }

    private KafkaProducer<String, String> rogueUnder(String transactionalId) {
        var props = new Properties();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        Map<String, Object> config = getKcu().transactionalProducerConfig(props);
        KafkaProducer<String, String> rogue = new KafkaProducer<>(config);
        rogues.add(rogue);
        return rogue;
    }

    @SneakyThrows
    private List<String> sendPhase(int phase) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < RECORDS_PER_PHASE; i++) {
            String key = "phase-" + phase + "-key-" + i;
            keys.add(key);
            getKcu().getProducer().send(new ProducerRecord<>(getTopic(), key, "source-" + key)).get();
        }
        return keys;
    }

    private void awaitResultsFor(List<String> keys) {
        Instant deadline = Instant.now().plus(SETTLE);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, String> polled = verifier.poll(ofMillis(250));
            for (ConsumerRecord<String, String> record : polled) {
                resultsByKey.computeIfAbsent(record.key(), ignored -> new ArrayList<>()).add(record.value());
            }
            if (keys.stream().allMatch(resultsByKey::containsKey)) {
                return;
            }
        }
        throw new AssertionError("Not every key's result became visible at read_committed within " + SETTLE + "; missing: "
                + keys.stream().filter(key -> !resultsByKey.containsKey(key)).collect(java.util.stream.Collectors.toList())
                + " (processed by the user function: " + keysProcessed + ")");
    }
}
