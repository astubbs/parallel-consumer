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
import java.util.concurrent.atomic.AtomicInteger;

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
    /** How many times the user function ran for each key - a replayed record runs at least twice. */
    private final Map<String, AtomicInteger> runsByKey = new ConcurrentHashMap<>();
    private Consumer<String, String> pcConsumer;
    private final List<KafkaProducer<String, String>> rogues = new ArrayList<>();
    private volatile String derivedTransactionalId;

    @BeforeEach
    void setUp() {
        setupTopic(ProducerFencingRecoveryIT.class.getSimpleName());
        outputTopic = getTopic() + "-output";
        getKcu().createTopic(outputTopic, numPartitions);

        pcConsumer = getKcu().<String, String>createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);

        verifier = getKcu().createNewConsumer("fencing-recovery-verifier-" + nextInt());
        verifier.subscribe(UniLists.of(outputTopic));
    }

    /**
     * @param commitInterval short, so a fence lands on an empty ledger and tests the replacement alone; long, so
     *                       completed work is still uncommitted when the fence lands and the replay runs on the wire
     */
    private void startPc(Duration commitInterval) {
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
                .commitInterval(commitInterval)
                .defaultMessageRetryDelay(ofMillis(500))
                .meterRegistry(registry)
                .build();
        pc = new ParallelEoSStreamProcessor<>(options);
        pc.subscribe(UniLists.of(getTopic()));
        pc.pollAndProduceMany(context -> {
            keysProcessed.add(context.key());
            runsByKey.computeIfAbsent(context.key(), ignored -> new AtomicInteger()).incrementAndGet();
            return UniLists.of(new ProducerRecord<>(outputTopic, context.key(), "result-for-" + context.key()));
        });
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
        startPc(ofMillis(500));
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
            assertTheRogueIsFencedInTurn(rogue, fence);
        }

        assertThat(pc.isClosedOrFailed()).isFalse();
        assertThat(recoveries()).isEqualTo(FENCES);
        assertEveryKeyHasExactlyOneResultAfterSettling(allKeys);
    }

    /**
     * The replay half of the guarantee, on the wire (R13, KTD5): the fence lands while a phase's records are
     * processed and produced but not yet committed - the commit interval is long enough that the ledger is full when
     * the rogue arrives - so the transaction that carried their output is aborted by the broker, PC puts every one of
     * them back, and the replacement produces and commits them. Each key's user function runs at least twice and
     * exactly one result per key is visible at {@code read_committed}. The first test fences on an empty ledger (its
     * commit interval is short and every prior key was already visible), so it proves the replacement alone; without
     * this case the replay could be stubbed out and the register stay green.
     */
    @Test
    @ProvesClaim(TransactionalClaim.PRODUCER_INVALIDATION_RECOVERED)
    void aFenceLandingOnCompletedButUncommittedWorkReplaysItAndEveryResultIsStillVisibleExactlyOnce() {
        startPc(ofSeconds(10));
        List<String> allKeys = new ArrayList<>();

        // phase 0: the first commit is immediate, whatever the interval, so this phase lands and is read back
        allKeys.addAll(sendPhase(0));
        awaitResultsFor(allKeys);
        log.info("Phase 0 read back; PC's transactional.id is {}", derivedTransactionalId);

        // phase 1: processed and produced into the open transaction, which the 10 s interval keeps uncommitted
        List<String> phaseOne = sendPhase(1);
        allKeys.addAll(phaseOne);
        await("phase 1 to be processed").atMost(SETTLE).until(() -> keysProcessed.containsAll(phaseOne));
        pollVerifierFor(ofSeconds(2));
        assertWithMessage("fixture: phase 1 is processed but not yet committed, so it is in the ledger when the fence lands")
                .that(resultsByKey.keySet()).containsNoneIn(phaseOne);

        double recoveriesBefore = recoveries();
        KafkaProducer<String, String> rogue = rogueUnder(derivedTransactionalId);
        rogue.initTransactions();
        log.info("Fence with a non-empty ledger: rogue initialised under {}", derivedTransactionalId);

        await("the recovery to complete").atMost(SETTLE).until(() -> recoveries() >= recoveriesBefore + 1);
        awaitResultsFor(allKeys);
        assertTheRogueIsFencedInTurn(rogue, 1);

        assertThat(pc.isClosedOrFailed()).isFalse();
        for (String key : phaseOne) {
            assertWithMessage("%s was in the aborted transaction, so it was put back and processed again", key)
                    .that(runsByKey.get(key).get()).isAtLeast(2);
        }
        assertEveryKeyHasExactlyOneResultAfterSettling(allKeys);
    }

    /**
     * Control: the replacement re-initialised under the same id, so the rogue is the fenced one now. A fenced
     * producer learns it on its first network call, not on beginTransaction (a local state transition), so the
     * control sends: the send must fail with the fence, and - the other half of the control - a rogue that was NOT
     * fenced would land a record in the output topic and fail the exact-keys assertion.
     */
    private void assertTheRogueIsFencedInTurn(KafkaProducer<String, String> rogue, int fence) {
        rogue.beginTransaction();
        var rogueSend = rogue.send(new ProducerRecord<>(outputTopic, "rogue-" + fence, "must never be visible"));
        var fencedRogue = assertThrows(Exception.class, () -> rogueSend.get(30, java.util.concurrent.TimeUnit.SECONDS));
        assertWithMessage("the rogue's send fails with the fence, not with something else: %s", fencedRogue)
                .that(RecoverableProducerCondition.find(fencedRogue).map(Object::getClass).orElse(null))
                .isAnyOf(ProducerFencedException.class, InvalidProducerEpochException.class);
        log.info("Fence {} control held: the rogue is fenced in turn ({})", fence, fencedRogue.getMessage());
    }

    /**
     * A duplicate lands after the first result, so "exactly one" is only meaningful after a quiet period: the
     * verifier keeps reading for a few seconds past the last awaited key before the count is asserted.
     */
    private void assertEveryKeyHasExactlyOneResultAfterSettling(List<String> allKeys) {
        pollVerifierFor(ofSeconds(5));
        for (String key : allKeys) {
            assertWithMessage("result for %s at read_committed", key).that(resultsByKey.get(key)).hasSize(1);
        }
        assertWithMessage("only the keys this test sent produced results").that(resultsByKey.keySet()).containsExactlyElementsIn(allKeys);
    }

    private void pollVerifierFor(Duration quietPeriod) {
        Instant until = Instant.now().plus(quietPeriod);
        while (Instant.now().isBefore(until)) {
            collect(verifier.poll(ofMillis(250)));
        }
    }

    private void collect(ConsumerRecords<String, String> polled) {
        for (ConsumerRecord<String, String> record : polled) {
            resultsByKey.computeIfAbsent(record.key(), ignored -> new ArrayList<>()).add(record.value());
        }
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
            collect(verifier.poll(ofMillis(250)));
            if (keys.stream().allMatch(resultsByKey::containsKey)) {
                return;
            }
        }
        throw new AssertionError("Not every key's result became visible at read_committed within " + SETTLE + "; missing: "
                + keys.stream().filter(key -> !resultsByKey.containsKey(key)).collect(java.util.stream.Collectors.toList())
                + " (processed by the user function: " + keysProcessed + ")");
    }
}
