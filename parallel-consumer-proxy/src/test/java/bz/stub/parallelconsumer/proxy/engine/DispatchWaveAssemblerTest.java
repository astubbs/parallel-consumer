package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import bz.stub.parallelconsumer.state.ShardKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The wave rules in isolation, where their timing is deterministic: emission at the size cap, at the window,
 * and at a flush; and KTD10's distinct-shard assertion - applied under restricted ordering, deliberately absent
 * under {@code UNORDERED} (AE22's acceptance half).
 *
 * @author Antony Stubbs
 */
class DispatchWaveAssemblerTest {

    /** Long enough that a test passing because the window elapsed would time out instead - the anti-flake margin. */
    private static final Duration NEVER_ELAPSES = Duration.ofHours(1);

    private static final Duration AWAIT_BUDGET = Duration.ofSeconds(30);

    private final List<Dispatch> emitted = new CopyOnWriteArrayList<>();

    private DispatchWaveAssembler assembler;

    @AfterEach
    void closeAssembler() {
        if (assembler != null) {
            assembler.close();
        }
    }

    private DispatchWaveAssembler assembler(boolean restricted, int sizeCap, Duration window) {
        assembler = new DispatchWaveAssembler(restricted, sizeCap, window, emitted::add);
        return assembler;
    }

    private static DispatchRecord dispatchAt(long offset) {
        return DispatchRecord.newBuilder()
                .setToken(Token.newBuilder().setRecordId("t/0/" + offset).setEpoch(1))
                .build();
    }

    /** A shard identity exactly as the engine computes it, from core's own {@link ShardKey}. */
    private static ShardKey shard(String key, int partition, ProcessingOrder ordering) {
        var record = new ConsumerRecord<>("t", partition, 0L,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8), new byte[0]);
        return ShardKey.of(record, ordering);
    }

    @Test
    void emitsOneWaveTheMomentTheSizeCapIsReached() {
        var assembler = assembler(true, 3, NEVER_ELAPSES);

        assembler.offer(shard("a", 0, ProcessingOrder.KEY), dispatchAt(0));
        assembler.offer(shard("b", 0, ProcessingOrder.KEY), dispatchAt(1));
        assertThat(emitted).isEmpty(); // below cap, window never elapses, no flush - nothing may emit yet

        assembler.offer(shard("c", 0, ProcessingOrder.KEY), dispatchAt(2));

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).getRecordsCount()).isEqualTo(3);
    }

    @Test
    void emitsBelowTheCapWhenTheCoalescingWindowElapses() {
        var assembler = assembler(true, 100, Duration.ofMillis(50));

        assembler.offer(shard("a", 0, ProcessingOrder.KEY), dispatchAt(0));
        assembler.offer(shard("b", 0, ProcessingOrder.KEY), dispatchAt(1));

        // nothing calls flush: the window timer alone must emit
        Awaitility.await().atMost(AWAIT_BUDGET).untilAsserted(() -> assertThat(emitted).hasSize(1));
        assertThat(emitted.get(0).getRecordsCount()).isEqualTo(2);
    }

    @Test
    void flushEmitsAPendingWaveImmediatelyRatherThanHoldingItForTheWindow() {
        var assembler = assembler(true, 100, NEVER_ELAPSES);

        assembler.offer(shard("a", 0, ProcessingOrder.KEY), dispatchAt(0));
        assembler.flush();

        // synchronous - the wave is out before flush returns, which is the lone-record latency bound
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).getRecordsCount()).isEqualTo(1);
    }

    @Test
    void flushWithNothingPendingEmitsNothing() {
        var assembler = assembler(true, 100, NEVER_ELAPSES);

        assembler.flush();

        assertThat(emitted).isEmpty(); // an empty wave would be a protocol-noise message
    }

    @Test
    void underRestrictedOrderingASecondRecordOfOneShardInAWaveIsAnInvariantViolation() {
        var assembler = assembler(true, 100, NEVER_ELAPSES);
        assembler.offer(shard("a", 0, ProcessingOrder.KEY), dispatchAt(0));

        var violation = assertThrows(IllegalStateException.class,
                () -> assembler.offer(shard("a", 0, ProcessingOrder.KEY), dispatchAt(1)));

        assertThat(violation).hasMessageThat().contains("one in-flight record per shard");
        // the wave already assembled is undisturbed by the rejected offer
        assembler.flush();
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).getRecordsCount()).isEqualTo(1);
    }

    @Test
    void underUnorderedManyRecordsOfOnePartitionShareAWave() {
        // covers AE22's acceptance half: under UNORDERED the shard is the partition and several of its records
        // are legitimately in flight at once, so the distinct-shard assertion must NOT fire
        var assembler = assembler(false, 100, NEVER_ELAPSES);

        assembler.offer(shard("a", 0, ProcessingOrder.UNORDERED), dispatchAt(0));
        assembler.offer(shard("b", 0, ProcessingOrder.UNORDERED), dispatchAt(1));
        assembler.offer(shard("c", 0, ProcessingOrder.UNORDERED), dispatchAt(2));
        assembler.flush();

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).getRecordsCount()).isEqualTo(3);
    }

    @Test
    void aShardMayAppearAgainInTheNextWave() {
        // the assertion is per wave, not per lifetime: after a wave emits, its shards are free again
        var assembler = assembler(true, 1, NEVER_ELAPSES);

        assembler.offer(shard("a", 0, ProcessingOrder.KEY), dispatchAt(0));
        assembler.offer(shard("a", 0, ProcessingOrder.KEY), dispatchAt(1));

        assertThat(emitted).hasSize(2);
    }
}
