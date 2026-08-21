package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <b>What one arm actually did</b>, accumulated while it runs: how many records it processed, and
 * how many distinct keys it saw.
 *
 * <h2>Why the two are counted together rather than separately</h2>
 *
 * Throughput alone cannot show the work happened - a fast arm and a short arm produce the same
 * shape of row. The demo contract therefore requires every arm to report <b>records</b> and
 * <b>keys</b> beside its rate, and those two are the only figures that are
 * <em>deterministic across languages</em>: every implementation replaying the same backlog reports
 * the same pair, which is what {@code bin/ci-demo-conformance.sh} can compare when elapsed and
 * msg/s never can be.
 * <p>
 * They are counted in one call, {@link #recordProcessed(byte[])}, deliberately: an arm that could
 * increment its count without noting the key would report a records figure the keys figure does not
 * corroborate, and that is precisely the failure the keys column exists to expose.
 *
 * <h2>Concurrency</h2>
 *
 * Every parallel arm calls {@link #recordProcessed(byte[])} from many worker threads at once, so
 * the count is atomic and the key set is a concurrent one. The latch is released by the record that
 * reaches the target - and, separately, by {@link #sessionEnded()} when a transport's stream ends
 * for its own reasons. Distinguishing those two is what
 * {@code ReferenceDemo#awaited} exists to do.
 *
 * @author Antony Stubbs
 */
final class ArmTally {

    private final int target;

    private final AtomicInteger processed = new AtomicInteger();

    /**
     * The distinct keys observed. Keys are the small ASCII strings {@link DemoBroker#seed} writes,
     * so decoding them is cheap and makes the set's identity semantics the obvious ones - a
     * {@code byte[]} would be de-duplicated by reference and count every delivery as a new key.
     */
    private final Set<String> keys = ConcurrentHashMap.newKeySet();

    private final CountDownLatch done = new CountDownLatch(1);

    ArmTally(int target) {
        this.target = target;
    }

    /**
     * Counts one processed record and notes its key.
     *
     * @param key the record's key bytes, or {@code null} for a keyless record - which is counted as
     *            a record but is not a key, because Kafka distinguishes the two
     */
    void recordProcessed(byte[] key) {
        if (key != null) {
            keys.add(new String(key, StandardCharsets.UTF_8));
        }
        if (processed.incrementAndGet() >= target) {
            done.countDown();
        }
    }

    /**
     * The arm's session ended for a reason of its own - a completed stream, or a failure.
     * <p>
     * This releases the latch <em>without</em> the target having been reached, which is exactly the
     * case {@code ReferenceDemo#awaited} refuses to report as a result. It is a real production
     * path: the raw-gRPC arm's stream observer ends the session on {@code onCompleted} and on an
     * error that arrives before the arm finished.
     */
    void sessionEnded() {
        done.countDown();
    }

    /** Whether the latch is still closed, so a caller can tell its own teardown from a failure. */
    boolean stillRunning() {
        return done.getCount() > 0;
    }

    /** Waits for the arm to finish, or for the budget to expire. */
    boolean awaitCompletion(Duration budget) throws InterruptedException {
        return done.await(budget.toMillis(), TimeUnit.MILLISECONDS);
    }

    int target() {
        return target;
    }

    int processed() {
        return processed.get();
    }

    int uniqueKeys() {
        return keys.size();
    }
}
