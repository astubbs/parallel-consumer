package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Delegating wrapper around {@link Consumer} that enforces thread confinement at runtime.
 * All consumer methods (except {@link #wakeup()}) must be called from the owning thread.
 * <p>
 * Uses Lombok {@code @Delegate} to generate passthrough methods for all Consumer methods we
 * don't explicitly override. We override all thread-unsafe methods with a {@link #checkThread}
 * guard. {@link #wakeup()} is left to the delegate (thread-safe per Kafka API).
 * <p>
 * Ownership is a full lifecycle, not a one-shot: the poll thread calls {@link #claimOwnership()}
 * before first use, and {@link #releaseOwnership()} when its loop exits and it will never touch
 * the consumer again. A thread that closes the consumer afterwards (the control thread, in
 * transactional mode) takes over with {@link #tryClaimOwnership()}, which never steals - it
 * succeeds only when the consumer is unclaimed or already owned by the caller. The guard
 * therefore asserts "no <i>other</i> thread currently owns this consumer", which is the property
 * Kafka's single-threaded consumer contract actually requires; comparing raw thread identity
 * forever would (and did) reject the legal sequential ownership handoff at close time, while a
 * guard that let close bypass the check would legalise closing mid-poll. While unclaimed
 * (before start, and between release and takeover during the strictly sequential close path),
 * all methods are allowed - init-time calls like subscribe need this.
 * <p>
 * Pattern follows {@link ProducerWrapper} which uses the same Lombok delegate approach.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a>
 */
@Slf4j
@RequiredArgsConstructor
class ThreadConfinedConsumer<K, V> implements Consumer<K, V> {

    private final AtomicReference<Thread> ownerThread = new AtomicReference<>();

    @NonNull
    @Delegate(excludes = ThreadUnsafeMethods.class)
    private final Consumer<K, V> delegate;

    /**
     * Claim this consumer for the current thread. After this call, any consumer method
     * (except wakeup) called from a different thread will throw IllegalStateException.
     */
    void claimOwnership() {
        Thread current = Thread.currentThread();
        Thread previousOwner = ownerThread.getAndSet(current);
        if (previousOwner != null && previousOwner != current) {
            // claimOwnership is the poll thread's start-of-life claim; a live previous owner here
            // means two poll loops share one consumer, which the guard exists to catch. Log loudly
            // rather than throw: throwing here would kill the new loop, not the misuse.
            log.warn("Consumer ownership claimed by thread '{}' but was still held by thread '{}' (alive:{}) - " +
                            "two components polling one consumer? See confluentinc#857.",
                    current.getName(), previousOwner.getName(), previousOwner.isAlive());
        } else {
            log.debug("Consumer ownership claimed by thread: {}", current.getName());
        }
    }

    /**
     * Release ownership. Called by the owning thread once it has permanently finished with the
     * consumer (poll loop exit - normal or by exception), making the consumer claimable by the
     * thread that performs the final {@link #close()}. No-op with a warning if the caller is not
     * the owner: releasing on another thread's behalf would silently disarm the guard.
     */
    void releaseOwnership() {
        Thread current = Thread.currentThread();
        if (ownerThread.compareAndSet(current, null)) {
            log.debug("Consumer ownership released by thread: {}", current.getName());
        } else {
            log.warn("Thread '{}' tried to release consumer ownership it does not hold (owner: {})",
                    current.getName(), describeOwner(ownerThread.get()));
        }
    }

    /**
     * Attempt to claim ownership for the current thread <b>without stealing</b>: succeeds only when
     * the consumer is unclaimed, or the caller already owns it. Used by the closing thread after
     * the poll loop has released - if the poll loop is still running (e.g. {@code closeAndWait}
     * timed out and close proceeded anyway), the claim fails and the subsequent guarded call
     * throws, exactly as before this method existed.
     *
     * @return true if the current thread owns the consumer after this call
     */
    boolean tryClaimOwnership() {
        Thread current = Thread.currentThread();
        if (ownerThread.compareAndSet(null, current)) {
            log.debug("Consumer ownership taken over by thread: {}", current.getName());
            return true;
        }
        boolean alreadyOwner = ownerThread.get() == current;
        if (!alreadyOwner) {
            log.debug("Thread '{}' could not take over consumer ownership - still held by {}",
                    current.getName(), describeOwner(ownerThread.get()));
        }
        return alreadyOwner;
    }

    private static String describeOwner(Thread owner) {
        return owner == null ? "unclaimed"
                : "'" + owner.getName() + "' (id:" + owner.getId() + ", alive:" + owner.isAlive() + ")";
    }

    private void checkThread(String methodName) {
        Thread current = Thread.currentThread();
        Thread owner = this.ownerThread.get();
        if (owner != null && current != owner) {
            String msg = "Consumer." + methodName + "() called from thread '" +
                    current.getName() + "' (id:" + current.getId() +
                    ") but consumer is owned by thread '" + owner.getName() +
                    "' (id:" + owner.getId() + ", alive:" + owner.isAlive() +
                    "). Only wakeup() is thread-safe. See confluentinc#857.";
            log.error(msg);
            throw new IllegalStateException(msg);
        }
        log.trace("Consumer.{}() on thread '{}' (owner: {})", methodName, current.getName(),
                owner != null ? owner.getName() : "unclaimed");
    }

    // --- Thread-unsafe method overrides (all check thread before delegating) ---

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        checkThread("poll");
        return delegate.poll(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        checkThread("commitSync");
        delegate.commitSync(offsets);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        checkThread("commitSync");
        delegate.commitSync(offsets, timeout);
    }

    @Override
    public void commitSync() {
        checkThread("commitSync");
        delegate.commitSync();
    }

    @Override
    public void commitSync(Duration timeout) {
        checkThread("commitSync");
        delegate.commitSync(timeout);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        checkThread("commitAsync");
        delegate.commitAsync(offsets, callback);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        checkThread("commitAsync");
        delegate.commitAsync(callback);
    }

    @Override
    public void commitAsync() {
        checkThread("commitAsync");
        delegate.commitAsync();
    }

    @Override
    public Set<TopicPartition> assignment() {
        checkThread("assignment");
        return delegate.assignment();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        checkThread("pause");
        delegate.pause(partitions);
    }

    @Override
    public Set<TopicPartition> paused() {
        checkThread("paused");
        return delegate.paused();
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        checkThread("resume");
        delegate.resume(partitions);
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        checkThread("groupMetadata");
        return delegate.groupMetadata();
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        checkThread("subscribe");
        delegate.subscribe(topics, callback);
    }

    @Override
    public void subscribe(Collection<String> topics) {
        checkThread("subscribe");
        delegate.subscribe(topics);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        checkThread("subscribe");
        delegate.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(Pattern pattern) {
        checkThread("subscribe");
        delegate.subscribe(pattern);
    }

    @Override
    public void close() {
        checkThread("close");
        delegate.close();
    }

    @Override
    public void close(Duration timeout) {
        checkThread("close");
        delegate.close(timeout);
    }

    // --- wakeup() is intentionally NOT overridden ---
    // Lombok @Delegate generates the passthrough: delegate.wakeup()
    // This is correct — wakeup() is the one thread-safe Consumer method.

    /**
     * Excludes interface for Lombok @Delegate. Methods listed here are NOT auto-delegated;
     * we override them above with thread-safety checks.
     * <p>
     * Note: method signatures must match the Consumer interface exactly for Lombok to exclude them.
     */
    @SuppressWarnings("unused")
    private interface ThreadUnsafeMethods<K, V> {
        ConsumerRecords<K, V> poll(Duration timeout);
        void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);
        void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout);
        void commitSync();
        void commitSync(Duration timeout);
        void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);
        void commitAsync(OffsetCommitCallback callback);
        void commitAsync();
        Set<TopicPartition> assignment();
        void pause(Collection<TopicPartition> partitions);
        Set<TopicPartition> paused();
        void resume(Collection<TopicPartition> partitions);
        ConsumerGroupMetadata groupMetadata();
        void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);
        void subscribe(Collection<String> topics);
        void subscribe(java.util.regex.Pattern pattern, ConsumerRebalanceListener callback);
        void subscribe(java.util.regex.Pattern pattern);
        void close();
        void close(Duration timeout);
    }
}
