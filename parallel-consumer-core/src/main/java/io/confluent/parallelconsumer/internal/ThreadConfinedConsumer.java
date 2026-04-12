package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
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
import java.util.regex.Pattern;

/**
 * Delegating wrapper around {@link Consumer} that enforces thread confinement at runtime.
 * All consumer methods (except {@link #wakeup()}) must be called from the owning thread.
 * <p>
 * Uses Lombok {@code @Delegate} to generate passthrough methods for all Consumer methods we
 * don't explicitly override. We override all thread-unsafe methods with a {@link #checkThread}
 * guard. {@link #wakeup()} is left to the delegate (thread-safe per Kafka API).
 * <p>
 * Call {@link #claimOwnership()} from the poll thread before first use. Before ownership is
 * claimed, all methods are allowed (for init-time calls like subscribe).
 * <p>
 * Pattern follows {@link ProducerWrapper} which uses the same Lombok delegate approach.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a>
 */
@Slf4j
@RequiredArgsConstructor
class ThreadConfinedConsumer<K, V> implements Consumer<K, V> {

    private volatile Thread ownerThread;

    @NonNull
    @Delegate(excludes = ThreadUnsafeMethods.class)
    private final Consumer<K, V> delegate;

    /**
     * Claim this consumer for the current thread. After this call, any consumer method
     * (except wakeup) called from a different thread will throw IllegalStateException.
     */
    void claimOwnership() {
        this.ownerThread = Thread.currentThread();
        log.debug("Consumer ownership claimed by thread: {}", ownerThread.getName());
    }

    private void checkThread(String methodName) {
        Thread owner = this.ownerThread;
        if (owner != null && Thread.currentThread() != owner) {
            String msg = "Consumer." + methodName + "() called from thread '" +
                    Thread.currentThread().getName() + "' (id:" + Thread.currentThread().getId() +
                    ") but consumer is owned by thread '" + owner.getName() +
                    "' (id:" + owner.getId() + "). Only wakeup() is thread-safe. See #857.";
            log.error(msg);
            throw new IllegalStateException(msg);
        }
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
