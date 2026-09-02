package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The completed-but-uncommitted ledger of one partition (R13, KTD5): every record completed since the last successful
 * commit, with the record itself, because completion drops the record and the shard retires the container, and an
 * offset restored without its record could never run again. Kept only in transactional commit mode, where a commit
 * can be aborted after the completions it carried were recorded; {@link #none()} is the other modes' no-op.
 * <p>
 * <b>Thread safety.</b> Written on the control thread ({@code WorkManager.handleFutureResult}, and the replay) and
 * read or trimmed on whichever thread commits - the poll thread does, through the revoke-path commit - so every field
 * access here is under {@link #monitor}. A plain monitor, never the producer's {@code ReadWriteLock}: the engine's
 * {@code AGENTS.md} records that {@code @GuardedBy} is inert on that kind. It is held only for the map operations in
 * this class - never across a lock acquisition, a client call, or a callback - so it can be entered from either thread
 * without an ordering. Its own class rather than three fields on {@code PartitionState} for two reasons: the ledger
 * is a concept of its own, and RacerD analyses a class once it contains a {@code synchronized} block, so the monitor
 * living in {@code PartitionState} put thirty of that class's unrelated accessors on the report.
 */
class UncommittedCompletions<K, V> {

    private final Object monitor = new Object();

    @GuardedBy("monitor")
    private final Map<Long, ConsumerRecord<K, V>> completedButUncommitted = new HashMap<>();

    /**
     * The offsets the commit being performed carries, snapshotted when its data was collected. Commit success removes
     * exactly these, so a completion that lands between collection and success - possible when the revoke-path commit
     * runs on the poll thread while the control thread drains its mailbox - stays for a later replay. The same guard
     * {@code PartitionState.setClean()} applies to the dirty flag through {@code stateChangedSinceCommitStart}.
     */
    @GuardedBy("monitor")
    private Set<Long> completionsInCommit = Collections.emptySet();

    /**
     * @return the ledger for a commit mode in which nothing is retained: every operation is a no-op and the snapshot
     *         is empty
     */
    @SuppressWarnings("unchecked")
    static <K, V> UncommittedCompletions<K, V> none() {
        return (UncommittedCompletions<K, V>) NONE;
    }

    private static final UncommittedCompletions<?, ?> NONE = new UncommittedCompletions<>() {
        @Override
        void record(long offset, ConsumerRecord<Object, Object> record) {
        }

        @Override
        void snapshotForCommit() {
        }

        @Override
        void onCommitSuccess() {
        }

        @Override
        Map<Long, ConsumerRecord<Object, Object>> snapshotInOffsetOrder() {
            return Collections.emptyMap();
        }

        @Override
        void forget(Collection<Long> offsets) {
        }
    };

    /** A record completed; retained until the commit carrying it succeeds. */
    void record(long offset, ConsumerRecord<K, V> record) {
        synchronized (monitor) {
            completedButUncommitted.put(offset, record);
        }
    }

    /** The commit whose data is being collected carries everything retained so far; only that is trimmed on success. */
    void snapshotForCommit() {
        synchronized (monitor) {
            completionsInCommit = new HashSet<>(completedButUncommitted.keySet());
        }
    }

    /** The commit succeeded: what it carried is forgotten, what landed since stays. */
    void onCommitSuccess() {
        synchronized (monitor) {
            completionsInCommit.forEach(completedButUncommitted::remove);
            completionsInCommit = Collections.emptySet();
        }
    }

    /**
     * @return a copy of the ledger in offset order, for the replay to put back; the ledger itself is untouched until
     *         {@link #forget(Collection)} says which of them are back in processing
     */
    Map<Long, ConsumerRecord<K, V>> snapshotInOffsetOrder() {
        synchronized (monitor) {
            return new TreeMap<>(completedButUncommitted);
        }
    }

    /**
     * The replay put these back into processing, so no commit will carry them again. Also clears the in-commit
     * snapshot: the commit it belonged to was the aborted one.
     */
    void forget(Collection<Long> offsets) {
        synchronized (monitor) {
            offsets.forEach(completedButUncommitted::remove);
            completionsInCommit = Collections.emptySet();
        }
    }
}
