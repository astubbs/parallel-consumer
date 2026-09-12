package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * One topic's way out: what the instance produced onto it, read in the order it was produced.
 *
 * <h2>Reading CONSUMES</h2>
 * Every read here takes records off this object's queue, so a second read returns the next record rather than the
 * same one again, and {@link #queueSize()} counts what is left rather than what arrived. That is the behaviour of
 * the broker-free test drivers of the stream-processing libraries users compare us with, and taking it unchanged is
 * deliberate (KTD16): a test written against one of them reads the same way here. What it costs is that two
 * assertions over the same record have to hold it - {@code var record = readRecord();} - rather than read twice,
 * and a drain such as {@link #readRecordsToList()} leaves the object empty.
 * <p>
 * The queue is this object's, not the producer's: two output topics over the same sandbox and the same topic each
 * read the whole of it, because the cursor lives here.
 *
 * <h2>Every read needs a settle first, and refuses without one</h2>
 * Processing is concurrent here - polled on one thread, dispatched on a worker, produced from that worker - so a
 * read taken before the run has settled races the very work it is asking about, and the answer it would give is an
 * empty list. <b>An empty list is what "my function produced nothing" looks like</b>, so a test that read too early
 * would pass for the wrong reason and go on passing. Every method on this class therefore refuses, naming the topic
 * and the call to make, until the sandbox has settled since the last record went in - {@code awaitSettled()} on
 * either sandbox, or {@code awaitBound(...)} on a driven one. {@link #isEmpty()} and {@link #queueSize()} refuse on
 * the same terms, because "nothing came out" is exactly the claim that has to be earned.
 *
 * <h2>What fills it</h2>
 * The mock producer's history: every record the instance produced, whichever topic it went to, in order. On the
 * classic API that is what a {@code pollAndProduce} definition writes, and the sandbox's producer has to have been
 * built ({@code producer(keySerializer, valueSerializer)}) for there to be a history at all. On the fluent API it is
 * where exported records will arrive once export lands; until then a fluent definition produces nothing and a fluent
 * output topic is honestly empty. Nothing is decoded on the way out either way - the records come back as the
 * producer's own types, which on the fluent path is raw bytes, because that is what the engine under the facade
 * produces.
 * <p>
 * <b>An output topic is not checked against the definition's routes</b>, unlike an input topic: a definition
 * produces wherever its function says, and the sandbox has no register of where that might be.
 *
 * <h2>Two differences from their read side, both stated rather than hidden</h2>
 * <b>The size accessor has no {@code get} prefix.</b> Theirs is {@code getQueueSize}; the word is theirs, the prefix
 * is not, because nothing else on this module's surface carries one and a single {@code get} would read as an
 * oversight. A reader arriving from there finds {@link #queueSize()} by the word they remember.
 * <p>
 * <b>There is no key-and-value list.</b> Theirs pairs a key with a value in a type of its own that this project does
 * not have, and inventing one to mirror the method would add a type for information {@link #readRecordsToList()}
 * already carries - a record holds its key. {@link #readKeyValuesToMap()} is here, because a map is a type everybody
 * already has.
 *
 * @param <K> the key type the instance produces - the instance's own on the classic API, raw bytes on the fluent one
 * @param <V> the value type, the same way
 */
@InterfaceStability.Unstable
public final class SandboxOutputTopic<K, V> {

    /**
     * The topic this object reads, which is what it filters the producer's history by.
     */
    private final String topic;

    /**
     * Everything the instance has produced so far, across every topic, newest last. A supplier rather than a list,
     * because the history grows while the object lives and a snapshot taken at construction would report a run that
     * had not happened yet.
     */
    private final Supplier<List<ProducerRecord<K, V>>> produced;

    /**
     * Whether the sandbox has settled since the last record the caller piped in - the guard every read runs first.
     * A supplier for the same reason as the history: it is a question about now, not about construction time.
     */
    private final BooleanSupplier settled;

    /**
     * How many of this topic's records have already been read through this object. Guarded by this object's own
     * monitor, along with every method that reads or advances it, so that a test which reads from more than one
     * thread cannot hand the same record out twice.
     */
    private int read;

    /**
     * Package-private: an output topic is reached through {@code createOutputTopic} on a sandbox, which is what
     * knows where the produced records are and what "settled" means for that sandbox.
     */
    SandboxOutputTopic(String topic, Supplier<List<ProducerRecord<K, V>>> produced, BooleanSupplier settled) {
        this.topic = Objects.requireNonNull(topic, "A topic must be supplied");
        this.produced = Objects.requireNonNull(produced, "Somewhere to read the produced records from must be "
                + "supplied");
        this.settled = Objects.requireNonNull(settled, "A way to ask whether the run has settled must be supplied");
    }

    /**
     * The topic this object reads.
     */
    public String topic() {
        return topic;
    }

    /**
     * The next unread record's value, consuming it.
     *
     * @throws NoSuchElementException if this topic has nothing unread - {@link #isEmpty()} is the question that
     *                                does not throw
     * @throws IllegalStateException  if the run has not settled since the last record went in
     */
    public synchronized V readValue() {
        return readRecord().value();
    }

    /**
     * The next unread record, consuming it - key, value, topic and headers, as the instance produced it.
     *
     * @throws NoSuchElementException if this topic has nothing unread
     * @throws IllegalStateException  if the run has not settled since the last record went in
     */
    public synchronized ProducerRecord<K, V> readRecord() {
        List<ProducerRecord<K, V>> unread = unread();
        if (unread.isEmpty()) {
            throw new NoSuchElementException("Nothing left to read on " + topic + " - the instance produced "
                    + producedHere() + " record(s) there and all of them have been read through this output topic. "
                    + "Reading consumes, so a record asserted on twice has to be held rather than read twice; "
                    + "isEmpty() and queueSize() ask without throwing.");
        }
        read++;
        return unread.get(0);
    }

    /**
     * Every unread value on this topic, in the order produced, consuming them - so this object is empty afterwards.
     *
     * @throws IllegalStateException if the run has not settled since the last record went in
     */
    public synchronized List<V> readValuesToList() {
        List<V> values = new ArrayList<>();
        for (ProducerRecord<K, V> record : readRecordsToList()) {
            values.add(record.value());
        }
        return Collections.unmodifiableList(values);
    }

    /**
     * Every unread record on this topic, in the order produced, consuming them - so this object is empty
     * afterwards. The read that keeps the keys, the headers and the order together.
     *
     * @throws IllegalStateException if the run has not settled since the last record went in
     */
    public synchronized List<ProducerRecord<K, V>> readRecordsToList() {
        List<ProducerRecord<K, V>> unread = unread();
        read += unread.size();
        return Collections.unmodifiableList(unread);
    }

    /**
     * Every unread record on this topic as a key-to-value map, consuming them.
     * <p>
     * <b>A key produced more than once keeps its last value</b>, which is the same as theirs and is why
     * {@link #readRecordsToList()} is the read to prefer when a test cares that a key was produced twice: a map
     * cannot say so, and it will not fail to say so either.
     *
     * @throws IllegalStateException if the run has not settled since the last record went in
     */
    public synchronized Map<K, V> readKeyValuesToMap() {
        Map<K, V> byKey = new LinkedHashMap<>();
        for (ProducerRecord<K, V> record : readRecordsToList()) {
            byKey.put(record.key(), record.value());
        }
        return Collections.unmodifiableMap(byKey);
    }

    /**
     * How many of this topic's records are still unread through this object - zero once a drain has run, whatever
     * the instance produced.
     *
     * @throws IllegalStateException if the run has not settled since the last record went in, because "none left"
     *                               and "none yet" are the same number and only one of them is an answer
     */
    public synchronized int queueSize() {
        return unread().size();
    }

    /**
     * Whether anything on this topic is still unread.
     *
     * @throws IllegalStateException if the run has not settled since the last record went in
     */
    public synchronized boolean isEmpty() {
        return queueSize() == 0;
    }

    /**
     * This topic's records that have not been read through this object yet, oldest first - a copy, so advancing the
     * cursor afterwards cannot change what a caller was handed.
     */
    private List<ProducerRecord<K, V>> unread() {
        requireSettled();
        List<ProducerRecord<K, V>> mine = mine();
        if (mine.size() <= read) {
            // A fresh mutable list rather than an empty immutable one: this method has one caller shape and every
            // one of them hands the result on, so two different mutabilities out of two branches is a trap.
            return new ArrayList<>();
        }
        return new ArrayList<>(mine.subList(read, mine.size()));
    }

    /**
     * Every record produced onto this topic, read or not, oldest first.
     */
    private List<ProducerRecord<K, V>> mine() {
        List<ProducerRecord<K, V>> mine = new ArrayList<>();
        for (ProducerRecord<K, V> record : produced.get()) {
            if (topic.equals(record.topic())) {
                mine.add(record);
            }
        }
        return mine;
    }

    /**
     * How many records reached this topic altogether, for a refusal that would otherwise leave a reader unable to
     * tell "produced nothing" from "already read".
     */
    private int producedHere() {
        return mine().size();
    }

    /**
     * Refuses a read taken while work may still be in flight. The message says which call earns it, because the
     * two sandboxes reach the same state by different routes and a caller should not have to know which one it
     * holds to fix this.
     */
    private void requireSettled() {
        if (!settled.getAsBoolean()) {
            throw new IllegalStateException("Nothing has settled on this sandbox since the last record went in, so "
                    + "reading " + topic + " now would race the work it is asking about: this engine dispatches on "
                    + "a worker thread and produces from there, so a read taken here could come back empty while "
                    + "the function that will fill it has not run - and an empty read is indistinguishable from a "
                    + "definition that produced nothing. Call awaitSettled() first, or awaitBound(...) on a sandbox "
                    + "the driver is running.");
        }
    }
}
