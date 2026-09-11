package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * What an operator can read about a set of parked records, and what they will be able to do about it (R28).
 * <p>
 * A view is a filter over the parked records the engine was holding when the handle was asked: it spans <b>every
 * partition of its route by default</b>, because a parked record is a record and an operator is looking for records,
 * not for partitions. {@link #partition(int)} narrows to one, which is the rare case, and {@link #byPartition()} is
 * the per-partition roll-out for a log line or a dashboard.
 * <p>
 * It is a value, taken when it was asked for. Holding one and reading it later gives the answer as it stood, which
 * is what makes {@link #count()} and {@link #records()} agree with each other.
 *
 * <h2>What is not available yet, and why</h2>
 * Three of R28's figures read empty in this version, and each is empty for the same reason: it is engine state with
 * no accessor. The payload fraction is computed inside the offset encoder at commit time and never stored; the
 * time-to-export estimate is derived from it; and the count of records held behind a parked key is known only to
 * its shard. All three arrive with the small-tier engine accessors, and until then an empty answer is the honest
 * one - a fabricated number an operator acted on would be worse than none.
 *
 * @see ConsumerHandle#topic(String)
 * @see ConsumerHandle#parkedAllTopics()
 */
@InterfaceStability.Unstable
public final class ParkedView {

    /**
     * Named once, so every refusal and every empty figure points at the same thing.
     */
    private static final String NEEDS_ENGINE_ACCESSORS =
            "it needs an engine accessor that does not exist yet, and is part of the next milestone of this work";

    /**
     * What this view is called in a log line or a {@link #toString()}, carried rather than derived from
     * {@link #topics} because a route declared over a topic set and the instance-wide roll-up are both several
     * topics and an operator needs to be able to tell them apart.
     */
    private final String name;

    /**
     * The topics this view is of, held so that a narrowed or rolled-out view keeps the scope it was taken with.
     * Unmodifiable and copied on the way in: a view is a value, and the caller's set is the caller's.
     */
    private final Set<String> topics;

    /**
     * Which partition this view is scoped to, or null for the whole route - the default.
     */
    private final Integer partition;

    /**
     * The records this view is over, already filtered to its topics and partition when it was built. Every figure
     * this class reports is derived from this one list, which is what makes {@link #count()} and {@link #records()}
     * answer about the same moment however long they are apart.
     */
    private final List<ParkedRecord> records;

    /**
     * When the engine's retry queue was read. Reported rather than kept private because an operator acting on a
     * parked set needs to know how old the figure in front of them is.
     */
    private final Instant takenAt;

    /**
     * The filtering form, used by the handle: it is handed everything parked and works out which of it is this
     * view's. Package-private because a view is only ever taken from a handle, never built by a user.
     */
    ParkedView(String name, Set<String> topics, Integer partition, List<ParkedRecord> allParked, Instant takenAt) {
        this(name, Collections.unmodifiableSet(new LinkedHashSet<>(topics)), partition, takenAt,
                filter(allParked, topics, partition));
    }

    /**
     * Every field as given, for a caller that has already worked out which records are this view's -
     * {@link #byPartition()}, which groups once instead of re-filtering the whole list per partition. The two
     * constructors differ by the order of their last two arguments, which is what lets the filtering one delegate
     * here.
     */
    private ParkedView(String name, Set<String> topics, Integer partition, Instant takenAt,
                       List<ParkedRecord> mine) {
        this.name = name;
        this.topics = topics;
        this.partition = partition;
        this.records = mine;
        this.takenAt = takenAt;
    }

    /**
     * Which of everything parked belongs to this view.
     *
     * @param partition null for the whole route, which is the default - a view spans every partition unless
     *                  somebody narrowed it
     * @return an unmodifiable list, so the view a caller holds cannot be changed under them by whoever built it
     */
    private static List<ParkedRecord> filter(List<ParkedRecord> allParked, Set<String> topics, Integer partition) {
        List<ParkedRecord> matching = new ArrayList<>();
        for (ParkedRecord parked : allParked) {
            if (topics.contains(parked.topic()) && (partition == null || partition == parked.partition())) {
                matching.add(parked);
            }
        }
        return Collections.unmodifiableList(matching);
    }

    /**
     * What this view is of: the route's topic, the route's topics when it was declared over a set, or the name the
     * instance roll-up carries.
     */
    public String name() {
        return name;
    }

    /**
     * Which topics' parked records this view's figures count, so a figure read from it can be attributed. A route
     * declared over a topic set yields one view covering all of them, not one view each.
     *
     * @return the topics this view covers - more than one when the route was declared over a topic set, and every
     * routed topic on the instance-wide roll-up
     */
    public Set<String> topics() {
        return topics;
    }

    /**
     * Whether this view was narrowed to one partition, which is what tells a reader whether its figures describe a
     * partition or the whole route. Empty is a real answer - the unnarrowed view - not a missing one.
     *
     * @return the partition this view was narrowed to, or empty when it spans every partition - which is the
     * default
     */
    public OptionalInt partition() {
        return partition == null ? OptionalInt.empty() : OptionalInt.of(partition);
    }

    /**
     * How many records are parked here right now.
     * <p>
     * <b>Not the same number as the parked meter's counter.</b> This is the size of the set an operator can act on;
     * the counter counts park <em>events</em> and never goes down (R19).
     */
    public int count() {
        return records.size();
    }

    /**
     * How long the oldest parked record here has been parked, or empty when nothing is parked.
     */
    public Optional<Duration> oldestAge() {
        Instant oldest = null;
        for (ParkedRecord parked : records) {
            if (oldest == null || parked.parkedSince().isBefore(oldest)) {
                oldest = parked.parkedSince();
            }
        }
        return oldest == null ? Optional.empty() : Optional.of(Duration.between(oldest, Instant.now()));
    }

    /**
     * The parked records themselves - offset, key, attempts, cycles, last failure, why, and since when (R28).
     */
    public List<ParkedRecord> records() {
        return records;
    }

    /**
     * When this view was taken. It is read from the engine's retry queue at the moment the handle is asked, so this
     * is how long ago that was rather than how stale a cached answer is.
     */
    public Instant takenAt() {
        return takenAt;
    }

    /**
     * Narrow to one partition - the rare case; the default view already spans them all.
     */
    public ParkedView partition(int partition) {
        if (this.partition != null && this.partition != partition) {
            throw new IllegalArgumentException(msg("This view is already narrowed to partition {}, so it cannot be "
                    + "narrowed to {} - take a fresh view from the handle", this.partition, partition));
        }
        return new ParkedView(name, topics, partition, records, takenAt);
    }

    /**
     * One view per partition that currently holds a parked record, in partition order. A partition with nothing
     * parked is not listed: this rolls out what is parked, it does not enumerate the assignment.
     */
    public List<ParkedView> byPartition() {
        // Grouped in one pass. Handing each new view the whole list and letting it filter made this quadratic -
        // one predicate evaluation per record per partition - on exactly the large parked sets an operator rolls
        // out per partition in order to read. A TreeMap keeps the partition order the TreeSet used to give.
        Map<Integer, List<ParkedRecord>> byPartition = new TreeMap<>();
        for (ParkedRecord parked : records) {
            byPartition.computeIfAbsent(parked.partition(), partition -> new ArrayList<>()).add(parked);
        }
        List<ParkedView> views = new ArrayList<>(byPartition.size());
        for (Map.Entry<Integer, List<ParkedRecord>> each : byPartition.entrySet()) {
            views.add(new ParkedView(name, topics, each.getKey(), takenAt,
                    Collections.unmodifiableList(each.getValue())));
        }
        return Collections.unmodifiableList(views);
    }

    // ---------------------------------------------------------------- not available until the engine accessors land

    /**
     * The partition's offset-map payload as a fraction of Kafka's commit-metadata cap - <b>empty in this
     * version</b>.
     *
     * @return always empty: the payload length is computed inside the offset encoder at commit time and never
     * stored, so there is nothing to read
     */
    public OptionalDouble payloadFraction() {
        return OptionalDouble.empty();
    }

    /**
     * How long until this partition's payload reaches the export percentage, at the current park rate - <b>empty in
     * this version</b>, because it is derived from {@link #payloadFraction()}.
     */
    public Optional<Duration> estimatedTimeToExport() {
        return Optional.empty();
    }

    /**
     * How many records are waiting behind this parked record because they share its key under key ordering - the
     * blast-radius figure (R28). <b>Empty in this version</b>: only the record's shard knows, and the shard has no
     * accessor for it.
     */
    public OptionalInt heldBehind(ParkedRecord parked) {
        // The argument is deliberately unread: there is nothing to look it up in. Kept on the signature because
        // the figure is per-record, and a method that gained its parameter later would break every caller.
        return OptionalInt.empty();
    }

    // ---------------------------------------------------------------- the two commands

    /**
     * Attempt this record again now.
     *
     * @throws UnsupportedOperationException always in this version - resuming means telling the engine to bring a
     *                                       record's retry time forward, and it has no command for that
     */
    public void resume(ParkedRecord parked) {
        throw notYetSupported("resume", parked);
    }

    /**
     * Attempt every record in this view again now.
     *
     * @see #resume(ParkedRecord)
     */
    public void resume() {
        throw notYetSupported("resume", null);
    }

    /**
     * Send this record to the route's dead-letter destination now.
     *
     * @throws UnsupportedOperationException always in this version - export is a re-dispatch on a later pass, and
     *                                       this version refuses a dead-letter destination at start for the same
     *                                       reason
     */
    public void dlq(ParkedRecord parked) {
        throw notYetSupported("dlq", parked);
    }

    /**
     * Send every record in this view to the route's dead-letter destination now.
     *
     * @see #dlq(ParkedRecord)
     */
    public void dlq() {
        throw notYetSupported("dlq", null);
    }

    /**
     * The one refusal both commands raise, written once so that the four entry points cannot drift into four
     * accounts of the same absence. It says what is missing, and then says that nothing is lost while it is
     * missing - a park holds no worker and keeps the record in the offset map - because the question an operator
     * asks next is whether they have to act now.
     *
     * @param parked the record asked for, or null on the whole-view form, which names none
     */
    private UnsupportedOperationException notYetSupported(String command, ParkedRecord parked) {
        return new UnsupportedOperationException(msg("{} is not supported in this version{}: {}. Parked records "
                        + "stay incomplete in the offset map and hold no worker, so nothing is lost while you "
                        + "wait; a restart re-delivers them, which is the way to re-attempt one today (R28).",
                command, parked == null ? "" : msg(" (asked for {})", parked), NEEDS_ENGINE_ACCESSORS));
    }

    /**
     * What {@link #toString()} prints for a figure that has no answer yet - said once, so the two cannot drift into
     * two spellings of the same absence.
     */
    private static final String NOT_AVAILABLE = "not available";

    /**
     * Renders one figure for {@link #toString()}, routing every absent figure through the single
     * {@link #NOT_AVAILABLE} wording. It exists so the four figures on that line cannot come to spell the same
     * absence in different ways.
     *
     * @return the figure, or {@link #NOT_AVAILABLE} - never the bare {@code OptionalDouble.empty} rendering, which
     * reads to an operator as a figure of zero rather than as a figure nobody has
     */
    private static String describe(OptionalDouble figure) {
        return figure.isPresent() ? String.valueOf(figure.getAsDouble()) : NOT_AVAILABLE;
    }

    /**
     * The whole view on one line, for a log statement about a parked set. It names the view, its partition when it
     * has one, and the four figures R28 asks for - including the ones that read empty in this version, because a
     * line that silently dropped them would look like a complete answer.
     */
    @Override
    public String toString() {
        // Derived rather than hardcoded: the two figures below read empty in this version, and when the engine
        // accessors land and they start answering, this line has to start answering with them.
        return "ParkedView(" + name + (partition == null ? "" : ", partition=" + partition) + ", count=" + count()
                + ", oldest=" + oldestAge().map(Duration::toString).orElse("none")
                + ", payloadFraction=" + describe(payloadFraction())
                + ", estimatedTimeToExport=" + estimatedTimeToExport().map(Duration::toString)
                .orElse(NOT_AVAILABLE) + ")";
    }
}
