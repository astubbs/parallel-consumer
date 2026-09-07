package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.collect.ImmutableSet;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * One conformance case, as {@link CaseLoader} hands it on: language-neutral data describing a topology, the records
 * fed through it, the perturbed twin that proves the differ can say no, and what is to be compared (R1, R2, R3, R8).
 * <p>
 * Instances are immutable and are only ever built by the loader, which has already applied every load-time rule - so
 * anything holding one of these is holding a case that passed all of them. There is no partially-valid case: a corpus
 * with any refusal fails as a whole.
 * <p>
 * The shape mirrors the wire's {@code BuilderCall} one-to-one (the {@code v1alpha1} Streams proto on the
 * {@code research/kafka-streams-foreign-wrappers} branch): a topology is an ORDERED list of entries, each entry
 * carries an author-chosen id standing in for the wire's server-minted handle, and each operation names its input
 * handle - or, for a join, both - by that id. A flat chain cannot express the corpus, because a join takes two inputs.
 * <p>
 * Two case classes live in one corpus. An <em>outcome</em> case is executed by the oracle and compared. A
 * <em>refusal</em> case (R15) declares the fault the wire must raise for an invalid specification, is flagged by
 * {@link #refusalClass()}, and is <strong>never executed on this rung</strong> - plain Kafka Streams never refuses
 * what this wire invented, so there is no oracle row to compute for one.
 */
public final class ConformanceCase {

    /**
     * The ten operations on the wrapper's builder surface (R2), each spelt as the case format spells it.
     * <p>
     * This is the module's own copy of an unfrozen proto that lives on another branch, so it is bounded to what this
     * module knows; reconciling it with the wire is a driver-rung obligation, not a claim this rung can make (KTD8).
     * The coverage gate (U5) and the oracle (U3) both hold the corpus against this enum rather than against a second
     * list of their own.
     */
    public enum OperationKind {

        SOURCE("source"),
        MAP_VALUES("map-values", "identity", "upper"),
        GROUP_BY_KEY("group-by-key"),
        COUNT("count"),
        REDUCE("reduce", "last-wins", "concat"),
        JOIN("join", "concat-sides"),
        WINDOWED_BY("windowed-by"),
        AGGREGATE("aggregate", "count-bytes", "concat"),
        TO_STREAM("to-stream"),
        SINK("sink");

        private final String spelling;

        // Guava's, not a wrapped HashSet: Error Prone's ImmutableEnumChecker reads the declared type, and an
        // unmodifiable view of a mutable set does not satisfy it however it was built.
        private final ImmutableSet<String> functionVocabulary;

        OperationKind(String spelling, String... functionVocabulary) {
            this.spelling = spelling;
            this.functionVocabulary = ImmutableSet.copyOf(functionVocabulary);
        }

        /** How the case format spells this operation - the YAML key, and what a refusal message names. */
        public String spelling() {
            return spelling;
        }

        /**
         * The closed set of function names this operation may name, or empty when it takes no function (KTD13).
         * <p>
         * Closed rather than open because the oracle and every driver must apply the <em>identical</em> function or
         * the comparison between them means nothing - which is why the vocabulary is fixed here beside the operation
         * rather than left to a case author.
         */
        public Set<String> functionVocabulary() {
            return functionVocabulary;
        }

        /** Whether this operation names a function from {@link #functionVocabulary()}. */
        public boolean takesFunction() {
            return !functionVocabulary.isEmpty();
        }

        /**
         * Whether this operation creates a state store on its own - the three aggregations. A topology holding none
         * of these and no sink can produce no final state, and R3 refuses it rather than letting it pass by
         * observing nothing.
         */
        public boolean createsStore() {
            return this == COUNT || this == REDUCE || this == AGGREGATE;
        }

        static OperationKind fromSpelling(String spelling) {
            for (OperationKind kind : values()) {
                if (kind.spelling.equals(spelling)) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("no operation is spelt " + spelling);
        }
    }

    /**
     * What a case asks to be compared (R3). {@code final-state} is the default and the only level this rung
     * implements; the loader refuses {@link #FINAL_STATE_AND_UPDATES} by name, because the update-stream observable,
     * its capture and its differ path are the driver rung's work (KTD5).
     */
    public enum Agreement {

        FINAL_STATE("final-state"),
        FINAL_STATE_AND_UPDATES("final-state+updates");

        private final String spelling;

        Agreement(String spelling) {
            this.spelling = spelling;
        }

        public String spelling() {
            return spelling;
        }

        static Agreement fromSpelling(String spelling) {
            for (Agreement level : values()) {
                if (level.spelling.equals(spelling)) {
                    return level;
                }
            }
            throw new IllegalArgumentException("no agreement level is spelt " + spelling);
        }
    }

    /**
     * A pinned emit rule (KTD5). One member today: the oracle suppresses a windowed aggregate until its window
     * closes. It is outside the wrapper's builder grammar - the one named exception to the otherwise one-to-one
     * translation between a case and a builder call - so a case naming it is oracle-only until the wrapper exposes an
     * emit control, and the coverage gate does not credit it toward binding coverage.
     */
    public enum EmitRule {

        ON_WINDOW_CLOSE("on-window-close");

        private final String spelling;

        EmitRule(String spelling) {
            this.spelling = spelling;
        }

        public String spelling() {
            return spelling;
        }

        static EmitRule fromSpelling(String spelling) {
            for (EmitRule rule : values()) {
                if (rule.spelling.equals(spelling)) {
                    return rule;
                }
            }
            throw new IllegalArgumentException("no emit rule is spelt " + spelling);
        }
    }

    /**
     * A time window, in milliseconds, carrying all four fields the wire requires. None of them defaults: the proto's
     * own {@code TimeWindowSpec} comment owns the reasoning, and retention is the sharpest case - Kafka's default is
     * {@code size + grace}, under which a one-hour window retains roughly the currently-open window and nothing else,
     * so a host that later range-read coexisting windows would find them gone for a reason with nothing to do with
     * its own code.
     */
    public static final class WindowSpec {

        private final long sizeMs;

        private final long advanceMs;

        private final long graceMs;

        private final long retentionMs;

        WindowSpec(long sizeMs, long advanceMs, long graceMs, long retentionMs) {
            this.sizeMs = sizeMs;
            this.advanceMs = advanceMs;
            this.graceMs = graceMs;
            this.retentionMs = retentionMs;
        }

        public long sizeMs() {
            return sizeMs;
        }

        /** The hop. Equal to {@link #sizeMs()} for a tumbling window. */
        public long advanceMs() {
            return advanceMs;
        }

        public long graceMs() {
            return graceMs;
        }

        public long retentionMs() {
            return retentionMs;
        }

        @Override
        public String toString() {
            return "window(size=" + sizeMs + "ms, advance=" + advanceMs + "ms, grace=" + graceMs + "ms, retention="
                    + retentionMs + "ms)";
        }
    }

    /**
     * One operation in a topology, with its input handles resolved to ids the loader has already checked exist.
     * <p>
     * Which of the optional members are set is fixed by {@link #kind()}: a source and a sink carry a topic, the three
     * store-creating operations carry a store name, a windowed-by carries a {@link WindowSpec}, and a
     * function-taking operation carries a {@code fn} from its kind's vocabulary. The loader refuses any other
     * combination, so a reader of a loaded case may take the shape for granted.
     */
    public static final class Operation {

        private final OperationKind kind;

        @Nullable
        private final String id;

        private final List<String> inputs;

        @Nullable
        private final String function;

        @Nullable
        private final String combine;

        @Nullable
        private final WindowSpec window;

        @Nullable
        private final String topic;

        @Nullable
        private final String store;

        Operation(OperationKind kind,
                  @Nullable String id,
                  List<String> inputs,
                  @Nullable String function,
                  @Nullable String combine,
                  @Nullable WindowSpec window,
                  @Nullable String topic,
                  @Nullable String store) {
            this.kind = kind;
            this.id = id;
            this.inputs = unmodifiableCopy(inputs);
            this.function = function;
            this.combine = combine;
            this.window = window;
            this.topic = topic;
            this.store = store;
        }

        public OperationKind kind() {
            return kind;
        }

        /**
         * The author-chosen handle id this operation mints, standing in for the wire's server-minted handle.
         * {@code null} for a sink alone, which mints nothing - the wire's own presence signal for "a handle was
         * minted", kept as one signal rather than two that could diverge.
         */
        @Nullable
        public String id() {
            return id;
        }

        /**
         * The ids this operation reads, in order. Empty for a source; one entry for every other operation except a
         * join, whose two entries are the stream side then the table side - see {@link #streamInput()}.
         */
        public List<String> inputs() {
            return inputs;
        }

        /** The stream side of a join. Only meaningful for {@link OperationKind#JOIN}. */
        @Nullable
        public String streamInput() {
            return inputs.get(0);
        }

        /** The table side of a join. Only meaningful for {@link OperationKind#JOIN}. */
        @Nullable
        public String tableInput() {
            return inputs.get(1);
        }

        /** The named function from this kind's {@link OperationKind#functionVocabulary()}, or {@code null}. */
        @Nullable
        public String function() {
            return function;
        }

        /**
         * The engine-executed combine an aggregate may name instead of a function, or {@code null}. The wire treats
         * the two as alternatives; a case setting both is describing two aggregations at once, and is refusal-class
         * data (R15) rather than something this loader resolves by precedence.
         */
        @Nullable
        public String combine() {
            return combine;
        }

        /** The window, for {@link OperationKind#WINDOWED_BY} only; {@code null} otherwise. */
        @Nullable
        public WindowSpec window() {
            return window;
        }

        /** The topic, for a source or a sink; {@code null} otherwise. */
        @Nullable
        public String topic() {
            return topic;
        }

        /** The store name, for a store-creating operation; {@code null} otherwise. */
        @Nullable
        public String store() {
            return store;
        }

        @Override
        public String toString() {
            return kind.spelling() + (id == null ? "" : "[" + id + "]");
        }
    }

    /**
     * One input record, timestamped explicitly relative to the case's base instant so nothing inherits wall-clock
     * time (R1). {@link #atMs()} is what the case file says; {@link #timestamp()} is that offset resolved against the
     * base instant, which is what the oracle pipes.
     */
    public static final class InputRecord {

        @Nullable
        private final String key;

        @Nullable
        private final String value;

        private final long atMs;

        private final Instant timestamp;

        private final String topic;

        InputRecord(@Nullable String key, @Nullable String value, long atMs, Instant timestamp, String topic) {
            this.key = key;
            this.value = value;
            this.atMs = atMs;
            this.timestamp = timestamp;
            this.topic = topic;
        }

        @Nullable
        public String key() {
            return key;
        }

        @Nullable
        public String value() {
            return value;
        }

        /** The offset from the case's base instant, in milliseconds - the number the case file carries. */
        public long atMs() {
            return atMs;
        }

        /** The absolute instant this record is piped at: the base instant plus {@link #atMs()}. */
        public Instant timestamp() {
            return timestamp;
        }

        /**
         * The source topic this record is piped to - always resolved, never absent. A case file may leave it out
         * only when the topology declares exactly one source, in which case the loader has already filled in that
         * source's topic; with any other number of sources the loader refuses a record that names none.
         */
        public String topic() {
            return topic;
        }

        @Override
        public String toString() {
            return "(" + key + ", " + value + ") at +" + atMs + "ms on " + topic;
        }
    }

    private final String name;

    private final Path sourceFile;

    private final Instant baseInstant;

    private final List<Operation> topology;

    private final List<InputRecord> inputs;

    private final List<InputRecord> perturbation;

    private final Agreement agreement;

    @Nullable
    private final EmitRule emit;

    @Nullable
    private final String expectsFault;

    private ConformanceCase(Builder builder) {
        // Required, and checked rather than assumed: the builder is package-private and only the loader drives it,
        // so a null here is a loader bug that must surface at the point of the mistake, not at a later read.
        this.name = Objects.requireNonNull(builder.name, "name");
        this.sourceFile = Objects.requireNonNull(builder.sourceFile, "sourceFile");
        this.baseInstant = Objects.requireNonNull(builder.baseInstant, "baseInstant");
        this.topology = unmodifiableCopy(builder.topology);
        this.inputs = unmodifiableCopy(builder.inputs);
        this.perturbation = unmodifiableCopy(builder.perturbation);
        this.agreement = builder.agreement;
        this.emit = builder.emit;
        this.expectsFault = builder.expectsFault;
    }

    /** Unique within the corpus (R1), so a red can name a case unambiguously. */
    public String name() {
        return name;
    }

    /** The file this case was read from - named alongside the case whenever a refusal is about the file itself. */
    public Path sourceFile() {
        return sourceFile;
    }

    /**
     * The instant every input offset is relative to. It sits past the window-clamp margin rather than at the epoch,
     * so a window arithmetic bug cannot hide behind a clamp at zero.
     */
    public Instant baseInstant() {
        return baseInstant;
    }

    /** The topology, in declaration order. Every operation's inputs name an id declared before it. */
    public List<Operation> topology() {
        return topology;
    }

    /** The records piped through the topology. Empty for a refusal-class case, which is never executed. */
    public List<InputRecord> inputs() {
        return inputs;
    }

    /**
     * The author-chosen perturbed twin of {@link #inputs()} (R8) - a perturbation this case's operations cannot
     * absorb. Executing it through the same oracle path is the positive control: it proves the whole pipeline from
     * execution to comparison is sensitive to its input, which perturbing a copy of the computed outcome would not.
     */
    public List<InputRecord> perturbation() {
        return perturbation;
    }

    /** What is compared. Never {@link Agreement#FINAL_STATE_AND_UPDATES} - the loader refuses that level. */
    public Agreement agreement() {
        return agreement;
    }

    /** The pinned emit rule, or {@code null} when the case names none (the default). */
    @Nullable
    public EmitRule emit() {
        return emit;
    }

    /**
     * The reserved foreign-call-log slot (R4), whose line format is the runner transcript of astubbs#390. Always
     * {@code null} on this rung: the slot is unset and not compared, and the driver rung fills it.
     */
    @Nullable
    public List<String> callLog() {
        return null;
    }

    /**
     * The fault the wire must raise for this case's invalid specification, or {@code null} for an outcome case
     * (R15).
     */
    @Nullable
    public String expectsFault() {
        return expectsFault;
    }

    /**
     * Whether this is a refusal-class case (R15) rather than an outcome case.
     * <p>
     * A refusal-class case is loaded into the same corpus and counted, and is <strong>never executed</strong> on this
     * rung: it declares a fault the wire must raise, and plain Kafka Streams - the oracle - never refuses what the
     * wire invented, so there is nothing to compute. The coverage gate counts these separately from outcome cases.
     */
    public boolean refusalClass() {
        return expectsFault != null;
    }

    @Override
    public String toString() {
        return name + " (" + (refusalClass() ? "refusal" : "outcome") + ", " + topology.size() + " ops)";
    }

    private static <T> List<T> unmodifiableCopy(List<T> source) {
        return Collections.unmodifiableList(new ArrayList<>(source));
    }

    /**
     * Assembles a case field by field. Package-private on purpose: only {@link CaseLoader} may build a case, because
     * only it has run the load-time rules that make the resulting shape safe to assume.
     */
    static final class Builder {

        @Nullable
        private String name;

        @Nullable
        private Path sourceFile;

        @Nullable
        private Instant baseInstant;

        private List<Operation> topology = Collections.emptyList();

        private List<InputRecord> inputs = Collections.emptyList();

        private List<InputRecord> perturbation = Collections.emptyList();

        private Agreement agreement = Agreement.FINAL_STATE;

        @Nullable
        private EmitRule emit;

        @Nullable
        private String expectsFault;

        Builder name(String value) {
            this.name = value;
            return this;
        }

        Builder sourceFile(Path value) {
            this.sourceFile = value;
            return this;
        }

        Builder baseInstant(@Nullable Instant value) {
            this.baseInstant = value;
            return this;
        }

        Builder topology(List<Operation> value) {
            this.topology = value;
            return this;
        }

        Builder inputs(List<InputRecord> value) {
            this.inputs = value;
            return this;
        }

        Builder perturbation(List<InputRecord> value) {
            this.perturbation = value;
            return this;
        }

        Builder agreement(Agreement value) {
            this.agreement = value;
            return this;
        }

        Builder emit(@Nullable EmitRule value) {
            this.emit = value;
            return this;
        }

        Builder expectsFault(@Nullable String value) {
            this.expectsFault = value;
            return this;
        }

        ConformanceCase build() {
            return new ConformanceCase(this);
        }
    }
}
