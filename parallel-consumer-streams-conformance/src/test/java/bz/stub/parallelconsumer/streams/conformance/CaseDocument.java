package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.List;

/**
 * The raw YAML shape, exactly as a case file is written, before any load-time rule has run.
 * <p>
 * It is deliberately the dumbest possible mirror of the file: every field is nullable, nothing is defaulted, and
 * nothing validates. That is what lets {@link CaseLoader} tell "the author omitted this" from "the author wrote a
 * zero" - a boxed {@code Long} distinguishes an absent {@code at-ms} from {@code at-ms: 0}, which a primitive could
 * not, and the missing-timestamp rule (R1) is exactly that distinction. {@link ConformanceCase} is the typed,
 * immutable, already-checked form the rest of the module sees; nothing outside this package's loader touches this.
 * <p>
 * <strong>Plain final classes, never Java records.</strong> Error Prone crashes on Jabel-desugared records, the
 * finding astubbs#387 recorded; the module compiles Java 17 source to Java 8 bytecode, so a record here breaks the
 * build in a way that reads as unrelated. Fields are package-private and bound by field access - the mapper sets
 * {@code FIELD} visibility to {@code ANY} - so there are no setters to keep in step either.
 * <p>
 * Field names are camel case and reach YAML as kebab case through the mapper's naming strategy: {@code baseInstant}
 * is {@code base-instant}, {@code atMs} is {@code at-ms}, {@code groupByKey} is {@code group-by-key}. Unknown
 * properties fail: a typo in a case file is a malformed case, not a field to ignore.
 */
final class CaseDocument {

    @Nullable
    String name;

    @Nullable
    String baseInstant;

    @Nullable
    List<Entry> topology;

    @Nullable
    List<Record> inputs;

    @Nullable
    List<Record> perturbation;

    @Nullable
    String agreement;

    @Nullable
    String emit;

    /** Reserved (R4). Unset on this rung and not compared; the driver rung fills it. */
    @Nullable
    List<String> callLog;

    @Nullable
    String expectsFault;

    /**
     * One topology entry: an id, plus exactly one operation. The ten operation fields mirror the wire's
     * {@code BuilderCall} {@code oneof} - which is why they are ten sibling fields rather than a kind plus a bag of
     * arguments. Setting none of them, or more than one, is refused by name.
     */
    static final class Entry {

        @Nullable
        String id;

        @Nullable
        Source source;

        @Nullable
        Unary mapValues;

        @Nullable
        Unary groupByKey;

        @Nullable
        Stateful count;

        @Nullable
        Stateful reduce;

        @Nullable
        Join join;

        @Nullable
        Windowed windowedBy;

        @Nullable
        Aggregate aggregate;

        @Nullable
        Unary toStream;

        @Nullable
        Sink sink;
    }

    static final class Source {

        @Nullable
        String topic;
    }

    /**
     * An operation reading one handle and naming nothing else. {@code fn} is carried here even for the kinds that
     * take no function, so that naming one is a <em>refusal</em> rather than an unknown-property parse error - the
     * message a case author needs says which function name was not in which kind's vocabulary.
     */
    static final class Unary {

        @Nullable
        String of;

        @Nullable
        String fn;
    }

    /** An operation reading one handle and creating a store. */
    static final class Stateful {

        @Nullable
        String of;

        @Nullable
        String fn;

        @Nullable
        String store;
    }

    /** The windowed-by. All four window fields are boxed so that "missing" is distinguishable from zero. */
    static final class Windowed {

        @Nullable
        String of;

        @Nullable
        Long sizeMs;

        @Nullable
        Long advanceMs;

        @Nullable
        Long graceMs;

        @Nullable
        Long retentionMs;
    }

    /** The windowed aggregation. {@code fn} and {@code combine} are the wire's alternatives; see R15. */
    static final class Aggregate {

        @Nullable
        String of;

        @Nullable
        String fn;

        @Nullable
        String combine;

        @Nullable
        String store;
    }

    /** The one non-linear operation: two handles in, one out. */
    static final class Join {

        @Nullable
        String stream;

        @Nullable
        String table;

        @Nullable
        String fn;
    }

    static final class Sink {

        @Nullable
        String of;

        @Nullable
        String topic;
    }

    static final class Record {

        @Nullable
        String key;

        @Nullable
        String value;

        @Nullable
        Long atMs;
    }
}
