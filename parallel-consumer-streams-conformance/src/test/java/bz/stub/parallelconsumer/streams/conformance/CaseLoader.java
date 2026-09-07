package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reads a directory of YAML case files into {@link ConformanceCase}s, applying every load-time rule in one pass.
 * <p>
 * <strong>A corpus with any refusal fails as a whole.</strong> There is no code path here that returns a partially
 * loaded corpus without throwing: either {@link #load(Path)} hands back cases that passed every rule, or it throws a
 * {@link CorpusRefusedException} listing <em>every</em> refusal it found. That is deliberate - a loader that
 * quietly dropped the cases it could not read would turn a malformed corpus into a smaller green run, which is the
 * silent failure this whole rung exists to prevent. One unreadable file therefore refuses the corpus, but it does
 * <em>not</em> abort the pass: the remaining files are still read, so one broken file produces one refusal rather
 * than hiding every rule the rest of the directory would have tripped.
 * <p>
 * Every refusal names the case, and names the part of it the rule is about - the field, the handle id, the record,
 * or, when the case has no readable name yet, the file. A maintainer has to be able to act on the message without
 * re-running anything.
 *
 * <h2>The rules</h2>
 *
 * <ul>
 *     <li><b>Readable</b> - a file that does not parse, or holds no document, is refused by file name (the rest of
 *     the directory is still read); a case with no name is refused by file name too.</li>
 *     <li><b>Unique names</b> (R1) - two files declaring one name are refused, naming the name and both files.</li>
 *     <li><b>Explicit timestamps</b> (R1) - every input and perturbation record carries {@code at-ms}, so no case
 *     inherits wall-clock time. A missing one names the record.</li>
 *     <li><b>Resolved topics</b> - every input and perturbation record is piped to a topic some source declares. A
 *     record may leave {@code topic} out only when the topology declares exactly one source, whose topic it then
 *     takes; a record naming none under any other source count is refused naming the record, and one naming a topic
 *     no source declares is refused naming the topic.</li>
 *     <li><b>Resolvable handles</b> (KTD2) - every operation names an input handle some <em>earlier</em> entry
 *     declared; naming none, or naming an unknown id, is refused with the id. A join names two <em>distinct</em>
 *     handles, because two handles going in is what makes it the one non-linear operation.</li>
 *     <li><b>Closed function vocabulary</b> (KTD13) - a function-taking operation names one of its kind's functions
 *     and an operation that takes none names nothing; either violation names the case and the name.</li>
 *     <li><b>Whole windows</b> - a {@code windowed-by} carries all four window fields, none defaulted; a missing one
 *     names the field.</li>
 *     <li><b>Agreement</b> (R3, AE5) - {@code final-state+updates} is refused as not yet implemented on this rung,
 *     whether or not the case also names an emit rule.</li>
 *     <li><b>Pinned emit</b> (KTD5) - see {@link #checkPinnedEmitHasATrailingRecord} for the exact rule.</li>
 *     <li><b>Observable final state</b> (R3, AE8) - a topology with no store-creating operation and no sink can
 *     produce no final state, so it would pass every proof by observing nothing.</li>
 *     <li><b>Case class</b> (R15) - a refusal-class case names its fault and carries no inputs, twin or agreement;
 *     an outcome case carries inputs and the twin (R8) and no fault.</li>
 * </ul>
 *
 * An empty directory yields an empty corpus and no refusal. Whether an empty corpus is itself a failure is U4's
 * call, at the gate, where "the run executed nothing and reported green" is actually decidable.
 */
public final class CaseLoader {

    /**
     * The engine-executed combines an aggregate may name instead of a function, mirroring the wire's
     * {@code CombineKind}. Held here rather than beside {@link ConformanceCase.OperationKind}'s function vocabulary
     * because nothing on this rung ever executes one: a case naming a combine at all is refusal-class data, since
     * the alternation between a host function and an engine combine is something the wire invented and plain Kafka
     * Streams has no opinion about.
     */
    private static final Set<String> COMBINE_VOCABULARY =
            Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList("last-bytes", "append-bytes")));

    private CaseLoader() {
    }

    /**
     * Reads every {@code *.yaml} file directly under {@code directory}, in file-name order.
     *
     * @return the loaded cases, ordered by case name; empty when the directory holds no case files
     * @throws CorpusRefusedException if any case or file was refused - listing every refusal, not just the first
     */
    public static List<ConformanceCase> load(Path directory) {
        List<String> refusals = new ArrayList<>();
        List<ConformanceCase> loaded = new ArrayList<>();
        Map<String, Path> filesByCaseName = new LinkedHashMap<>();
        ObjectMapper mapper = mapper();

        for (Path file : yamlFilesIn(directory)) {
            CaseDocument document;
            try {
                document = mapper.readValue(file.toFile(), CaseDocument.class);
            } catch (IOException e) {
                // Refuses the corpus, but does not abort the pass: the files after this one are still read, so a
                // single broken file cannot hide every other rule the directory would have tripped.
                refusals.add("file " + file.getFileName() + " cannot be read as a case: " + firstLineOf(e));
                continue;
            }
            if (document == null) {
                refusals.add("file " + file.getFileName() + " holds no case document");
                continue;
            }
            String declaredName = document.name;
            if (declaredName == null || declaredName.trim().isEmpty()) {
                refusals.add("file " + file.getFileName() + " declares no name; a case names itself so a red can "
                        + "name it back");
                continue;
            }
            Path alreadySeenIn = filesByCaseName.put(declaredName, file);
            if (alreadySeenIn != null) {
                refusals.add("case " + declaredName + " is declared in two files: " + alreadySeenIn.getFileName()
                        + " and " + file.getFileName() + "; case names are unique within a corpus so a red can name "
                        + "one unambiguously");
                continue;
            }

            ConformanceCase built = buildCase(document, declaredName, file, refusals);
            if (built != null) {
                loaded.add(built);
            }
        }

        if (!refusals.isEmpty()) {
            throw new CorpusRefusedException(refusals, namesOf(loaded));
        }
        loaded.sort((left, right) -> left.name().compareTo(right.name()));
        return Collections.unmodifiableList(loaded);
    }

    /**
     * {@link #load(Path)} against a directory on the test classpath, which is how the module reaches its own
     * resources - {@code cases} for the corpus, {@code invalid-cases/<rule>} for the fixtures.
     */
    public static List<ConformanceCase> loadClasspathDirectory(String resourceName) {
        URL resource = CaseLoader.class.getClassLoader().getResource(resourceName);
        if (resource == null) {
            throw new IllegalArgumentException("no directory on the test classpath at " + resourceName);
        }
        try {
            return load(Paths.get(resource.toURI()));
        } catch (java.net.URISyntaxException e) {
            throw new IllegalArgumentException("cannot resolve the classpath directory " + resourceName, e);
        }
    }

    // ------------------------------------------------------------------------------------------ one case

    /** @return the case, or {@code null} when any rule refused it - in which case {@code refusals} has grown */
    @Nullable
    private static ConformanceCase buildCase(CaseDocument document,
                                             String name,
                                             Path file,
                                             List<String> refusals) {
        int refusalsBefore = refusals.size();

        Instant baseInstant = null;
        String declaredBaseInstant = document.baseInstant;
        if (declaredBaseInstant == null || declaredBaseInstant.trim().isEmpty()) {
            refuse(refusals, name, file, "declares no base-instant; every record timestamp is relative to one, and "
                    + "it sits past the window-clamp margin rather than at the epoch");
        } else {
            try {
                baseInstant = Instant.parse(declaredBaseInstant);
            } catch (DateTimeParseException e) {
                refuse(refusals, name, file, "base-instant " + declaredBaseInstant + " is not an ISO-8601 instant");
            }
        }

        List<ConformanceCase.Operation> topology = resolveTopology(document, name, file, refusals);

        ConformanceCase.Agreement agreement = ConformanceCase.Agreement.FINAL_STATE;
        if (document.agreement != null) {
            try {
                agreement = ConformanceCase.Agreement.fromSpelling(document.agreement);
            } catch (IllegalArgumentException e) {
                refuse(refusals, name, file, "declares agreement level " + document.agreement + ", which is not one "
                        + "of " + spellingsOf(ConformanceCase.Agreement.values()));
            }
            if (agreement == ConformanceCase.Agreement.FINAL_STATE_AND_UPDATES) {
                refuse(refusals, name, file, "declares agreement level final-state+updates, which is not yet "
                        + "implemented on this rung - the update-stream observable, its capture and its differ path "
                        + "are the driver rung's work, and naming an emit rule does not buy the level (R3, KTD5)");
            }
        }

        ConformanceCase.EmitRule emit = null;
        if (document.emit != null) {
            try {
                emit = ConformanceCase.EmitRule.fromSpelling(document.emit);
            } catch (IllegalArgumentException e) {
                refuse(refusals, name, file, "declares emit rule " + document.emit + ", which is not one of "
                        + spellingsOf(ConformanceCase.EmitRule.values()));
            }
        }

        if (document.callLog != null) {
            refuse(refusals, name, file, "declares a call-log, which is reserved and unset on this rung; the driver "
                    + "rung fills it (R4)");
        }

        List<String> sourceTopics = sourceTopicsOf(topology);
        List<ConformanceCase.InputRecord> inputs =
                resolveRecords(document.inputs, "input record", baseInstant, sourceTopics, name, file, refusals);
        List<ConformanceCase.InputRecord> perturbation = resolveRecords(document.perturbation,
                "perturbation record", baseInstant, sourceTopics, name, file, refusals);

        boolean refusalClass = !isBlank(document.expectsFault);
        if (refusalClass) {
            // R15: it declares the fault the wire must raise for an invalid specification. There is no outcome to
            // compute, so carrying inputs, a twin or an agreement level would be describing one anyway.
            if (document.inputs != null || document.perturbation != null || document.agreement != null) {
                refuse(refusals, name, file, "expects the fault " + document.expectsFault + " and also declares "
                        + "inputs, a perturbation or an agreement level; a refusal-class case carries the fault in "
                        + "place of all three, because it is never executed (R15)");
            }
        } else {
            if (inputs.isEmpty()) {
                refuse(refusals, name, file, "declares no input records; an outcome case with nothing piped through "
                        + "it observes nothing");
            }
            if (perturbation.isEmpty()) {
                refuse(refusals, name, file, "declares no perturbation; the author-chosen perturbed twin is what "
                        + "proves the differ can say no on this case (R8)");
            }
            checkSomeFinalStateIsObservable(topology, name, file, refusals);
            if (emit == ConformanceCase.EmitRule.ON_WINDOW_CLOSE) {
                checkPinnedEmitHasATrailingRecord(topology, inputs, baseInstant, name, file, refusals);
            }
        }

        if (refusals.size() != refusalsBefore) {
            return null;
        }
        return new ConformanceCase.Builder()
                .name(name)
                .sourceFile(file)
                .baseInstant(baseInstant)
                .topology(topology)
                .inputs(inputs)
                .perturbation(perturbation)
                .agreement(agreement)
                .emit(emit)
                .expectsFault(document.expectsFault)
                .build();
    }

    // ------------------------------------------------------------------------------------------ topology

    private static List<ConformanceCase.Operation> resolveTopology(CaseDocument document,
                                                                   String name,
                                                                   Path file,
                                                                   List<String> refusals) {
        List<ConformanceCase.Operation> topology = new ArrayList<>();
        if (document.topology == null || document.topology.isEmpty()) {
            refuse(refusals, name, file, "declares an empty topology");
            return topology;
        }

        // Ids are collected as the list is walked, so an operation may only name a handle an EARLIER entry declared.
        // A topology is ordered exactly as the wire's builder calls are, and a forward reference would be a call
        // naming a handle the server has not minted yet.
        Set<String> declaredSoFar = new LinkedHashSet<>();
        for (int position = 0; position < document.topology.size(); position++) {
            CaseDocument.Entry entry = document.topology.get(position);
            ConformanceCase.Operation operation = resolveOperation(entry, position, name, file, refusals);
            if (operation == null) {
                continue;
            }
            for (String input : operation.inputs()) {
                if (input == null || !declaredSoFar.contains(input)) {
                    refuse(refusals, name, file, operation + " names input handle " + input + ", which no earlier "
                            + "entry declares");
                }
            }
            String streamSide = operation.kind() == ConformanceCase.OperationKind.JOIN
                    ? operation.streamInput()
                    : null;
            if (streamSide != null && streamSide.equals(operation.tableInput())) {
                refuse(refusals, name, file, operation + " names the same handle " + streamSide
                        + " on both sides; a join reads two distinct handles, which is what makes it the one "
                        + "non-linear operation in the grammar");
            }
            String mintedId = operation.id();
            if (mintedId != null && !declaredSoFar.add(mintedId)) {
                refuse(refusals, name, file, operation + " re-declares handle id " + mintedId);
            }
            topology.add(operation);
        }
        return topology;
    }

    /** @return the operation, or {@code null} when the entry could not be read as one at all */
    @Nullable
    private static ConformanceCase.Operation resolveOperation(CaseDocument.Entry entry,
                                                              int position,
                                                              String name,
                                                              Path file,
                                                              List<String> refusals) {
        String where = "topology entry " + (position + 1) + (entry.id == null ? "" : " (" + entry.id + ")");

        // The ten operation fields mirror the wire's BuilderCall oneof, so exactly one of them is set.
        Map<ConformanceCase.OperationKind, Object> set = new LinkedHashMap<>();
        putIfSet(set, ConformanceCase.OperationKind.SOURCE, entry.source);
        putIfSet(set, ConformanceCase.OperationKind.MAP_VALUES, entry.mapValues);
        putIfSet(set, ConformanceCase.OperationKind.GROUP_BY_KEY, entry.groupByKey);
        putIfSet(set, ConformanceCase.OperationKind.COUNT, entry.count);
        putIfSet(set, ConformanceCase.OperationKind.REDUCE, entry.reduce);
        putIfSet(set, ConformanceCase.OperationKind.JOIN, entry.join);
        putIfSet(set, ConformanceCase.OperationKind.WINDOWED_BY, entry.windowedBy);
        putIfSet(set, ConformanceCase.OperationKind.AGGREGATE, entry.aggregate);
        putIfSet(set, ConformanceCase.OperationKind.TO_STREAM, entry.toStream);
        putIfSet(set, ConformanceCase.OperationKind.SINK, entry.sink);

        if (set.isEmpty()) {
            refuse(refusals, name, file, where + " names no operation");
            return null;
        }
        if (set.size() > 1) {
            refuse(refusals, name, file, where + " names more than one operation: " + spellingsOf(set.keySet()));
            return null;
        }

        ConformanceCase.OperationKind kind = set.keySet().iterator().next();
        if (kind == ConformanceCase.OperationKind.SINK) {
            if (entry.id != null) {
                refuse(refusals, name, file, where + " gives a sink an id; a sink mints no handle, which is the "
                        + "wire's one presence signal for whether a call minted one");
            }
        } else if (entry.id == null || entry.id.trim().isEmpty()) {
            refuse(refusals, name, file, where + " declares no id; every handle-minting operation carries one, "
                    + "because later operations name it");
            return null;
        }

        // Each branch re-states the check above as a requireNonNull: the map holds exactly one operation, so the
        // field this kind names is present, and asserting it beats reading the guarantee off a line further up.
        switch (kind) {
            case SOURCE: {
                CaseDocument.Source source = Objects.requireNonNull(entry.source);
                return operation(kind, entry.id, Collections.emptyList(), null, null, null,
                        required(source.topic, "topic", where, name, file, refusals), null);
            }
            case MAP_VALUES: {
                CaseDocument.Unary mapValues = Objects.requireNonNull(entry.mapValues);
                return unaryOperation(kind, entry.id, mapValues.of, mapValues.fn, where, name, file, refusals);
            }
            case GROUP_BY_KEY: {
                CaseDocument.Unary groupByKey = Objects.requireNonNull(entry.groupByKey);
                return unaryOperation(kind, entry.id, groupByKey.of, groupByKey.fn, where, name, file, refusals);
            }
            case TO_STREAM: {
                CaseDocument.Unary toStream = Objects.requireNonNull(entry.toStream);
                return unaryOperation(kind, entry.id, toStream.of, toStream.fn, where, name, file, refusals);
            }
            case COUNT: {
                CaseDocument.Stateful count = Objects.requireNonNull(entry.count);
                return statefulOperation(kind, entry.id, count.of, count.fn, count.store, where, name, file,
                        refusals);
            }
            case REDUCE: {
                CaseDocument.Stateful reduce = Objects.requireNonNull(entry.reduce);
                return statefulOperation(kind, entry.id, reduce.of, reduce.fn, reduce.store, where, name, file,
                        refusals);
            }
            case AGGREGATE:
                return aggregateOperation(Objects.requireNonNull(entry.aggregate), entry.id, where, name, file,
                        refusals);
            case JOIN:
                return joinOperation(Objects.requireNonNull(entry.join), entry.id, where, name, file, refusals);
            case WINDOWED_BY:
                return windowedByOperation(Objects.requireNonNull(entry.windowedBy), entry.id, where, name, file,
                        refusals);
            case SINK: {
                CaseDocument.Sink sink = Objects.requireNonNull(entry.sink);
                return operation(kind, null, Collections.singletonList(sink.of), null, null, null,
                        required(sink.topic, "topic", where, name, file, refusals), null);
            }
            default:
                throw new IllegalStateException("unhandled operation kind " + kind);
        }
    }

    private static ConformanceCase.Operation unaryOperation(ConformanceCase.OperationKind kind,
                                                            @Nullable String id,
                                                            @Nullable String of,
                                                            @Nullable String function,
                                                            String where,
                                                            String name,
                                                            Path file,
                                                            List<String> refusals) {
        return operation(kind, id, Collections.singletonList(of),
                checkedFunction(kind, function, where, name, file, refusals), null, null, null, null);
    }

    private static ConformanceCase.Operation statefulOperation(ConformanceCase.OperationKind kind,
                                                               @Nullable String id,
                                                               @Nullable String of,
                                                               @Nullable String function,
                                                               @Nullable String store,
                                                               String where,
                                                               String name,
                                                               Path file,
                                                               List<String> refusals) {
        return operation(kind, id, Collections.singletonList(of),
                checkedFunction(kind, function, where, name, file, refusals), null, null, null,
                required(store, "store", where, name, file, refusals));
    }

    private static ConformanceCase.Operation aggregateOperation(CaseDocument.Aggregate aggregate,
                                                                @Nullable String id,
                                                                String where,
                                                                String name,
                                                                Path file,
                                                                List<String> refusals) {
        if (aggregate.combine != null && !COMBINE_VOCABULARY.contains(aggregate.combine)) {
            refuse(refusals, name, file, where + " names combine " + aggregate.combine + ", which is not one of "
                    + COMBINE_VOCABULARY);
        }
        return operation(ConformanceCase.OperationKind.AGGREGATE, id, Collections.singletonList(aggregate.of),
                checkedFunction(ConformanceCase.OperationKind.AGGREGATE, aggregate.fn, where, name, file, refusals),
                aggregate.combine, null, null, required(aggregate.store, "store", where, name, file, refusals));
    }

    private static ConformanceCase.Operation joinOperation(CaseDocument.Join join,
                                                           @Nullable String id,
                                                           String where,
                                                           String name,
                                                           Path file,
                                                           List<String> refusals) {
        return operation(ConformanceCase.OperationKind.JOIN, id, Arrays.asList(join.stream, join.table),
                checkedFunction(ConformanceCase.OperationKind.JOIN, join.fn, where, name, file, refusals),
                null, null, null, null);
    }

    private static ConformanceCase.Operation windowedByOperation(CaseDocument.Windowed windowed,
                                                                 @Nullable String id,
                                                                 String where,
                                                                 String name,
                                                                 Path file,
                                                                 List<String> refusals) {
        // All four, always, none defaulted: every default here is a trap, and the sharpest is retention - Kafka's
        // own default of size + grace retains roughly the currently-open window and nothing else.
        long size = requiredWindowField(windowed.sizeMs, "size-ms", true, where, name, file, refusals);
        long advance = requiredWindowField(windowed.advanceMs, "advance-ms", true, where, name, file, refusals);
        long grace = requiredWindowField(windowed.graceMs, "grace-ms", false, where, name, file, refusals);
        long retention = requiredWindowField(windowed.retentionMs, "retention-ms", true, where, name, file, refusals);
        return operation(ConformanceCase.OperationKind.WINDOWED_BY, id,
                Collections.singletonList(windowed.of), null, null,
                new ConformanceCase.WindowSpec(size, advance, grace, retention), null, null);
    }

    private static ConformanceCase.Operation operation(ConformanceCase.OperationKind kind,
                                                       @Nullable String id,
                                                       List<String> inputs,
                                                       @Nullable String function,
                                                       @Nullable String combine,
                                                       @Nullable ConformanceCase.WindowSpec window,
                                                       @Nullable String topic,
                                                       @Nullable String store) {
        return new ConformanceCase.Operation(kind, id, inputs, function, combine, window, topic, store);
    }

    @Nullable
    private static String checkedFunction(ConformanceCase.OperationKind kind,
                                          @Nullable String function,
                                          String where,
                                          String name,
                                          Path file,
                                          List<String> refusals) {
        if (!kind.takesFunction()) {
            if (function != null) {
                refuse(refusals, name, file, where + " names function " + function + ", but " + kind.spelling()
                        + " takes none");
            }
            return null;
        }
        if (function == null) {
            refuse(refusals, name, file, where + " names no fn; " + kind.spelling() + " takes one of "
                    + kind.functionVocabulary());
            return null;
        }
        if (!kind.functionVocabulary().contains(function)) {
            refuse(refusals, name, file, where + " names function " + function + ", which is not in "
                    + kind.spelling() + "'s vocabulary " + kind.functionVocabulary() + "; the vocabulary is closed "
                    + "because the oracle and every driver must apply the identical function");
            return null;
        }
        return function;
    }

    private static long requiredWindowField(@Nullable Long value,
                                            String field,
                                            boolean mustBePositive,
                                            String where,
                                            String name,
                                            Path file,
                                            List<String> refusals) {
        if (value == null) {
            refuse(refusals, name, file, where + " omits " + field + "; all four window fields are always present "
                    + "and never defaulted, because every default here is a trap");
            return mustBePositive ? 1 : 0;
        }
        if (mustBePositive ? value <= 0 : value < 0) {
            refuse(refusals, name, file, where + " gives " + field + " as " + value + ", which is out of range");
            return mustBePositive ? 1 : 0;
        }
        return value;
    }

    // ------------------------------------------------------------------------------------------- records

    private static List<ConformanceCase.InputRecord> resolveRecords(@Nullable List<CaseDocument.Record> documentRecords,
                                                                    String what,
                                                                    @Nullable Instant baseInstant,
                                                                    List<String> sourceTopics,
                                                                    String name,
                                                                    Path file,
                                                                    List<String> refusals) {
        List<ConformanceCase.InputRecord> records = new ArrayList<>();
        if (documentRecords == null) {
            return records;
        }
        for (int index = 0; index < documentRecords.size(); index++) {
            CaseDocument.Record record = documentRecords.get(index);
            String where = what + " " + (index + 1) + " (key " + record.key + ")";
            Long atMs = record.atMs;
            if (atMs == null) {
                refuse(refusals, name, file, where + " omits at-ms; every record carries an explicit timestamp "
                        + "relative to the case's base instant, so no case inherits wall-clock time (R1)");
                continue;
            }
            String topic = resolveTopic(record.topic, sourceTopics, where, name, file, refusals);
            if (topic == null || baseInstant == null) {
                continue;
            }
            records.add(new ConformanceCase.InputRecord(record.key, record.value, atMs,
                    baseInstant.plusMillis(atMs), topic));
        }
        return records;
    }

    /**
     * Which source topic a record is piped to (R1's sibling: nothing about a run may be implicit).
     * <p>
     * A record MAY name a {@code topic}. When the topology declares exactly one source it may leave it out and take
     * that source's topic; with any other number there is no obvious default, and inventing one would silently
     * decide which side of a join a record feeds - which is precisely the thing a conformance case has to state
     * rather than inherit. A topic no source declares is refused whatever the source count, because a record piped
     * to a topic nothing reads simply vanishes.
     *
     * @return the resolved topic, or {@code null} when the record was refused - in which case {@code refusals} has
     *         grown and the record is dropped rather than built with a topic nobody chose
     */
    @Nullable
    private static String resolveTopic(@Nullable String declared,
                                       List<String> sourceTopics,
                                       String where,
                                       String name,
                                       Path file,
                                       List<String> refusals) {
        if (declared == null) {
            if (sourceTopics.size() == 1) {
                return sourceTopics.get(0);
            }
            refuse(refusals, name, file, where + " names no topic, and the topology declares " + sourceTopics.size()
                    + " sources " + sourceTopics + "; a record may leave its topic to the default only when there is "
                    + "exactly one source, because with more than one the default would silently decide which side "
                    + "of a join the record feeds");
            return null;
        }
        if (!sourceTopics.contains(declared)) {
            refuse(refusals, name, file, where + " names topic " + declared + ", which no source declares; the "
                    + "topology's sources are " + sourceTopics + ", and a record piped to a topic nothing reads "
                    + "vanishes without observing anything");
            return null;
        }
        return declared;
    }

    /** The topics the topology's sources declare, in declaration order; a source whose topic was refused is out. */
    private static List<String> sourceTopicsOf(List<ConformanceCase.Operation> topology) {
        return topology.stream()
                .filter(operation -> operation.kind() == ConformanceCase.OperationKind.SOURCE)
                .map(ConformanceCase.Operation::topic)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    // --------------------------------------------------------------------------------- the outcome rules

    /**
     * AE8, R3: a topology holding no store-creating operation and producing no sink record has an empty final state,
     * so it would pass determinism and every other proof by observing nothing at all.
     */
    private static void checkSomeFinalStateIsObservable(List<ConformanceCase.Operation> topology,
                                                        String name,
                                                        Path file,
                                                        List<String> refusals) {
        boolean anyStore = topology.stream().anyMatch(op -> op.kind().createsStore());
        boolean anySink = topology.stream().anyMatch(op -> op.kind() == ConformanceCase.OperationKind.SINK);
        if (!anyStore && !anySink) {
            refuse(refusals, name, file, "has no store-creating operation and no sink, so its final state is empty; "
                    + "a case may not pass by observing nothing (R3, AE8)");
        }
    }

    /**
     * KTD5: a pinned-emit case must contain a record that actually closes a window, or it observes nothing about
     * emit while passing every proof on its store alone.
     * <p>
     * <b>The exact rule.</b> Take each {@code windowed-by} in the topology, with size {@code S}, advance {@code A}
     * and grace {@code G}. A record at absolute time {@code t} opens windows up to and including the one starting at
     * {@code floor(t / A) * A}, so the latest window it opens ends at {@code floor(t / A) * A + S}. Under
     * close-driven suppression Kafka evicts a suppressed window when observed stream time reaches its end plus the
     * grace period, and {@code TopologyTestDriver} never advances stream time on {@code close()} - so the case is
     * accepted only when it holds a record at time {@code t} with
     * <p>
     * {@code t >= G + max(window end over every record STRICTLY EARLIER than t)}
     * <p>
     * The "strictly earlier" is load-bearing: measured against every record including {@code t} itself the condition
     * is unsatisfiable, because {@code t} opens a window of its own that ends after {@code t}. Records sharing a
     * timestamp are considered together, since neither is earlier than the other.
     */
    private static void checkPinnedEmitHasATrailingRecord(List<ConformanceCase.Operation> topology,
                                                          List<ConformanceCase.InputRecord> inputs,
                                                          @Nullable Instant baseInstant,
                                                          String name,
                                                          Path file,
                                                          List<String> refusals) {
        List<ConformanceCase.Operation> windows = topology.stream()
                .filter(op -> op.kind() == ConformanceCase.OperationKind.WINDOWED_BY)
                .collect(Collectors.toList());
        if (windows.isEmpty()) {
            refuse(refusals, name, file, "names emit on-window-close but has no windowed-by, so there is no window "
                    + "for a close to be driven by");
            return;
        }
        if (baseInstant == null) {
            return;
        }

        // Distinct, ascending: records sharing a timestamp are neither earlier nor later than each other, and a
        // window end depends only on the timestamp, so one entry per distinct time is the whole population.
        Set<Long> distinctAscendingTimes = new TreeSet<>();
        for (ConformanceCase.InputRecord record : inputs) {
            boolean ignoredAlreadyPresent = distinctAscendingTimes.add(record.timestamp().toEpochMilli());
        }

        for (ConformanceCase.Operation windowedBy : windows) {
            // A windowed-by always carries one - the loader refuses one that does not before it ever gets here.
            ConformanceCase.WindowSpec window = Objects.requireNonNull(windowedBy.window());
            long latestCloseSoFar = Long.MIN_VALUE;
            boolean satisfied = false;
            for (long time : distinctAscendingTimes) {
                if (latestCloseSoFar != Long.MIN_VALUE && time >= latestCloseSoFar + window.graceMs()) {
                    satisfied = true;
                    break;
                }
                long latestWindowEnd = Math.floorDiv(time, window.advanceMs()) * window.advanceMs() + window.sizeMs();
                latestCloseSoFar = Math.max(latestCloseSoFar, latestWindowEnd);
            }
            if (!satisfied) {
                long wouldNeed = latestCloseSoFar == Long.MIN_VALUE
                        ? window.sizeMs()
                        : latestCloseSoFar + window.graceMs() - baseInstant.toEpochMilli();
                refuse(refusals, name, file, "names emit on-window-close, but no input record sits at or past the "
                        + "close of a window an earlier record opens: " + windowedBy + " " + window + " would need a "
                        + "record at at-ms " + wouldNeed + " or later, and without one nothing is ever emitted - "
                        + "TopologyTestDriver does not advance stream time on close()");
            }
        }
    }

    // ---------------------------------------------------------------------------------------------- plumbing

    private static void refuse(List<String> refusals, String name, Path file, String what) {
        refusals.add("case " + name + " (" + file.getFileName() + ") " + what);
    }

    @Nullable
    private static String required(@Nullable String value, String field, String where, String name, Path file,
                                   List<String> refusals) {
        if (isBlank(value)) {
            refuse(refusals, name, file, where + " declares no " + field);
            return null;
        }
        return value;
    }

    private static void putIfSet(Map<ConformanceCase.OperationKind, Object> set,
                                 ConformanceCase.OperationKind kind,
                                 @Nullable Object value) {
        if (value != null) {
            Object ignoredPrevious = set.put(kind, value); // one entry per kind, so there is never a previous
        }
    }

    private static List<Path> yamlFilesIn(Path directory) {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.filter(path -> path.getFileName().toString().endsWith(".yaml"))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot read the case directory " + directory, e);
        }
    }

    private static ObjectMapper mapper() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        // Case files are kebab case; CaseDocument's fields are camel case. Binding is by FIELD, so there are no
        // setters to keep in step, and an unknown property fails rather than being ignored - a typo in a case file
        // is a malformed case, not a field to skip.
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(Visibility.ANY)
                .withGetterVisibility(Visibility.NONE)
                .withIsGetterVisibility(Visibility.NONE)
                .withSetterVisibility(Visibility.NONE)
                .withCreatorVisibility(Visibility.NONE));
        return mapper;
    }

    private static List<String> namesOf(List<ConformanceCase> cases) {
        return cases.stream().map(ConformanceCase::name).collect(Collectors.toList());
    }

    private static String spellingsOf(ConformanceCase.Agreement[] values) {
        return Arrays.stream(values).map(ConformanceCase.Agreement::spelling).collect(Collectors.joining(", "));
    }

    private static String spellingsOf(ConformanceCase.EmitRule[] values) {
        return Arrays.stream(values).map(ConformanceCase.EmitRule::spelling).collect(Collectors.joining(", "));
    }

    private static String spellingsOf(Set<ConformanceCase.OperationKind> kinds) {
        return kinds.stream().map(ConformanceCase.OperationKind::spelling).collect(Collectors.joining(", "));
    }

    private static String firstLineOf(Exception e) {
        String message = e.getMessage();
        if (message == null) {
            return e.getClass().getSimpleName();
        }
        int newline = message.indexOf('\n');
        return newline < 0 ? message : message.substring(0, newline);
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }

    /**
     * Thrown when a corpus holds anything the loader refuses - carrying <em>every</em> refusal, so one run of the
     * loader tells a maintainer everything to fix rather than one thing at a time.
     */
    public static final class CorpusRefusedException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private final List<String> refusals;

        private final List<String> loadedCaseNames;

        CorpusRefusedException(List<String> refusals, List<String> loadedCaseNames) {
            super(refusals.size() + " refusal(s) loading the corpus:" + System.lineSeparator()
                    + String.join(System.lineSeparator(), refusals));
            this.refusals = Collections.unmodifiableList(new ArrayList<>(refusals));
            this.loadedCaseNames = Collections.unmodifiableList(new ArrayList<>(loadedCaseNames));
        }

        /** One line per refusal, each naming the case (or the file, when the case had no readable name). */
        public List<String> refusals() {
            return refusals;
        }

        /**
         * The cases that did load before the corpus was refused - never a usable corpus, and exposed only so a
         * caller can show that one bad file did not abort the pass over the rest.
         */
        public List<String> loadedCaseNames() {
            return loadedCaseNames;
        }
    }
}
