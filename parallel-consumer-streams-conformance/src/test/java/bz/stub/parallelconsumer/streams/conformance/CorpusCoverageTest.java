package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.conformance.ConformanceCase.OperationKind;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The coverage gate: the corpus is held against {@link BuilderSurface}, and an operation counts as covered only when
 * the oracle's <em>translated topology</em> contains a node for it (R16, KTD8).
 *
 * <h2>The five assertions</h2>
 *
 * <ol>
 *     <li><b>Every builder operation has an outcome case, or a reasoned entry in {@link #DELIBERATELY_UNCOVERED}.</b>
 *     Reported as ONE failure naming every uncovered operation, because a gate that reddens on the first one makes
 *     filling the corpus an exercise in re-running the build.</li>
 *     <li><b>Every case names only operations the surface knows.</b> With the surface derived from
 *     {@link OperationKind}, a loaded case cannot name anything else - see
 *     {@link #anOperationSpellingTheSurfaceDoesNotKnowIsRefusedAtLoad}, which is where that claim is actually
 *     falsifiable.</li>
 *     <li><b>Every outcome case is non-vacuous</b>: at least one input record, and a final state with at least one
 *     store or sink holding an entry. Refusal-class cases are counted separately and never executed (R15).</li>
 *     <li><b>A deliberately-uncovered entry whose operation has gained a case fails</b>, naming the entry - so the
 *     exclusion list cannot go stale.</li>
 *     <li><b>Translation</b>: an operation a case names but the oracle's topology does not contain is a red naming
 *     the case and the operation. This is what stops an oracle that silently dropped an operation from passing a
 *     gate that only read the YAML.</li>
 * </ol>
 *
 * <h2>What green means today: nothing this module knows ships uncovered</h2>
 *
 * {@link #UNCOVERED_TODAY} is <b>empty</b>, and {@link #theUncoveredOperationSetIsEmpty} asserts exactly that: every
 * one of the ten builder operations is credited by an outcome case in the committed corpus. It was not always - the
 * set was pinned to the four operations the corpus had no case for while U6 was writing them, and it reddened in
 * either direction while it was, so the gate could not drift quietly as the corpus grew. U6 emptied it, and the same
 * assertion now reads as the property rather than the leftover: an operation losing its credit reddens here, and so
 * does a new operation arriving on the surface without a case.
 * <p>
 * {@link #DELIBERATELY_UNCOVERED} is <b>empty and must stay empty</b>: an exclusion means "this operation is
 * deliberately not covered", and padding the list to go green would convert a corpus gap into a decision nobody made.
 *
 * <h2>Everything is testable without the real corpus</h2>
 *
 * {@link #coverage(List, Map, Function)} takes its corpus, its exclusion list and its oracle as arguments, so each
 * finding can be driven from a hand-built corpus and, where the finding is about what the oracle built, from a stub
 * oracle. The real-corpus tests then run the identical function on
 * {@link CaseLoader#loadClasspathDirectory(String)} and {@link Oracle#run}.
 *
 * @see BuilderSurface
 */
class CorpusCoverageTest {

    /** The committed corpus - the same directory {@link CorpusGateTest} executes. */
    private static final String CORPUS = CorpusGateTest.CORPUS;

    /**
     * Operations this module knows and deliberately does not cover, each with the reason it is excused.
     * <p>
     * <b>Empty, and that is not an oversight.</b> Nothing on the builder surface is deliberately uncovered, and
     * nothing is uncovered at all - {@link #theUncoveredOperationSetIsEmpty} is where that is asserted. An entry
     * added here to make the gate green would turn a corpus gap into a recorded decision, which is precisely the lie
     * this list exists to prevent.
     */
    private static final ImmutableMap<OperationKind, String> DELIBERATELY_UNCOVERED = ImmutableMap.of();

    /**
     * The operations no outcome case in the committed corpus credits. <b>Empty since U6 filled the corpus</b>, and
     * an entry appearing here again is a corpus gap to fill rather than a set to update: the assertion below is what
     * makes that gap fail the build.
     * <p>
     * Two things do not count towards a credit, and the corpus's own cases are where that is observed rather than
     * asserted in the abstract. A refusal-class case (R15) is never executed, so it credits nothing - which is why
     * {@code aggregate} stayed uncovered while the only case naming it was the refusal-class one. A pinned-emit case
     * (KTD5) is oracle-only, so it credits nothing either, and every operation
     * {@code windowed-aggregate-emitted-on-window-close} names is covered by another case.
     */
    private static final ImmutableSet<OperationKind> UNCOVERED_TODAY = ImmutableSet.of();

    // ============================================================== the five assertions, on the real corpus

    /** Assertion 1: no operation this module knows ships without a case. */
    @Test
    void theUncoveredOperationSetIsEmpty() {
        Coverage coverage = coverage(realCorpus(), DELIBERATELY_UNCOVERED, Oracle::run);

        assertWithMessage("the corpus covers %s of the %s builder operations, and the uncovered set must be EMPTY - "
                        + "U6 emptied it, and an operation appearing in it again is a case to write. Do NOT make it "
                        + "green by adding entries to DELIBERATELY_UNCOVERED - an exclusion means 'deliberately not "
                        + "covered', which is a decision, not a gap. Remember what does NOT credit: a refusal-class "
                        + "case is never executed and an emit-pinned case is oracle-only. What credits what: %s",
                BuilderSurface.all().size() - coverage.uncovered().size(), BuilderSurface.all().size(),
                BuilderSurface.table())
                .that(coverage.uncovered()).containsExactlyElementsIn(UNCOVERED_TODAY);
    }

    /** The exclusion list starts empty (KTD8), and nothing about filling the corpus is a reason to grow it. */
    @Test
    void theDeliberatelyUncoveredListIsEmpty() {
        assertWithMessage("an entry here is a decision that an operation will not be covered, with a reason. "
                + "Adding one to go green converts a gap in the corpus into a claim nobody made")
                .that(DELIBERATELY_UNCOVERED).isEmpty();
    }

    /** Assertion 2: structural, given the surface is derived from the enum the loader binds against. */
    @Test
    void everyCaseNamesOnlyOperationsTheSurfaceKnows() {
        Coverage coverage = coverage(realCorpus(), DELIBERATELY_UNCOVERED, Oracle::run);

        assertWithMessage("a case naming an operation the surface does not know would be uncoverable by "
                + "construction; the falsifiable half of this claim is the loader's refusal, tested below")
                .that(coverage.unknownOperations()).isEmpty();
    }

    /** Assertion 3: no outcome case observes nothing, and refusal-class cases are counted, never executed. */
    @Test
    void everyOutcomeCaseIsNonVacuousAndNoRefusalCaseIsExecuted() {
        List<String> executed = new ArrayList<>();
        Coverage coverage = coverage(realCorpus(), DELIBERATELY_UNCOVERED, conformanceCase -> {
            executed.add(conformanceCase.name());
            return Oracle.run(conformanceCase);
        });

        assertWithMessage("a case that observes nothing passes every proof by observing nothing")
                .that(coverage.vacuousCases()).isEmpty();
        assertWithMessage("the oracle threw for a case the gate had to execute")
                .that(coverage.oracleFailures()).isEmpty();
        assertWithMessage("the corpus holds outcome cases at all - a corpus of refusal-class cases alone would "
                + "leave every assertion here true over nothing")
                .that(coverage.outcomeCases()).isGreaterThan(0);
        assertWithMessage("the corpus holds refusal-class cases at all, so the claim below is about something")
                .that(refusalCaseNames(realCorpus())).isNotEmpty();
        assertWithMessage("every refusal-class case is counted separately - against the corpus's own flags rather "
                + "than a number written here, which would need editing every time a case is added and would say "
                + "nothing while it was right")
                .that(coverage.refusalCases()).isEqualTo(refusalCaseNames(realCorpus()).size());
        assertWithMessage("and is never executed: plain Kafka Streams never refuses what the wire invented, so "
                + "there is no oracle row to compute for one (R15)")
                .that(executed).containsNoneIn(refusalCaseNames(realCorpus()));
    }

    /** Assertion 4: an exclusion for an operation that has gained a case is stale and must fail. */
    @Test
    void noDeliberatelyUncoveredEntryHasGainedACase() {
        Coverage coverage = coverage(realCorpus(), DELIBERATELY_UNCOVERED, Oracle::run);

        assertWithMessage("an exclusion whose operation is now covered is a stale claim, and a stale exclusion "
                + "silently excuses an operation the corpus already exercises")
                .that(coverage.staleExclusions()).isEmpty();
    }

    /** Assertion 5: every operation a case names is in the topology the oracle built for it (KTD8). */
    @Test
    void everyOperationACaseNamesIsInTheTopologyTheOracleBuilt() {
        Coverage coverage = coverage(realCorpus(), DELIBERATELY_UNCOVERED, Oracle::run);

        assertWithMessage("coverage measures what the oracle BUILT, not what the YAML said - an operation named "
                + "by a case and absent from its topology is an oracle that dropped it")
                .that(coverage.namedButNotBuilt()).isEmpty();
    }

    // ================================================================ the same function, on hand-built corpora

    /** Happy path: three cases between them exercise all ten operations, and the gate passes with no exclusions. */
    @Test
    void aCorpusExercisingEveryOperationLeavesNothingUncovered(@TempDir Path directory) {
        Coverage coverage = coverage(corpus(directory, COUNT_WITH_MAP_VALUES, WINDOWED_AGGREGATE, JOIN_OVER_A_REDUCE),
                Collections.emptyMap(), Oracle::run);

        assertWithMessage("all ten operations are named and built; what credits what: %s", BuilderSurface.table())
                .that(coverage.uncovered()).isEmpty();
        assertThat(coverage.namedButNotBuilt()).isEmpty();
        assertThat(coverage.vacuousCases()).isEmpty();
        assertThat(coverage.outcomeCases()).isEqualTo(3);
        assertThat(coverage.refusalCases()).isEqualTo(0);
    }

    /** Edge: an operation with only a refusal-class case is uncovered - a refusal case is never executed. */
    @Test
    void anOperationWithOnlyARefusalClassCaseCountsAsUncovered(@TempDir Path directory) {
        Coverage coverage = coverage(corpus(directory, COUNT_WITH_MAP_VALUES, AGGREGATE_REFUSAL),
                Collections.emptyMap(), Oracle::run);

        assertWithMessage("the refusal case names aggregate, and it is still uncovered: a case that is never "
                + "executed cannot credit an operation")
                .that(coverage.uncovered()).contains(OperationKind.AGGREGATE);
        assertThat(coverage.refusalCases()).isEqualTo(1);
        assertThat(coverage.outcomeCases()).isEqualTo(1);
    }

    /** Edge (KTD5): the oracle-only pinned-emit case is counted, and credits nothing toward binding coverage. */
    @Test
    void theOracleOnlyPinnedEmitCaseCreditsNothing(@TempDir Path directory) {
        Coverage coverage = coverage(corpus(directory, PINNED_EMIT_WINDOWED_COUNT), Collections.emptyMap(),
                Oracle::run);

        assertWithMessage("emit: on-window-close is outside the wrapper's builder grammar, so a binding cannot "
                + "exercise it and the case must not credit windowed-by toward binding coverage (KTD5)")
                .that(coverage.uncovered()).containsAtLeast(OperationKind.WINDOWED_BY, OperationKind.COUNT);
        assertWithMessage("but it IS counted - an oracle-only case is a case, not a case that vanished")
                .that(coverage.outcomeCases()).isEqualTo(1);
        assertWithMessage("and its topology still has to contain what it names: oracle-only excuses the credit, "
                + "never the translation check")
                .that(coverage.namedButNotBuilt()).isEmpty();
    }

    /** Error: an exclusion for an operation that has a case fails, naming the entry. */
    @Test
    void anExclusionForAnOperationThatHasACaseIsStale(@TempDir Path directory) {
        Map<OperationKind, String> stale = Collections.singletonMap(OperationKind.COUNT,
                "no corpus case counts anything yet");

        Coverage coverage = coverage(corpus(directory, COUNT_WITH_MAP_VALUES), stale, Oracle::run);

        assertWithMessage("count is credited by a case, so excusing it is a claim that is no longer true")
                .that(coverage.staleExclusions()).containsExactly(OperationKind.COUNT);
        assertWithMessage("and a stale exclusion must not also excuse the operation from the uncovered set - it "
                + "would then be excused by a claim the same run just refuted")
                .that(coverage.uncovered()).doesNotContain(OperationKind.COUNT);
    }

    /** And an exclusion for an operation that genuinely has no case excuses it, which is what the list is for. */
    @Test
    void anExclusionForAnOperationWithNoCaseExcusesIt(@TempDir Path directory) {
        Map<OperationKind, String> excused = Collections.singletonMap(OperationKind.JOIN,
                "the join corpus lands with the driver rung");

        Coverage coverage = coverage(corpus(directory, COUNT_WITH_MAP_VALUES), excused, Oracle::run);

        assertThat(coverage.uncovered()).doesNotContain(OperationKind.JOIN);
        assertThat(coverage.staleExclusions()).isEmpty();
    }

    /**
     * Error, and the arm for assertion 5: an oracle that drops one operation from the topology it builds is caught
     * naming the case and the operation - even though its outcome is unchanged.
     * <p>
     * The stub is what makes this a <em>unit</em> test of the crediting rule. The corresponding sabotage arm moves a
     * term in the real {@link Oracle} and is recorded in the PR body; a stub could never stand in for that, because
     * the thing under test there is the oracle, not this function.
     */
    @Test
    void anOracleThatDropsAnOperationFailsNamingTheCaseAndTheOperation(@TempDir Path directory) {
        List<ConformanceCase> loaded = corpus(directory, COUNT_WITH_MAP_VALUES);

        Coverage coverage = coverage(loaded, Collections.emptyMap(), withoutTheSink(Oracle::run));

        assertWithMessage("the sink is named by the case and missing from the topology, so it is a translation "
                + "red naming both")
                .that(coverage.namedButNotBuilt()).hasSize(1);
        assertThat(coverage.namedButNotBuilt().get(0)).contains("count-with-map-values");
        assertThat(coverage.namedButNotBuilt().get(0)).contains("sink");
        assertWithMessage("and the operation is uncovered as well, because a credit is a node in the built "
                + "topology and there is no longer one")
                .that(coverage.uncovered()).contains(OperationKind.SINK);
    }

    /** A case whose final state holds nothing is vacuous, whatever its topology promised. */
    @Test
    void aCaseWhoseFinalStateIsEmptyIsVacuous(@TempDir Path directory) {
        List<ConformanceCase> loaded = corpus(directory, COUNT_WITH_MAP_VALUES);

        Coverage coverage = coverage(loaded, Collections.emptyMap(),
                conformanceCase -> new FinalState(emptyObservable("counts"), emptyObservable("out"),
                        Oracle.run(conformanceCase).topologyDescription()));

        assertWithMessage("a store and a sink that are both present and both empty observe nothing, and every "
                + "assertion over nothing passes")
                .that(coverage.vacuousCases()).hasSize(1);
        assertThat(coverage.vacuousCases().get(0)).contains("count-with-map-values");
    }

    /**
     * The other half of vacuity: a case with no input record. The loader already refuses that shape, so this is the
     * one test in the module that builds a {@link ConformanceCase} directly rather than through
     * {@link CaseLoader} - the check exists as a second line of defence, and a check nothing can redden is not a
     * check.
     */
    @Test
    void aCaseWithNoInputRecordIsVacuous() {
        ConformanceCase pipeNothing = new ConformanceCase.Builder()
                .name("pipes-nothing")
                .sourceFile(Paths.get("pipes-nothing.yaml"))
                .baseInstant(Instant.EPOCH)
                .topology(Collections.emptyList())
                .build();

        Coverage coverage = coverage(Collections.singletonList(pipeNothing), Collections.emptyMap(),
                conformanceCase -> new FinalState(Collections.singletonMap("counts",
                        Collections.singletonList("\"a\" -> 1")), Collections.emptyMap(), ""));

        assertWithMessage("a case with nothing piped through it cannot be exercising anything, however full its "
                + "final state looks")
                .that(coverage.vacuousCases()).hasSize(1);
        assertThat(coverage.vacuousCases().get(0)).contains("pipes-nothing");
    }

    /**
     * Assertion 2's falsifiable half: the surface cannot be given an operation it does not know, because the loader
     * refuses an unknown operation spelling before a case is ever built.
     */
    @Test
    void anOperationSpellingTheSurfaceDoesNotKnowIsRefusedAtLoad(@TempDir Path directory) {
        CaseLoader.CorpusRefusedException thrown = assertThrows(CaseLoader.CorpusRefusedException.class,
                () -> corpus(directory, ""
                        + "name: flat-mapped\n"
                        + "base-instant: 1970-01-01T02:00:00Z\n"
                        + "topology:\n"
                        + "  - {id: in, source: {topic: in}}\n"
                        + "  - {id: f, flat-map: {of: in}}\n"
                        + "inputs:\n"
                        + "  - {key: a, value: x, at-ms: 0}\n"
                        + "perturbation:\n"
                        + "  - {key: b, value: x, at-ms: 0}\n"));

        assertWithMessage("the loader binds a case against the same ten operations the surface holds, so an "
                + "eleventh cannot reach the gate at all")
                .that(thrown).hasMessageThat().contains("flat-map");
    }

    /** The surface and the case format's operation list are one list, not two that could drift. */
    @Test
    void theSurfaceIsExactlyTheCaseFormatsOperations() {
        assertWithMessage("a second list of operations would let the gate hold the corpus against a surface the "
                + "loader does not enforce")
                .that(BuilderSurface.all()).containsExactlyElementsIn(OperationKind.values()).inOrder();
        for (OperationKind kind : BuilderSurface.all()) {
            assertWithMessage("%s must have at least one crediting node, or it can never be covered", kind)
                    .that(BuilderSurface.creditingNodePrefixes(kind)).isNotEmpty();
        }
    }

    // ================================================================================== the coverage function

    /**
     * Holds the corpus against the builder surface and reports every finding, rather than throwing on the first.
     *
     * @param corpus                the loaded cases - outcome and refusal-class alike
     * @param deliberatelyUncovered operations excused from coverage, each with its reason
     * @param oracle                what computes an outcome case's final state; the real {@link Oracle#run} on the
     *                              committed corpus, and a stub where a test needs to control what was built
     * @return every finding, so one run tells a maintainer everything to fix
     */
    static Coverage coverage(List<ConformanceCase> corpus,
                             Map<OperationKind, String> deliberatelyUncovered,
                             Function<ConformanceCase, FinalState> oracle) {
        Set<OperationKind> surface = new LinkedHashSet<>(BuilderSurface.all());
        Map<OperationKind, List<String>> creditedBy = new EnumMap<>(OperationKind.class);
        surface.forEach(kind -> creditedBy.put(kind, new ArrayList<>()));

        List<String> unknownOperations = new ArrayList<>();
        List<String> namedButNotBuilt = new ArrayList<>();
        List<String> vacuousCases = new ArrayList<>();
        List<String> oracleFailures = new ArrayList<>();
        int outcomeCases = 0;
        int refusalCases = 0;

        for (ConformanceCase conformanceCase : corpus) {
            if (conformanceCase.refusalClass()) {
                // Counted, never executed (R15): it declares the fault the wire must raise, and plain Kafka
                // Streams never refuses what the wire invented, so it credits nothing.
                refusalCases++;
                continue;
            }
            outcomeCases++;

            Set<OperationKind> named = new LinkedHashSet<>();
            for (ConformanceCase.Operation operation : conformanceCase.topology()) {
                if (surface.contains(operation.kind())) {
                    boolean ignoredAlreadyNamed = named.add(operation.kind());
                    // The set is what matters, not whether this was the first mention of the kind in the case.
                } else {
                    unknownOperations.add(conformanceCase.name() + " names " + operation.kind()
                            + ", which the builder surface does not know");
                }
            }

            if (conformanceCase.inputs().isEmpty()) {
                vacuousCases.add(conformanceCase.name() + " pipes no input record, so whatever it observes was "
                        + "not produced by this case");
            }

            FinalState state;
            try {
                state = oracle.apply(conformanceCase);
            } catch (Oracle.OracleExecutionException e) {
                oracleFailures.add(conformanceCase.name() + ": " + e.getMessage());
                continue;
            }

            if (isEmpty(state)) {
                vacuousCases.add(conformanceCase.name() + " produced a final state with no entry in any store or "
                        + "sink, so every comparison over it passes by comparing nothing: " + state);
            }

            // KTD5: a pinned-emit case is oracle-only. It is executed and counted, and its translation is still
            // checked, but it credits nothing toward binding coverage - no binding can exercise an emit rule the
            // wrapper's builder surface does not expose.
            boolean oracleOnly = conformanceCase.emit() != null;

            for (OperationKind kind : named) {
                if (BuilderSurface.creditedBy(kind, state.topologyDescription())) {
                    if (!oracleOnly) {
                        creditedBy.get(kind).add(conformanceCase.name());
                    }
                } else {
                    namedButNotBuilt.add(conformanceCase.name() + " names " + kind.spelling() + " and the topology "
                            + "the oracle built for it contains no crediting node - " + BuilderSurface
                            .creditExplanation(kind) + ". Either the oracle dropped the operation, or the case's "
                            + "shape gives that credit nothing to witness; the topology was:"
                            + System.lineSeparator() + state.topologyDescription());
                }
            }
        }

        List<OperationKind> uncovered = new ArrayList<>();
        List<OperationKind> staleExclusions = new ArrayList<>();
        for (OperationKind kind : BuilderSurface.all()) {
            boolean covered = !creditedBy.get(kind).isEmpty();
            if (covered && deliberatelyUncovered.containsKey(kind)) {
                staleExclusions.add(kind);
            }
            if (!covered && !deliberatelyUncovered.containsKey(kind)) {
                uncovered.add(kind);
            }
        }

        return new Coverage(uncovered, staleExclusions, unknownOperations, namedButNotBuilt, vacuousCases,
                oracleFailures, creditedBy, outcomeCases, refusalCases);
    }

    /** Every finding of one coverage pass. Immutable, and reported whole so one run names everything to fix. */
    static final class Coverage {

        private final List<OperationKind> uncovered;

        private final List<OperationKind> staleExclusions;

        private final List<String> unknownOperations;

        private final List<String> namedButNotBuilt;

        private final List<String> vacuousCases;

        private final List<String> oracleFailures;

        private final Map<OperationKind, List<String>> creditedBy;

        private final int outcomeCases;

        private final int refusalCases;

        Coverage(List<OperationKind> uncovered,
                 List<OperationKind> staleExclusions,
                 List<String> unknownOperations,
                 List<String> namedButNotBuilt,
                 List<String> vacuousCases,
                 List<String> oracleFailures,
                 Map<OperationKind, List<String>> creditedBy,
                 int outcomeCases,
                 int refusalCases) {
            this.uncovered = Collections.unmodifiableList(new ArrayList<>(uncovered));
            this.staleExclusions = Collections.unmodifiableList(new ArrayList<>(staleExclusions));
            this.unknownOperations = Collections.unmodifiableList(new ArrayList<>(unknownOperations));
            this.namedButNotBuilt = Collections.unmodifiableList(new ArrayList<>(namedButNotBuilt));
            this.vacuousCases = Collections.unmodifiableList(new ArrayList<>(vacuousCases));
            this.oracleFailures = Collections.unmodifiableList(new ArrayList<>(oracleFailures));
            Map<OperationKind, List<String>> copied = new LinkedHashMap<>();
            creditedBy.forEach((kind, cases) -> {
                List<String> previous = copied.put(kind, Collections.unmodifiableList(new ArrayList<>(cases)));
                // One entry per operation, so there is never a previous - named rather than dropped.
                assert previous == null : "two credit lists for " + kind;
            });
            this.creditedBy = Collections.unmodifiableMap(copied);
            this.outcomeCases = outcomeCases;
            this.refusalCases = refusalCases;
        }

        /** Operations no outcome case credits, and no exclusion excuses - assertion 1. */
        List<OperationKind> uncovered() {
            return uncovered;
        }

        /** Excluded operations that a case now credits - assertion 4. */
        List<OperationKind> staleExclusions() {
            return staleExclusions;
        }

        /** Operations a case named that the surface does not know - assertion 2. */
        List<String> unknownOperations() {
            return unknownOperations;
        }

        /** Operations a case named that its built topology does not contain - assertion 5. */
        List<String> namedButNotBuilt() {
            return namedButNotBuilt;
        }

        /** Outcome cases that pipe nothing, or whose final state holds nothing - assertion 3. */
        List<String> vacuousCases() {
            return vacuousCases;
        }

        /** Outcome cases whose oracle threw; no credit and no comparison came from them (KTD10). */
        List<String> oracleFailures() {
            return oracleFailures;
        }

        /** Which cases credit each operation - what a red about coverage has to show. */
        Map<OperationKind, List<String>> creditedBy() {
            return creditedBy;
        }

        /** Outcome cases executed. */
        int outcomeCases() {
            return outcomeCases;
        }

        /** Refusal-class cases counted and skipped (R15). */
        int refusalCases() {
            return refusalCases;
        }
    }

    // ------------------------------------------------------------------------------------------- plumbing

    private static boolean isEmpty(FinalState state) {
        return state.stores().values().stream().allMatch(List::isEmpty)
                && state.sinks().values().stream().allMatch(List::isEmpty);
    }

    private static List<ConformanceCase> realCorpus() {
        return CaseLoader.loadClasspathDirectory(CORPUS);
    }

    private static List<String> refusalCaseNames(List<ConformanceCase> corpus) {
        List<String> names = new ArrayList<>();
        corpus.stream().filter(ConformanceCase::refusalClass).forEach(each -> names.add(each.name()));
        return names;
    }

    private static Map<String, List<String>> emptyObservable(String name) {
        return Collections.singletonMap(name, Collections.emptyList());
    }

    /** A stub oracle that computes the real outcome and then erases the sink node from the description. */
    private static Function<ConformanceCase, FinalState> withoutTheSink(
            Function<ConformanceCase, FinalState> delegate) {
        return conformanceCase -> {
            FinalState real = delegate.apply(conformanceCase);
            StringBuilder kept = new StringBuilder();
            for (String line : real.topologyDescription().split("\\R")) {
                if (!line.contains("KSTREAM-SINK-")) {
                    kept.append(line).append(System.lineSeparator());
                }
            }
            return new FinalState(real.stores(), real.sinks(), kept.toString());
        };
    }

    /** Writes each case into its own file under one directory and loads them, so every fixture is a real case. */
    private static List<ConformanceCase> corpus(Path directory, String... yamls) {
        try {
            Path created = Files.createDirectories(directory);
            for (int index = 0; index < yamls.length; index++) {
                Path written = Files.write(created.resolve("case-" + index + ".yaml"),
                        yamls[index].getBytes(StandardCharsets.UTF_8));
                assertThat(Files.exists(written)).isTrue();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write the hand-built corpus into " + directory, e);
        }
        return CaseLoader.load(directory);
    }

    // -------------------------------------------------------------------------------------- the fixtures

    private static final String BASE_INSTANT = "1970-01-01T02:00:00Z";

    /** source, map-values, group-by-key, count, to-stream, sink - six of the ten in one non-windowed case. */
    private static final String COUNT_WITH_MAP_VALUES = ""
            + "name: count-with-map-values\n"
            + "base-instant: " + BASE_INSTANT + "\n"
            + "topology:\n"
            + "  - {id: in, source: {topic: in}}\n"
            + "  - {id: m, map-values: {of: in, fn: upper}}\n"
            + "  - {id: g, group-by-key: {of: m}}\n"
            + "  - {id: c, count: {of: g, store: counts}}\n"
            + "  - {id: s, to-stream: {of: c}}\n"
            + "  - {sink: {of: s, topic: out}}\n"
            + "inputs:\n"
            + "  - {key: a, value: x, at-ms: 0}\n"
            + "perturbation:\n"
            + "  - {key: b, value: x, at-ms: 0}\n";

    /** windowed-by and aggregate, the two the case above does not reach. */
    private static final String WINDOWED_AGGREGATE = ""
            + "name: windowed-aggregate\n"
            + "base-instant: " + BASE_INSTANT + "\n"
            + "topology:\n"
            + "  - {id: in, source: {topic: in}}\n"
            + "  - {id: g, group-by-key: {of: in}}\n"
            + "  - {id: w, windowed-by: {of: g, size-ms: 1000, advance-ms: 1000, grace-ms: 0, retention-ms: 60000}}\n"
            + "  - {id: a, aggregate: {of: w, fn: concat, store: aggs}}\n"
            + "  - {id: s, to-stream: {of: a}}\n"
            + "  - {sink: {of: s, topic: out}}\n"
            + "inputs:\n"
            + "  - {key: a, value: x, at-ms: 0}\n"
            + "perturbation:\n"
            + "  - {key: b, value: x, at-ms: 0}\n";

    /** join and reduce - the table side of a join is what a reduce is here for. */
    private static final String JOIN_OVER_A_REDUCE = ""
            + "name: join-over-a-reduce\n"
            + "base-instant: " + BASE_INSTANT + "\n"
            + "topology:\n"
            + "  - {id: s, source: {topic: left}}\n"
            + "  - {id: r, source: {topic: right}}\n"
            + "  - {id: rg, group-by-key: {of: r}}\n"
            + "  - {id: rt, reduce: {of: rg, fn: last-wins, store: table}}\n"
            + "  - {id: j, join: {stream: s, table: rt, fn: concat-sides}}\n"
            + "  - {sink: {of: j, topic: out}}\n"
            + "inputs:\n"
            + "  - {key: a, value: x, at-ms: 0, topic: right}\n"
            + "  - {key: a, value: y, at-ms: 1000, topic: left}\n"
            + "perturbation:\n"
            + "  - {key: a, value: q, at-ms: 0, topic: right}\n"
            + "  - {key: a, value: y, at-ms: 1000, topic: left}\n";

    /** Oracle-only (KTD5): the trailing record past window end plus grace is what makes a suppressed window emit. */
    private static final String PINNED_EMIT_WINDOWED_COUNT = ""
            + "name: pinned-emit-windowed-count\n"
            + "base-instant: " + BASE_INSTANT + "\n"
            + "topology:\n"
            + "  - {id: in, source: {topic: in}}\n"
            + "  - {id: g, group-by-key: {of: in}}\n"
            + "  - {id: w, windowed-by: {of: g, size-ms: 1000, advance-ms: 1000, grace-ms: 0, retention-ms: 60000}}\n"
            + "  - {id: c, count: {of: w, store: counts}}\n"
            + "  - {id: s, to-stream: {of: c}}\n"
            + "  - {sink: {of: s, topic: out}}\n"
            + "emit: on-window-close\n"
            + "inputs:\n"
            + "  - {key: a, value: x, at-ms: 0}\n"
            + "  - {key: a, value: x, at-ms: 10000}\n"
            + "perturbation:\n"
            + "  - {key: b, value: x, at-ms: 0}\n"
            + "  - {key: b, value: x, at-ms: 10000}\n";

    /** Refusal-class (R15): the wire's alternation between a host function and an engine combine. */
    private static final String AGGREGATE_REFUSAL = ""
            + "name: aggregate-names-both\n"
            + "base-instant: " + BASE_INSTANT + "\n"
            + "topology:\n"
            + "  - {id: in, source: {topic: in}}\n"
            + "  - {id: g, group-by-key: {of: in}}\n"
            + "  - {id: w, windowed-by: {of: g, size-ms: 1000, advance-ms: 1000, grace-ms: 0, retention-ms: 60000}}\n"
            + "  - {id: a, aggregate: {of: w, fn: concat, combine: last-bytes, store: aggs}}\n"
            + "expects-fault: aggregate-names-function-and-combine\n";
}
