package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.List;

/**
 * Builds every selected language's runner ONCE, before the matrix exists - the whole of this module's
 * answer to "who builds a runner".
 * <p>
 * <b>Why a step outside the tests rather than a hook inside them.</b> The suite is parallel twice over: the
 * matrix runs at {@code junit.jupiter.execution.parallel.config.fixed.parallelism=4}, and the {@code ci}
 * profile forks a JVM per core ({@code surefire.forkCount=1C}), which spreads this module's test classes
 * across several of them. A build command writes into ONE output directory per language, so two test
 * classes that both want {@code dotnet} started two {@code dotnet build}s at once and the loser died on the
 * file the winner was writing:
 * {@code MSB4018 ... conformance-runner.runtimeconfig.json ... being used by another process}. Measured, on
 * four cold runs of {@code -Dpc.conformance.language=dotnet}: two red, and each red run had logged the same
 * build starting twice within 200ms. C++ (a BuildKit export into a directory the build first deletes) and
 * TypeScript ({@code npm ci} into one {@code node_modules}) are the same collision wearing different
 * exceptions, and went green on reruns, which is how it read as flakiness for so long.
 * <p>
 * A JUnit lifecycle hook - {@code @BeforeAll}, a {@code LauncherSessionListener}, a memo on the registry
 * entry - cannot fix that, because <b>the second builder is a different JVM</b>. Anything inside the test
 * JVM can only serialise the forks it can see, which is none of them. Maven can: it runs this on
 * {@code process-test-classes}, once, in a phase that has to finish before surefire forks anything.
 * <p>
 * <b>It builds what the run selected, using the suite's own selector</b>, so a CI row that installed one
 * toolchain builds one runner, and a selector naming a language nobody registered fails here - loudly and
 * before a scenario has run - exactly as it fails in the matrix.
 * <p>
 * <b>It does not stop at the first broken toolchain.</b> Every selected language is attempted and the
 * failures are reported together, with each build's own output, for the same reason every Maven lane in
 * this repository passes {@code --fail-at-end}: one run should tell you everything that is wrong with the
 * machine, not the first thing.
 *
 * @author Antony Stubbs
 * @see LanguageRunner
 * @see ConformanceBindings
 */
public final class ConformanceRunnerPrebuild {

    /** Prefixes every line this step prints, so its output is separable from the toolchains' own. */
    private static final String TAG = "[conformance-prebuild] ";

    /**
     * @param args ignored - the selection comes from {@code -Dpc.conformance.language}, the same property
     *             the matrix reads, so the two cannot disagree about which languages this run is about
     */
    public static void main(String[] args) {
        var runners = ConformanceBindings.selected().stream()
                .filter(LanguageRunner.class::isInstance)
                .map(LanguageRunner.class::cast)
                .toList();

        if (runners.isEmpty()) {
            // Not a failure: -Dpc.conformance.language=core, or a selection of JVM clients only, is a
            // legitimate run with nothing to shell out for. A selector that matches NOTHING has already
            // thrown out of selected() above.
            System.out.println(TAG + "no spawned runner in this selection - nothing to build");
            return;
        }

        var failures = new ArrayList<String>();
        for (var runner : runners) {
            System.out.println(TAG + runner.language() + ": " + describe(runner));
            try {
                runner.build();
                runner.ensureAvailable();
            } catch (RuntimeException failure) {
                // Every language is attempted; the verdict is passed on below with all the output intact.
                failures.add(runner.language() + ": " + failure.getMessage());
            }
        }

        if (failures.isEmpty()) {
            System.out.println(TAG + "built " + runners.size() + " runner(s), before any scenario runs");
            return;
        }
        report(failures);
        // The exit status IS the verdict, as it is for a runner: exec-maven-plugin fails the build on it,
        // and a language that could not be built must never reach the matrix looking like one that agreed.
        System.exit(1);
    }

    private static String describe(LanguageRunner runner) {
        if (runner.buildCommand().isEmpty()) {
            return "no build command - the Maven reactor already built it; checking the runner is there";
        }
        return String.join(" ", runner.buildCommand()) + " (in " + runner.workingDirectory() + ")";
    }

    private static void report(List<String> failures) {
        System.err.println(TAG + failures.size() + " runner(s) could not be built. A language nobody could "
                + "build is not a language that passed, so this FAILS the build rather than letting the "
                + "matrix skip it:");
        failures.forEach(failure -> System.err.println(TAG + failure));
    }

    private ConformanceRunnerPrebuild() {
    }
}
