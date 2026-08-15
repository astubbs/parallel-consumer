package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The registry: every language whose runner this suite drives.
 * <p>
 * <b>Registering the next language is one entry.</b> It names where the runner is built, the command that
 * builds it, and where the binary lands - nothing else, because everything else about a runner is the
 * contract in {@link RunnerContract}, which is identical in every language. The four still to come are
 * commented below with the entry each of them needs, so the work is visible rather than remembered.
 *
 * @author Antony Stubbs
 * @see LanguageRunner
 */
public final class LanguageRunners {

    /**
     * The deliberate, visible way to run fewer languages: {@code -Dpc.conformance.languages=go}. Absence of
     * a toolchain is NOT a way - that fails, loudly, per {@link LanguageRunner#ensureBuilt()}.
     */
    public static final String LANGUAGES_PROPERTY = "pc.conformance.languages";

    /** Go: the first language through the harness, and the thinnest-tested before it. */
    public static LanguageRunner go() {
        var module = RepoLayout.clientsRoot().resolve("parallel-consumer-proxy-client-go");
        return new LanguageRunner("go", module,
                List.of("go", "build", "-o", "target/conformance-runner", "./cmd/conformance-runner"),
                module.resolve("target").resolve("conformance-runner"));
    }

    // The remaining four wave-one languages. Each needs (1) a cmd/conformance-runner equivalent implementing
    // RunnerContract in its own idiom, (2) the build command that produces one binary, (3) an entry here:
    //
    //   python     - .../parallel-consumer-proxy-client-python, a console script or a `python -m` shim;
    //                the "executable" is whatever the venv build drops, and the build command creates it
    //   typescript - .../parallel-consumer-proxy-client-typescript, `npm run build` then a node entry point
    //   rust       - .../parallel-consumer-proxy-client-rust, `cargo build --bin conformance-runner`
    //   ruby       - .../parallel-consumer-proxy-client-ruby, an executable script; the build command may be
    //                `bundle install` or empty
    //
    // Adding one changes nothing else in this module: the scenarios, the assertions and the driver are
    // already language-blind.

    /** Every registered language, filtered by {@link #LANGUAGES_PROPERTY} when it is set. */
    public static List<LanguageRunner> registered() {
        var all = List.of(go());
        var requested = System.getProperty(LANGUAGES_PROPERTY);
        if (requested == null || requested.isBlank()) {
            return all;
        }
        var wanted = Arrays.stream(requested.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
        var selected = all.stream().filter(runner -> wanted.contains(runner.language())).collect(Collectors.toList());
        var known = all.stream().map(LanguageRunner::language).toList();
        var unknown = wanted.stream().filter(name -> !known.contains(name)).toList();
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("-D" + LANGUAGES_PROPERTY + " names languages this suite does not "
                    + "register: " + unknown + " (registered: " + known + "). A typo here would otherwise run "
                    + "nothing and read as a pass.");
        }
        return selected;
    }

    /**
     * A runner that does not exist, for the negative control that proves absence fails. Registered nowhere:
     * it is constructed by the test that asserts on it.
     */
    public static LanguageRunner absent() {
        return new LanguageRunner("absent-language", RepoLayout.scratch(), List.of(),
                RepoLayout.scratch().resolve("no-such-conformance-runner"));
    }

    /**
     * A runner that starts, takes its time, and then fails - the crashed-runner control, and the second
     * language the parallelism proof needs. It is a shell one-liner rather than a real client because what
     * is being proved is that the suite reports a bad runner as bad while another language runs beside it.
     */
    public static LanguageRunner deliberatelyFailing(Path executable) {
        return new LanguageRunner("deliberately-failing", RepoLayout.scratch(), List.of(), executable);
    }

    private LanguageRunners() {
    }
}
