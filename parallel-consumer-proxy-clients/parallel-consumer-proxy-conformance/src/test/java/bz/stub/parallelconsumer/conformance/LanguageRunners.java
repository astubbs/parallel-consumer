package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.nio.file.Path;
import java.util.List;

/**
 * The registry: every language whose runner this suite drives.
 * <p>
 * <b>Registering the next language is one entry.</b> It names where the runner is built, the command that
 * builds it, and where the binary lands - nothing else, because everything else about a runner is the
 * contract in {@link RunnerContract}, which is identical in every language.
 * <p>
 * <b>Every entry's executable is a committed wrapper or a compiled binary</b>, never an interpreter plus a
 * script: {@link LanguageRunner#ensureBuilt()} checks a path is executable, and "python3 my_runner.py" is
 * not a path. The interpreted languages therefore keep a two-line wrapper in their own module, beside the
 * runner it launches, so the registry entry stays the same shape in every language.
 * <p>
 * Which languages this run actually drives is {@link ConformanceBindings}' - the selector lives there,
 * with the core control arm.
 *
 * @author Antony Stubbs
 * @see LanguageRunner
 */
public final class LanguageRunners {

    /** Go: the first language through the harness, and the thinnest-tested before it. */
    public static LanguageRunner go() {
        var module = module("go");
        return new LanguageRunner("go", module,
                List.of("go", "build", "-o", "target/conformance-runner", "./cmd/conformance-runner"),
                module.resolve("target").resolve("conformance-runner"));
    }

    /**
     * Python. {@code make build} is the module's own recipe for its venv, so the suite installs the client
     * exactly as a developer and the CI row do rather than inventing a third way to get grpcio in place.
     */
    public static LanguageRunner python() {
        var module = module("python");
        return new LanguageRunner("python", module, List.of("make", "build"),
                module.resolve("scripts").resolve("conformance-runner"));
    }

    /** TypeScript: {@code npm run build} installs and compiles; the wrapper runs the emitted JavaScript. */
    public static LanguageRunner typescript() {
        var module = module("typescript");
        return new LanguageRunner("typescript", module, List.of("npm", "run", "--silent", "build"),
                module.resolve("scripts").resolve("conformance-runner"));
    }

    /** Rust: an ordinary cargo binary target, so the runner is the compiled artefact itself. */
    public static LanguageRunner rust() {
        var module = module("rust");
        return new LanguageRunner("rust", module, List.of("cargo", "build", "--bin", "conformance-runner"),
                module.resolve("target").resolve("debug").resolve("conformance-runner"));
    }

    /** Ruby: {@code bundle install} puts the gems in place; the runner is an executable Ruby script. */
    public static LanguageRunner ruby() {
        var module = module("ruby");
        return new LanguageRunner("ruby", module, List.of("bundle", "install", "--quiet"),
                module.resolve("scripts").resolve("conformance-runner"));
    }

    /**
     * .NET: a console project in the module's own solution, so an ordinary {@code dotnet build} keeps it
     * compiling. The apphost the SDK emits beside the assembly is the executable.
     */
    public static LanguageRunner dotnet() {
        var module = module("dotnet");
        return new LanguageRunner("dotnet", module,
                List.of("dotnet", "build", "tests/ConformanceRunner/ConformanceRunner.csproj",
                        "--configuration", "Release", "--nologo"),
                module.resolve("tests").resolve("ConformanceRunner").resolve("bin").resolve("Release")
                        .resolve("net8.0").resolve("conformance-runner"));
    }

    /** Every language with a runner today, whether or not this run selected it. */
    public static List<LanguageRunner> all() {
        return List.of(go(), python(), typescript(), rust(), ruby(), dotnet());
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

    private static Path module(String language) {
        return RepoLayout.clientsRoot().resolve("parallel-consumer-proxy-client-" + language);
    }

    private LanguageRunners() {
    }
}
