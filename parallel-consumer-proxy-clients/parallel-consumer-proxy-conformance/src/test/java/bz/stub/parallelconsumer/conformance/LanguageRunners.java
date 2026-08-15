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
     * compiling. The wrapper launches the assembly through {@code dotnet} rather than through the apphost
     * the SDK emits beside it - the apphost finds the runtime in the machine's install locations and fails
     * on a box whose SDK came from a version manager, which its own header records.
     */
    public static LanguageRunner dotnet() {
        var module = module("dotnet");
        return new LanguageRunner("dotnet", module,
                List.of("dotnet", "build", "tests/ConformanceRunner/ConformanceRunner.csproj",
                        "--configuration", "Release", "--nologo"),
                module.resolve("scripts").resolve("conformance-runner"));
    }

    /**
     * Kotlin: a JVM client, so its runner is a JVM plus a resolved classpath and its wrapper says so.
     * <p>
     * <b>It is the one entry with no build command, and that is the reactor's doing rather than an
     * omission.</b> Every other language shells out to its own toolchain; Kotlin's toolchain is the Maven
     * build already running, and the conformance module test-depends on this module precisely so that build
     * compiles the runner and writes its classpath file before a scenario starts. A nested {@code mvn} here
     * would rewrite the class directories of the JVM executing the suite, while it ran. The wrapper still
     * fails loudly when the classpath file is absent, so a module that was never built does not read as one
     * that passed.
     */
    public static LanguageRunner kotlin() {
        var module = module("kotlin");
        return new LanguageRunner("kotlin", module, List.of(),
                module.resolve("scripts").resolve("conformance-runner"));
    }

    /**
     * C++: the one entry whose build command is not its language's own tool, because there is no C++
     * toolchain on this machine to run.
     * <p>
     * gRPC and protobuf arrive as system DEV PACKAGES rather than as a versioned toolchain, which is why
     * C++ is one of the two languages mise cannot serve here and why its module builds in a container. So
     * the command is {@code bin/build-client.sh cpp}, the same one a developer runs and the same one the
     * Maven module runs: BuildKit compiles the runner inside the image and exports it - a STATICALLY LINKED
     * binary, which is what lets the suite then execute it on this host like any other entry. The script
     * also runs the module's own tests and its static analysis in there, so a red one fails this build
     * rather than surfacing later.
     * <p>
     * <b>It needs Docker, and it says so by failing.</b> A missing daemon exits 2 out of that script,
     * which fails the build and reaches the reader as {@link RunnerUnavailableException} naming the
     * command - never as a skip, which is the rule this whole registry exists to keep.
     */
    public static LanguageRunner cpp() {
        var module = module("cpp");
        return new LanguageRunner("cpp", module,
                List.of(RepoLayout.workingTreeRoot().resolve("bin").resolve("build-client.sh").toString(), "cpp"),
                module.resolve("target").resolve("container").resolve("pc-cpp-conformance-runner"));
    }

    /**
     * Scala: a JVM client like Kotlin, so its "binary" is a wrapper over {@code java} and a classpath the
     * build already wrote, and its build command is empty for the same reason - a nested {@code mvn} would
     * rewrite the class directories of the JVM running this suite while it ran. The wrapper fails loudly
     * when that classpath file is absent, so a module nobody built does not read as one that passed.
     * <p>
     * It wraps the same gRPC transport {@code java-grpc} exposes, so what this entry proves is the Scala
     * surface over a session that is already covered - which is the point of wrapping rather than
     * reimplementing, and why the wave that wrote it could inherit the queue, the ceiling and the session
     * end rather than writing them again.
     */
    public static LanguageRunner scala() {
        var module = module("scala");
        return new LanguageRunner("scala", module, List.of(),
                module.resolve("scripts").resolve("conformance-runner"));
    }

    /** Every language with a runner today, whether or not this run selected it. */
    public static List<LanguageRunner> all() {
        return List.of(go(), python(), typescript(), rust(), ruby(), dotnet(), kotlin(), scala(), cpp());
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
