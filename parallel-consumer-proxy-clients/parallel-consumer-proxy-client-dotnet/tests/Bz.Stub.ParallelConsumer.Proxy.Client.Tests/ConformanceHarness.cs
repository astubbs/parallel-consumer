// Copyright (C) 2026 Antony Stubbs and contributors

using System.Diagnostics;
using Bz.Stub.ParallelConsumer.Proxy.Client.Jvm;


namespace Bz.Stub.ParallelConsumer.Proxy.Client.Tests;

/// <summary>
/// The conformance scenario names. A scenario name is the suite's identity everywhere - the
/// harness CLI, this list, and the test that runs it - and it is ALSO the topic name to subscribe
/// to, because the harness seeds its records on the topic it is named after.
/// </summary>
internal static class Scenarios
{
    public const string ProcessedRecordAdvancesOffset = "a-processed-record-advances-the-committed-offset";
    public const string UnreportedRecordHoldsCommit = "an-unreported-record-holds-back-the-commit";
    public const string FailedRecordIsRedelivered = "a-failed-record-is-redelivered-with-its-failure-history";
    public const string KeyOrdering = "records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently";
}

/// <summary>
/// Locates the JVM-side sidecars so a .NET test can spawn one as an ordinary sidecar binary.
/// </summary>
/// <remarks>
/// <para>THERE ARE TWO, AND THEY ANSWER DIFFERENT QUESTIONS. Both are classpath invocations rather
/// than binaries - so "the sidecar binary" for a test is the JVM launcher and the classpath is an
/// argument, and everything awkward about that lives here rather than in each test.</para>
/// <para><see cref="EngineLessSidecar"/> runs <c>parallel-consumer-proxy</c>'s
/// <c>NoEngineMain</c>, shipped in that module's TEST jar beside <c>TestModeMain</c>. It hosts no
/// Parallel Consumer engine: it binds, announces its port, admits one
/// connection under the transport's rules, and answers every session UNIMPLEMENTED
/// (astubbs/parallel-consumer#384). A test that spawns it exercises the whole client-side path up
/// to and including the handshake and stops exactly where the engine would begin.</para>
/// <para><see cref="ForScenario"/> runs <c>TestModeMain</c>, shipped in the proxy module's TEST jar
/// so it can never reach a client package. That one IS engine-backed, which is what makes the
/// dispatch scenarios above runnable end to end.</para>
/// </remarks>
internal sealed record ConformanceHarness(string Path, IReadOnlyList<string> Arguments)
{
    private const string MainClass = "bz.stub.parallelconsumer.proxy.NoEngineMain";

    private const string TestModeMainClass = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain";

    /// <summary>
    /// What the sidecar's refusal must name, so a client author does not debug their own code.
    /// </summary>
    public const string NoEngineDescription = "hosts no Parallel Consumer engine";

    /// <summary>Written by the <c>dotnet-e2e-harness</c> profile in this module's pom.</summary>
    private const string ClasspathFile = "sidecar-classpath.txt";

    private const string BuildCommand =
        "run './mvnw test -pl :parallel-consumer-proxy-client-dotnet -am -Dpc.foreignClients' from "
        + "the repository root, which is the same wiring the CI matrix row uses";

    /// <summary>Builds the command that runs the real sidecar shell.</summary>
    /// <remarks>
    /// NO ARGUMENTS, and that is the sidecar's own rule rather than this method being terse: it
    /// takes none and refuses to start when given one, because everything is configured
    /// connect-time over the protocol.
    /// <para>It FAILS rather than skips when the sidecar is not built. A test that quietly does not
    /// run is not a passing test, and nothing goes red to say so; the exception names the build
    /// command instead.</para>
    /// </remarks>
    public static ConformanceHarness EngineLessSidecar()
    {
        var root = RepositoryRoot();
        return new ConformanceHarness(JavaLauncher(), new[] { "-cp", Classpath(root), MainClass });
    }

    /// <summary>
    /// Builds the command that serves one conformance scenario in mock mode, engine-backed.
    /// </summary>
    /// <remarks>
    /// It FAILS rather than skips when the harness is not built, for the same reason
    /// <see cref="EngineLessSidecar"/> does.
    /// </remarks>
    public static ConformanceHarness ForScenario(string scenario)
    {
        var root = RepositoryRoot();
        return new ConformanceHarness(
            JavaLauncher(),
            new[] { "-cp", Classpath(root), TestModeMainClass, "--mock", "--scenario", scenario });
    }

    /// <summary>Walks up from the test's working directory to the enclosing git working tree.</summary>
    /// <remarks>
    /// `.git` is a FILE in a worktree and a directory in a plain clone, so both are accepted - this
    /// repository's work happens in worktrees.
    /// </remarks>
    private static string RepositoryRoot()
    {
        var directory = new DirectoryInfo(Directory.GetCurrentDirectory());
        while (directory is not null)
        {
            var marker = System.IO.Path.Combine(directory.FullName, ".git");
            if (File.Exists(marker) || Directory.Exists(marker))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException(
            "no git working tree above the test's working directory");
    }

    /// <summary>
    /// Resolves the JVM launcher, naming this suite's own override variable.
    /// </summary>
    /// <remarks>
    /// The lookup itself is shared with the demo, which spawns the same sidecar the same way -
    /// see <c>shared/JvmToolchain.cs</c>, compiled into both projects by link.
    /// </remarks>
    private static string JavaLauncher() =>
        JvmToolchain.JavaLauncher(Environment.GetEnvironmentVariable("PC_PROXY_TEST_JAVA"));

    /// <summary>The sidecar's classpath, as Maven resolved it.</summary>
    /// <remarks>
    /// ONE ROUTE, AND IT THROWS RATHER THAN GUESSING. The <c>dotnet-e2e-harness</c> profile in
    /// this module's pom writes <c>target/sidecar-classpath.txt</c> on
    /// <c>generate-test-resources</c>, which is the only thing that reliably knows where the proxy
    /// module's output and its dependencies are - in a reactor run they are class DIRECTORIES rather
    /// than jars, so hunting for a jar finds nothing after a <c>test</c>-phase build and reports it
    /// as an unbuilt module. Same arrangement as the Go, Python, TypeScript, Rust and Ruby
    /// harnesses. It is never committed: it is a list of absolute paths into one machine's local
    /// repository.
    /// </remarks>
    private static string Classpath(string root)
    {
        var file = System.IO.Path.Combine(
            root, "parallel-consumer-proxy-clients", "parallel-consumer-proxy-client-dotnet",
            "target", ClasspathFile);
        if (!File.Exists(file))
        {
            throw new InvalidOperationException($"{file} is missing - {BuildCommand}");
        }

        var classpath = File.ReadAllText(file).Trim();
        if (classpath.Length == 0)
        {
            throw new InvalidOperationException($"{file} is empty - {BuildCommand}");
        }

        return classpath;
    }
}
