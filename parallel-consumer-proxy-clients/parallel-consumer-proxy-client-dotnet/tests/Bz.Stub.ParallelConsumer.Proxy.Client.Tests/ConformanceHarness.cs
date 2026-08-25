// Copyright (C) 2026 Antony Stubbs and contributors

using System.Diagnostics;

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
/// Locates the JVM-side conformance harness so a .NET test can spawn it as an ordinary sidecar
/// binary.
/// </summary>
/// <remarks>
/// The harness is <c>TestModeMain</c>, shipped in the proxy module's TEST jar so it can never reach
/// a client package. That makes it a classpath invocation rather than a binary, so "the sidecar
/// binary" for a conformance test is the JVM launcher and the classpath is an argument - and
/// everything awkward about that lives here rather than in each test.
/// </remarks>
internal sealed record ConformanceHarness(string Path, IReadOnlyList<string> Arguments)
{
    private const string MainClass = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain";

    private const string BuildCommand = "bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests";

    /// <summary>Builds the command that serves one conformance scenario in mock mode.</summary>
    /// <remarks>
    /// It FAILS rather than skips when the harness is not built. A test that quietly does not run is
    /// not a passing test, and nothing goes red to say so; the exception names the build command
    /// instead.
    /// </remarks>
    public static ConformanceHarness ForScenario(string scenario)
    {
        var root = RepositoryRoot();
        return new ConformanceHarness(
            JavaLauncher(),
            new[] { "-cp", Classpath(root), MainClass, "--mock", "--scenario", scenario });
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
    /// Resolves the JVM launcher. A PATH lookup is acceptable HERE and nowhere else: this is test
    /// scaffolding choosing a JVM, not a client library choosing which sidecar receives the user's
    /// Kafka credentials.
    /// </summary>
    private static string JavaLauncher()
    {
        var explicitJava = Environment.GetEnvironmentVariable("PC_PROXY_TEST_JAVA");
        if (!string.IsNullOrEmpty(explicitJava))
        {
            return explicitJava;
        }

        var javaHome = Environment.GetEnvironmentVariable("JAVA_HOME");
        if (!string.IsNullOrEmpty(javaHome))
        {
            var candidate = System.IO.Path.Combine(javaHome, "bin", "java");
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        foreach (var entry in (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
                     .Split(System.IO.Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            var candidate = System.IO.Path.Combine(entry, "java");
            if (File.Exists(candidate))
            {
                return System.IO.Path.GetFullPath(candidate);
            }
        }

        throw new InvalidOperationException(
            "no JVM found - set JAVA_HOME or PC_PROXY_TEST_JAVA");
    }

    /// <summary>
    /// Assembles the proxy module's test classpath: its test jar (which carries the harness), its
    /// main jar, and its test-scope dependencies.
    /// </summary>
    /// <remarks>
    /// The dependency list comes from Maven and is cached under this module's <c>target/</c>,
    /// because resolving it costs seconds and the answer only changes when the proxy module's poms
    /// do. It is never committed: it is a list of absolute paths into one machine's local
    /// repository.
    /// </remarks>
    private static string Classpath(string root)
    {
        var proxyTarget = System.IO.Path.Combine(root, "parallel-consumer-proxy", "target");
        var testsJar = SingleJar(proxyTarget, "-tests.jar");
        var mainJar = SingleJar(proxyTarget, ".jar");

        var cacheDirectory = System.IO.Path.Combine(
            root, "parallel-consumer-proxy-clients", "parallel-consumer-proxy-client-dotnet", "target");
        var cache = System.IO.Path.Combine(cacheDirectory, "proxy-test-classpath.txt");
        if (!File.Exists(cache))
        {
            Directory.CreateDirectory(cacheDirectory);
            ResolveDependencies(root, cache);
        }

        var dependencies = File.ReadAllText(cache).Trim();
        return string.Join(
            System.IO.Path.PathSeparator, new[] { testsJar, mainJar, dependencies });
    }

    private static void ResolveDependencies(string root, string cache)
    {
        var startInfo = new ProcessStartInfo(System.IO.Path.Combine(root, "mvnw"))
        {
            WorkingDirectory = root,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var argument in new[]
                 {
                     "-q", "-pl", ":parallel-consumer-proxy", "dependency:build-classpath",
                     "-Dmdep.outputFile=" + cache, "-Dmdep.includeScope=test",
                 })
        {
            startInfo.ArgumentList.Add(argument);
        }

        using var maven = Process.Start(startInfo)
            ?? throw new InvalidOperationException("could not run the Maven wrapper");
        var output = maven.StandardOutput.ReadToEnd() + maven.StandardError.ReadToEnd();
        maven.WaitForExit();
        if (maven.ExitCode != 0 || !File.Exists(cache))
        {
            throw new InvalidOperationException(
                $"resolving the proxy module's test classpath failed (exit {maven.ExitCode}):\n{output}");
        }
    }

    private static string SingleJar(string directory, string suffix)
    {
        if (!Directory.Exists(directory))
        {
            throw new InvalidOperationException(
                $"{directory} is not built - run '{BuildCommand}' first");
        }

        var matches = Directory.EnumerateFiles(directory, "*" + suffix)
            .Where(path => suffix != ".jar" || !EndsWithClassifier(path))
            .ToArray();
        if (matches.Length != 1)
        {
            throw new InvalidOperationException(
                $"expected exactly one '{suffix}' jar in {directory}, found {matches.Length} - " +
                $"run '{BuildCommand}'");
        }

        return matches[0];
    }

    /// <summary>
    /// Whether a jar name carries a classifier. <c>-sources.jar</c>, <c>-javadoc.jar</c> and
    /// <c>-tests.jar</c> all end in <c>.jar</c>; the plain artifact is the one that does not.
    /// </summary>
    private static bool EndsWithClassifier(string path) =>
        path.EndsWith("-tests.jar", StringComparison.Ordinal)
        || path.EndsWith("-sources.jar", StringComparison.Ordinal)
        || path.EndsWith("-javadoc.jar", StringComparison.Ordinal);
}
