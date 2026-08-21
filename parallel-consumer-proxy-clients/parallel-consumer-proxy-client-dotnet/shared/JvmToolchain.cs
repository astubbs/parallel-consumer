// Copyright (C) 2026 Antony Stubbs and contributors

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Jvm;

/// <summary>
/// Finding a JVM to launch the sidecar with - the one thing this module's test harness and its demo
/// genuinely do the same way.
/// </summary>
/// <remarks>
/// IT IS COMPILED INTO BOTH PROJECTS BY LINK, not shared through an assembly, because neither the
/// test project nor the demo is a library anybody references, and inventing a third assembly to
/// hold twenty-five lines would cost more than it saves. A <c>Compile Include</c> item in each
/// <c>.csproj</c> points here; there is no project reference to follow.
/// <para>
/// WHAT IS DELIBERATELY NOT HERE: the classpath. The harness wants the proxy's TEST classpath, to
/// reach <c>TestModeMain</c>; the demo wants its RUNTIME classpath, to reach the real <c>Main</c> -
/// and the harness resolves it by shelling out to Maven while the demo is handed one by its
/// entry-point script. Two different answers to two different questions, so they stay apart.
/// </para>
/// </remarks>
internal static class JvmToolchain
{
    /// <summary>
    /// Resolves the JVM launcher: the caller's explicit choice, then <c>JAVA_HOME</c>, then
    /// <c>PATH</c>.
    /// </summary>
    /// <remarks>
    /// A PATH lookup is acceptable HERE and nowhere else. This is scaffolding choosing a JVM to run
    /// the sidecar with, not a client library choosing which binary receives a user's Kafka
    /// credentials - the library still refuses a sidecar path that is not absolute, which is why
    /// this returns one.
    /// </remarks>
    /// <param name="explicitLauncher">
    /// The launcher the caller was told to use, if any - each caller reads its own environment
    /// variable, because the demo's is not the test suite's.
    /// </param>
    /// <returns>An absolute path to a <c>java</c> launcher.</returns>
    /// <exception cref="InvalidOperationException">When no JVM can be found.</exception>
    public static string JavaLauncher(string? explicitLauncher)
    {
        if (!string.IsNullOrEmpty(explicitLauncher))
        {
            return explicitLauncher;
        }

        var javaHome = Environment.GetEnvironmentVariable("JAVA_HOME");
        if (!string.IsNullOrEmpty(javaHome))
        {
            var candidate = Path.Combine(javaHome, "bin", "java");
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        foreach (var entry in (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
                     .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            var candidate = Path.Combine(entry, "java");
            if (File.Exists(candidate))
            {
                return Path.GetFullPath(candidate);
            }
        }

        throw new InvalidOperationException("no JVM found - set JAVA_HOME or name one explicitly");
    }
}
