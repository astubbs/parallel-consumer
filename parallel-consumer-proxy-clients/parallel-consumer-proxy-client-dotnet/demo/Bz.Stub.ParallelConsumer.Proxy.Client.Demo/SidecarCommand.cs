// Copyright (C) 2026 Antony Stubbs and contributors

using Bz.Stub.ParallelConsumer.Proxy.Client.Jvm;

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Demo;

/// <summary>
/// The sidecar binary this demo hands to the client library, and the arguments it is launched with.
/// </summary>
/// <remarks>
/// THE SIDECAR IS A JVM PROGRAM IN THIS REPOSITORY, not a shipped executable, so "the binary" is a
/// <c>java</c> launcher and the proxy is a classpath argument. That is scaffolding, not the product
/// model: the library still launches it directly and never through a shell, because a wrapper
/// process would inherit the write end of the lifecycle pipe and defeat the sidecar's parent-death
/// signal.
/// <para>
/// IT IS ALSO NOT A COMPOSE SERVICE, in the container or out of it. KTD41's product model is that
/// the client library spawns and supervises the sidecar itself, so the user never installs, deploys
/// or operates a process - and a demo that showed one would be teaching a deployment the product
/// does not ask for.
/// </para>
/// </remarks>
/// <param name="Path">The absolute path of the binary to launch.</param>
/// <param name="Arguments">Its arguments, passed verbatim.</param>
internal sealed record SidecarCommand(string Path, IReadOnlyList<string> Arguments)
{
    /// <summary>The sidecar's real entry point - not <c>TestModeMain</c>, which serves fixtures.</summary>
    private const string MainClass = "bz.stub.parallelconsumer.proxy.Main";

    /// <summary>
    /// The whole classpath, when the caller already knows it. The demo's container sets this,
    /// because its image holds the built reactor and its Maven repository at paths this program
    /// would otherwise have to rediscover.
    /// </summary>
    private const string ClasspathVariable = "PC_DEMO_SIDECAR_CLASSPATH";

    /// <summary>The JVM to launch it with, when the caller has an opinion.</summary>
    private const string JavaVariable = "PC_DEMO_JAVA";

    /// <summary>
    /// What run.sh runs to produce the two things below. Named in the failure message rather than
    /// only in a README, because the reader who hits it is not reading a README.
    /// </summary>
    private const string BuildCommand =
        "./mvnw --batch-mode -pl :parallel-consumer-proxy -am -DskipTests package "
        + "dependency:build-classpath -Dmdep.includeScope=runtime "
        + "'-Dmdep.outputFile=${project.build.directory}/sidecar-classpath.txt'";

    private const string ProxyModule = "parallel-consumer-proxy";

    /// <summary>Resolves the command, from the environment when it says, by discovery otherwise.</summary>
    /// <returns>The launcher and its arguments.</returns>
    public static SidecarCommand Resolve() => new(
        JvmToolchain.JavaLauncher(Environment.GetEnvironmentVariable(JavaVariable)),
        ["-cp", Classpath(), MainClass]);

    /// <summary>
    /// The sidecar's classpath: the proxy's own compiled output plus its runtime dependencies.
    /// </summary>
    /// <remarks>
    /// <c>target/classes</c> rather than the packaged jar, deliberately - it is what a
    /// <c>package</c> build always leaves behind, it carries the proxy's resources (its logging
    /// configuration among them), and it needs no jar-name matching that a classifier could fool.
    /// </remarks>
    private static string Classpath()
    {
        var supplied = Environment.GetEnvironmentVariable(ClasspathVariable);
        if (!string.IsNullOrWhiteSpace(supplied))
        {
            return supplied;
        }

        var proxy = System.IO.Path.Combine(RepositoryRoot(), ProxyModule, "target");
        var classes = System.IO.Path.Combine(proxy, "classes");
        var dependencies = System.IO.Path.Combine(proxy, "sidecar-classpath.txt");
        if (!Directory.Exists(classes) || !File.Exists(dependencies))
        {
            throw new InvalidOperationException(
                $"the sidecar is not built - run '{BuildCommand}', or set {ClasspathVariable}");
        }

        return classes + System.IO.Path.PathSeparator + File.ReadAllText(dependencies).Trim();
    }

    /// <summary>
    /// Walks up from this assembly to the repository root.
    /// </summary>
    /// <remarks>
    /// THE MARKER IS THE PROXY MODULE'S POM, NOT <c>.git</c>, and that is not a stylistic choice:
    /// the demo's image is built from a context whose <c>.dockerignore</c> excludes <c>.git</c>, so
    /// a <c>.git</c> walk finds nothing there and the container path would fail for a reason that
    /// has nothing to do with the demo. The pom is present wherever the sidecar could be built.
    /// </remarks>
    private static string RepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(System.IO.Path.Combine(directory.FullName, ProxyModule, "pom.xml")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException(
            $"no checkout above {AppContext.BaseDirectory} holds {ProxyModule}/pom.xml - "
            + $"set {ClasspathVariable} to name the sidecar's classpath directly");
    }
}
