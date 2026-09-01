// Copyright (C) 2026 Antony Stubbs and contributors

using System.Net;
using System.Net.Sockets;
using Grpc.Core;
using Xunit;

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Tests;

/// <summary>
/// The handshake, against a real sidecar process, over the real wire.
/// </summary>
/// <remarks>
/// <para>This module's one against-a-real-process test, and the only claim it can honestly make on
/// this stack. The sidecar spawned is <c>parallel-consumer-proxy</c>'s production entry point - a
/// real bind, the real authority allowlist, the real single-connection guard, and the real session
/// service. That service hosts no engine and refuses every session, so there is no dispatch to
/// observe here and none is invented.</para>
/// <para>What IS observed is everything this library does before an engine would matter: launch the
/// child directly, read <c>port:</c> off its stdout, hold its stdin as the parent-death lifeline,
/// open the channel, put <c>Configure</c> on the wire, and turn what came back into an exception.
/// The dispatch scenarios - one record end to end, the in-flight ceiling, the redelivery history -
/// belong to the shared conformance suite and are deferred until an engine exists.</para>
/// <para>THE STATUS CODE IS THE ASSERTION, NOT MERELY "IT FAILED". A refusal from the authority
/// allowlist is <c>PermissionDenied</c> and one from the admission slot is
/// <c>ResourceExhausted</c>, both raised by interceptors BEFORE the service method runs. Only
/// <c>Unimplemented</c> can have come from the service itself, so the code is what separates "the
/// connection was turned away" from "the handshake was delivered and answered".</para>
/// </remarks>
public sealed class HandshakeTests
{
    /// <summary>Bounds the whole test, spawn and JVM startup included.</summary>
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(90);

    /// <summary>Hoisted because the analyzers reject a fresh array at each call site (CA1861).</summary>
    private static readonly string[] HandshakeTopics = { "handshake-topic" };

    [Fact(DisplayName = "the handshake reaches the session service and its refusal reaches the caller")]
    public async Task TheHandshakeReachesTheSessionServiceAndItsRefusalReachesTheCaller()
    {
        var sidecar = ConformanceHarness.EngineLessSidecar();
        using var deadline = new CancellationTokenSource(Deadline);

        var refused = await Assert.ThrowsAsync<RpcException>(async () =>
            await ParallelConsumerClient.ConnectAsync(
                new ClientOptions
                {
                    SidecarPath = sidecar.Path,
                    SidecarArguments = sidecar.Arguments,
                    Topics = HandshakeTopics,

                    // The sidecar reads no properties at all on this build. Real credentials never
                    // belong in a test, and there is nothing here to give them to.
                    KafkaProperties = new Dictionary<string, string>(),
                    InstanceTag = "dotnet-handshake",
                },
                deadline.Token));

        Assert.True(
            refused.StatusCode == StatusCode.Unimplemented,
            $"handshake failed with {refused.StatusCode} - Unimplemented is the only code the "
            + "session SERVICE raises, so it is what proves the Configure was delivered rather "
            + $"than turned away by an interceptor: {refused.Status.Detail}");
        Assert.Contains(ConformanceHarness.NoEngineDescription, refused.Status.Detail, StringComparison.Ordinal);
    }

    /// <summary>
    /// The control arm, permanent rather than a one-off demonstration: pointed at a port nothing is
    /// listening on, the same client fails in a way that is not the refusal above. Without it, the
    /// test that matters could be passing on any failure at all - which is the shape of an
    /// assertion that cannot fail for the reason it names.
    /// </summary>
    /// <remarks>
    /// The stand-in announces a port and then holds its stdin, which is the spawning contract's
    /// whole client-visible surface, so the library takes its REAL connect path at a dead port
    /// rather than the different path a child that printed nothing would take.
    /// </remarks>
    [Fact(DisplayName = "a sidecar that is not listening fails differently from one that refuses")]
    public async Task ASidecarThatIsNotListeningFailsDifferentlyFromOneThatRefuses()
    {
        var announcer = WriteAnnouncer(ReserveThenReleaseAPort());
        using var deadline = new CancellationTokenSource(Deadline);

        var failed = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await ParallelConsumerClient.ConnectAsync(
                new ClientOptions
                {
                    SidecarPath = announcer,
                    Topics = HandshakeTopics,
                    KafkaProperties = new Dictionary<string, string>(),
                    InstanceTag = "dotnet-handshake-control",
                },
                deadline.Token));

        Assert.False(
            failed is RpcException { StatusCode: StatusCode.Unimplemented },
            $"nothing answered, so nothing can have refused: {failed}");
    }

    /// <summary>A loopback port the OS has just handed out and nothing is listening on.</summary>
    private static int ReserveThenReleaseAPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// A sidecar that announces a port and then holds its stdin. <c>printf</c> and <c>read</c> are
    /// shell builtins, so it is one process holding its own lifeline and no grandchild survives the
    /// library's reap.
    /// </summary>
    private static string WriteAnnouncer(int port)
    {
        var directory = Directory.CreateTempSubdirectory("pc-dotnet-announcer").FullName;
        var script = System.IO.Path.Combine(directory, "announcer.sh");
        File.WriteAllText(
            script,
            "#!/bin/sh\n"
            + $"printf 'port: {port}\\n'\n"
            + "while read -r _ignored; do :; done\n"
            + "exit 0\n");
        // POSIX-only, and guarded rather than suppressed: the spawning contract this repository
        // tests is a POSIX one throughout (a /bin/sh announcer, an inherited stdin lifeline), and
        // there is no Windows lane. The guard is what tells the platform-compatibility analyzer so.
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(
                script,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }

        return script;
    }
}
