// Copyright (C) 2026 Antony Stubbs and contributors

using System.Diagnostics;
using System.Globalization;

namespace Bz.Stub.ParallelConsumer.Proxy.Client;

/// <summary>
/// The proxy child process and the lifecycle pipe that keeps it alive.
/// </summary>
/// <remarks>
/// The pipe is the parent-death signal: this process holds the write end of the child's standard
/// input and never writes to it, so EOF there is proof the parent is gone. That is why the binary
/// is launched DIRECTLY and never through a shell - a wrapper process would hold the write end open
/// and leak a JVM that still holds group membership.
/// </remarks>
internal sealed class Sidecar : IAsyncDisposable
{
    /// <summary>The lifecycle channel's whole vocabulary.</summary>
    private const string PortLinePrefix = "port: ";

    private readonly Process _process;
    private readonly Task _stdoutPump;
    private readonly Task _stderrPump;

    private Sidecar(Process process, int port, Task stdoutPump, Task stderrPump)
    {
        _process = process;
        _stdoutPump = stdoutPump;
        _stderrPump = stderrPump;
        Port = port;
    }

    /// <summary>The loopback port the proxy is serving on.</summary>
    public int Port { get; }

    public static async Task<Sidecar> StartAsync(ClientOptions options, CancellationToken cancellationToken)
    {
        var startInfo = new ProcessStartInfo(options.SidecarPath)
        {
            // Directly, not through a shell - see the type remarks.
            UseShellExecute = false,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        foreach (var argument in options.SidecarArguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        var process = new Process { StartInfo = startInfo };
        if (!process.Start())
        {
            process.Dispose();
            throw new InvalidOperationException($"the sidecar at '{options.SidecarPath}' did not start");
        }

        var portFound = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stdoutPump = PumpStdoutAsync(process, portFound);
        var stderrPump = PumpStderrAsync(process, options.SidecarErrorLog);

        try
        {
            var port = await portFound.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new Sidecar(process, port, stdoutPump, stderrPump);
        }
        catch
        {
            await StopAsync(process, stdoutPump, stderrPump).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Reads the lifecycle channel until the port line, then keeps draining.
    /// </summary>
    /// <remarks>
    /// The specification's contract is that the port is standard output's FIRST line. The
    /// conformance harness diverges - it logs before it - and the guide says a test absorbs that
    /// rather than asserting the position, so this SCANS for the line instead of reading exactly
    /// one; scanning satisfies both. Draining continues for the child's whole life, so a sidecar
    /// that keeps logging never blocks on a full pipe buffer.
    /// </remarks>
    private static async Task PumpStdoutAsync(Process process, TaskCompletionSource<int> portFound)
    {
        try
        {
            while (await process.StandardOutput.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                if (portFound.Task.IsCompleted || !line.StartsWith(PortLinePrefix, StringComparison.Ordinal))
                {
                    continue;
                }

                var rest = line[PortLinePrefix.Length..].Trim();
                if (int.TryParse(rest, NumberStyles.Integer, CultureInfo.InvariantCulture, out var port))
                {
                    portFound.TrySetResult(port);
                }
                else
                {
                    portFound.TrySetException(new InvalidOperationException(
                        $"the sidecar printed an unparseable port line '{line}'"));
                }
            }

            portFound.TrySetException(new InvalidOperationException(
                $"the sidecar's stdout ended before a '{PortLinePrefix}<n>' line"));
        }
        catch (Exception exception)
        {
            portFound.TrySetException(exception);
        }
    }

    private static async Task PumpStderrAsync(Process process, TextWriter? destination)
    {
        try
        {
            while (await process.StandardError.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                if (destination is not null)
                {
                    await destination.WriteLineAsync(line).ConfigureAwait(false);
                }
            }
        }
        catch (IOException)
        {
            // The pipe closed with the child. Nothing to report: the session's own error, if there
            // was one, is the interesting one.
        }
    }

    /// <summary>
    /// Closes the lifecycle pipe and reaps the child.
    /// </summary>
    /// <remarks>
    /// Closing standard input IS the reap: it is the parent-death signal the sidecar watches, and
    /// it is also the only thing that ends the conformance harness, which serves until stdin EOF
    /// and does not exit after a clean drain. Killing is the backstop for a child that ignores
    /// both. Never do this while the session stream is still open - that turns a clean drain into a
    /// reconnect-window recovery for the next group member.
    /// </remarks>
    public ValueTask DisposeAsync() => new(StopAsync(_process, _stdoutPump, _stderrPump));

    private static async Task StopAsync(Process process, Task stdoutPump, Task stderrPump)
    {
        try
        {
            process.StandardInput.Close();

            using var grace = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            try
            {
                await process.WaitForExitAsync(grace.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync().ConfigureAwait(false);
            }

            await Task.WhenAll(stdoutPump, stderrPump).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or InvalidOperationException)
        {
            // The child was already gone. Reaping something that has exited is success, not an
            // error to raise over whatever the caller was really doing.
        }
        finally
        {
            process.Dispose();
        }
    }
}
