// Copyright (C) 2026 Antony Stubbs and contributors

using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Grpc.Core;
using Grpc.Net.Client;
using Bz.Stub.ParallelConsumer.Proxy.Protocol.V1;

namespace Bz.Stub.ParallelConsumer.Proxy.Client;

/// <summary>
/// One session: one sidecar process, one gRPC stream, one dispatch queue.
/// </summary>
/// <remarks>
/// The shape, which is the same in every language:
/// <code>
/// application process
/// ├── the user's function (an ordinary delegate - the proxy never learns what it is)
/// ├── this library
/// │   ├── admin      - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue
/// │   └── executors  - tasks, each: take record → run the function → report the outcome
/// └── sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
/// </code>
/// <para>
/// THE LIBRARY IS STATELESS PER RECORD. The fencing token rides from dispatch to report on the
/// executing task's stack and is echoed as the object that arrived; there is no request map, no
/// dedupe cache and no completion registry, because a client that holds no per-record state cannot
/// have a per-record state bug. Fencing is the proxy's job.
/// </para>
/// <para>
/// <see cref="ConnectAsync"/> opens the session, <see cref="PollAsync"/> runs it, and
/// <see cref="DisposeAsync"/> performs the client-initiated shutdown.
/// </para>
/// </remarks>
public sealed class ParallelConsumerClient : IAsyncDisposable
{
    /// <summary>How long the shutdown waits for the proxy to drain and complete the stream.</summary>
    private static readonly TimeSpan DrainGrace = TimeSpan.FromSeconds(15);

    private readonly Sidecar _sidecar;
    private readonly GrpcChannel _channel;
    private readonly AsyncDuplexStreamingCall<ClientMessage, ProxyMessage> _call;
    private readonly CancellationTokenSource _callCancellation;
    private readonly Channel<DispatchRecord> _queue;

    /// <summary>Serializes stream writes: gRPC permits one write at a time, and every executor reports.</summary>
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    /// <summary>Cancelled to stop hand-out. Executing records are unaffected and report normally.</summary>
    private readonly CancellationTokenSource _handoutStop = new();

    private readonly SemaphoreSlim _shutdownLock = new(1, 1);

    private Task[] _executors = Array.Empty<Task>();
    private Task? _receiveTask;
    private CancellationTokenRegistration _applicationCancellation;
    private ExceptionDispatchInfo? _failure;
    private int _polled;
    private bool _shutDown;

    private ParallelConsumerClient(
        Sidecar sidecar,
        GrpcChannel channel,
        AsyncDuplexStreamingCall<ClientMessage, ProxyMessage> call,
        CancellationTokenSource callCancellation,
        SessionInfo session)
    {
        _sidecar = sidecar;
        _channel = channel;
        _call = call;
        _callCancellation = callCancellation;
        Session = session;

        // The queue's depth IS the proxy's declared in-flight ceiling, so in a correct system it
        // can never overflow - which is what makes an overflow a protocol violation rather than a
        // load condition. FullMode is immaterial: nothing here ever waits to write.
        _queue = System.Threading.Channels.Channel.CreateBounded<DispatchRecord>(
            new BoundedChannelOptions(session.MaxConcurrency) { SingleWriter = true });
    }

    /// <summary>
    /// The EFFECTIVE configuration this session is running with, including the negotiated
    /// capability set. Assert on this, never on the options you asked for.
    /// </summary>
    public SessionInfo Session { get; }

    /// <summary>
    /// Spawns the sidecar, connects to it, and completes the fresh-session handshake. It returns
    /// once the proxy's effective configuration has arrived - only then is the session open.
    /// </summary>
    /// <param name="options">The session's configuration.</param>
    /// <param name="cancellationToken">Bounds the spawn and the handshake only; the session itself
    /// lives until it is disposed.</param>
    /// <returns>The open client.</returns>
    public static async Task<ParallelConsumerClient> ConnectAsync(
        ClientOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        var sidecar = await Sidecar.StartAsync(options, cancellationToken).ConfigureAwait(false);
        GrpcChannel? channel = null;
        AsyncDuplexStreamingCall<ClientMessage, ProxyMessage>? call = null;
        var callCancellation = new CancellationTokenSource();
        try
        {
            // Plaintext loopback, and the authority the proxy's allowlist expects. No TLS, no
            // interceptors, no load balancing: the protocol uses a deliberately narrow slice of
            // gRPC, which is what makes every language's implementation sufficient.
            var address = string.Create(
                CultureInfo.InvariantCulture, $"http://127.0.0.1:{sidecar.Port}");
            channel = GrpcChannel.ForAddress(address);
            call = new ProxyService.ProxyServiceClient(channel)
                .Session(cancellationToken: callCancellation.Token);

            // NOTE what is NOT in any message below: the configuration itself. Configure carries
            // kafka_properties, and a natural rendering of it would put credentials in a log line.
            await call.RequestStream
                .WriteAsync(new ClientMessage { Configure = options.ToConfigure() }, cancellationToken)
                .ConfigureAwait(false);

            if (!await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
            {
                throw new ProxyProtocolViolationException(
                    "the session stream ended before the handshake reply");
            }

            var reply = call.ResponseStream.Current;
            if (reply.MessageCase != ProxyMessage.MessageOneofCase.Configured)
            {
                throw new ProxyProtocolViolationException(
                    $"the handshake reply was {reply.MessageCase}, not Configured");
            }

            var session = SessionInfo.From(reply.Configured);

            // Absence is a protocol violation, never "unlimited": the in-flight ceiling is always
            // finite and always reported, and it is also this client's queue depth, so there is
            // nothing to fall back on.
            if (session.MaxConcurrency < 1)
            {
                throw new ProxyProtocolViolationException(
                    "Configured carried no usable max_concurrency - the in-flight ceiling is always reported");
            }

            if (session.ExecutorCount < 1)
            {
                throw new ProxyProtocolViolationException("Configured carried no usable executor_count");
            }

            return new ParallelConsumerClient(sidecar, channel, call, callCancellation, session);
        }
        catch
        {
            callCancellation.Cancel();
            call?.Dispose();
            channel?.Dispose();
            callCancellation.Dispose();
            await sidecar.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Runs the session with the user's function: spawns the executors, reads the stream, and
    /// reports every record's outcome. At most once per client.
    /// </summary>
    /// <remarks>
    /// THE RETURNED TASK IS THE SESSION. It completes when the session ends - because the proxy
    /// completed the stream, because <paramref name="cancellationToken"/> was cancelled, or because
    /// the client was disposed - and it faults with the session's first fatal error. So awaiting it
    /// is how an application waits for the session, and not awaiting it is how an application runs
    /// the session in the background; there is no separate "is it done yet" surface, because in C#
    /// the <see cref="Task"/> already is one.
    /// <para>
    /// Cancelling <paramref name="cancellationToken"/> requests the ordinary client-initiated
    /// shutdown - stop hand-out, let executing records report, half-close - and is handed to every
    /// invocation of <paramref name="processor"/>.
    /// </para>
    /// <para>
    /// Call it promptly after <see cref="ConnectAsync"/>: the proxy may dispatch as soon as it has
    /// answered the handshake, and nothing reads the stream until this is called.
    /// </para>
    /// </remarks>
    /// <param name="processor">The user's function.</param>
    /// <param name="cancellationToken">Cancels the session, gracefully.</param>
    /// <returns>A task that completes when the session ends.</returns>
    public Task PollAsync(RecordProcessor processor, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(processor);
        if (Interlocked.Exchange(ref _polled, 1) == 1)
        {
            throw new InvalidOperationException(
                "PollAsync has already been called on this client - the poll-with-a-function shape is at most once per client");
        }

        _executors = Enumerable.Range(0, Session.ExecutorCount)
            .Select(_ => Task.Run(() => RunExecutorAsync(processor, cancellationToken), CancellationToken.None))
            .ToArray();
        _receiveTask = ReceiveAsync();
        _applicationCancellation = cancellationToken.Register(
            static state => _ = ((ParallelConsumerClient)state!).ShutDownAsync(), this);

        return RunSessionAsync();
    }

    private async Task RunSessionAsync()
    {
        try
        {
            await _receiveTask!.ConfigureAwait(false);
        }
        finally
        {
            await _handoutStop.CancelAsync().ConfigureAwait(false);
            await Task.WhenAll(_executors).ConfigureAwait(false);
        }

        _failure?.Throw();
    }

    /// <summary>
    /// The admin loop. IT ALWAYS READS - backpressure is never applied by not reading, because the
    /// stream also carries the control plane and an admin that stops reading head-of-line-blocks
    /// itself.
    /// </summary>
    private async Task ReceiveAsync()
    {
        try
        {
            while (await _call.ResponseStream.MoveNext(CancellationToken.None).ConfigureAwait(false))
            {
                var message = _call.ResponseStream.Current;
                if (message.MessageCase == ProxyMessage.MessageOneofCase.Dispatch)
                {
                    Enqueue(message.Dispatch);
                }
                else
                {
                    // Every other proxy message is gated by a capability this client does not
                    // declare, and the rule for an un-negotiated message is that the receiver never
                    // acts on it. Recording it keeps the violation visible; ignoring it is the
                    // non-fatal half of the same rule.
                    Fail(new ProxyProtocolViolationException(
                        $"the proxy sent {message.MessageCase} outside the negotiated capability set " +
                        $"[{string.Join(", ", Session.Capabilities)}] - ignored"));
                }
            }
        }
        catch (Exception exception) when (IsSessionEnd(exception))
        {
            // The stream completed, or this client cancelled it during shutdown.
        }
        catch (ProxyProtocolViolationException violation)
        {
            Fail(violation);

            // A gRPC client cannot answer with a status; cancelling the call is the whole of what
            // it can do, and there is no way back from an overflow.
            await _callCancellation.CancelAsync().ConfigureAwait(false);
        }
        catch (RpcException rpcException)
        {
            Fail(rpcException);
        }
    }

    /// <summary>
    /// Queues a wave in record order. Hand-out is FIFO, by arrival and then by the wave's own order,
    /// which a bounded channel gives directly.
    /// </summary>
    private void Enqueue(Dispatch dispatch)
    {
        foreach (var record in dispatch.Records)
        {
            // TryWrite, never WriteAsync: the transport must never block on the queue. A full queue
            // means the proxy exceeded its own declared ceiling - a protocol violation, never load.
            // Never drop a record, never grow the queue.
            if (!_queue.Writer.TryWrite(record))
            {
                // Named in full because the count reaches nobody else: v1 has no client-to-proxy
                // diagnostic message, so the proxy learns only that the call was cancelled.
                throw new ProxyProtocolViolationException(
                    $"a Dispatch overflowed the client queue - depth {Session.MaxConcurrency}, " +
                    $"negotiated max_concurrency {Session.MaxConcurrency}, overflowing token " +
                    $"{record.Token} - the proxy exceeded its own in-flight ceiling");
            }
        }
    }

    /// <summary>One executor: take a record, run the function, report the outcome.</summary>
    private async Task RunExecutorAsync(RecordProcessor processor, CancellationToken applicationToken)
    {
        var reader = _queue.Reader;
        try
        {
            while (await reader.WaitToReadAsync(_handoutStop.Token).ConfigureAwait(false))
            {
                // ONE record per pass, deliberately: draining the queue into a single executor
                // would leave the others idle while the work is already spoken for.
                if (reader.TryRead(out var dispatched))
                {
                    await ProcessOneAsync(processor, dispatched, applicationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Hand-out stopped: the session is shutting down. Anything this executor was already
            // running has finished and reported above.
        }
    }

    private async Task ProcessOneAsync(
        RecordProcessor processor,
        DispatchRecord dispatched,
        CancellationToken applicationToken)
    {
        // The token is echoed VERBATIM - the message the proxy sent, never one rebuilt from parsed
        // parts. It is opaque: nothing here reads record_id or compares epochs.
        var report = new Report { Token = dispatched.Token };
        try
        {
            var outcome = await processor(InboundRecord.From(dispatched), applicationToken)
                .ConfigureAwait(false);
            if (outcome.IsSuccess)
            {
                var success = new Report.Types.Success();
                foreach (var produce in outcome.Produce)
                {
                    success.Produce.Add(produce.ToProduceRecord());
                }

                report.Success = success;
            }
            else
            {
                report.Failure = new Report.Types.Failure { Reason = outcome.FailureReason };
            }
        }
#pragma warning disable CA1031 // The one place a processor's exception becomes a failure outcome.
        catch (Exception exception)
        {
            // A worker that throws must produce a failure report, not tear down the stream. This is
            // the ONLY place in the library that translates an exception into an outcome.
            report.Failure = new Report.Types.Failure { Reason = ReasonFor(exception) };
        }
#pragma warning restore CA1031

        await SendAsync(new ClientMessage { Report = report }).ConfigureAwait(false);
    }

    /// <summary>
    /// The reason text for a thrown exception: its message, or its type when the message is empty.
    /// </summary>
    /// <remarks>
    /// The message and not <see cref="Exception.ToString"/>: the reason is worker-supplied text that
    /// reaches the proxy's logs and rides the redelivery, so it is neither the place for a stack
    /// trace nor for anything the exception may have picked up from a record's payload.
    /// </remarks>
    private static string ReasonFor(Exception exception) =>
        string.IsNullOrWhiteSpace(exception.Message) ? exception.GetType().FullName! : exception.Message;

    private async Task SendAsync(ClientMessage message)
    {
        await _writeLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            await _call.RequestStream.WriteAsync(message).ConfigureAwait(false);
        }
        catch (Exception exception) when (IsSessionEnd(exception))
        {
            // The session ended under us; the record returns to scheduling by the engine's own
            // reclaim path, which is exactly what an unreported record is for.
        }
        catch (RpcException rpcException)
        {
            Fail(rpcException);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Performs the client-initiated shutdown: stop handing records out, let executing records
    /// finish and report, half-close the stream, then reap the sidecar.
    /// </summary>
    /// <remarks>
    /// The half-close IS the shutdown signal - there is no shutdown-request message. Closing the
    /// lifecycle pipe is the reap; the sidecar is never killed with the stream still open, because
    /// that turns a clean drain into a reconnect-window recovery for the next group member.
    /// <para>
    /// The queued records are DISCARDED rather than reported <c>Released</c>: this client does not
    /// declare the <c>shutdown</c> capability, and the negotiation rule forbids sending an outcome
    /// variant outside the negotiated set. The proxy returns them to scheduling as unheld records,
    /// with their attempt counts unchanged.
    /// </para>
    /// </remarks>
    /// <returns>A task that completes once the session is closed and the sidecar reaped.</returns>
    public ValueTask DisposeAsync() => new(ShutDownAsync());

    private async Task ShutDownAsync()
    {
        await _shutdownLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (_shutDown)
            {
                return;
            }

            _shutDown = true;

            // Stop hand-out; executing records keep running and report normally.
            await _handoutStop.CancelAsync().ConfigureAwait(false);
            await Task.WhenAll(_executors).ConfigureAwait(false);

            // Half-close: everything run has been reported, so there is nothing left to say.
            try
            {
                await _call.RequestStream.CompleteAsync().ConfigureAwait(false);
            }
            catch (Exception exception) when (IsSessionEnd(exception) || exception is RpcException)
            {
                // Already ended from the other side.
            }

            // Give the proxy its drain: it commits, completes the stream, and the admin loop ends.
            if (_receiveTask is not null)
            {
                try
                {
                    await _receiveTask.WaitAsync(DrainGrace).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    Fail(new TimeoutException(
                        $"the proxy did not complete the session stream within {DrainGrace}"));
                }
            }

            await _callCancellation.CancelAsync().ConfigureAwait(false);

            // Unregister, not Dispose: this method is itself what a cancelled application token
            // invokes, and disposing a registration can wait for its own callback to finish.
            _applicationCancellation.Unregister();
            _call.Dispose();
            _channel.Dispose();
            _callCancellation.Dispose();
            _handoutStop.Dispose();

            // Closing the lifecycle pipe is the reap, and it happens only now - after the stream
            // is done.
            await _sidecar.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            // The two semaphores are deliberately not disposed: neither one's wait handle is ever
            // taken, so disposal buys nothing, and disposing the one this method is standing in
            // would race a concurrent second call against its own Release.
            _shutdownLock.Release();
        }
    }

    private void Fail(Exception exception) =>
        Interlocked.CompareExchange(ref _failure, ExceptionDispatchInfo.Capture(exception), null);

    /// <summary>
    /// Whether an error is an ordinary end of the session rather than a fault: the stream
    /// completing, or this client cancelling it during shutdown.
    /// </summary>
    private static bool IsSessionEnd(Exception exception) => exception switch
    {
        OperationCanceledException => true,
        ObjectDisposedException => true,
        RpcException { StatusCode: StatusCode.Cancelled or StatusCode.Unavailable } => true,
        _ => false,
    };
}
