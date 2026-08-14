// Copyright (C) 2026 Antony Stubbs and contributors

namespace Bz.Stub.ParallelConsumer.Proxy.Client;

/// <summary>
/// The protocol's capability tokens. The capability list is the protocol's only versioning
/// mechanism: there is no version number, and both sides compare declared sets by intersection.
/// </summary>
public static class Capabilities
{
    /// <summary><c>Dispatch</c> waves, proxy to client.</summary>
    public const string Dispatch = "dispatch";

    /// <summary><c>Heartbeat</c> and the lease semantics, client to proxy.</summary>
    public const string Heartbeat = "heartbeat";

    /// <summary><c>Manifest</c> reconnects and the <c>Drop</c> replies that answer them.</summary>
    public const string Manifest = "manifest";

    /// <summary><c>WorkerDied</c>, client to proxy.</summary>
    public const string WorkerDeath = "worker-death";

    /// <summary><c>Shutdown</c>, proxy to client, and the <c>Released</c> outcome that answers it.</summary>
    public const string Shutdown = "shutdown";

    /// <summary>The <c>Terminal</c> outcome.</summary>
    public const string Terminal = "terminal";

    /// <summary>
    /// What this client actually honours today.
    /// </summary>
    /// <remarks>
    /// Wave one implements the dispatch wave, the client-side queue and per-record reporting;
    /// heartbeats, the manifest reconnect, worker-death reporting, terminal outcomes and the
    /// shutdown drain are later waves, so their tokens are not declared. This list is deliberately
    /// not empty: an empty <c>Configure.capabilities</c> means "the whole v1 baseline", which would
    /// entitle the proxy to send messages this client does not implement - lease-expiry
    /// redeliveries for heartbeats it never sends, above all.
    /// </remarks>
    public static IReadOnlyList<string> Implemented { get; } = new[] { Dispatch };
}
