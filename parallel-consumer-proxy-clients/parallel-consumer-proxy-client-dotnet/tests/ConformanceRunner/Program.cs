// Copyright (C) 2026 Antony Stubbs and contributors

using System.Globalization;
using System.Text;

using Bz.Stub.ParallelConsumer.Proxy.Client;

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Conformance;

/// <summary>
/// .NET's half of the shared cross-language conformance suite (astubbs#242, confluentinc#154).
/// </summary>
/// <remarks>
/// IT ASSERTS NOTHING, DELIBERATELY. The suite that knows what correct looks like - offset
/// frontiers, ordering, redelivery, attempt counts - is the Java module
/// <c>parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance</c>, and it keeps owning
/// that knowledge for every language. This program's whole job is to DO WHAT THE SCENARIO SAYS and
/// then exit; if it were free to decide what "correct" means, ten languages would each decide it
/// slightly differently and the agreement between them would prove nothing.
/// <para>
/// Its contract - flags, exit codes, the two stdout lines per record, the behaviour tokens - is
/// documented once, in that module's <c>README.md</c>, and is identical in every language.
/// </para>
/// <para>
/// THIS DOES NOT REPLACE THIS MODULE'S OWN TESTS. The shared suite proves every client behaves
/// identically on the protocol; the test project beside this one catches what is invisible from
/// outside the process - a cancellation swallowed into a failure, a task that outlives its session.
/// Both layers are load-bearing.
/// </para>
/// </remarks>
internal static class Program
{
    /// <summary>
    /// Exit statuses ARE the verdict channel. There is no results file and no report message: a
    /// scenario passed if this process exited 0 and the Java suite's own assertions about engine
    /// state held.
    /// </summary>
    private const int ExitOk = 0;

    private const int ExitBehaviourFailed = 1;

    private const int ExitUsage = 2;

    private const string BehaviourSucceed = "succeed";
    private const string BehaviourReportNothing = "report-nothing";
    private const string BehaviourFailThenSucceed = "fail-then-succeed";
    private const string BehaviourHoldFirstUntilSecond = "hold-first-until-second";
    private const string BehaviourHoldUntilCeilingFull = "hold-until-ceiling-full";

    private static readonly string[] Behaviours =
    {
        BehaviourSucceed, BehaviourReportNothing, BehaviourFailThenSucceed, BehaviourHoldFirstUntilSecond,
        BehaviourHoldUntilCeilingFull,
    };

    /// <summary>
    /// The exact text a fail-then-succeed run reports. The Java suite asserts the redelivery carries
    /// it back VERBATIM, so it is a fixed literal of the contract in every language, never a message
    /// this runner composes.
    /// </summary>
    private const string PrescribedFailureReason = "conformance-prescribed-failure";

    /// <summary>
    /// Fixed session tunables, contract rather than this runner's judgement: they exist only so
    /// scenarios converge at unit-test speed against the engine's production defaults (a 5s commit
    /// interval, a 1s retry delay). Every language sets the same two values.
    /// </summary>
    private static readonly TimeSpan CommitInterval = TimeSpan.FromMilliseconds(100);

    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// How long a report-nothing run keeps its session OPEN after its last observation.
    /// </summary>
    /// <remarks>
    /// IT IS WHAT MAKES THE NEGATIVE CONTROL A CONTROL. Without it the runner exits the instant the
    /// record arrives, and a sabotaged runner that DID report success has its report killed in
    /// flight by the process exit - so the suite sees an unadvanced offset either way and the
    /// scenario passes for a broken client. Measured in the Go wave, not reasoned about: reporting
    /// success from this behaviour left the suite green until the hold existed.
    /// </remarks>
    private static readonly TimeSpan ReportNothingHold = TimeSpan.FromSeconds(3);

    /// <summary>
    /// How long <c>hold-until-ceiling-full</c> keeps a FULL group held before releasing it.
    /// </summary>
    /// <remarks>
    /// IT IS WHAT TURNS "THE CEILING WAS NEVER EXCEEDED" FROM A RACE INTO A MEASUREMENT. Release the
    /// group the instant it fills and a client that declared a larger ceiling still passes - its
    /// extra records arrive a few milliseconds later, by which time the outstanding count has
    /// already fallen back. Holding the full ceiling still means the extra dispatch arrives INSIDE
    /// the window and prints its line while every other record is unresolved. A correct engine
    /// cannot dispatch anything during the window at all, so the wait costs a conforming client
    /// nothing but time.
    /// </remarks>
    private static readonly TimeSpan CeilingSettle = TimeSpan.FromMilliseconds(250);

    private static async Task<int> Main(string[] arguments)
    {
        if (!Arguments.TryParse(arguments, out var parsed, out var problem))
        {
            await Console.Error.WriteLineAsync($"conformance-runner: {problem}").ConfigureAwait(false);
            return ExitUsage;
        }

        return await RunAsync(parsed).ConfigureAwait(false);
    }

    private static async Task<int> RunAsync(Arguments arguments)
    {
        // The budget is created first because the tracker holds it: every wait the prescription can
        // block on - including the ceiling group - is bounded by the same wall clock the run is.
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(arguments.TimeoutSeconds));
        var tracker = new Tracker(arguments.ExpectDispatches, arguments.MaxConcurrency, budget.Token);

        ParallelConsumerClient client;
        try
        {
            client = await ParallelConsumerClient.ConnectAsync(new ClientOptions
            {
                SidecarPath = arguments.Sidecar,
                // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
                Topics = new[] { arguments.Scenario },
                // The ceiling is the SCENARIO'S to choose and this runner never derives one: it is
                // set from --max-concurrency and from nothing else. Deriving it from
                // --expect-dispatches, which is what this line used to do, is by construction a
                // ceiling no scenario can reach - so no scenario could ask this client to prove it
                // respected one.
                MaxConcurrency = arguments.MaxConcurrency,
                CommitInterval = CommitInterval,
                DefaultMessageRetryDelay = RetryDelay,
                // The mock lane builds mock Kafka clients and reads no properties. Real credentials
                // never belong in a conformance test.
                KafkaProperties = new Dictionary<string, string>(StringComparer.Ordinal),
                SidecarErrorLog = Console.Error,
                InstanceTag = "conformance-runner-dotnet",
            }).ConfigureAwait(false);
        }
        catch (Exception failure) when (failure is not OperationCanceledException)
        {
            await Console.Error.WriteLineAsync($"conformance-runner: opening the session: {failure.Message}")
                .ConfigureAwait(false);
            return ExitBehaviourFailed;
        }

        // The session task is deliberately not awaited here: it completes when the session ENDS, and
        // what this runner waits for is the prescription finishing. It is kept so the shutdown below
        // can observe it.
        var session = client.PollAsync(
            (record, cancellationToken) => ProcessAsync(arguments.Behaviour, tracker, record, cancellationToken));

        // report-nothing completes at OBSERVATION, because by prescription its records are never
        // reported and so can never complete. Every other behaviour completes when the last record
        // it was handed has had its outcome decided.
        var reportNothing = arguments.Behaviour == BehaviourReportNothing;
        if (!await tracker.WaitForPrescribedBehaviourAsync(reportNothing).ConfigureAwait(false))
        {
            await Console.Error.WriteLineAsync(string.Create(CultureInfo.InvariantCulture,
                    $"conformance-runner: scenario {arguments.Scenario} behaviour {arguments.Behaviour} did not " +
                    $"complete within {arguments.TimeoutSeconds}s - observed {tracker.Observed} of " +
                    $"{arguments.ExpectDispatches}, completed {tracker.Completed}"))
                .ConfigureAwait(false);
            await CloseQuietlyAsync(client, session).ConfigureAwait(false);
            return ExitBehaviourFailed;
        }

        if (reportNothing)
        {
            // Hold the session open, reporting nothing, so the suite is watching a LIVE client rather
            // than the wreckage of one - see ReportNothingHold.
            await Task.Delay(ReportNothingHold).ConfigureAwait(false);

            // PRESCRIBED: the record is never reported and the session is abandoned rather than
            // drained - a worker that vanished mid-record is exactly what this scenario models.
            // Exiting closes the sidecar's lifecycle pipe, which reaps it, so nothing is leaked by
            // not disposing.
            await Console.Out.FlushAsync().ConfigureAwait(false);
            Environment.Exit(ExitOk);
        }

        try
        {
            await client.DisposeAsync().ConfigureAwait(false);
            await session.ConfigureAwait(false);
        }
        catch (Exception failure)
        {
            await Console.Error.WriteLineAsync($"conformance-runner: closing the session: {failure.Message}")
                .ConfigureAwait(false);
            return ExitBehaviourFailed;
        }

        return ExitOk;
    }

    private static async ValueTask<Outcome> ProcessAsync(
        string behaviour, Tracker tracker, InboundRecord record, CancellationToken cancellationToken)
    {
        var ordinal = tracker.Observe(record);

        // Every branch prints its settled line BEFORE it counts the record as complete: the count is
        // what releases the main flow to dispose the client and exit, so completing first would race
        // the process exit against the line that says how the record ended.
        switch (behaviour)
        {
            case BehaviourSucceed:
                tracker.Settle(record, string.Empty);
                tracker.Complete();
                return Outcome.Succeed();

            case BehaviourReportNothing:
                // Never report, and print NO settled line - by prescription this record is never
                // resolved and the absence is the observation. A task that never completes is how a
                // .NET worker says "this record's function has not returned"; the process exits with
                // the record still in flight.
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                return Outcome.Succeed();

            case BehaviourFailThenSucceed:
                // The reason is the contract's fixed literal, never a message this runner composes:
                // the suite asserts the redelivery carries it back verbatim.
                var reported = record.Attempt == 1 ? PrescribedFailureReason : string.Empty;
                tracker.Settle(record, reported);
                tracker.Complete();
                return reported.Length == 0 ? Outcome.Succeed() : Outcome.Fail(reported);

            case BehaviourHoldFirstUntilSecond:
                if (ordinal == 1)
                {
                    // Hold the first record until a SECOND is dispatched. Whether one arrives at all,
                    // and which key it carries, is the whole of what the scenario is asking - and it
                    // is the Java suite that decides what the answer means.
                    await tracker.SecondArrived.WaitAsync(cancellationToken).ConfigureAwait(false);
                }

                tracker.Settle(record, string.Empty);
                tracker.Complete();
                return Outcome.Succeed();

            case BehaviourHoldUntilCeilingFull:
                // Hold until --max-concurrency records are held AT ONCE, keep the full group still
                // for the settle window, then release the whole group as successes. Not returning is
                // how this runner says the record's function has not returned, so a held record is
                // genuinely unresolved for as long as it looks - the property the scenario measures.
                if (!await tracker.EnterCeilingGroupAsync().ConfigureAwait(false))
                {
                    // The prescription could not be carried out. Reporting the reason rather than a
                    // plausible-looking success is the same verdict the Java binding gives, and the
                    // record is deliberately NOT counted complete: the budget that released this
                    // wait has released the main one too, so the run exits 1.
                    var never = string.Create(CultureInfo.InvariantCulture,
                        $"conformance: the ceiling group of {tracker.MaxConcurrency} never filled");
                    tracker.Settle(record, never);
                    return Outcome.Fail(never);
                }

                tracker.Settle(record, string.Empty);
                tracker.Complete();
                return Outcome.Succeed();

            default:
                // unreachable: Arguments.TryParse rejects an unknown behaviour before the session opens
                return Outcome.Fail($"conformance: unknown behaviour {behaviour}");
        }
    }

    private static async Task CloseQuietlyAsync(ParallelConsumerClient client, Task session)
    {
        try
        {
            await client.DisposeAsync().ConfigureAwait(false);
            await session.ConfigureAwait(false);
        }
        catch (Exception failure)
        {
            await Console.Error.WriteLineAsync($"conformance-runner: while shutting down: {failure.Message}")
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The six flags, spelled identically in every language - including the British
    /// <c>--behaviour</c>:
    /// <c>--scenario --behaviour --sidecar --expect-dispatches --max-concurrency --timeout-seconds</c>.
    /// All six are required, and anything missing or out of range is a usage error.
    /// </summary>
    private sealed record Arguments(
        string Scenario, string Behaviour, string Sidecar, int ExpectDispatches, int MaxConcurrency,
        int TimeoutSeconds)
    {
        public static bool TryParse(string[] argv, out Arguments parsed, out string problem)
        {
            parsed = new Arguments(string.Empty, string.Empty, string.Empty, 0, 0, 0);
            var values = new Dictionary<string, string>(StringComparer.Ordinal);
            for (var index = 0; index < argv.Length; index += 2)
            {
                if (index + 1 >= argv.Length || !argv[index].StartsWith("--", StringComparison.Ordinal))
                {
                    problem = $"expected --flag value pairs, got {argv[index]}";
                    return false;
                }

                values[argv[index]] = argv[index + 1];
            }

            foreach (var flag in new[]
                     {
                         "--scenario", "--behaviour", "--sidecar", "--expect-dispatches", "--max-concurrency",
                         "--timeout-seconds",
                     })
            {
                if (!values.TryGetValue(flag, out var value) || value.Length == 0)
                {
                    problem = $"{flag} is required";
                    return false;
                }
            }

            var behaviour = values["--behaviour"];
            if (Array.IndexOf(Behaviours, behaviour) < 0)
            {
                problem = $"unknown behaviour \"{behaviour}\"";
                return false;
            }

            var sidecar = values["--sidecar"];
            if (!Path.IsPathRooted(sidecar))
            {
                problem = $"--sidecar must be absolute, got \"{sidecar}\"";
                return false;
            }

            if (!int.TryParse(values["--expect-dispatches"], NumberStyles.Integer, CultureInfo.InvariantCulture,
                    out var expect) || expect < 1)
            {
                problem = "--expect-dispatches must be at least 1";
                return false;
            }

            if (!int.TryParse(values["--max-concurrency"], NumberStyles.Integer, CultureInfo.InvariantCulture,
                    out var ceiling) || ceiling < 1)
            {
                problem = "--max-concurrency must be at least 1";
                return false;
            }

            if (!int.TryParse(values["--timeout-seconds"], NumberStyles.Integer, CultureInfo.InvariantCulture,
                    out var budget) || budget < 1)
            {
                problem = "--timeout-seconds must be at least 1";
                return false;
            }

            parsed = new Arguments(values["--scenario"], behaviour, sidecar, expect, ceiling, budget);
            problem = string.Empty;
            return true;
        }
    }

    /// <summary>
    /// Counts deliveries and outcomes, and prints the observation line. It holds no per-record state
    /// - only counts - because the client library holds none either, and this runner must not become
    /// the place where a client's missing bookkeeping is quietly supplied.
    /// </summary>
    private sealed class Tracker
    {
        private readonly int _expected;

        private readonly int _maxConcurrency;

        /// <summary>The run's whole wall clock, which bounds every wait the prescription can block on.</summary>
        private readonly CancellationToken _budget;

        private readonly object _printing = new();

        /// <summary>
        /// The <c>hold-until-ceiling-full</c> group: how many records are held right now, and the
        /// task the current generation of them is waiting on. A cyclic barrier of the scenario's
        /// ceiling, written out rather than taken from a library, because these two fields are the
        /// whole of what a runner author in any language has to reproduce.
        /// </summary>
        private readonly object _ceilingGroup = new();

        private TaskCompletionSource _groupReleased =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _heldInGroup;

        private readonly TaskCompletionSource _second =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource _allObserved =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource _allCompleted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _observed;

        private int _completed;

        public Tracker(int expected, int maxConcurrency, CancellationToken budget)
        {
            _expected = expected;
            _maxConcurrency = maxConcurrency;
            _budget = budget;
        }

        public int Observed => Volatile.Read(ref _observed);

        public int Completed => Volatile.Read(ref _completed);

        /// <summary>The ceiling the session was configured with, and the ceiling group's width.</summary>
        public int MaxConcurrency => _maxConcurrency;

        /// <summary>Completes once a second delivery has arrived - the ordering scenario's instrument.</summary>
        public Task SecondArrived => _second.Task;

        /// <summary>Prints the delivery and returns its 1-based ordinal in arrival order.</summary>
        public int Observe(InboundRecord record)
        {
            int ordinal;
            lock (_printing)
            {
                ordinal = ++_observed;
                // Printed at the moment of delivery, before the behaviour acts on it, and under the
                // same lock as the ordinal so the transcript's ORDER is the arrival order: executors
                // are tasks here and two of them share one stdout. reason comes last because it is
                // worker-supplied text that may contain spaces.
                Console.Out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"dispatch key={Text(record.Key)} offset={record.Offset} attempt={record.Attempt} " +
                    $"reason={record.LastFailureReason ?? string.Empty}"));
                Console.Out.Flush();
            }

            if (ordinal >= 2)
            {
                _second.TrySetResult();
            }

            if (ordinal >= _expected)
            {
                _allObserved.TrySetResult();
            }

            return ordinal;
        }

        /// <summary>
        /// Prints the record's outcome, at the moment the prescribed behaviour decided it - which is
        /// the moment the record stops being unresolved.
        /// </summary>
        /// <remarks>
        /// UNDER THE SAME LOCK AS THE DISPATCH LINE, because the suite reads overlap purely from the
        /// ORDER of the two line types and no clock is involved: a dispatch opens a record's
        /// unresolved window, its settled line closes it, and the running difference between the two
        /// counts in line order is how many records this client was holding at that instant.
        /// Executors here are tasks sharing one stdout, so an unserialized write would report a peak
        /// that never happened about a client that behaved perfectly.
        /// </remarks>
        /// <param name="record">The delivery whose outcome has been decided.</param>
        /// <param name="reason">The failure reason THIS runner is reporting; empty for a success.</param>
        public void Settle(InboundRecord record, string reason)
        {
            lock (_printing)
            {
                Console.Out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"settled key={Text(record.Key)} offset={record.Offset} attempt={record.Attempt} " +
                    $"reason={reason}"));
                Console.Out.Flush();
            }
        }

        /// <summary>
        /// The cyclic barrier at the heart of <c>hold-until-ceiling-full</c>: hold this record until
        /// it is one of <see cref="MaxConcurrency"/> held at once, keep the full group still for
        /// <see cref="CeilingSettle"/>, and release it. Called AFTER the dispatch line is printed.
        /// </summary>
        /// <remarks>
        /// A group also releases once every prescribed delivery has been observed, so a scenario
        /// whose record count is not a multiple of its ceiling cannot strand its last, short group.
        /// </remarks>
        /// <returns>
        /// False if the group never filled inside the budget - this runner failing to carry out the
        /// prescription, rather than the client being wrong about anything.
        /// </returns>
        public async Task<bool> EnterCeilingGroupAsync()
        {
            Task released;
            bool releasing;
            lock (_ceilingGroup)
            {
                // The task captured here IS this record's generation: the releaser swaps in a fresh
                // one, so awaiting the captured task is exactly "wait until the generation is no
                // longer mine". C# cannot await inside a lock, which is what makes an async barrier
                // read differently from the Java one - the state is decided under the lock and every
                // wait happens after it has been left.
                released = _groupReleased.Task;
                _heldInGroup++;
                releasing = _heldInGroup >= _maxConcurrency || Observed >= _expected;
            }

            if (!releasing)
            {
                try
                {
                    await released.WaitAsync(_budget).ConfigureAwait(false);
                    return true;
                }
                catch (OperationCanceledException)
                {
                    return false;
                }
            }

            // THE SETTLE WINDOW, HELD OUTSIDE THE LOCK so a record the engine should not be
            // dispatching can still print its arrival line if it turns up - that arrival is the whole
            // thing the scenario looks for. A correct engine cannot dispatch anything here, the
            // ceiling being full, so a dispatch line inside this window IS the excess.
            await Task.Delay(CeilingSettle).ConfigureAwait(false);

            lock (_ceilingGroup)
            {
                _heldInGroup = 0;
                var waiters = _groupReleased;
                _groupReleased = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                waiters.SetResult();
            }

            return true;
        }

        public void Complete()
        {
            if (Interlocked.Increment(ref _completed) >= _expected)
            {
                _allCompleted.TrySetResult();
            }
        }

        /// <summary>Whether the prescription finished inside the budget.</summary>
        public async Task<bool> WaitForPrescribedBehaviourAsync(bool atObservation)
        {
            var finished = atObservation ? _allObserved.Task : _allCompleted.Task;
            try
            {
                await finished.WaitAsync(_budget).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        private static string Text(byte[]? bytes) =>
            bytes is null ? string.Empty : Encoding.UTF8.GetString(bytes);
    }
}
