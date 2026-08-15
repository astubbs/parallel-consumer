# Copyright (C) 2026 Antony Stubbs and contributors

"""Python's half of the shared cross-language conformance suite (astubbs#242, confluentinc#154).

IT ASSERTS NOTHING, DELIBERATELY. The suite that knows what correct looks like - offset frontiers,
ordering, redelivery, attempt counts - is the Java module
parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance, and it keeps owning that
knowledge for every language. This program's whole job is to DO WHAT THE SCENARIO SAYS and then
exit; if it were free to decide what "correct" means, ten languages would each decide it slightly
differently and the agreement between them would prove nothing.

Its contract - flags, exit codes, the stdout line, the behaviour tokens - is documented once, in
parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance/README.md, and is identical in
every language. Read that before changing anything here.

THIS DOES NOT REPLACE THE PACKAGE'S OWN TESTS. The shared suite proves every client behaves
identically on the protocol; tests/ catches what is invisible from outside the process - a channel
inherited across a fork, a worker that dies holding a record, a queue that hands out wrongly. Both
layers are load-bearing.

WHY THE COORDINATION IS multiprocessing PRIMITIVES. This client runs the user's function in worker
PROCESSES, so the scenario's own bookkeeping - the delivery ordinal, "has a second arrived yet" -
cannot be a closure over ordinary variables: each worker would increment its own copy. They are
fork-context primitives, created before the client exists and inherited by every worker.
"""

from __future__ import annotations

import argparse
import multiprocessing
import os
import queue
import sys
import time
from datetime import timedelta

from parallel_consumer import ClientOptions, ParallelConsumerClient

# Exit statuses ARE the verdict channel. There is no results file, no report message and no second
# protocol: a scenario passed if this process exited 0 and the Java suite's own assertions about
# engine state held.
EXIT_OK = 0
EXIT_BEHAVIOUR_FAILED = 1
EXIT_USAGE = 2

BEHAVIOUR_SUCCEED = "succeed"
BEHAVIOUR_REPORT_NOTHING = "report-nothing"
BEHAVIOUR_FAIL_THEN_SUCCEED = "fail-then-succeed"
BEHAVIOUR_HOLD_FIRST_UNTIL_SECOND = "hold-first-until-second"
BEHAVIOURS = (
    BEHAVIOUR_SUCCEED,
    BEHAVIOUR_REPORT_NOTHING,
    BEHAVIOUR_FAIL_THEN_SUCCEED,
    BEHAVIOUR_HOLD_FIRST_UNTIL_SECOND,
)

# The exact text a fail-then-succeed run reports. The Java suite asserts the redelivery carries it
# back VERBATIM, so it is a fixed literal of the contract in every language, never composed here.
PRESCRIBED_FAILURE_REASON = "conformance-prescribed-failure"

# Fixed session tunables, contract rather than this runner's judgement: they exist only so scenarios
# converge at unit-test speed against the engine's production defaults (a 5s commit interval, a 1s
# retry delay). Every language sets the same two values.
COMMIT_INTERVAL = timedelta(milliseconds=100)
RETRY_DELAY = timedelta(milliseconds=50)

# How long a report-nothing run keeps its session OPEN after its last observation.
#
# IT IS WHAT MAKES THE NEGATIVE CONTROL A CONTROL. Without it the runner ends the instant the record
# arrives, and a sabotaged runner that DID report success has its report killed in flight by the
# shutdown - so the suite sees an unadvanced offset either way and the scenario passes for a broken
# client. Measured in the Go wave, not reasoned about: reporting success from this behaviour left
# the suite green until the hold existed.
REPORT_NOTHING_HOLD_SECONDS = 3.0

# How long the report-nothing shutdown waits for a record that will never be reported.
#
# THIS IS WHERE PYTHON DEVIATES FROM THE CONTRACT'S LETTER, AND WHY. The contract says a
# report-nothing runner exits "without a clean close" - Go can abandon its session because its
# workers are goroutines that die with the process. Python's workers are PROCESSES, and a runner
# that abandoned them would leave a blocked interpreter behind on the machine for every negative
# control it ran. So the session is shut down instead, with a drain short enough that the held
# record is never reported: observably identical - no outcome for the record, ever - and it reaps
# what it started.
REPORT_NOTHING_DRAIN_SECONDS = 2.0

DISPATCH_LINE = "dispatch key={key} offset={offset} attempt={attempt} reason={reason}"


def main(argv: list[str]) -> int:
    options = parse(argv)
    if isinstance(options, int):
        return options
    return run(options)


def parse(argv: list[str]) -> argparse.Namespace | int:
    """The five flags, spelled identically in every language - including the British --behaviour."""
    parser = argparse.ArgumentParser(prog="conformance-runner", add_help=False)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--behaviour", required=True, choices=BEHAVIOURS)
    parser.add_argument("--sidecar", required=True)
    parser.add_argument("--expect-dispatches", required=True, type=int)
    parser.add_argument("--timeout-seconds", required=True, type=int)
    try:
        parsed = parser.parse_args(argv)
    except SystemExit:
        # argparse exits 2 itself, which is this contract's usage status - but going through
        # SystemExit would skip the flush below, so it is caught and returned.
        return EXIT_USAGE

    if not os.path.isabs(parsed.sidecar):
        print(f"conformance-runner: --sidecar must be absolute, got {parsed.sidecar!r}",
              file=sys.stderr)
        return EXIT_USAGE
    if parsed.expect_dispatches < 1:
        print("conformance-runner: --expect-dispatches must be at least 1", file=sys.stderr)
        return EXIT_USAGE
    if parsed.timeout_seconds < 1:
        print("conformance-runner: --timeout-seconds must be at least 1", file=sys.stderr)
        return EXIT_USAGE
    return parsed


def run(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + args.timeout_seconds
    tracker = Tracker(args.expect_dispatches, args.behaviour)

    options = ClientOptions(
        # THE SCENARIO NAME IS ALSO THE TOPIC NAME.
        topics=[args.scenario],
        # Enough executors for every dispatch the scenario prescribes, so a scenario that holds a
        # record cannot deadlock on an executor count smaller than its own shape.
        max_concurrency=args.expect_dispatches,
        commit_interval=COMMIT_INTERVAL,
        default_message_retry_delay=RETRY_DELAY,
        # The mock lane builds mock Kafka clients and reads no properties. Real credentials never
        # belong in a conformance test.
        kafka_properties={},
    )
    drain = (REPORT_NOTHING_DRAIN_SECONDS if args.behaviour == BEHAVIOUR_REPORT_NOTHING else 30.0)
    client = ParallelConsumerClient(options, sidecar=args.sidecar, drain_timeout=drain)

    try:
        client.poll(tracker.processor())
    except Exception as failure:
        print(f"conformance-runner: starting the poll: {failure}", file=sys.stderr)
        client.close()
        return EXIT_BEHAVIOUR_FAILED

    # report-nothing completes at OBSERVATION, because by prescription its records are never
    # reported and so can never complete. Every other behaviour completes when the last record it
    # was handed has had its outcome decided.
    if not tracker.await_prescribed_behaviour(deadline):
        print(f"conformance-runner: scenario {args.scenario!r} behaviour "
              f"{args.behaviour!r} did not complete within {args.timeout_seconds}s - observed "
              f"{tracker.observed} of {args.expect_dispatches}, completed {tracker.completed}",
              file=sys.stderr)
        close_quietly(client)
        return EXIT_BEHAVIOUR_FAILED

    if args.behaviour == BEHAVIOUR_REPORT_NOTHING:
        # Hold the session open, reporting nothing, so the suite is watching a LIVE client rather
        # than the wreckage of one - see REPORT_NOTHING_HOLD_SECONDS.
        time.sleep(REPORT_NOTHING_HOLD_SECONDS)
        close_quietly(client)
        return EXIT_OK

    try:
        client.close()
    except Exception as failure:
        print(f"conformance-runner: closing the session: {failure}", file=sys.stderr)
        return EXIT_BEHAVIOUR_FAILED
    return EXIT_OK


def close_quietly(client: ParallelConsumerClient) -> None:
    """Shuts down when the verdict is already decided: a close error must not rewrite it."""
    try:
        client.close()
    except Exception as failure:
        print(f"conformance-runner: while shutting down: {failure}", file=sys.stderr)


class Tracker:
    """Counts deliveries and outcomes across the worker processes, and prints the observations.

    It holds no per-record state - only counts - because the client library holds none either, and
    this runner must not become the place where a client's missing bookkeeping is quietly supplied.
    """

    def __init__(self, expected: int, behaviour: str) -> None:
        self._expected = expected
        self._behaviour = behaviour
        # The fork context, matching the one the client's pool uses: these primitives are created
        # before any worker exists and are inherited by every one of them.
        context = multiprocessing.get_context("fork")
        self._lock = context.Lock()
        self._ordinal = context.Value("i", 0)
        self._second_arrived = context.Event()
        self._held_forever = context.Event()
        self._events: multiprocessing.Queue[str] = context.Queue()
        self.observed = 0
        self.completed = 0

    def processor(self):
        """The user's function, as a closure over this tracker - which is why the pool must fork."""
        expected = self._expected
        behaviour = self._behaviour
        lock = self._lock
        ordinal_counter = self._ordinal
        second_arrived = self._second_arrived
        held_forever = self._held_forever
        events = self._events

        def process(record):
            with lock:
                ordinal_counter.value += 1
                ordinal = ordinal_counter.value
                # Printed at the moment of delivery, before the behaviour acts on it, and under the
                # same lock as the ordinal so the transcript's ORDER is the arrival order: two
                # shards deliver into two processes writing one stdout.
                print(DISPATCH_LINE.format(
                    key="" if record.key is None else record.key.decode("utf-8", "replace"),
                    offset=record.offset,
                    attempt=record.attempt,
                    reason=record.last_failure_reason or ""), flush=True)
            events.put("observed")
            if ordinal >= 2:
                second_arrived.set()

            if behaviour == BEHAVIOUR_SUCCEED:
                events.put("completed")
                return None

            if behaviour == BEHAVIOUR_REPORT_NOTHING:
                # Never report. Blocking here is how a Python worker says "this record's function
                # has not returned"; the shutdown terminates the worker with the record still out.
                held_forever.wait()
                return None

            if behaviour == BEHAVIOUR_FAIL_THEN_SUCCEED:
                events.put("completed")
                if record.attempt == 1:
                    # A raise IS Python's failure idiom, and its text is the reason verbatim.
                    raise RuntimeError(PRESCRIBED_FAILURE_REASON)
                return None

            if behaviour == BEHAVIOUR_HOLD_FIRST_UNTIL_SECOND:
                # Hold the first record until a SECOND is dispatched. Whether one arrives at all,
                # and which key it carries, is the whole of what the scenario is asking - and it is
                # the Java suite that decides what the answer means.
                if ordinal == 1 and not second_arrived.wait(timeout=expected * 60):
                    raise RuntimeError("conformance: no second dispatch arrived while the first "
                                       "was held")
                events.put("completed")
                return None

            # unreachable: parse() rejects an unknown behaviour before the session opens
            raise RuntimeError(f"conformance: unknown behaviour {behaviour!r}")

        return process

    def await_prescribed_behaviour(self, deadline: float) -> bool:
        """Drains the workers' events until the prescription is finished, or the budget runs out."""
        while True:
            enough = (self.observed if self._behaviour == BEHAVIOUR_REPORT_NOTHING
                      else self.completed)
            if enough >= self._expected:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            try:
                event = self._events.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                # Not yet - the deadline above is what decides when "not yet" becomes "never".
                continue
            if event == "observed":
                self.observed += 1
            elif event == "completed":
                self.completed += 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
