# Copyright (C) 2026 Antony Stubbs and contributors

"""Python's half of the shared cross-language conformance suite (astubbs#242, confluentinc#154).

IT ASSERTS NOTHING, DELIBERATELY. The suite that knows what correct looks like - offset frontiers,
ordering, redelivery, attempt counts - is the Java module
parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance, and it keeps owning that
knowledge for every language. This program's whole job is to DO WHAT THE SCENARIO SAYS and then
exit; if it were free to decide what "correct" means, ten languages would each decide it slightly
differently and the agreement between them would prove nothing.

Its contract - flags, exit codes, the two stdout lines, the behaviour tokens - is documented once,
in parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance/README.md, and is identical
in every language. Read that before changing anything here.

THIS DOES NOT REPLACE THE PACKAGE'S OWN TESTS. The shared suite proves every client behaves
identically on the protocol; tests/ catches what is invisible from outside the process - a channel
inherited across a fork, a worker that dies holding a record, a queue that hands out wrongly. Both
layers are load-bearing.

WHY THE COORDINATION IS multiprocessing PRIMITIVES. This client runs the user's function in worker
PROCESSES, so the scenario's own bookkeeping - the delivery ordinal, "has a second arrived yet",
the ceiling group's held count and generation - cannot be a closure over ordinary variables: each
worker would increment its own copy. They are fork-context primitives, created before the client
exists and inherited by every worker. A threading.Lock or threading.Condition here would coordinate
nothing at all: it would be duplicated by the fork and every worker would hold its own, uncontended.
That is why every prescribed hold below - the barrier included - is written against
multiprocessing.Condition, Event and Value rather than the threading spellings the pseudocode in
the contract reads like.
"""

from __future__ import annotations

import argparse
import multiprocessing
import os
import queue
import sys
import time
from datetime import timedelta
from typing import Any

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
BEHAVIOUR_HOLD_UNTIL_CEILING_FULL = "hold-until-ceiling-full"
BEHAVIOURS = (
    BEHAVIOUR_SUCCEED,
    BEHAVIOUR_REPORT_NOTHING,
    BEHAVIOUR_FAIL_THEN_SUCCEED,
    BEHAVIOUR_HOLD_FIRST_UNTIL_SECOND,
    BEHAVIOUR_HOLD_UNTIL_CEILING_FULL,
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

# How long hold-until-ceiling-full keeps a FULL group held before releasing it.
#
# IT IS WHAT TURNS "the ceiling was never exceeded" FROM A RACE INTO A MEASUREMENT. Release the
# group the instant it fills and a client that declared a larger ceiling still passes - its extra
# records arrive a few milliseconds later, by which time the outstanding count has already fallen
# back. Holding the full ceiling still means the extra dispatch arrives INSIDE the window and prints
# its line while every other record is unresolved. A correct engine cannot dispatch anything during
# the window at all, so the wait costs a conforming client nothing but time.
CEILING_SETTLE_SECONDS = 0.25

DISPATCH_LINE = "dispatch key={key} offset={offset} attempt={attempt} reason={reason}"

# The second line per record, printed the moment the prescribed behaviour has DECIDED that record's
# outcome. Its reason is the failure this runner is REPORTING - empty for a success - not the one
# the record arrived with. The suite reads overlap from the ORDER of the two line types alone, no
# clock involved, which is why both are printed under the delivery lock below.
SETTLED_LINE = "settled key={key} offset={offset} attempt={attempt} reason={reason}"


def main(argv: list[str]) -> int:
    options = parse(argv)
    if isinstance(options, int):
        return options
    return run(options)


def parse(argv: list[str]) -> argparse.Namespace | int:
    """The six flags, spelled identically in every language - including the British --behaviour."""
    parser = argparse.ArgumentParser(prog="conformance-runner", add_help=False)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--behaviour", required=True, choices=BEHAVIOURS)
    parser.add_argument("--sidecar", required=True)
    parser.add_argument("--expect-dispatches", required=True, type=int)
    parser.add_argument("--max-concurrency", required=True, type=int)
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
    if parsed.max_concurrency < 1:
        print("conformance-runner: --max-concurrency must be at least 1", file=sys.stderr)
        return EXIT_USAGE
    if parsed.timeout_seconds < 1:
        print("conformance-runner: --timeout-seconds must be at least 1", file=sys.stderr)
        return EXIT_USAGE
    return parsed


def run(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + args.timeout_seconds
    tracker = Tracker(args.expect_dispatches, args.behaviour, args.max_concurrency,
                      args.timeout_seconds)

    options = ClientOptions(
        # THE SCENARIO NAME IS ALSO THE TOPIC NAME.
        topics=[args.scenario],
        # The ceiling is the SCENARIO's to choose and this runner never derives one - it comes from
        # --max-concurrency and from nothing else. Deriving it from --expect-dispatches, which is
        # what this line used to do, is a ceiling no scenario can reach, so no scenario could ask a
        # client to prove it respected one.
        max_concurrency=args.max_concurrency,
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


def await_ceiling_group(ceiling: Any, held: Any, generation: Any, observed: Any,
                        max_concurrency: int, expected: int, budget: float) -> bool:
    """The cyclic barrier at the heart of hold-until-ceiling-full, ACROSS PROCESSES.

    Block until this record is one of ``max_concurrency`` held at once, keep the full group still
    for CEILING_SETTLE_SECONDS, and release it. A group also releases once every prescribed delivery
    has been observed, so a scenario whose record count is not a multiple of its ceiling cannot
    strand its last, short group.

    Every argument except the last three is a fork-context primitive: the workers holding this
    barrier are different processes, so ``held`` and ``generation`` are shared memory and the
    condition is a multiprocessing one. The threading spellings would compile, run, and synchronize
    each worker with only itself.

    :returns: False if the group never filled inside the budget, which is this runner failing rather
        than the client being wrong about anything.
    """
    deadline = time.monotonic() + budget
    with ceiling:
        mine = generation.value
        held.value += 1
        releasing = held.value >= max_concurrency or observed.value >= expected
        if not releasing:
            while generation.value == mine:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                ceiling.wait(timeout=remaining)
            return True

    # THE SETTLE WINDOW, HELD OUTSIDE THE LOCK so a record the engine should not be dispatching can
    # still print its arrival if it turns up - that arrival is the whole thing the scenario looks
    # for. A correct engine cannot dispatch anything here, the ceiling being full, so an extra
    # dispatch line inside this window IS the excess.
    time.sleep(CEILING_SETTLE_SECONDS)
    with ceiling:
        held.value = 0
        generation.value += 1
        ceiling.notify_all()
    return True


class Tracker:
    """Counts deliveries and outcomes across the worker processes, and prints the observations.

    It holds no per-record state - only counts - because the client library holds none either, and
    this runner must not become the place where a client's missing bookkeeping is quietly supplied.
    """

    def __init__(self, expected: int, behaviour: str, max_concurrency: int, budget: float) -> None:
        self._expected = expected
        self._behaviour = behaviour
        self._max_concurrency = max_concurrency
        self._budget = budget
        # The fork context, matching the one the client's pool uses: these primitives are created
        # before any worker exists and are inherited by every one of them.
        context = multiprocessing.get_context("fork")
        self._lock = context.Lock()
        self._ordinal = context.Value("i", 0)
        self._second_arrived = context.Event()
        self._held_forever = context.Event()
        # The ceiling group's whole state: how many records are held right now, and which generation
        # of the group they belong to. Both are unlocked shared memory because every access is made
        # under the condition, which carries the only lock there is - the same three lines of state
        # every other language writes out.
        self._ceiling = context.Condition()
        self._held_in_group = context.Value("i", 0, lock=False)
        self._generation = context.Value("l", 0, lock=False)
        self._events: multiprocessing.Queue[str] = context.Queue()
        self.observed = 0
        self.completed = 0

    def processor(self):
        """The user's function, as a closure over this tracker - which is why the pool must fork."""
        expected = self._expected
        behaviour = self._behaviour
        max_concurrency = self._max_concurrency
        budget = self._budget
        lock = self._lock
        ordinal_counter = self._ordinal
        second_arrived = self._second_arrived
        held_forever = self._held_forever
        ceiling = self._ceiling
        held_in_group = self._held_in_group
        generation = self._generation
        events = self._events

        def process(record):
            key = "" if record.key is None else record.key.decode("utf-8", "replace")

            def settled(reason: str) -> None:
                """Prints this record's outcome, under the SAME lock the dispatch line took.

                The suite reads how many records were unresolved at an instant from the running
                difference between the two line types in line order, so the order of the writes has
                to be the order of the events - and here that means the order across processes.
                """
                with lock:
                    print(SETTLED_LINE.format(key=key, offset=record.offset,
                                              attempt=record.attempt, reason=reason), flush=True)

            with lock:
                ordinal_counter.value += 1
                ordinal = ordinal_counter.value
                # Printed at the moment of delivery, before the behaviour acts on it, and under the
                # same lock as the ordinal so the transcript's ORDER is the arrival order: two
                # shards deliver into two processes writing one stdout.
                print(DISPATCH_LINE.format(
                    key=key,
                    offset=record.offset,
                    attempt=record.attempt,
                    reason=record.last_failure_reason or ""), flush=True)
            events.put("observed")
            if ordinal >= 2:
                second_arrived.set()

            if behaviour == BEHAVIOUR_SUCCEED:
                settled("")
                events.put("completed")
                return None

            if behaviour == BEHAVIOUR_REPORT_NOTHING:
                # Never report, and print no settled line EVER: by prescription this record is never
                # resolved, and the absence is the observation. Blocking here is how a Python worker
                # says "this record's function has not returned"; the shutdown terminates the worker
                # with the record still out.
                held_forever.wait()
                return None

            if behaviour == BEHAVIOUR_FAIL_THEN_SUCCEED:
                reason = PRESCRIBED_FAILURE_REASON if record.attempt == 1 else ""
                settled(reason)
                events.put("completed")
                if reason:
                    # A raise IS Python's failure idiom, and its text is the reason verbatim.
                    raise RuntimeError(reason)
                return None

            if behaviour == BEHAVIOUR_HOLD_FIRST_UNTIL_SECOND:
                # Hold the first record until a SECOND is dispatched. Whether one arrives at all,
                # and which key it carries, is the whole of what the scenario is asking - and it is
                # the Java suite that decides what the answer means.
                if ordinal == 1 and not second_arrived.wait(timeout=expected * 60):
                    # The prescription could not be carried out, so this reports a conformance
                    # failure and never completes: the parent's budget is what turns that into
                    # exit 1, which is the path every give-up in this runner already takes.
                    failure = "conformance: no second dispatch arrived while the first was held"
                    settled(failure)
                    raise RuntimeError(failure)
                settled("")
                events.put("completed")
                return None

            if behaviour == BEHAVIOUR_HOLD_UNTIL_CEILING_FULL:
                # Hold EVERY delivery until max_concurrency of them are held at once, keep the full
                # group still for the settle window, then succeed all of them and begin the next
                # group. Blocking is how this layer says the record's function has not returned, so
                # a held record is genuinely unresolved for as long as it looks - which is the
                # property the scenario measures.
                if not await_ceiling_group(ceiling, held_in_group, generation, ordinal_counter,
                                           max_concurrency, expected, budget):
                    failure = f"conformance: the ceiling group of {max_concurrency} never filled"
                    settled(failure)
                    raise RuntimeError(failure)
                settled("")
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
