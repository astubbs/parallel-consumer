# Copyright (C) 2026 Antony Stubbs and contributors

"""The demo's dials, and Python's copy of the interface every per-language demo mirrors.

The contract is `parallel-consumer-proxy/demo/README.md`; the seed that defines the spellings is
the Java demo's ``DemoOptions``. Flags beat environment variables beat defaults - the ordinary
convention, chosen because a container passes configuration by environment while a person at a
terminal passes flags, and both must be able to override the other's layer.

**R39 does not govern a demo.** R39 constrains how configuration reaches the *proxy*; a demo is an
application, so its flags are not a violation of it. Nothing here reaches the sidecar by argv or
environment - the client library still carries every setting in the connect-time handshake.

**One default diverges from the seed, deliberately: ``--concurrency``.** In Java 100 in-flight
records are 100 sleeping threads. Here they are 100 worker *processes*, because the proxy's
executor-count function is `IntUnaryOperator.identity()` today
(`docs/inflight/blocker-executor-count-formula.md` - an open owner decision, not this demo's to
make). A demo that spawned 100 interpreters on a laptop would be measuring the machine's swap
behaviour, so this one asks for :data:`DEFAULT_CONCURRENCY`. Every other default is the seed's.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
from collections.abc import Mapping

__all__ = ["ENV_PREFIX", "USAGE", "DemoOptions", "parse"]

ENV_PREFIX = "PC_DEMO_"
"""Prefix for every environment variable this demo reads, so a reader can grep one string."""

DEFAULT_RECORDS = 2000
DEFAULT_DELAY_MS = 2
DEFAULT_CONCURRENCY = 16
"""The one default that is not the seed's. See this module's docstring for why.

Sixteen rather than a hundred, and rather than a count derived from ``os.cpu_count()``: a demo
whose headline setting changes with the machine cannot be compared between two readers' runs, and
the fingerprint would be the only place the difference showed. A fixed number is reproducible, and
the flag is there for a reader with a bigger machine.
"""
DEFAULT_PARTITIONS = 10
DEFAULT_REPLAY_FACTOR = 20

USAGE = f"""usage: demo/run.sh [options]

  --records N        records in the comparison replay   (default {DEFAULT_RECORDS})
  --delay-ms N       simulated work per record, ms      (default {DEFAULT_DELAY_MS})
  --concurrency N    max in-flight records              (default {DEFAULT_CONCURRENCY}, not 100:
                     in Python that many in-flight records is that many worker PROCESSES)
  --partitions N     partitions on the demo topic       (default {DEFAULT_PARTITIONS})
  --replay-factor N  big replay = records x N; 1 skips  (default {DEFAULT_REPLAY_FACTOR})
  --bootstrap ADDR   an existing broker; omit and run.sh starts one
  --topic NAME       an existing topic; omit to create one

Every flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
Flags beat the environment beats the defaults."""


@dataclasses.dataclass(frozen=True)
class DemoOptions:
    """The effective configuration, after flags, environment and defaults have been resolved."""

    records: int = DEFAULT_RECORDS
    delay_ms: int = DEFAULT_DELAY_MS
    max_concurrency: int = DEFAULT_CONCURRENCY
    partitions: int = DEFAULT_PARTITIONS
    replay_factor: int = DEFAULT_REPLAY_FACTOR
    bootstrap: str | None = None
    topic: str | None = None

    @property
    def big_replay_records(self) -> int:
        """The records the big replay consumes in total, including the small replay's own."""
        return self.records * max(1, self.replay_factor)

    @property
    def big_replay_wanted(self) -> bool:
        """True when the big replay is worth running at all; a factor of 1 or less skips it."""
        return self.replay_factor > 1

    @property
    def delay_seconds(self) -> float:
        return self.delay_ms / 1000.0

    def fingerprint(self, topic: str) -> str:
        """The effective configuration, for printing before the run.

        A number without its settings is not reproducible, so this is part of the contract every
        language's demo keeps rather than a debugging aid. **The bootstrap address is deliberately
        absent**: own-cluster mode puts a user's real broker address here, and the
        credential-hygiene rule that binds the proxy binds a demo too. The keys are the seed's
        spellings, camel case included, so that two languages' fingerprints diff cleanly.
        """
        return (
            f"records = {self.records}"
            f"\n  delayMs = {self.delay_ms}"
            f"\n  maxConcurrency = {self.max_concurrency}"
            f"\n  partitions = {self.partitions}"
            f"\n  replayFactor = {self.replay_factor}"
            f"\n  topic = {topic}"
        )


class OptionError(Exception):
    """A flag, or an environment variable, that this demo cannot act on.

    Raised rather than tolerated: a demo that silently ignores a misspelled flag reports numbers
    for settings the user did not ask for.
    """


def is_help_requested(argv: list[str]) -> bool:
    """Whether the caller asked for the usage text rather than a run.

    Handled here rather than only in ``run.sh``, because the script is not the only way in:
    ``docker compose run demo --help`` reaches ``reference_demo`` directly, and answering that
    with "unrecognized arguments" would be a poor first impression of the demo ten languages copy.
    """
    return any(argument in ("-h", "--help") for argument in argv)


def parse(argv: list[str], env: Mapping[str, str] | None = None) -> DemoOptions:
    """Parses the demo's command line, falling back to the environment and then to the defaults.

    :param argv: the process arguments, which may legitimately be empty - that is the double-click
        case, and it must work.
    :param env: the environment to read, passed in rather than read from :mod:`os` so this is
        testable without mutating the interpreter's own environment.
    :raises OptionError: on an unknown flag, a missing value, or a value that is not a number in
        range.
    """
    environment = os.environ if env is None else env

    # From the environment first, so a flag of the same name overwrites it below. argparse's own
    # defaults are therefore the environment's values rather than the constants.
    defaults = DemoOptions(
        records=_positive("PC_DEMO_RECORDS", _from_env(environment, "RECORDS"), DEFAULT_RECORDS),
        delay_ms=_non_negative(
            "PC_DEMO_DELAY_MS", _from_env(environment, "DELAY_MS"), DEFAULT_DELAY_MS),
        max_concurrency=_positive(
            "PC_DEMO_CONCURRENCY", _from_env(environment, "CONCURRENCY"), DEFAULT_CONCURRENCY),
        partitions=_positive(
            "PC_DEMO_PARTITIONS", _from_env(environment, "PARTITIONS"), DEFAULT_PARTITIONS),
        # 1 or less skips the big replay, so this one is allowed to be zero.
        replay_factor=_non_negative(
            "PC_DEMO_REPLAY_FACTOR", _from_env(environment, "REPLAY_FACTOR"),
            DEFAULT_REPLAY_FACTOR),
        bootstrap=_from_env(environment, "BOOTSTRAP"),
        topic=_from_env(environment, "TOPIC"),
    )

    # add_help=False and exit_on_error are both deliberate: argparse's own --help text would not be
    # the contract's, and its default behaviour on a bad flag is to print to stderr and call
    # sys.exit - which a caller that wants to report the error itself cannot intercept.
    parser = argparse.ArgumentParser(prog="demo/run.sh", add_help=False, exit_on_error=False)
    parser.add_argument("--records", type=str)
    parser.add_argument("--delay-ms", dest="delay_ms", type=str)
    parser.add_argument("--concurrency", dest="max_concurrency", type=str)
    parser.add_argument("--partitions", type=str)
    parser.add_argument("--replay-factor", dest="replay_factor", type=str)
    parser.add_argument("--bootstrap", type=str)
    parser.add_argument("--topic", type=str)

    try:
        parsed, extra = parser.parse_known_args(argv)
    except argparse.ArgumentError as bad:
        raise OptionError(str(bad)) from bad
    if extra:
        raise OptionError(f"unknown option: {extra[0]}")

    options = DemoOptions(
        records=_positive("--records", parsed.records, defaults.records),
        delay_ms=_non_negative("--delay-ms", parsed.delay_ms, defaults.delay_ms),
        max_concurrency=_positive(
            "--concurrency", parsed.max_concurrency, defaults.max_concurrency),
        partitions=_positive("--partitions", parsed.partitions, defaults.partitions),
        replay_factor=_non_negative(
            "--replay-factor", parsed.replay_factor, defaults.replay_factor),
        bootstrap=parsed.bootstrap if parsed.bootstrap else defaults.bootstrap,
        topic=parsed.topic if parsed.topic else defaults.topic,
    )

    # Checked here rather than trusted later. Python integers do not overflow, so the hazard the
    # seed guards against cannot arise - but a big replay of a hundred million records is still a
    # demo that never finishes, and saying so now beats saying nothing for an hour.
    if options.big_replay_records > 100_000_000:
        raise OptionError(
            f"--records times --replay-factor is {options.big_replay_records}, which is more "
            "records than this demo will finish in a sane wall clock; lower one of them")
    return options


def _from_env(env: Mapping[str, str], suffix: str) -> str | None:
    raw = env.get(ENV_PREFIX + suffix)
    return None if raw is None or not raw.strip() else raw.strip()


def _positive(name: str, raw: str | None, fallback: int) -> int:
    if raw is None:
        return fallback
    parsed = _number(name, raw)
    if parsed < 1:
        raise OptionError(f"{name} must be at least 1, got {parsed}")
    return parsed


def _non_negative(name: str, raw: str | None, fallback: int) -> int:
    if raw is None:
        return fallback
    parsed = _number(name, raw)
    if parsed < 0:
        raise OptionError(f"{name} must not be negative, got {parsed}")
    return parsed


def _number(name: str, raw: str) -> int:
    try:
        return int(raw.strip())
    except ValueError as bad:
        raise OptionError(f"{name} needs a whole number, got {raw!r}") from bad
