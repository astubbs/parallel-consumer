# Copyright (C) 2026 Antony Stubbs and contributors

"""The broker the demo talks to, and the backlog every arm replays.

This is Python's counterpart to the Java demo's ``DemoBroker``, minus one thing it deliberately
does not do: **it never starts a broker.** The seed uses Testcontainers, which has no equivalent
here that would not put Docker's API into a client library's demo, so ``demo/run.sh`` starts the
compose broker and hands this an address. From the reader's side the promise is unchanged - run
``demo/run.sh`` with no arguments and a broker appears - and it keeps the rule that matters: a
demo container is never given the host Docker socket, so inside the container the address is a
compose sibling either way.

Everything here uses `confluent-kafka` (librdkafka), which is Python's own Apache Kafka client.
It is the AK core arm's engine, and it is also what creates the topic and produces the backlog -
exactly as the seed's single JVM does, and for the same reason: a comparison needs both sides.
The sidecar arm's *path* still carries no Kafka I/O in this process, which is the claim the arm
actually makes.
"""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

__all__ = ["KEY_SPACE", "consumer_properties", "ensure_topic", "key_of", "key_slot", "seed"]

log = logging.getLogger(__name__)

KEY_SPACE = 1_000
"""How many distinct keys the backlog uses, matching the seed so the workloads are comparable.

It is load-bearing for a key-ordered engine and merely descriptive for an unordered one; this
demo runs unordered, so it is here for parity rather than for effect.
"""

_KEY_PREFIX = "key-"
"""The backlog's key format, written once because two places now depend on it.

:func:`seed` produces it and :func:`key_slot` reads it back, and the reason the second exists is
the contract's **unique keys** column: the sidecar arm's user function runs in worker *processes*,
so "the set of keys this arm saw" cannot be a Python ``set`` in any one of them. A key that maps
back to an ordinal maps to a slot in a shared array instead - one byte, no lock, no IPC.
"""


_TOPIC_CREATION_TIMEOUT = 30.0
"""Long enough for a broker that has just booted to elect leaders for every partition."""


def key_of(ordinal: int) -> bytes:
    """The key the backlog's ``ordinal``-th record carries."""
    return f"{_KEY_PREFIX}{ordinal % KEY_SPACE}".encode()


def key_slot(key: bytes | None) -> int | None:
    """Which of :data:`KEY_SPACE` slots ``key`` occupies, or ``None`` if it is not ours.

    ``None`` means a record this demo did not seed - a pre-existing topic named with ``--topic``
    holding somebody else's records. The demo counts those separately and says so rather than
    guessing at a slot, because a hashed slot would collide and quietly *under*-report the one
    figure the contract calls deterministic.
    """
    if key is None:
        return None
    try:
        text = key.decode()
    except UnicodeDecodeError:
        return None
    if not text.startswith(_KEY_PREFIX):
        return None
    try:
        ordinal = int(text[len(_KEY_PREFIX):])
    except ValueError:
        return None
    return ordinal if 0 <= ordinal < KEY_SPACE else None


def ensure_topic(
    bootstrap: str, topic: str, partitions: int, config: dict[str, str] | None = None,
) -> None:
    """Creates the demo's topic, tolerating one that a previous run already left behind.

    Reusing a topic silently is fine; reusing one with a **different partition count** is not,
    because the effective-configuration block would print a ``--partitions`` value that never
    applied - and that block is the demo's whole reproducibility promise.

    ``config`` applies only when this call creates the topic; a reused topic keeps whatever it
    already had. A caller whose measurement depends on a config therefore has to verify it on
    the records themselves rather than trust it was applied - the streams demo does exactly that
    with the sink's timestamp type.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    # config is only ever passed when there is one: NewTopic type-checks config as a dict, so
    # config=None is a TypeError rather than a default.
    extra: dict[str, Any] = {"config": dict(config)} if config else {}
    wanted = NewTopic(topic, num_partitions=partitions, replication_factor=1, **extra)
    created = admin.create_topics([wanted])
    try:
        created[topic].result(timeout=_TOPIC_CREATION_TIMEOUT)
        log.info("Created topic %s with %d partitions", topic, partitions)
    except Exception as failure:
        # librdkafka reports this as a KafkaException wrapping TOPIC_ALREADY_EXISTS. Matching on
        # the error CODE rather than the message text, because the text is librdkafka's to change.
        if not _is_topic_exists(failure):
            raise RuntimeError(f"could not create the demo topic {topic}: {failure}") from failure
        existing = _partitions_of(admin, topic)
        if existing != partitions:
            raise RuntimeError(
                f"topic {topic} already exists with {existing} partitions, but this run asked "
                f"for {partitions} - pass --topic to name a fresh one, or --partitions {existing}"
            ) from failure
        log.info("Topic %s already exists with the requested %d partitions, reusing it",
                 topic, partitions)


def seed(bootstrap: str, topic: str, first: int, last: int) -> None:
    """Produces the backlog every arm then replays, for record ordinals ``[first, last)``.

    **Pre-produced rather than produced alongside the arms**, and that is what makes the workload
    closed-loop - which is in turn why no arm reports latency. A per-record timing here would be
    flattered by however far an arm had fallen behind, so throughput is the only honest number
    this shape can produce.

    A failed send is fatal. ``flush()`` does not raise for a send that failed, so without the
    delivery callback below the demo would report a full backlog, run every arm against a short
    one, and print numbers for a workload that never existed.
    """
    if last <= first:
        return

    failures: list[Any] = []

    def delivered(error: Any, _message: Any) -> None:
        if error is not None:
            failures.append(error)

    log.info("Producing records %d to %d...", first, last)
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 20,
        # librdkafka buffers in the producer's own memory; the default 100k message ceiling is
        # below the big replay's backlog, and hitting it raises BufferError per send rather than
        # blocking. Polling below keeps the queue draining, and this keeps the headroom honest.
        "queue.buffering.max.messages": 1_000_000,
    })
    for ordinal in range(first, last):
        producer.produce(
            topic,
            key=key_of(ordinal),
            value=f"record-{ordinal}".encode(),
            on_delivery=delivered,
        )
        # Serve delivery callbacks as we go: librdkafka queues them, and a callback backlog is
        # memory this process holds until the flush at the end.
        if ordinal % 10_000 == 0:
            producer.poll(0)
    producer.flush()
    if failures:
        raise RuntimeError(f"the demo could not seed its backlog: {failures[0]}")
    log.info("Produced %d records", last - first)


def consumer_properties(bootstrap: str, group_id: str) -> dict[str, str]:
    """The Kafka properties every arm's consumer needs to reach this broker.

    ``enable.auto.commit`` is set false explicitly. The sidecar forces it false itself
    (``KafkaClientFactory`` sets it "whatever the map says"), because Parallel Consumer owns offset
    commits and core refuses a consumer that commits underneath it - but stating it here keeps the
    two arms configured from one place, and means the AK core arm below is not quietly running
    with auto-commit while the sidecar arm is not.

    These are **Java** Kafka client property names, because the sidecar hands them to a
    ``KafkaConsumer``. That they are also librdkafka's spellings for the four settings used here is
    a convenience, not a rule: `confluent-kafka` and the Java client do not agree on every key.
    """
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }


def _is_topic_exists(failure: Exception) -> bool:
    args = getattr(failure, "args", ())
    code = getattr(args[0], "code", None) if args else None
    if code is None:
        return False
    from confluent_kafka import KafkaError
    return bool(code() == KafkaError.TOPIC_ALREADY_EXISTS)


def _partitions_of(admin: AdminClient, topic: str) -> int:
    metadata = admin.list_topics(topic=topic, timeout=_TOPIC_CREATION_TIMEOUT)
    described = metadata.topics.get(topic)
    if described is None:
        raise RuntimeError(f"could not describe the existing topic {topic}")
    return len(described.partitions)
