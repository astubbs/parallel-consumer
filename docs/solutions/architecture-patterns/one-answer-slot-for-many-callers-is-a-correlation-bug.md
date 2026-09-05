---
title: "One answer slot for many callers is a correlation bug - key the channel by identity before concurrency finds it"
date: 2026-08-25
category: architecture-patterns
module: parallel-consumer-proxy-streams
problem_type: architecture_pattern
component: request_response_protocol
severity: critical
applies_when:
  - A request/response protocol has some message pairs that carry a call id and others that do not
  - A client holds one result field and one event per message TYPE rather than one per outstanding CALL
  - Adding a second concurrent caller to an API that was only ever exercised sequentially
  - A sequential test passes and you are about to conclude the waiting logic is correct
  - Deciding whether an unmatched or late reply should be delivered to whoever is waiting, or dropped
tags:
  - correlation-id
  - request-response
  - concurrency
  - silent-wrong-answer
  - protocol-design
  - version-skew
  - grpc
  - kafka-streams
related_components:
  - StreamsSession
  - StreamsSessionService
---

## The shape

A client had four blocking call types over one bidirectional stream. Two of them - the builder
calls - carried a `call_id` and were settled through a per-call waiter keyed by it. The other two -
the point query and the topology describe - carried **no correlation at all**, and the client held
exactly one result field and one event for each.

Two host threads calling the query concurrently therefore received **each other's answers**:

```
asked for alpha, got: b'beta'
asked for beta,  got: b'beta'
```

No exception, no fault, no log line. The caller is handed a confident wrong value for a key it never
asked about.

## Why it survived so long

**Every existing test was sequential.** A test named for waiting on "its own answer" exercised two
queries one after another, which the client's `clear()`-then-`wait()` handles correctly. Sequential
coverage of a concurrent mechanism reads as coverage and is not.

It was found only incidentally, while running an unrelated re-entrancy experiment. That experiment
made the same slot collide by a different route - a query that timed out left its answer in flight,
and the next caller collected it - which exposed the underlying single-slot design rather than the
symptom that had been predicted.

## The fix, and the ordering trap that comes with it

Give the uncorrelated pairs a call id and key their waiters by it, copying the pattern the builder
calls already used. **Do not invent a second mechanism** - there was already one correct
implementation in the same file.

**The ordering matters and is not obvious.** The same session also had a re-entrancy defect: user
functions ran inline on the reader thread, so a host function calling back into the engine
deadlocked until timeout. It is tempting to fix that first, because it is the louder bug. Doing so
**makes this one worse**: moving user functions onto worker threads converts a self-deadlock into
*more concurrent callers contending for the one answer slot*, which is exactly the silent
mis-delivery. Correlate first, then unblock the reader.

## Deliver to nobody rather than to the wrong caller

An answer no waiter claims is **dropped with a warning**, not handed to whoever happens to be
waiting. That covers three cases that were previously indistinguishable: a caller that timed out and
gave up, a duplicate answer, and an engine too old to echo the call id at all. Delivering any of
them to an arbitrary waiter is the original bug wearing a different hat.

The version-skew case is worth calling out. After this change, an older engine that answers without
a call id causes every query to time out rather than mis-deliver - a loud failure replacing a silent
one, which is the correct trade. A stale local test stub reproduced exactly that and looked like a
regression until the warning was read.

## The general rule

This is the cache-key rule in protocol clothing: **key a channel by everything needed to distinguish
its users, at the point you design it - and when a second dimension turns out to be needed, re-key
the original rather than adding a parallel path.** A protocol where some message pairs correlate and
others do not is not a protocol with an exception; it is a protocol with a latent correlation bug
whose trigger is the first concurrent caller.

The tell that the trap is present: a result field and an event named after a message *type* rather
than after an outstanding *call*.

## Testing note - two different markers for two different states

The concurrent-query test was written asserting the CORRECT behaviour under
`pytest.mark.xfail(strict=True)`, not as a characterisation test asserting the broken behaviour. The
distinction is deliberate and worth copying: `strict` means the moment the fix lands the test becomes
an XPASS **failure** telling whoever fixed it to delete the marker. Pinning a defect as passing
behaviour would instead have made the eventual fix look like a regression.

Its neighbours - the re-entrancy hangs - were plain characterisation tests, because those pinned an
accepted limitation rather than a defect. Same file, two markers, and the file says why.
