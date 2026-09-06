# Logging: the next three iterations

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

The siloed streams landed on 2026-08-28 as a first cut ([`docs/logging.md`](../logging.md) owns the
why). Three things it deliberately did not do, each of which has a specific trigger rather than a
date.

## 1. A ring buffer for the verbose stream - needed before the first long soak

Three days of DEBUG is unusable and enormous; the chaos job log already reaches 126MB in minutes.
Keep the verbose stream in a bounded in-memory buffer and write it out only when something fires, so
the context around a failure survives and the other 99.9% does not.

`ch.qos.logback.core.read.CyclicBufferAppender` holds the window; the flush is triggered from the
detector that fires. **`errors.log` and `warnings.log` stay outside this**, permanently - a ring
buffer is the right answer to volume and the wrong answer to a warning from two days ago, which is
the line wanted when something fails on day three.

**Trigger:** before the first soak that runs longer than a few hours. Until then the master stream is
small enough to keep whole, and an unnecessary buffer is a way to lose evidence.

## 2. Error classification, so an unforced error is a finding

An error the system EXPECTED - a retriable failure, an induced fault, a chaos action landing - is the
harness working. The signal is the error nobody forced. While both share a stream, neither is
actionable: a reader cannot tell which is which, and a real defect sits invisibly beside routine
churn.

`PCRetriableException.isPresentIn` already draws this line on the processing path. **The close and
revoke paths never ask** - they `catch (Exception)`, warn, and swallow, so a commit failing because
partitions were revoked mid-flight is indistinguishable from a commit failing for a reason that
matters. That is recorded separately as its own defect; this note wants the same split reflected in
the log streams, so `errors.log` becomes a list of things to explain rather than a list of things
that happened.

**The goal state: in a long run, an unforced error is a bug until shown otherwise.** That is only
possible once forced ones are labelled.

**Trigger:** alongside the soak harness, since that is the run where an unreadable error log costs
days rather than minutes.

## 3. An autoscaling decision stream - gated on astubbs/parallel-consumer#333

Adaptive concurrency is a controller, and a controller is judged on a trajectory: what the admission
target was, what moved it, what the in-flight count and downstream latency were at the time. That is
a time series, and prose logging is the wrong shape for it entirely.

Give it a dedicated stream emitting structured records rather than sentences, so "did the target
track the load" is answerable by plotting rather than by reading. This is also the point where
scraping micrometer to a file starts paying, per the soak-harness note.

**Trigger:** when astubbs/parallel-consumer#333 lands. Building it before there is a controller to
observe would be guessing at the fields.
