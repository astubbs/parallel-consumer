# BLOCKER: the executor-count formula is `identity`, and ten client authors inherit it

<!-- inflight-type: bug -->
<!-- inflight-impact: correctness -->

**What it costs you not to know:** a Python application configured with `maxConcurrency` 500 spawns
**500 worker processes**. Not threads - the Python client runs worker processes (`WorkerPool` /
`RecordProcessor`, fork and spawn contexts). The same number in a language with cheap threads is
unremarkable; in Python it is a machine falling over, and the user never asked for 500 processes.

## Where it is

`OptionsMapper.EXECUTOR_COUNT_FUNCTION` in `parallel-consumer-proxy`, currently
`IntUnaryOperator.identity()`. It travels once in `Configured.executor_count` and is never revised
(KTD38, R47). Its own javadoc names this as unresolved and points at the plan's Deferred / Open
Questions: *"KTD38's executor-count function is named but never defined"*.

## Why it is not simply fixed

The reviewer-recommended `min(maxConcurrency, client-supplied cap)` **needs a client-supplied cap to
exist in the schema first**, so it is not a one-line change to a function - it is a protocol
addition with its own unit. It also needs a deliberate yes rather than an agent's judgement, because
it is a decision every one of the ten client authors inherits and cannot locally override.

What must NOT happen is deriving it from anything the proxy observes the client do. That is the
credit ledger KTD38 exists to keep deleted, and four consecutive review rounds died on that seam.

## Why it is filed as a blocker rather than a note

It blocks nothing today - the Java seed runs, and Java threads at 500 are fine. It blocks the
**Python demo**, which is the first per-language demo to build (it is one of the two KTD40
divergences, so it stresses the contract hardest). A Python demo at the current default either
spawns 500 processes or quietly caps the number itself, and the second is a per-language divergence
invented in a demo rather than decided in the plan.

**Owner decision needed before the Python demo lands.**
