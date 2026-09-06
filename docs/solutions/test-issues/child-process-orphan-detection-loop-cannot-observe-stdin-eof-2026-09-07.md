---
title: A child process's orphan-detection loop polled stdin.ready() and available() for EOF, which neither API can ever report on a closed pipe
date: 2026-09-07
category: test-issues
module: parallel-consumer-core
problem_type: test_failure
component: testing
symptoms:
  - ChildPcMain.awaitStopSignal polled stdin.ready() every 50ms and, on false, checked System.in.available() < 0 as its EOF test - available() never returns negative and ready() is false on a closed pipe with nothing buffered, so neither call ever signals EOF
  - A child whose parent died with runSeconds = 0 ran forever - exactly the orphan the class javadoc claimed the EOF route prevented, and the claim had never been exercised by a test
  - "A sibling defect in the same close path: a child whose processor close() threw swallowed the exception, still emitted its ledger, and exited 0 - so the parent's stops-cleanly check accepted an unhealthy child as healthy"
  - The harness self-test (ChildPcProcessHarnessIT) had no scenario that closed a child's stdin without sending a stop line, so the untestable EOF path went unexercised until the fix added one
  - Found by code review (the correctness reviewer and an independent cross-model pass both cited the available() < 0 line), not by a failing test - the old loop produced no red build on its own
root_cause: wrong_api
resolution_type: test_fix
severity: high
related_components:
  - ChildPcMain
  - ChildPcProcess
  - ChildPcProcessHarnessIT
tags:
  - stdin-eof
  - child-process
  - orphan-detection
  - test-harness
  - polling
  - wrong-api
  - red-proof
  - multi-process-integration-test
related:
  - "../workflow-issues/a-check-that-reports-success-without-having-run.md - the class - a check that reports success without having verified the thing it exists to verify; this is an instance in a test harness"
  - "../workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md - the discipline the new scenario applies - produce the EOF and watch the child stop"
  - "dormant-regression-test-uncollected-by-surefire-2026-08-07.md - the same genus - a guard that looked present and never ran"
---
# A child process's orphan-detection loop polled stdin.ready() and available() for EOF, which neither API can ever report on a closed pipe

> Extracted from astubbs/parallel-consumer#456

## Problem

The multi-process integration harness for the navigator's partition-share allocator launches child
JVMs, each running one `ParallelEoSStreamProcessor` on its own consumer. A child is meant to stop on
one of three signals: a `stop` line on stdin, stdin EOF (the parent process is gone), or a
`runSeconds` budget. `ChildPcMain`'s own class javadoc states the EOF route as an explicit promise -
"EOF on stdin (the parent died) stops the child too, so an orphan never outlives its test" - in
`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/utils/ChildPcMain.java`.

That promise was never tested, and it was false. The method that was supposed to detect the EOF,
`awaitStopSignal`, polled two APIs that cannot report EOF on a pipe. A child launched with
`runSeconds = 0` (the "run until told to stop" mode) whose parent died would simply run forever - the
exact orphan the javadoc claims never happens.

## Symptoms

- A child process launched with no run-seconds budget outlives its parent indefinitely once the
  parent's end of the pipe closes, because nothing in the polling loop ever observes the close.
- The failure mode is invisible in ordinary use: a harness test that also sends an explicit `stop`
  line, or that sets a `runSeconds` budget, never exercises the EOF branch at all, so the suite stays
  green while the promised orphan protection is dead code.
- The defect was not found by running anything red. It was found by code review - the correctness
  reviewer and an independent cross-model (Codex) pass both flagged the same line,
  `System.in.available() < 0`, as a test that can never be true.

## What Didn't Work

The pre-fix loop - from the commit before the fix, in the same file and method,
`ChildPcMain.awaitStopSignal` - read:

```java
while (Instant.now().isBefore(deadline)) {
    try {
        if (stdin.ready()) {
            String line = stdin.readLine();
            if (line == null || STOP_COMMAND.equals(line.trim())) {
                return;
            }
            System.err.println("ignoring unknown stdin command '" + line + "'");
        } else if (System.in.available() < 0) {
            return;
        }
    } catch (IOException e) {
        return; // stdin closed under us - the parent is gone
    }
    TimeUnit.MILLISECONDS.sleep(50);
}
```

Every 50 milliseconds it tried three different tests for "the parent is gone," and every one of them
was structurally unable to fire on a closed pipe with nothing buffered:

- **`BufferedReader.ready()` returning false.** The JDK's contract for `ready()` is that it reports
  whether the stream is guaranteed not to block on the *next* read - true only when input is already
  buffered or the underlying stream is known to have data available. A closed pipe with no buffered
  bytes reports `false`, exactly indistinguishable from "no data yet, but the parent is still alive
  and might send some later." The loop's `else` branch is the only place that ever gets a chance to
  detect the close, and `ready() == false` is also what an ordinary idle, still-alive parent produces
  every single tick.
- **`System.in.available() < 0`.** `InputStream.available()`'s contract only ever promises a
  non-negative estimate of bytes that can be read without blocking; on EOF that estimate is `0`, the
  same value it returns for "nothing sent yet." Nothing in the `InputStream` API returns a negative
  number for this or any other condition, so the loop's own EOF test could never be satisfied - it
  compares an unsigned quantity against a bound it can never cross.
- **The `catch (IOException e)` fallback.** The loop only reaches a read (`stdin.readLine()`) once
  `ready()` has already reported `true`, so on a pipe that just went to EOF this branch is unreachable
  by construction - there is no read happening whose failure could be caught. The exception handler
  is real, but it protects against nothing that this call sequence can produce.

The result: `ready()` is permanently false on a closed pipe, `available()` never returns a negative
number to trip the second test, and no exception is ever thrown because nothing ever attempts the
read that would notice. All three tests were dead code against the exact condition they were written
to detect, and the loop would spin at its 50ms cadence until `runSeconds` expired - or, with
`runSeconds = 0`, forever.

## Solution

The fix replaces polling with a blocking read on a dedicated daemon thread, gated by a latch that the
main thread awaits (still respecting the `runSeconds` timeout when one is configured):

```java
private static void awaitStopSignal(ChildPcOptions options) throws InterruptedException {
    CountDownLatch stop = new CountDownLatch(1);
    Thread reader = daemon("child-pc-stdin", () -> {
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        try {
            String line;
            while ((line = stdin.readLine()) != null) {
                if (STOP_COMMAND.equals(line.trim())) {
                    break;
                }
                System.err.println("ignoring unknown stdin command '" + line + "'");
            }
        } catch (IOException e) {
            // stdin closed under us - the parent is gone; fall through to the stop
        }
        stop.countDown();
    });
    reader.start();
    if (options.getRunSeconds() > 0) {
        boolean ignoredSignalled = stop.await(options.getRunSeconds(), TimeUnit.SECONDS); // either way, stop
    } else {
        stop.await();
    }
}
```

`readLine()` blocks until either a line arrives, the stream hits EOF (returning `null`), or the
underlying pipe throws. All three exits - the `stop` line, `null` from EOF, and the `IOException` -
now converge on the same `stop.countDown()`, so the EOF path is exercised by the same code as the
graceful-stop path rather than living in a branch nothing can reach. The method's own javadoc in
`ChildPcMain.java` states the reasoning directly: "a polling loop over them never learns the parent
died and an orphan runs forever - the harness's own self-test closes stdin without a stop line to
prove this path."

The harness self-test gained a scenario for exactly that path,
`ChildPcProcessHarnessIT.childStopsGracefullyAndEmitsItsLedgerWhenStdinClosesWithoutAStopLine`, in
`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/ChildPcProcessHarnessIT.java`.
It launches a child, waits for it to be ready and the consumer group stable, then calls
`ChildPcProcess.closeStdin()` - closing the child's end of the pipe without ever sending a `stop`
line - and asserts on `ChildPcProcess.awaitExit(Duration)` that the child exits **on its own**, within
budget, with exit code 0, and that its ledger record still arrives. Both launcher methods are new:
`closeStdin()` in `ChildPcProcess.java` closes the process's `OutputStream` (the child's stdin) to
simulate "what a dead parent looks like from inside the child"; `awaitExit(Duration)` waits for the
process to exit on its own, returning the exit code or empty if it is still alive within the budget.
Against the pre-fix polling loop this scenario would time out, because nothing in that loop can
notice the close; that arm was reasoned from the API contracts above rather than run. Against the
fix it passes because the blocking read does. The self-test class carries nine scenarios total (`ChildPcProcessHarnessIT.java`),
all of which pass with the fix in place.

A second, related defect landed in the same commit, in the same `run()` method. A child whose
processor `close()` threw during shutdown used to swallow the exception, still emit its ledger, and
exit 0 - so the parent's "stops cleanly" assertion would accept a child that had actually failed to
close. The fix keeps emitting the ledger (it is the diagnostic evidence) but now tracks the failure in
an `AtomicReference<RuntimeException> closeFailure` set inside the shutdown `Runnable` in
`ChildPcMain.run`, and re-throws it after `shutdown.run()` returns, so `main()`'s outer `catch
(Throwable t)` sends the process out with a non-zero exit code instead of 0. This is a second, sibling
instance of the same defect class: a harness path that could report "healthy" while the thing it was
checking was not.

## Why This Works

EOF on a pipe is a **read result**, not an observable state. Nothing about a closed `InputStream` sets
a flag that `ready()` or `available()` can inspect in advance - the only way the JVM's I/O layer
surfaces "there will never be more data" is by returning it as the outcome of an actual blocking read
attempt (`null` from `readLine()`, or an `IOException`). A polling loop that only ever asks "is there
data ready right now, without blocking?" gets the same non-committal answer whether the writer is
alive-but-idle or gone-forever, because both cases legitimately produce "no bytes available yet." The
fix works because it stops asking a question the API cannot answer and instead performs the one
action - a blocking read - that the API is actually able to report EOF through.

## Prevention

- **A harness promise about process lifecycle needs a scenario that exercises exactly that path.**
  `ChildPcMain`'s javadoc had stated the EOF-stops-the-child guarantee since it was written, and
  nothing in the test suite ever closed a child's stdin without also sending a stop line or a
  `runSeconds` budget - so the untested branch of the promise sat there, plausible-looking and dead,
  until review caught the specific API misuse. Any comment or javadoc that describes what happens on
  a process's abnormal exit, disconnection, or orphaning is a claim that needs its own test closing
  exactly that door, not just the graceful one.
- **An exit-code check is only as good as what the child does on failure.** The sibling fix in the
  same commit is the same lesson from the other direction: the parent's "stops cleanly" assertion was
  only ever as trustworthy as the child's own exit code, and the child was silently turning a failure
  into a 0. Checking a child's exit code is only a real assertion once the child itself is honest
  about failure.
- **The repo already knew this, on a branch that has not merged (session history).** The sidecar
  work in astubbs/parallel-consumer#328 carries `ParentDeathWatchdog` in the proxy module's
  lifecycle package (on that branch only, not on this tree), which detects
  parent death by a blocking read on an inherited pipe - the kernel closes the last write end when
  the parent dies, even by SIGKILL, and the read returns end-of-stream - and its javadoc records
  the one hole in that signal (a wrapper process holding the write end open after the real parent
  is gone) with a parent-pid poll as the backstop. The harness re-derived the same rule because
  that class lives on an unmerged branch and in a module the core module's tests cannot depend on.
  When the sidecar lands, a harness child that wants a backstop against a wrapper holding its
  stdin open should copy that shape rather than this one; until then, this is the repo's
  worked instance that prior art lives on branches.
- **Read any other child-process spawn helper in the repo for the same poll.** Checked:
  `parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance/src/test/java/bz/stub/parallelconsumer/conformance/ConformanceDriver.java`,
  which spawns language-runner child processes and whose own comment says it "mirrors" `ChildPcProcess`'s
  spawn. Read in full: its `spawn` method never writes to, closes, or otherwise inspects the child's
  stdin at all - it drains the child's stdout and stderr with a `StreamPump` and waits for exit via
  `process.waitFor(budget, TimeUnit.SECONDS)` with a fixed runner budget plus `REAP_SLACK`, killing the
  process if that budget is exceeded. It does not rely on stdin EOF detection for lifecycle in any
  form, so it does not share this defect.

## Related Issues

- astubbs/parallel-consumer#456 - the PR both defects were found and fixed in, as part of the
  navigator partition-share rung's code review.
- astubbs/parallel-consumer#328 - the sidecar branch whose `ParentDeathWatchdog` is the repo's
  earlier, fuller treatment of the same signal.
- `docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md` - the class:
  a check that reports success without having verified the thing it exists to verify; this is an
  instance in a test harness rather than in build tooling.
- `docs/solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md` -
  the discipline the new scenario applies: before trusting that a child stops on EOF, produce the
  EOF and watch it stop.
- `docs/solutions/test-issues/dormant-regression-test-uncollected-by-surefire-2026-08-07.md` - the
  same genus, a guard that looked present and never ran.
