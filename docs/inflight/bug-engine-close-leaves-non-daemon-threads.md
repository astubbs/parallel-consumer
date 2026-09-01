# `close()` reports "Close complete" and leaves non-daemon threads running

Found 2026-08-22 on `feats/polyglot-demos`. The symptom is the Java demo hanging forever on the
container path; the defect is in `parallel-consumer-core`, and a user's application would hang the
same way. **The mechanism below is established; why it is intermittent is not.**

## The defect

In a hung run, `AbstractParallelEoSStreamProcessor` logs, for **all four** of the demo's in-process
engines:

```
Signaling to close...        x4
Close complete.              x4
Control loop ending clean (state:CLOSED)...   x4
```

and yet the JVM will not exit, because four **non-daemon** threads are still alive with
`DestroyJavaVM` blocked behind them:

<!-- issue-refs: exempt-begin - the #NN below are JVM thread ids in a quoted jstack, not issues -->

```
"pc-broker-poll" #98  prio=5 ... elapsed=173.04s  waiting on condition
"pc-control"     #99  prio=5 ... elapsed=173.04s  waiting on condition
"pc-broker-poll" #118 prio=5 ... elapsed=171.99s  waiting on condition
"pc-control"     #119 prio=5 ... elapsed=171.99s  waiting on condition
"DestroyJavaVM"  #188 prio=5 ... elapsed=166.96s  waiting on condition
```

<!-- issue-refs: exempt-end -->

**So `close()` returning is not evidence that the engine's threads are gone**, and neither is
`Control loop ending clean`. Two of the four engines shut down completely; two logged the same
success and left a `pc-broker-poll` / `pc-control` pair behind. Their `elapsed` values place them
5-6s before `DestroyJavaVM` appeared, which is the **big** replay's `pc-core` and `java-direct` arms.

That a shutdown path can report success while leaving non-daemon threads is the finding. Anything
built on "close completed, therefore we are done" is built on nothing.

## Do not start from the demo

The demo's arms are correct: `pcCore` closes in a `finally`, `javaDirect` uses try-with-resources,
and the log confirms `close()` was called on every engine and returned. An earlier version of this
note said "two engine instances outlived their arms" - that was wrong, and it is the hypothesis a
reader will reach for first, so it is written down here as refuted.

One counting trap, because it cost time: the demo pumps each spawned **sidecar's** stdout into its
own log, so a naive `grep -c "Confluent Parallel Consumer initialise"` returns **10**, not 4. Six of
those are engines in *other processes*. Filter on `[main]`, or the arithmetic says six engines were
never closed and sends you after a leak that does not exist.

## What was measured

Same image, same compose broker, same dials (`--records 20 --concurrency 4 --partitions 2
--replay-factor 2`); a temporary `pc.demo.experiment.skipUds` flag removed only the demo's own arm
and left epoll **available**, so exactly one term moves.

| run | UDS arm | epoll | outcome |
|---|---|---|---|
| `compose up` (original) | present | available | **hung** - killed at >4min |
| `-Dio.netty.transport.noNative=true` | absent | disabled | exited, 19s |
| `skipUds=true` | absent | **available** | exited, 23s |
| `skipUds=false` (control) | present | available | **hung** - still running at 240s |
| native, macOS | absent (no epoll on macOS) | n/a | exited, 50s |

The third and fourth rows are the pair that matters: one term, outcome flips. The second row alone
could not convict the arm, because `noNative` also moves every other gRPC arm off epoll.

**Do not read this as "the UDS arm is the bug".** It is the term that makes the defect appear, and
the defect is that a completed `close()` can leave threads running. The likeliest reading is that
the extra arm changes timing or load enough to lose a race that is always present - which is exactly
the kind of cause that a 2-versus-2 run count cannot distinguish from a hard dependency. Establish
the rate before believing either.

## Open, in order

1. **Find what those two threads are waiting on.** The dump says `waiting on condition` with no
   stack captured in what was collected; a full `jstack` with stacks is one command and constrains
   this more than any further black-box runs.
2. **Get the reproduction rate**, at fixed dials, with and without the arm. Everything above is
   n=2 per configuration, and an intermittent race and a deterministic dependency look identical at
   that sample size.
3. **Name the arm in the engine's thread name.** Two of four engines leak and the dump cannot say
   which; this settles it in one run instead of by inference from timestamps.
4. **Then decide whether the demo works around it**, and only then. A workaround written before the
   library defect is understood will hide it.

## Blast radius, and what must not be done

`demo/run.sh --docker` ends in the same `compose up --abort-on-container-exit` that hung, and
**neither `bin/ci-demo-test.sh` nor `bin/ci-demo-conformance.sh` wraps a run in a timeout** - so a
hang is not a red check, it is a job that burns to the CI limit. Note also that the clean native run
was on macOS, where there is no UDS arm at all; **CI is Linux, where the native path carries it**, so
that result is not evidence CI is safe.

**Do not "fix" this with a harness timeout or a `System.exit`.** Both turn a process that does not
terminate into one that appears to, and the second would hide the same defect in the library the
demo exists to show off. A timeout belongs in the harness *as well*, so a future hang fails loudly
instead of burning a job - but it is not the fix.
