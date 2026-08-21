# The Java demo prints everything, then never exits

Found 2026-08-22 on `feats/polyglot-demos`, running the container path. **Not yet root-caused, and
not yet known to be new** - both are open questions below, and neither should be guessed at.

## What was observed

`docker compose up --build --abort-on-container-exit` at `--records 20 --concurrency 4
--partitions 2 --replay-factor 2`:

- both tables printed in full, all six arms, records/keys `20/20` then `40/40`, and the closing
  footnote;
- the container then stayed `Up` for **more than four minutes** past its final line;
- it only ended because `docker compose down` was run against it: the recorded exit is **143**
  (SIGTERM), not 0. **No clean exit has been observed on this path.**

A thread dump of the container's JVM (pid 1) at ~262s uptime shows `DestroyJavaVM` blocked and
**four non-daemon threads still alive**:

```
"pc-broker-poll" #99  prio=5 ... elapsed=252.31s  waiting on condition
"pc-control"     #100 prio=5 ... elapsed=252.31s  waiting on condition
"pc-broker-poll" #119 prio=5 ... elapsed=251.26s  waiting on condition
"pc-control"     #120 prio=5 ... elapsed=251.26s  waiting on condition
"DestroyJavaVM"  #189 prio=5 ... elapsed=246.01s  waiting on condition
```

None carries the `daemon` marker, which is why the JVM will not exit. They are
`parallel-consumer-core` threads, so **two engine instances outlived their arms**, and the two
`elapsed` figures are 1.05s apart.

**That 1.05s does NOT identify which replay they came from**, though an earlier version of this note
claimed it did. The arm timeline from the same run shows both replays have a `pc-core` ->
`java-direct` gap of almost exactly that size:

| replay | `pc-core` finished | `java-direct` finished | gap |
|---|---|---|---|
| small | `51:00.254` | `51:01.322` | 1.07s |
| big | `51:07.737` | `51:08.782` | 1.05s |

So the pair is `pc-core` + `java-direct` from *one* of the two replays, and the thread dump alone
cannot say which. Reconstructing it from `DestroyJavaVM`'s own elapsed time lands between the two
candidates and is not precise enough to separate them. Whichever it is, note the asymmetry the cause
has to explain: **four** in-process engines run and only **two** leak.

## Why the obvious explanation is wrong

`ReferenceDemo.pcCore` closes its engine in a `finally`, and `javaDirect` uses
try-with-resources. **So this is not a missing `close()`**, and anyone starting from that hypothesis
will waste the time this note exists to save. Whatever is happening, `close()` is being called and
the threads are surviving it - or one of the four engine instances is not the one being closed.

A cause that applied uniformly to every engine would leave four pairs or none, so whatever
distinguishes the surviving two is the shape of the bug.

## Blast radius

**This is on the supported entry point, and nothing bounds it.** `demo/run.sh --docker` ends in

```
exec docker compose ... up --build --abort-on-container-exit --exit-code-from demo
```

which is the same command that hung. `bin/ci-demo-test.sh` drives the container path through that
script, and **neither `bin/ci-demo-test.sh` nor `bin/ci-demo-conformance.sh` wraps any run in a
timeout** - so a hang there is not a failing check, it is a job that runs until the CI limit and
reports whatever a killed job reports. That is the same class of problem as a silent skip: the
signal that something is wrong does not arrive as "wrong".

## The native path does NOT hang - measured, same host, same dials

```
./run.sh --records 20 --concurrency 4 --partitions 2 --replay-factor 2
NATIVE_EXIT=0  elapsed=50s
```

It ran `pc-core` and `java-direct` in **both** replays - the very arms whose threads were left behind
in the container - and the JVM exited on its own. So **"the in-process engine arms leak" is refuted
as a standalone claim.** Something about the containerised run is required to produce it, and any
fix aimed only at those two arms is aimed at the wrong thing.

This narrows it but does not convict anything, because the container moves **two** terms at once:

| | native (macOS) | container |
|---|---|---|
| `java-grpc-uds` arm | absent - no epoll domain socket on macOS | **present** |
| broker | Testcontainers, same host | compose sibling, another container |

## Open questions, in the order worth answering

1. **Move one term.** The cheapest single-term experiment available is a **native run on Linux**,
   where the UDS arm *is* present and the broker is still Testcontainers. That yields a real
   prediction to state before running it: **if the UDS arm is the cause, a Linux native run hangs
   too.** If it exits cleanly, the broker arrangement is implicated instead and the UDS arm is
   cleared. Either way one term has moved, which is more than a second container run can offer.
2. **Note what this implies for CI, which runs on Linux.** If the UDS arm is the cause, then CI's
   *native* half hangs as well as its container half, because on Linux the native path carries the
   UDS arm. The macOS-only clean run above is therefore not evidence that CI is fine.
3. **Is it new?** The polish wave's language agents reported exit 0 for their own demos, but only
   Java has in-process engine arms, so their runs say nothing about this. Check whether a container
   run from before the polish merge exits.
4. **Which two of the four engine instances leak?** The dump cannot say - see the table above.
   Naming an engine's arm in its thread name would settle it in one run and is cheaper than any
   further inference from timestamps.

## What must not be done

**Do not "fix" this by adding a timeout to the harness, or by calling `System.exit`.** Both convert
a demo that does not terminate into a demo that appears to, and the second would also mask the same
defect in the library the demo exists to show off - a user's application would hang the same way.
The timeout belongs in the harness *as well*, so a future hang fails loudly instead of burning a CI
job, but it is not the fix.
