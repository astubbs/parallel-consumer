# Control-thread and worker-pool contract debts

<!-- inflight-type: task -->
<!-- inflight-impact: reliability -->


Four things astubbs#296 surfaced and did not fix. Kept because each is a contract that is currently
enforced by a reader's attention, and all four live in the same class.

Ranked by how likely the next person is to trip over them.

## 1. The control thread's interrupt flag is a shared channel with four meanings

`AbstractParallelEoSStreamProcessor` signals its control thread by **interrupting it**, and that one
JVM bit is the transport for at least four unrelated messages:

| Meaning | Where |
|---|---|
| "you have mail, wake up" | `notifySomethingToDo` -> `interruptControlThread`, from five call sites |
| "stop blocking on the mailbox" | `processWorkCompleteMailBox`'s poll, and the 1 ms spin-avoidance sleep, both catch `InterruptedException` and treat it as *woke up* |
| "shut down" | `supervisorLoop`'s `catch (InterruptedException)` -> `doClose` |
| "the next commit-lock acquisition will throw" | not a message anyone sends - a side effect of the other three |

**The tell is that the class has accumulated defensive clears rather than a fix.** Four sites now
handle the collision by hand:

- `Thread.interrupted(); //clear interrupted flag as during close need to acquire commit locks and
  interrupted flag will cause it to throw another interrupted exception.`
- `if (Thread.interrupted()) { //clear interrupted flag` in the supervisor's error path
- `log.warn("control thread interrupted - may lead to issues with transactional commit lock
  acquisition")` in `innerDoClose` - which does not clear it, and does not know whether the interrupt
  meant *wake up* or *shut down*. The code is admitting it cannot tell.
- one added by astubbs#296 itself, after `transitionToClosing` interrupted the very thread that was
  about to run `doClose`

A signalling channel that every consumer must manually clear is not a channel; it is a shared mutable
global with no owner. Each clear is correct in isolation and none of them can be checked, so the next
person to send a wakeup near a close reintroduces the same defect - as astubbs#296 did.

## The fix, designed but deliberately not built

Sketched with the author 2026-08-17 and parked with the decomposition (item 2). Recorded at this
depth because the design turns on details that are expensive to rediscover.

**Read [`core-actor-revival.md`](core-actor-revival.md) before building any of it.** The 2022
micro-actor family (`sweep-2023-actor-ipc`) already carries a framework - 537 lines, 4 files, one
16-line marker interface - and a ranked set of six directions for reviving it. What follows is a
concrete first slice, not a greenfield design, and survivor 5 (skeleton-first strangler) is the shape
it should land in.

**The rule: an interrupt carries no meaning. Messages carry all of it.**

An interrupt is a wakeup primitive, not an instruction - it has no payload, no sender and no reason,
so anything inferred from it is a guess. Today "shut down" is inferred from it, which is the same
category error as the wakeup, only less obvious. Under the rule, a thread that is interrupted learns
nothing from the interrupt itself: it wakes and reads its mailbox.

**1. The mailbox already exists, and one message bypasses it.** `ControllerEventMessage` is an actor
message in all but name - its own javadoc calls it *"an Either type class"* - and the control loop
already blocks on `workMailBox.poll(timeToBlockFor)`. Add a payload-free `NUDGE` variant and have
`notifySomethingToDo` enqueue it. The poll returns immediately, which is exactly what a wakeup should
do.

The confirming detail: `blockableControlThread`'s javadoc says it is *"used for waking up a blocking
poll against a collection sooner"*. **That field exists only because the wakeup has no message.**

**2. Shutdown becomes a message too.** Then nothing is inferred from the interrupt at all. This also
takes a bite out of item 2 below: today shutdown is conveyed by the *non-volatile* `state` field plus
an interrupt, and a message is both the signal and the memory barrier.

**3. Coalescing is the answer to flooding, and it is not a compromise.** Five call sites into an
unbounded queue would pile up nudges. Gate the send on an `AtomicBoolean`:

```
if (nudgePending.compareAndSet(false, true)) workMailBox.add(NUDGE);
```

N nudges mean exactly what one means - *there is something to look at* - so collapsing them is the
correct semantics, not a lossy optimisation. The same edge-trigger already used for the pool-gone
diagnosis.

**The ordering is the part that will be got wrong: clear the flag BEFORE draining, never after.**
Clearing after processing loses any nudge that arrived during it, and that loss is a silent stall -
the failure mode this whole area keeps producing.

**4. The interrupt cannot go away entirely, and that is fine.** The control thread blocks in five
places, and a queue element only reaches the first:

| Blocks in | Reachable by a message? |
|---|---|
| `workMailBox.poll(timeToBlockFor)` | yes - the main idle wait |
| `Thread.sleep(1)` spin avoidance | no, and irrelevant at 1 ms |
| `Thread.sleep(100)` waiting for a transaction to commit | no |
| `awaitTermination` during close | no - already excluded by `notifySomethingToDo`'s guard |
| commit-lock acquisition | no |

So the interrupt survives as the mechanism for unparking a thread the mailbox cannot reach - carrying
no meaning, saying only *wake up and read your mailbox*. **`currentlyPollingWorkCompleteMailBox`
already tracks which case applies**, so the send site can tell whether a message alone suffices.

Hand-clears of the flag may still be wanted as hygiene before operations that dislike it, but they
stop being disambiguation guesses, which is the actual defect.

## 2. This class is already queued for decomposition, and this work grew it

`docs/refactoring.md` carries **"Decompose the God class - `AbstractParallelEoSStreamProcessor`"**,
marked high risk, plus SpotBugs findings in the same class
(`AT_NONATOMIC_OPERATIONS_ON_SHARED_VARIABLE`, `AT_STALE_THREAD_WRITE_OF_PRIMITIVE` on
`lastWorkRequestWasFulfilled`), and warns *"Fixing piecemeal now may conflict"* with the thread-model
rework.

astubbs#296 added to it anyway - deliberately, because the alternative was shipping a silent
work-drop. Worth deciding explicitly whether further hardening here waits for the decomposition.

Directly relevant to item 1: the `state` field these paths read is **non-volatile**, written by one
thread and read by another.

## 3. `RejectedExecutionException` does not mean "pool shut down"

The JDK throws it for **two** conditions - the executor is shut down, **and** the pool is saturated
(bounded queue at capacity, max threads busy). The name says neither, so every reader re-derives the
distinction, and `submitWorkToPoolInner` has to call `isShutdown()` to tell them apart.

Raised: translate it at the boundary into a domain exception whose name states the condition. Not
designed. Note the library does not control the throw - this would be catch-inspect-rethrow at the
point of use, not a replacement.

## 4. Bounding the worker queue changes what a rejection means

`setupWorkerPool` uses an unbounded `LinkedBlockingQueue`, so with `AbortPolicy` a rejection can only
mean *shut down*. **Bound it and it also means *saturated***, and any code branching on a rejection
must then distinguish the two or silently drop live work under healthy load. astubbs#296's catch is
written for that, and `requireRejectionIsVisible` refuses a handler that would swallow it - but the
constraint itself is unenforced.

**Do not read "unbounded" as "never rejects".** `ThreadPoolExecutor#execute` rejects a task submitted
to a *shut down* pool before it ever offers it to the queue, so the handler is reachable on the default
pool. Measured: unbounded queue plus `DiscardPolicy`, shut down, and `submit` returns without throwing
and hands back a `Future` that never completes. An unbounded queue removes *saturation*, not
*rejection* - a distinction a review of astubbs#296 got wrong in the direction of weakening the guard,
now pinned by `anUnboundedQueueDoesNotMakeALosingHandlerHarmless`.

Recorded on astubbs#216 ("Metrics: expose the buffers that have no upper bound"), which astubbs#116
names as the mitigation for buffers that cannot be bounded. That queue is unbounded by *type* but
self-limited by the control loop's in-flight target, so it is not the JStream failure mode - its bound
is emergent from `numberRecordsOutForProcessing` bookkeeping rather than structural.

There is no general "bound our queues" initiative to attach to: astubbs#116 settled that the JStream
deque is unbounded by design, and observability is the answer instead.
