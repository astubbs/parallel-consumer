# SpotBugs findings the rename made visible

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: deferred - after v6, none diagnosed as a live defect -->


**30 findings, surfaced on astubbs#294. None are new, none block anything, and they will go invisible
again the moment that PR lands.** That last part is why they are written down here: this is a one-time
window, and closing it without an inventory wastes it.

## Why they appeared

`maven.yml` generates a SpotBugs baseline **on every push to master** and converts each `BugInstance`
into a `Match` element **keyed on class + method + bug type**. PR builds pass that as an exclude filter,
so only genuinely new bugs are reported.

The rename changes the fully-qualified name of every class. The baseline therefore matches **nothing**,
and every long-standing finding in the codebase reports as if it were introduced by this PR.

Verified rather than assumed: `RecordContext.java`, the first annotation, differs from master only in
its `package` line, two `import` lines and the added copyright line. The flagged constructor is
byte-identical. The content commit only rewrites package strings; it cannot introduce a bug.

The job is `neutral`, not failing. `static: spotbugs` passes.

## It will heal itself, which is the problem

The baseline job is `if: github.event_name == 'push'`. Once the rename is on master the baseline
regenerates with `bz.stub.*` class names, the exclude filter matches again, and all 30 disappear from
PR annotations. Nothing will have been fixed - they will simply be hidden again, as they were before.

## Three lanes broke the same way, and all three self-heal

Every PR-scoped or baseline-keyed tool degrades on a mass rename, because each keys on an identity the
rename changes. None of them indicates a defect in the change.

| Lane | What it keys on | Heals when |
|---|---|---|
| `dups: similarity` | file path | master carries the rename |
| SpotBugs baseline | class + method + bug type | baseline regenerates on push to master |
| Mutation (PIT, PR-scoped) | "classes changed in this PR" - here, all 234 | the next PR touches few files |

If you are reading this because a rename-shaped PR lit up your CI: that is what happened, and it is
expected. Do not spend a day building a deferral registry for it.

## What is actually worth acting on

**Three were checked in the source rather than judged by their labels.** The other 27 are grouped by
what that inspection implies, and are not individually verified.

### Worth triage - 14

**10 are concurrency findings in the control loop and poller of a concurrency library**, which is the
one place a stale-read finding deserves a look rather than a shrug.

`AT_NONATOMIC_OPERATIONS_ON_SHARED_VARIABLE` (8)

- `internal/AbstractParallelEoSStreamProcessor.java:421,449,464` - `numberOfAssignedPartitions`
- `internal/ConsumerManager.java:128` `correctPollWakeups`, `:160` and `:238` `noWakeups`,
  `:213` and `:245` `erroneousWakups`

`AT_STALE_THREAD_WRITE_OF_PRIMITIVE` (2)

- `internal/ConsumerManager.java:293` - **CHECKED, and real.** `commitRequested` is a plain
  non-volatile `boolean`, written by `onCommitRequested()` on one thread and read in the poll loop on
  the broker-poll thread, with no happens-before edge between them. The consequence is not corruption,
  it is **commit latency**: the poller can miss the flag and fail to shorten its long poll when a commit
  has been requested. Worth connecting to the commit-timeout flakes this project already tracks.
- `internal/AbstractParallelEoSStreamProcessor.java:980` - `lastWorkRequestWasFulfilled`, same shape

`JLM_JSR166_UTILCONCURRENT_MONITORENTER` (4)

- `internal/AbstractParallelEoSStreamProcessor.java:1310,1487,1521,1527` - `synchronized` on an
  `AtomicBoolean`. It works, since any object can serve as a monitor, but it signals a confusion
  between atomicity and mutual exclusion. A smell rather than a defect; cheap to look at.

### Real but cosmetic - 1

`UR_UNINIT_READ`, `internal/DynamicLoadFactor.java:71` - **CHECKED, and real.**
`private long lastSteppedFactor = currentFactor;` runs during field initialisation, *before* the
constructor assigns `currentFactor`, so it is always 0 and never the initial factor. Exactly as
described. But it feeds only the `delta` in a debug log, so the whole consequence is one wrong log line
on the first step-up.

### Dismissible, with the reason - 15

`EI_EXPOSE_REP` (3) and `EI_EXPOSE_REP2` (7) - `AbstractParallelEoSStreamProcessor.java:231,581`,
`PCModule.java:34,38,124`, `BrokerPollSystem.java:84,86`, `ProducerManager.java:79`,
`ProducerWrapper.java:70`, `RecordContext.java:42`.
These flag storing or returning a mutable object that was passed in. That is precisely how this library
composes its collaborators - `PCModule` holds the options, `BrokerPollSystem` holds the
`ConsumerManager` - and defensive copying would break it rather than fix anything. Noise in this
context, and the largest single group.

`SF_SWITCH_NO_DEFAULT` (3) - `AbstractParallelEoSStreamProcessor.java:901`,
`BrokerPollSystem.java:95,153`. Style; exhaustive switches over an enum.

`DCN_NULLPOINTER_EXCEPTION` (1) - `AbstractParallelEoSStreamProcessor.java:480`. Catching NPE
deliberately.

`SS_SHOULD_BE_STATIC` (1) - `DynamicLoadFactor.java:57`. **CHECKED, and a false alarm as worded.** The
annotation says "Unread field: `stepUpFactorBy`" and the field is plainly read on line 96. That phrasing
is SpotBugs' canned text for "instance `final` initialised to a compile-time constant, consider making
it `static`". A style nit wearing an alarming message - worth knowing, because it is the finding most
likely to send someone hunting for dead configuration that is not dead.

## If you want to act on this

The baseline hides these by design, so acting on them has to be deliberate. The concurrency group is
the only part with a plausible payoff, and `ConsumerManager.commitRequested` is the concrete place to
start. Everything else here is either noise or cosmetic, and the value of this file is mostly that it
says so with reasons, so nobody re-derives it the next time a wide change makes the baseline miss.
