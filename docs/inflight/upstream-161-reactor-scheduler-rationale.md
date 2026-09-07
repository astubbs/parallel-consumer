# confluentinc#543 - the reactor Scheduler is answerable, but not from the mirror body

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

astubbs#161 (mirror of confluentinc#543) asks why `ReactorProcessor` subscribes on a `Scheduler` when
the user's pipeline is already non-blocking. There is no defect and no code work: the question has an
answer, and the opt-out the reporter wants already exists in the API. What is recorded here is why
the issue's own `## Fork status` section cannot be posted as that answer, and what the answer
collides with.

## The mirror body names the wrong thread

`## Fork status` says the remaining `subscribeOn` moves the user function "off the PC control
thread". It does not. `ExternalEngine` overrides `setupWorkerPool` to force a pool of one, so the
user function was never on the control thread - it runs on this module's **single dispatch thread**.
Upstream's own bug title for the defect that produced today's code says the same: confluentinc#793,
"executes a user-provided function in a thread from the pc-pool rather than in the provided
scheduler".

That correction changes the answer rather than polishing it. With a dispatch pool of one, a function
that blocks before returning its publisher serialises the dispatch of *all* in-flight work - a
stronger reason to keep the hop than the mirror gives, and the reason `Schedulers::immediate` is a
safe opt-out only when constructing the publisher is instantaneous.

The rest of the section was checked against HEAD and holds: `publishOn` is gone (`ReactorProcessor`
retains only `subscribeOn(getScheduler())`, removed by confluentinc#798 fixing confluentinc#793), the
supplier is injectable via `defaultSchedulerSupplier`, and "scheduler" appears nowhere in
`src/docs/README_TEMPLATE.adoc` near the `[[project-reactor]]` anchor.

## The prescribed fix is already written, on astubbs#303

The mirror prescribes "a short README subsection under Project Reactor". Open PR astubbs#303 adds a
per-module reactor README already stating that subscription runs on a supplied `Scheduler` defaulting
to `Schedulers::boundedElastic`, and that the module is pinned to a single dispatch thread that must
never block. Writing the prescribed root-README subsection would state those facts a second time, in
a file generated from a template. The one thing astubbs#303 omits is what confluentinc#543 actually
asked - that `Schedulers::immediate` removes the hop. That sentence belongs in astubbs#303's file
while it is open, not in the root README.
<!-- file-refs: N/A - the per-module reactor README arrives with astubbs#303 and does not exist on this branch, which is the collision this paragraph records -->

## Two code findings nothing else records

- **The supplier is resolved per wrapped user-function invocation, not once.** `getScheduler()`
  calls `schedulerSupplier.get()` inside the wrapper, once per invocation - which is once per
  `PollContext`, so once per batch rather than once per record. A caller passing a factory rather
  than an accessor gets a new `Scheduler`, and its threads, on every one - and nothing disposes them.
  Safe by default only because `Schedulers::boundedElastic` returns a shared instance, and nothing
  documents the requirement. `MutinyProcessor` already does this correctly, resolving
  `newExecutorSupplier` once in its constructor. This is a defect in the very escape hatch the answer
  below points users at; the fix is small and preserves default behaviour.
- **The two-argument constructor has no test.** `ReactorUnitTestBase` builds
  `new ReactorProcessor<>(build)` and no test in the tree passes a scheduler.

## The answer to post, then close

astubbs#161 is not on the 0.6.0.0 blocker register in
[`release-0600-blockers.md`](release-0600-blockers.md); its `0.6.0.0` label groups it with the other
question mirrors to answer around the release. Post this and close, keeping the existing labels:

> Good question, and the answer changed after you filed this.
>
> The exact line you linked - `publishOn(getScheduler())` - is gone. It was removed in
> confluentinc/parallel-consumer#798 (fixing confluentinc/parallel-consumer#793), first released in
> 0.5.3.0, so there is now one
> scheduler boundary rather than two.
>
> The remaining `.subscribeOn(getScheduler())` is deliberate, but not for the reason you might
> expect. `ExternalEngine` overrides `setupWorkerPool` to pin the reactor module to a **single
> dispatch thread** - that thread exists only to invoke your function, take the publisher it
> returns, subscribe, and go back for more work. So a function that blocks *before* returning its
> publisher does not just slow itself down; it serialises the dispatch of every other in-flight
> record. Parallel Consumer cannot verify that a given function is non-blocking, so it cannot drop
> that protection by default.
>
> But you are right that you should not have to pay for it, and you do not have to. The scheduler is
> injectable:
>
> ```java
> var pc = new ReactorProcessor<>(options, Schedulers::immediate);
> ```
>
> With a BlockHound-clean pipeline, `Schedulers.immediate()` subscribes on the dispatch thread itself
> and the extra hop disappears. The caveat is the one above: constructing the publisher must be
> genuinely instantaneous, because anything spent there is spent on the single dispatch thread.
>
> One thing to watch if you pass your own - the supplier is invoked on every wrapped invocation, so pass an
> accessor to a shared scheduler (`Schedulers::immediate`, `Schedulers::boundedElastic`, or
> `() -> myScheduler`), never a factory such as `() -> Schedulers.newParallel(...)`, which would
> create a scheduler and its threads for every batch.
>
> The real gap here was that none of this was discoverable - "scheduler" appears nowhere in the
> README. That is being fixed in astubbs/parallel-consumer#303, which gives the reactor module its
> own README covering the scheduler, its default, and the single-dispatch-thread constraint. Closing
> this as answered; the documentation work is tracked there.
