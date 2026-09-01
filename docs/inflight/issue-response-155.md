# Draft response to astubbs#155 - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the pre-release sweep posts it.
     It deliberately outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

Written while the context is here, per `docs/inflight/AGENTS.md` - the agents who did the work hold
the best context at merge time, and by release time it has to be re-mined from commit logs.

**Not posted, and it survives this PR.** It is deleted when it is posted and not before; the sweep in
[`docs/releasing.md`](../releasing.md), "Post the drafted issue responses before you freeze the
section", is what consumes it, so all of these go out together with one view of what shipped.

A backlink is also worth posting on confluentinc#402, where the original report lives.

---

Fixed in astubbs/parallel-consumer#201.

**To answer the original question first: `Max loading factor steps reached: 100/100` was never the
cause of your stall.** It says the in-flight target has scaled to its configured ceiling and will not
grow further - a saturation signal, not an error. The stall itself was stale work leaking
`WorkManager#numberRecordsOutForProcessing`, fixed in confluentinc#547 / confluentinc#606, and this
fork has since fixed three further stall causes in the confluentinc#857 family (astubbs#119).

**What changed is only the reporting; buffering behaviour is untouched.** The loading factor, the
queue target and the step-up rules are exactly as before.

- **A fixed factor no longer warns at all.** `DynamicLoadFactor#isStaticFactor()` is true when the
  factor starts at its own ceiling - which is what setting `messageBufferSize` does, and what
  configuring `initialLoadFactor == maximumLoadFactor` does. There is nothing to step to and nothing
  wrong, so it reports at debug.
- **A dynamic factor at its cap still warns**, because it tells you the in-flight target will not
  grow any further and you may want to act on it - but it is rate limited to once per 30s and
  reworded to read as saturation, naming `maximumLoadFactor` / `messageBufferSize` as the thing to
  raise. Deliberately not demoted: quietening a real signal to fix a volume problem would be the
  wrong trade.

**There was a second half nobody had reported.** `PCModule#initDynamicLoadFactor()` builds
`DynamicLoadFactor(n, n)` when `messageBufferSize` is set, so `isMaxReached()` held from
construction and the WARN fired from the very first control-loop pass. Following the README's own
PARTITION-ordering buffer-tuning advice therefore earned permanent log noise saying nothing was
wrong.

Regression coverage is `LoadFactorCeilingReportingTest`, which drives 500 real
`checkPipelinePressure()` passes and asserts on what reaches the log - 500 warnings before the fix in
each configuration, zero and one after.
