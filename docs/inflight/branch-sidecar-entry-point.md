# Branch: the sidecar entry point (U10), and the seam it leaves open

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->

Branch `feats/sidecar-entry-point`. **Language-proxy plan work, not demo work** - U10 in
[`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`](../plans/2026-08-14-001-feat-language-proxy-plan.md).
Isolated so it does not move ground under another agent; **folds back into the language-proxy branch
later, deliberately not now**. It currently rides in PR astubbs#328 because the demo needs it and PRs
are expensive.

## Landed

`ParentDeathWatchdog` (KTD19), `DrainCoordinator` (KTD17), `Main` - the module's first production
entry point - and `SidecarLifecycleIT`, its first test against a real broker. `SetExecutorCount` is
proven never sent, structurally, with the guard proven able to fail.

**Not added: `lifecycle/ExecutorCount.java`**, which U10's fileset lists. That work already landed in
`OptionsMapper` during U7 and is tested; creating the file would duplicate it, and `OptionsMapper` is
the right home per KTD38's own wording - a function on the configuration, not a policy object. A
reader diffing the fileset against the tree will find it missing and should not re-add it.

## The seam nobody owns: spawn-and-connect

`GrpcParallelConsumerClient`'s javadoc says it plainly - *"Spawning the sidecar process is the
lifecycle unit's job; this client connects to a [port]"*. U10 now supplies a spawnable `Main`. **The
two are not joined by anything.**

KTD41 says what the joined thing should be: the client package **vendors** the platform-matched
binary and **spawns and supervises it on first use**, so the user never installs, deploys or operates
a process - the invisible sidecar, with an explicit address as the escape hatch. That is assigned to
U11, U12, U22 and U32, **not to U10**, so this branch deliberately stops short of it.

What exists instead is `SidecarProcess` in this module's `src/test-integration` - the spawn contract
(launch directly, never through a shell; drain both child streams or the pipe fills and the child
blocks). It is test-scope in the *sidecar* module, so a **client** module cannot reach it. Anything
needing spawn-and-connect before KTD41's vendoring lands has three options, and the third is the
right one:

1. Hand-roll a spawn beside it - which is how the spawn contract silently drifts.
2. Widen `SidecarProcess` into production code here - which is quietly doing U11/U12's job inside a
   lifecycle unit.
3. Take the demo's spawn from a shared test fixture and leave productised spawning to its own unit.

## Open, and not this branch's to answer

The **executor-count formula** - see
[`blocker-executor-count-formula.md`](blocker-executor-count-formula.md).
