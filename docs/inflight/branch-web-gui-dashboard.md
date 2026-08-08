# Branch: `feats/web-gui` - embedded dashboard, Phase 1

Work on astubbs/parallel-consumer#215. Plan:
[`docs/plans/2026-08-07-002-feat-embedded-web-dashboard-plan.md`](../plans/2026-08-07-002-feat-embedded-web-dashboard-plan.md).

**Phase 1 only.** A new opt-in module, `parallel-consumer-dashboard`, samples PC's state on the control
loop, publishes an immutable snapshot, and serves it over HTTP and SSE to a plain-JS page whose one
graphic is the offset ribbon. `bin/dashboard-demo.sh` brings up a Testcontainers broker, a workload, PC
and the page in one command. Phases 2-5 are deferred and listed in the plan's Phased Delivery section.

## What it touches outside its own module

- `parallel-consumer-core/src/test-integration/.../chaostests/` - `ChaosConductor` is **generalised in
  place** into a scenario driver (KTD14), rather than a second harness being built beside it. W1 and W4
  become single-phase scenarios. **Nothing under `parallel-consumer-core/src/main` is modified**, which
  keeps this clear of astubbs#57's five owned files.
- Root `pom.xml` (`<modules>`) and both hard-coded directory lists in `.github/workflows/maven.yml`.
- `AGENTS.md` - the scenario-framework section, the seed-stability invariant, and the Truth
  subject-generator two-compile trap.

## Things a reader needs to know before touching it

- **Never change the seeded draw stream.** `PlanSourceSeedStabilityTest` holds golden plans captured
  from the *pre*-generalisation implementation. If it fails, revert - do not update the goldens. Every
  recorded chaos seed and the probe calibration depend on it, and nothing else goes red when it breaks.
- **The control loop is the constraint.** Sampling runs in a loop-end callback; the notify path is one
  CAS plus at most one non-blocking `runOnContext`. confluentinc/parallel-consumer#618 is the recorded
  incident for off-thread PC access.
- Offsets are strings on the wire and BigInt in JS; numbers are pixel geometry only.

## TODO before merge: re-cut two commits

Decided: re-cut rather than squash. The branch holds several genuinely separable workstreams (the
module, the chaos scenario driver, the demo, the review fixes) that someone will later want to bisect
to or revert independently, and releases after 0.6.0.0 generate their notes from the commit log.

Two commits carry work that is not theirs. **The content is complete and correct across the branch -
only the attribution is wrong**, so this is a history fix, not a code fix.

| Commit | Says it does | Also contains, wrongly |
|---|---|---|
| `9cd60ee0` | serialise the state document | the whole chaos-scenario framework, which belongs with `39922d72` |
| `64b5c446` | drop TLS from the MVP | the GraalVM + Selenium pom entries, which belong with `d23ca7a5` |

Cause in both cases: `git add -A` ran in this shared worktree while a parallel agent was still
writing to a different subtree. Staging explicit paths would have avoided it.

Method, per the merge-strategy rule in `AGENTS.md`:

1. `git fetch origin master` **first, every time** - a stale ref silently reverts whatever master
   gained meanwhile.
2. `git reset --mixed <merge-base>` - the **merge-base**, not `origin/master`. The tell that you used
   the wrong base is files appearing in the staged set that this branch never touched.
3. Restage into atomic commits. The test for atomic is whether the message needs an "and also".
4. Verify with `git diff <old-tip> HEAD` - it must be **empty**, proving history changed and content
   did not.
5. Rebase-merge, so each lands on master on its own.

## TODO later: end-user docs and promotional material in the README

Not started, and not in Phase 1. Plan unit **U11** already scopes the mechanics (how to wire it up,
the security posture, a runnable example); what is additionally wanted is **promotional** material -
telling a reader why this is worth their attention, not only how to switch it on.

**The trap: `README.adoc` is generated. Never hand-edit it.** Edit
[`src/docs/README_TEMPLATE.adoc`](../../src/docs/README_TEMPLATE.adoc) and regenerate, or the work is
silently overwritten on the next build.

Raw material already written and ready to draw on:

- The plan's **`## Promotional Potential`** section, which carries two claims with their evidence and,
  more importantly, their caveats: *running safely ahead of the commit point* (records finished past
  where a single-threaded consumer would have stopped, encoded into the commit metadata, so a restart
  does not replay them) and *exact time lag* (KIP-489 has sat Under Discussion since January 2020, so
  the whole external tooling market interpolates; PC holds the `ConsumerRecord` and can subtract).
  **Re-read the caveats before publishing either** - each has wording that is wrong on the facts if
  copied carelessly, and the time-lag one needs KIP-489's status re-checked at publication time.
- The screenshots and the self-recording demo (`bin/dashboard-demo.sh --record`, plan U13, not yet
  built) - a recording of the ribbon showing head-of-line blocking being avoided is the most direct
  answer to "why not just use share groups?".
- astubbs#208 (the parked documentation site) is where a landing page would live; this README work is
  the interim home and should not wait for it.

Related: `docs/inflight/parked-docs-site.md` records that the fork's biggest problem is people finding
upstream, reading "no longer maintained", and leaving - so promotional copy here is not vanity.

## Other open items

- `ShowcaseScenarioIT` is untagged and costs ~150s in the default integration lane. Tagging it out
  would mean nothing gates the demo, so it stays until there is a reason to move it.
- Offsets lose exactness above 2^53 at the sampler, because `Gauge.value()` is a `double`. The
  string-on-the-wire encoding preserves what is left but cannot restore what the gauge already
  rounded. Reading them from `DirectStateSource` instead is the follow-on that would close it.
- Core has no `removeLoopEndCallBack` to pair with `addLoopEndCallBack`, so `DashboardServer.close()`
  stops sampling with a volatile flag rather than actually deregistering. A remover in core is the
  durable fix.

## Settled here

- **The browser UI suite never skips - anywhere.** If Chrome cannot start, the tests fail, locally and
  in CI alike. There is no opt-out property. Two reasons: a skipped suite is indistinguishable from a
  passing one, and a tool the build needs is a tool everyone working on the project should have.
- **Nothing needs installing for that to be reasonable.** Selenium Manager (built in since 4.6, able
  to fetch Chrome for Testing itself since 4.11; this module is on 4.36) resolves the driver *and* the
  browser on first use and caches them under `~/.cache/selenium`. An earlier version of this harness
  pre-empted that with its own availability check and skipped - it was second-guessing the component
  whose entire job is to make the browser available.

Delete this file when the work lands.
