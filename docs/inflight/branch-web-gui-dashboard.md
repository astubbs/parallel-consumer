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

## Open at time of writing

- Two commits carry work belonging to their neighbours (`git add -A` ran while a parallel agent was
  writing): `9cd60ee0` swallowed the chaos framework, `64b5c446` swallowed the GraalVM/Selenium pom
  entries. Content is complete across the branch; the attribution is not. **Re-cut at merge prep.**
- Whether CI should hard-fail rather than skip the browser UI suite when Chrome is absent.
- `ShowcaseScenarioIT` is untagged and costs ~150s in the default integration lane.

Delete this file when the work lands.
