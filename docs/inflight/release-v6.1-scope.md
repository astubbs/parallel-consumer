# 6.1 (0.6.1.0) release scope

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->

**The source of truth for what 0.6.1.0 is for.** 0.6.0.0 was a bug release, overdue, with the bar set at "the fixes already built are merged"; its scope note on master, `release-v6-scope`, owns that story and the burn-down. 6.1 is the release after it, and the owner opened it on 2026-09-11 as the home for the first big feature work since the fork: things kept out of v6 so that v6 could ship. This note is the list, and the decisions as they are taken. It follows the v6 note's shape so an agent that knows one knows the other; the mechanics stay in [`docs/releasing.md`](../releasing.md).

## The theme: performance and UX

Set by the owner on 2026-09-12. 0.6.0.0 was a bug release; 6.1 is where the first feature work since the fork lands, and it is themed rather than a grab bag. Every candidate below is a performance or a user-experience claim, and a candidate that is neither needs the theme changed before it earns a row.

## What 6.1 is

- A feature release on the 0.6 line, not a bug release and not a major. The roadmap's horizon ladder ([`docs/data/roadmap.yaml`](../data/roadmap.yaml)) has no rung for a minor, so 6.1 candidates sit at `next-0x` there and name 6.1 in their `caveat`; this note is where the 6.1 targeting lives.
- The bar is not set yet. The v6 bar was "enough once the built fixes merge"; 6.1 needs its own, and the owner has not stated one. Recorded as open below.

## Candidates, and where each is owned

| Candidate | Owned by | State on 2026-09-12 |
|---|---|---|
| The MCP (Model Context Protocol) interface embedded in the runtime: a read-only hello world on one instance, the first of four surfaces over one API | [`docs/plans/2026-09-11-002-feat-hasten-mcp-interface-plan.md`](../plans/2026-09-11-002-feat-hasten-mcp-interface-plan.md), which astubbs#508 landed | Requirements drafted and reviewed; nothing built; the build depends on the embedded dashboard (astubbs#268) landing or being built on <!-- post-merge: checked --> |
| **OpenTelemetry** across the client libraries | `parked-opentelemetry-across-the-clients` | The owner's idea from 2026-08-15, parked explicitly *post-v6*. Targeting it at 6.1 unparks it; it has no plan, and the note records a gap it is blocked on |
| **Fluent UX MVP** - the modern way to define a consumer | The UX-modernisation plan, `2026-09-09-002-feat-ux-modernisation-plan`, on branch `docs/ux-modernisation` | Milestone A, astubbs#502, open and draft; stacked on astubbs#506 with astubbs#507 above it |
| **Web GUI MVP** - the embedded dashboard | The dashboard plan on `feats/web-gui` | astubbs#268, open and draft. The MCP row below depends on it |
| **CLI MVP** | No owning document yet. The nearest thing is KD13 of the MCP plan, which names the CLI a later projection of the same substrate | A prior-art pass across 602 refs found nothing: no note, no plan, no commit, no PR. It needs an owning document before it earns a row under this note's own rules |
| **Dead-lettering** | The feature record on branch `feat/504-dead-letter` | Carries the surface migrated out of astubbs#502 plus the reaction verb it lacked. Cannot work until the engine can produce from the parked set, which is Milestone C of the UX plan |
| **Producer creation stack** | astubbs#225's PRs: astubbs#472 (recovery groundwork), astubbs#420 (PC-built producer), astubbs#410 (fencing) | All three open and draft. **Reading to confirm with the owner:** his words were "existing prs - so we can build on top of the router creation", which reads as wanting these in 6.1 because the fluent API builds its own clients from properties, so route creation needs PC-built producers underneath it. If that reading is wrong the row's justification changes, not its presence |
| The v6 announcement theme and its figures | The v6 announcement note on master, `release-v6-announcement` | Qualified on 2026-09-09 as 6.1 material, because its figures come from an experimental branch |

Anything else that claims 6.1 gets a row here in the PR that claims it, with the note or plan that owns it. A row is a candidate, not a promise: the roadmap's `stage` says how real each one is.

Three of the rows above have no owning document, or an owner that parks the work: OpenTelemetry, the CLI, and dead-lettering's dependency on a milestone that has not landed. Under this note's own rule each needs one before its row means anything, and naming the gap is what the row is for until then.

## Open decisions

- **The bar for 6.1.** What has to be in it for it to be worth cutting, and what is allowed to slip to 6.2. Owner's call; nothing below can be sequenced until it is stated. The theme is not the bar: seven targets can all be on theme and still be more than one release holds.
- **Decided 2026-09-11: the dashboard module, which the MCP server and the web GUI share, runs at Java 17.** So 6.1 raises that module's runtime floor; the core library stays at its Java 8 bytecode target. Recorded here because it is a release-note fact, not a plan detail.

## Tag-day checks

Inherit the v6 list from its scope note on master when 6.1 approaches; nothing 6.1-specific yet.
