# v6 release-documentation data

This branch turns the four v6 documentation ideas into renderer-independent YAML under `docs/data/`
and `docs/features/`. It is deliberately **source data only**: the documentation-site renderer and
the later documentation/promotional passes consume this data. Do not claim that those public pages or
their PR gate exist here, and do not delete the four source `next-*.md` notes until their work has
actually landed.

## Trust is the purpose of the testing data

The testing data is not an inventory intended to prove that the repository has many tests. Its job is
to seed later documentation and promotional passes with a falsifiable answer to: *why should a reader
trust this fork after upstream?* The resulting material must show what the infrastructure detects,
why each control is stronger than a green test run, and how the fork improved the suite since the last
upstream release.

The current `testing-evidence.yaml` has the raw ingredients, but needs these explicit seed records
before a later pass treats it as complete:

- **Testing-infrastructure capabilities and why they matter:** real-broker integration tests for
  consumer-group progress and rebalances; ambient-probe autopsies that distinguish broker contention
  from a real progress failure; calibrated chaos scenarios for alive-but-stuck failure modes; mutation
  testing for whether tests notice meaningful code changes; negative controls proving a regression
  test fails without its fix; and the quarantine lane, which preserves a diagnosed red signal while
  an open owning fix exists and blocks a release while non-empty.
- **The quality-producing system, not only the test layers:** required checks, static and dynamic
  analysis, dependency review, mutation coverage and automated PR review. A later promotional pass
  needs an inspectable workflow/script path and the merge/on-demand posture for each, so it can explain
  how the next defect is caught rather than simply celebrating the last one.
- **Improvement since upstream, in a reader-checkable shape:** defect -> proof/control arm -> recurrence
  guard. Seed records for the drain/stall investigation, the latest-offset-reset control arm, the
  vacuous-await correction, ambient probes, quarantine discipline and the chaos pain suite. Link an
  upstream mirror where one exists. The intended conclusion is that this fork investigated inherited
  failure modes and made fixes reproducible; it is not merely an upstream codebase with extra features.
- **Honest boundaries:** unknown defects remain possible. Streams and Connect are alpha and need their
  own evidence; they cannot borrow the stable-module conclusion. A thin area belongs in the open ledger,
  not behind softened wording.

## v6 is the stability-release outlier

The v6 position is now stronger and simpler than the initial plan language: **0.6.0.0 is cut only when
all known defects in the release scope are gone**. This is a release gate, not a claim that an uncut
branch has already achieved omniscience; unknown defects remain possible. The gate is intentionally
not weakened by deferring virtual threads, micro-batching and a dead-letter queue, because those are
new capabilities rather than defects to excuse.

The 2026-08-09 PR-queue snapshot makes that claim concrete. The stability input includes the
`confluentinc#857` rebalance-progress fix (astubbs#29), stale same-offset record safety (astubbs#31),
transactional correctness (astubbs#257 and astubbs#261), and offset/lifecycle/operator fixes
(astubbs#57, astubbs#116, astubbs#201, astubbs#203, astubbs#204 and astubbs#207). It also includes
the work that makes a green test result worth trusting: no retry-hidden flakes (astubbs#224), honest
commit assertions (astubbs#260), a transactional-claim register (astubbs#262), inactive-test
remediation (astubbs#264), and causality-based test repairs (astubbs#265). The data records this as a
release snapshot and says to reconcile it against the final merged queue before cutting v6; it does
not turn the roadmap into an issue tracker.

## Data gaps found in review

- **Module evidence:** `testing-evidence.yaml` says a module cannot borrow core-only confidence, but
  its concrete inspection paths are core paths. Add a module-to-evidence mapping for core, Vert.x,
  Reactor, Mutiny, examples, Streams and Connect, and link each `module-maturity.yaml` row to it.
  Planned alpha evidence may remain conditional, but must name its release-time proof.
- **Release-critical reliability proof:** the generic manual release review must retain the concrete
  `confluentinc#857` deadlock condition, its `RebalanceEoSDeadlockTest` stress proof, the needed
  astubbs#29 resolution and the post-fix parallel-integration proof. Otherwise the stable reliability
  claim has no reproducible decision rule.
- **Feature coverage and gate semantics:** add a Mutiny integration feature definition with Java 17,
  setup and boundaries. Define *user-visible* precisely enough that the PR-template feature-data
  checkbox can become an honest future gate; a refactor can be N/A, a new option or consumable module
  normally cannot.
- **Feature provenance and navigation:** published feature data says it records an identifying
  implementation commit, but the schema permits it to be absent and several first-release records lack
  one. Make it required, fill those values, and verify all stored README anchors against generated
  AsciiDoc IDs.
- **Roadmap relationships:** the roadmap has theme order and per-theme progress criteria, but not the
  promised public explanation of what one theme unblocks for another. Add those relationships without
  copying issue-by-issue status or adding dates.

## Plan-versus-current-work research (2026-08-09)

This is the review record for the data currently on this branch, checked against the unchanged
`next-testing-suite-as-product-docs.md`, `next-module-maturity-table.md`,
`next-living-roadmap.md`, and `next-per-pr-docs-and-feature-index.md` plans. It records observed
shortcomings, not a claim that the data or later public material has landed.

- **Testing breadth is asserted but not evidenced per module.** The plan calls out core, Vert.x,
  Reactor, Mutiny, examples and the Streams alpha as separately tested. The current data has five
  concrete `inspect.path` entries, all below `parallel-consumer-core/`; `module-maturity.yaml` has no
  per-module evidence field. That is a direct mismatch with the data's own no-borrowing rule.
- **The strongest trust story is not modelled yet.** The plan asks for a skimmable account of the test
  system's breadth, control arms, engineering process and changes since upstream. Current data models
  six test layers, three investigation records, negative controls and quarantine, but no structured
  quality-system records for static analysis, required checks, dependency review or automated PR review;
  it also does not connect the post-upstream improvements into the required defect -> proof -> guard
  sequence.
- **The maturity release check lost its specific falsifier.** The plan names the `confluentinc#857`
  deadlock, astubbs#29, `RebalanceEoSDeadlockTest` and the required post-fix parallel-integration proof.
  Current release validation says only to review known critical defects. A later pass cannot tell what
  makes the stable-module claim true without recovering that research.
- **The feature model is broad but incomplete.** There are 28 feature YAML files (26 published and two
  planned), but none describes the existing Mutiny module even though module-maturity data calls it a
  stable integration. Nine published feature files omit the implementation commit that the feature-data
  README says should establish their first-release provenance.
- **Two future consumers have unresolved data semantics.** `user-visible` is not defined well enough
  for a feature-page gate, and several feature README anchors use unverified hyphenated fragments rather
  than explicit AsciiDoc IDs. The roadmap records ordering and `advances_when`, but no theme-to-theme
  unblocking relation despite the plan promising one.

Research checks also confirmed that the four source plan files and the shared
`release-0.6.0.0.md` entry are unchanged by this branch's data work. `git diff --check` is clean.

## Research pass applied after the review

The initial-review shortcomings above were used as a work list. The following is the corresponding
evidence review, and the parts now represented in data rather than left as a promise:

- **Harness architecture and why it is credible:** the deterministic core harness makes ordering,
  commit and bounded-concurrency assertions observable without broker timing; `BrokerIntegrationTest`
  and `KafkaClientUtils` supply a controlled real-Kafka path; CI runs that path in JVM forks so
  broker contention is not mistaken for product behavior. `AmbientProbeExtension` records rebalance
  dwell, stagnant lag and frozen commits in a failure autopsy. The seeded chaos harness adds the
  alive-but-not-progressing failure class and a no-loss/bounded-duplicate ledger; mutation and soak
  lanes have deliberately narrower, stated meanings. These facts now live in `testing-evidence.yaml`
  with an inspect path and limitation for each, instead of only as a list of suite names.
- **Per-module breadth, with its limits:** core has deterministic, real-broker and chaos evidence;
  Vert.x has a real-Kafka/WireMock concurrency-and-drain test; Reactor and Mutiny have adapter unit
  harnesses that exercise delayed completion, bounds and commits; examples exercise application
  wiring, with real-broker Streams and metrics examples. Reactor and Mutiny do *not* currently have
  adapter-specific real-broker suites, examples are not API guarantees, and the planned Streams and
  Connect alphas still require release-time proof. `module-maturity.yaml` now references these exact
  records, so core confidence cannot be silently borrowed.
- **Quality is produced by more than a test layer:** test-convention architecture checks, required
  unit/integration/performance lanes, quarantine audit, static/dependency analysis, PR-scoped
  mutation and review-delivery verification are now structured quality-system records. Their gate
  state and scope are recorded rather than implied; a non-gating green lane is not presented as
  merge approval.
- **Post-upstream defect → proof → guard:** the data now includes the drain-path zombie,
  latest-reset nudge race, vacuous backpressure await and thread-parallel contamination cases, each
  with the investigation or control arm and recurrence guard. The empty quarantine registry is
  explicitly explained as enforced evidence, not an absence of process. The release-critical
  `confluentinc#857` test and astubbs#29 resolution remain an explicit manual v6 falsifier.
- **Feature coverage:** a planned v6 Mutiny feature record now exposes its Java 17 floor, async-Uni
  contract, known evidence and first-release caveat. It is planned rather than retroactively marked
  published because its implementation commit is not contained in any release tag. The feature set
  is now 29 YAML records: 26 published and three planned.

Remaining data work is deliberately kept visible: fill the missing first-release implementation
commits for nine older published feature records, and verify stored README anchors against generated
AsciiDoc IDs. The feature-data contract now defines `user-visible` and the future gate's burden for
an N/A, but does not implement that gate. Renderer work, the public testing page and promotional copy
remain downstream consumers of this data, not delivered by this branch.

## Downstream boundary

Site rendering, the public testing-as-a-feature page, promotional copy, and enforced feature-page PR
gating are separate follow-up work. They should begin from this data and preserve its claim boundaries;
they must not invent reliability claims or hide known limits to make the release sound stronger.
