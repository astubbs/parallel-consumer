---
date: 2026-08-10
topic: module-maturity-table
focus: the shape and content of the per-module maturity table for 0.6.0.0
mode: repo-grounded
---

# Ideation: the per-module maturity table

Revised after review. The first pass assumed the note's framing - two independent axes, graded per module across the stable set - and produced seven candidates inside it. That framing does not survive contact with what 0.6.0.0 actually ships. What follows is the corrected set; the original candidates are preserved in the rejection summary with the reason each fell.

## Grounding Context

### The planning assumption

Plan for the post-merge state: every open PR lands except the DLQ draft and microbatching, and the Connect, Kafka Streams and GUI experiments ship as experiments. Anything that contradicts the current tree because its PR has not landed is recorded in the ledger rather than written into the artefact.

### Codebase Context

Master's reactor is five modules (`pom.xml`): `parallel-consumer-core`, `-vertx`, `-reactor`, `-mutiny`, `-examples`. Connect, Kafka Streams and the GUI are spikes on branches with no open PR, so they enter the table only once their PRs land.

None of the open PRs add a module. They are core fixes, tests, CI, docs, a health-check surface and virtual threads. The module set changes only through the experiments.

The table sits beside `=== Java Version per Module` (`src/docs/README_TEMPLATE.adoc:1072-1090`), which establishes the house pattern: an asciidoc `|===` table stating the fact, followed by a `WARNING:` admonition stating what goes wrong if the reader ignores it. That table omits `parallel-consumer-examples`, a live uncaught omission sitting where the new table goes.

The README currently makes no stability, maturity or production-readiness claim at all. This table is additive, not corrective: there is no misleading badge to fix, only silence. There is no precedent anywhere in the template for a status or badge column.

The adapters are thin. Core is 11,498 lines against vertx 650, reactor 137 and mutiny 151, and they all drive the same engine. `parallel-consumer-examples` is never published (`maven.deploy.skip` and `maven.install.skip` at `parallel-consumer-examples/pom.xml:34-35`, `skipPublishing` at line 45).

The quarantine machinery is on master, not a branch: `docs/QUARANTINED_TESTS.md`, `bin/check-quarantine-registry.sh`, `bin/check-quarantine-owners.sh` and the `release.yml` guard. The registry is empty there. It is real, but it currently gates nothing, and it is engineering-system machinery, which the run plan's own contract assigns to the testing-as-product artefact rather than to this table.

API stability has no enforcement of any kind. The `internal` package holds 19 classes, all public, with no `package-info.java` and no ArchUnit rule referencing it in any of the six `TestConventionsArchTest` files. The boundary is naming-only.

### Past Learnings

No prior art; this project has never designed a maturity table. Two adjacent findings apply: published prose in this repo is audited as fact before release (`docs/inflight/release-0600-blockers.md` records release-time triage catching three classes of false published claim), and README template edits are durable text, unlike the CHANGELOG section which is regenerated from the git log.

### External Context

Every named taxonomy studied splits maturity into more than one dimension rather than collapsing it: the Node.js Stability Index, Kubernetes feature gates, OpenTelemetry status per component per signal, and the CNCF adoption-risk ladder. The mkenney convention's Release Candidate level already contains almost exactly the claim this project wants to make: "fairly settled and is in use in production systems. Backwards-compatibility will be maintained unless serious issues are discovered."

The convergent failure mode: maturity labels go stale the moment they are written unless tied to an enforced signal. The countermeasures found in the wild are gRPC's `@ExperimentalApi` one-way ratchet and Rust's per-feature tracking issues.

## Topic Axes

Decomposition skipped - atomic subject. The candidate axes here are the deliverable itself.

---

## Ranked Ideas

### 1. The table's job is one boundary, not a gradient

**Description:** Reliability does not vary across the stable set. Core, vertx, reactor and mutiny are the same engine with thin adapters, every open fix lands before the tag, and the release ships on the condition that known critical defects are fixed - so a column grading them against each other would have every cell reading the same thing. Reliability varies at exactly one place: the line between the shipped library and the experiments. The table's whole job is to draw that line unmissably.

**Basis:** `direct:` core is 11,498 lines against vertx 650, reactor 137 and mutiny 151, all driving one engine, so a per-module defect gradient across the stable set has nothing to grade. `reasoned:` if the release is conditional on known critical defects being fixed, then at release time the known-defect count is uniform by construction. A column whose cells are all identical is not a claim, it is padding.

**Rationale:** This is the difference between a table that carries information and one that performs rigour. It also means the artefact is far smaller than the note assumed, which matters when brevity is a stated requirement rather than a preference.

**Downsides:** It abandons the per-module maturity framing the note was built around. It also means the table says little until the experiments actually land, which is why idea 3 exists.

**Confidence:** 90%
**Complexity:** Low

---

### 2. The two-axis point is a sentence, not a column

**Description:** "Pre-1.0 reserves the API surface, not reliability" is the load-bearing content of this whole effort, and it needs one prominent sentence rather than a table structure. Both axes move together across the stable/experiment line - the experiments are neither reliable nor API-stable, and the shipped modules are reliable with a reserved API - so two columns would be perfectly correlated and would carry the same single distinction twice.

**Basis:** `direct:` the README makes no stability claim at all today, so the baseline is silence and one good sentence is already a large improvement. `direct:` the house pattern at `src/docs/README_TEMPLATE.adoc:1074` does exactly this - "Most modules run on Java 8. The Mutiny module does not, because SmallRye Mutiny itself is built for Java 17" states the default in prose and lets the table carry the exception. `external:` no source found states the pre-1.0 framing this way, so it is a differentiating sentence rather than a borrowed one.

**Rationale:** The risk being managed is a reader seeing `0.6.0` and inferring the library is not ready. A sentence corrects that inference directly. A two-column table asks the reader to infer it from structure, which is slower and, on this row set, structurally redundant.

**Downsides:** A sentence is easier to skim past than a table. It needs to sit where the reader arrives, not only beside a table late in the document.

**Confidence:** 85%
**Complexity:** Low

---

### 3. Experiment rows stay in the ledger until their PRs land

**Description:** Connect, Kafka Streams and the GUI have no open PRs and are not in master's reactor. The table cannot list them yet without asserting something the tree contradicts. Record the intended rows and their wording in `docs/inflight/next-module-maturity-table.md`, and let whichever PR lands each module add its own row.

**Basis:** `direct:` master's `<modules>` block lists five modules; none of the open PRs add one. `direct:` `docs/inflight/release-0600-blockers.md` records release-time triage catching three classes of published claim that had drifted from reality - the exact failure mode of writing a row ahead of its module.

**Rationale:** It keeps the artefact true at every commit rather than true only at some future point, and it puts the row where the person who knows the module's real state will be standing.

**Downsides:** The table is thin until the experiments land, so its most important content arrives last. If an experiment slips past the tag, the table shipped saying less than intended.

**Confidence:** 90%
**Complexity:** Low

---

### 4. Each experiment row carries the blast radius, because that is the reader's actual question

**Description:** The question a reader has on seeing an experimental module is not "how mature is it" but "can it hurt me if I do not use it". The answer is unusually clean here: depending on the artifact is the entire opt-in. The experiment rows should say that, not grade the experiment.

**Basis:** `direct:` `docs/inflight/release-0.6.0.0.md` already argues this and gives the three reasons that make it provable rather than reassuring - the modules are leaves, the patched Apache Kafka classes ship only inside the experimental jar, and the seam is unreachable from core, vertx and reactor. `external:` UL's Listed versus Recognized Component distinction, where a component is certified within the assembly it was tested in and carries no standalone claim.

**Rationale:** It converts the experiments from a liability in the table into a demonstration of how the project is put together, and it answers the question the reader actually has rather than the one a maturity grade answers.

**Downsides:** The blast-radius claims were verified on a spike branch against `origin/master`. If the experimental modules change shape before they land, the three reasons need re-verifying against what actually ships.

**Confidence:** 80%
**Complexity:** Low

---

### 5. A "use this if" column instead of a status vocabulary

**Description:** Skip the alpha/beta/stable vocabulary entirely. Each row says what the module is for and who should reach for it, with the experiments saying plainly that they are not for production use yet. A status word invites an argument about which word; a use statement does not.

**Basis:** `external:` every taxonomy studied - Node's index, Kubernetes gates, the CNCF ladder - pairs its level with a one-line promise, and the promise is what readers act on rather than the level name. `direct:` the neighbouring Java table carries no status vocabulary either; it states a fact and a consequence.

**Rationale:** It sidesteps the vocabulary question the note left open, keeps the house style of the table beside it, and gives the sceptical skimmer something actionable rather than a grade to discount.

**Downsides:** Prose cells are longer than one-word cells, and the table has to stay narrow. It also loses the at-a-glance sortability a status column gives.

**Confidence:** 75%
**Complexity:** Low

---

### 6. A drift check, because the module set is about to change

**Description:** Add a seconds-fast CI check that fails when the table's rows drift from the reactor's published modules. The neighbouring Java table already omits `parallel-consumer-examples` and nobody caught it, and three modules are about to be added by three separate PRs.

**Basis:** `direct:` master's reactor lists five modules; the Java table at `src/docs/README_TEMPLATE.adoc:1072-1090` lists four. `direct:` exact precedent in the repo - `bin/check-quarantine-registry.sh` "fails on any drift between the `@Quarantined` annotations in the code and the entries below", enforced by a seconds-fast audit job on every PR.

**Rationale:** The cheapest and most certain source of staleness is a module landing with nobody remembering its row, and that is precisely what is about to happen three times. It also means an experiment cannot land without declaring itself.

**Downsides:** It needs an explicit exclusion for `parallel-consumer-examples`, which is never published, so it is not a straight copy of the quarantine script. Building it before the experiments land is speculative.

**Confidence:** 70%
**Complexity:** Medium

---

### 7. Say that the two axes are not equally backed, or give the API axis teeth

**Description:** Reliability has machinery behind it; API stability has none. No annotation marks an unstable public surface, and `internal` is a naming convention rather than a boundary. Either write that asymmetry down honestly, or build the ratchet - an `@ApiStability` annotation, an ArchUnit rule and a drift check in the shape the quarantine registry already uses.

**Basis:** `direct:` the `internal` package holds 19 public classes with no `package-info.java` and no ArchUnit rule in any of the six `TestConventionsArchTest` files. `external:` gRPC's `@ExperimentalApi`, which cannot be re-added once removed, and Rust's per-feature tracking issues.

**Rationale:** A reader assumes two stated axes are backed the same way. Naming the asymmetry is honest and free; building the ratchet is the real answer and is separately schedulable.

**Downsides:** The enforced version is multi-part engineering, not a table cell. Tightening `internal` is also a migration rather than a rule, since reducing visibility risks breaking users already importing those classes - a breaking change in a release whose pitch is that reliability is not what pre-1.0 reserves.

**Confidence:** 70%
**Complexity:** High for the enforced version, Low for the stated asymmetry

---

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | The reliability cell cites the enforced quarantine release gate | Wrong artefact and overclaimed. The gate proves no known-failing test is held out of the gating suites, which is not a reliability guarantee; the registry is empty so it currently gates nothing; and engineering-system machinery belongs to the testing-as-product section under the run plan's own contract. All five ideation frames reached it and none checked it against that contract |
| 2 | Key the rows by processing guarantee rather than by module | Weakened by the planning assumption. Its force came from EoS having far thinner production exposure than the unordered path, but the deadlock fix lands before the tag, and the reliability line that survives is stable-versus-experiment, not configuration |
| 3 | Split reliability into correctness and liveness | Solved a problem that goes away. It existed to state the deadlock honestly without hedging; the fix lands before the tag |
| 4 | An open-advisories cell beside the rating | Same. Its content becomes the release-time cross-check instead, recorded in the ledger |
| 5 | Reliability as an envelope scoped to configuration | Subsumed. With one reliability line rather than a gradient, there is no cell for an envelope to qualify |
| 6 | Two tables, one per axis, keyed differently | Obsolete. The axes are perfectly correlated across the stable/experiment line, so two tables would carry one distinction twice |
| 7 | The table doubles as the 1.0 graduation checklist | Scope overrun - the run plan assigns 1.0 exit criteria to the roadmap artefact |
| 8 | PR authors cite the affected row for a release-time audit | Basis refuted - no release-time audit exists; the closest thing runs per PR |
| 9 | The `internal` package is an already-true API stability statement | Basis refuted - 19 public classes, no rule. Survives inside idea 7 as work to be done |
| 10 | Last-breaking-change date column instead of labels | Weak - a long-untouched date reads as ambiguous between stable and abandoned |
| 11 | Metric names are the undeclared API surface | Real and under-considered, but belongs to the API stability work rather than the table's shape |
| 12 | Inline stability sentence at each module usage section | Placement is a real concern; folded into idea 2 rather than kept separate |
| 13 | Every cell carries an evidence link or is deleted | A drafting rule rather than a direction; adopt it while writing |
| 14 | A "not claimed" column | Duplicative - the README already makes this move and is trusted for it |
| 15 | Adapters marked as inheriting the core engine | Subsumed by idea 1, which is the same observation applied to the whole stable set |
| 16 | Credit-rating style "what would move this" column | Overlaps idea 5 at higher maintenance cost |
| 17 | Publish the misreading each label is designed to prevent | Good legend-writing advice; apply it when wording, do not choose between |
| 18 | Default-alpha stance for modules added after 0.6.0.0 | Sound and cheap, but a policy line rather than a shape decision; adopt alongside idea 6 |
| 19 | Anchor the table as the canonical "is this maintained" citation | Nearly free; adopt while writing |
| 20 | Mirror the Java table's exact skeleton with no status column | Below the ambition floor - a default nobody would contest |
