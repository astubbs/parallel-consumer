# Documentation site: design for astubbs#208

**Status:** design agreed, not yet implemented.
**Written:** 2026-08-06
**Issue:** [astubbs#208](https://github.com/astubbs/parallel-consumer/issues/208) - *Publish the docs as a
versioned documentation site, not one 1578-line README*
**Parked note superseded:** [`docs/inflight/parked-docs-site.md`](../inflight/parked-docs-site.md), which
is deleted when this work lands.

This document records the design and, more usefully, the four places it **departs from astubbs#208's own
recommendation** and why. The issue is unusually complete - it already carries an options table and a
recommendation - so the value here is in what changed after checking the facts, not in restating it.

---

## 0. What changed against the issue, and why

| astubbs#208 says | This design does | Because |
|---|---|---|
| Version with `mike` | **No `mike`** | Read the Docs versions natively from git tags/branches and supplies its own flyout. `mike` does the same job by committing built HTML to `gh-pages`. Both means two mechanisms disagreeing over what `latest` is. |
| Deploy to GitHub Pages, "consider RTD" | **Read the Docs** | The account exists, and it brings PR preview builds - reviewing a docs change by reading the rendered page beats reading a diff. |
| (silent on where site source lives) | **`src/docs/site/`**, not `docs/` | MkDocs defaults `docs_dir` to `docs/`, which in this repo is contributor scratch that astubbs#208 explicitly puts **out of scope for publication**. |
| Native snippet includes "might" fail silently, so plan a custom injection script | **Native `pymdownx.snippets`, no custom script** | Verified in the extension's source: with `check_paths: true` a missing **section** raises `SnippetMissingError`, not just a missing file. The constraint is met natively. |

---

## 1. The thing that makes this migration non-trivial

`README.adoc` is **generated output, not a source file.** Its first line says so:

```
// STOP!!! Make sure you're editing the TEMPLATE version of the README, in /src/docs/README_TEMPLATE.adoc
```

Source of truth is `src/docs/README_TEMPLATE.adoc` (1396 lines), rendered to `README.adoc` (1654 lines)
by `asciidoc-template-maven-plugin` 1.0.21 at `process-sources` (`pom.xml:604-624`). The plugin resolves
**19 `include::` directives across 10 distinct source files**, each naming a tagged region of real,
compiling code:

| Source | Tags |
|---|---|
| `parallel-consumer-example-core/.../CoreApp.java` | `example`, `exampleSetup`, `exampleProduce`, `customRetryDelay`, `maxRetries`, `circuitBreaker`, `batching`, `closeModes` |
| `parallel-consumer-core/.../ParallelConsumer.java` | `javadoc` |
| `parallel-consumer-core/.../ParallelConsumerOptions.java` | `transactionalJavadoc` |
| `VertxApp.java`, `ReactorApp.java`, `StreamsApp.java`, metrics `CoreApp.java` | `example` |
| 3 example `pom.xml` files | `exampleDep` |

astubbs#208 is explicit that this property is **non-negotiable**: Asciidoc "was chosen because
`include::` pulls named, tagged regions straight out of the real source files, so a documented example
cannot drift from the code that compiles."

So the risk in this work is not markup conversion - that is mechanical. It is that a documented example
can start lying about the code.

---

## 2. Constraint: `docs/` is already occupied

MkDocs defaults `docs_dir: docs`. This repo's `docs/` holds contributor scratch - `inflight/`, `plans/`,
`solutions/`, `refactoring.md`, `TODO_INDEX.md`, `QUARANTINED_TESTS.md`, `SELF_HOSTED_RUNNER.md` -
about 45 files that `AGENTS.md` devotes its "Where things live" table to keeping separate, and that
astubbs#208 lists under **out of scope**.

Publishing them would be wrong, and `exclude`-ing them one by one would be a permanent tax paid every
time someone adds an inflight note.

**Decision:** site source lives in **`src/docs/site/`**, beside the existing `src/docs/README_TEMPLATE.adoc`
and `src/docs/development/`. That directory already means "documentation source" here, so it is the
idiomatic home rather than a new convention. `mkdocs.yml` sets `docs_dir: src/docs/site`.

**Consequence:** MkDocs writes output to `site/`, and `.gitignore` currently ignores only `target`
(`.gitignore:92`). `site/` must be added, or the first local build offers 100+ HTML files for commit.

---

## 3. PR 1 - restructure the content (tool-agnostic)

astubbs#208 step 1, and worth doing on its own: it lets the tool decision be made late and keeps a
~1600-line content move out of the same diff as a new toolchain.

Convert with `npx downdoc` (Node 22 available locally; no global install), then hand-correct. The
constructs that convert badly and need eyes: admonition blocks (`[IMPORTANT]` + `====`), `[qanda]`
definition lists, `image::` macros, and the `ifdef::env-github[]` conditionals which have no Markdown
equivalent and simply go.

Chapter mapping from today's sections:

| Chapter | From `README.adoc` |
|---|---|
| `index.md` | *deliberately thin - see §7* |
| `quickstart.md` | Maven, Common Preparation, Simple Message Process, Demo |
| `when-to-use.md` | When to use this library (vs KIP-932) - **migrated as-is** |
| `concepts/motivation.md` | Motivation, Why would I need this, Background, FAQ, Scenarios |
| `concepts/ordering.md` | Ordering Guarantees (all 5 subsections) |
| `guides/retries.md` | Retries, Retry Delay Function, Skipping Records, Circuit Breaker, Head of Line Blocking |
| `guides/batching.md` | Batching (usage + restrictions) |
| `configuration.md` | Result Models, Commit Mode, Shutdown and Close Modes |
| `modules/vertx.md`, `modules/reactor.md`, `modules/streams.md` | HTTP with Vert.x, Project Reactor, Kafka Streams Concurrent Processing |
| `metrics.md` | Metrics (6 meter groups) + Example Metrics setup steps |
| `migration.md` | Upgrading |
| `requirements.md` | Usage Requirements, Java Version per Module |
| `performance.md` | Performance, Illustrative Performance Example |
| `internals.md` | Implementation Details, Core/Vert.x/Transactional architecture, Offset Map |
| `roadmap.md` | Roadmap |
| `contributing.md` | Development Information, Maven targets, Build Scripts, Testing, Releasing |

`README.adoc` shrinks to: badges, the one-paragraph description, the fork / drop-in-replacement
`IMPORTANT` block with the Maven coordinates, one quickstart snippet, and links onward. It stays,
because Maven Central and GitHub both render it - astubbs#208's own recommendation on its third open
question.

**KIP-932 scope:** the existing `When to use` section migrates unchanged. astubbs#208 wants that chapter
*extended* into a full share-groups comparison plus a "who cannot adopt share groups" subsection, and is
explicit that the facts need checking at writing time rather than taken from the issue. That is real
research and would swamp a migration PR - it becomes follow-up work. The landing route stays free so the
stretch pitch page can drop in without an information-architecture rewrite.

---

## 4. PR 2 - toolchain

### 4.1 Files added

- **`mkdocs.yml`** - Material theme, `docs_dir: src/docs/site`, nav mirroring §3, `pymdownx.snippets`
  with `check_paths: true`. **No `site_url`** - see §6.
- **`.readthedocs.yaml`** - `mkdocs` build, pinned Python and deps.
- **`.gitignore`** - add `site/`.

### 4.2 Snippet migration

The 19 `include::{project_root}/path[tag=name]` become `--8<-- "path:name"`.

This needs a **second marker style** added to the Java and `pom.xml` sources, because the two syntaxes
differ:

```java
// tag::example[]                  <- AsciiDoc, read by the Maven plugin
// --8<-- [start:example]          <- Snippets, read by MkDocs
```

Both sets coexist in the same file - each tool ignores the other's comments. That is what lets PR 1 and
PR 2 be independent: the AsciiDoc tags keep working while the Markdown ones arrive, so `README.adoc` is
never broken mid-migration.

The slimmed README keeps the `exampleDep` include (Maven coordinates) and the `example` quickstart
snippet, so the Maven plugin and its GitHub-Packages repository (`pom.xml:193`) stay - resolving 2
includes instead of 19. Retiring the plugin entirely was considered and rejected: a hand-written
quickstart snippet in the most-read file in the repo is exactly the drift astubbs#208 is trying to
prevent.

### 4.3 Versioning

No `mike`. RTD builds each git ref independently and supplies the version flyout.

- `latest` <- `master`
- activate from the **`v0.6.0.0`** tag onward

0.5.x is **not** published: its tags predate the chapter split, so publishing them means back-porting
this migration onto a released tag. (astubbs#208 leaves this open; this is the answer, and it is a
dashboard toggle if the maintainer later disagrees.)

---

## 5. Error handling - the non-negotiable, and its test

astubbs#208: *"a missing or renamed tag must fail the docs build loudly"* - because the failure mode
where a tool emits an empty snippet is the worst outcome, docs going quietly wrong rather than visibly
broken.

**Verified** in `pymdownx/snippets.py`, `extract_section`:

```python
if not found and self.check_paths:
    raise SnippetMissingError(f"Snippet section '{section}' could not be located")
```

`check_paths` governs missing **sections** as well as missing files. So the native mechanism satisfies
the constraint and the custom pre-build injection script astubbs#208 describes as a fallback is not
needed. This is the single fact that decides §0's fourth row.

astubbs#208 adds: *"that behaviour wants a test."* Two CI guards:

1. **Snippet guard** - rename a tag in a scratch copy, assert `mkdocs build` exits non-zero, restore.
   Proves `check_paths` is actually on and actually strict; a config flag with no test silently
   regresses the moment someone reformats `mkdocs.yml`.
2. **`mkdocs build --strict`** - fails on broken internal links.

These are unrelated jobs and neither subsumes the other: `check_paths` catches broken *code*
references, `--strict` catches broken *internal links*. The second matters here specifically because one
1654-line file becomes ~16 cross-linked chapters, and every new cross-reference is a chance to typo.

---

## 6. Read the Docs: no account coupling

**`.readthedocs.yaml` v2 has no field for account, organisation, project name or slug.** Its complete
top-level key set is `version`, `formats`, `python`, `conda`, `build`, `sphinx`, `mkdocs`, `submodules`,
`search` - purely "how to build", never "whose docs these are".

Everything account-shaped lives in the RTD dashboard, outside git: which account owns the project, the
slug that becomes `<slug>.readthedocs.io`, the GitHub App connection, active versions, custom domain.

So the config is portable, and the project can be imported under a throwaway account to verify the build
and re-imported under the maintainer's for production with **no repo change**.

**Therefore PR 2 ships no site URL at all** - no `site_url`, no README link to the hosted site, no Maven
`<url>` change. Nothing to un-pick on handover.

**Known cost of omitting `site_url`:** Material uses it for canonical link tags and the sitemap, so SEO
is weaker until it is set. That matters more than usual here, because part of astubbs#208's motivation is
that users currently find the *unmaintained upstream* first and leave. It is a reason not to leave the
handover sitting.

### Maintainer handover (astubbs#208 step 4)

Only the repo owner can do these:

1. **Import on RTD under an account with `astubbs/parallel-consumer` admin rights.** RTD's guidance:
   *"The Read the Docs user who sets up the project should also have admin rights to the Git
   repository."* With them, the GitHub App wires builds on push - *"No need to create webhooks."*
   A manual import works without admin but then needs a webhook added by hand, so builds would not
   trigger on push.
2. Activate `latest` + `v0.6.0.0` onward.
3. Optionally `CNAME` for `parallelconsumer.stub.bz` (canonical) with `pc.stub.bz` redirecting, per
   astubbs#208's open question, then set `site_url`.
4. Point README, Maven Central metadata and the GitHub description at the live URL.

RTD Community also supports inviting maintainers with full ownership rights, so a project imported
elsewhere can have the maintainer added rather than re-imported - but re-importing under their account is
cleaner, since it puts the GitHub App install on the side that has repo admin.

---

## 7. Out of scope, deliberately

- **The 45 internal `.md` files** - astubbs#208 says so, and §2 is the mechanism that keeps them out.
- **The pitch page** - stretch in astubbs#208. `index.md` stays thin so it drops in later.
- **Extending the KIP-932 comparison** - needs current per-vendor facts (Kafka 4.x early-access vs GA,
  Redpanda / WarpStream / Event Hubs, managed-cloud gating); own issue.
- **The domain** - needs DNS the implementer does not control.
- **0.5.x docs** - see §4.3.

---

## 8. Repo conventions this must satisfy

Not boilerplate: three CI gates sit between this work and a merge.

- **Branch** `docs/208-documentation-site` - branches encode the number.
- **Commit subject** `docs #208: ...` - issue at the **front**. The trailing `(#N)` slot belongs to the
  PR number, which GitHub appends on squash-merge; putting the issue there produces two bare numbers
  with no way to tell them apart.
- **No `(scope)`** - `AGENTS.md` is explicit that a directory name is not a scope and that plain
  `docs #208: ...` beats one that adds nothing.
- **PR body from `.github/PULL_REQUEST_TEMPLATE.md`**, every box checked or `N/A - <reason>`. The
  `PR Checklist` gate fails on a missing checklist *or* an unresolved box, so dropping the template is
  not a bypass.
- **Issue-ref gate** - fails added lines containing an unqualified `#NN` below 1000. The migrated
  content carries many references, so each needs qualifying as `astubbs#NNN` or `confluentinc#NNN`,
  hyperlinked where the format allows, and **resolved in both repos before choosing the prefix** - the
  numbering ranges overlap almost entirely, so a wrong reference that resolves is worse than a broken
  one.
- **No `CHANGELOG.adoc` entry** - a PR never adds one; release notes are generated from the commit log.
- **Delete `docs/inflight/parked-docs-site.md`** when this lands, per the inflight rules.
- **Merge strategy** - recommend one at merge time with reasons, rather than defaulting.

---

## 9. Sequence

**PR 1** - restructure: `src/docs/site/*.md` chapters, `README_TEMPLATE.adoc` cut to a landing page,
`README.adoc` re-rendered, references qualified. Reviewable as *content*.

**PR 2** - toolchain: `mkdocs.yml`, `.readthedocs.yaml`, `.gitignore`, dual snippet markers in the 10
source files, the two CI guards, handover notes in the PR body. Reviewable as *build config*.

PR 2 declares `depends on #<PR 1>` in its description, per `AGENTS.md`'s stacked-PR convention. (That
convention refers to a PR-dependency gate; no implementation of it was found under `.github/`, so it is
presumably a repo ruleset or app rather than a workflow. Either way the line is what the convention asks
for.)
