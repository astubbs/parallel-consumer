# Documentation site: design for astubbs#208

**Status:** signed off by the maintainer; phase 1 not yet implemented.
**Written:** 2026-08-06. **Rewritten:** 2026-08-14, around Starlight.
**Issue:** [astubbs#208](https://github.com/astubbs/parallel-consumer/issues/208) - *Publish the docs as a
versioned documentation site, not one 1578-line README*
**Parked note superseded:** [`docs/inflight/parked-docs-site.md`](../inflight/parked-docs-site.md), which
is deleted when this work lands.

The generator was decided by spike, not argument, and the decision reversed once mid-way. This document
records the design as signed off, and - more usefully - **which claims are measured and which are still
open**, because three of the earlier version's load-bearing claims turned out to be wrong.

---

## 0. Decision log: what changed, and who changed it

| Decision | Earlier position | Now | Whose call |
|---|---|---|---|
| Generator | MkDocs + Material | **Starlight** (Astro) | Maintainer, after the spike |
| Hosting | Read the Docs | **Cloudflare Pages** | Maintainer - RTD Community is ads-supported |
| Versioning | `mike`, then RTD-native | **None in phase 1** - one version | Maintainer |
| Branch flow | Two stacked PRs | **Long-lived `docs-site` branch**, PRs in parallel | Maintainer |
| Site source | `src/docs/site/` | **`src/docs/site/content/docs/`** - see §2 | Measured, §2 |
| Loud failure | Native `check_paths` was enough | **Custom remark plugin + `build:done` guard** | Measured, §5 |

**Withdrawn, for the record.** Material for MkDocs has a published EOL of **5 November 2026**, which
killed the original recommendation; MkDocs itself last released 1.6.1 in August 2024. Zensical (the
Material team's successor) lost to Starlight on the maintainer's one non-negotiable - see §5.

---

## 1. The thing that makes this migration non-trivial

`README.adoc` is **generated output, not a source file.** Its first line says so:

```
// STOP!!! Make sure you're editing the TEMPLATE version of the README, in /src/docs/README_TEMPLATE.adoc
```

Source of truth is `src/docs/README_TEMPLATE.adoc`, rendered to `README.adoc` by
`asciidoc-template-maven-plugin` 1.0.21 at `process-sources` (`pom.xml:603-623`). It resolves
**19 `include::` directives, 17 distinct file/tag pairs** (verified by `grep -c` and
`grep -o ... | sort -u`; earlier comments on the issue said 19 distinct, then 14 - both wrong), each
naming a tagged region of real, compiling code:

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

So the risk here is not markup conversion - that is mechanical. It is that a documented example can start
lying about the code, **quietly**. Every §5 decision follows from that.

---

## 2. Where the site source lives - and why it is deeper than expected

Two constraints, pulling against each other.

**`docs/` is occupied.** MkDocs' default `docs_dir` was `docs/`, and this repo's `docs/` is contributor
scratch - `inflight/`, `plans/`, `solutions/`, `refactoring.md`, `TODO_INDEX.md` - which astubbs#208 puts
explicitly **out of scope for publication**, and which `AGENTS.md` devotes its "Where things live" table
to keeping separate. Publishing it would be wrong; an `exclude:` list would be a tax paid every time
someone adds an inflight note. That argument survives the generator change intact.

**Starlight will not simply be pointed elsewhere.** `docsLoader()` hardcodes its base to
`src/content/docs` (verified in `@astrojs/starlight/loaders.ts` - it wraps Astro's `glob()` with
`base: getCollectionPathFromRoot(...)` and exposes no `base` option).

The obvious workaround is to skip `docsLoader()` and call `glob({ base: './src/docs/site' })` directly,
which Starlight's own test fixtures demonstrate. **Measured: it half-works, and the half that fails is
the important half.** Pages render and routes generate correctly (`/quickstart/`,
`/concepts/ordering/`), but every internal link is then reported invalid:

```
╭─ docs/site/concepts/ordering.md
14 | /guides/retries/
   ·               ╰── invalid link
Found 4 invalid links in 3 files.
```

`starlight-links-validator` computes a page's identity by stripping the literal string `content/docs`
from its path (`libs/markdown.ts:67`, `libs/validation.ts:315`). Relocate the collection and every page
resolves to a slug that does not match the routes Astro actually built, so **a green build becomes
impossible while the link checker is on**. There is no option to change it - the full option schema is
`components`, `errorOnFallbackPages`, `errorOnInconsistentLocale`, `errorOnRelativeLinks`,
`errorOnInvalidHashes`, `errorOnLocalLinks`, `exclude`, `failOnError`, `reporters`, `sameSitePolicy`.
Nothing addresses the base path.

**Control run**, because "the validator is wrong" and "my links are wrong" produce the identical error:
the same four pages with the same four links, at the default location, report *All internal links are
valid*. The relocation is the cause.

**Decision:** move Astro's **`srcDir`**, not the collection. `srcDir: './src/docs/site'` keeps content at
`<srcDir>/content/docs`, so `docsLoader()` and the validator both keep their assumption, while the tree
still lives where this repo wants it:

```
src/docs/site/               <- Astro srcDir
  content.config.ts          <- must be INSIDE srcDir, not at src/
  content/docs/*.md          <- the pages
  assets/
astro.config.mjs             <- repo root
```

**Verified end to end:** `5 page(s) built`, *All internal links are valid*, exit 0. Two traps found by
running it rather than reasoning about it: `content.config.ts` must move inside `srcDir` (leave it at
`src/` and the collection is silently empty, surfacing as the *misleading* error `The slug "quickstart"
specified in the Starlight sidebar config does not exist`), and image assets resolve relative to the new
`srcDir`.

**Consequences:** `.gitignore` currently ignores only `target` (`.gitignore:92`); it needs `dist/`,
`node_modules/`, `.astro/` and `.starlight-links-validator/`. Note the output directory is `dist/`, not
the `site/` the MkDocs-era version of this document said.

---

## 3. Content restructure (its own PR, tool-agnostic)

astubbs#208 step 1, signed off as worth doing alone. Convert with `npx downdoc`, then hand-correct.
Constructs that convert badly and need eyes: admonition blocks (`[IMPORTANT]` + `====`), `[qanda]`
definition lists, `image::` macros, and the `ifdef::env-github[]` conditionals, which have no Markdown
equivalent and simply go.

Chapter mapping from today's sections - the maintainer approved this list as-is ("it's the README's
existing sections, which is exactly the right amount of change for a migration PR"):

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

**Diátaxis:** the maintainer raised <https://diataxis.fr> and was explicit that reorganising around it is
**not phase 1**. Where a page falls naturally into tutorial / how-to / reference / explanation and the
move is free, take it; **do not rewrite anything to make it fit**.

**Link style is forced.** `starlight-links-validator` sets `errorOnRelativeLinks: true` by default, so
cross-references are root-relative (`/concepts/ordering/`), not `../concepts/ordering/`. This cuts
against the maintainer's "write portable Markdown" insurance policy, and it is the one place where the
default pushes the less portable way. Flagging rather than burying it: the alternative is turning the
flag off and losing the check that already caught a stale link in the spike.

`README.adoc` shrinks to badges, the one-paragraph description, the fork / drop-in-replacement
`IMPORTANT` block with Maven coordinates, one quickstart snippet, and links onward. It stays, because
Maven Central and GitHub both render it.

**KIP-932 scope:** `When to use` migrates unchanged. Extending it into a full share-groups comparison
needs current per-vendor facts checked at writing time - that is research, not migration, and phase 2 is
explicitly decided after phase 1 lands.

---

## 4. Toolchain

### 4.1 Files added

- **`astro.config.mjs`** (repo root) - `srcDir: './src/docs/site'`, the region-import remark plugin, the
  region guard, `starlight-links-validator`, nested `sidebar` mirroring §3.
- **`src/docs/site/content.config.ts`** - stock `docsLoader()` + `docsSchema()`. Inside `srcDir`, per §2.
- **`package.json`** / **`package-lock.json`** - `astro`, `@astrojs/starlight`,
  `starlight-links-validator`, `sharp`. The spike ran 251 packages / 231MB; the maintainer accepted this
  against Zensical's 11 packages / 106MB, noting migration is cheap "these days".
- **`.gitignore`** - `dist/`, `node_modules/`, `.astro/`, `.starlight-links-validator/`.
- **`remark-region-import.mjs`** (~103 lines) and **`region-guard-integration.mjs`** (~26 lines),
  promoted from the spike.
- **No `site` / `site_url`** until the domain exists - see §6.

### 4.2 Code inclusion

The 19 `include::{project_root}/path[tag=name]` become fenced blocks with
` ```java region="path:name" `. The plugin reads the **AsciiDoc markers already in the sources**, so no
second marker style is added anywhere:

```java
// tag::example[]      <- one set of markers, read by both the Maven plugin and the site
```

This was Starlight's clearest win over Zensical, which cannot read AsciiDoc tags and would have needed
dual markers in two files. The maintainer's "the markers will get replaced, not added to" holds for 17 of
the 19 includes; `tag::example[]` in `CoreApp.java` and `tag::exampleDep[]` in the example `pom.xml`
**stay**, because the slimmed README still resolves them through the Maven plugin. So the plugin and its
GitHub-Packages repository (`pom.xml:193`) remain, resolving 2 includes instead of 19.

Retiring the plugin and hand-writing that snippet was considered and rejected: a hand-written quickstart
in the most-read file in the repo is exactly the drift astubbs#208 exists to prevent.

### 4.3 Versioning

**None in phase 1** - one version, so no versioning tool at all. The URL shape is fixed now because it is
expensive to change later: **root serves the newest release, older versions under a version path.**
Nothing to build; just nothing designed that forecloses it.

`starlight-versions` is third-party and **untested** - a real gap, deferred to phase 2 rather than
presented as settled.

---

## 5. Loud failure - the non-negotiable, and what it actually took

astubbs#208: *"a missing or renamed tag must fail the docs build loudly"* - because a tool emitting an
empty snippet is the worst outcome: docs going quietly wrong rather than visibly broken.

Two earlier claims in this document were wrong, and both mattered:

1. **"Native `pymdownx.snippets` is enough."** Wrong. `check_paths` guards *entry* into a region, never
   its termination: `found` is set by the start branch alone, so a renamed **end** marker splices the
   remainder of the file - measured at **199 lines published, 15 leaked `tag::` markers, exit 0** even
   under `--strict`. The maintainer spotted this before the spike did.
2. **"Starlight can never fail the build."** Also wrong, and this reversed the recommendation. Astro does
   swallow content-rendering errors (`astro/dist/content/loaders/glob.js:130-132`, core, not Starlight) -
   but **`astro:build:done` propagates**. The remark plugin records failures into a shared array; a
   ~26-line integration throws there if it is non-empty.

Measured behaviour of the design as specified:

| | start renamed | end renamed | broken cross-link |
|---|---|---|---|
| Zensical `--strict` | exit 1 | **exit 0**, 199 lines published | exit 1, native |
| **Starlight + region guard** | **exit 1** | **exit 1** | **exit 1** (plugin) |

**The cache difference decided it.** Both generators cache on the Markdown file, not the included source.
Break a marker and rebuild without clearing: Starlight **exits 1** (the guard reads the filesystem at
build time, so a warm cache cannot hide a break); Zensical **exits 0** and serves a stale but
correct-looking page. Zensical's correctness depends on remembering `--clean`, and a discipline that must
be remembered will eventually be forgotten.

**Re-verified against the final §2 layout**, not just the spike's default one - the guard reads paths, so
relocating `srcDir` could plausibly have broken it:

```
[ERROR] [region-guard] [remark-region-import] .../content/docs/quickstart.md: no end marker
  "end::example[]" in CoreApp.java (start marker found - an unterminated region would
  silently publish the rest of the file)
```

`EXIT=1` with the marker renamed, `EXIT=0` restored.

**The guard needs its own test** - the same argument the maintainer made about `check_paths`, and it
applies with more force to code we wrote ourselves. CI gates on: the guard's unit test, then the build
(which carries the region guard and the link validator in-process). Note this repo already runs
`node .github/scripts/*.test.js` in the `PR Checklist` job, with the precedent stated in that file's own
words - "the gate's own logic is subtle enough to have shipped two real misjudgements already, so it is
unit tested and the tests run before the gate does."

**Still open, for the maintainer:** whether the region guard should fail the build (as specified) or only
a separate CI step. Asked on the issue, not yet answered; the in-build version is what the spike
measured, and it is what makes the cache hazard go away.

---

## 6. Publishing

GitHub Actions builds; `wrangler pages deploy dist/ --branch=<branch>` publishes. A non-production
`--branch` gives a stable preview URL per branch, posted on the PR - the same command produces production
and previews, so they cannot drift. Custom domain and SSL are free, and the DNS is the maintainer's.

**Requires two repo secrets only the maintainer can add:** `CLOUDFLARE_API_TOKEN` and the account ID.
This blocks *publishing*, not the content or the toolchain, so it is not on the critical path until the
end.

**No `site` / `site_url` until the domain is decided.** Cost, stated rather than hidden: Starlight uses it
for canonical tags and the sitemap (`@astrojs/sitemap` warns and skips without it), so SEO is weaker
until it is set. That matters more than usual here, because part of astubbs#208's motivation is that
users find the *unmaintained upstream* first - a reason not to let the handover sit.

---

## 7. Out of scope, deliberately

- **The ~45 internal `.md` files** - astubbs#208 says so; §2 is the mechanism that keeps them out.
- **The pitch page** - stretch. `index.md` stays thin so it drops in later without an IA rewrite.
- **Extending the KIP-932 comparison** - needs current per-vendor facts; phase 2.
- **The domain** - needs DNS the implementer does not control.
- **0.5.x docs** - not published, not back-ported; its tags predate the chapter split.
- **Diátaxis reorganisation** - §3.
- **Generator-specific components** (`<Tabs>`, `<CardGrid>`) - the maintainer's insurance policy is to
  treat each as a deliberate spend and keep usage near zero, so the cost of migrating off Starlight stays
  proportional to almost nothing. MDX is explicitly **not** a requirement.

---

## 8. Repo conventions this must satisfy

Not boilerplate - CI gates sit between this work and a merge.

- **Branch** `docs/208-documentation-site`, merged up from `master` regularly; everything lands on the
  long-lived **`docs-site`** branch, and one merge flips it live. No window where `master` has less
  documentation than today.
- **Commit subject** `docs #208: ...` - issue at the **front**. The trailing `(#N)` slot belongs to the PR
  number, which GitHub appends on squash-merge (`AGENTS.md:402`). No `(scope)`: `AGENTS.md` is explicit
  that a directory name is not a scope and that plain `docs #208: ...` beats one that adds nothing.
- **PR body from `.github/PULL_REQUEST_TEMPLATE.md`**, every box ticked or `N/A - <reason>`. The
  `PR Checklist` gate fails on a *missing* checklist as well as an unresolved box, so dropping the
  template is not a bypass.
- **Issue-ref gate** - fails added lines carrying an unqualified `#NN` below 1000, *even when it resolves*
  ("resolving is not evidence the reference is right", `issue-ref-gate.test.js:26-31`). The migrated
  content carries many references, so each needs qualifying as `astubbs#NNN` / `upstream #NNN` and
  **resolved in both repos before choosing the prefix** - the ranges overlap almost entirely, so a wrong
  reference that resolves is worse than a broken one.
- **No `CHANGELOG.adoc` entry** - a PR never adds one; release notes are generated from the commit log.
- **Delete `docs/inflight/parked-docs-site.md`** when this lands, per the inflight rules.
- **Fork-PR CI caveat** - self-hosted lanes are skipped for PRs from forks (`AGENTS.md:211`), so a branch
  on the origin repo gets fuller CI than a fork PR. This bears on the access question in §10.

---

## 9. Sequence - parallel, not stacked

The `docs-site` branch removes the need to stack, which the maintainer called out explicitly: *"restructure
and toolchain can run in parallel. Develop the toolchain against a couple of sample pages, rebase onto the
content once it merges."*

1. **Content** - `src/docs/site/content/docs/*.md` from `README.adoc`, references qualified, links made
   root-relative. Reviewable as *content*.
2. **Toolchain** - `astro.config.mjs`, `content.config.ts`, `package.json`, plugin + guard, `.gitignore`,
   the guard's unit test. Developed against 2-3 sample pages; rebased onto (1). Reviewable as *build
   config*.
3. **README slim-down** - `README_TEMPLATE.adoc` cut to a landing page, `README.adoc` re-rendered, the
   broken Maven Central badge removed (§10).
4. **Publishing** - the Actions workflow and `wrangler` deploy. Needs the maintainer's secrets (§6).

As many PRs as the work wants; (1) and (2) do not block each other.

---

## 10. Open with the maintainer

Tracked here so they do not get lost between issue comments.

1. **Push access.** The flow needs a long-lived `docs-site` branch on `origin`. It **does not exist yet**
   (checked ~200 branches), the implementer has no push access (`GET /collaborators` → 403), and no fork
   exists. Either the maintainer creates the branch and adds a collaborator, or the work goes via fork PRs
   - at the cost of the self-hosted CI lanes (§8).
2. **Cloudflare secrets** - §6.
3. **Region guard: in-build or separate CI step?** - §5.
4. **The broken badge** (their side-ask). It is the Maven Central badge,
   `README_TEMPLATE.adoc:47`, and it is broken twice over: `maven-badges.herokuapp.com` died with Heroku's
   free tier, **and** nothing is published under `bz.stub.parallelconsumer` yet (`repo1.maven.org` group
   path → 404; `pom.xml:10-13` is `0.6.0.0-SNAPSHOT`), so *any* badge on *any* service reads "not found"
   today. Repointing at shields.io returns HTTP 200 and an SVG whose text says `maven-central: not found`
   - which is why the status code is not the thing to check. **Agreed course:** remove it now, re-add the
   link when 0.6.0.0 publishes; the re-add note belongs in
   [`docs/inflight/release-0.6.0.0.md`](../inflight/release-0.6.0.0.md). The GitHub Actions badge on
   line 49 is fine (HTTP 200, renders).
