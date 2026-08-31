# Copyright headers and provenance

This fork carries two kinds of file - upstream-derived and fork-original - and the header a file
gets depends on which it is and what you did to it. AGENTS.md states that headers are enforced and
that there is no tool to write them; this is the rule for what to write.

## Enforcement

`bin/check-copyright-headers.sh` is the enforcement. It runs in the build itself (`validate` phase,
via exec-maven-plugin), so a plain `mvn` catches violations - not only the `Copyright Headers`
workflow. Skip it with `-Dcopyright.skip=true`.

**It covers the whole tree, not just Java**, and it does so by classifying *every* tracked file into
one of three sets. `ENFORCED_TYPES` names the filename globs that must carry a header and the
comment syntax each uses; `EXEMPT_PATHS` names what carries none, each entry with its reason; and a
file matching neither is reported as a violation telling you which table to add it to. That third
set is the point of the design - an extension list on its own goes stale the moment a language
nobody listed arrives, silently, whereas an unclassified file fails the build. `--report` prints the
classification of every file, which is how to see what the tables actually cover rather than assume.

`ENFORCED_TYPES` deliberately names languages this tree does not have yet - the table was written
against the language-proxy client stack - because the cost of an unused entry is nothing and the
alternative is a red build on a correct file.

`bin/test-check-copyright-headers.sh` generates a red-and-green case for **every** entry in
`ENFORCED_TYPES`, reading the table out of the scanner, so a language cannot be added there without
being exercised.

**`-Dlicense.skip` no longer exists as a property**; drop it from any command you copy out of an
older doc or script.

**There is no header-applying tool: the scanner checks, it does not write.** New files get their
header written by hand, per the rules below. The mycila `license-maven-plugin` used to fill that
role and was removed - it knew only the Confluent header template, so its `format` goal stamped the
wrong attribution onto fork-original files, and its git-year resolver auto-bumped years and broke in
worktrees.

## The rules

- **Do not change copyright headers on existing files** unless the file has substantive code
  changes in the same commit.
- **Do not bump copyright years** as an incidental or standalone change.
- The `NOTICE` file at the repo root contains the legal attribution structure for the fork.
- **New files written entirely for the fork** use
  `Copyright (C) <year> Antony Stubbs and contributors` - never the Confluent header.
- **Upstream-derived files MODIFIED on the fork** retain the Confluent notice and ADD
  `Modifications Copyright (C) <year> Antony Stubbs and contributors` beneath it. This is Apache
  2.0 4(b) (retain notices) plus 4(c) (state changes) - the convention used by e.g. Amazon Corretto
  and MariaDB for derived files. The scanner detects modification against the fork point
  automatically, so forgetting the line fails CI.
- **Files renamed or extracted from upstream keep the Confluent header.** Register renames in
  `RENAMED_FROM_UPSTREAM` (`newpath|oldpath` lines) and extractions in `EXTRACTED_FROM_UPSTREAM`,
  both inside `bin/check-copyright-headers.sh`. Renames with content changes, and all extractions,
  also require the modifications line.
- **Upstream code RECOVERED from a branch that never reached the fork point is a third case**, and
  gets its own table, `RECOVERED_FROM_UPSTREAM_BRANCH` (`path|origin-commit`), in the same script.
  The header it needs is an extraction's - Confluent plus the modifications line - so the reason for
  a separate table is not the header but the **check**: a recovery names one origin commit, so the
  scanner *verifies* the claim rather than trusting it, and an entry whose commit does not hold the
  file fails the build. That only works while the commit stays reachable, so **archive such a branch
  as a tag before deleting it** - otherwise the check degrades to a warning.
- **An upstream file that carried NO notice at the fork point is not required to grow one.**
  Upstream marked its `.java`, its poms, its shell scripts and some resources - but not its
  workflows, its IDE run configurations, its Maven wrapper or its prose. Apache 2.0 4(b) says to
  retain the notices that exist; there is nothing to retain, and stamping a Confluent notice onto a
  file Confluent chose not to mark would be inventing an attribution. The scanner reads the
  fork-point blob to decide, so this is a fact about immutable history rather than a list that can
  rot, and it counts what it grandfathered rather than skipping quietly. **The limit matters as much
  as the rule**: a file that *was* marked and has since lost its header is still a violation.
- **A Confluent claim is `Copyright (C) ... Confluent` on ONE line.** A fork-original file may
  discuss the fork's provenance in its header prose - `.github/workflows/copyright.yml` and
  `bin/deps-version-rules.xml` both do - without that reading as a claim.
- **A header that is present can still be wrong**, and the scanner checks all three cases: the
  notice must sit inside a comment in that language's syntax, never above a `#!` shebang, and never
  above an `<?xml ...?>` declaration. Note also that an XML comment may not contain `--`, so the
  header text has to be written without one.
- **A whole-package MOVE is a rule, not ~200 rename entries.** `PACKAGE_MOVES`, in the same script,
  maps a current path back to its fork-point path before every lookup, so provenance survives the
  fork's package rename (`bin/rename-packages.sh`). Without it the verdict *inverts*: every
  upstream-derived file misses the fork-point lookup, is judged fork-original, and its required
  Confluent header becomes a violation - measured at 0 → 197, in maven's `validate` phase, so every
  `./mvnw` on the tree dies before it starts. The script's header carries the reasoning.

## Why a header at all, and why prose does not get one

**A notice does not create protection.** Copyright subsists automatically on creation under Berne,
and in the US has required no notice since 1989. Every file in this tree is protected whether or not
it carries a header, prose included. So any argument that `.md` files are somehow unprotected
because they lack a notice is wrong, and it is not why this gate exists.

What a notice actually does is narrower, and every part of it concerns **a reader who has the file
and not the repository**:

- it carries the licence terms to a file that has been separated from `LICENSE`;
- it makes the file legible to a licence scanner - no header reads as *unknown licence*, which in
  many corporate ingestion pipelines means *cannot use*;
- it defeats an innocent-infringement defence in mitigation of damages (17 U.S.C. 401(d));
- for upstream-derived files it discharges Apache-2.0 4(b), which requires modified files to carry
  prominent notice that they were changed.

**So the question a file has to answer is whether it can reach someone detached from `LICENSE`.**
The build publishes sources jars, so `.java` files do exactly that: somebody opens a source file out
of a `-sources.jar` with no other context, and the header is the only thing telling them the terms.
The poms travel the same way, published beside the jar, which is why the root `pom.xml`'s
`Modifications Copyright` line is the most load-bearing line this gate enforces.

Prose cannot. No `.md` or `.adoc` file lives under `src/main`, nothing sweeps documentation into an
artefact, and `README.adoc` is generated at build time from `README_TEMPLATE.adoc` and sits at the
repository root. Anyone reading one of these is looking at the repository, and the repository
contains `LICENSE`. A per-file notice there has no recipient.

**Everything else with a comment syntax is headered uniformly, including files that never ship** -
workflows, `bin/` scripts, `codecov.yml`. That is deliberately over-inclusive, for a mechanical
reason rather than a legal one: "does this file travel?" is a per-file judgement, it is easy to get
wrong, and it is **silent** when wrong. A rule keyed on file type is one a scanner can enforce and a
reviewer can check. The cost is notices on files nobody will read; the cost of the alternative is a
shipped file with no notice, and nothing to catch it.

The line is therefore drawn at **prose versus everything else that can hold a comment**, and it is
drawn there because prose is the one category that is both never distributed and read constantly by
people working in the repository - where a notice on every working note is pure noise.

**Decided 2026-08-24 on astubbs#338.** The exemption already existed and was right; its stated
reason was not. It read "the notice would render into the document", which is false - markdown,
AsciiDoc and HTML all have non-rendering comment syntax, and this repository already depends on
that, since every `docs/inflight/` note carries `inflight-type` markers no reader ever sees. A false
reason is worse than none, because the obvious way to "fix" it is to add headers to a hundred
documents. The rule stands; the reasoning is now the real one.

## What is exempt, and why

The reasons live beside the globs in `EXEMPT_PATHS`; only four kinds qualify, and "nobody got round
to it" is not one of them - that makes the file a violation, which is the point.

| Kind | What it covers |
|---|---|
| **Generated** | protoc and ts-proto output (`_generated/`, `generated/`, `*_pb.rb`), lockfiles, `go.sum`. Regeneration overwrites the whole file, so a header cannot survive there. |
| **No comment syntax** | `*.json`, `*.sln`, binary fixtures, `py.typed`, the ServiceLoader registry. Strict JSON has no comments at all; where a notice has to sit in-band the conventions are a `"//"` key (`tsconfig.json`) or the `description` string (`package.json`) - but a scanner cannot demand either in general. |
| **Vendored** | the Maven wrapper (`mvnw`, `mvnw.cmd`, `.mvn/`). The header is its author's business, and `mvn wrapper:wrapper` rewrites it. |
| **Not distributed** | prose (`*.md`, `*.adoc`, `*.html`) - see the section above - and IDE/VCS/tool configuration (`.idea/`, `.gitignore`, `.editorconfig`, `.gitmessage`, `CODEOWNERS`, `.claude/`). `LICENSE` and `NOTICE` are the licence texts themselves. |
