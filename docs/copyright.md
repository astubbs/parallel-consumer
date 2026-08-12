# Copyright headers and provenance

This fork carries two kinds of file - upstream-derived and fork-original - and the header a file
gets depends on which it is and what you did to it. AGENTS.md states that headers are enforced and
that there is no tool to write them; this is the rule for what to write.

## Enforcement

`bin/check-copyright-headers.sh` is the enforcement. It runs in the build itself (`validate` phase,
via exec-maven-plugin), so a plain `mvn` catches violations - not only the `Copyright Headers`
workflow. Skip it with `-Dcopyright.skip=true`.

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
