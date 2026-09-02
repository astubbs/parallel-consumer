<!-- What does this PR change, and why? Keep it in sync with the actual content as the PR evolves. -->

<!-- Issue references in this body are checked by the same gate as the diff: below the threshold, a
     reference must name its repo. Use the FULLY qualified form here - astubbs/parallel-consumer#NN
     or confluentinc/parallel-consumer#NN - because it is the only one that both names the repo and
     auto-links on GitHub, and the only one `Fixes`/`Closes` will act on. Fenced code blocks are
     skipped, so pasted logs and command output are fine. -->

<!-- If this PR serves an issue, make the FIRST LINE a link to it, fully qualified so it actually
     links - e.g. "Closes astubbs/parallel-consumer#155." The reference in the title is NOT clickable.
     If it serves no single issue, cite none: a loosely related one is a misdirection. -->

## Description

...

## Checklist

<!-- Keep this checklist and resolve EVERY box: tick it [x], or mark it "N/A - <reason>". The "PR Checklist"
     CI check fails a human PR when the checklist is missing entirely, or when a box has no [x] and no N/A.
     Only real bot authors (Dependabot/Renovate etc.) are exempt. -->

- [ ] Docs updated - or `N/A`
- [ ] User-facing feature documentation data added under `docs/features/` - or `N/A - <reason>`
- [ ] Tests added/updated - or `N/A`
<!-- The working note: a PR earns its docs/inflight/ note at its FIRST commit, not its last - a note
     opened at the end is a reconstruction. docs/inflight/AGENTS.md owns the rule. If no note is
     needed (nothing here that `gh` cannot show), replace this box's tail with "N/A - <reason>", on
     the box's own line. This box deliberately omits the usual placeholder: the gate reads the box
     line, and a pre-written placeholder satisfies it untouched. -->
- [ ] `docs/inflight/` working note (`pr-`/`branch-`) started at the PR's first commit - or say why none is needed
- [ ] Title & body reflect the final content of this PR
- [ ] Ran `ce-simplify` and `ce-code-review` locally - or `N/A - <reason>`

<!-- The last box asks what you actually did, not what you should have done. "No, and here is why"
     is a complete answer - so say which of the two you skipped and why, rather than ticking it to
     get past the gate. Reasons that are good ones, not excuses:

       * DOCS-ONLY OR CONFIG-ONLY CHANGE. `ce-simplify` works on code and will refuse to run on a
         diff that has none, so `N/A - docs only` is the honest answer rather than a dodge.
       * SPENDING THE REVIEW SOMEWHERE CHEAPER. A local `ce-code-review` is the most expensive
         option. Asking `@claude review this` on the PR, or `@codex review`, costs far less - and
         the `@claude` comment route is the ONLY one that can open inline review threads, which are
         what mechanically block a merge here (docs/ci.md). Preferring it is a real choice, so name
         it: `N/A - asked for @claude review on the PR instead`.
       * THE CHANGE IS MECHANICAL. A rename, a version bump, a generated file.

     What this box is guarding against is not skipping the step; it is skipping it SILENTLY, so
     nobody can tell whether the diff was looked at hard or merely passed CI. -->

