# v6 release: the "Phoenix — Spreading the Love" theme, and the announcement

Owner's idea, 2026-08-15, for the v6 release. Recorded now because a theme is worth deciding while
the work that justifies it is fresh, and forgotten otherwise.

## The theme

**Phoenix — Spreading the Love.**

- **Phoenix** — the project rising from the ashes in a new form, better than before: a fork of a
  no-longer-maintained upstream, brought back with new features rather than merely kept alive. In
  the owner's framing it is personal as well as technical, about himself as much as the codebase.
- **Spreading the Love** — the multi-language client libraries. What was a JVM-only library reaching
  other runtimes.

The two halves work together: the first says the project is alive, the second says who it is now for.

## The announcement, and its order

**This is the last task of the v6 release, not an early one.** It describes what shipped, so it
cannot be written honestly before the shipping is done — and it is the fun part, which is a reason
to protect it from being rushed rather than to skip it.

A funnel, in three pieces:

1. **The LinkedIn post — short, a teaser.** Names the themes, **apologises for the release taking
   longer than promised**, and says he comes bearing gifts. Its only job is the click through to the
   blog post. Keep it short: it is not the announcement, it is the invitation to read it.
2. **The blog post — the actual announcement.** What is in v6, what the multi-language work means for
   someone who is not on the JVM, and why the project is back. Links onward to the release notes.
3. **The release notes** — wherever they end up published. The blog links to them rather than
   restating them.

## Two practical notes for whoever writes it

- **The release notes are generated from the commit log** (`docs/releasing.md` owns how). That makes
  the blog post cheaper to write than it looks: the *why* behind each change is already in the commit
  bodies, by design, rather than needing to be reconstructed months later.
- **The strongest material is evidence, not adjectives.** The most persuasive things this release can
  say are testable: clients in N languages that all pass one shared conformance suite, a protocol
  frozen against a breaking-change gate, and a specification a fresh author implemented from the
  documents alone. See [`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md) —
  that note exists because "these are not generated slop" is a claim the announcement will have to
  make, and it is only worth making if it is backed.
