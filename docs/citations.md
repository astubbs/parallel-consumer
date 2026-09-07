# Repairing a citation in a record you may not rewrite

How to fix a reference that no longer resolves, in a document whose claims are fixed. AGENTS.md
carries the rule that produces durable citations in the first place - cite a greppable anchor, never
a `file:line`, a form the repo already relies on in `.github/workflows/pr-checklist.yml`, whose
todo-index entries are "keyed by marker TEXT, not line number". This is what to do about the ones
written before that rule, and about targets that have since moved, been renamed or been deleted
outright.

## Dated documents are records, so exactly one edit is allowed and one is required

Documents under `docs/plans/` and `docs/solutions/` say what was known on the day they were written.

- **Rewriting their claims, findings or conclusions to match today's code is forbidden** - it
  falsifies the record.
- **Repairing a reference so it still finds what the document was pointing at is required.** The
  document said "look here"; a citation that no longer resolves makes the record *less* faithful,
  not more.

The two are easy to confuse, because both start with reading today's code. The test is what the edit
changes: the *address* of the thing the document was talking about, or *what the document says about
it*.

## When the target is gone, point at the history holding it

Do not delete the reference, and do not substitute the nearest surviving file. Point at the commit
that still contains the thing:

```bash
git log -S'<string>' -- <path>          # the commit that removed the string
git log --diff-filter=D -- <path>       # the commit that deleted the file
git show <sha>^:<path>                  # the file as the document read it
```

Give the reader the command and the anchor to grep once they run it, so the pointer survives further
history rewriting of everything except that commit.

## A successor is not "the nearest plausible file"

When a document cites something that has been split, moved or superseded, a successor is only a
successor if it **contains the same item**. Naming a file that merely lives in the same area sends
follow-up work at the wrong mechanism, and it does so *credibly*, which is worse than a pointer that
obviously fails. Before naming one, run the grep and read what it lands on. In particular:

- A **fixed** item must not be pointed at an **open** one, even where both concern the same test or
  class - the reader will treat the open item as the continuation of the closed one.
- A roster or status note that has drifted out of date is not a live successor. Point at the history
  for the stance the document relied on, and leave correcting the live note to whoever owns it.
- If the item did not survive the move at all, **say so**. "It did not carry over; the history above
  is the record of it" is a complete and honest repair. Inventing a target is not.

## When the target still exists but now means something different

That is not a repair. Flag it beside the citation and **leave the claim alone** - deciding whether
the finding still holds is a fresh piece of work, not a documentation edit.

## Verify every anchor you write

Run the grep the citation implies and confirm it lands on the intended line in the intended file.
An anchor nobody ran is a `file:line` with extra steps.

## The path is checked; the anchor is not

**`bin/check-file-refs.sh` fails a cited repo path that does not exist**, and the `PR Checklist`
workflow runs the same module - `.github/scripts/file-ref-gate.js`, unit tested by its self-test, so
the local and CI answers cannot drift. It replaces the sentence that used to close the section
above: "nothing in CI checks any of this, so the only thing standing between a reader and a
confidently wrong pointer is the author having run it." That was true, and it cost what it looks
like it would cost - `docs/ci.md` told readers to run `bin/check-review-gate-contract.sh`, a script
that has never existed, and the citation survived every review from astubbs#287 onward.
<!-- file-refs: N/A - the whole point of the sentence is that this path never resolved -->

**It reads the whole tree, not the diff, and ratchets against the base.** Deleting a file does not
change a single line in the documents that cite it, so a diff-scoped gate is blind to the commonest
way a citation breaks: of the 87 dangling references repaired when the gate landed, 59 came from two
moves - the in-flight ledger becoming a directory, and the `io.confluent` -> `bz.stub` rename.

It fails only on findings the **base did not already have**, which is not a softening but the thing
that makes whole-tree scanning survivable here. The first cut failed on any finding, assuming a
clean tree it would keep clean; master gained 90 dangling references from ordinary work within a
day, about fifty of them documents describing modules and plans that live on **feature branches**
(`parallel-consumer-streams/` paths, `docs/plans/` entries not yet merged). That is how this repo
writes things down, and a gate red on every PR for it would be switched off within a week. So a red
result means **this branch** broke something - which still includes deleting a file, since the
citations pointing at it were resolving in the base and stop resolving here.

A citation resolves three ways, and the last two are what keep the gate quiet enough to use: from
the repo root, relative to the citing document, or as the tail of a real path
(`internal/ConsumerManager.java` - the shorthand the anchor rule above encourages).

**If a rename makes the gate blame you for a citation you never touched, this is why.** A finding is
identified by *which document* holds it plus *which path* it cites - deliberately, so that copying a
broken citation into a second document cannot ride along on the first one's excuse. The cost is that
**renaming a document carries its inherited findings across as new ones**: same text, same dangling
path, different key. This repo renames things (`io.confluent` -> `bz.stub`, the in-flight ledger),
so it will happen. The fix is the ordinary one - repair the citation, or close its paragraph with a
marker - and it is worth doing, since a rename is exactly when nobody is checking whether the
pointers still resolve.

**What it cannot check is the anchor**, which is the half that decides whether a citation is any
good. A gate can say the file is there; only you can say your quoted string is still in it, and
still means what the document claims. So the rule above is unchanged - run the grep.

**It is narrow on purpose.** A token counts as a citation only if it carries a file extension, so
prose about a directory ("under `src/test`") is ignored, as are globs, `<placeholders>`, URLs and
absolute host paths. A gate that flagged prose would be opted out of instead of used.

### Naming a path that is gone, on purpose

This procedure requires it - "point at the commit that still contains the thing" writes the dead
path out - so the gate has to allow it, and it does, two ways.

**A history pointer is the repair, so the gate reads it as one.** A path written behind a git
revision - `git show 0de96fc^:<path>` - is never treated as a live citation, and a document carrying
such a pointer has its other citations of that same path satisfied too. That is not a concession; it
is the repair this document prescribes, recognised. Thirty of the last thirty-one findings on master
were one correctly repaired record being reported as thirty defects, which is how a gate teaches
people to stop applying the fix.

**Anything else named deliberately gets a marker**, by paragraph:

```markdown
The note lived at docs/inflight.md until astubbs#112 split it into a directory.
<!-- file-refs: N/A - naming the replaced file is the point of the sentence -->
```

The marker covers **the paragraph above it**, up to the first blank line. It never reaches downward: an escape covering text the author has not read is no narrower
than having no gate. The reason is mandatory and must start with a letter - written bare inside an
HTML comment, `<!-- file-refs: N/A -->`, the `-->` would otherwise pass as the reason.

`file-refs: N/A - <reason>` on its own line in the **PR body** silences the gate for the whole PR.
Prefer the paragraph marker; the PR-wide form hides any real breakage that lands alongside it.
