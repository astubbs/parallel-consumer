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
An anchor nobody ran is a `file:line` with extra steps: nothing in CI checks any of this, so the
only thing standing between a reader and a confidently wrong pointer is the author having run it.
