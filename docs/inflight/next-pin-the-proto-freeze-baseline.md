# The proto freeze gate compares against a moving target, so it cannot mean "v1 never changes"

Raised 2026-08-17 (astubbs#242). Not started. The gate works; what it *asserts* is weaker than what
the freeze claims.

## What it compares against today

`bin/check-proto-breaking.sh` resolves its baseline to **`origin/master`** unless
`PROTO_BREAKING_BASELINE_REF` overrides it. Two consequences follow, and the second is the real one:

- **Before the schema reaches master it is unarmed.** On this branch the gate passes with *"nothing
  frozen to compare against"* - correct behaviour, and it means the freeze is not load-bearing until
  astubbs/parallel-consumer#293 merges.
- **After that, the baseline moves with master.** A break is caught once, on the PR that introduces
  it. Merge it and master becomes the new baseline, so every later run compares against the broken
  schema and reports clean. The gate protects against *accidental drift in a PR*; it does not protect
  the contract.

## Why that is not what "frozen" means here

The plan froze v1 at design time - the claim is **v1 never changes**, not "v1 matches whatever master
last agreed to". Those differ the moment a break lands.

**Per-release is closer but still not it.** A release-tag baseline says "we have not broken what we
published", which is the right promise for a *versioned* schema that may evolve between majors. For a
schema declared frozen, the baseline is the commit where it was frozen, and it does not move at all.

**The repo already does this correctly one layer down.** The golden-byte fixtures are checked in, so
they are immune to master moving - which is exactly the property the breaking-change gate lacks.

## What to change

- **Pin the baseline** to the freeze commit rather than `origin/master`. It is already known:
  `c237940081620ab000d5e06edafa94c769903007`, identified while confirming the schema was unchanged
  apart from the six per-language file options landed deliberately before the gate armed.
- A checked-in descriptor snapshot would do the same job without depending on a commit being
  reachable, and matches how the golden fixtures already work. Either is better than a moving ref;
  decide which on how the release process wants to handle a future v2.
- **This also arms the gate immediately**, on this branch, instead of waiting for a merge - which is
  the more valuable half, since the schema is being changed *now* and protected *later*.

## Watch for

Pinning makes the gate assert something stronger, so **run it against the current tree before
committing the change** - if v1 has already drifted from the freeze commit in some way nobody
intended, a pinned baseline is what surfaces it, and that is a finding rather than a reason to back
out.
