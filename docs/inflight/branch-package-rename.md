# Package rename `io.confluent.parallelconsumer.*` → `bz.stub.parallelconsumer.*`

**Decided: it goes ahead in 0.6.0.0, and it must land before v6 ships.** Nothing has ever been
published under the fork's `bz.stub.parallelconsumer` groupId, so no downstream code imports our
packages yet. Renaming now costs users nothing. Renaming after v6 asks everyone who adopted the fork
to migrate a second time, for a reason that will look cosmetic to them. There is no third moment.

**The README is already written for the new namespace** - its `== Upgrading` section tells users to
find-and-replace their imports. That makes the docs ahead of the code until the rename lands, so if
this ever slips out of v6 the README has to be reverted in the same breath rather than left
describing imports that do not exist.

Full task inventory, evidence and Apache 2.0 analysis:
[`docs/plans/2026-08-11-001-refactor-package-rename-plan.md`](../plans/2026-08-11-001-refactor-package-rename-plan.md).

## Why it is worth doing at all

Apache 2.0 §6 grants no trademark rights. `io.confluent.*` is Confluent's mark, and it is our last
remaining use of it as an identifier in shipped artifacts on a fork Confluent does not maintain.
Moving off it reduces exposure; the licence permits the rename outright (§4), and the obligations it
does impose - retain the Confluent copyright headers, keep `NOTICE`, mark modified files - are
unaffected either way.

**This is a different question from the Apache trademark work.** That one is about the ASF's `Kafka`
mark in our product *branding*; this one is about Confluent's mark in our *namespace*. They share a
shape and nothing else - do not let a future session merge them into one "rebrand".

## What the next session must not trust

- **A clean `grep -rn "io\.confluent"` is not evidence the rename is complete.** Three files encode
  the package as an escaped regex (`io\.confluent\.parallelconsumer\.`) and are invisible to both a
  find-and-replace and that verification sweep. Search allowing the backslash:
  `grep -rnE 'io[\\.]*confluent'`.
- **The mutation gate fails open.** Stale, `bin/ci-mutation-test.sh` matches nothing, prints
  `PIT: no core main-source classes changed - nothing to mutate, skipping` and exits 0 - green
  forever while scoring zero mutants. After the rename, assert the lane actually scores mutants on
  the first PR; do not accept the tick.
- **An ArchUnit rule goes vacuous silently.** `TestConventionRules.java` pins a fully-qualified class
  name as a *string*; stale, the condition never fires and the rule passes, so the guard keeping
  Docker-dependent tests out of surefire quietly stops guarding. `failOnEmptyShould` does not catch
  it.

## What the work actually is

Not the `sed`. `bin/check-copyright-headers.sh` decides provenance by exact path match against the
fork-point file listing, so moving the package directories makes every upstream-derived file look
fork-original and its retained Confluent header an error - 197 violations, measured by performing
the rename in a throwaway clone rather than predicting it. Redesigning that provenance model is the
engineering; 121 files then need a `Modifications Copyright` line, which is bookkeeping on top.

## Settled, so nobody re-investigates

- **No wire-format exposure.** Offset metadata is magic-byte plus bitset/run-length plus base64; no
  class name reaches the wire. The rename cannot break offset compatibility. This was the main risk
  and it is closed.
- **Downstream migration is small.** ~25 public types at most; all five example apps combined import
  8 distinct names, a typical consumer 4-6.

## Follow-up that only makes sense after the rename

Vet every remaining `confluent` occurrence one at a time and confirm each is legitimate attribution
(`NOTICE`, copyright headers, upstream links, the pinned `master-confluent` mirror) rather than
something the sweep missed. Given the escaped-regex trap above, this pass is the real completeness
check, not the grep.
