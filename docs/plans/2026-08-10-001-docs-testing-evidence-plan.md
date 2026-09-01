# Present the test suite as a product, for the 0.6.0.0 release

**Status:** the data exists as `docs/data/testing-evidence.yaml`. The published prose does not. Shortcomings surfaced while writing it are tracked separately, and the disabled-test sweep it depends on became a v6 release gate.
**Migrated:** 2026-08-10, from `docs/inflight/`. The reasoning below is why the artefact has the shape it does.

Wanted as promotional material for 0.6.0.0. The fork's strongest claim is not its feature list - it is
that the system has been investigated, and that specific long-standing defects are now *provably*
fixed. Nothing currently says so in one place, so a reader has to take it on faith or go digging.

## Write for one reader

**The sceptical expert who skims.** Documents like this are rarely read in full. What happens instead
is that one reader who knows the domain forms a judgement, says so publicly, and that judgement is
adopted second-hand by everyone else until something displaces it. The document is therefore not
addressed to a general audience at all; it is addressed to the person whose assessment will be
repeated.

Two consequences follow, and between them they determine the format:

1. **Every claim must be independently verifiable in seconds.** Name the test class, the script, the
   number. A claim that cannot be checked cheaply is worse than no claim at all, because a single
   unverifiable assertion invites the reader to discount the rest.
2. **It must survive skimming.** Tables, single sentences, headings that carry the argument on their
   own. A structure that only works when read from start to finish has already failed.

**Brevity is a requirement, not a stylistic preference.** The material is genuinely interesting and
will expand without resistance, and length is self-defeating here: a longer document is a less-read
one. Where a section outgrows a glance, reduce it to its claim and a link, and let the repository
carry the detail.

## What it should cover

The suite's architecture, the breadth of what it exercises, the depth at which it does so, the
distinct kinds of testing employed, and the extent to which its claims are demonstrable rather than
asserted. Most of this material already exists and needs assembling rather than inventing:

| Strand | Where the evidence already is |
|---|---|
| Layers - unit, integration, mutation, chaos, soak, performance | `bin/ci-unit-test.sh`, `ci-integration-test.sh`, `ci-mutation-test.sh`, `chaos-test.sh`, `soak-test.sh`, `performance-test.sh` |
| What gates a merge vs what is on demand | AGENTS.md → CI section; required checks vs dispatch-only lanes |
| Breadth across modules | core, vertx, reactor, mutiny, examples, and the Streams alpha each carry their own suites |
| Corner cases and provability | offset-encoding codecs, transactional guarantees, rebalance/stall behaviour |
| Flake discipline | `docs/solutions/test-flakiness/` - each write-up is a diagnosis, not a mute |
| Quarantine, and that it is *empty* | `check-quarantine-registry.sh`, `quarantine-lane-report.sh` |
| Negative controls | AGENTS.md's rule that a regression test must be shown to go red without its fix |

The last two are the most persuasive to the target reader and the least likely to be found by
accident. A quarantine mechanism that exists *and is empty* is a much stronger signal than no
mechanism. Negative controls are the thing that separates a suite that tests from a suite that
merely passes.

## The meta layer: how quality is produced, not just measured

The apparatus around the code is itself part of the quality claim, and is currently invisible:

- **The engineering system** - static and dynamic analysis, the mutation lane, automated PR review
  coverage, the required-check gates. For this audience it is often *more* persuasive than a test
  count, because it speaks to catching the **next** defect rather than the last one.
- **The investigation and generation process** - the plan documents, the `docs/solutions/` diagnosis
  write-ups, the `docs/inflight/` ledger of known-open work, and the rules in AGENTS.md that force
  control arms, negative controls, and prior-art checks before a fix is believed.

**Show the artifacts, do not describe the methodology.** This is the one strand most likely to read
as self-congratulation, and the target reader discounts process claims instantly. A dated corpus of
diagnosis write-ups they can open is evidence; "we follow a rigorous process" is noise. Same test as
everywhere else in this document: can they check it in seconds?

The most persuasive single item is probably the **open** ledger - publishing what is known to be
unfinished is a costly signal that the finished claims are honest.

## The section that does the real work: what changed since upstream

A dedicated part covering the testing done **since the last upstream release** - which is to say,
since this fork's first. Its purpose is specific: to answer the reasonable suspicion that the fork
amounts to a set of new features attached to an inherited codebase. The accurate account is close to
the opposite. The majority of the effort went into establishing how the system actually failed,
recording those findings, and demonstrating that the fixes hold.

Frame each item as **defect → how it was proven → how it is prevented from returning**, not as a
changelog. Upstream issue numbers are worth citing where a mirror exists: the reader can see the
problem was real and pre-existing, not invented here.

`docs/solutions/` is the spine of this section - it is already written in exactly this shape.

## Shortcomings go to the ledger, not into the prose

Writing this will surface gaps: areas that are thinner than the document implies, claims that cannot
be evidenced, suites that look impressive but assert little. **Track those here in `docs/inflight/`
as they are found.** Do not quietly soften the wording to accommodate a gap - the target reader is
exactly the person who will notice, and one padded claim discredits the honest ones.

Being explicit about a known-thin area is *more* credible than silence, so a short "what is not
covered" note is worth having rather than avoiding.

## Remaining

The rendered page, generated from the data. Anything this surfaces that cannot be evidenced goes to `docs/inflight/` as its own entry rather than being softened here.
