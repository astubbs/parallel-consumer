# The per-flag coverage gates fail, and the asymmetry behind them is not proven

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`codecov/project/unit` and `codecov/project/integration` report large negative deltas on branches
that change no Java. They are required checks, so this reads as "the branch under review dropped coverage" when the
evidence says otherwise.

**Why it looks wrong rather than merely red.** On the same commit the OVERALL project number RISES
and patch coverage reports "not affected". A real regression confined to two flags cannot raise the
total, and cannot leave patch coverage untouched.

**They were passing for a reason nobody should take comfort from.** Until astubbs/parallel-consumer#400
landed there was no per-flag data on the base, and both gates reported *"No coverage information
found on base report"* - and passed. Their first real comparison is the one that failed. A green that
means "could not compare" is the shape this repository keeps getting caught by.

## What is established

- **The two lanes upload different file sets per flag.** `build` (push-only) sends
  `jacoco/jacoco.xml` as `unit` and `jacoco-it/jacoco.xml` as `integration` - one file each. The
  PR-only `test` matrix sends BOTH files under one flag per suite. [`docs/ci.md`](../ci.md)'s Codecov
  section owns that table.
  <!-- file-refs: N/A - jacoco paths are generated build output under target/, and which file goes
       to which flag IS the asymmetry this note is about -->
- `codecov.yml` asserts both sides come from "the same pom executions with the same `-Pci` profile
  and the same `-Dexcluded.groups`". That is true of the maven invocation and says nothing about
  which jacoco files each upload carries, which is where the two lanes actually differ.
- The deltas are stable across heads.

## What is NOT established, and the dead end already walked

The arithmetic from that asymmetry to the specific figures. Codecov's API cannot settle it: the
`report/?flag=` parameter is **ignored** - querying `unit` and `integration` on the same commit
returns identical numbers, which are the overall total. Do not spend the attempt again.

So the asymmetry is a strong hypothesis with a mechanism, not a demonstrated cause.

## The experiment that would settle it

Make the PR lane upload one file per flag, as `build` already does, and see whether the next
comparison is clean. That is a one-line change per upload step and its outcome is the proof - which
is why it is worth doing as its own change rather than folded into whatever PR happens to notice the
red next.

Deliberately not done in the change that documented this: altering what a required coverage gate
measures is a behaviour change to a merge gate, and it wants to be the only thing in its own diff so
the before/after is legible.
