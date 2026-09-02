# Run the chaos suite on a long loop under load

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->

The suite runs once per PR and finishes in minutes. The defects this project keeps finding are the
ones that need time and load to appear - so the suite is weakest against exactly the shape of defect
the work keeps turning up. A long soak supplies both, and it is cheap: the machine is idle overnight
either way.

**It has already paid for itself once.** The A/B harness (`soak-deadlock-probe.sh`, which moved to
the overnight-harness branch - astubbs/parallel-consumer#405 - because it is the same kind of
instrument) ran a
dozen invocations per arm overnight and turned a single ambiguous result into an unambiguous one,
with a red control. And a random-seed CI sweep in the same window produced the first replayable seed
the async stall line has ever had. Both came from running longer, not from thinking harder.

## What to build

**A soak mode that runs for hours rather than repetitions** - wall-clock bounded, looping the whole
suite, on a schedule rather than on demand.

**Under load, deliberately.** The family ledger records that outcomes here are load-sensitive and
that the reproduction of at least one signature differed between a contended and an uncontended box.
A soak that runs on an idle machine is measuring the easy case.

## What makes a long soak useless, and it is always the same thing

**Reaping.** A twelve-hour run that fails once and buries the evidence in a log that truncates is
worth less than no run at all, because it costs a day and produces a rumour. Today's runs needed the
uploaded reports artefact to be readable at all; `gh run view --log-failed` returned truncated noise,
which `docs/solutions/workflow-issues/gh-run-view-log-truncation.md` already owns.

So the capture side is the work, not the looping:

- **One line per iteration**, appended, so the result is a tally and not a log to read.
- **The seed on every line**, because the seed is the asset and console output expires.
- **A thread dump on the failure**, at the moment of failure. The six captures that identified the
  revoke deadlock exist only because something took a dump; every earlier sighting was a signature
  without a mechanism.
- **What the run reached, not just whether it passed** - the window discipline again.
- **Artefacts that survive**, since the reports outlive the log.

**And the report filenames collide.** With more than one repetition every rep rewrites
`TEST-<class>.xml`, so an artefact holds several same-named reports and a grep picks an arbitrary
one. Two reads of one artefact disagreed on 2026-08-28 for exactly this reason. A soak multiplies it
by the iteration count, so it has to be fixed before the loop is long - and the same fix serves the
chaos sharding work.
