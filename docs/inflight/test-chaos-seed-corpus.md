# Keep the suspect seeds, and replay them in CI on a schedule

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->

Chaos seeds are recorded in prose, scattered across the family ledger and its split-out sighting
files, and nothing replays them. That makes every one a one-off observation whose deciding experiment
never runs - the ledger says so about its own contents: *"Not one of the five captured seeds has ever
been replayed"*.

**A seed that reproduces is the most valuable artefact this suite produces, and it is currently
stored as a sentence.** On 2026-08-28 seed `9086872209853284830` was shown to reproduce the async
`NO_PROGRESS` stall in most runs, on unmodified master, in minutes - the first handle this line has
ever had. It was found by random-seed hunting, not by replaying anything, and it was nearly lost in a
CI log.

## What to build

**A seed corpus as data, not prose** - a file listing each suspect seed with its scenario, the
signature it produced, where it came from, and its observed reproduction rate. `docs/features/`
already holds machine-readable data of this shape, and `docs/data/` holds the roadmap, so the
convention exists.

**A scheduled lane that replays the corpus** and reports which seeds still reproduce. Two things fall
out of that which nothing currently provides:

- **A regression signal.** A seed that reproduced last month and does not today means something
  changed - possibly a fix nobody attributed, possibly a detector that stopped detecting. Both are
  worth knowing and neither is visible now.
- **A rate rather than a sighting.** Every entry in the family ledger is careful to say "two captures
  is still not a rate". A corpus replayed on a schedule turns that into a number.

## Two things to get right

**A seed that no longer reproduces must not be silently dropped.** That is a finding, not a cleanup -
it is the closest this project gets to evidence a defect was fixed. Retire it with the run that last
reproduced it and the run that stopped.

**Distinguish "did not reproduce" from "did not run".** The recurring failure in this repo is a green
that never exercised the thing it claims to test - the mutation lane that exited 0 scoring nothing,
the deadlock probe whose window never opened, both arms of a soak going green with the mechanism
untouched. A corpus lane needs the same window discipline: record what the run actually reached, not
just its exit code.

## Sources to seed it from

The family ledger and its split-out sighting files carry the seeds already, including the six
thread-dump captures of the revoke deadlock, the async `NO_PROGRESS` seeds, and a set explicitly
marked as passing control arms that must not be replayed expecting failure.
