# BrokerPollSystem's public pause API is racy, and nothing in main code calls it

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-labels: concurrency -->

Surfaced by the torn-read hunt of 2026-08-24 as an out-of-family finding, and split out of
[`bug-torn-read-family.md`](bug-torn-read-family.md) so it is not deleted with that dossier when the
family's work closes.

`BrokerPollSystem`'s pause API is public and racy, and has **zero main-code callers** - so it is
latent rather than live. That is exactly what makes it worth a note: it is invisible to any
reproduction attempt (nothing exercises it), it is reachable by anyone consuming the library, and
wiring it up later would introduce the race without the change that wires it looking dangerous.

The decision this needs is not a fix but a disposition: make it correct, make it non-public, or
delete it. Fixing a racy API nobody calls is the least defensible of the three, and it is the one a
reader arrives ready to do.
