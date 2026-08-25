# Parked: a hosted gallery of every language's demo

Cut from the language-proxy plan (astubbs#242) by user decision on 2026-08-14, which retired the
plan's R74 (`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Parked, not rejected - the
idea was liked; it is just not that plan's to deliver.

**The idea.** The project website hosts a running demo per language, each against the mock so it
boots fast and cannot fail on a broker, with a prettified snippet of that language's client code
beneath the running visual. The point is product, not documentation: the argument for the language
proxy is how little code a client library needs, and a snippet beside a running demo makes that
argument in one screen, in ten languages, to someone who will not read a README.

**Why it is parked.** No hosting substrate exists: the documentation site it would ride on is
itself parked (astubbs#208) with platform and domain undecided, and "a running demo per language"
needs a runtime host, not a static site generator. It would also have been the plan's only
internet-facing deployment, so it needs a recorded security posture - no visitor input reaching a
demo or sidecar process, resource limits per demo, only the web frontend internet-reachable - and
an owner for the compute. None of that belongs inside a plan whose product is the proxy and its
clients.

**What still ships without it.** The per-language demo containers and the shared demo contract
(the plan's R72, R73, R75-R77) land with the plan, mock mode included - so when this is unparked,
the gallery is deployment work over demos that already exist, not demo work.

**Restart trigger.** Whoever unparks astubbs#208 should read this: if that platform can host live
mock-backed demo containers, the gallery is its natural first dynamic content. Failing that, the
plan's review recorded the fallback ladder in descending order of honesty: separate server-side
demo hosting, then in-browser or recorded captures.

**Related (2026-08-20):** [`branch-classic-comparison-demo.md`](branch-classic-comparison-demo.md)
starts the per-language demo containers this file promises "still ship without it" - one per language,
comparison-shaped, no hosting. If this gallery is ever unparked, those are the demos it would host.
