# Next: let people write Kafka Connect connectors in a foreign language, on top of PC

Product idea surfaced during the language-proxy ideation (astubbs#242). Recorded so it is not lost;
deliberately not leaned on while the proxy's own interaction model is still being settled.

## The idea

Kafka Connect is config-driven **for users** but Java **for connector developers** — a sink connector
is a JAR you deploy and configure, and there is no per-record user function crossing a language
boundary. So Connect has no polyglot seam of its own to wrap.

The inverse is the interesting part. A PC-backed sink connector that delegates its record handling
across the same boundary the language proxy is building would let people **write connectors** in
Python or Go. That is a different audience from PC's users, and plausibly a larger one: connector
authors are a much bigger population than people embedding a consumer library, and "you must write
Java" is the standing barrier.

If the proxy's client wrapper is the layer, then a Connect connector is another thing that can sit on
top of it — the same wrapper serving a second product surface, rather than a second integration to
build and maintain.

## Why it is not being acted on now

The proxy's own interaction model is unsettled, and this would widen the surface before the narrow
case works. It also inherits every open question there — worker lifecycle, fencing, failure
semantics — so it cannot be designed ahead of them.

## What to check before leaning on it

`feats/connect-on-pc-spike` exists in this repo and has not been read against this idea. It may
already assume a shape that helps or conflicts. Read it before treating any of the above as
available.
