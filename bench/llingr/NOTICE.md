<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Read this before building or running anything in this directory

This directory is **private research**. It is not part of the product, nothing built here ships, and
no number it produces may be published.

## Licence

Parallel Consumer is Apache 2.0. `llingr-demux`, which this arm measures, is **AGPL-3.0 or a
commercial licence, and patent pending**. Three consequences that are not negotiable:

- **Nothing in this directory is vendored.** `go.mod` names the dependency; the AGPL source is
  fetched into the module cache at build time and never enters this repository. Keep it that way -
  do not vendor, do not commit `vendor/`, do not copy code out of the module cache.
- **The binary this builds is a derived work of AGPL code and must not be distributed.** The
  harness writes it to `$BENCH_WORK`, outside the repo, for that reason. Do not publish it, do not
  attach it to a release, do not run it as a network service.
- **This is a separate Go module** from `parallel-consumer-proxy-client-go`, so no shipped artifact
  can pick the dependency up transitively.

## No published comparisons

The owner's decision, recorded in
[`docs/inflight/next-competitor-llingr.md`](../../docs/inflight/next-competitor-llingr.md), is
explicit: **no public comment on llingr** - not in issues, not in docs, not in marketing, not on
social. Results from this arm are for internal research and internal marketing input only.

Read that note before drawing any conclusion from a number here. It also records why throughput is
the *wrong* axis to compete on: the two projects share a processing model, a Go engine will beat a
JVM one, and the differentiators worth pressing are features - auto-scaling, offset encoding past
gaps, transactional produce - not messages per second.
