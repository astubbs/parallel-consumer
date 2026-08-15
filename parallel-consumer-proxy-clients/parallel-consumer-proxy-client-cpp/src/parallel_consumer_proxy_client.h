// Copyright (C) 2026 Antony Stubbs and contributors
//
// The one header an application includes. Everything below it is this library's own layering.
//
//   application process
//   |- the user's function (a lambda or callable - the proxy never learns what it is)
//   |- this library
//   |  |- transport  - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue
//   |  \- executors  - std::thread workers, each: take record -> run the function -> report
//   \- sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
//
// STATELESS PER RECORD. The fencing token rides from dispatch to report on the executor's own call
// stack and is echoed back byte-identically; there is no request map, no dedupe cache and no
// completion registry, because a client that holds no per-record state cannot have a per-record
// state bug. Fencing is the proxy's job.
//
// WAVE ONE. Implemented: connect, Configure, a Dispatch wave, the user's function, the report, and
// a clean client-initiated shutdown. Not implemented, and therefore NOT DECLARED in the handshake
// (see implemented_capabilities): heartbeats and the liveness lease, the manifest reconnect,
// worker-death reporting, terminal outcomes, and the proxy-initiated shutdown drain.

#ifndef PARALLELCONSUMER_PROXY_CLIENT_LIBRARY_H
#define PARALLELCONSUMER_PROXY_CLIENT_LIBRARY_H

#include "client.h"
#include "error.h"
#include "logging.h"
#include "options.h"
#include "outcome.h"
#include "record.h"
#include "session.h"

#endif  // PARALLELCONSUMER_PROXY_CLIENT_LIBRARY_H
