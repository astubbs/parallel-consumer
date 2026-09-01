#!/bin/sh
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE SIDECAR, AS AN EXECUTABLE THE CLIENT LIBRARY CAN SPAWN. The sidecar is a JVM program, and a
# foreign client library spawns a binary by absolute path - it knows nothing about classpaths. This
# launcher is the whole of the adaptation, and the demo image installs it at
# /app/sidecar/sidecar. Nobody runs it by hand: the client library spawns and supervises it (KTD41),
# which is why it is not a compose service either.
#
# `exec` IS LOAD-BEARING, NOT STYLE. The client holds the write end of this process's stdin and
# never writes to it, so EOF there is the sidecar's proof that its parent is gone (KTD19). A wrapper
# that FORKED the JVM and waited would keep that pipe open in a process the proxy is not watching,
# and a dead application would leave a live JVM holding group membership. Replacing this shell with
# the JVM keeps the pipe attached to the process that reads it.
#
# It takes no arguments of its own and passes on whatever it is given: the proxy REFUSES arguments,
# because every setting - bootstrap servers, credentials, ordering, concurrency, subscription -
# arrives connect-time over the protocol (R39). "$@" is here so that a caller who passes something
# gets the proxy's own refusal rather than a silent discard.
#
# AND THE SIDECAR IS QUIET, FOR THE SAME TWO REASONS THE BROKER IS.
#
# This image gives the sidecar an SLF4J binding, because without one the proxy prints
# `No SLF4J providers were found` and then nothing at all - see demo/Dockerfile. At slf4j-simple's
# default level that binding then emits several hundred lines per run, and two of them matter:
#
#   1. IT BURIES THE TABLES. Measured on a twenty-record run: the overwhelming majority of the
#      demo's own output was the sidecar's routine INFO, including a full `ConsumerConfig values`
#      dump per arm. A demo is a thing someone WATCHES, which is why every compose file here already
#      pins the broker to WARN.
#   2. IT PRINTS THE BROKER ADDRESS. `bootstrap.servers = [...]` appears in those config dumps, and
#      the sidecar's stderr is inherited by the demo's container - so it lands in the demo's own
#      output, which the contract says must never carry that address (own-cluster mode puts a user's
#      real broker there). `bin/ci-demo-conformance.sh` asserts exactly this and would fail on it.
#
# WARN, NOT OFF: the whole point of giving this image a binding was that a misconfigured broker
# should explain itself rather than become an unexplained hang. Warnings and errors still come
# through; only the routine goes. `PC_SIDECAR_LOG_LEVEL=info` turns it back up for a debugging run
# without editing anything.
set -eu

PC_SIDECAR_HOME="${PC_SIDECAR_HOME:-/app/sidecar}"
PC_SIDECAR_LOG_LEVEL="${PC_SIDECAR_LOG_LEVEL:-warn}"

exec java -cp "${PC_SIDECAR_HOME}/lib/*:${PC_SIDECAR_HOME}/classes" \
    "-Dorg.slf4j.simpleLogger.defaultLogLevel=${PC_SIDECAR_LOG_LEVEL}" \
    bz.stub.parallelconsumer.proxy.Main "$@"
