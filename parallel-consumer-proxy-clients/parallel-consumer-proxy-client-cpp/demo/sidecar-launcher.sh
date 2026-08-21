#!/bin/sh
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE SIDECAR, AS AN EXECUTABLE THE CLIENT LIBRARY CAN SPAWN. The sidecar is a JVM program, and a
# foreign client library spawns a binary by absolute path - it knows nothing about classpaths. This
# four-line launcher is the whole of the adaptation, and the demo image installs it at
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
set -eu

PC_SIDECAR_HOME="${PC_SIDECAR_HOME:-/app/sidecar}"

exec java -cp "${PC_SIDECAR_HOME}/lib/*:${PC_SIDECAR_HOME}/classes" \
    bz.stub.parallelconsumer.proxy.Main "$@"
