#!/bin/sh
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE SIDECAR, as a foreign client's client library expects to find it: one absolute path, executed
# directly, that announces `port: <n>` on stdout and holds its stdin as the parent-death lifeline.
#
# `exec` IS LOAD-BEARING, NOT TIDINESS. The client library spawns this file directly and keeps the
# write end of its stdin open for the child's whole life - closing that pipe is how it reaps the
# sidecar. Without `exec` this shell would stay in the middle, holding stdin itself, and the JVM
# would survive the reap still holding its consumer-group membership.
#
# The classpath is a wildcard rather than a list because the demo image puts exactly the proxy's
# runtime graph in that directory and nothing else - see demo/Dockerfile's `sidecar` stage. It is
# single-quoted so the SHELL does not expand it; the JVM does.
#
# NOTHING HERE TAKES CONFIGURATION. The sidecar's Kafka properties and every other setting reach it
# over the protocol stream, from the client library, and never by argv, environment or file (R39).
# The arguments forwarded below exist because the library may pass fixture selection to a test
# harness sidecar; a shipped one is configured by nothing on this line.
exec java -cp '/opt/parallel-consumer/lib/*' bz.stub.parallelconsumer.proxy.Main "$@"
