#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Regenerates the Ruby protobuf/gRPC stubs under lib/parallelconsumer/ from the FROZEN schema at
# parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto.
#
# The generated code is COMMITTED. Ruby has no codegen step at `bundle install` time, so a
# consumer of this library must find the stubs already there - the same reason the Go client
# commits its own. Re-running this script on an unchanged .proto must leave `git status` clean;
# that is the regeneration check, and it is run by hand in wave one.
#
# TOOLCHAIN: grpc-tools, pinned in the Gemfile at the same version as the runtime `grpc` gem. It
# ships its own protoc AND the Ruby gRPC plugin, so the generators are pinned by the same lockfile
# as everything else and nothing is taken from PATH. `grpc_tools_ruby_protoc` additionally puts
# protobuf's well-known types (duration.proto, timestamp.proto) on the include path for us, which
# is the whole reason to prefer it over a bare protoc.
#
# THE FROZEN SCHEMA ALREADY CARRIES `option ruby_package = "Bz::Stub::ParallelConsumer::Proxy::V1"`,
# so - unlike Go, whose author had to supply the mapping on the command line - there is nothing to
# override here. The generated FILE paths still follow the .proto's own path, which is why the
# stubs land under lib/parallelconsumer/proxy/v1/ while the CONSTANTS live under Bz::Stub::*.

set -euo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$MODULE_DIR" && git rev-parse --show-toplevel)"
PROTO_ROOT="$REPO_ROOT/parallel-consumer-proxy-protocol/src/main/proto"
PROTO_FILE="parallelconsumer/proxy/v1/proxy.proto"

cd "$MODULE_DIR"

rm -rf lib/parallelconsumer
mkdir -p lib/parallelconsumer

bundle exec grpc_tools_ruby_protoc \
    -I "$PROTO_ROOT" \
    --ruby_out=lib \
    --grpc_out=lib \
    "$PROTO_FILE"

echo "generate-proto: wrote $(git ls-files --others --cached lib/parallelconsumer | wc -l) file(s) under lib/parallelconsumer/"
