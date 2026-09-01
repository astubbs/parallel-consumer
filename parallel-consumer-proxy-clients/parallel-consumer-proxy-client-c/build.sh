#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Builds the C reach probe: Parallel Consumer, consumed from C, with no gRPC and no JVM.
#
# Cross-platform across macOS and Linux; anything it cannot find is named rather than worked around.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
PROTO_DIR="$REPO_ROOT/parallel-consumer-proxy-protocol/src/main/proto"

case "$(uname -s)" in
    Darwin) LIB_EXT="dylib" ;;
    Linux)  LIB_EXT="so" ;;
    *) echo "unsupported platform $(uname -s): this script knows macOS and Linux" >&2; exit 1 ;;
esac

# nanopb, NOT protobuf-c. protobuf-c rejects proto3 `optional`, which this protocol uses in 42
# places, so it cannot compile the wire format at all - see this directory's README.
NANOPB_PREFIX="${NANOPB_PREFIX:-$(brew --prefix nanopb 2>/dev/null || echo /usr/local)}"
if [ ! -f "$NANOPB_PREFIX/include/nanopb/pb_encode.h" ]; then
    echo "no nanopb runtime at $NANOPB_PREFIX. Install it (brew install nanopb, or your distro's" >&2
    echo "libprotobuf-nanopb-dev) or set NANOPB_PREFIX." >&2
    exit 1
fi
command -v protoc >/dev/null || { echo "protoc is not on PATH" >&2; exit 1; }
command -v protoc-gen-nanopb >/dev/null || {
    echo "protoc-gen-nanopb is not on PATH (brew install nanopb ships it)" >&2; exit 1; }

LIBPC_DIR="${PC_EMBEDDED_LIBDIR:-$REPO_ROOT/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/ffi/build}"
if [ ! -f "$LIBPC_DIR/libpc.$LIB_EXT" ]; then
    echo "no embedded engine at $LIBPC_DIR/libpc.$LIB_EXT. Build it with:" >&2
    echo "  $REPO_ROOT/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/ffi/build-shared-library.sh session" >&2
    echo "or set PC_EMBEDDED_LIBDIR." >&2
    exit 1
fi

echo "==> generating nanopb bindings"
rm -rf "$HERE/gen"; mkdir -p "$HERE/gen"
PROTOBUF_INCLUDE="$(brew --prefix protobuf 2>/dev/null || echo /usr/local)/include"
# The well-known types have to be generated too. nanopb ships no pre-generated Duration or
# Timestamp, unlike the C++ and Java runtimes where they arrive with the library.
# TWO INVOCATIONS, and the split is not cosmetic. Generating the well-known types in the same pass
# makes the generator report every pattern in proxy.options as "did not match any fields" - true of
# duration.proto, and alarming to read, since it is exactly what a genuinely broken options file
# says. The bounds are applied either way; the warning is not.
protoc -I"$PROTO_DIR" -I"$PROTOBUF_INCLUDE" -I"$HERE" \
    --nanopb_out="--options-file=$HERE/proxy.options:$HERE/gen" \
    "$PROTO_DIR/parallelconsumer/proxy/v1/proxy.proto"
protoc -I"$PROTOBUF_INCLUDE" --nanopb_out="$HERE/gen" \
    "$PROTOBUF_INCLUDE/google/protobuf/duration.proto" \
    "$PROTOBUF_INCLUDE/google/protobuf/timestamp.proto"

echo "==> compiling"
mkdir -p "$HERE/build"
cc -std=c11 -O2 -Wall -Wextra \
    -I"$HERE/gen" -I"$NANOPB_PREFIX/include" -I"$NANOPB_PREFIX/include/nanopb" \
    -o "$HERE/build/pc-c-probe" \
    "$HERE/src/pc_client.c" "$HERE/gen/parallelconsumer/proxy/v1/proxy.pb.c" \
    "$HERE/gen/google/protobuf/duration.pb.c" "$HERE/gen/google/protobuf/timestamp.pb.c" \
    -L"$NANOPB_PREFIX/lib" -lprotobuf-nanopb \
    -L"$LIBPC_DIR" -lpc -Wl,-rpath,"$LIBPC_DIR" -Wl,-rpath,"$NANOPB_PREFIX/lib"

echo "==> built $HERE/build/pc-c-probe"
