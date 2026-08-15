# Copyright (C) 2026 Antony Stubbs and contributors
#
# Toolchain smoke for the containerised C++ build environment (astubbs#242). Separate from the
# module's own CMakeLists.txt, which the C++ wave owns and which still declares LANGUAGES NONE:
# this project is only ever configured INSIDE the image built by the Dockerfile beside it, where a
# compiler, protoc, grpc_cpp_plugin and the gRPC/protobuf dev packages are all present.
#
# It builds the same source twice on purpose. The static target is what ships; the dynamic one is
# the CONTROL - extracted alongside it, it fails on a host with no libgrpc++.so, which is the
# evidence that the static link is doing the work rather than the host happening to be similar to
# the image. bin/build-client.sh runs both.

cmake_minimum_required(VERSION 3.20)

project(pc-cpp-toolchain-smoke LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# The FROZEN schema, mounted in as a BuildKit named context - never copied into this module, so
# there is exactly one proxy.proto in the tree.
set(PC_PROTO_ROOT "" CACHE PATH "Root of the proto import path (contains parallelconsumer/proxy/v1)")
if(NOT PC_PROTO_ROOT)
    message(FATAL_ERROR "PC_PROTO_ROOT must name the directory containing parallelconsumer/proxy/v1/proxy.proto")
endif()
set(PC_PROTO_FILE "${PC_PROTO_ROOT}/parallelconsumer/proxy/v1/proxy.proto")
if(NOT EXISTS "${PC_PROTO_FILE}")
    message(FATAL_ERROR "No proto at ${PC_PROTO_FILE}")
endif()

find_program(PC_PROTOC protoc REQUIRED)
find_program(PC_GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(PC_GEN_DIR "${CMAKE_BINARY_DIR}/gen")
set(PC_GEN_SOURCES
        "${PC_GEN_DIR}/parallelconsumer/proxy/v1/proxy.pb.cc"
        "${PC_GEN_DIR}/parallelconsumer/proxy/v1/proxy.grpc.pb.cc")

add_custom_command(
        OUTPUT ${PC_GEN_SOURCES}
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${PC_GEN_DIR}"
        COMMAND "${PC_PROTOC}"
        "--proto_path=${PC_PROTO_ROOT}"
        "--cpp_out=${PC_GEN_DIR}"
        "--grpc_out=${PC_GEN_DIR}"
        "--plugin=protoc-gen-grpc=${PC_GRPC_CPP_PLUGIN}"
        "${PC_PROTO_FILE}"
        DEPENDS "${PC_PROTO_FILE}"
        COMMENT "protoc: generating C++ and gRPC stubs from the frozen proxy.proto"
        VERBATIM)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PC_GRPCPP REQUIRED grpc++)
pkg_check_modules(PC_PROTOBUF REQUIRED protobuf)

# pkg-config emits an ORDERED list, and archive order is load-bearing at link time, so it is passed
# through as one verbatim string: CMake de-duplicates repeated -l items in a list and gRPC's
# transitive absl set genuinely repeats them.
# Debian's grpc++.pc under-declares the static case: libre2.a calls into absl's LOG machinery, whose
# archives appear in no .pc Requires line, so a static link fails on absl::log_internal symbols with
# the pkg-config list alone. Every libabsl_*.a in the same directory is added inside a link group -
# unreferenced archives contribute nothing, and the group makes the absl<->re2<->grpc cycle resolve
# without hand-ordering a set that changes with every Debian gRPC bump.
find_library(PC_ABSL_BASE_ARCHIVE NAMES libabsl_base.a REQUIRED)
get_filename_component(PC_ABSL_LIBDIR "${PC_ABSL_BASE_ARCHIVE}" DIRECTORY)
file(GLOB PC_ABSL_ARCHIVES "${PC_ABSL_LIBDIR}/libabsl_*.a")
list(LENGTH PC_ABSL_ARCHIVES PC_ABSL_ARCHIVE_COUNT)
message(STATUS "absl static archives found in ${PC_ABSL_LIBDIR}: ${PC_ABSL_ARCHIVE_COUNT}")
string(REPLACE ";" " " PC_ABSL_ARCHIVE_FLAGS "${PC_ABSL_ARCHIVES}")

string(REPLACE ";" " " PC_GRPCPP_STATIC_FLAGS "${PC_GRPCPP_STATIC_LDFLAGS}")
string(REPLACE ";" " " PC_PROTOBUF_STATIC_FLAGS "${PC_PROTOBUF_STATIC_LDFLAGS}")
string(REPLACE ";" " " PC_GRPCPP_SHARED_FLAGS "${PC_GRPCPP_LDFLAGS}")
string(REPLACE ";" " " PC_PROTOBUF_SHARED_FLAGS "${PC_PROTOBUF_LDFLAGS}")

function(pc_smoke_target name)
    add_executable(${name} "${CMAKE_CURRENT_SOURCE_DIR}/smoke.cpp" ${PC_GEN_SOURCES})
    target_include_directories(${name} PRIVATE "${PC_GEN_DIR}" ${PC_GRPCPP_INCLUDE_DIRS} ${PC_PROTOBUF_INCLUDE_DIRS})
endfunction()

# Static: gRPC, protobuf, absl, OpenSSL, libstdc++ and libgcc all come from archives. glibc stays
# dynamic - a fully static glibc link breaks NSS lookups, and the image's glibc is not the
# constraint anyway, since every artifact consumer runs a glibc at least as new.
pc_smoke_target(pc-cpp-toolchain-smoke)
target_link_options(pc-cpp-toolchain-smoke PRIVATE -static-libstdc++ -static-libgcc)
target_link_libraries(pc-cpp-toolchain-smoke PRIVATE
        "-Wl,-Bstatic -Wl,--start-group ${PC_GRPCPP_STATIC_FLAGS} ${PC_PROTOBUF_STATIC_FLAGS} ${PC_ABSL_ARCHIVE_FLAGS} -Wl,--end-group -Wl,-Bdynamic")

# Dynamic control - expected to FAIL on a host without the gRPC runtime installed.
pc_smoke_target(pc-cpp-toolchain-smoke-dynamic)
target_link_libraries(pc-cpp-toolchain-smoke-dynamic PRIVATE
        "${PC_GRPCPP_SHARED_FLAGS} ${PC_PROTOBUF_SHARED_FLAGS}")
