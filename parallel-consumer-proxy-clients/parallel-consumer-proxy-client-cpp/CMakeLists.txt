# Copyright (C) 2026 Antony Stubbs and contributors
#
# The C++ proxy client (astubbs#242). This project is only ever configured INSIDE the image built by
# the Dockerfile beside it: gRPC and protobuf arrive as system DEV PACKAGES rather than as a
# versioned toolchain, which is the whole reason C++ is one of the two languages mise cannot serve
# here (docs/inflight/parked-containerised-toolchains-and-runtime.md).
#
# It replaced a `LANGUAGES NONE` skeleton and a separate toolchain-smoke/ project. The smoke's own
# header said to delete it "once a real target proves the same three things" - generates, links,
# runs off-image - and pc-cpp-selftest below proves all three AND that the client library links,
# which the smoke could not say because it had no library to link.
#
# FOUR TARGETS, and the pairing matters:
#   pc-cpp-selftest            statically linked; extracted and RUN ON THE HOST
#   pc-cpp-selftest-dynamic    the same source, dynamically linked - the CONTROL, expected to FAIL
#                              on a host with no libgrpc++.so. A run where both work means the host
#                              happens to look like the image and the static link proved nothing.
#   pc-cpp-conformance-runner  this language's half of the shared conformance suite
#   pc-cpp-tests               the module's own tests, run by ctest inside the image

cmake_minimum_required(VERSION 3.20)

project(parallel-consumer-proxy-client-cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

enable_testing()

# ── The FROZEN schema, mounted in as a BuildKit named context ────────────────────────────────────
# Never copied into this module, so there is exactly one proxy.proto in the tree. Placement comes
# from the .proto's own options and never from this command line.
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

# ── Linking, static and dynamic ──────────────────────────────────────────────────────────────────
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

# ── The library ──────────────────────────────────────────────────────────────────────────────────
# An OBJECT library so the generated protobuf translation units - much the largest thing here - are
# compiled ONCE and linked into four executables, two of which differ only in how they link.
set(PC_CLIENT_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/src/client.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/dispatch_queue.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/options.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/record.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/session.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/sidecar.cpp")

set(PC_INCLUDE_DIRS
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${PC_GEN_DIR}"
        ${PC_GRPCPP_INCLUDE_DIRS}
        ${PC_PROTOBUF_INCLUDE_DIRS})

add_library(pc-proxy-client-objects OBJECT ${PC_GEN_SOURCES} ${PC_CLIENT_SOURCES})
target_include_directories(pc-proxy-client-objects PRIVATE ${PC_INCLUDE_DIRS})
target_compile_options(pc-proxy-client-objects PRIVATE -Wall -Wextra -Werror)
# Generated code is not this module's to keep warning-clean, and -Werror over it would make a Debian
# protoc bump break the build for a reason nobody here can fix. Source properties are appended after
# the target's options, so this wins.
set_source_files_properties(${PC_GEN_SOURCES} PROPERTIES COMPILE_OPTIONS "-w")

function(pc_client_executable name)
    add_executable(${name} ${ARGN} $<TARGET_OBJECTS:pc-proxy-client-objects>)
    target_include_directories(${name} PRIVATE ${PC_INCLUDE_DIRS} "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    target_compile_options(${name} PRIVATE -Wall -Wextra -Werror)
    target_link_libraries(${name} PRIVATE Threads::Threads)
endfunction()

function(pc_link_static name)
    # gRPC, protobuf, absl, OpenSSL, libstdc++ and libgcc all come from archives. glibc stays dynamic
    # - a fully static glibc link breaks NSS lookups, and the image's glibc is not the constraint
    # anyway, since every artifact consumer runs a glibc at least as new.
    target_link_options(${name} PRIVATE -static-libstdc++ -static-libgcc)
    target_link_libraries(${name} PRIVATE
            "-Wl,-Bstatic -Wl,--start-group ${PC_GRPCPP_STATIC_FLAGS} ${PC_PROTOBUF_STATIC_FLAGS} ${PC_ABSL_ARCHIVE_FLAGS} -Wl,--end-group -Wl,-Bdynamic")
endfunction()

pc_client_executable(pc-cpp-selftest "${CMAKE_CURRENT_SOURCE_DIR}/src/selftest.cpp")
pc_link_static(pc-cpp-selftest)

# The CONTROL. Expected to FAIL on a host without the gRPC runtime installed - see the file header.
pc_client_executable(pc-cpp-selftest-dynamic "${CMAKE_CURRENT_SOURCE_DIR}/src/selftest.cpp")
target_link_libraries(pc-cpp-selftest-dynamic PRIVATE
        "${PC_GRPCPP_SHARED_FLAGS} ${PC_PROTOBUF_SHARED_FLAGS}")

pc_client_executable(pc-cpp-conformance-runner "${CMAKE_CURRENT_SOURCE_DIR}/src/conformance_runner.cpp")
pc_link_static(pc-cpp-conformance-runner)

pc_client_executable(pc-cpp-tests
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_main.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/dispatch_queue_test.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/options_test.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/sidecar_test.cpp")
pc_link_static(pc-cpp-tests)

add_test(NAME unit COMMAND pc-cpp-tests)
