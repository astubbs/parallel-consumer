# Copyright (C) 2026 Antony Stubbs and contributors
#
# The C++ demo (astubbs#242, plan unit U35). Like the module's own CMake project beside it, this is
# only ever configured INSIDE an image - here demo/Dockerfile - because gRPC, protobuf and
# librdkafka arrive as system DEV PACKAGES rather than as a versioned toolchain.
#
# A PROJECT OF ITS OWN RATHER THAN A TARGET IN ../CMakeLists.txt, and the reason is the build
# environment rather than taste: the demo needs librdkafka, which the client library does not, and
# adding it to the module's project would put a Kafka client into the image that `bin/build-client.sh
# cpp` builds and into the artifact-portability claim that image makes. The demo is a RUNTIME image;
# that one is a BUILD environment that exports artifacts and runs nothing. Keeping the projects
# separate keeps that distinction true.
#
# TWO TARGETS:
#   pc-cpp-demo        the demo itself - both arms, the two tables
#   pc-cpp-demo-tests  the entry path and the OUTPUT SHAPE, run by ctest inside the image build
#                      (see tests/) - the two parts of the demo that need no broker

cmake_minimum_required(VERSION 3.20)

project(parallel-consumer-proxy-client-cpp-demo LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

enable_testing()

set(PC_MODULE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/..")

# ── The FROZEN schema, mounted in as a BuildKit named context ────────────────────────────────────
# Identical in shape to the module's own: never copied into this module, and the placement comes
# from the .proto's own options rather than from this command line.
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

# ── Dependencies ─────────────────────────────────────────────────────────────────────────────────
# DYNAMICALLY LINKED, unlike the module's static artifacts, and that is the runtime image's whole
# point: nothing here is extracted to a host, so there is no portability claim to prove and no
# reason to pay for the static link's absl archive group. The demo runs where it was built.
find_package(PkgConfig REQUIRED)
pkg_check_modules(PC_GRPCPP REQUIRED grpc++)
pkg_check_modules(PC_PROTOBUF REQUIRED protobuf)
# librdkafka: C++'s own Kafka client, and the AK core arm's whole reason to exist. The C API, not
# rdkafka++ - see demo/src/demo_broker.h.
pkg_check_modules(PC_RDKAFKA REQUIRED rdkafka)

# ── The client library, compiled from the module beside this one ─────────────────────────────────
# GLOBBED rather than listed, so the demo cannot fall out of step with a library that grows a source
# file: a stale list would fail at LINK time with an undefined symbol, days later and far from the
# cause. The two `main`s next door are removed by name; a third would announce itself immediately as
# a duplicate main rather than silently.
file(GLOB PC_CLIENT_SOURCES "${PC_MODULE_ROOT}/src/*.cpp")
list(REMOVE_ITEM PC_CLIENT_SOURCES
        "${PC_MODULE_ROOT}/src/selftest.cpp"
        "${PC_MODULE_ROOT}/src/conformance_runner.cpp")
list(LENGTH PC_CLIENT_SOURCES PC_CLIENT_SOURCE_COUNT)
if(PC_CLIENT_SOURCE_COUNT LESS 1)
    message(FATAL_ERROR "No client library sources found under ${PC_MODULE_ROOT}/src")
endif()

set(PC_INCLUDE_DIRS
        "${PC_MODULE_ROOT}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${PC_GEN_DIR}"
        ${PC_GRPCPP_INCLUDE_DIRS}
        ${PC_PROTOBUF_INCLUDE_DIRS}
        ${PC_RDKAFKA_INCLUDE_DIRS})

add_executable(pc-cpp-demo
        "${CMAKE_CURRENT_SOURCE_DIR}/src/demo.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/demo_broker.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/demo_options.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/demo_report.cpp"
        ${PC_CLIENT_SOURCES}
        ${PC_GEN_SOURCES})
target_include_directories(pc-cpp-demo PRIVATE ${PC_INCLUDE_DIRS})
target_compile_options(pc-cpp-demo PRIVATE -Wall -Wextra -Werror)
# Generated code is not this module's to keep warning-clean, and -Werror over it would make a Debian
# protoc bump break the build for a reason nobody here can fix.
set_source_files_properties(${PC_GEN_SOURCES} PROPERTIES COMPILE_OPTIONS "-w")
target_link_libraries(pc-cpp-demo PRIVATE
        Threads::Threads
        ${PC_GRPCPP_LDFLAGS}
        ${PC_PROTOBUF_LDFLAGS}
        ${PC_RDKAFKA_LDFLAGS})

# ── The demo's own tests ─────────────────────────────────────────────────────────────────────────
# The module's harness, not a second one: tests/test_main.cpp next door is the runner, and
# test_support.h is the sixty lines of macros. Only demo_options.cpp and demo_report.cpp are linked
# in - the two units with no broker in them - so these tests need neither gRPC nor Kafka and run in
# a second inside the image build. demo.cpp and demo_broker.cpp are deliberately absent: an arm
# cannot be tested without a broker, whereas the contract's rules about the OUTPUT can be.
add_executable(pc-cpp-demo-tests
        "${PC_MODULE_ROOT}/tests/test_main.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/demo_options_test.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/demo_report_test.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/demo_options.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/demo_report.cpp")
target_include_directories(pc-cpp-demo-tests PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${PC_MODULE_ROOT}/tests")
target_compile_options(pc-cpp-demo-tests PRIVATE -Wall -Wextra -Werror)
target_link_libraries(pc-cpp-demo-tests PRIVATE Threads::Threads)

add_test(NAME demo-entry-and-output COMMAND pc-cpp-demo-tests)
