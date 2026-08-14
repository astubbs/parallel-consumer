# Copyright (C) 2026 Antony Stubbs and contributors
#
# Skeleton, seeded by the language-proxy plan (astubbs#242). The C++ wave owns this file and
# everything else in this directory; the pom.xml beside it wraps this project into the Maven
# reactor under -Dpc.foreignClients. LANGUAGES NONE keeps the skeleton configurable on a machine
# with no C++ compiler; the wave that starts this module declares CXX and its targets.

cmake_minimum_required(VERSION 3.20)

project(parallel-consumer-proxy-client-cpp LANGUAGES NONE)

enable_testing()
