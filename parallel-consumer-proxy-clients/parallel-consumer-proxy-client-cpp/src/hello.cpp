// Copyright (C) 2026 Antony Stubbs and contributors

// Prints the one line bin/foreign-client-step.sh checks for. The C++ end of the polyglot build
// scaffolding (astubbs#242); the real client, with its CMake build and its container, replaces it
// when the C++ wave lands - see this module's pom for why neither is here yet.

#include <iostream>

int main() {
    std::cout << "parallel-consumer-proxy-client hello fixture: cpp";
    return 0;
}
