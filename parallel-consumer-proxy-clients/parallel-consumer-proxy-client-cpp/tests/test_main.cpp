// Copyright (C) 2026 Antony Stubbs and contributors
//
// The module's own tests: what a live session cannot reach cheaply, or at all.
//
// THEY DO NOT REPLACE THE SHARED CONFORMANCE SUITE and the suite does not replace them. The suite
// proves every client behaves identically on the protocol, from outside the process; these catch
// what is invisible from out there - the in-flight ceiling counting the wrong thing, a credential in
// a rendering, a port line missed among the harness's log chatter. Every one of those has been a
// real defect in this fan-out.
//
// A HAND-ROLLED HARNESS, deliberately. Debian's googletest ships as sources that each image would
// have to compile, and a client library this size does not need a framework to say "this value was
// wrong". The whole of it is the sixty lines below, and it fails loudly rather than counting
// silently.

#include <exception>
#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include "test_support.h"

namespace pcp_test {

std::vector<Test>& registry() {
    static std::vector<Test> tests;
    return tests;
}

int failures = 0;

void fail(const std::string& file, int line, const std::string& detail) {
    ++failures;
    std::cerr << "  FAILED " << file << ":" << line << " - " << detail << '\n';
}

}  // namespace pcp_test

int main() {
    int failed_tests = 0;
    for (const auto& test : pcp_test::registry()) {
        const int before = pcp_test::failures;
        std::cout << "- " << test.name << '\n';
        try {
            test.body();
        } catch (const std::exception& thrown) {
            pcp_test::fail(test.name, 0, std::string("threw: ") + thrown.what());
        } catch (...) {
            pcp_test::fail(test.name, 0, "threw a non-standard exception");
        }
        if (pcp_test::failures > before) {
            ++failed_tests;
        }
    }
    std::cout << pcp_test::registry().size() << " tests, " << failed_tests << " failed\n";
    return failed_tests == 0 ? 0 : 1;
}
