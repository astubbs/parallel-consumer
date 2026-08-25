// Copyright (C) 2026 Antony Stubbs and contributors
//
// The sixty-line harness. See test_main.cpp for why it is not a framework.

#ifndef PARALLELCONSUMER_PROXY_TEST_SUPPORT_H
#define PARALLELCONSUMER_PROXY_TEST_SUPPORT_H

#include <functional>
#include <sstream>
#include <string>
#include <vector>

namespace pcp_test {

struct Test {
    std::string name;
    std::function<void()> body;
};

std::vector<Test>& registry();
void fail(const std::string& file, int line, const std::string& detail);

struct Registrar {
    Registrar(const std::string& name, std::function<void()> body) {
        registry().push_back(Test{name, std::move(body)});
    }
};

template <typename T>
std::string render(const T& value) {
    std::ostringstream out;
    out << value;
    return out.str();
}

}  // namespace pcp_test

/// Declares a test. The name is a sentence, because a failing test's name is the first thing anybody
/// reads about it.
#define PCP_TEST(identifier, sentence)                                       \
    static void identifier();                                                \
    static const pcp_test::Registrar identifier##_registrar(sentence, identifier); \
    static void identifier()

#define PCP_CHECK(condition)                                                              \
    do {                                                                                  \
        if (!(condition)) {                                                               \
            pcp_test::fail(__FILE__, __LINE__, "expected " #condition);                   \
        }                                                                                 \
    } while (false)

#define PCP_CHECK_EQ(actual, expected)                                                              \
    do {                                                                                            \
        const auto& pcp_actual = (actual);                                                          \
        const auto& pcp_expected = (expected);                                                      \
        if (!(pcp_actual == pcp_expected)) {                                                        \
            pcp_test::fail(__FILE__, __LINE__,                                                      \
                           std::string(#actual) + " was " + pcp_test::render(pcp_actual) +          \
                               ", expected " + pcp_test::render(pcp_expected));                     \
        }                                                                                           \
    } while (false)

#define PCP_CHECK_CONTAINS(haystack, needle)                                                        \
    do {                                                                                            \
        const std::string pcp_haystack = (haystack);                                                \
        const std::string pcp_needle = (needle);                                                    \
        if (pcp_haystack.find(pcp_needle) == std::string::npos) {                                   \
            pcp_test::fail(__FILE__, __LINE__,                                                      \
                           std::string(#haystack) + " does not contain '" + pcp_needle + "': " +    \
                               pcp_haystack);                                                       \
        }                                                                                           \
    } while (false)

#define PCP_CHECK_ABSENT(haystack, needle)                                                          \
    do {                                                                                            \
        const std::string pcp_haystack = (haystack);                                                \
        const std::string pcp_needle = (needle);                                                    \
        if (pcp_haystack.find(pcp_needle) != std::string::npos) {                                   \
            pcp_test::fail(__FILE__, __LINE__,                                                      \
                           std::string(#haystack) + " must not contain '" + pcp_needle + "': " +    \
                               pcp_haystack);                                                       \
        }                                                                                           \
    } while (false)

#endif  // PARALLELCONSUMER_PROXY_TEST_SUPPORT_H
