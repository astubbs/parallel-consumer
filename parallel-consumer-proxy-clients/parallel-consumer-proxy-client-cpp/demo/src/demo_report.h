// Copyright (C) 2026 Antony Stubbs and contributors
//
// WHAT A READER ACTUALLY SEES, which the contract treats as the demo's product rather than its
// packaging: "a demo that is correct and unreadable has failed at the only thing it does"
// (parallel-consumer-proxy/demo/README.md, "The output a reader actually sees").
//
// SEPARATED FROM demo.cpp SO IT CAN BE TESTED, and for no other reason. The banner's wording, the
// column set and their order are contract - eleven languages must print the same shape - and the
// arms that produce the figures need a broker, which the image build has none of. Rendering does
// not, so it is a pure function of an ArmResult here and tests/demo_report_test.cpp holds it to the
// contract inside the image build. That is the same split demo_options made, for the same reason.
//
// THESE FUNCTIONS RETURN STRINGS RATHER THAN PRINTING. A renderer that writes to std::cout can only
// be tested by capturing a stream, and the thing worth asserting is the text.

#ifndef PARALLELCONSUMER_PROXY_DEMO_REPORT_H
#define PARALLELCONSUMER_PROXY_DEMO_REPORT_H

#include <chrono>
#include <string>
#include <vector>

namespace parallelconsumer::proxy::demo {

/// What one arm achieved: how long it took, over how many records, and across how many keys.
///
/// THE LAST TWO ARE DETERMINISTIC AND THE FIRST IS NOT, which is the whole point of carrying them.
/// Elapsed and msg/s depend on the machine, so no two languages can ever be compared on them;
/// records and keys are a property of the backlog, so every language processing the same records
/// must report the same pair. That is what makes `bin/ci-demo-conformance.sh` able to compare
/// languages at all, and what turns the table from an assertion that work happened into a
/// demonstration of it.
struct ArmResult {
    std::string arm;
    std::chrono::nanoseconds elapsed{0};

    /// Records this arm's user function ran on. Must equal the target: a short arm is a failed arm,
    /// not a fast one.
    int processed = 0;

    /// Distinct record keys this arm observed, which is what shows the backlog was really spread
    /// rather than one key repeated.
    int unique_keys = 0;

    /// Throughput, which is the only *timing* figure this demo reports - no latency, ever, because
    /// the backlog is pre-produced and the workload is therefore closed-loop.
    [[nodiscard]] double rate_per_second() const;
};

/// The language this demo is for, as it appears in the banner. C++, not "cpp": the banner is for a
/// human, and the module directory's name is not what the language is called.
inline constexpr const char* kLanguage = "C++";

/// The first thing the demo prints, and the same in every language but its name.
///
/// A READER WHO RUNS THIS MUST BE TOLD WHAT THEY ARE LOOKING AT. Before this existed the first line
/// out of the demo was a configuration dial, which names neither the product nor the point - the
/// contract calls that out by name as having told the reader nothing.
[[nodiscard]] std::string banner();

/// One results table: the title, then a row per arm.
///
/// @param baseline the arm every ratio is measured against, or nullptr for no ratio column content
/// @param across_replays whether the baseline came from the OTHER replay, which makes the ratio not
///        like-for-like and must be said rather than left for the reader to notice
[[nodiscard]] std::string render(const std::string& title, const std::vector<ArmResult>& results,
                                 const ArmResult* baseline, bool across_replays);

/// A whole number with thousands separators, hand-rolled rather than taken from a locale: the demo
/// image is a slim one and its locale is C, so `std::locale("en_US.UTF-8")` would throw there and
/// nowhere else.
[[nodiscard]] std::string with_thousands(long value);

}  // namespace parallelconsumer::proxy::demo

#endif  // PARALLELCONSUMER_PROXY_DEMO_REPORT_H
