// Copyright (C) 2026 Antony Stubbs and contributors

#include "demo_report.h"

#include <cstdio>
#include <string>
#include <vector>

namespace parallelconsumer::proxy::demo {
namespace {

/// The banner's rule, the width the contract prints it at.
constexpr int kRuleWidth = 64;

/// The arm column. Wide enough for the longest label this language has - a label now carries the
/// library that produced the row, so "cpp-grpc (this client)" is the size to fit.
///
/// COLUMN WIDTH IS DELIBERATELY NOT CONTRACT, and bin/ci-demo-conformance.sh says so: a language
/// with a longer arm name would otherwise be in permanent violation of an alignment rule. Column
/// IDENTITY and ORDER are what every language must match.
constexpr const char* kHeaderFormat = "  %-24s %10s %8s %10s %12s %12s\n";
constexpr const char* kRowFormat = "  %-24s %10s %8s %9.1fs %12s %12s\n";

}  // namespace

double ArmResult::rate_per_second() const {
    const double seconds = std::chrono::duration<double>(elapsed).count();
    return seconds > 0 ? processed / seconds : 0;
}

std::string with_thousands(long value) {
    std::string digits = std::to_string(value);
    // A negative number would otherwise have a separator inserted after its sign; nothing here
    // produces one, but a renderer that mangles input it was not expecting is a bug waiting for a
    // caller.
    const std::size_t floor = digits.empty() || digits.front() != '-' ? 0 : 1;
    for (std::size_t at = digits.size(); at > floor + 3;) {
        at -= 3;
        digits.insert(at, ",");
    }
    return digits;
}

std::string banner() {
    const std::string rule(kRuleWidth, '=');
    return "\n" + rule + "\n  PARALLEL CONSUMER  -  " + kLanguage + " demo\n"
           + "  The same records, twice: one at a time, then all at once.\n" + rule + "\n";
}

std::string render(const std::string& title, const std::vector<ArmResult>& results, const ArmResult* baseline,
                   bool across_replays) {
    char line[512];
    std::string table = "\n\n" + title + "\n";
    std::snprintf(line, sizeof(line), kHeaderFormat, "arm", "records", "keys", "elapsed", "msg/s",
                  across_replays ? "vs AK core*" : "vs AK core");
    table += line;
    for (const ArmResult& result : results) {
        std::string ratio = "-";
        if (baseline != nullptr && baseline->rate_per_second() > 0) {
            char rendered[32];
            std::snprintf(rendered, sizeof(rendered), "%.1fx",
                          result.rate_per_second() / baseline->rate_per_second());
            ratio = rendered;
        }
        std::snprintf(line, sizeof(line), kRowFormat, result.arm.c_str(),
                      with_thousands(result.processed).c_str(),
                      with_thousands(result.unique_keys).c_str(),
                      std::chrono::duration<double>(result.elapsed).count(),
                      with_thousands(static_cast<long>(result.rate_per_second())).c_str(),
                      ratio.c_str());
        table += line;
    }
    if (across_replays) {
        table += "\n  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.\n";
    }
    return table;
}

}  // namespace parallelconsumer::proxy::demo
