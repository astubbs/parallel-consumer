// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE OUTPUT IS THE CONTRACT, so it is the thing tested.
//
// parallel-consumer-proxy/demo/README.md fixes the banner's wording, the column set, the column
// order, and the fact that an arm names the client that produced its row - eleven languages must
// print the same shape or `bin/ci-demo-conformance.sh` has nothing to compare. None of that needs a
// broker, an image or a running arm: it is a pure function of an ArmResult, which is why
// demo_report.{h,cpp} exists as a unit at all.
//
// WHAT THIS CANNOT SEE, deliberately: whether the FIGURES are right. That the AK core arm counts
// the keys librdkafka handed it, and the sidecar arm counts the keys the client library handed it,
// is proven by running the demo - these tests prove the table says what the contract requires once
// the arms have reported.
//
// They run under the module's own sixty-line harness and INSIDE THE IMAGE BUILD, like every other
// C++ test here: this host has no C++ toolchain.

#include <chrono>
#include <string>
#include <vector>

#include "demo_report.h"
#include "test_support.h"

namespace {

namespace demo = parallelconsumer::proxy::demo;

demo::ArmResult arm(const std::string& name, double seconds, int processed, int unique_keys) {
    demo::ArmResult result;
    result.arm = name;
    result.elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::duration<double>(seconds));
    result.processed = processed;
    result.unique_keys = unique_keys;
    return result;
}

PCP_TEST(the_banner_names_the_product_first,
         "a reader who runs this is told what they are looking at, not which dial was set") {
    const std::string printed = demo::banner();

    PCP_CHECK_CONTAINS(printed, "PARALLEL CONSUMER");
    PCP_CHECK_CONTAINS(printed, "C++ demo");
    PCP_CHECK_CONTAINS(printed, "The same records, twice: one at a time, then all at once.");
    // The rule the contract draws it with, at the width the contract draws it.
    PCP_CHECK_CONTAINS(printed, std::string(64, '='));
    // The banner is a heading, not a fingerprint: a dial here would be the mistake it replaced.
    PCP_CHECK_ABSENT(printed, "records = ");
}

PCP_TEST(the_banner_says_the_language_rather_than_the_module_directory,
         "the module is parallel-consumer-proxy-client-cpp; the language is C++") {
    PCP_CHECK_ABSENT(demo::banner(), "cpp demo");
}

PCP_TEST(the_table_carries_the_contract_columns_in_order,
         "arm, records, keys, elapsed, msg/s, vs AK core - identity and order are contract") {
    const std::vector<demo::ArmResult> results{arm("AK core (librdkafka)", 2.0, 20, 20)};

    const std::string header = demo::render("Small replay", results, nullptr, false);
    const std::size_t at_arm = header.find("arm");
    const std::size_t at_elapsed = header.find("elapsed");
    const std::size_t at_rate = header.find("msg/s");
    const std::size_t at_records = header.find("records");
    const std::size_t at_keys = header.find("keys");
    const std::size_t at_ratio = header.find("vs AK core");

    PCP_CHECK(at_arm != std::string::npos);
    PCP_CHECK(at_arm < at_records);
    PCP_CHECK(at_records < at_keys);
    PCP_CHECK(at_keys < at_elapsed);
    PCP_CHECK(at_elapsed < at_rate);
    PCP_CHECK(at_rate < at_ratio);
}

PCP_TEST(every_arm_reports_what_it_did_and_not_only_how_fast,
         "records and keys are what demonstrate the run rather than assert it") {
    const std::vector<demo::ArmResult> results{arm("AK core (librdkafka)", 2.0, 2000, 1000),
                                               arm("cpp-grpc (this client)", 0.5, 2000, 1000)};

    const std::string table = demo::render("Small replay", results, &results.front(), false);

    // Both figures, on both rows, with the separators the rate column already used.
    PCP_CHECK_CONTAINS(table, "2,000");
    PCP_CHECK_CONTAINS(table, "1,000");
    // The serial arm is its own baseline, and the parallel arm is four times it at a quarter of the
    // wall clock - the ratio the reader came for.
    PCP_CHECK_CONTAINS(table, "1.0x");
    PCP_CHECK_CONTAINS(table, "4.0x");
}

PCP_TEST(an_arm_names_the_client_that_produced_its_row,
         "'AK core' is a category; a reader cannot judge a comparison without the library") {
    const std::vector<demo::ArmResult> results{arm("AK core (librdkafka)", 2.0, 20, 20),
                                               arm("cpp-grpc (this client)", 1.0, 20, 20)};

    const std::string table = demo::render("Small replay", results, &results.front(), false);

    PCP_CHECK_CONTAINS(table, "AK core (librdkafka)");
    PCP_CHECK_CONTAINS(table, "cpp-grpc (this client)");
}

PCP_TEST(a_cross_replay_ratio_says_so,
         "the big replay's baseline came from the small replay, which is not like-for-like") {
    const std::vector<demo::ArmResult> baseline{arm("AK core (librdkafka)", 2.0, 20, 20)};
    const std::vector<demo::ArmResult> big{arm("cpp-grpc (this client)", 1.0, 40, 40)};

    const std::string same_replay = demo::render("Small replay", baseline, &baseline.front(), false);
    const std::string across = demo::render("Big replay", big, &baseline.front(), true);

    PCP_CHECK_ABSENT(same_replay, "vs AK core*");
    PCP_CHECK_CONTAINS(across, "vs AK core*");
    PCP_CHECK_CONTAINS(across, "not like-for-like");
}

PCP_TEST(no_arm_reports_latency,
         "the backlog is pre-produced, so the workload is closed-loop and a per-record timing lies") {
    const std::vector<demo::ArmResult> results{arm("AK core (librdkafka)", 2.0, 20, 20)};

    const std::string table = demo::render("Small replay", results, &results.front(), false);

    // The same words bin/ci-demo-conformance.sh refuses across every language.
    PCP_CHECK_ABSENT(table, "latency");
    PCP_CHECK_ABSENT(table, "p99");
    PCP_CHECK_ABSENT(table, "percentile");
}

PCP_TEST(a_zero_length_arm_reports_no_throughput_rather_than_a_division,
         "an arm that finished inside the clock's resolution must not print an infinity") {
    const std::vector<demo::ArmResult> results{arm("AK core (librdkafka)", 0.0, 20, 20)};

    PCP_CHECK_EQ(results.front().rate_per_second(), 0.0);
    // A nullptr-rate baseline yields the placeholder rather than a nan.
    const std::string table = demo::render("Small replay", results, &results.front(), false);
    PCP_CHECK_ABSENT(table, "inf");
    PCP_CHECK_ABSENT(table, "nan");
}

PCP_TEST(thousands_separators_are_hand_rolled_because_the_image_locale_is_C,
         "std::locale(\"en_US.UTF-8\") throws in the demo image and nowhere else") {
    PCP_CHECK_EQ(demo::with_thousands(0), std::string("0"));
    PCP_CHECK_EQ(demo::with_thousands(999), std::string("999"));
    PCP_CHECK_EQ(demo::with_thousands(1000), std::string("1,000"));
    PCP_CHECK_EQ(demo::with_thousands(1234567), std::string("1,234,567"));
    PCP_CHECK_EQ(demo::with_thousands(-1234), std::string("-1,234"));
}

}  // namespace
