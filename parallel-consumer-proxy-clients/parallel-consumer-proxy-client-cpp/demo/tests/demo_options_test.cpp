// Copyright (C) 2026 Antony Stubbs and contributors
//
// The demo's option surface, which is the part of it that has actually broken before.
//
// EVERY FAILURE THIS FAN-OUT'S DEMOS HAVE HAD LIVED IN THE ENTRY PATH, not in the arms: the
// no-argument case, the precedence between a flag and an environment variable, a value that
// silently wrapped. They are checked here because they are the only part of the demo that can be
// checked without a broker - the arms need one, and the image build has none.
//
// They run under the module's own sixty-line harness (../../tests/test_main.cpp) rather than a
// second one, and they run INSIDE THE DEMO IMAGE at build time, for the same reason the client
// library's tests do: this host has no C++ toolchain, so a check that only ran on the host would
// only ever run in CI.

#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "demo_options.h"
#include "test_support.h"

namespace {

namespace demo = parallelconsumer::proxy::demo;

/// An environment that is exactly what the test says it is - no more, and nothing inherited from
/// the process running the test.
demo::Environment environment_of(std::map<std::string, std::string> values) {
    return [values = std::move(values)](const std::string& name) -> std::optional<std::string> {
        const auto found = values.find(name);
        return found == values.end() ? std::nullopt : std::optional<std::string>(found->second);
    };
}

demo::Environment empty_environment() { return environment_of({}); }

std::string refusal(const std::vector<std::string>& args, const demo::Environment& environment) {
    try {
        demo::DemoOptions::parse(args, environment);
    } catch (const std::exception& refused) {
        return refused.what();
    }
    return {};
}

PCP_TEST(no_arguments_is_the_documented_defaults,
         "the no-argument run - the double-click case - is the contract's defaults") {
    const demo::DemoOptions options = demo::DemoOptions::parse({}, empty_environment());

    PCP_CHECK_EQ(options.records, 2000);
    PCP_CHECK_EQ(options.delay_ms, 2);
    PCP_CHECK_EQ(options.max_concurrency, 100);
    PCP_CHECK_EQ(options.partitions, 10);
    PCP_CHECK_EQ(options.replay_factor, 20);
    PCP_CHECK(options.bootstrap.empty());
    PCP_CHECK(options.topic.empty());
}

PCP_TEST(flags_beat_the_environment_beats_the_defaults,
         "a flag overrides an environment variable, which overrides the default") {
    const demo::Environment environment = environment_of({{"PC_DEMO_RECORDS", "50"},
                                                          {"PC_DEMO_DELAY_MS", "7"},
                                                          {"PC_DEMO_CONCURRENCY", "9"},
                                                          {"PC_DEMO_PARTITIONS", "3"},
                                                          {"PC_DEMO_REPLAY_FACTOR", "4"},
                                                          {"PC_DEMO_BOOTSTRAP", "broker:9092"},
                                                          {"PC_DEMO_TOPIC", "from-env"}});

    const demo::DemoOptions options = demo::DemoOptions::parse({"--records", "11", "--topic", "from-flag"},
                                                               environment);

    PCP_CHECK_EQ(options.records, 11);              // the flag won
    PCP_CHECK_EQ(options.topic, std::string("from-flag"));
    PCP_CHECK_EQ(options.delay_ms, 7);              // the environment won over the default
    PCP_CHECK_EQ(options.max_concurrency, 9);
    PCP_CHECK_EQ(options.partitions, 3);
    PCP_CHECK_EQ(options.replay_factor, 4);
    PCP_CHECK_EQ(options.bootstrap, std::string("broker:9092"));
}

PCP_TEST(every_flag_has_an_environment_variable,
         "the seven flags and the seven PC_DEMO_ variables are the same seven settings") {
    const demo::DemoOptions options = demo::DemoOptions::parse(
            {"--records", "1", "--delay-ms", "2", "--concurrency", "3", "--partitions", "4",
             "--replay-factor", "5", "--bootstrap", "flagged:9092", "--topic", "flagged"},
            empty_environment());

    PCP_CHECK_EQ(options.records, 1);
    PCP_CHECK_EQ(options.delay_ms, 2);
    PCP_CHECK_EQ(options.max_concurrency, 3);
    PCP_CHECK_EQ(options.partitions, 4);
    PCP_CHECK_EQ(options.replay_factor, 5);
    PCP_CHECK_EQ(options.bootstrap, std::string("flagged:9092"));
    PCP_CHECK_EQ(options.topic, std::string("flagged"));
}

PCP_TEST(an_empty_environment_value_is_unset,
         "a compose file's empty PC_DEMO_TOPIC means 'name your own', not 'the empty topic'") {
    const demo::DemoOptions options =
            demo::DemoOptions::parse({}, environment_of({{"PC_DEMO_TOPIC", ""}, {"PC_DEMO_BOOTSTRAP", "  "}}));

    PCP_CHECK(options.topic.empty());
    PCP_CHECK(options.bootstrap.empty());
}

PCP_TEST(a_misspelled_flag_is_refused,
         "an unknown flag stops the run rather than reporting numbers for settings nobody asked for") {
    PCP_CHECK_CONTAINS(refusal({"--record", "10"}, empty_environment()), "unknown option: --record");
    PCP_CHECK_CONTAINS(refusal({"--records"}, empty_environment()), "--records needs a value");
    PCP_CHECK_CONTAINS(refusal({"--records", "ten"}, empty_environment()), "needs a whole number");
    PCP_CHECK_CONTAINS(refusal({"--records", "0"}, empty_environment()), "must be at least 1");
    PCP_CHECK_CONTAINS(refusal({"--delay-ms", "-1"}, empty_environment()), "must not be negative");
    PCP_CHECK_CONTAINS(refusal({}, environment_of({{"PC_DEMO_RECORDS", "0"}})),
                       "PC_DEMO_RECORDS must be at least 1");
}

PCP_TEST(the_big_replay_cannot_overflow,
         "records times replay-factor is refused rather than wrapped into a confident small number") {
    PCP_CHECK_CONTAINS(refusal({"--records", "2000000", "--replay-factor", "2000"}, empty_environment()),
                       "more records than the demo can count");
}

PCP_TEST(a_replay_factor_of_one_skips_the_big_replay,
         "the big replay runs only when it is bigger than the small one") {
    PCP_CHECK(!demo::DemoOptions::parse({"--replay-factor", "1"}, empty_environment()).big_replay_wanted());
    PCP_CHECK(!demo::DemoOptions::parse({"--replay-factor", "0"}, empty_environment()).big_replay_wanted());
    PCP_CHECK(demo::DemoOptions::parse({"--replay-factor", "2"}, empty_environment()).big_replay_wanted());

    // A skipped big replay still consumes the small replay's own records, never zero.
    PCP_CHECK_EQ(demo::DemoOptions::parse({"--records", "10", "--replay-factor", "0"}, empty_environment())
                         .big_replay_records(),
                 10);
}

PCP_TEST(the_fingerprint_never_prints_the_broker,
         "the effective configuration carries every dial and no address") {
    const demo::DemoOptions options =
            demo::DemoOptions::parse({"--bootstrap", "secret-broker.internal:9092"}, empty_environment());

    const std::string fingerprint = options.describe();

    // Own-cluster mode puts a user's real broker in there, and a demo is bound by the same
    // credential-hygiene rule as the client library.
    PCP_CHECK_ABSENT(fingerprint, "secret-broker.internal");
    PCP_CHECK_CONTAINS(fingerprint, "records = 2000");
    PCP_CHECK_CONTAINS(fingerprint, "delayMs = 2");
    PCP_CHECK_CONTAINS(fingerprint, "maxConcurrency = 100");
    PCP_CHECK_CONTAINS(fingerprint, "partitions = 10");
    PCP_CHECK_CONTAINS(fingerprint, "replayFactor = 20");
}

PCP_TEST(help_is_answered_wherever_it_arrives,
         "--help reaches this binary directly under `docker compose run demo --help`") {
    PCP_CHECK(demo::DemoOptions::help_requested({"--help"}));
    PCP_CHECK(demo::DemoOptions::help_requested({"--records", "10", "-h"}));
    PCP_CHECK(!demo::DemoOptions::help_requested({"--records", "10"}));
    PCP_CHECK_CONTAINS(demo::usage(), "PC_DEMO_DELAY_MS");
}

}  // namespace
