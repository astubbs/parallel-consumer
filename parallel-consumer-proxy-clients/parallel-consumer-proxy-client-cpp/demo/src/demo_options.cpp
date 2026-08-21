// Copyright (C) 2026 Antony Stubbs and contributors

#include "demo_options.h"

#include <cstdlib>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace parallelconsumer::proxy::demo {
namespace {

/// Trims the whitespace an environment variable set from a compose file routinely carries. An
/// empty value after trimming is treated as unset, so `PC_DEMO_TOPIC=` in a compose file means
/// "the demo names its own" rather than "the empty topic".
std::string trim(const std::string& raw) {
    const std::size_t first = raw.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return {};
    }
    const std::size_t last = raw.find_last_not_of(" \t\r\n");
    return raw.substr(first, last - first + 1);
}

int number(const std::string& source, const std::string& raw) {
    const std::string text = trim(raw);
    try {
        std::size_t consumed = 0;
        const long parsed = std::stol(text, &consumed);
        if (consumed != text.size() || parsed > std::numeric_limits<int>::max()
            || parsed < std::numeric_limits<int>::min()) {
            throw std::invalid_argument("out of range");
        }
        return static_cast<int>(parsed);
    } catch (const std::exception&) {
        throw std::invalid_argument(source + " needs a whole number, got '" + raw + "'");
    }
}

int positive(const std::string& source, const std::string& raw) {
    const int parsed = number(source, raw);
    if (parsed < 1) {
        throw std::invalid_argument(source + " must be at least 1, got " + std::to_string(parsed));
    }
    return parsed;
}

int non_negative(const std::string& source, const std::string& raw) {
    const int parsed = number(source, raw);
    if (parsed < 0) {
        throw std::invalid_argument(source + " must not be negative, got " + std::to_string(parsed));
    }
    return parsed;
}

std::optional<std::string> from_environment(const Environment& environment, const std::string& suffix) {
    const std::optional<std::string> raw = environment(std::string(kEnvironmentPrefix) + suffix);
    if (!raw.has_value()) {
        return std::nullopt;
    }
    const std::string trimmed = trim(*raw);
    return trimmed.empty() ? std::nullopt : std::optional<std::string>(trimmed);
}

const std::string& value(const std::vector<std::string>& args, std::size_t index, const std::string& flag) {
    if (index >= args.size()) {
        throw std::invalid_argument(flag + " needs a value");
    }
    return args[index];
}

}  // namespace

Environment process_environment() {
    return [](const std::string& name) -> std::optional<std::string> {
        const char* raw = std::getenv(name.c_str());
        return raw == nullptr ? std::nullopt : std::optional<std::string>(raw);
    };
}

bool DemoOptions::help_requested(const std::vector<std::string>& args) {
    for (const std::string& argument : args) {
        if (argument == "-h" || argument == "--help") {
            return true;
        }
    }
    return false;
}

DemoOptions DemoOptions::parse(const std::vector<std::string>& args, const Environment& environment) {
    DemoOptions options;

    // The environment first, so a flag can override it below.
    if (const auto raw = from_environment(environment, "RECORDS")) {
        options.records = positive("PC_DEMO_RECORDS", *raw);
    }
    if (const auto raw = from_environment(environment, "DELAY_MS")) {
        options.delay_ms = non_negative("PC_DEMO_DELAY_MS", *raw);
    }
    if (const auto raw = from_environment(environment, "CONCURRENCY")) {
        options.max_concurrency = positive("PC_DEMO_CONCURRENCY", *raw);
    }
    if (const auto raw = from_environment(environment, "PARTITIONS")) {
        options.partitions = positive("PC_DEMO_PARTITIONS", *raw);
    }
    if (const auto raw = from_environment(environment, "REPLAY_FACTOR")) {
        options.replay_factor = non_negative("PC_DEMO_REPLAY_FACTOR", *raw);
    }
    if (const auto raw = from_environment(environment, "BOOTSTRAP")) {
        options.bootstrap = *raw;
    }
    if (const auto raw = from_environment(environment, "TOPIC")) {
        options.topic = *raw;
    }

    for (std::size_t index = 0; index < args.size(); ++index) {
        const std::string& flag = args[index];
        if (flag == "--records") {
            options.records = positive(flag, value(args, ++index, flag));
        } else if (flag == "--delay-ms") {
            options.delay_ms = non_negative(flag, value(args, ++index, flag));
        } else if (flag == "--concurrency") {
            options.max_concurrency = positive(flag, value(args, ++index, flag));
        } else if (flag == "--partitions") {
            options.partitions = positive(flag, value(args, ++index, flag));
        } else if (flag == "--replay-factor") {
            // 1 or less skips the big replay, so this one is allowed to be zero
            options.replay_factor = non_negative(flag, value(args, ++index, flag));
        } else if (flag == "--bootstrap") {
            options.bootstrap = value(args, ++index, flag);
        } else if (flag == "--topic") {
            options.topic = value(args, ++index, flag);
        } else {
            throw std::invalid_argument("unknown option: " + flag);
        }
    }

    // Checked as a long here rather than trusted as an int later: records * replay_factor overflows
    // silently, and a wrapped value turns the big replay into a tiny one that still prints a
    // confident throughput figure.
    const long big = static_cast<long>(options.records) * (options.replay_factor < 1 ? 1 : options.replay_factor);
    if (big > std::numeric_limits<int>::max()) {
        throw std::invalid_argument("--records times --replay-factor is " + std::to_string(big)
                                    + ", which is more records than the demo can count; lower one of them");
    }
    return options;
}

int DemoOptions::big_replay_records() const {
    return records * (replay_factor < 1 ? 1 : replay_factor);
}

bool DemoOptions::big_replay_wanted() const { return replay_factor > 1; }

std::string DemoOptions::describe() const {
    std::ostringstream out;
    out << "records = " << records << "\n  delayMs = " << delay_ms << "\n  maxConcurrency = " << max_concurrency
        << "\n  partitions = " << partitions << "\n  replayFactor = " << replay_factor;
    return out.str();
}

std::string usage() {
    return "\nusage: demo/run.sh [options]\n"
           "  --records N        records in the comparison replay   (default 2000)\n"
           "  --delay-ms N       simulated work per record, ms      (default 2)\n"
           "  --concurrency N    max in-flight records              (default 100)\n"
           "  --partitions N     partitions on the demo topic       (default 10)\n"
           "  --replay-factor N  big replay = records x N; 1 skips  (default 20)\n"
           "  --bootstrap ADDR   an existing broker; omit to use the compose sibling\n"
           "  --topic NAME       an existing topic; omit to create one\n"
           "\nEvery flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.\n"
           "Flags beat the environment beats the defaults.\n";
}

}  // namespace parallelconsumer::proxy::demo
