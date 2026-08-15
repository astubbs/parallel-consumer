// Copyright (C) 2026 Antony Stubbs and contributors
//
// C++'s half of the shared cross-language conformance suite (astubbs#242, confluentinc#154).
//
// IT ASSERTS NOTHING, DELIBERATELY. The suite that knows what correct looks like - offset
// frontiers, ordering, redelivery, attempt counts - is the Java module
// parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance, and it keeps owning that
// knowledge for every language. This binary's whole job is to DO WHAT THE SCENARIO SAYS and then
// exit; if it were free to decide what "correct" means, eleven languages would each decide it
// slightly differently and the agreement between them would prove nothing.
//
// Its contract - the five flags, the three exit codes, the stdout observation line, the four
// behaviour tokens, the fixed literals - is documented once, in that module's README.md, and is
// identical in every language.
//
// THIS DOES NOT REPLACE THE MODULE'S OWN TESTS. The shared suite proves every client behaves
// identically on the protocol; tests/ catches what is invisible from outside the process - the
// ceiling counting the wrong thing, a credential in a rendering, a port line missed among log lines.
// Both layers are load-bearing.

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "parallel_consumer_proxy_client.h"

namespace pcp = parallelconsumer::proxy;

namespace {

/// Exit statuses ARE the verdict channel. There is no results file and no report message: a scenario
/// passed if this process exited 0 and the Java suite's own assertions about engine state held.
constexpr int kExitOk = 0;
constexpr int kExitBehaviourFailed = 1;
constexpr int kExitUsage = 2;

constexpr const char* kSucceed = "succeed";
constexpr const char* kReportNothing = "report-nothing";
constexpr const char* kFailThenSucceed = "fail-then-succeed";
constexpr const char* kHoldFirstUntilSecond = "hold-first-until-second";

/// The exact text a fail-then-succeed run reports. The Java suite asserts the redelivery carries it
/// back VERBATIM, so it is a fixed literal of the contract in every language, never composed here.
constexpr const char* kPrescribedFailureReason = "conformance-prescribed-failure";

// Fixed session tunables, contract rather than this runner's judgement: they exist only so scenarios
// converge at unit-test speed against the engine's production defaults (a 5s commit interval, a 1s
// retry delay). Every language sets the same two values.
constexpr std::chrono::milliseconds kCommitInterval{100};
constexpr std::chrono::milliseconds kRetryDelay{50};

/// How long a report-nothing run keeps its session OPEN after its last observation.
///
/// IT IS WHAT MAKES THE NEGATIVE CONTROL A CONTROL. Without it the runner exits the instant the
/// record arrives, and a sabotaged runner that DID report success has its report killed in flight by
/// the process exit - so the suite sees an unadvanced offset either way and the scenario passes for
/// a broken client. Measured in the Go wave, not reasoned about.
constexpr std::chrono::seconds kReportNothingHold{3};

struct Arguments {
    std::string scenario;
    std::string behaviour;
    std::string sidecar;
    int expect_dispatches = 0;
    int timeout_seconds = 0;
};

/// The five flags, spelled identically in every language - including the British --behaviour.
bool parse(const std::vector<std::string>& argv, Arguments& arguments, std::string& problem) {
    std::vector<std::pair<std::string, std::string>> values;
    for (std::size_t index = 0; index < argv.size(); index += 2) {
        if (argv[index].rfind("--", 0) != 0) {
            problem = "expected --flag value pairs, got " + argv[index];
            return false;
        }
        if (index + 1 >= argv.size()) {
            problem = argv[index] + " takes a value";
            return false;
        }
        values.emplace_back(argv[index], argv[index + 1]);
    }
    const auto take = [&values](const std::string& name) -> std::string {
        const auto found = std::find_if(values.begin(), values.end(),
                                        [&name](const auto& entry) { return entry.first == name; });
        return found == values.end() ? std::string{} : found->second;
    };

    arguments.scenario = take("--scenario");
    arguments.behaviour = take("--behaviour");
    arguments.sidecar = take("--sidecar");
    const std::string expect = take("--expect-dispatches");
    const std::string timeout = take("--timeout-seconds");
    for (const auto& required : {std::make_pair("--scenario", arguments.scenario),
                                 std::make_pair("--behaviour", arguments.behaviour),
                                 std::make_pair("--sidecar", arguments.sidecar),
                                 std::make_pair("--expect-dispatches", expect),
                                 std::make_pair("--timeout-seconds", timeout)}) {
        if (required.second.empty()) {
            problem = std::string(required.first) + " is required";
            return false;
        }
    }
    if (arguments.behaviour != kSucceed && arguments.behaviour != kReportNothing &&
        arguments.behaviour != kFailThenSucceed && arguments.behaviour != kHoldFirstUntilSecond) {
        problem = "unknown behaviour '" + arguments.behaviour + "'";
        return false;
    }
    if (arguments.sidecar.front() != '/') {
        problem = "--sidecar must be absolute, got '" + arguments.sidecar + "'";
        return false;
    }
    try {
        arguments.expect_dispatches = std::stoi(expect);
        arguments.timeout_seconds = std::stoi(timeout);
    } catch (const std::exception&) {
        problem = "--expect-dispatches and --timeout-seconds must be positive integers";
        return false;
    }
    if (arguments.expect_dispatches < 1 || arguments.timeout_seconds < 1) {
        problem = "--expect-dispatches and --timeout-seconds must be at least 1";
        return false;
    }
    return true;
}

/// Counts deliveries and outcomes, and prints the observation line. It holds no per-record state -
/// only counts - because the client library holds none either, and this runner must not become the
/// place where a client's missing bookkeeping is quietly supplied.
class Tracker {
public:
    /// Prints the delivery and returns its 1-based ordinal in arrival order. The lock covers the
    /// increment AND the print together, so the transcript's order is the order the ordinals were
    /// handed out in.
    int observe(const pcp::InboundRecord& record) {
        int ordinal;
        {
            const std::lock_guard<std::mutex> lock(mutex_);
            ordinal = ++observed_;
            // Printed at the moment of delivery, before the behaviour acts on it. reason comes last
            // because it is worker-supplied text that may contain spaces.
            std::cout << "dispatch key=" << record.key.value_or("") << " offset=" << record.offset
                      << " attempt=" << record.attempt << " reason=" << record.last_failure_reason.value_or("")
                      << '\n'
                      << std::flush;
        }
        changed_.notify_all();
        return ordinal;
    }

    void complete() {
        {
            const std::lock_guard<std::mutex> lock(mutex_);
            ++completed_;
        }
        changed_.notify_all();
    }

    int observed() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return observed_;
    }

    int completed() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return completed_;
    }

    /// Resolves once a second delivery has been observed - the instrument the ordering scenario is.
    bool await_second(std::chrono::seconds budget) {
        std::unique_lock<std::mutex> lock(mutex_);
        return changed_.wait_for(lock, budget, [this] { return observed_ >= 2; });
    }

    /// Whether the prescription finished inside the budget. report-nothing completes at OBSERVATION,
    /// because by prescription its records are never reported and so can never complete.
    bool await_prescribed(bool at_observation, int expected, std::chrono::seconds budget) {
        std::unique_lock<std::mutex> lock(mutex_);
        return changed_.wait_for(lock, budget, [this, at_observation, expected] {
            return (at_observation ? observed_ : completed_) >= expected;
        });
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable changed_;
    int observed_ = 0;
    int completed_ = 0;
};

}  // namespace

int main(int argc, char** argv) {
    Arguments arguments;
    std::string problem;
    if (!parse(std::vector<std::string>(argv + 1, argv + argc), arguments, problem)) {
        std::cerr << "conformance-runner: " << problem << '\n';
        return kExitUsage;
    }

    const auto budget = std::chrono::seconds(arguments.timeout_seconds);
    Tracker tracker;
    std::atomic<bool> hold_expired{false};

    pcp::ClientOptions options;
    options.sidecar_path = arguments.sidecar;
    // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
    options.topics = {arguments.scenario};
    // Enough capacity for every dispatch the scenario prescribes, so a scenario that holds a record
    // cannot deadlock on an executor count smaller than its own shape.
    options.max_concurrency = arguments.expect_dispatches;
    options.commit_interval = kCommitInterval;
    options.default_message_retry_delay = kRetryDelay;
    // The mock lane builds mock Kafka clients and reads no properties. Real credentials never belong
    // in a conformance test.
    options.kafka_properties = {};
    options.instance_tag = "conformance-runner-cpp";
    // Diagnostics go to stderr, which the suite captures and attaches to any failure message. Debug
    // is left off: it is per-record, and a transcript is not a log.
    options.logger = [](pcp::LogLevel level, const std::string& line) {
        if (level != pcp::LogLevel::Debug) {
            std::cerr << "conformance-runner [" << pcp::to_string(level) << "] " << line << '\n';
        }
    };

    std::unique_ptr<pcp::Client> client;
    try {
        client = pcp::Client::connect(options);
    } catch (const std::exception& refused) {
        std::cerr << "conformance-runner: opening the session: " << refused.what() << '\n';
        return kExitBehaviourFailed;
    }

    const std::string behaviour = arguments.behaviour;
    client->poll([&](const pcp::InboundRecord& record) -> pcp::Outcome {
        const int ordinal = tracker.observe(record);

        if (behaviour == kSucceed) {
            tracker.complete();
            return pcp::Outcome::success();
        }
        if (behaviour == kReportNothing) {
            // PRESCRIBED: never report. Blocking here for longer than the whole run is how a C++
            // worker says "this record's function has not returned"; the process exits with the
            // record still in flight, which is a worker that vanished mid-record.
            for (;;) {
                std::this_thread::sleep_for(std::chrono::hours(1));
            }
        }
        if (behaviour == kFailThenSucceed) {
            tracker.complete();
            if (record.attempt == 1) {
                return pcp::Outcome::failure(kPrescribedFailureReason);
            }
            return pcp::Outcome::success();
        }
        // hold-first-until-second: hold the FIRST record until a SECOND is dispatched. Whether one
        // arrives at all, and which key it carries, is the whole of what the scenario is asking -
        // and it is the Java suite that decides what the answer means.
        if (ordinal == 1 && !tracker.await_second(budget)) {
            hold_expired = true;
            tracker.complete();
            return pcp::Outcome::failure("no second delivery arrived within the budget");
        }
        tracker.complete();
        return pcp::Outcome::success();
    });

    const bool report_nothing = behaviour == kReportNothing;
    if (!tracker.await_prescribed(report_nothing, arguments.expect_dispatches, budget)) {
        std::cerr << "conformance-runner: scenario '" << arguments.scenario << "' behaviour '" << behaviour
                  << "' did not complete within " << arguments.timeout_seconds << "s - observed "
                  << tracker.observed() << " of " << arguments.expect_dispatches << ", completed "
                  << tracker.completed() << '\n';
        return kExitBehaviourFailed;
    }
    if (hold_expired) {
        std::cerr << "conformance-runner: the held record never saw a second delivery\n";
        return kExitBehaviourFailed;
    }

    if (report_nothing) {
        // Hold the session open, reporting nothing, so the suite is watching a LIVE client rather
        // than the wreckage of one - see kReportNothingHold.
        std::this_thread::sleep_for(kReportNothingHold);
        // PRESCRIBED: the record is never reported and the session is abandoned rather than drained.
        // _Exit rather than a clean return, because the executor holding the record never returns
        // and any orderly path would join it. Flushed first, or the transcript the suite reads dies
        // with the process; the sidecar is reaped anyway, because exiting closes its lifecycle pipe.
        std::cout << std::flush;
        std::cerr << std::flush;
        std::_Exit(kExitOk);
    }

    try {
        client->shutdown();
    } catch (const std::exception& closing) {
        std::cerr << "conformance-runner: closing the session: " << closing.what() << '\n';
        return kExitBehaviourFailed;
    }
    return kExitOk;
}
