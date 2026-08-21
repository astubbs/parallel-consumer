// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE C++ DEMO (astubbs#242, plan unit U35). The same records through two arms:
//
//   - AK core (librdkafka)     - C++'s own Kafka client, one record at a time. "AK core" is always
//                 spelled out, because bare "core" reads as parallel-consumer-core (CONCEPTS.md) -
//                 and never left bare EITHER, because "AK core" is a category rather than a client
//                 and a reader cannot judge a comparison without knowing what produced it.
//   - cpp-grpc (this client)   - this application as a FOREIGN CLIENT. The client library in the
//                 module above spawns the sidecar, receives records over a socket, runs the same
//                 sleep on them and reports outcomes back. On this path the application does no
//                 Kafka I/O at all: the sidecar owns the consumer, the producer, the group
//                 membership and the offsets.
//
// THE ARM GOES THROUGH THE CLIENT LIBRARY, NEVER THROUGH HAND-WRITTEN gRPC. The Java seed was
// written the other way first and had to be rewritten: speaking the protocol by hand proves the
// ENGINE works and says nothing about the client library, which is the artifact users actually
// touch. Java keeps a hand-written arm as a CONTROL because one JVM can hold every arm at once and
// the pair prices the library; C++ has nothing to compare it against, so two arms is the whole
// demo here - which is the contract everywhere except Java.
//
// NO LATENCY IS REPORTED, and that is a rule rather than an omission. The backlog is pre-produced,
// so the workload is closed-loop and per-record timings are flattered by however far an arm fell
// behind. Throughput is the only honest number this shape can produce.
//
// The contract this file keeps is parallel-consumer-proxy/demo/README.md. Read that first.

#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <future>
#include <iostream>
#include <optional>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "demo_broker.h"
#include "demo_options.h"
#include "demo_report.h"
#include "parallel_consumer_proxy_client.h"

namespace pcp = parallelconsumer::proxy;

namespace parallelconsumer::proxy::demo {
namespace {

using Clock = std::chrono::steady_clock;

/// No arm may take longer than this before the demo calls it stalled rather than slow.
constexpr std::chrono::minutes kArmBudget{10};

/// How often the sidecar arm looks up from waiting to ask whether the session is still alive.
constexpr std::chrono::milliseconds kProgressCheck{200};

/// THE ARM LABELS, AND WHY EACH CARRIES A LIBRARY NAME.
///
/// "AK core" is a CATEGORY, not a client: it means "that language's own Kafka client", and the
/// answer differs in every language. A reader cannot judge a comparison without knowing what
/// produced it, so the role and the library are both said. In C++ the library is librdkafka - see
/// demo/README.md for why there is no second candidate worth running as its own arm.
///
/// The sidecar arm is labelled with what DRIVES it, which is this module's client library rather
/// than hand-written gRPC. That distinction is the whole point of the arm, and until it was in the
/// label the output did not carry it.
constexpr const char* kAkCore = "AK core (librdkafka)";
constexpr const char* kSidecarArm = "cpp-grpc (this client)";

/// Where the sidecar binary is when nothing said otherwise - the path the demo image installs it
/// at. `PC_DEMO_SIDECAR` overrides it, which is what a reader running the binary outside that image
/// would set.
///
/// IT IS NOT A FLAG, deliberately: the contract fixes the demo's flag list at seven, and this is a
/// property of the image rather than of the run. See demo/README.md.
constexpr const char* kDefaultSidecarPath = "/app/sidecar/sidecar";

/// A fresh group per arm per replay, so every arm reads the same records from the beginning.
std::string group_id(const std::string& arm) {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return "pc-demo-" + arm + "-"
           + std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

ArmResult finished(const std::string& arm, Clock::time_point started, int processed, std::size_t unique_keys) {
    const auto elapsed = Clock::now() - started;
    std::cout << "=== " << arm << " finished: " << processed << " records over " << unique_keys
              << " keys in " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
              << "ms ===" << std::endl;
    return ArmResult{arm, elapsed, processed, static_cast<int>(unique_keys)};
}

/// The simulated work, identical in both arms so they differ by transport and nothing else.
///
/// A BLOCKING SLEEP IS THE RIGHT ANSWER IN C++, and the contract says so: the divergence it calls
/// out - a non-occupying wait - binds Python, whose client runs worker processes, and TypeScript,
/// whose single event loop a blocking sleep would stop. A C++ executor is a thread, and a sleeping
/// thread occupies nothing.
void simulated_work(int delay_ms) {
    if (delay_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
}

/// The two tables, same columns and same order in every language. Rendered next door in
/// demo_report.cpp so the shape can be tested without a broker; printed here.
void report(const std::string& title, const std::vector<ArmResult>& results, const ArmResult* baseline,
            bool across_replays) {
    std::cout << render(title, results, baseline, across_replays) << std::flush;
}

/// The demo itself.
class ReferenceDemo {
public:
    ReferenceDemo(DemoOptions options, std::string topic, std::string sidecar)
        : options_(std::move(options)),
          broker_(options_.bootstrap),
          topic_(std::move(topic)),
          sidecar_(std::move(sidecar)) {}

    void run() {
        // Then the fingerprint, before anything runs: a number without its settings is not
        // reproducible. The bootstrap address is deliberately not in it.
        std::cout << "\nEffective configuration:\n  " << options_.describe() << "\n  topic = " << topic_
                  << std::endl;

        broker_.ensure_topic(topic_, options_.partitions);
        broker_.seed(topic_, 0, options_.records);

        std::vector<ArmResult> small;
        small.push_back(ak_core(options_.records));
        small.push_back(cpp_grpc(options_.records));
        report("Small replay - every arm over the same " + std::to_string(options_.records)
                       + " records (the comparison)",
               small, &small.front(), false);

        if (!options_.big_replay_wanted()) {
            std::cout << "\nBig replay skipped (--replay-factor " << options_.replay_factor << ")."
                      << std::endl;
            return;
        }

        const int total = options_.big_replay_records();
        broker_.seed(topic_, options_.records, total);

        // AK core is excluded here because it does not go parallel: it would need total * delayMs
        // milliseconds to finish a backlog the sidecar arm clears in seconds, and a demo that makes
        // a reader wait that long to learn nothing new is not worth the wall clock.
        std::vector<ArmResult> big;
        big.push_back(cpp_grpc(total));
        report("Big replay - " + std::to_string(total) + " records, parallel arms only (AK core is serial"
                       + " and would take " + std::to_string(static_cast<long>(total) * options_.delay_ms / 1000)
                       + "s+)",
               big, &small.front(), true);
    }

private:
    /// The serial arm: C++'s own Kafka client, one record at a time, the same sleep.
    ArmResult ak_core(int target) {
        std::cout << "\n=== " << kAkCore << " starting over " << target << " records ===" << std::endl;
        KafkaHandle consumer = broker_.subscribed_consumer(topic_, group_id("ak-core"));

        int processed = 0;
        // THE KEYS THIS ARM ACTUALLY SAW, which is half of what turns the table from an assertion
        // that work happened into a demonstration of it. A set rather than a counter because
        // "unique" is the claim; a counter would report the same number as `processed` and prove
        // nothing about the backlog being spread.
        std::set<std::string> keys;
        // The clock starts AFTER the consumer is built and stops before it closes, because this arm
        // is the denominator of every ratio in both tables and no other arm charges itself for
        // client construction or teardown.
        const auto started = Clock::now();
        const auto deadline = started + kArmBudget;
        while (processed < target) {
            if (Clock::now() > deadline) {
                throw std::runtime_error(std::string(kAkCore) + " stalled at " + std::to_string(processed)
                                         + " of " + std::to_string(target));
            }
            rd_kafka_message_t* message = rd_kafka_consumer_poll(consumer.get(), 500);
            if (message == nullptr) {
                continue;
            }
            if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                // Partition EOF and the periodic informational errors are not records and not
                // failures; anything else is worth saying out loud rather than silently retrying
                // until the budget runs out.
                if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    std::cerr << "[AK core] " << rd_kafka_message_errstr(message) << '\n';
                }
                rd_kafka_message_destroy(message);
                continue;
            }
            simulated_work(options_.delay_ms);
            ++processed;
            // The key as bytes and by length, because a Kafka key is neither a C string nor
            // guaranteed to be text. A null key is legal and is not a key, so it is not counted -
            // this demo seeds `key-<index modulo the key space>` on every record, which makes the
            // count `min(records, key space)` and the same in every language.
            if (message->key != nullptr) {
                keys.emplace(static_cast<const char*>(message->key), message->key_len);
            }
            rd_kafka_message_destroy(message);
        }
        ArmResult result = finished(kAkCore, started, processed, keys.size());
        rd_kafka_consumer_close(consumer.get());
        return result;
    }

    /// The sidecar arm: the same work, reached through THIS MODULE'S CLIENT LIBRARY.
    ///
    /// The library spawns the sidecar as a child process and supervises it, so the reader installs,
    /// deploys and operates nothing (KTD41). That is why the sidecar is not a compose service in
    /// the file beside this one.
    ArmResult cpp_grpc(int target) {
        std::cout << "\n=== " << kSidecarArm << " starting over " << target << " records ==="
                  << std::endl;

        pcp::ClientOptions client_options;
        client_options.sidecar_path = sidecar_;
        client_options.topics = {topic_};
        client_options.max_concurrency = options_.max_concurrency;
        client_options.ordering = pcp::ProcessingOrder::Unordered;
        client_options.kafka_properties = broker_.consumer_properties(group_id("cpp-grpc"));
        client_options.instance_tag = "pc-cpp-demo";
        // The library says nothing until an application asks it to, so the demo asks - on stderr,
        // leaving stdout to the tables. Debug is per-record and stays off.
        client_options.logger = [](pcp::LogLevel level, const std::string& line) {
            if (level != pcp::LogLevel::Debug) {
                std::cerr << "[client " << pcp::to_string(level) << "] " << line << '\n';
            }
        };

        std::mutex mutex;
        std::condition_variable changed;
        int processed = 0;
        // GUARDED BY THE SAME MUTEX AS `processed`, because unlike the AK core arm's set this one is
        // written by every executor thread at once. An unsynchronised std::set here is a data race
        // that would usually look like a slightly wrong key count rather than like a crash.
        std::set<std::string> keys;

        std::unique_ptr<pcp::Client> client = pcp::Client::connect(client_options);
        // The clock starts once the session is open, for the same reason the AK core arm's starts
        // once its consumer is built: neither arm charges itself for start-up.
        const auto started = Clock::now();
        client->poll([&](const pcp::InboundRecord& record) {
            simulated_work(options_.delay_ms);
            {
                const std::lock_guard<std::mutex> lock(mutex);
                ++processed;
                if (record.key.has_value()) {
                    keys.insert(*record.key);
                }
            }
            changed.notify_all();
            return pcp::Outcome::success();
        });

        std::shared_future<void> session_end = client->session_end();
        const auto deadline = started + kArmBudget;
        for (;;) {
            std::unique_lock<std::mutex> lock(mutex);
            if (changed.wait_for(lock, kProgressCheck, [&] { return processed >= target; })) {
                break;
            }
            const int so_far = processed;
            lock.unlock();

            // Reaching the target is not the only thing that can end this wait. A session that
            // failed or completed early must be reported as exactly that: without this check a
            // broken run would sit here until the budget expired and then blame a stall.
            if (session_end.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                session_end.get();  // rethrows the fault, when the session died of one
                throw std::runtime_error(std::string(kSidecarArm) + " ended early at "
                                         + std::to_string(so_far) + " of " + std::to_string(target)
                                         + " - the session ended before the backlog did");
            }
            if (Clock::now() > deadline) {
                throw std::runtime_error(std::string(kSidecarArm) + " stalled at " + std::to_string(so_far)
                                         + " of " + std::to_string(target));
            }
        }

        int completed = 0;
        std::size_t distinct_keys = 0;
        {
            const std::lock_guard<std::mutex> lock(mutex);
            completed = processed;
            distinct_keys = keys.size();
        }
        ArmResult result = finished(kSidecarArm, started, completed, distinct_keys);
        // After the clock stops: the drain and the sidecar's reaping are teardown, and no other arm
        // charges itself for that either.
        client->shutdown();
        return result;
    }

    DemoOptions options_;
    DemoBroker broker_;
    std::string topic_;
    std::string sidecar_;
};

/// The sidecar binary this demo spawns, and proof that it is there before anything is timed.
std::string resolve_sidecar(const Environment& environment) {
    const std::optional<std::string> configured = environment("PC_DEMO_SIDECAR");
    const std::string path = configured.value_or(kDefaultSidecarPath);
    if (path.empty() || path.front() != '/') {
        throw std::runtime_error("PC_DEMO_SIDECAR must be an absolute path, got '" + path + "'");
    }
    if (::access(path.c_str(), X_OK) != 0) {
        throw std::runtime_error(
                "no executable sidecar at " + path
                + " - the demo image installs one there; outside it, set PC_DEMO_SIDECAR to the "
                  "sidecar launcher. The client library spawns it; you never start it yourself.");
    }
    return path;
}

}  // namespace
}  // namespace parallelconsumer::proxy::demo

int main(int argc, char** argv) {
    namespace demo = parallelconsumer::proxy::demo;

    const std::vector<std::string> args(argv + 1, argv + argc);

    // THE VERY FIRST THING PRINTED, WHATEVER HAPPENS NEXT - before the usage text, before a refused
    // flag, before the fingerprint. A reader who runs this must be told what they are looking at:
    // the demo used to open on a configuration dial, which names neither the product nor the point.
    std::cout << demo::banner();

    if (demo::DemoOptions::help_requested(args)) {
        std::cout << demo::usage();
        return 0;
    }

    const demo::Environment environment = demo::process_environment();
    demo::DemoOptions options;
    try {
        options = demo::DemoOptions::parse(args, environment);
    } catch (const std::exception& refused) {
        // A misspelled flag must not be reported as a result for settings nobody asked for.
        std::cerr << refused.what() << '\n' << demo::usage();
        return 2;
    }

    if (options.bootstrap.empty()) {
        std::cerr << "\nNo broker address. The C++ demo never starts one of its own - it runs inside "
                     "its\nown container, which is never granted the host Docker socket, so the broker "
                     "is a\ncompose sibling. Run demo/run.sh, which starts both, or pass --bootstrap / "
                     "set\nPC_DEMO_BOOTSTRAP to reach a cluster you already have.\n";
        return 2;
    }

    try {
        const std::string sidecar = demo::resolve_sidecar(environment);
        std::string topic = options.topic;
        if (topic.empty()) {
            topic = "pc-demo-" + std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                        std::chrono::system_clock::now().time_since_epoch())
                                                        .count());
        }
        demo::ReferenceDemo(options, topic, sidecar).run();
    } catch (const std::exception& failed) {
        // Loudly, and with a non-zero status: a demo whose numbers ten other languages copy must
        // never exit 0 having printed a plausible table for a run that did not happen.
        std::cerr << "\nThe demo failed: " << failed.what() << '\n';
        return 1;
    }
    return 0;
}
