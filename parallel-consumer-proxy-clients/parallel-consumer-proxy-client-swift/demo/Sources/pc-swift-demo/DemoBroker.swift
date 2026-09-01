// Copyright (C) 2026 Antony Stubbs and contributors
//
// The broker the demo reads from, and the backlog every arm then replays.
//
// SWIFT CANNOT START ITS OWN BROKER, AND THAT IS A CONSEQUENCE OF TWO RULES MEETING RATHER THAN A
// GAP. The Java seed starts one with Testcontainers when none is supplied; there is no Swift
// toolchain on a developer machine here, so this demo only ever runs inside its own container -
// and a demo container is never granted the host Docker socket (plan unit U35). So the address is
// always supplied: by `docker-compose.yml`, naming a compose SIBLING on the demo's own network, or
// by the caller's `--bootstrap`. `demo/run.sh` is what makes that invisible to a reader.
//
// THE ADDRESS IS NEVER LOGGED. The same door serves own-cluster mode, where it is a user's real
// cluster, so it appears in no line this demo prints - including the effective-configuration block.

import Foundation
import Kafka
import Logging
import ServiceLifecycle

/// Shortens the one name in this file long enough to force the formatter to break a closure's
/// signature across two lines, which it then flags.
private typealias BrokerAddress = KafkaConfiguration.BrokerAddress

struct DemoBroker: Sendable {

    /// The key space the seeded records spread over. Ordering is unordered in both arms, so this
    /// changes nothing today; it exists so a key-ordered lane added later has more than one key to
    /// shard across, rather than needing the seeding rewritten first. The Java seed uses the same.
    private static let keySpace = 1000

    let bootstrap: String
    private let addresses: [BrokerAddress]

    /// Uses the broker the caller supplied, and refuses to guess when there is none.
    static func resolve(_ supplied: String?) throws -> DemoBroker {
        guard let supplied, !supplied.trimmingCharacters(in: .whitespaces).isEmpty else {
            throw DemoError(
                "no broker address. Unlike the Java seed this demo cannot start one - it runs "
                    + "inside a container, and a demo container is never granted the host Docker "
                    + "socket. Run it with demo/run.sh, which starts a broker as a compose sibling "
                    + "and passes its address in, or pass --bootstrap yourself.")
        }
        let trimmed = supplied.trimmingCharacters(in: .whitespaces)
        let addresses = try trimmed.split(separator: ",").map { entry -> BrokerAddress in
            let parts = entry.split(separator: ":")
            guard parts.count <= 2, let host = parts.first, !host.isEmpty else {
                // The address itself is NOT quoted back: it may be a user's real cluster.
                throw DemoError("--bootstrap is not a host:port list")
            }
            let port: Int? = parts.count == 2 ? Int(parts[1]) : 9092
            guard let port, port > 0 else { throw DemoError("--bootstrap has a bad port") }
            return BrokerAddress(host: String(host), port: port)
        }
        guard !addresses.isEmpty else { throw DemoError("--bootstrap is empty") }
        return DemoBroker(bootstrap: trimmed, addresses: addresses)
    }

    /// The Kafka properties an arm needs to reach this broker.
    ///
    /// `enable.auto.commit` is false because Parallel Consumer owns offset commits and the engine
    /// refuses a consumer that does not agree. The sidecar forces it itself whatever the map says;
    /// setting it here keeps the map an honest statement of what the session runs with, and matches
    /// the Java seed property for property so the two demos are comparing the same configuration.
    func proxyProperties(groupID: String) -> [String: String] {
        [
            "bootstrap.servers": bootstrap,
            "group.id": groupID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        ]
    }

    /// A consumer configuration for the AK core arm - this language's own Kafka client, nothing else.
    func consumerConfiguration(groupID: String, topic: String) -> KafkaConsumerConfiguration {
        var configuration = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: groupID, topics: [topic]),
            bootstrapBrokerAddresses: addresses)
        configuration.autoOffsetReset = .earliest
        return configuration
    }

    /// Produces the backlog every arm then replays, and does not return until the broker has
    /// acknowledged every record.
    ///
    /// PRE-PRODUCED, WHICH IS WHAT MAKES THE WORKLOAD CLOSED-LOOP - and in turn why no arm reports
    /// latency. Waiting for the acknowledgements is not politeness either: a discarded delivery
    /// report is how a demo reports a full backlog, runs every arm against a short one, and prints
    /// confident numbers for a workload that never existed.
    func seed(topic: String, from: Int, to: Int, logger: Logger) async throws {
        guard to > from else { return }
        let total = to - from
        Note.say("Producing records \(from) to \(to)...")

        var configuration = KafkaProducerConfiguration(bootstrapBrokerAddresses: addresses)
        // The topic is created by this send, by the broker, with the broker's own num.partitions.
        // See the note in demo/README.md: swift-kafka-client has no admin client, so `--partitions`
        // reaches the broker through docker-compose.yml rather than through a CreateTopics call.
        configuration.isAutoCreateTopicsEnabled = true
        // The Java seed's linger.ms = 20, so the two demos batch their seeding the same way.
        configuration.queue.maximumMessageQueueTime = .milliseconds(20)

        let (producer, events) = try KafkaProducer.makeProducerWithEvents(
            configuration: configuration, logger: logger)
        let serviceGroup = ServiceGroup(services: [producer], logger: logger)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await serviceGroup.run() }
            group.addTask {
                // Sending and draining run at once, deliberately: librdkafka's send queue is
                // bounded, and a demo that sent everything before reading a single acknowledgement
                // would stall against that bound on any backlog worth measuring.
                async let sent: Void = Self.send(
                    producer: producer, topic: topic, from: from, to: to)
                let failure = await Self.drainAcknowledgements(events, expecting: total)
                try await sent
                await serviceGroup.triggerGracefulShutdown()
                if let failure { throw failure }
            }
            try await group.waitForAll()
        }
        Note.say("Produced \(total) records")
    }

    private static func send(
        producer: KafkaProducer, topic: String, from: Int, to: Int
    ) async throws {
        for index in from..<to {
            let message = KafkaProducerMessage(
                topic: topic, key: "key-\(index % keySpace)", value: "record-\(index)")
            // A full send queue is a back-pressure signal, not a failure: retry it rather than
            // abandoning the backlog. The budget exists so a broker that has stopped accepting
            // anything ends the demo instead of spinning here for ever.
            let deadline = ContinuousClock.now + .seconds(120)
            while true {
                do {
                    _ = try producer.send(message)
                    break
                } catch {
                    if ContinuousClock.now >= deadline {
                        throw DemoError("the demo could not seed its backlog: \(error)")
                    }
                    try await Task.sleep(for: .milliseconds(20))
                }
            }
        }
    }

    /// Counts delivery reports until every seeded record has one, and returns the FIRST failure -
    /// later ones are consequences of it far more often than they are new information.
    private static func drainAcknowledgements(_ events: KafkaProducerEvents, expecting total: Int)
        async -> DemoError?
    {
        var acknowledged = 0
        var firstFailure: DemoError?
        for await event in events {
            if case .deliveryReports(let reports) = event {
                for report in reports {
                    acknowledged += 1
                    if case .failure(let error) = report.status, firstFailure == nil {
                        firstFailure = DemoError("the demo could not seed its backlog: \(error)")
                    }
                }
            }
            if acknowledged >= total { break }
        }
        if acknowledged < total && firstFailure == nil {
            firstFailure = DemoError(
                "the producer stopped reporting after \(acknowledged) of \(total) records")
        }
        return firstFailure
    }
}
