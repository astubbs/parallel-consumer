# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "rdkafka"

module ParallelConsumer
  module Demo
    # The broker the demo reads from, and the only place this demo speaks Kafka on its own behalf.
    #
    # THE ADDRESS ALWAYS ARRIVES FROM OUTSIDE, which is a rule rather than a shortcut. The Java seed
    # can start a broker itself with Testcontainers; Ruby has no equivalent this demo would rather
    # depend on, and it does not need one - `demo/run.sh` starts the compose broker on the host and
    # hands the address in, exactly as compose does inside the container. THE DEMO CONTAINER IS
    # NEVER GRANTED THE HOST DOCKER SOCKET (plan unit U35, step 2): a documented socket mount is
    # root-equivalent host access taught as the normal way to run the product.
    #
    # The same door serves own-cluster mode, where the address is the user's real cluster - so
    # nothing here logs or echoes it.
    class Broker
      # The key space the seeded records spread over. Ordering is UNORDERED in both arms, so this
      # changes nothing today; it exists so that a KEY-ordered lane added later has more than one
      # key to shard across, rather than needing the seeding rewritten first.
      KEY_SPACE = 1000

      # How many produce handles are awaited before the next batch is queued. librdkafka's send
      # queue is bounded (queue.buffering.max.messages), and a demo asked for a big enough backlog
      # would otherwise be refused mid-seed by its own producer rather than by anything real.
      SEED_BATCH = 10_000

      # How long to wait for one produce or one topic creation. Generous: this is a first run on a
      # cold broker, not a latency measurement, and nothing here is on any reported clock.
      #
      # MILLISECONDS, because `max_wait_timeout` in seconds is deprecated in rdkafka and prints a
      # warning per call - which at one call per produced record buries the demo's own output in
      # its own deprecation notices.
      ADMIN_TIMEOUT_MS = 60_000

      def initialize(bootstrap)
        @bootstrap = bootstrap
      end

      # Creates the demo's topic, tolerating one a previous run already left behind.
      def ensure_topic(topic, partitions)
        admin = config.admin
        admin.create_topic(topic, partitions, 1).wait(max_wait_timeout_ms: ADMIN_TIMEOUT_MS)
        puts "Created topic #{topic} with #{partitions} partitions"
      rescue Rdkafka::RdkafkaError => e
        raise unless e.code == :topic_already_exists

        reuse(topic, partitions)
      ensure
        admin&.close
      end

      # Produces the backlog both arms then replay.
      #
      # PRE-PRODUCED RATHER THAN PRODUCED ALONGSIDE THE ARMS, and that is what makes the workload
      # closed-loop - which is in turn why no arm reports latency. A per-record timing here would be
      # flattered by however far an arm had fallen behind, so throughput is the only honest number
      # this shape can produce.
      def seed(topic, from, to)
        return if to <= from

        puts "Producing records #{from} to #{to}..."
        producer = config("linger.ms" => "20").producer
        handles = []
        (from...to).each do |index|
          handles << producer.produce(topic: topic, key: "key-#{index % KEY_SPACE}",
                                      payload: "record-#{index}")
          handles = settle(handles) if handles.size >= SEED_BATCH
        end
        settle(handles)
        puts "Produced #{to - from} records"
      ensure
        producer&.close
      end

      # The Kafka properties each arm's consumer needs to reach this broker.
      #
      # ONE MAP FOR BOTH ARMS, and it has to be: the AK core arm hands it to librdkafka and the
      # sidecar arm hands it through the handshake to the JVM consumer the proxy owns. The four
      # keys are spelled identically in both clients, which is the only reason a single map can
      # serve them - a fifth key would have to be checked against both.
      #
      # `enable.auto.commit` is false because Parallel Consumer owns offset commits and the engine
      # refuses a consumer with it on. The AK core arm does not need it off - it forms a fresh group
      # per run and never commits anything that matters - but it must not be the one arm configured
      # differently.
      def consumer_properties(group_id)
        {
          "bootstrap.servers" => @bootstrap,
          "group.id" => group_id,
          "auto.offset.reset" => "earliest",
          "enable.auto.commit" => "false"
        }
      end

      # A consumer for the AK core arm, subscribed and ready to poll.
      def subscribed_consumer(group_id, topic)
        consumer = config(consumer_properties(group_id)).consumer
        consumer.subscribe(topic)
        consumer
      end

      private

      def config(extra = {})
        Rdkafka::Config.new({ "bootstrap.servers" => @bootstrap }.merge(extra))
      end

      # Waits for a batch of produce handles, and returns a fresh empty batch.
      #
      # EVERY HANDLE IS AWAITED, none discarded. `flush` does not report a send that failed and a
      # dropped handle swallows the reason, so without this the demo would report a full backlog,
      # run both arms against a short one, and print numbers for a workload that never existed.
      def settle(handles)
        handles.each { |handle| handle.wait(max_wait_timeout_ms: ADMIN_TIMEOUT_MS) }
        []
      end

      # Reusing a topic silently is fine; reusing one with a DIFFERENT partition count is not,
      # because the effective-configuration block would print a --partitions value that never
      # applied - and that block is the demo's whole reproducibility promise.
      def reuse(topic, partitions)
        producer = config.producer
        existing = producer.partition_count(topic)
        if existing != partitions
          raise "topic #{topic} already exists with #{existing} partitions, but this run asked for " \
                "#{partitions} - pass --topic to name a fresh one, or --partitions #{existing}"
        end

        puts "Topic #{topic} already exists with the requested #{partitions} partitions, reusing it"
      ensure
        producer&.close
      end
    end
  end
end
