# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "parallel_consumer"

require "parallel_consumer/demo/arm_result"
require "parallel_consumer/demo/broker"
require "parallel_consumer/demo/sidecar"

module ParallelConsumer
  module Demo
    # <b>The Ruby demo:</b> the same records through Ruby's own Kafka client, and through Ruby over
    # the sidecar. The contract it keeps is `parallel-consumer-proxy/demo/README.md`, and the
    # reference implementation it is transcribed from is the Java seed's `ReferenceDemo`.
    #
    # == Two arms, and why Ruby has exactly two
    #
    # - <b>AK core</b> - librdkafka through the `rdkafka` gem, one record at a time. Always spelled
    #   "AK core", never bare "core", which reads as `parallel-consumer-core` (`CONCEPTS.md`).
    # - <b>ruby-grpc</b> - this module's client library, which spawns the sidecar, receives records
    #   over a socket, runs this demo's block on its executor threads and reports outcomes back.
    #   <b>The application does no Kafka I/O on this path</b>: the sidecar owns the consumer, the
    #   producer, the group membership and the offsets.
    #
    # Java carries four further arms because one JVM can hold every engine at once. Ruby cannot: the
    # engine is Java, so there is no in-process arm to compare against and nothing a hand-written
    # wire arm would isolate that the Java seed does not already price. <b>Two arms is the whole
    # contract everywhere except Java.</b>
    #
    # == It is not a like-for-like engine comparison, and no per-language demo is
    #
    # The two arms differ in client library as well as in engine - librdkafka's C consumer against
    # a JVM consumer inside the sidecar. That is what a Ruby user actually chooses between, which is
    # why the demo is worth running; it is not the isolated price of the sidecar hop, which only
    # Java can measure and does.
    class Comparison
      # No arm may take longer than this before the demo calls it stalled rather than slow.
      ARM_BUDGET = 600

      AK_CORE = "AK core"

      SIDECAR_ARM = "ruby-grpc"

      # How long the AK core arm waits for one record before checking its budget again. It is not a
      # timeout for the arm - that is ARM_BUDGET - only how often a consumer with nothing to hand
      # back returns to this loop.
      POLL_TIMEOUT_MS = 500

      # THE ONE ACROSS-REPLAYS FOOTNOTE, and it is not decoration: the big replay's ratio column
      # compares a big-replay rate against the SMALL replay's AK core arm, because the serial arm
      # does not run at the big volume. A reader who takes that for a like-for-like ratio has been
      # misled by the table rather than by the demo.
      FOOTNOTE = "\n  * against the SMALL replay's AK core arm. Across replays, so not " \
                 "like-for-like.\n"

      # The column layout is the Java seed's, character for character, so that two languages' tables
      # can be read side by side. Annotated format tokens are RuboCop's requirement, not the seed's;
      # the widths are what carry over.
      HEADER = "  %<arm>-14s %<elapsed>10s %<rate>14s %<ratio>14s"

      ROW = "  %<arm>-14s %<elapsed>9.1fs %<rate>14s %<ratio>14s"

      def initialize(options, broker, topic)
        @options = options
        @broker = broker
        @topic = topic
        @sidecar = Sidecar.resolve
      end

      # Runs the whole demo and hands back every arm's result.
      def run
        puts "\nEffective configuration:\n  #{@options}\n  topic = #{@topic}"
        @broker.ensure_topic(@topic, @options.partitions)
        @broker.seed(@topic, 0, @options.records)

        small = [ak_core(@options.records), sidecar(@options.records)]
        report("Small replay - every arm over the same #{@options.records} records (the comparison)",
               small, baseline_of(small), across_replays: false)
        unless @options.big_replay?
          puts "\nBig replay skipped (--replay-factor #{@options.replay_factor})."
          return small
        end

        small + big_replay(baseline_of(small))
      end

      private

      # The big replay excludes AK core because it does not go parallel: it would need
      # `records * delay_ms` milliseconds to finish a backlog the sidecar clears in seconds, and a
      # demo that makes a reader wait that long to learn nothing new is not worth the wall clock.
      def big_replay(baseline)
        total = @options.big_replay_records
        @broker.seed(@topic, @options.records, total)
        big = [sidecar(total)]
        serial_estimate = total * @options.delay_ms / 1000
        report("Big replay - #{total} records, parallel arms only (AK core is serial and would " \
               "take #{serial_estimate}s+)", big, baseline, across_replays: true)
        big
      end

      # THE SERIAL ARM: one record at a time, the same sleep, in this process.
      #
      # `Rdkafka::Consumer#poll` hands back one record per call, which is this arm's shape anyway -
      # there is no batch to iterate, because the point of the arm is that nothing overlaps.
      def ak_core(target)
        puts "\n=== #{AK_CORE} starting over #{target} records ==="
        consumer = @broker.subscribed_consumer(group_id("ak-core"), @topic)
        # The clock starts AFTER the consumer is built and stops before it closes, because this arm
        # is the denominator of every ratio in both tables and the other arm does not charge itself
        # for client construction or teardown either.
        started = Demo.clock
        processed = drain(consumer, target, started + ARM_BUDGET)
        finished(AK_CORE, Demo.clock - started, processed)
      ensure
        consumer&.close
      end

      def drain(consumer, target, deadline)
        processed = 0
        while processed < target
          # The one arm that does not wait on a condition variable still needs a budget, or a
          # backlog shorter than the target spins here forever with no output.
          raise "#{AK_CORE} stalled at #{processed} of #{target}" if Demo.clock > deadline

          next unless consumer.poll(POLL_TIMEOUT_MS)

          sleep(@options.delay_seconds)
          processed += 1
        end
        processed
      end

      # THE ARM THE WHOLE DESIGN EXISTS FOR: the client library over a real sidecar it spawns.
      #
      # On this path the application does no Kafka I/O - it starts a binary, receives records over a
      # socket, runs its own block on them, and reports outcomes back. That is a claim about the
      # PATH, not about this process: the same process created the topic, produced the backlog and
      # ran the AK core arm with an ordinary Kafka client, because a comparison needs both sides. A
      # genuinely foreign application carries no Kafka client library at all, which is the property
      # this arm stands in for.
      def sidecar(target)
        puts "\n=== #{SIDECAR_ARM} starting over #{target} records ==="
        completion = Completion.new(target)
        client = ParallelConsumer::Client.open(client_options(group_id(SIDECAR_ARM)),
                                               sidecar: @sidecar.path, sidecar_args: @sidecar.args)
        puts "#{SIDECAR_ARM}: the proxy granted #{client.session.executor_count} executor threads, " \
             "ceiling #{client.session.max_concurrency}"
        started = Demo.clock
        run_sidecar_arm(client, completion)
        elapsed = Demo.clock - started
        # Re-raises the session's first fatal error, if there was one, in preference to this demo's
        # own account of the symptom.
        client.close
        verdict(completion, elapsed)
      end

      def run_sidecar_arm(client, completion)
        client.poll do |_record|
          # THE SIMULATED WORK, and a blocking sleep is the right one for Ruby rather than merely
          # the allowed one: MRI releases the global VM lock around `sleep`, so N executor threads
          # sleeping is N records in flight, not one. The contract names Python and TypeScript as
          # the languages where this would be a lie; Ruby's executors are threads, not processes,
          # and the sleep occupies none of them.
          sleep(@options.delay_seconds)
          completion.record
          ParallelConsumer::Outcome.success
        end
        # The session ending short of the target must wake the waiter rather than leave it sitting
        # out the whole budget. `wait` re-raises the session's failure; `close` will raise it again
        # with the same object, and there it is not swallowed.
        Thread.new do
          client.wait
        rescue StandardError
          nil
        ensure
          completion.abandon
        end
        raise "#{SIDECAR_ARM} did not finish within #{ARM_BUDGET}s" unless completion.await(ARM_BUDGET)
      end

      # Reaching the target is not the only thing that ends the wait: a completed or failed session
      # ends it too. Without this check a broken run prints a plausible row at a plausible rate and
      # exits 0, which is the worst thing a demo can do.
      def verdict(completion, elapsed)
        count = completion.count
        raise "#{SIDECAR_ARM} ended early at #{count} of #{completion.target}" if count < completion.target

        finished(SIDECAR_ARM, elapsed, count)
      end

      def client_options(group)
        ParallelConsumer::ClientOptions.new(
          topics: [@topic],
          max_concurrency: @options.concurrency,
          ordering: :unordered,
          kafka_properties: @broker.consumer_properties(group)
        )
      end

      def finished(arm, elapsed, processed)
        puts "=== #{arm} finished: #{processed} records in #{(elapsed * 1000).round}ms ==="
        ArmResult.new(arm: arm, elapsed: elapsed, processed: processed)
      end

      def baseline_of(results)
        results.find { |result| result.arm == AK_CORE }
      end

      # A fresh group per arm per replay, so every arm reads the same records from the beginning.
      def group_id(arm)
        "pc-demo-#{arm.tr(' ', '-')}-#{Process.clock_gettime(Process::CLOCK_REALTIME, :nanosecond)}"
      end

      def report(title, results, baseline, across_replays:)
        puts "\n\n#{title}"
        puts format(HEADER, arm: "arm", elapsed: "elapsed", rate: "msg/s",
                            ratio: across_replays ? "vs AK core*" : "vs AK core")
        results.each { |result| puts row(result, baseline) }
        puts FOOTNOTE if across_replays
      end

      def row(result, baseline)
        ratio = if baseline.nil? || baseline.rate_per_second.zero?
                  "-"
                else
                  format("%<ratio>.1fx", ratio: result.rate_per_second / baseline.rate_per_second)
                end
        format(ROW, arm: result.arm, elapsed: result.elapsed,
                    rate: thousands(result.rate_per_second.to_i), ratio: ratio)
      end

      # 12345 => "12,345". The Java seed prints its rates with the platform's grouping separator, so
      # a reader comparing two languages' tables is not also comparing two number formats.
      def thousands(number)
        number.to_s.reverse.scan(/\d{1,3}/).join(",").reverse
      end
    end
  end
end
