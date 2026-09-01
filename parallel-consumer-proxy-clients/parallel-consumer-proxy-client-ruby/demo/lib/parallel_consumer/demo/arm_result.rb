# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

module ParallelConsumer
  module Demo
    # What one arm achieved: how long it took, over how many records, and across how many keys.
    #
    # THROUGHPUT IS NOT THE ONLY FIGURE, AND IT CANNOT BE. A rate on its own cannot show the work
    # happened - an arm that processed half the backlog looks FASTER than one that processed all of
    # it. `processed` and `unique_keys` are what make the table demonstrate the run rather than
    # assert it: the first must equal the target, and the second shows the backlog was spread over
    # many keys rather than one repeated.
    #
    # They are also the only two figures here that are DETERMINISTIC, which is why the conformance
    # harness can compare them across languages and can compare nothing else.
    #
    # NO LATENCY FIELD, and its absence is contract rather than omission. The backlog is
    # pre-produced, so the workload is closed-loop and per-record timings are flattered by however
    # far an arm fell behind.
    ArmResult = Data.define(:arm, :elapsed, :processed, :unique_keys) do
      def rate_per_second
        elapsed.positive? ? processed / elapsed : 0.0
      end
    end

    # The finish line for an arm that runs its records concurrently: N of them are done, or the
    # session ended without them.
    #
    # A MUTEX AND A CONDITION VARIABLE RATHER THAN A COUNTER AND A SLEEP, because the executors are
    # threads sharing this object and the arm's clock stops at the moment the last record completes
    # - a polling loop would charge the arm up to one poll interval of nothing.
    class Completion
      attr_reader :target

      def initialize(target)
        @target = target
        @count = 0
        # NO `require "set"` ANYWHERE IN THIS DEMO, and its absence is deliberate rather than an
        # oversight: Set is an autoloaded constant from Ruby 3.2, which is this module's floor, and
        # RuboCop's Lint/RedundantRequireStatement rejects the require outright.
        @keys = Set.new
        @abandoned = false
        @mutex = Mutex.new
        @progress = ConditionVariable.new
      end

      def count
        @mutex.synchronize { @count }
      end

      # How many distinct keys the executors saw between them.
      #
      # THE SET IS UNDER THE SAME MUTEX AS THE COUNTER, not beside it. N executor threads reach
      # this object at once - a bare Set here would be the one piece of shared mutable state in the
      # demo without a lock on it, and would under-count silently rather than raise.
      def unique_keys
        @mutex.synchronize { @keys.size }
      end

      # @param key [String, nil] the record's key as Kafka held it; nil is a legitimate key and
      #   counts as one distinct value, which is what a Set gives for free
      def record(key)
        @mutex.synchronize do
          @count += 1
          @keys << key
          @progress.broadcast if @count >= @target
        end
        nil
      end

      # Wakes the waiter without the target having been reached: the session ended underneath it.
      #
      # WITHOUT THIS AN ARM WHOSE SESSION DIED WOULD SIT OUT ITS WHOLE BUDGET before saying so, and
      # the reader would watch ten silent minutes of a run that was already over. The waiter then
      # reports too few records rather than a stall, which is the truer statement.
      def abandon
        @mutex.synchronize do
          @abandoned = true
          @progress.broadcast
        end
        nil
      end

      # @return [Boolean] whether the target was reached inside the budget
      def await(budget)
        deadline = Demo.clock + budget
        @mutex.synchronize do
          loop do
            return true if @count >= @target
            return false if @abandoned

            remaining = deadline - Demo.clock
            return false if remaining <= 0

            @progress.wait(@mutex, remaining)
          end
        end
      end
    end

    # The clock every duration in this demo is measured on. Monotonic, so a wall-clock correction
    # arriving mid-run cannot turn an arm's elapsed time into a negative number or a record one.
    def self.clock
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
