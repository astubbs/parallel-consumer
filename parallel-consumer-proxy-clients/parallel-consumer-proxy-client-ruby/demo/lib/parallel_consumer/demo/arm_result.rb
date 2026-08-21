# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

module ParallelConsumer
  module Demo
    # What one arm achieved: how long it took, and over how many records.
    #
    # THROUGHPUT IS THE ONLY FIGURE, and the absence of a latency field is the contract rather than
    # an omission. The backlog is pre-produced, so the workload is closed-loop and per-record
    # timings are flattered by however far an arm fell behind.
    ArmResult = Data.define(:arm, :elapsed, :processed) do
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
        @abandoned = false
        @mutex = Mutex.new
        @progress = ConditionVariable.new
      end

      def count
        @mutex.synchronize { @count }
      end

      def record
        @mutex.synchronize do
          @count += 1
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
