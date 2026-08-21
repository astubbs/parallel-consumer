# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

module ParallelConsumer
  module Demo
    # The demo's dials, and a transcription of the interface every per-language demo mirrors -
    # `parallel-consumer-proxy/demo/README.md`, whose reference implementation is Java's
    # `DemoOptions`.
    #
    # FLAGS BEAT THE ENVIRONMENT BEATS THE DEFAULTS, the ordinary convention, and the reason it is
    # stated rather than implied is that both layers have to be able to override the other: a
    # container passes configuration by environment, a person at a terminal passes flags.
    #
    # R39 DOES NOT GOVERN A DEMO. R39 constrains how configuration reaches the *proxy*; a demo is an
    # application, so `--records` is not a violation of it. Without this note someone reads the flag
    # as breaking the plan's own rule and deletes it (plan unit U35, step 5).
    class Options
      # Prefix for every environment variable this demo reads, so a reader can grep one string.
      ENV_PREFIX = "PC_DEMO_"

      # flag => [attribute, kind]. ONE table, because the flag names, the environment names and the
      # usage text are three views of the same list and a demo whose `--help` and parser disagree is
      # worse than one with no help at all. `:positive` is >= 1; `:count` allows zero, which
      # --replay-factor needs (1 or less skips the big replay) and --delay-ms allows on purpose.
      SPEC = {
        "--records" => %i[records positive],
        "--delay-ms" => %i[delay_ms count],
        "--concurrency" => %i[concurrency positive],
        "--partitions" => %i[partitions positive],
        "--replay-factor" => %i[replay_factor count],
        "--bootstrap" => %i[bootstrap text],
        "--topic" => %i[topic text]
      }.freeze

      DEFAULTS = {
        records: 2000, delay_ms: 2, concurrency: 100, partitions: 10, replay_factor: 20,
        bootstrap: nil, topic: nil
      }.freeze

      # More records than any demo should replay. The Java seed checks this to catch a silent int
      # overflow in `records * replayFactor`; Ruby's integers do not overflow, so here it is only
      # what it says - a guard against a run nobody meant to ask for, kept because the two demos
      # refusing different inputs is a divergence a reader would have to discover by trying it.
      MAX_REPLAY_RECORDS = (2**31) - 1

      # Raised for a flag this demo does not have, a flag with no value, or a value that is not a
      # number. NOT rescued into a default: a demo that silently ignores a misspelled flag reports
      # numbers for settings the user did not ask for.
      class UsageError < StandardError; end

      attr_reader(*DEFAULTS.keys)

      # @param argv [Array<String>] the process arguments, which may legitimately be empty - that is
      #   the double-click case, and it must work
      # @param env [Hash] the environment, passed in rather than read from ENV so this is testable
      def initialize(argv, env)
        values = DEFAULTS.merge(from_environment(env)).merge(from_arguments(argv))
        DEFAULTS.each_key { |name| instance_variable_set(:"@#{name}", values[name]) }
        validate!
        freeze
      end

      def self.help_requested?(argv)
        argv.intersect?(["-h", "--help"])
      end

      # The simulated work, in the unit Ruby's `sleep` takes.
      def delay_seconds
        delay_ms / 1000.0
      end

      # The records the big replay consumes in total, including the small replay's own.
      def big_replay_records
        records * [replay_factor, 1].max
      end

      # True when the big replay is worth running at all; a factor of 1 or less skips it.
      def big_replay?
        replay_factor > 1
      end

      # The effective configuration, for printing before the run.
      #
      # A number without its settings is not reproducible, so this is part of the contract every
      # language's demo keeps rather than a debugging aid. THE BOOTSTRAP ADDRESS IS DELIBERATELY
      # ABSENT: own-cluster mode puts a user's real broker address there, and the credential-hygiene
      # rule that binds the proxy binds a demo too - nothing logged, nothing echoed. The key
      # spelling is the Java seed's, not Ruby's, so that two demos' fingerprints can be read
      # side by side without translating.
      def to_s
        "records = #{records}\n  delayMs = #{delay_ms}\n  maxConcurrency = #{concurrency}" \
          "\n  partitions = #{partitions}\n  replayFactor = #{replay_factor}"
      end

      def self.usage
        <<~USAGE
          usage: demo/run.sh [options]

            --records N        records in the comparison replay   (default 2000)
            --delay-ms N       simulated work per record, ms      (default 2)
            --concurrency N    max in-flight records              (default 100)
            --partitions N     partitions on the demo topic       (default 10)
            --replay-factor N  big replay = records x N; 1 skips  (default 20)
            --bootstrap ADDR   an existing broker; omit to start one
            --topic NAME       an existing topic; omit to create one

          Every flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
          Flags beat the environment beats the defaults.
        USAGE
      end

      private

      def from_arguments(argv)
        values = {}
        remaining = argv.dup
        until remaining.empty?
          flag = remaining.shift
          attribute, kind = SPEC[flag]
          raise UsageError, "unknown option: #{flag}" unless attribute
          raise UsageError, "#{flag} needs a value" if remaining.empty?

          values[attribute] = coerce(flag, remaining.shift, kind)
        end
        values
      end

      # An environment variable that is unset, empty or whitespace means "not supplied" - it must
      # not read as a request for zero. Compose forwards every PC_DEMO_ variable to the demo
      # container whether the caller set it or not, so blank arrives routinely rather than rarely.
      def from_environment(env)
        SPEC.each_value.with_object({}) do |(attribute, kind), values|
          name = "#{ENV_PREFIX}#{attribute.to_s.upcase}"
          raw = env[name]
          next if raw.nil? || raw.strip.empty?

          values[attribute] = coerce(name, raw.strip, kind)
        end
      end

      def coerce(source, raw, kind)
        return raw if kind == :text

        number = Integer(raw.strip, exception: false)
        raise UsageError, "#{source} needs a whole number, got '#{raw}'" if number.nil?
        raise UsageError, "#{source} must be at least 1, got #{number}" if kind == :positive && number < 1
        raise UsageError, "#{source} must not be negative, got #{number}" if number.negative?

        number
      end

      def validate!
        return if big_replay_records <= MAX_REPLAY_RECORDS

        raise UsageError, "--records times --replay-factor is #{big_replay_records}, which is more " \
                          "records than the demo can sensibly replay; lower one of them"
      end
    end
  end
end
