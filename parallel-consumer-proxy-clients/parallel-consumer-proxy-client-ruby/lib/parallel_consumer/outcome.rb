# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

module ParallelConsumer
  # The per-record verdict: success (optionally carrying records for the proxy to produce) or
  # failure (carrying a reason). A closed two-armed value, mirroring the reference surface - there
  # is deliberately no third arm, because a block that cannot decide has not finished processing.
  #
  # RUBY'S IDIOM IS THAT A BLOCK RETURNS A VALUE OR RAISES, and this surface leans on it rather
  # than making every user construct an Outcome:
  #
  #   client.poll { |record| charge(record) }                      # returns => success
  #   client.poll { |record| raise "no such customer" }            # raises  => failure, verbatim
  #   client.poll { |record| Outcome.failure("declined") }         # explicit failure
  #   client.poll { |record| Outcome.success(produce: [OutboundRecord.new(...)]) }
  #
  # A block that returns anything other than an Outcome has succeeded. That is the whole rule, and
  # it means the common case - a block that raises on trouble, like every other Ruby block -
  # needs no knowledge of this class at all.
  class Outcome
    # The records the proxy should produce before this record's offset may advance. Always an
    # array, empty on a plain success.
    attr_reader :produce

    # The failure text, which rides back to the worker on the redelivery as
    # InboundRecord#last_failure_reason. +nil+ on a success.
    attr_reader :reason

    class << self
      # Success. Pass +produce:+ to have the proxy write records on this worker's behalf.
      def success(produce: [])
        new(success: true, produce: produce, reason: nil)
      end

      # Failure: the record goes back to the engine's retry scheduling, exactly as an in-process
      # user function raising would. The reason travels with the redelivery.
      def failure(reason = nil)
        new(success: false, produce: [], reason: reason)
      end

      # Coerces whatever a user's block returned into an Outcome. Anything that is not already one
      # is a success - see the class comment.
      def coerce(returned)
        returned.is_a?(Outcome) ? returned : success
      end
    end

    def initialize(success:, produce:, reason:)
      @success = success
      @produce = produce.freeze
      @reason = reason
      freeze
    end

    def success?
      @success
    end

    def failure?
      !@success
    end
  end
end
