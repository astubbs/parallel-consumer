# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

module ParallelConsumer
  # The EFFECTIVE configuration the proxy replied with, including the negotiated capability set.
  #
  # ASSERT ON THIS, NEVER ON THE OPTIONS. Unset options mean "the engine's default", and the
  # handshake reply is where each default resolved to a value. The same rule governs features:
  # a capability is on for this session iff it appears here, whatever the client asked for.
  class Session
    attr_reader :topics, :topic_pattern, :max_concurrency, :executor_count, :capabilities

    # Builds a session from the handshake reply, checking the two fields whose absence the
    # specification calls a protocol violation rather than a default.
    #
    # @api private
    def self.from_configured(configured)
      unless configured.has_max_concurrency? && configured.max_concurrency >= 1
        raise ProtocolViolation,
              "Configured carried no usable max_concurrency - the in-flight ceiling is always " \
              "reported, and absence never means unlimited"
      end
      unless configured.has_executor_count? && configured.executor_count >= 1
        raise ProtocolViolation, "Configured carried no usable executor_count"
      end

      new(configured)
    end

    def initialize(configured)
      @topics = configured.topics.to_a.freeze
      @topic_pattern = configured.has_topic_pattern? ? configured.topic_pattern : nil
      @max_concurrency = configured.max_concurrency
      @executor_count = configured.executor_count
      @capabilities = configured.capabilities.to_a.freeze
      freeze
    end

    # Is this capability token in the negotiated set? Every gated duty asks this before it acts:
    # a duty whose token is absent does not exist on this session, and sending its messages anyway
    # would be the client's own protocol violation.
    def negotiated?(token)
      capabilities.include?(token.to_s)
    end

    def to_s
      "#<#{self.class.name} topics=#{topics.inspect} max_concurrency=#{max_concurrency} " \
        "executor_count=#{executor_count} capabilities=#{capabilities.inspect}>"
    end
    alias inspect to_s
  end
end
