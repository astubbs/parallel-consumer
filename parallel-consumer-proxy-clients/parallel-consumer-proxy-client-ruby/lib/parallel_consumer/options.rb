# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "google/protobuf/well_known_types"
require "parallelconsumer/proxy/v1/proxy_pb"

module ParallelConsumer
  # Connect-time configuration: what the user tells Parallel Consumer, expressed once.
  #
  # CONFIGURATION IS CODE. It travels in the handshake and nowhere else - this library reads no
  # file, no environment variable and no shell, and passes nothing to the sidecar by argv. Unset
  # means "take the engine's default", so this class holds no defaults of its own and never
  # guesses one; the proxy answers with the value each default resolved to, and {Session} is what
  # you assert on.
  #
  # +kafka_properties+ carries credentials. It is never logged, never echoed in an error, and
  # {#inspect} omits it - which is not decoration: the natural rendering of a configuration object
  # is exactly how credentials reach a log line.
  class ClientOptions
    ORDERINGS = {
      unordered: :PROCESSING_ORDER_UNORDERED,
      partition: :PROCESSING_ORDER_PARTITION,
      key: :PROCESSING_ORDER_KEY
    }.freeze

    # The capability tokens THIS library implements, declared explicitly in +Configure+.
    #
    # DECLARING AN EMPTY LIST WOULD BE A BUG, not a shortcut: the specification reads an empty list
    # as "the v1 baseline", which claims heartbeats, the manifest reconnect, worker-death reporting
    # and the shutdown drain. Wave one implements none of those, so claiming them would have the
    # proxy expect heartbeats this client never sends and return the session's records the moment
    # its lease expired.
    CAPABILITIES = ["dispatch"].freeze

    attr_reader :topics, :topic_pattern, :max_concurrency, :kafka_properties, :ordering,
                :commit_interval, :default_message_retry_delay, :instance_tag

    # @param topics [Array<String>] topic-list subscription; exactly one of this and
    #   +topic_pattern+ must be given
    # @param topic_pattern [String, nil] regex subscription, in Java regex syntax (the proxy
    #   compiles it)
    # @param max_concurrency [Integer, nil] the proxy's in-flight ceiling, and therefore this
    #   client's dispatch-queue depth. Must be >= 1 when given
    # @param kafka_properties [Hash{String=>String}] Kafka client configuration, credentials
    #   included
    # @param ordering [Symbol, nil] one of +:key+, +:partition+, +:unordered+
    # @param commit_interval [Numeric, nil] seconds between offset commits
    # @param default_message_retry_delay [Numeric, nil] seconds before a failed record is retried
    # @param instance_tag [String, nil] instance tag for the engine's metrics and logging
    def initialize(topics: [], topic_pattern: nil, max_concurrency: nil, kafka_properties: {},
                   ordering: nil, commit_interval: nil, default_message_retry_delay: nil,
                   instance_tag: nil)
      @topics = Array(topics).freeze
      @topic_pattern = topic_pattern
      @max_concurrency = max_concurrency
      @kafka_properties = kafka_properties.freeze
      @ordering = ordering
      @commit_interval = commit_interval
      @default_message_retry_delay = default_message_retry_delay
      @instance_tag = instance_tag
      validate!
      freeze
    end

    # The +Configure+ message this configuration produces. Internal: the wire is not this
    # library's user-facing surface.
    #
    # @api private
    def to_configure
      message = Bz::Stub::ParallelConsumer::Proxy::V1::Configure.new(
        topics: topics,
        kafka_properties: kafka_properties,
        capabilities: CAPABILITIES
      )
      # Only what the user actually set is put on the wire. An unset option means "take the
      # engine's default", and writing a zero value instead would silently ask for something.
      optional_fields.each { |field, value| message.public_send(:"#{field}=", value) }
      message
    end

    # Deliberately omits kafka_properties - it may carry credentials, at any log level.
    def inspect
      "#<#{self.class.name} topics=#{topics.inspect} topic_pattern=#{topic_pattern.inspect} " \
        "max_concurrency=#{max_concurrency.inspect} ordering=#{ordering.inspect} " \
        "kafka_properties=(#{kafka_properties.size} entries, redacted)>"
    end
    alias to_s inspect

    private

    def optional_fields
      {
        topic_pattern: topic_pattern,
        max_concurrency: max_concurrency,
        ordering: ordering && ORDERINGS.fetch(ordering),
        commit_interval: commit_interval && duration(commit_interval),
        default_message_retry_delay: default_message_retry_delay && duration(default_message_retry_delay),
        pc_instance_tag: instance_tag
      }.compact
    end

    def validate!
      if topics.empty? == topic_pattern.nil?
        raise ConfigurationError,
              "exactly one of topics: and topic_pattern: is required (got topics=#{topics.inspect}, " \
              "topic_pattern=#{topic_pattern.inspect})"
      end
      if max_concurrency && max_concurrency < 1
        raise ConfigurationError, "max_concurrency must be >= 1 when given, got #{max_concurrency}"
      end
      return if ordering.nil? || ORDERINGS.key?(ordering)

      raise ConfigurationError, "ordering must be one of #{ORDERINGS.keys.inspect}, got #{ordering.inspect}"
    end

    def duration(seconds)
      whole = seconds.floor
      Google::Protobuf::Duration.new(seconds: whole, nanos: ((seconds - whole) * 1_000_000_000).round)
    end
  end
end
