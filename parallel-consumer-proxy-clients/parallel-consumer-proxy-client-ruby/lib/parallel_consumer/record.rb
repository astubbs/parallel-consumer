# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

module ParallelConsumer
  # One record as handed to the user's block: the Kafka record plus the delivery state an
  # in-process Parallel Consumer user function would see.
  #
  # Nothing transport-specific appears here - no tokens, no epochs, no connection identity. The
  # fencing token rides beside the record on the executor's own stack and is echoed to the proxy
  # verbatim; the user never sees it and this library never stores it.
  #
  # +key+ and +value+ are the BYTES Kafka held (binary-encoded strings, Ruby's byte string), since
  # the proxy never deserializes. Either may be +nil+, because Kafka distinguishes a null key or
  # value - a tombstone - from an empty one, and this surface preserves that distinction.
  #
  # DATA, NOT STRUCT, AND THE REASON IS THE FIELD NAMED +partition+. Struct includes Enumerable,
  # so a member called +partition+ silently overrides +Enumerable#partition+ - RuboCop's
  # Lint/StructNewOverride found it here. Data includes no such module, and an immutable value
  # object is what this wanted anyway: a record handed to N executor threads must not be
  # something one of them can edit.
  InboundRecord = Data.define(
    :topic, :partition, :offset, :key, :value, :attempt, :last_failure_at, :last_failure_reason
  ) do
    # Has this record failed on a previous delivery? Presence of the failure timestamp is the wire
    # form of "has failed before", so this reads it rather than inferring from the attempt count.
    def failed_before?
      !last_failure_at.nil?
    end

    # Deliberately omits the payload: a record's value can be large, and it is untrusted input
    # that inspection output routinely carries into a log line.
    def to_s
      "#{topic}/#{partition}/#{offset} (attempt #{attempt})"
    end
  end

  # A record for the proxy to produce on the worker's behalf, carried back on a successful
  # outcome. Workers never hold a producer: this is the only sanctioned route for output to Kafka.
  #
  # +key+ and +value+ are bytes here too, and +nil+ means a null key or value.
  OutboundRecord = Data.define(:topic, :key, :value)
end
