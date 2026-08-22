# Copyright (C) 2026 Antony Stubbs and contributors

# frozen_string_literal: true

module ParallelConsumer
  # Base class for everything this library raises, so an application can rescue the library as a
  # whole without listing its errors.
  class Error < StandardError; end

  # The options handed to {Client.open} cannot produce a legal +Configure+.
  class ConfigurationError < Error; end

  # The sidecar process could not be started, or never announced its port.
  class SidecarError < Error; end

  # The session ended, or could not be opened. Carries the underlying gRPC error as its +cause+
  # where there was one.
  class SessionError < Error; end

  # The peer broke the protocol. Distinct from {SessionError} deliberately: a session error is
  # something that happened TO the session, while this is something the proxy did that the
  # specification forbids, and the difference is what a bug report needs.
  #
  # THE SPECIFICATION ASKS FOR SOMETHING A gRPC CLIENT CANNOT DO here. The authoring guide's
  # dispatch-queue section says to answer a queue overflow by failing the stream with
  # +FAILED_PRECONDITION+ - but only the SERVER side of a gRPC call sets a status; a client can
  # cancel the call and nothing more. So the client cancels and raises this, naming the count. The
  # Python and Go waves reached the same conclusion independently; recorded in
  # docs/inflight/clients/ruby.md.
  class ProtocolViolation < Error; end

  # {Client#poll} was called twice. The poll-with-a-block shape is at most once per client.
  class AlreadyPollingError < Error; end
end
