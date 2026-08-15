# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "grpc"
require "parallelconsumer/proxy/v1/proxy_services_pb"

module ParallelConsumer
  # One session: one sidecar process, one gRPC stream, one dispatch queue, N executor threads.
  #
  #   client = ParallelConsumer::Client.open(options, sidecar: "/opt/pc/parallel-consumer-proxy")
  #   client.poll { |record| charge(record) }
  #   client.wait
  #   client.close
  #
  # or, with the resource block Ruby expects for anything that must be closed:
  #
  #   ParallelConsumer::Client.open(options, sidecar: path) do |client|
  #     client.poll { |record| charge(record) }
  #     client.wait(30)
  #   end
  #
  # == poll DOES NOT BLOCK
  #
  # {#poll} starts consumption and returns; {#wait} is how you block. The specification does not
  # settle this, so it is a decision, and it went this way for two reasons. A blocking poll makes
  # {#close} unreachable from the thread that called it, so every user would need a second thread
  # or a signal handler to shut down cleanly - and shutting down cleanly is the difference between
  # a drained consumer group and a rebalance. And the reference surface every language mirrors is
  # non-blocking, so a blocking poll here would be a shape divergence, not a translation. Ruby
  # already spells "start it, then wait for it" as two calls, +Thread#join+ being the model, so the
  # idiom does not have to be bent to get it.
  #
  # == The concurrency model is THREADS
  #
  # +Configured.executor_count+ executors are Ruby threads. The reasoning, which differs from
  # Python's for stated reasons, is in the module documentation of {ParallelConsumer} and in
  # docs/inflight/clients/ruby.md.
  #
  # == Stateless per record
  #
  # The fencing token rides from dispatch to report on the executor thread's own stack and is
  # echoed byte-identically - the same message object the proxy sent, never one rebuilt from parsed
  # parts. There is no request map, no dedupe cache and no completion registry, because a client
  # that holds no per-record state cannot have a per-record state bug. Fencing is the proxy's job.
  class Client
    V1 = Bz::Stub::ParallelConsumer::Proxy::V1

    # How long {#open} waits for the port line and for the handshake reply.
    DEFAULT_CONNECT_TIMEOUT = 30

    # How long {#close} waits for the proxy to finish its drain before it stops being polite.
    DEFAULT_CLOSE_GRACE = 15

    attr_reader :session

    class << self
      # Spawns the sidecar, connects, and completes the fresh-session handshake. Returns once the
      # proxy's effective configuration has arrived - only then is the session open.
      #
      # Given a block, yields the client and closes it afterwards, whatever happens.
      #
      # @param options [ClientOptions]
      # @param sidecar [String, SidecarCommand] the sidecar binary, as an ABSOLUTE path
      # @param sidecar_args [Array<String>] arguments for it. Configuration never travels here -
      #   it travels in the handshake and nowhere else
      def open(options, sidecar:, sidecar_args: [], **kwargs)
        client = new(options, sidecar: sidecar, sidecar_args: sidecar_args, **kwargs)
        return client unless block_given?

        begin
          yield client
        ensure
          client.close
        end
      end
    end

    def initialize(options, sidecar:, sidecar_args: [], connect_timeout: DEFAULT_CONNECT_TIMEOUT,
                   close_grace: DEFAULT_CLOSE_GRACE, logger: nil, stderr: :close)
      @options = options
      @close_grace = close_grace
      @logger = logger
      @executors = []
      @failure = nil
      @failure_mutex = Mutex.new
      @close_mutex = Mutex.new
      @closed = false

      @sidecar = Sidecar.new(SidecarCommand.coerce(sidecar, args: sidecar_args),
                             timeout: connect_timeout, stderr: stderr)
      begin
        connect(connect_timeout)
      rescue StandardError
        shutdown_transport
        @sidecar.stop(@close_grace)
        raise
      end
    end

    # Starts consumption. The block is the user's function: it receives an {InboundRecord} and may
    # return an {Outcome}, return anything else (a success), or raise (a failure, reason verbatim).
    #
    # Returns immediately - see the class documentation. At most once per client.
    def poll(&processor)
      raise ArgumentError, "poll needs a block - it IS the user's function" unless processor
      raise AlreadyPollingError, "poll has already been called on this client" unless @executors.empty?

      @session.executor_count.times do |index|
        @executors << Thread.new { executor_loop(processor, index) }
      end
      nil
    end

    # Blocks until the session ends - the proxy completing the stream, {#close}, or a fatal error.
    #
    # @param timeout [Numeric, nil] seconds to wait; +nil+ waits indefinitely
    # @return [Boolean] whether the session had ended when this returned
    def wait(timeout = nil)
      ended = !(timeout.nil? ? @receiver.join : @receiver.join(timeout)).nil?
      raise_any_failure
      ended
    end

    # The client-initiated shutdown, and the only correct way to end a session.
    #
    # Stop hand-out; deal with what is still queued; let executing records finish and report
    # normally; then HALF-CLOSE the stream. The half-close is the shutdown signal - there is no
    # shutdown-request message, because a client that has reported or released everything has
    # nothing left to say. Only then is the sidecar reaped, by closing its lifecycle pipe.
    #
    # Idempotent. Re-raises the session's first fatal error, if there was one.
    def close
      # Under a mutex, not a bare flag: an application closing from a signal handler while the
      # block form of Client.open closes from its ensure is an ordinary way to arrive here twice
      # at once, and half a shutdown running twice is worse than one running late.
      @close_mutex.synchronize do
        return if @closed

        @closed = true
      end
      undelivered = @queue.stop_handout
      release_or_discard(undelivered)
      @executors.each(&:join)
      @outbound.close # half-close: the write loop ends once the queued reports have gone out
      @receiver.join(@close_grace) || @operation.cancel
      shutdown_transport
      @sidecar.stop(@close_grace)
      raise_any_failure
      nil
    end

    private

    def connect(connect_timeout)
      # An ordinary host:port authority, which is what the proxy's loopback allowlist expects. No
      # TLS, no interceptors, no load balancing, no per-call deadline: the protocol uses a narrow
      # slice of gRPC deliberately, so that every language's implementation suffices.
      target = "127.0.0.1:#{@sidecar.port}"
      @channel = GRPC::Core::Channel.new(target, {}, :this_channel_is_insecure)
      stub = V1::ProxyService::Stub.new(target, nil, channel_override: @channel)

      @outbound = Thread::Queue.new
      @handshake = Thread::Queue.new

      # gRPC drives this enumerator from its own write thread, so it is the single place messages
      # are serialized onto the stream - every executor just pushes. Closing the queue ends the
      # enumerator, which is the half-close.
      requests = Enumerator.new do |stream|
        while (message = @outbound.pop)
          stream << message
        end
      end

      # return_op: the operation view is the only handle that can CANCEL the call, which is the
      # strongest thing a gRPC CLIENT can do to a stream - see ProtocolViolation.
      @operation = stub.session(requests, return_op: true) { |message| receive(message) }

      emit(V1::ClientMessage.new(configure: @options.to_configure))
      @receiver = Thread.new { receive_loop }

      @handshake.pop(timeout: connect_timeout)
      raise_any_failure
      raise SessionError, "no Configured arrived within #{connect_timeout}s" if @session.nil?
    end

    def receive_loop
      @operation.execute
    rescue GRPC::Cancelled, GRPC::Unavailable
      nil # the session ended: this client cancelled it, or the proxy completed the stream
    rescue StandardError => e
      record_failure(SessionError.new("the session stream ended: #{e.class}: #{e.message}"))
    ensure
      @handshake.close
    end

    # The admin loop's body. IT ALWAYS READS - backpressure is never applied by not reading, since
    # this stream also carries the control plane and an admin that stops reading head-of-line-blocks
    # itself.
    def receive(message)
      case message.message
      when :configured then configured(message.configured)
      when :dispatch then enqueue(message.dispatch)
      else unnegotiated(message)
      end
    end

    # THE SESSION AND THE QUEUE ARE BUILT HERE, ON THE RECEIVING THREAD, and not by the thread
    # waiting on the handshake. The proxy may send a dispatch wave immediately behind its
    # +Configured+, so anything the wave's handler needs must exist before this method returns -
    # building the queue on the waiting thread leaves a window in which a wave arrives and finds
    # no queue.
    def configured(effective)
      @session = Session.from_configured(effective)
      @queue = DispatchQueue.new(@session.max_concurrency)
      @handshake.push(true)
    rescue ProtocolViolation => e
      record_failure(e)
      @operation.cancel
    ensure
      @handshake.close
    end

    def enqueue(dispatch)
      dispatch.records.each { |record| @queue.offer(record) }
    rescue ProtocolViolation => e
      # A gRPC client cannot answer with a status - only a server can. Cancelling the call is the
      # strongest equivalent, and the error names the count.
      record_failure(e)
      @operation.cancel
    end

    # Every proxy message other than Dispatch is gated by a capability this client does not
    # declare. The rule for one arriving anyway is that the receiver never ACTS on it; recording it
    # keeps the violation visible without tearing down an otherwise healthy stream.
    def unnegotiated(message)
      record_failure(ProtocolViolation.new(
                       "the proxy sent #{message.message} outside the negotiated capability set " \
                       "#{@session&.capabilities.inspect} - ignored"
                     ))
    end

    def executor_loop(processor, index)
      while (dispatched = @queue.take)
        run_one(processor, dispatched)
      end
    rescue StandardError => e
      record_failure(SessionError.new("executor #{index} died: #{e.class}: #{e.message}"))
    end

    def run_one(processor, dispatched)
      outcome = invoke(processor, inbound(dispatched.record, dispatched))
      # The token is echoed VERBATIM: the very message the proxy sent. It is opaque - nothing here
      # reads record_id or compares epochs.
      emit(V1::ClientMessage.new(report: report_for(dispatched.token, outcome)))
    ensure
      # REPORTED, SO NO LONGER UNRESOLVED. Taking the record off the queue never freed its slot
      # against the proxy's ceiling - only this does. In an +ensure+ so that an executor dying
      # mid-record cannot leave the ceiling permanently short of a slot.
      @queue.settle
    end

    # Runs the user's block, translating a raised exception into a failure outcome - once, in one
    # place. A block that blows up must produce a failure report, not tear the stream down.
    def invoke(processor, record)
      Outcome.coerce(processor.call(record))
    rescue StandardError => e
      Outcome.failure("#{e.class}: #{e.message}")
    end

    def report_for(token, outcome)
      report = V1::Report.new(token: token)
      if outcome.success?
        report.success = V1::Report::Success.new(
          produce: outcome.produce.map do |out|
            V1::ProduceRecord.new(topic: out.topic, key: out.key, value: out.value)
          end
        )
      else
        failure = V1::Report::Failure.new
        failure.reason = outcome.reason if outcome.reason
        report.failure = failure
      end
      report
    end

    def inbound(record, dispatched)
      InboundRecord.new(
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        # Absent is a null key or value - a tombstone is not an empty value - so presence is read
        # rather than the field, which would flatten both onto "".
        key: record.has_key? ? record.key : nil,
        value: record.has_value? ? record.value : nil,
        attempt: dispatched.attempt,
        last_failure_at: dispatched.has_last_failure_at? ? dispatched.last_failure_at.to_time : nil,
        last_failure_reason: dispatched.has_last_failure_reason? ? dispatched.last_failure_reason : nil
      )
    end

    # Queued-but-never-run records at shutdown.
    #
    # THE SPECIFICATION ASKS FOR TWO INCOMPATIBLE THINGS HERE. Its shutdown section says to report
    # every queued record +Released+; its negotiation rule forbids sending any message outside the
    # negotiated set, and +Released+ is gated by +shutdown+. On a session that did not negotiate
    # +shutdown+ - which is every session the conformance harness serves today - there is no legal
    # message for these records. Sending one anyway would be this client's own violation, so the
    # queue is discarded instead and the proxy reclaims the records with their attempt counts
    # unchanged, which loses nothing. Recorded in docs/inflight/clients/ruby.md; Python and Go
    # resolved it the same way.
    def release_or_discard(undelivered)
      return if undelivered.empty?

      unless @session.negotiated?("shutdown")
        @logger&.debug { "discarding #{undelivered.size} queued record(s): 'shutdown' is not negotiated" }
        return
      end

      undelivered.each do |dispatched|
        emit(V1::ClientMessage.new(
               report: V1::Report.new(token: dispatched.token, released: V1::Report::Released.new)
             ))
      end
    end

    # Named +emit+ rather than +send+ on purpose: +send+ is +Object#send+, and shadowing it inside
    # a class whose objects are handed to user code is how a library acquires a mystery.
    def emit(message)
      @outbound.push(message)
    rescue ClosedQueueError
      # The stream is already half-closed. A report arriving after that is the caller's race, not
      # a fault to escalate; the record redelivers.
      @logger&.debug { "dropped a #{message.message} after half-close" }
    end

    def shutdown_transport
      @channel&.close
    rescue StandardError => e
      @logger&.debug { "closing the channel: #{e.class}: #{e.message}" }
    end

    # The FIRST fatal error wins: later ones are usually its consequences, and the first is the
    # one a bug report needs.
    def record_failure(error)
      @failure_mutex.synchronize { @failure ||= error }
      nil
    end

    def raise_any_failure
      failure = @failure_mutex.synchronize { @failure }
      raise failure if failure
    end
  end
end
