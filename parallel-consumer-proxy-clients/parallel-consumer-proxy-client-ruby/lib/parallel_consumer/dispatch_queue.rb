# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

module ParallelConsumer
  # The client-side dispatch queue: the gap between the proxy's in-flight ceiling and this
  # client's executor count. Its rules are normative for every client in every language, which is
  # why they are implemented here rather than improvised at the call sites.
  #
  # RUBY'S OBVIOUS ANSWER, +Thread::SizedQueue+, IS THE WRONG ONE, and the reason is the first
  # rule: its +push+ BLOCKS when the queue is full, and the only thread that pushes here is the
  # one reading the session stream. That stream also carries the control plane, so an admin that
  # stops reading to slow the proxy down head-of-line-blocks itself. This queue therefore never
  # blocks a producer - it raises instead.
  #
  # And raising is right, because the depth is +Configured.max_concurrency+ - the proxy's OWN
  # declared in-flight ceiling. A full queue means the proxy exceeded its own ceiling, so overflow
  # is a protocol violation and never a load condition. Never drop a record, never grow the queue.
  #
  # WHAT THE CEILING COUNTS IS *UNRESOLVED* RECORDS - queued PLUS executing - not the depth of the
  # array. A record handed to an executor has left the array but is still in flight against the
  # proxy's ceiling, and only its report frees the slot ({#settle}). Counting the array alone would
  # let hand-out make room, so the guide's own worked example - a fourth record arriving while
  # three are unresolved, two of them executing - could never be detected here.
  #
  # Hand-out is FIFO: by arrival, and within one dispatch wave by the wave's record order. FIFO is
  # not an ordering guarantee - shard ordering is the engine's - it is the one order every
  # language expresses identically, which keeps the clients comparable.
  class DispatchQueue
    attr_reader :depth

    def initialize(depth)
      @depth = depth
      @items = []
      @unresolved = 0
      @stopped = false
      @mutex = Mutex.new
      @available = ConditionVariable.new
    end

    # Queues one record. NEVER BLOCKS: raises {ProtocolViolation} when the proxy already has its
    # declared ceiling of records unresolved.
    def offer(item)
      @mutex.synchronize do
        raise ProtocolViolation, overflow_message(@unresolved) if @unresolved >= @depth

        @unresolved += 1
        @items.push(item)
        @available.signal
      end
    end

    # One record reached a verdict and was reported: it stops counting against the ceiling. This is
    # the ONLY thing that frees a slot for an executing record - {#take} never did.
    def settle
      @mutex.synchronize { @unresolved -= 1 if @unresolved.positive? }
      nil
    end

    # Takes the next record, waiting for one. Returns +nil+ once hand-out has stopped, which is
    # how an executor thread learns to finish.
    def take
      @mutex.synchronize do
        @available.wait(@mutex) while @items.empty? && !@stopped
        @items.shift
      end
    end

    # Stops hand-out and returns everything still queued, in order.
    #
    # The caller decides what those records deserve, because the answer is capability-dependent:
    # with +shutdown+ negotiated they are reported +Released+; without it there is no legal message
    # to send, so they are discarded and the proxy reclaims them. Either way they are NOT run -
    # the client never invents a verdict for work it did not do, and never runs work it was told
    # to stop handing out.
    def stop_handout
      @mutex.synchronize do
        @stopped = true
        undelivered = @items
        @items = []
        # Released or discarded, these records get no report, so nothing else will ever settle
        # them. Executing records are untouched: they keep running and report normally.
        @unresolved -= undelivered.size
        @available.broadcast
        undelivered
      end
    end

    # Queued but not yet handed to an executor.
    def size
      @mutex.synchronize { @items.size }
    end

    # Dispatched and not yet reported - queued plus executing. THIS is what the ceiling bounds.
    def unresolved
      @mutex.synchronize { @unresolved }
    end

    private

    def overflow_message(unresolved)
      "the proxy dispatched a record while #{unresolved} were already unresolved - queued plus " \
        "executing - past the max_concurrency of #{@depth} it declared itself: the proxy exceeded " \
        "its own in-flight ceiling, which is a protocol violation, not load"
    end
  end
end
