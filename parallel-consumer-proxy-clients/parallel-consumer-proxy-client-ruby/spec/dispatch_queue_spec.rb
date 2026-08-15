# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "spec_helper"

# The dispatch queue's normative rules (client-authoring guide §3, KTD39), which the end-to-end
# spec cannot reach: it dispatches ONE record, so FIFO hand-out past the executor count and the
# overflow negative control never fire there. This is the client half of the conformance scenario
# `the-client-queue-hands-out-fifo-and-releases-on-shutdown`, asserted directly - its harness
# support lands with a later engine unit.
#
# The overflow specs are written in the guide's own worked shape: records OUT WITH EXECUTORS, not
# merely queued. A queue that counted only its array passes every other spec in this file.

RSpec.describe ParallelConsumer::DispatchQueue do
  # Stands in for a DispatchRecord: the queue is deliberately ignorant of what it carries.
  def record(id) = { token: id }

  describe "hand-out" do
    it "is FIFO, by arrival and within a wave by the wave's own order" do
      queue = described_class.new(3)
      %w[a b c].each { |id| queue.offer(record(id)) }

      expect([queue.take, queue.take, queue.take]).to eq([record("a"), record("b"), record("c")])
    end

    it "releases an executor waiting on an empty queue when hand-out stops" do
      queue = described_class.new(2)
      taken = Thread.new { queue.take }
      # Parked on the condition variable, which is where an idle executor waits. Bounded so a
      # queue that never parks fails here rather than hanging to the spec_helper timeout.
      deadline = Time.now + 5
      sleep 0.01 until taken.status == "sleep" || Time.now > deadline

      queue.stop_handout

      expect(taken.value).to be_nil
    end
  end

  describe "the in-flight ceiling" do
    it "still counts a record that an executor took, so hand-out never makes room" do
      queue = described_class.new(3)
      %w[a b c].each { |id| queue.offer(record(id)) }

      queue.take # executor-1 takes a
      queue.take # executor-2 takes b

      expect(queue.size).to eq(1)
      expect(queue.unresolved).to eq(3)
    end

    it "is a protocol violation when a fourth record arrives with three unresolved" do
      queue = described_class.new(3)
      %w[a b c].each { |id| queue.offer(record(id)) }
      queue.take
      queue.take

      expect { queue.offer(record("d")) }
        .to raise_error(ParallelConsumer::ProtocolViolation,
                        /3 were already unresolved.*max_concurrency of 3/m)
    end

    it "is freed by the report, and by nothing else" do
      queue = described_class.new(1)
      queue.offer(record("a"))
      queue.take

      expect { queue.offer(record("b")) }.to raise_error(ParallelConsumer::ProtocolViolation)

      queue.settle

      expect { queue.offer(record("b")) }.not_to raise_error
    end

    it "raises rather than blocking the thread that reads the stream" do
      queue = described_class.new(1)
      queue.offer(record("a"))

      # A blocking offer would head-of-line-block the control plane, which is rule 1. Any answer
      # other than an immediate raise hangs this example until the spec_helper timeout.
      expect { queue.offer(record("b")) }.to raise_error(ParallelConsumer::ProtocolViolation)
    end
  end

  describe "stopping hand-out" do
    it "returns what was never handed out, in order, and stops counting it" do
      queue = described_class.new(4)
      %w[a b c].each { |id| queue.offer(record(id)) }
      queue.take # a is out with an executor and keeps running

      undelivered = queue.stop_handout

      expect(undelivered).to eq([record("b"), record("c")])
      expect(queue.unresolved).to eq(1) # only the executing record still occupies the ceiling
      expect(queue.take).to be_nil # a stopped, drained queue hands out nothing
    end
  end
end
