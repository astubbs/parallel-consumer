# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "spec_helper"

# Wave one's whole claim: one record, end to end, against the real wire.
#
# The spec name carries the conformance scenario name verbatim, because that name is the suite's
# identity in every language. The committed offset itself is engine state no client can see, and
# the harness has no verdict channel - it exits 0 whatever happened - so the client-side assertion
# is the wire-observable consequence: the record arrives once, the success report is followed by
# silence rather than a redelivery, and the session closes cleanly.
RSpec.describe "a-processed-record-advances-the-committed-offset" do
  # How long the spec watches for a second delivery after reporting success. The harness's
  # redelivery path is fast, so this waits for an event that should never come rather than racing
  # one that should.
  let(:redelivery_settle) { 3 }

  let(:scenario) { Harness::PROCESSED_RECORD_ADVANCES_OFFSET }
  let(:sidecar) { Harness.for_scenario(scenario) }

  let(:options) do
    ParallelConsumer::ClientOptions.new(
      # THE SCENARIO NAME IS ALSO THE TOPIC NAME.
      topics: [scenario],
      # The mock harness builds mock Kafka clients and reads no properties. Real credentials never
      # belong in a conformance test.
      kafka_properties: {},
      instance_tag: "ruby-client-wave-one"
    )
  end

  it "carries one record through a worker thread and reports it once" do
    seen = []
    seen_mutex = Mutex.new
    first = Thread::Queue.new

    client = ParallelConsumer::Client.open(options, sidecar: sidecar.path, sidecar_args: sidecar.args)
    begin
      # Assert what came back, never what was asked for.
      expect(client.session.max_concurrency).to be >= 1
      expect(client.session.executor_count).to be >= 1
      expect(client.session).to be_negotiated("dispatch")

      client.poll do |record|
        seen_mutex.synchronize { seen << record }
        first.push(true) if first.empty?
      end

      expect(first.pop(timeout: 60)).to be(true), "no record was dispatched before the deadline"

      # A success is followed by silence. Had the report not landed, or not been honoured, the
      # record would come back.
      sleep redelivery_settle
    ensure
      client.close
    end

    delivered = seen_mutex.synchronize { seen.dup }
    expect(delivered.size).to eq(1), "the record was delivered #{delivered.size} times: #{delivered.map(&:to_s)}"

    record = delivered.first
    expect(record.topic).to eq(scenario)
    expect(record.attempt).to eq(1)
    expect(record).not_to be_failed_before
    expect(record.value).not_to be_nil
    expect(record.value).not_to be_empty
  end
end
