# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "google/protobuf/well_known_types"

require "parallel_consumer/errors"
require "parallel_consumer/outcome"
require "parallel_consumer/record"
require "parallel_consumer/options"
require "parallel_consumer/session"
require "parallel_consumer/dispatch_queue"
require "parallel_consumer/sidecar"
require "parallel_consumer/client"

# Ordered concurrent consumption from Kafka, with one consumer, in Ruby.
#
#   require "parallel_consumer"
#
#   options = ParallelConsumer::ClientOptions.new(
#     topics: ["orders"],
#     max_concurrency: 64,
#     kafka_properties: { "bootstrap.servers" => "localhost:9092", "group.id" => "orders" }
#   )
#
#   ParallelConsumer.open(options, sidecar: "/opt/pc/parallel-consumer-proxy") do |client|
#     client.poll { |record| charge(record.key, record.value) }
#     client.wait
#   end
#
# == The shape, which is the same in every language
#
#   application process
#   ├── the user's block - the proxy never learns what it is
#   ├── this library
#   │   ├── admin      - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue
#   │   └── executors  - threads, each: take a record -> run the block -> report the outcome
#   └── sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
#
# == Why the executors are THREADS, and not processes
#
# MRI has a global VM lock, so this is a real decision and not a default. Python's client reached
# the opposite answer for its language; four things make Ruby's different.
#
# 1. THE WORKLOAD THIS PRODUCT EXISTS FOR IS IO-BOUND. Parallel Consumer's reason to exist is
#    keeping ordering while a slow external call runs - an HTTP request, a database write. Ruby
#    releases the VM lock around blocking IO, so threads deliver real concurrency for exactly that
#    case. Where they do not is a CPU-bound block, and that limitation is named below rather than
#    engineered around.
#
# 2. A RUBY BLOCK CANNOT CROSS A PROCESS BOUNDARY. +Proc+ is not marshalable, and Ruby has no
#    equivalent of Python's +spawn+ fallback, so a worker-process design here can only be
#    fork-and-inherit. That is not portable: +fork+ is absent on Windows and unsupported on JRuby
#    and TruffleRuby, so choosing processes would narrow this library to MRI on Unix. Threads run
#    everywhere - and on JRuby and TruffleRuby, which have no VM lock at all, they are already
#    fully parallel including for CPU-bound blocks.
#
# 3. THE grpc GEM DECLARES A LIVE STREAM FORK-UNSAFE ITSELF. Its bidirectional write loop is
#    bracketed by +GRPC::Core.fork_unsafe_begin+ / +fork_unsafe_end+. Python's client had to solve
#    this by forking a launcher process before any channel existed and having IT fork the workers
#    once the executor count arrived; the count only arrives on an open channel, so the ordering
#    took real machinery. Threads make the constraint vanish rather than manageable.
#
# 4. THE THINNER CLIENT WINS EVERY CLOSE CALL. With ten languages, anything thickening a client is
#    written and debugged ten times. Threads cost no launcher process, no inter-process queues, no
#    serialization boundary for records and outcomes, and no fork-safety hazard.
#
# THE LIMITATION, STATED PLAINLY: on MRI a CPU-bound block gets concurrency but not parallelism -
# +executor_count+ threads will not use +executor_count+ cores. The answer is Ruby's usual one, and
# the one Puma and Sidekiq give: threads within a process, processes without. Run several
# application processes, each with its own sidecar, in the same consumer group; Kafka distributes
# the partitions across them and each one's threads handle its own IO concurrency. Nothing about
# the protocol changes - the proxy has no idea how many of you there are.
module ParallelConsumer
  # Opens a session. See {Client.open}, which this delegates to and which documents the arguments.
  def self.open(options, sidecar:, **kwargs, &block)
    Client.open(options, sidecar: sidecar, **kwargs, &block)
  end
end
