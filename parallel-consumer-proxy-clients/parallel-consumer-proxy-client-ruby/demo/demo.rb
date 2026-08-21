#!/usr/bin/env ruby
# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

# The Ruby demo's main program. `demo/run.sh` is the entry point a reader uses; this is what it
# runs, and what the demo container's entrypoint runs, so both paths execute the same code.
#
# IT IS ALSO REACHED DIRECTLY - `docker compose run demo --help` lands here rather than in run.sh -
# which is why the usage text and the flag parsing live on this side of the boundary and not in the
# script. A demo that answered `--help` with "unknown option" would be a poor first impression of a
# contract nine other languages copy.

require "bundler/setup"

$LOAD_PATH.unshift(File.expand_path("lib", __dir__))
# The client library this demo exists to exercise, from the module it lives in - not from a
# published gem. Nothing in this wave is published anywhere, and a demo that installed one would be
# demonstrating a version nobody can build from this checkout.
$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "parallel_consumer/demo/options"
require "parallel_consumer/demo/comparison"

module ParallelConsumer
  module Demo
    EXIT_OK = 0
    EXIT_USAGE = 2

    # WHY THE DEMO DOES NOT START A BROKER ITSELF. The Java seed starts one with Testcontainers when
    # no address is supplied. Ruby's entry point starts the same compose broker on the host and
    # hands the address in - so the promise the contract makes ("omit --bootstrap to start one") is
    # kept by `demo/run.sh`, one process out. Inside the demo container it could not be kept any
    # other way regardless: a demo container is never granted the host Docker socket (plan unit
    # U35), so it reaches a compose sibling and starts nothing.
    NO_BROKER = <<~MESSAGE
      No broker address reached this demo.

      Start it through its own entry point, which starts a broker for you:

          parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby/demo/run.sh

      Or point it at one you already have: --bootstrap host:port (PC_DEMO_BOOTSTRAP).
    MESSAGE

    def self.main(argv, env)
      if Options.help_requested?(argv)
        puts Options.usage
        return EXIT_OK
      end

      begin
        options = Options.new(argv, env)
      rescue Options::UsageError => e
        warn(e.message)
        warn(Options.usage)
        return EXIT_USAGE
      end

      run(options)
      EXIT_OK
    end

    def self.run(options)
      raise NO_BROKER if options.bootstrap.nil? || options.bootstrap.strip.empty?

      topic = options.topic || "pc-demo-#{Process.clock_gettime(Process::CLOCK_REALTIME, :nanosecond)}"
      Comparison.new(options, Broker.new(options.bootstrap.strip), topic).run
      nil
    end
  end
end

exit(ParallelConsumer::Demo.main(ARGV, ENV)) if $PROGRAM_NAME == __FILE__
