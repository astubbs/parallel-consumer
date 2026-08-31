# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

# Locates the JVM-side sidecar so a Ruby spec can spawn it as an ordinary sidecar binary.
#
# The sidecar is parallel-consumer-proxy's production Main, which is a classpath invocation rather
# than a binary - so "the sidecar binary" for a spec is the JVM launcher and the classpath is an
# argument. Everything awkward about that lives here rather than in each spec.
#
# WHAT THIS BUILD'S SIDECAR DOES. It hosts no Parallel Consumer engine: it binds, announces its
# port, admits one connection under the transport's rules, and answers every session UNIMPLEMENTED
# (astubbs/parallel-consumer#384). So a spec that spawns it exercises the whole client-side path up
# to and including the handshake and stops exactly where the engine would begin. The dispatch
# scenarios below are the shared conformance suite's identities, deferred until an engine exists to
# run them against; nothing here stands in for one.
module Harness
  MAIN_CLASS = "bz.stub.parallelconsumer.proxy.Main"

  # What the sidecar's refusal must name, so a client author does not debug their own code.
  NO_ENGINE_DESCRIPTION = "hosts no Parallel Consumer engine"

  # The conformance scenario names, which are the suite's identity everywhere: the harness CLI,
  # this list, and the spec names that run them. A SCENARIO NAME IS ALSO THE TOPIC NAME - the
  # harness seeds its records on the topic it is named after.
  PROCESSED_RECORD_ADVANCES_OFFSET = "a-processed-record-advances-the-committed-offset"
  UNREPORTED_RECORD_HOLDS_COMMIT = "an-unreported-record-holds-back-the-commit"
  FAILED_RECORD_IS_REDELIVERED = "a-failed-record-is-redelivered-with-its-failure-history"
  KEY_ORDERING = "records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently"

  # Written by the ruby-sidecar-harness profile in this module's pom.
  CLASSPATH_FILE = "sidecar-classpath.txt"

  BUILD_COMMAND = "run `./mvnw test -pl :parallel-consumer-proxy-client-ruby -am " \
                  "-Dpc.foreignClients` from the repository root, which is the same wiring the CI " \
                  "matrix row uses"

  Sidecar = Struct.new(:path, :args)

  class MissingError < StandardError; end

  module_function

  # The command that runs the real sidecar shell.
  #
  # NO ARGUMENTS, and that is the sidecar's own rule rather than this method being terse: it takes
  # none and refuses to start when given one, because everything is configured connect-time over
  # the protocol.
  #
  # It RAISES rather than skips when the sidecar is not built. A spec that quietly does not run is
  # not a passing spec, and nothing goes red to say so; the error names the build command instead.
  def engine_less_sidecar
    Sidecar.new(java_binary, ["-cp", classpath, MAIN_CLASS])
  end

  def repo_root
    @repo_root ||= begin
      root = File.expand_path("../../../..", __dir__)
      raise MissingError, "no git working tree above #{__dir__}" unless File.exist?(File.join(root, ".git"))

      root
    end
  end

  def module_dir
    @module_dir ||= File.expand_path("../..", __dir__)
  end

  # Resolves the JVM launcher. A PATH lookup is acceptable HERE and nowhere else: this is test
  # scaffolding choosing a JVM, not a client library choosing which sidecar receives the user's
  # Kafka credentials.
  def java_binary
    found = ENV.fetch("PC_PROXY_TEST_JAVA", nil) || java_home_binary || java_on_path
    raise MissingError, "no JVM found - set JAVA_HOME or PC_PROXY_TEST_JAVA" unless found

    found
  end

  def java_home_binary
    home = ENV.fetch("JAVA_HOME", nil)
    return nil unless home

    candidate = File.join(home, "bin", "java")
    File.executable?(candidate) ? candidate : nil
  end

  def java_on_path
    ENV.fetch("PATH", "").split(File::PATH_SEPARATOR)
       .map { |dir| File.join(dir, "java") }
       .find { |path| File.executable?(path) }
  end

  # The sidecar's classpath, as Maven resolved it.
  #
  # ONE ROUTE, AND IT RAISES RATHER THAN GUESSING. The ruby-sidecar-harness profile in this module's
  # pom writes target/sidecar-classpath.txt on generate-test-resources, which is the only thing that
  # reliably knows where the proxy module's output and its dependencies are - in a reactor run they
  # are class DIRECTORIES rather than jars, so hunting for a jar finds nothing after a `test`-phase
  # build and reports it as an unbuilt module. Same arrangement as the Go, Python, TypeScript and
  # Rust harnesses.
  def classpath
    @classpath ||= begin
      file = File.join(module_dir, "target", CLASSPATH_FILE)
      raise MissingError, "#{file} is missing - #{BUILD_COMMAND}" unless File.file?(file)

      found = File.read(file).strip
      raise MissingError, "#{file} is empty - #{BUILD_COMMAND}" if found.empty?

      found
    end
  end
end
