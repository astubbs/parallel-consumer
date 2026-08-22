# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

# Locates the JVM-side conformance harness so a Ruby spec can spawn it as an ordinary sidecar
# binary.
#
# The harness is TestModeMain, shipped in the proxy module's TEST jar so it can never reach a
# client package. That makes it a classpath invocation rather than a binary, so "the sidecar
# binary" for a conformance test is the JVM launcher and the classpath is an argument. Everything
# awkward about that lives here rather than in each spec.
module Harness
  MAIN_CLASS = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain"

  # The conformance scenario names, which are the suite's identity everywhere: the harness CLI,
  # this list, and the spec names that run them. A SCENARIO NAME IS ALSO THE TOPIC NAME - the
  # harness seeds its records on the topic it is named after.
  PROCESSED_RECORD_ADVANCES_OFFSET = "a-processed-record-advances-the-committed-offset"
  UNREPORTED_RECORD_HOLDS_COMMIT = "an-unreported-record-holds-back-the-commit"
  FAILED_RECORD_IS_REDELIVERED = "a-failed-record-is-redelivered-with-its-failure-history"
  KEY_ORDERING = "records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently"

  BUILD_COMMAND = "bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests"

  Sidecar = Struct.new(:path, :args)

  class MissingError < StandardError; end

  module_function

  # The command that serves one conformance scenario in mock mode.
  #
  # It RAISES rather than skips when the harness is not built. A spec that quietly does not run is
  # not a passing spec, and nothing goes red to say so; the error names the build command instead.
  def for_scenario(scenario)
    Sidecar.new(java_binary, ["-cp", classpath, MAIN_CLASS, "--mock", "--scenario", scenario])
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

  # The proxy module's test classpath: its test jar (which carries the harness), its main jar, and
  # its test-scope dependencies.
  #
  # The dependency list comes from Maven and is cached beside this module's build output, because
  # resolving it costs seconds and the answer changes only when the proxy module's poms do. It is
  # never committed: it is a list of absolute paths into a local repository, so it is
  # machine-specific.
  def classpath
    @classpath ||= [single_jar("-tests.jar"), single_jar(".jar"), dependency_classpath]
                   .join(File::PATH_SEPARATOR)
  end

  def proxy_target
    File.join(repo_root, "parallel-consumer-proxy", "target")
  end

  def single_jar(suffix)
    unless File.directory?(proxy_target)
      raise MissingError, "#{proxy_target} is not built - run '#{BUILD_COMMAND}' first"
    end

    matches = Dir.children(proxy_target).select { |name| name.end_with?(suffix) }
    matches.reject! { |name| name.end_with?("-tests.jar", "-sources.jar", "-javadoc.jar") } if suffix == ".jar"
    unless matches.size == 1
      raise MissingError,
            "expected exactly one #{suffix.inspect} jar in #{proxy_target}, found #{matches.size} - " \
            "run '#{BUILD_COMMAND}'"
    end

    File.join(proxy_target, matches.first)
  end

  def dependency_classpath
    cache = File.join(module_dir, "target", "proxy-test-classpath.txt")
    return File.read(cache).strip if File.exist?(cache)

    FileUtils.mkdir_p(File.dirname(cache))
    ok = system(File.join(repo_root, "mvnw"), "-q", "-pl", ":parallel-consumer-proxy",
                "dependency:build-classpath", "-Dmdep.outputFile=#{cache}",
                "-Dmdep.includeScope=test", chdir: repo_root)
    raise MissingError, "resolving the proxy module's test classpath failed" unless ok

    File.read(cache).strip
  end
end
