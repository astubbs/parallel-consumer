# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

module ParallelConsumer
  module Demo
    # Where the sidecar binary is, for a language that cannot build one.
    #
    # THE SIDECAR IS A JVM PROGRAM AND RUBY IS NOT A JVM LANGUAGE, which is the whole difficulty
    # this class holds. The Java demo asks its own runtime - `System.getProperty("java.class.path")`
    # is already the answer there. Ruby has nothing to ask, so the launcher and the classpath are
    # handed in by whichever entry point ran: `demo/run.sh` computes them with Maven natively, and
    # the demo image bakes them in at build time.
    #
    # THAT IS WHY THESE TWO VARIABLES ARE NOT PART OF THE FLAG CONTRACT. Every `--flag` in
    # `parallel-consumer-proxy/demo/README.md` has a `PC_DEMO_` variable and this pair has no flag:
    # they are plumbing between the entry point and the demo, not a dial a reader turns. A flag
    # would invite pointing the demo at an arbitrary binary, and which binary receives your Kafka
    # credentials is a security decision the client library deliberately refuses to make (its
    # `SidecarCommand` takes an absolute path and never a PATH lookup).
    #
    # THE COMMAND IS `java` ITSELF, NEVER A WRAPPER SCRIPT. Parent death is detected by EOF on the
    # inherited pipe (KTD19); a shell wrapper inherits the write end and holds it open, which
    # defeats the signal and leaks a JVM that still holds Kafka group membership.
    class Sidecar
      MAIN_CLASS = "bz.stub.parallelconsumer.proxy.Main"

      CLASSPATH_VARIABLE = "PC_DEMO_SIDECAR_CLASSPATH"

      JAVA_VARIABLE = "PC_DEMO_SIDECAR_JAVA"

      # `.freeze` because the message interpolates a constant, so `frozen_string_literal` does not
      # cover it - RuboCop's Style/MutableConstant is what noticed.
      MISSING = <<~MESSAGE.freeze
        #{CLASSPATH_VARIABLE} is not set, so this demo does not know where the sidecar is.

        Start the demo through its own entry point, which builds the sidecar and sets it:

            parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby/demo/run.sh
      MESSAGE

      attr_reader :path, :args

      def initialize(path, args)
        @path = path
        @args = args
      end

      # The command that runs the sidecar, or a raise saying how to get one.
      def self.resolve(env = ENV)
        classpath = env[CLASSPATH_VARIABLE].to_s
        raise MISSING if classpath.strip.empty?

        new(java_binary(env), ["-cp", classpath, MAIN_CLASS])
      end

      # The JVM launcher, as an ABSOLUTE path - the client library refuses anything else, and a
      # relative or PATH-resolved launcher is exactly the ambiguity it refuses it for.
      #
      # A PATH SEARCH IS ACCEPTABLE HERE AND NOWHERE ELSE: this is a demo choosing a JVM to run a
      # sidecar it just built, not a library choosing which binary receives a user's credentials.
      def self.java_binary(env)
        candidates = [env[JAVA_VARIABLE], env["JAVA_HOME"] && File.join(env["JAVA_HOME"], "bin", "java")]
        candidates += env.fetch("PATH", "").split(File::PATH_SEPARATOR).map { |dir| File.join(dir, "java") }
        found = candidates.compact.find { |candidate| File.executable?(candidate) }
        raise "no JVM found - set #{JAVA_VARIABLE} or JAVA_HOME" unless found

        File.expand_path(found)
      end
      private_class_method :java_binary
    end
  end
end
