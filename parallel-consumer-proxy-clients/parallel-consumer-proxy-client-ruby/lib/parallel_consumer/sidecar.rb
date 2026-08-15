# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

module ParallelConsumer
  # The sidecar command: an absolute path plus its arguments.
  #
  # The application supplies the binary's location EXPLICITLY. This library never resolves it
  # through +PATH+, a relative lookup, or any directory an attacker could influence - this process
  # hands the sidecar its Kafka credentials, so which binary runs is a security decision and not a
  # convenience.
  SidecarCommand = Struct.new(:path, :args) do
    def self.coerce(sidecar, args: [])
      return sidecar if sidecar.is_a?(SidecarCommand)

      new(sidecar.to_s, args)
    end

    def initialize(path, args = [])
      super(File.expand_path(path.to_s), Array(args).map(&:to_s))
      raise SidecarError, "the sidecar path must be absolute, got #{path.inspect}" unless self.path.start_with?("/")
    end
  end

  # The sidecar child process and the lifecycle pipe that keeps it alive.
  #
  # THE PIPE IS THE PARENT-DEATH SIGNAL: this process holds its write end and never writes to it,
  # so EOF on the child's stdin is proof the parent is gone. That is also why the binary is
  # launched DIRECTLY and never through a shell - a shell wrapper would hold the write end open
  # and leak a JVM that still holds Kafka group membership.
  class Sidecar
    PORT_LINE = /^port:\s*(\d+)\s*$/

    # A STREAM YOU WILL NOT READ MUST BE REDIRECTED, NEVER CLOSED - authoring guide §10.1, and this
    # client used to break it with +err: :close+. That does not discard the child's stderr: it
    # starts the JVM with FILE DESCRIPTOR 2 CLOSED, so the next file the JVM opens can be handed
    # fd 2 by the kernel, and everything written to stderr afterwards lands in that file. It also
    # destroys the death diagnostic - a sidecar that dies during startup has nowhere to say why,
    # and "the sidecar produced no 'port: <n>' line" is then all anyone gets. Nor may it be a pipe
    # nobody drains, which fills and blocks the child. Redirect: inherit, or the null device.
    # +:err+ is Process.spawn's own name for "this process's stderr", which is also what an
    # unspecified redirect does - named here so the choice is visible rather than implicit.
    STDERR_DESTINATIONS = { inherit: :err, null: File::NULL }.freeze

    attr_reader :pid, :port

    # Spawns the sidecar and waits for it to announce its port.
    #
    # @param command [SidecarCommand]
    # @param timeout [Numeric] seconds to wait for the port line
    # @param stderr [IO, Symbol, String] where the child's stderr goes: +:inherit+ (this process's
    #   own stderr, the default, so a dying sidecar can still explain itself), +:null+, or any IO
    #   or path you will actually drain. +:close+ is refused - see {STDERR_DESTINATIONS}
    def initialize(command, timeout:, stderr: :inherit)
      @stdin_read, @stdin = IO.pipe
      @stdout, @stdout_write = IO.pipe

      # The [cmd, argv0] form is Ruby's explicit "never a shell", true even for a command with no
      # arguments - which the bare string form does not guarantee.
      @pid = Process.spawn([command.path, command.path], *command.args,
                           in: @stdin_read, out: @stdout_write, err: destination_for(stderr))
      @stdin_read.close
      @stdout_write.close

      @port = read_port(timeout)
    rescue StandardError
      stop(0)
      raise
    end

    # Closes the lifecycle pipe and reaps the child.
    #
    # CLOSING STDIN IS THE REAP: it is the parent-death signal the sidecar watches, and for the
    # conformance harness it is the ONLY exit - the harness serves sessions until stdin EOF and
    # does not exit after a clean drain. Killing is the backstop for a child that honours neither,
    # and is never the first move: killing a sidecar with the stream still open turns a clean
    # drain into a reconnect-window recovery for the next member of the consumer group.
    def stop(grace)
      close_pipe(@stdin)
      reap(grace)
      @drain&.join(grace)
      close_pipe(@stdout)
      nil
    end

    private

    # Resolves the caller's choice to a spawn redirect, REFUSING the one that closes fd 2 rather
    # than documenting that it is wrong: a guard the caller can walk past is not a guard.
    def destination_for(stderr)
      return STDERR_DESTINATIONS.fetch(stderr) if STDERR_DESTINATIONS.key?(stderr)
      raise ArgumentError, closed_stderr_message if stderr == :close

      stderr
    end

    def closed_stderr_message
      "the sidecar's stderr must be redirected, never closed: :close hands the child a CLOSED " \
        "file descriptor 2, so the next file it opens can silently become its stderr, and a " \
        "sidecar that dies during startup has nowhere to say why. Use :inherit, :null, or an IO " \
        "you drain."
    end

    def close_pipe(pipe)
      pipe.close if pipe && !pipe.closed?
    end

    def reap(grace)
      return unless @pid
      return if wait_for_exit(grace)

      Process.kill("KILL", @pid)
      wait_for_exit(grace)
    end

    # Scans the lifecycle channel for the port line, and keeps draining afterwards.
    #
    # The specification's contract is that the port is stdout's FIRST line. The conformance
    # harness diverges - it logs before it - and the guide says a test absorbs that rather than
    # asserting the position, so this SCANS for the line instead of reading exactly one. Scanning
    # satisfies both. Draining continues for the child's whole life so a sidecar that keeps
    # logging never blocks on a full pipe buffer.
    def read_port(timeout)
      found = Thread::Queue.new
      @drain = Thread.new do
        @stdout.each_line do |line|
          match = PORT_LINE.match(line)
          found.push(Integer(match[1])) if match && found.empty?
        end
      rescue IOError
        nil # the pipe was closed by #stop; that is an ordinary end, not a failure
      ensure
        found.close
      end

      port = found.pop(timeout: timeout)
      raise SidecarError, "the sidecar produced no 'port: <n>' line within #{timeout}s" if port.nil?

      port
    end

    def wait_for_exit(grace)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + grace
      loop do
        return true if Process.wait(@pid, Process::WNOHANG)
        return false if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

        sleep 0.02
      end
    rescue Errno::ECHILD
      true # already reaped
    end
  end
end
