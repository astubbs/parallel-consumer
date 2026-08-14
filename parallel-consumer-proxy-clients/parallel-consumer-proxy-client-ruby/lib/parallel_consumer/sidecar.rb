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

    attr_reader :pid, :port

    # Spawns the sidecar and waits for it to announce its port.
    #
    # @param command [SidecarCommand]
    # @param timeout [Numeric] seconds to wait for the port line
    # @param stderr [IO, Symbol] where the child's stderr goes; +:close+ discards it
    def initialize(command, timeout:, stderr: :close)
      @stdin_read, @stdin = IO.pipe
      @stdout, @stdout_write = IO.pipe

      # The [cmd, argv0] form is Ruby's explicit "never a shell", true even for a command with no
      # arguments - which the bare string form does not guarantee.
      @pid = Process.spawn([command.path, command.path], *command.args,
                           in: @stdin_read, out: @stdout_write, err: stderr)
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
