# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

# Where the sidecar's stderr goes (authoring guide §10.1: a stream you will not read must be
# REDIRECTED, never closed).
#
# WHY THE CONSEQUENCE ITSELF IS NOT ASSERTED HERE: `err: :close` starts the child with file
# descriptor 2 closed, so the next file it opens can be handed that number and its later
# diagnostics land in that file. A JVM does exactly this; MRI reopens its own std descriptors at
# startup, so a Ruby fake sidecar cannot exhibit it and a test built on one would prove nothing.
# What is asserted instead is the boundary this client controls: the redirect it asks the kernel
# for, and its refusal of the one value that closes the descriptor.
RSpec.describe ParallelConsumer::Sidecar do
  let(:announced_port) { 7101 }

  around do |example|
    Dir.mktmpdir("pc-sidecar-stderr") { |dir| example.run(@dir = dir) }
  end

  # Says something on stderr, announces its port, then waits for the parent-death signal.
  def fake_sidecar
    path = File.join(@dir, "fake-sidecar")
    File.write(path, <<~RUBY)
      #!/usr/bin/env ruby
      $stderr.puts("the sidecar's own diagnostic")
      $stderr.flush
      $stdout.puts("port: #{announced_port}")
      $stdout.flush
      $stdin.read
    RUBY
    File.chmod(0o755, path)
    ParallelConsumer::SidecarCommand.new(path)
  end

  it "refuses :close rather than documenting that it is wrong" do
    expect { described_class.new(fake_sidecar, timeout: 10, stderr: :close) }
      .to raise_error(ArgumentError, /CLOSED file descriptor 2/)
  end

  it "defaults to this process's own stderr, so a dying sidecar can still explain itself" do
    expect(Process).to receive(:spawn).with(anything, hash_including(err: :err)).and_call_original

    sidecar = described_class.new(fake_sidecar, timeout: 10)

    expect(sidecar.port).to eq(announced_port)
  ensure
    sidecar&.stop(5)
  end

  it "sends the child's stderr wherever it is pointed, and the child keeps running" do
    log = File.join(@dir, "sidecar.err")

    sidecar = described_class.new(fake_sidecar, timeout: 10, stderr: log)

    expect(sidecar.port).to eq(announced_port)
    expect(File.read(log)).to include("the sidecar's own diagnostic")
  ensure
    sidecar&.stop(5)
  end
end
