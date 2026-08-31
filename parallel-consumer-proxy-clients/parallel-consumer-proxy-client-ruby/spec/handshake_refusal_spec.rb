# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "spec_helper"
require "socket"
require "tmpdir"

# The handshake, against a real sidecar process, over the real wire.
#
# This module's one against-a-real-process spec, and the only claim it can honestly make on this
# stack. The sidecar spawned is parallel-consumer-proxy's production entry point - a real bind, the
# real authority allowlist, the real single-connection guard, and the real session service. That
# service hosts no engine and refuses every session, so there is no dispatch to observe here and
# none is invented.
#
# What IS observed is everything this library does before an engine would matter: launch the child
# directly, read "port:" off its stdout, hold its stdin as the parent-death lifeline, open the
# channel, put Configure on the wire, and turn what came back into a Ruby exception. The dispatch
# scenarios - one record end to end, the in-flight ceiling, the redelivery history - belong to the
# shared conformance suite and are deferred until an engine exists to run them against.
#
# THE STATUS NAME IS THE ASSERTION, NOT MERELY "IT FAILED". A refusal from the authority allowlist
# is PermissionDenied and one from the admission slot is ResourceExhausted, both raised by
# interceptors BEFORE the service method runs. Only Unimplemented can have come from the service
# itself, so it is what separates "the connection was turned away" from "the handshake was
# delivered and answered".
RSpec.describe "the sidecar handshake" do
  let(:options) do
    ParallelConsumer::ClientOptions.new(
      topics: ["handshake-topic"],
      # The sidecar reads no properties at all on this build. Real credentials never belong in a
      # spec, and there is nothing here to give them to.
      kafka_properties: {},
      instance_tag: "ruby-handshake"
    )
  end

  it "reaches the session service, and its refusal reaches the caller" do
    sidecar = Harness.engine_less_sidecar

    expect do
      ParallelConsumer::Client.open(options, sidecar: sidecar.path, sidecar_args: sidecar.args)
    end.to raise_error(ParallelConsumer::Error) { |raised|
      expect(raised.message).to include("Unimplemented"),
                                "Unimplemented is the only status the session SERVICE raises, so it is what " \
                                "proves the Configure was delivered rather than turned away by an " \
                                "interceptor: #{raised.message}"
      expect(raised.message).to include(Harness::NO_ENGINE_DESCRIPTION),
                                "the refusal must name what is missing, or a client author debugs their own " \
                                "code: #{raised.message}"
    }
  end

  # The control arm, permanent rather than a one-off demonstration: pointed at a port nothing is
  # listening on, the same client fails in a way that is not the refusal above. Without it, the
  # example that matters could be passing on any failure at all - which is the shape of an
  # assertion that cannot fail for the reason it names.
  #
  # The stand-in announces a port and then holds its stdin, which is the spawning contract's whole
  # client-visible surface, so the library takes its REAL connect path at a dead port rather than
  # the different path a child that printed nothing would take.
  it "fails differently when nothing is listening on the announced port" do
    Dir.mktmpdir("pc-ruby-announcer") do |dir|
      announcer = write_announcer(dir, reserve_then_release_a_port)

      expect do
        ParallelConsumer::Client.open(options, sidecar: announcer)
      end.to raise_error(StandardError) { |raised|
        nothing_refused = "nothing answered, so nothing can have refused: #{raised.message}"
        expect(raised.message).not_to include(Harness::NO_ENGINE_DESCRIPTION), nothing_refused
      }
    end
  end

  # A loopback port the OS has just handed out and nothing is listening on.
  def reserve_then_release_a_port
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    server.close
    port
  end

  # A sidecar that announces a port and then holds its stdin. printf and read are shell builtins,
  # so it is one process holding its own lifeline and no grandchild survives the library's reap.
  def write_announcer(dir, port)
    path = File.join(dir, "announcer.sh")
    File.write(path, <<~SHELL)
      #!/bin/sh
      printf 'port: #{port}\\n'
      while read -r _ignored; do :; done
      exit 0
    SHELL
    File.chmod(0o700, path)
    path
  end
end
