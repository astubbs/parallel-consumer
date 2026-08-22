# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "fileutils"
require "timeout"

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "parallel_consumer"
require_relative "support/harness"

RSpec.configure do |config|
  config.expect_with(:rspec) { |expectations| expectations.syntax = :expect }
  config.disable_monkey_patching!
  config.order = :defined

  # These specs spawn a JVM and drive a real gRPC stream. A hung one must fail rather than hang a
  # CI row to its 30-minute timeout.
  config.around do |example|
    Timeout.timeout(180) { example.run }
  end
end
