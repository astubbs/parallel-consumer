# Copyright (C) 2026 Antony Stubbs and contributors
# frozen_string_literal: true

require "spec_helper"

# What a raised exception becomes on the wire: the reason the NEXT delivery of that record carries
# back, and which the user reads as +last_failure_reason+.
#
# THE REASON IS THE USER'S TEXT, NOT THIS LIBRARY'S RENDERING OF IT. The reason used to arrive
# prefixed with the exception's class - +"RuntimeError: no stock"+ for a +raise "no stock"+ - which
# this module's README already contradicted, and which no sibling client does: Go reports
# +err.Error()+, Python +str(exception)+, TypeScript +error.message+. The cross-language conformance
# suite is what caught it (astubbs#242): its redelivery scenario asserts the previous reason comes
# back VERBATIM, and Ruby was the only language handing back something the user never wrote.
#
# +invoke+ is reached through +send+ deliberately. It is private, and it is also the ONE place a
# raise becomes an outcome - so the alternative is a whole session, a sidecar and a stream to
# observe a pure translation.
# Ruby's default message for a bare raise IS the class name, so an exception with a deliberately
# empty message is what reaches the class fallback at all. Defined out here because a class defined
# inside the describe block is a constant defined inside a block, which RuboCop's Lint department
# flags - correctly, since it would leak into the enclosing namespace either way.
class SpeechlessError < StandardError
  def message = ""
end

RSpec.describe "a raised exception's failure reason" do
  def reason_for(&block)
    outcome = ParallelConsumer::Client.allocate.send(:invoke, block, :a_record)
    expect(outcome).to be_failure
    outcome.reason
  end

  it "is the message the user raised, verbatim" do
    expect(reason_for { raise "conformance-prescribed-failure" }).to eq("conformance-prescribed-failure")
  end

  it "keeps a message that would look like a class prefix, rather than adding a second one" do
    expect(reason_for { raise "ArgumentError: from the payload" }).to eq("ArgumentError: from the payload")
  end

  it "falls back to the class when the exception carries no message of its own" do
    expect(reason_for { raise SpeechlessError }).to eq(SpeechlessError.to_s)
  end
end
