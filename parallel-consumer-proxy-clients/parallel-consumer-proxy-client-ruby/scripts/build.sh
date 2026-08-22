#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# WHAT MAVEN'S compile PHASE RUNS for the Ruby client, and why it is a script rather than two words
# in pom.xml. The phase has two steps - install the gems, then parse every source file - and
# exec-maven-plugin runs ONE executable with no shell to chain them. `bundle install` alone was the
# whole of it until now, which is why `mvn compile -Dpc.foreignClients` reported SUCCESS on a
# lib/parallel_consumer.rb ending in `this is not valid ruby @@@`.
#
# THE PARSE ITSELF LIVES IN THE Rakefile, as `rake syntax`, and that is where its reasoning and its
# file list are. This file holds no policy: it is the adapter between one exec and two commands, so
# that a developer running `bundle install && bundle exec rake syntax` runs exactly what CI runs.
#
# THE ORDER IS NOT ARBITRARY. `ruby -c` needs no gems, but `rake` does, so the install has to come
# first - on a machine that has never built this module, `bundle exec` fails before any task runs.

set -euo pipefail

cd "$(dirname "$0")/.."

bundle install
exec bundle exec rake syntax
