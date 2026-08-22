#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The one broker every bench harness shares.
#
# Extracted when a second harness (run-divergence.sh) needed the same container. One partition
# (KAFKA_NUM_PARTITIONS=1) is deliberate and load-bearing - concurrency past partition count is the
# case both this project and its comparators exist to serve - and two harnesses quietly agreeing on
# a DIFFERENT partition count would produce numbers that look comparable and are not.
#
# Both run-bisect.sh and run-divergence.sh source this. Neither has its own copy - a change to the
# broker must land in one place or the two harnesses drift onto differently-configured brokers.
#
# Contract: sets nothing, reads $BROKER_NAME/$BROKER_PORT/$BROKER_IMAGE, defines start_broker.
# Sourced, never executed.
#
# WHY THE BROKER IS PARAMETERISED, as of 2026-08-22.
#
# KIP-932 share groups went GA in Kafka 4.2.0, and the bench's broker was 3.9.0. Adding the share
# arm therefore needs a Kafka 4 broker - but the 3.9.0 container is SHARED between concurrent
# sessions on this machine, and replacing it would have moved the ground under every other sweep
# running at the time. So a second broker is started alongside it under its own name, port and
# image, and the caller chooses.
#
# THE DEFAULTS ARE THE OLD HARDCODED VALUES, exactly, so every existing invocation and every
# committed results file still means what it meant. A caller who never heard of these variables gets
# the broker they always got.
#
# BROKER_PORT IS BOTH THE PUBLISHED PORT AND THE LISTENER PORT, which looks redundant and is not:
# the container advertises localhost:$BROKER_PORT, so anything running INSIDE the container - the
# kafka-share-groups.sh and kafka-configs.sh tools this arm needs for group configuration and for
# reading share state - bootstraps to that address and must find a listener there. The first 4.3.1
# broker published 19093:9092 and advertised localhost:19093, and every in-container CLI call timed
# out with "Timed out waiting for a node assignment" while the same call from the host worked fine.

# NOTHING IS DEFAULTED AT SOURCE TIME, and that is the fix for a defect this file had for exactly one
# sweep. Defaulting BROKER_NAME here ran BEFORE use_share_broker could be called, so `${BROKER_NAME:-
# pc-bench-broker-4}` inside it found the name already set and kept the 3.9.0 one. The sweep then
# said "reusing running broker pc-bench-broker" and ran the share arm against a broker with no share
# coordinator - which fails, but fails as RUN_FAILED, indistinguishable from a broken arm.
#
# So every default is applied at the point of USE (`:=`), after any selection has had its say.
broker_defaults() {
  : "${BROKER_NAME:=pc-bench-broker}"
  : "${BROKER_PORT:=19092}"
  : "${BROKER_IMAGE:=apache/kafka:3.9.0}"
  # Extra `docker run` arguments, for broker features a particular sweep needs. Share groups need
  # three settings that a 3.9.0 broker has never heard of, so they cannot be unconditional here.
  : "${BROKER_EXTRA_ARGS:=}"
}

# Reuses a running broker rather than recreating it. Nothing is torn down at the end, because a
# sweep is normally run more than once and re-producing the dataset is the cost this avoids.
start_broker() {
  broker_defaults
  if docker ps --filter "name=$BROKER_NAME" --format '{{.Names}}' | grep -q "$BROKER_NAME"; then
    echo "[bench] reusing running broker $BROKER_NAME" >&2
    return
  fi
  docker rm -f "$BROKER_NAME" >/dev/null 2>&1
  echo "[bench] starting broker $BROKER_NAME ($BROKER_IMAGE) on $BROKER_PORT" >&2
  # shellcheck disable=SC2086 # BROKER_EXTRA_ARGS is deliberately word-split into separate arguments
  docker run -d --name "$BROKER_NAME" -p "$BROKER_PORT:$BROKER_PORT" \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS="PLAINTEXT://:$BROKER_PORT,CONTROLLER://:9093" \
    -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:$BROKER_PORT" \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_NUM_PARTITIONS=1 \
    $BROKER_EXTRA_ARGS \
    "$BROKER_IMAGE" >/dev/null
  # Poll rather than sleep a fixed 20s: the 4.x images take longer to come up than 3.9.0 did, and a
  # fixed sleep that is too short fails as a produce timeout, which reads as a wedged broker.
  local waited=0
  until docker logs "$BROKER_NAME" 2>&1 | grep -q "Kafka Server started"; do
    sleep 2; waited=$((waited + 2))
    if [ "$waited" -ge 90 ]; then
      echo "[bench] FATAL: $BROKER_NAME did not start within ${waited}s" >&2
      docker logs "$BROKER_NAME" 2>&1 | tail -20 >&2
      return 1
    fi
  done
  echo "[bench] broker $BROKER_NAME up after ${waited}s" >&2
}

# The share-group broker: Kafka 4.2+ with KIP-932 turned on. One call so run-bisect.sh and any later
# harness cannot configure it differently - the same rule the partition count already follows.
#
# THREE SETTINGS, and all three are required on a single-node broker:
#   * group.coordinator.rebalance.protocols must LIST share - the share rebalance protocol is not on
#     by default even though the share.version feature flag is finalized at level 1.
#   * share.coordinator.state.topic.replication.factor and .min.isr default to 3 and 2. The share
#     coordinator keeps per-record delivery state in the internal __share_group_state topic, and on a
#     one-broker cluster that topic cannot be created until both are 1 - so share groups fail at the
#     first acknowledgement rather than at startup, which is a confusing place to find out.
use_share_broker() {
  : "${BROKER_NAME:=pc-bench-broker-4}"
  : "${BROKER_PORT:=19093}"
  : "${BROKER_IMAGE:=apache/kafka:4.3.1}"
  BROKER_EXTRA_ARGS="-e KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer,share \
    -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1 ${BROKER_EXTRA_ARGS:-}"
}
