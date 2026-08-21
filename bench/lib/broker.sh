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
# Contract: sets nothing, reads $BROKER_NAME, defines start_broker. Sourced, never executed.

BROKER_NAME=${BROKER_NAME:-pc-bench-broker}

# Reuses a running broker rather than recreating it. Nothing is torn down at the end, because a
# sweep is normally run more than once and re-producing the dataset is the cost this avoids.
start_broker() {
  if docker ps --filter "name=$BROKER_NAME" --format '{{.Names}}' | grep -q "$BROKER_NAME"; then
    echo "[bench] reusing running broker $BROKER_NAME" >&2
    return
  fi
  docker rm -f "$BROKER_NAME" >/dev/null 2>&1
  echo "[bench] starting broker $BROKER_NAME" >&2
  docker run -d --name "$BROKER_NAME" -p 19092:9092 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_NUM_PARTITIONS=1 \
    apache/kafka:3.9.0 >/dev/null
  sleep 20
}
