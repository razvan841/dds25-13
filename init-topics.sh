#!/usr/bin/env bash
#set -euo pipefail

# NUM_SHARDS controls how many shard-specific topic sets to create.
# TOPICS is a comma-separated list of base topic names (without shard suffix).
# Each base topic will be created as <topic>.<shard> for shard in 0..NUM_SHARDS-1.
NUM_SHARDS="${NUM_SHARDS:-3}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"

echo "Initializing Kafka topics for ${NUM_SHARDS} shards..."
IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

for topic in "${TOPIC_ARRAY[@]}"; do
  for shard in $(seq 0 $((NUM_SHARDS - 1))); do
    full_topic="${topic}.${shard}"
    kafka-topics --create \
      --topic "$full_topic" \
      --partitions "$PARTITIONS" \
      --replication-factor "$KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" \
      --if-not-exists \
      --bootstrap-server "$BOOTSTRAP"
    echo "Created topic $full_topic"
  done
done

echo "All Kafka topics created successfully (${NUM_SHARDS} shards)."
