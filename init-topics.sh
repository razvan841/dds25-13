#!/usr/bin/env bash
#set -euo pipefail

echo "Initializing Kafka Services..."
IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

for topic in "${TOPIC_ARRAY[@]}"; do
  kafka-topics --create \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor "$KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" \
    --if-not-exists \
    --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092
  echo "Created topic $topic"
done

echo "All Kafka topics created successfully."