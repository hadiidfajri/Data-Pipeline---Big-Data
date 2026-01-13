#!/bin/bash
# Kafka Topics Creation Script

set -e

echo "Creating Kafka topics..."

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

# Create india_finance_topic
kafka-topics --create \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic india_finance_topic \
    --if-not-exists \
    --config retention.ms=604800000 \
    --config segment.ms=86400000

echo "✓ Created topic: india_finance_topic"

# Create upi_transaction_topic
kafka-topics --create \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic upi_transaction_topic \
    --if-not-exists \
    --config retention.ms=604800000 \
    --config segment.ms=86400000

echo "✓ Created topic: upi_transaction_topic"

# List all topics
echo ""
echo "Available topics:"
kafka-topics --list --bootstrap-server kafka:9092

echo ""
echo "✓ Kafka topics setup completed!"