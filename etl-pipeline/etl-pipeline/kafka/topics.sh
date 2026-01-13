#!/bin/bash

# Create Kafka topics for ETL pipeline

echo "Creating Kafka topics..."

# India Finance Topic
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic india_finance_topic \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# UPI Transaction Topic
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic upi_transaction_topic \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# List all topics
echo "Listing all topics:"
kafka-topics --list --bootstrap-server localhost:9092

echo "Kafka topics created successfully!"
