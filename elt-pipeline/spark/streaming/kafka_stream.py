#!/usr/bin/env python3
"""
Spark Structured Streaming from Kafka
Real-time data processing and monitoring
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
CHECKPOINT_DIR = '/opt/data/checkpoints/streaming'

# Schema definitions
FINANCE_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("income", DoubleType(), True),
    StructField("age", IntegerType(), True),
    StructField("occupation", StringType(), True),
])

UPI_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("amount_inr", DoubleType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("sender_age_group", StringType(), True),
])

class KafkaStreamProcessor:
    def __init__(self):
        self.spark = None
        
    def initialize_spark(self):
        """Initialize Spark with streaming configuration"""
        try:
            self.spark = SparkSession.builder \
                .appName("Kafka_Stream_Processor") \
                .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
                .config("spark.sql.shuffle.partitions", "2") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark Streaming session initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            raise
    
    def stream_finance_data(self):
        """Stream India Personal Finance data"""
        try:
            logger.info("Starting finance data stream...")
            
            # Read from Kafka
            finance_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", "india_finance_topic") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON
            finance_data = finance_stream.select(
                from_json(col("value").cast("string"), FINANCE_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp")
            
            # Add processing timestamp
            finance_data = finance_data.withColumn("processed_at", current_timestamp())
            
            # Write stream with monitoring
            query = finance_data \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 10) \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info("Finance stream started successfully")
            return query
            
        except Exception as e:
            logger.error(f"Finance stream error: {e}")
            raise
    
    def stream_upi_data(self):
        """Stream UPI Transaction data"""
        try:
            logger.info("Starting UPI data stream...")
            
            # Read from Kafka
            upi_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", "upi_transaction_topic") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON
            upi_data = upi_stream.select(
                from_json(col("value").cast("string"), UPI_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp")
            
            # Add processing timestamp
            upi_data = upi_data.withColumn("processed_at", current_timestamp())
            
            # Aggregate by time window (example)
            windowed_data = upi_data \
                .withWatermark("kafka_timestamp", "10 minutes") \
                .groupBy(
                    window("kafka_timestamp", "5 minutes"),
                    "transaction_status",
                    "sender_age_group"
                ).count()
            
            # Write stream
            query = windowed_data \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info("UPI stream started successfully")
            return query
            
        except Exception as e:
            logger.error(f"UPI stream error: {e}")
            raise
    
    def monitor_streams(self, queries):
        """Monitor streaming queries"""
        try:
            logger.info(f"Monitoring {len(queries)} streaming queries...")
            
            for query in queries:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            logger.info("Stopping streams due to user interrupt...")
            for query in queries:
                query.stop()
                
        except Exception as e:
            logger.error(f"Stream monitoring error: {e}")
            raise

def main():
    """Main execution"""
    processor = KafkaStreamProcessor()
    
    try:
        logger.info("Starting Kafka Stream Processor...")
        
        # Initialize
        processor.initialize_spark()
        
        # Start streams
        finance_query = processor.stream_finance_data()
        upi_query = processor.stream_upi_data()
        
        # Monitor
        processor.monitor_streams([finance_query, upi_query])
        
        logger.info("Stream processing completed")
        
    except Exception as e:
        logger.error(f"Stream processor failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
