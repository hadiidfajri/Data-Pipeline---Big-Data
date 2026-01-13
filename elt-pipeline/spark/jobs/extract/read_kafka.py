#!/usr/bin/env python3
"""
Spark Job: Extract data from Kafka Topics
Reads streaming data from Kafka and prepares for loading
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
CHECKPOINT_DIR = '/opt/data/checkpoints'

# Schema for India Personal Finance
FINANCE_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("income", DoubleType(), True),
    StructField("age", IntegerType(), True),
    StructField("dependents", IntegerType(), True),
    StructField("occupation", StringType(), True),
    StructField("city_tier", StringType(), True),
    StructField("rent", DoubleType(), True),
    StructField("loan_repayment", DoubleType(), True),
    StructField("insurance", DoubleType(), True),
    StructField("groceries", DoubleType(), True),
    StructField("transport", DoubleType(), True),
    StructField("eating_out", DoubleType(), True),
    StructField("entertainment", DoubleType(), True),
    StructField("utilities", DoubleType(), True),
    StructField("healthcare", DoubleType(), True),
    StructField("education", DoubleType(), True),
    StructField("miscellaneous", DoubleType(), True),
    StructField("desired_savings_percentage", DoubleType(), True),
    StructField("desired_savings", DoubleType(), True),
    StructField("disposable_income", DoubleType(), True),
    StructField("potential_savings_groceries", DoubleType(), True),
    StructField("potential_savings_transport", DoubleType(), True),
    StructField("potential_savings_eating_out", DoubleType(), True),
    StructField("potential_savings_entertainment", DoubleType(), True),
    StructField("potential_savings_utilities", DoubleType(), True),
    StructField("potential_savings_healthcare", DoubleType(), True),
    StructField("potential_savings_education", DoubleType(), True),
    StructField("potential_savings_miscellaneous", DoubleType(), True),
    StructField("_extracted_at", StringType(), True),
    StructField("_source", StringType(), True)
])

# Schema for UPI Transaction
UPI_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount_inr", DoubleType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("sender_age_group", StringType(), True),
    StructField("receiver_age_group", StringType(), True),
    StructField("sender_state", StringType(), True),
    StructField("sender_bank", StringType(), True),
    StructField("receiver_bank", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("network_type", StringType(), True),
    StructField("fraud_flag", IntegerType(), True),
    StructField("hour_of_day", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("is_weekend", IntegerType(), True),
    StructField("_extracted_at", StringType(), True),
    StructField("_source", StringType(), True)
])

class KafkaExtractor:
    def __init__(self, spark):
        self.spark = spark
    
    def read_kafka_stream(self, topic, schema):
        """Read streaming data from Kafka topic"""
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .load()
            
            # Parse JSON value
            parsed_df = df.select(
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp")
            
            # Add processing timestamp
            final_df = parsed_df.withColumn("_processed_at", current_timestamp())
            
            logger.info(f"Successfully configured Kafka stream for topic: {topic}")
            return final_df
            
        except Exception as e:
            logger.error(f"Error reading from Kafka topic {topic}: {e}")
            raise
    
    def extract_finance_data(self):
        """Extract India Personal Finance data"""
        topic = os.getenv('KAFKA_TOPIC_POSTGRES', 'india_finance_topic')
        return self.read_kafka_stream(topic, FINANCE_SCHEMA)
    
    def extract_upi_data(self):
        """Extract UPI Transaction data"""
        topic = os.getenv('KAFKA_TOPIC_UPI', 'upi_transaction_topic')
        return self.read_kafka_stream(topic, UPI_SCHEMA)

def main():
    """Main execution"""
    try:
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Kafka_Data_Extractor") \
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
        
        # Initialize extractor
        extractor = KafkaExtractor(spark)
        
        # Extract data
        logger.info("Starting data extraction from Kafka...")
        finance_df = extractor.extract_finance_data()
        upi_df = extractor.extract_upi_data()
        
        logger.info("Kafka extraction configured successfully")
        
        return finance_df, upi_df
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    finance_stream, upi_stream = main()
    logger.info("Extraction process completed")
