from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UPIExtractor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.kafka_config = config['kafka']
        
    def extract_from_kafka(self):
        """Extract UPI data from Kafka topic"""
        try:
            logger.info("Starting extraction from Kafka topic: upi_transaction_topic")
            
            # Read from Kafka
            df = self.spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers']) \
                .option("subscribe", self.kafka_config['topic_upi']) \
                .option("startingOffsets", "earliest") \
                .load()
            
            # Convert value from binary to string
            df = df.selectExpr("CAST(value AS STRING) as json_data")
            
            # Define schema for UPI transactions
            schema = StructType([
                StructField("transaction_id", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("transaction_type", StringType(), True),
                StructField("merchant_category", StringType(), True),
                StructField("amount_(inr)", StringType(), True),
                StructField("transaction_status", StringType(), True),
                StructField("sender_age_group", StringType(), True),
                StructField("receiver_age_group", StringType(), True),
                StructField("sender_state", StringType(), True),
                StructField("sender_bank", StringType(), True),
                StructField("receiver_bank", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("network_type", StringType(), True),
                StructField("fraud_flag", StringType(), True),
                StructField("hour_of_day", StringType(), True),
                StructField("day_of_week", StringType(), True),
                StructField("is_weekend", StringType(), True),
                StructField("_extract_timestamp", StringType(), True),
                StructField("_source", StringType(), True),
                StructField("_record_number", StringType(), True)
            ])
            
            df = df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")
            
            # Add extraction metadata
            df = df.withColumn("extract_date", current_timestamp()) \
                   .withColumn("source_system", lit("csv_kafka"))
            
            logger.info(f"Extracted {df.count()} records from Kafka")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting from Kafka: {e}")
            raise
            
    def save_raw_data(self, df, output_path):
        """Save raw extracted data"""
        try:
            logger.info(f"Saving raw data to: {output_path}")
            
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            logger.info("Raw data saved successfully")
            
        except Exception as e:
            logger.error(f"Error saving raw data: {e}")
            raise

def run_extraction(spark, config):
    """Main extraction function"""
    extractor = UPIExtractor(spark, config)
    
    # Extract data
    df = extractor.extract_from_kafka()
    
    # Save raw data
    output_path = "/opt/spark-data/processed/spark/raw/upi_transactions"
    extractor.save_raw_data(df, output_path)
    
    return df