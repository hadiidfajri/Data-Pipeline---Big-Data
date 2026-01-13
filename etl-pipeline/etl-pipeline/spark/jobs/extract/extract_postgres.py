from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresExtractor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.kafka_config = config['kafka']
        
    def extract_from_kafka(self):
        """Extract data from Kafka topic (india_finance_topic)"""
        try:
            logger.info("Starting extraction from Kafka topic: india_finance_topic")
            
            # Read from Kafka
            df = self.spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers']) \
                .option("subscribe", self.kafka_config['topic_finance']) \
                .option("startingOffsets", "earliest") \
                .load()
            
            # Convert value from binary to string
            df = df.selectExpr("CAST(value AS STRING) as json_data")
            
            # Parse JSON and select fields
            from pyspark.sql.types import *
            
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("income", StringType(), True),
                StructField("age", StringType(), True),
                StructField("dependents", StringType(), True),
                StructField("occupation", StringType(), True),
                StructField("city_tier", StringType(), True),
                StructField("rent", StringType(), True),
                StructField("loan_repayment", StringType(), True),
                StructField("insurance", StringType(), True),
                StructField("groceries", StringType(), True),
                StructField("transport", StringType(), True),
                StructField("eating_out", StringType(), True),
                StructField("entertainment", StringType(), True),
                StructField("utilities", StringType(), True),
                StructField("healthcare", StringType(), True),
                StructField("education", StringType(), True),
                StructField("miscellaneous", StringType(), True),
                StructField("desired_savings_percentage", StringType(), True),
                StructField("desired_savings", StringType(), True),
                StructField("disposable_income", StringType(), True),
                StructField("potential_savings_groceries", StringType(), True),
                StructField("potential_savings_transport", StringType(), True),
                StructField("potential_savings_eating_out", StringType(), True),
                StructField("potential_savings_entertainment", StringType(), True),
                StructField("potential_savings_utilities", StringType(), True),
                StructField("potential_savings_healthcare", StringType(), True),
                StructField("potential_savings_education", StringType(), True),
                StructField("potential_savings_miscellaneous", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("_extract_timestamp", StringType(), True),
                StructField("_source", StringType(), True)
            ])
            
            df = df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")
            
            # Add extraction metadata
            df = df.withColumn("extract_date", current_timestamp()) \
                   .withColumn("source_system", lit("postgres_kafka"))
            
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
    extractor = PostgresExtractor(spark, config)
    
    # Extract data
    df = extractor.extract_from_kafka()
    
    # Save raw data
    output_path = "/opt/spark-data/processed/spark/raw/india_finance"
    extractor.save_raw_data(df, output_path)
    
    return df