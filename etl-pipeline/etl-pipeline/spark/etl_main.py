from pyspark.sql import SparkSession
import yaml
import logging
import sys
from datetime import datetime

# Import ETL modules
sys.path.append('/opt/spark-apps/jobs')
from extract import extract_postgres, extract_upi_csv
from transform import clean_finance, clean_upi, join_datasets, quality_checks
from load import load_to_hive

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/spark-logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.spark = None
        self.config = None
        self.start_time = datetime.now()
        
    def initialize_spark(self):
        """Initialize Spark session with Hive and Kafka support"""
        logger.info("Initializing Spark session")
        
        self.spark = SparkSession.builder \
            .appName("India_Finance_UPI_ETL") \
            .config("spark.sql.warehouse.dir", "/opt/hive-data/warehouse") \
            .config("spark.sql.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2," +
                   "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully")
        
    def load_config(self):
        """Load configuration from YAML files"""
        logger.info("Loading configuration")
        
        with open('/opt/spark-config/postgres.yaml', 'r') as f:
            postgres_config = yaml.safe_load(f)
        
        with open('/opt/spark-config/kafka.yaml', 'r') as f:
            kafka_config = yaml.safe_load(f)
        
        with open('/opt/spark-config/spark.yaml', 'r') as f:
            spark_config = yaml.safe_load(f)
        
        self.config = {
            'postgres': postgres_config,
            'kafka': kafka_config,
            'spark': spark_config
        }
        
        logger.info("Configuration loaded successfully")
        
    def extract_phase(self):
        """Extract data from sources"""
        logger.info("=" * 50)
        logger.info("PHASE 1: EXTRACTION")
        logger.info("=" * 50)
        
        try:
            # Extract India Personal Finance data from Kafka (via Postgres)
            logger.info("Extracting India Personal Finance data...")
            df_finance = extract_postgres.run_extraction(self.spark, self.config)
            
            # Extract UPI Transaction data from Kafka (via CSV)
            logger.info("Extracting UPI Transaction data...")
            df_upi = extract_upi_csv.run_extraction(self.spark, self.config)
            
            logger.info("Extraction phase completed successfully")
            return df_finance, df_upi
            
        except Exception as e:
            logger.error(f"Error in extraction phase: {e}")
            raise
    
    def transform_phase(self, df_finance, df_upi):
        """Transform and clean data"""
        logger.info("=" * 50)
        logger.info("PHASE 2: TRANSFORMATION")
        logger.info("=" * 50)
        
        try:
            # Clean India Personal Finance data
            logger.info("Cleaning India Personal Finance data...")
            df_finance_clean = clean_finance.run_cleaning(
                self.spark,
                "/opt/spark-data/processed/spark/raw/india_finance"
            )
            
            # Save cleaned finance data
            df_finance_clean.write \
                .mode('overwrite') \
                .parquet("/opt/spark-data/processed/spark/clean/india_finance")
            
            # Clean UPI Transaction data
            logger.info("Cleaning UPI Transaction data...")
            df_upi_clean, df_upi_rejected = clean_upi.run_cleaning(
                self.spark,
                "/opt/spark-data/processed/spark/raw/upi_transactions"
            )
            
            # Save cleaned and rejected UPI data
            df_upi_clean.write \
                .mode('overwrite') \
                .parquet("/opt/spark-data/processed/spark/clean/upi_transactions")
            
            df_upi_rejected.write \
                .mode('overwrite') \
                .parquet("/opt/spark-data/processed/spark/rejected/upi_transactions")
            
            # Join datasets
            logger.info("Joining datasets...")
            df_joined, df_aggregated = join_datasets.run_join(
                self.spark,
                "/opt/spark-data/processed/spark/clean/india_finance",
                "/opt/spark-data/processed/spark/clean/upi_transactions"
            )
            
            # Save joined data
            df_joined.write \
                .mode('overwrite') \
                .parquet("/opt/spark-data/processed/spark/clean/joined_data")
            
            df_aggregated.write \
                .mode('overwrite') \
                .parquet("/opt/spark-data/processed/spark/clean/aggregated_data")
            
            # Data Quality Checks
            logger.info("Running data quality checks...")
            check_config = {
                'uniqueness_columns': ['transaction_id'],
                'range_rules': {
                    'amount': (0, 100000),
                    'age': (18, 100),
                    'income': (0, 500000)
                },
                'expected_types': {
                    'amount': 'double',
                    'age': 'int',
                    'income': 'double'
                },
                'numerical_columns': ['amount', 'income', 'disposable_income']
            }
            
            quality_report = quality_checks.run_quality_checks(
                self.spark,
                df_joined,
                check_config
            )
            
            # Log quality report
            logger.info(f"Quality Score: {quality_report['quality_score']}%")
            logger.info(f"Passed Checks: {quality_report['passed_checks']}/{quality_report['total_checks']}")
            
            # Save quality report
            with open('/opt/spark-logs/quality_report.json', 'w') as f:
                import json
                json.dump(quality_report, f, indent=2, default=str)
            
            logger.info("Transformation phase completed successfully")
            return df_joined
            
        except Exception as e:
            logger.error(f"Error in transformation phase: {e}")
            raise
    
    def load_phase(self, df_joined):
        """Load data to Hive data warehouse"""
        logger.info("=" * 50)
        logger.info("PHASE 3: LOAD")
        logger.info("=" * 50)
        
        try:
            hive_config = {
                'warehouse_dir': '/opt/hive-data/warehouse'
            }
            
            load_to_hive.run_load(self.spark, df_joined, hive_config)
            
            logger.info("Load phase completed successfully")
            
        except Exception as e:
            logger.error(f"Error in load phase: {e}")
            raise
    
    def run(self):
        """Execute complete ETL pipeline"""
        try:
            logger.info("=" * 70)
            logger.info("STARTING ETL PIPELINE: India Finance & UPI Transaction Analysis")
            logger.info("=" * 70)
            
            # Initialize
            self.initialize_spark()
            self.load_config()
            
            # Extract
            df_finance, df_upi = self.extract_phase()
            
            # Transform
            df_joined = self.transform_phase(df_finance, df_upi)
            
            # Load
            self.load_phase(df_joined)
            
            # Calculate execution time
            end_time = datetime.now()
            execution_time = (end_time - self.start_time).total_seconds()
            
            logger.info("=" * 70)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Total execution time: {execution_time:.2f} seconds")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()
