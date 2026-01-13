import os
import json
import logging
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UPICSVKafkaProducer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.topic = os.getenv('KAFKA_TOPIC_UPI', 'upi_transaction_topic')
        self.batch_size = int(os.getenv('ETL_BATCH_SIZE', 1000))
        self.csv_path = '/opt/spark-data/raw/csv/upi_transaction/UPI_Transaction.csv'
        self.producer = None
        
    def connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info("Connected to Kafka successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
            
    def read_and_produce(self):
        """Read CSV file and produce to Kafka"""
        try:
            # Read CSV with all columns as string to prevent data loss
            logger.info(f"Reading CSV file from: {self.csv_path}")
            df = pd.read_csv(self.csv_path, dtype=str)
            
            # Clean column names
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
            
            logger.info(f"Total records to process: {len(df)}")
            logger.info(f"Columns: {list(df.columns)}")
            
            records_produced = 0
            
            # Process in batches
            for start_idx in range(0, len(df), self.batch_size):
                end_idx = min(start_idx + self.batch_size, len(df))
                batch = df.iloc[start_idx:end_idx]
                
                for idx, row in batch.iterrows():
                    # Convert row to dictionary
                    record = row.to_dict()
                    
                    # Add metadata
                    record['_extract_timestamp'] = datetime.now().isoformat()
                    record['_source'] = 'csv_file'
                    record['_record_number'] = str(idx)
                    
                    # Produce to Kafka
                    self.producer.send(self.topic, value=record)
                    records_produced += 1
                    
                    if records_produced % 1000 == 0:
                        logger.info(f"Produced {records_produced}/{len(df)} records")
                
                # Flush after each batch
                self.producer.flush()
                
            logger.info(f"Successfully produced {records_produced} records to Kafka topic: {self.topic}")
            
        except Exception as e:
            logger.error(f"Error during CSV reading and production: {e}")
            raise
            
    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.producer.close()
        logger.info("Producer closed")
        
    def run(self):
        """Main execution method"""
        try:
            self.connect_kafka()
            self.read_and_produce()
        except Exception as e:
            logger.error(f"Error in producer: {e}")
            raise
        finally:
            self.close()

if __name__ == "__main__":
    producer = UPICSVKafkaProducer()
    producer.run()