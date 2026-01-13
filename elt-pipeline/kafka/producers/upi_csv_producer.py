#!/usr/bin/env python3
"""
Kafka Producer for UPI Transaction CSV Data
Reads CSV file and streams to Kafka topic
"""

import os
import sys
import json
import csv
import logging
from datetime import datetime
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/logs/upi_producer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CSV_FILE_PATH = '/opt/data/raw/csv/upi_transaction/UPI_Transaction.csv'
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'topic': os.getenv('KAFKA_TOPIC_UPI', 'upi_transaction_topic')
}

class UPIKafkaProducer:
    def __init__(self):
        self.producer = None
        self.records_sent = 0
        
    def connect_kafka(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info("Connected to Kafka successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def read_and_stream_csv(self, batch_size=100):
        """Read CSV and stream to Kafka"""
        try:
            with open(CSV_FILE_PATH, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                batch = []
                
                for row in reader:
                    # Clean and prepare data
                    record = {
                        'transaction_id': row.get('transaction id', '').strip(),
                        'timestamp': row.get('timestamp', '').strip(),
                        'transaction_type': row.get('transaction type', '').strip(),
                        'merchant_category': row.get('merchant_category', '').strip(),
                        'amount_inr': float(row.get('amount (INR)', 0)),
                        'transaction_status': row.get('transaction_status', '').strip(),
                        'sender_age_group': row.get('sender_age_group', '').strip(),
                        'receiver_age_group': row.get('receiver_age_group', '').strip(),
                        'sender_state': row.get('sender_state', '').strip(),
                        'sender_bank': row.get('sender_bank', '').strip(),
                        'receiver_bank': row.get('receiver_bank', '').strip(),
                        'device_type': row.get('device_type', '').strip(),
                        'network_type': row.get('network_type', '').strip(),
                        'fraud_flag': int(row.get('fraud_flag', 0)),
                        'hour_of_day': int(row.get('hour_of_day', 0)),
                        'day_of_week': row.get('day_of_week', '').strip(),
                        'is_weekend': int(row.get('is_weekend', 0))
                    }
                    
                    # Add metadata
                    record['_extracted_at'] = datetime.now().isoformat()
                    record['_source'] = 'csv_file'
                    
                    batch.append(record)
                    
                    # Send batch when full
                    if len(batch) >= batch_size:
                        self.send_batch(batch)
                        batch = []
                
                # Send remaining records
                if batch:
                    self.send_batch(batch)
                
                logger.info(f"Total records sent: {self.records_sent}")
                
        except FileNotFoundError:
            logger.error(f"CSV file not found: {CSV_FILE_PATH}")
            raise
        except Exception as e:
            logger.error(f"Error reading/streaming CSV: {e}")
            raise
    
    def send_batch(self, batch):
        """Send a batch of records to Kafka"""
        for record in batch:
            try:
                future = self.producer.send(
                    KAFKA_CONFIG['topic'],
                    key=record['transaction_id'],
                    value=record
                )
                future.get(timeout=10)
                self.records_sent += 1
                
                if self.records_sent % 100 == 0:
                    logger.info(f"Sent {self.records_sent} records to Kafka")
                    
            except Exception as e:
                logger.error(f"Failed to send record {record['transaction_id']}: {e}")
        
        self.producer.flush()
    
    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")

def main():
    """Main execution"""
    producer = UPIKafkaProducer()
    
    try:
        logger.info("Starting UPI CSV to Kafka producer...")
        
        # Check if file exists
        if not os.path.exists(CSV_FILE_PATH):
            logger.error(f"CSV file not found: {CSV_FILE_PATH}")
            sys.exit(1)
        
        # Connect and stream
        producer.connect_kafka()
        producer.read_and_stream_csv()
        
        logger.info("CSV streaming completed successfully")
        
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        sys.exit(1)
    
    finally:
        producer.close()

if __name__ == "__main__":
    main()