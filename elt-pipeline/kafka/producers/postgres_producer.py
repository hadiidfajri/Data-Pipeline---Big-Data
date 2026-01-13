#!/usr/bin/env python3
"""
Kafka Producer for PostgreSQL India Personal Finance Data
Reads data from PostgreSQL and streams to Kafka topic
"""

import os
import sys
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/logs/postgres_producer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration from environment
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'elt_database'),
    'user': os.getenv('POSTGRES_USER', 'elt_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'elt_password')
}

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'topic': os.getenv('KAFKA_TOPIC_POSTGRES', 'india_finance_topic')
}

class PostgresKafkaProducer:
    def __init__(self):
        self.producer = None
        self.pg_conn = None
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
    
    def connect_postgres(self):
        """Initialize PostgreSQL connection"""
        try:
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logger.info("Connected to PostgreSQL successfully")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def fetch_and_stream_data(self, batch_size=100):
        """Fetch data from PostgreSQL and stream to Kafka"""
        try:
            cursor = self.pg_conn.cursor(cursor_factory=RealDictCursor)
            
            # Query to fetch data in batches
            query = """
                SELECT 
                    id, income, age, dependents, occupation, city_tier,
                    rent, loan_repayment, insurance, groceries, transport,
                    eating_out, entertainment, utilities, healthcare,
                    education, miscellaneous, desired_savings_percentage,
                    desired_savings, disposable_income,
                    potential_savings_groceries, potential_savings_transport,
                    potential_savings_eating_out, potential_savings_entertainment,
                    potential_savings_utilities, potential_savings_healthcare,
                    potential_savings_education, potential_savings_miscellaneous,
                    created_at, updated_at
                FROM source_data.india_personal_finance
                ORDER BY id
            """
            
            cursor.execute(query)
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                for row in rows:
                    # Convert to JSON-serializable format
                    record = {k: str(v) if isinstance(v, datetime) else float(v) if isinstance(v, type(1.0)) else v 
                             for k, v in dict(row).items()}
                    
                    # Add metadata
                    record['_extracted_at'] = datetime.now().isoformat()
                    record['_source'] = 'postgres'
                    
                    # Send to Kafka
                    future = self.producer.send(
                        KAFKA_CONFIG['topic'],
                        key=str(record['id']),
                        value=record
                    )
                    
                    # Wait for confirmation
                    future.get(timeout=10)
                    self.records_sent += 1
                    
                    if self.records_sent % 100 == 0:
                        logger.info(f"Sent {self.records_sent} records to Kafka")
                
                # Flush after each batch
                self.producer.flush()
            
            cursor.close()
            logger.info(f"Total records sent: {self.records_sent}")
            
        except Exception as e:
            logger.error(f"Error streaming data: {e}")
            raise
    
    def close(self):
        """Close connections"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")
        
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("PostgreSQL connection closed")

def main():
    """Main execution"""
    producer = PostgresKafkaProducer()
    
    try:
        logger.info("Starting PostgreSQL to Kafka producer...")
        
        # Connect to services
        producer.connect_kafka()
        producer.connect_postgres()
        
        # Stream data
        producer.fetch_and_stream_data()
        
        logger.info("Data streaming completed successfully")
        
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        sys.exit(1)
    
    finally:
        producer.close()

if __name__ == "__main__":
    main()