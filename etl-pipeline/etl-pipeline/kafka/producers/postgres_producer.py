import os
import json
import logging
import psycopg2
from kafka import KafkaProducer
from datetime import datetime
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgresKafkaProducer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.topic = os.getenv('KAFKA_TOPIC_FINANCE', 'india_finance_topic')
        self.batch_size = int(os.getenv('ETL_BATCH_SIZE', 1000))
        
        # PostgreSQL connection parameters
        self.pg_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'india_finance_db'),
            'user': os.getenv('POSTGRES_USER', 'etl_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'etl_password')
        }
        
        self.producer = None
        self.connection = None
        
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
            
    def connect_postgres(self):
        """Connect to PostgreSQL"""
        try:
            self.connection = psycopg2.connect(**self.pg_config)
            logger.info("Connected to PostgreSQL successfully")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
            
    def extract_and_produce(self):
        """Extract data from PostgreSQL and produce to Kafka"""
        cursor = self.connection.cursor()
        
        try:
            # Get total count
            cursor.execute("SELECT COUNT(*) FROM india_personal_finance")
            total_records = cursor.fetchone()[0]
            logger.info(f"Total records to process: {total_records}")
            
            # Extract data in batches
            offset = 0
            records_produced = 0
            
            while offset < total_records:
                query = f"""
                    SELECT 
                        id, income, age, dependents, occupation, city_tier,
                        rent, loan_repayment, insurance, groceries, transport,
                        eating_out, entertainment, utilities, healthcare, education,
                        miscellaneous, desired_savings_percentage, desired_savings,
                        disposable_income, potential_savings_groceries,
                        potential_savings_transport, potential_savings_eating_out,
                        potential_savings_entertainment, potential_savings_utilities,
                        potential_savings_healthcare, potential_savings_education,
                        potential_savings_miscellaneous, created_at, updated_at
                    FROM india_personal_finance
                    ORDER BY id
                    LIMIT {self.batch_size} OFFSET {offset}
                """
                
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                
                for row in cursor.fetchall():
                    record = dict(zip(columns, row))
                    
                    # Convert all values to string for Kafka
                    record_str = {
                        k: str(v) if v is not None else None 
                        for k, v in record.items()
                    }
                    
                    # Add metadata
                    record_str['_extract_timestamp'] = datetime.now().isoformat()
                    record_str['_source'] = 'postgres'
                    
                    # Produce to Kafka
                    self.producer.send(self.topic, value=record_str)
                    records_produced += 1
                    
                    if records_produced % 100 == 0:
                        logger.info(f"Produced {records_produced}/{total_records} records")
                
                offset += self.batch_size
                
            # Flush remaining messages
            self.producer.flush()
            logger.info(f"Successfully produced {records_produced} records to Kafka topic: {self.topic}")
            
        except Exception as e:
            logger.error(f"Error during extraction and production: {e}")
            raise
        finally:
            cursor.close()
            
    def close(self):
        """Close connections"""
        if self.producer:
            self.producer.close()
        if self.connection:
            self.connection.close()
        logger.info("Connections closed")
        
    def run(self):
        """Main execution method"""
        try:
            self.connect_kafka()
            self.connect_postgres()
            self.extract_and_produce()
        except Exception as e:
            logger.error(f"Error in producer: {e}")
            raise
        finally:
            self.close()

if __name__ == "__main__":
    producer = PostgresKafkaProducer()
    producer.run()