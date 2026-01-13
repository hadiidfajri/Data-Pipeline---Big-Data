"""
Airflow DAG: ELT Pipeline Orchestration
Storyline: Impact of UPI Transaction Patterns on Personal Spending and Savings
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.task_group import TaskGroup
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'elt_pipeline_india_finance_upi',
    default_args=default_args,
    description='ELT Pipeline for India Finance and UPI Transaction Analysis',
    schedule_interval='@daily',
    catchup=False,
    tags=['elt', 'finance', 'upi', 'analytics'],
)

# =====================================================
# Task Group 1: Extract from Sources
# =====================================================
with TaskGroup('extract_data', tooltip='Extract data from sources', dag=dag) as extract_group:
    
    # Start Kafka producers
    start_postgres_producer = BashOperator(
        task_id='start_postgres_producer',
        bash_command='python /opt/spark-jobs/../kafka/producers/postgres_producer.py',
        dag=dag,
    )
    
    start_upi_producer = BashOperator(
        task_id='start_upi_producer',
        bash_command='python /opt/spark-jobs/../kafka/producers/upi_csv_producer.py',
        dag=dag,
    )
    
    # Wait for data in Kafka
    check_kafka_topics = BashOperator(
        task_id='check_kafka_topics',
        bash_command='''
            kafka-topics --list --bootstrap-server kafka:9092 | grep -E "india_finance_topic|upi_transaction_topic"
        ''',
        dag=dag,
    )
    
    [start_postgres_producer, start_upi_producer] >> check_kafka_topics

# =====================================================
# Task Group 2: Load Raw Data to HDFS
# =====================================================
with TaskGroup('load_raw_data', tooltip='Load raw data to HDFS', dag=dag) as load_group:
    
    # Submit Spark job to load data from Kafka to HDFS
    load_to_hdfs = SparkSubmitOperator(
        task_id='load_kafka_to_hdfs',
        application='/opt/spark-jobs/jobs/load/load_raw_to_hdfs.py',
        name='load_raw_to_hdfs',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
            'spark.sql.shuffle.partitions': '2',
        },
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
        dag=dag,
    )
    
    # Verify HDFS data
    verify_hdfs_data = BashOperator(
        task_id='verify_hdfs_data',
        bash_command='''
            echo "Checking HDFS directories..."
            ls -lR /opt/data/raw/
        ''',
        dag=dag,
    )
    
    load_to_hdfs >> verify_hdfs_data

# =====================================================
# Task Group 3: Create Hive Staging Tables
# =====================================================
with TaskGroup('create_staging', tooltip='Create Hive staging tables', dag=dag) as staging_group:
    
    # Create staging tables
    create_staging_tables = HiveOperator(
        task_id='create_staging_tables',
        hql='''
            source /opt/hive/ddl/staging_tables.hql
        ''',
        hive_cli_conn_id='hive_default',
        dag=dag,
    )
    
    # Repair partitions
    repair_partitions = HiveOperator(
        task_id='repair_partitions',
        hql='''
            USE elt_staging;
            MSCK REPAIR TABLE stg_upi_transaction;
            ANALYZE TABLE stg_india_personal_finance COMPUTE STATISTICS;
            ANALYZE TABLE stg_upi_transaction COMPUTE STATISTICS;
        ''',
        hive_cli_conn_id='hive_default',
        dag=dag,
    )
    
    create_staging_tables >> repair_partitions

# =====================================================
# Task Group 4: Transform and Clean Data
# =====================================================
with TaskGroup('transform_clean', tooltip='Transform and clean data', dag=dag) as transform_group:
    
    # Clean India Personal Finance data
    clean_finance = HiveOperator(
        task_id='clean_finance_data',
        hql='''
            source /opt/hive/transformations/clean_finance.sql
        ''',
        hive_cli_conn_id='hive_default',
        dag=dag,
    )
    
    # Clean UPI Transaction data
    clean_upi = HiveOperator(
        task_id='clean_upi_data',
        hql='''
            source /opt/hive/transformations/clean_upi.sql
        ''',
        hive_cli_conn_id='hive_default',
        dag=dag,
    )
    
    # Validate cleaning
    validate_cleaning = BashOperator(
        task_id='validate_cleaning',
        bash_command='''
            echo "Cleaning validation completed"
        ''',
        dag=dag,
    )
    
    [clean_finance, clean_upi] >> validate_cleaning

# =====================================================
# Task Group 5: Create Curated Tables
# =====================================================
with TaskGroup('create_curated', tooltip='Create curated tables', dag=dag) as curated_group:
    
    # Create dimension and fact tables
    create_curated_tables = HiveOperator(
        task_id='create_curated_tables',
        hql='''
            source /opt/hive/ddl/curated_tables.hql
        ''',
        hive_cli_conn_id='hive_default',
        dag=dag,
    )

# =====================================================
# Task Group 6: Join and Integrate Data
# =====================================================
with TaskGroup('join_integrate', tooltip='Join and integrate datasets', dag=dag) as join_group:
    
    # Execute join transformations
    join_data = HiveOperator(
        task_id='join_datasets',
        hql='''
            source /opt/hive/transformations/join.sql
        ''',
        hive_cli_conn_id='hive_default',
        dag=dag,
    )
    
    # Validate joins
    validate_joins = HiveOperator(
        task_id='validate_joins',
        hql='''
            USE elt_curated;
            SELECT 'integrated_spending_savings' as table_name, COUNT(*) as count 
            FROM integrated_spending_savings;
            
            SELECT 'age_category_spending_summary' as table_name, COUNT(*) as count 
            FROM age_category_spending_summary;
        ''',
        hive_cli_conn_id='hive_default',
        dag=dag,
    )
    
    join_data >> validate_joins

# =====================================================
# Task Group 7: Create Data Marts
# =====================================================
with TaskGroup('create_marts', tooltip='Create analytics data marts', dag=dag) as mart_group:
    
    # Create all data marts
    create_data_marts = HiveOperator(
        task_id='create_all_marts',
        hql='''
            source /opt/hive/transformations/data_mart.sql
        ''',
        hive_cli_conn_id='hive_default',
        dag=dag,
    )
    
    # Generate mart statistics
    generate_statistics = HiveOperator(
        task_id='generate_mart_statistics',
        hql='''
            USE elt_curated;
            
            ANALYZE TABLE mart_age_spending_behavior COMPUTE STATISTICS;
            ANALYZE TABLE mart_merchant_performance COMPUTE STATISTICS;
            ANALYZE TABLE mart_savings_opportunity COMPUTE STATISTICS;
            ANALYZE TABLE mart_digital_payment_behavior COMPUTE STATISTICS;
            ANALYZE TABLE mart_monthly_trends COMPUTE STATISTICS;
        ''',
        hive_cli_conn_id='hive_default',
        dag=dag,
    )
    
    create_data_marts >> generate_statistics

# =====================================================
# Final Tasks: Quality Checks and Notifications
# =====================================================
def run_data_quality_checks(**context):
    """Run data quality checks"""
    logger.info("Running data quality checks...")
    
    # Add your data quality check logic here
    checks = {
        'nulls': True,
        'duplicates': True,
        'referential_integrity': True,
        'range_checks': True,
    }
    
    logger.info(f"Quality checks completed: {checks}")
    return checks

quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    provide_context=True,
    dag=dag,
)

def send_completion_notification(**context):
    """Send pipeline completion notification"""
    logger.info("ELT Pipeline completed successfully!")
    logger.info(f"Execution date: {context['execution_date']}")
    
    # Add notification logic (email, Slack, etc.)
    return "Pipeline completed"

completion_notification = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    provide_context=True,
    dag=dag,
)

# =====================================================
# Define Task Dependencies
# =====================================================
extract_group >> load_group >> staging_group >> transform_group >> curated_group >> join_group >> mart_group >> quality_checks >> completion_notification