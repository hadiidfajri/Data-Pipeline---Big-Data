from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'india_finance_upi_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for India Personal Finance and UPI Transaction Analysis',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['etl', 'finance', 'upi', 'india'],
)

# Task 1: Initialize Kafka topics
init_kafka_topics = BashOperator(
    task_id='init_kafka_topics',
    bash_command='docker exec etl-kafka bash /kafka-scripts/topics.sh',
    dag=dag,
)

# Task 2: Produce India Finance data to Kafka
produce_finance_data = BashOperator(
    task_id='produce_finance_data',
    bash_command='docker exec etl-spark-master python3 /kafka-scripts/producers/postgres_producer.py',
    dag=dag,
)

# Task 3: Produce UPI data to Kafka
produce_upi_data = BashOperator(
    task_id='produce_upi_data',
    bash_command='docker exec etl-spark-master python3 /kafka-scripts/producers/upi_csv_producer.py',
    dag=dag,
)

# Task 4: Extract data from Kafka
extract_data = SparkSubmitOperator(
    task_id='extract_data',
    application='/opt/spark-apps/jobs/extract/extract_postgres.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0',
    },
    dag=dag,
)

# Task 5: Transform India Finance data
transform_finance = SparkSubmitOperator(
    task_id='transform_finance',
    application='/opt/spark-apps/jobs/transform/clean_finance.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
    },
    dag=dag,
)

# Task 6: Transform UPI data
transform_upi = SparkSubmitOperator(
    task_id='transform_upi',
    application='/opt/spark-apps/jobs/transform/clean_upi.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
    },
    dag=dag,
)

# Task 7: Join datasets
join_data = SparkSubmitOperator(
    task_id='join_datasets',
    application='/opt/spark-apps/jobs/transform/join_datasets.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
    },
    dag=dag,
)

# Task 8: Quality checks
quality_checks = SparkSubmitOperator(
    task_id='quality_checks',
    application='/opt/spark-apps/jobs/transform/quality_checks.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
    },
    dag=dag,
)

# Task 9: Load to Hive
load_to_hive = SparkSubmitOperator(
    task_id='load_to_hive',
    application='/opt/spark-apps/jobs/load/load_to_hive.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.warehouse.dir': '/opt/hive-data/warehouse',
        'spark.sql.hive.metastore.uris': 'thrift://hive-metastore:9083',
    },
    dag=dag,
)

# Task 10: Run complete ETL pipeline
run_complete_etl = SparkSubmitOperator(
    task_id='run_complete_etl',
    application='/opt/spark-apps/etl_main.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0',
        'spark.sql.warehouse.dir': '/opt/hive-data/warehouse',
        'spark.sql.hive.metastore.uris': 'thrift://hive-metastore:9083',
    },
    dag=dag,
)

# Task 11: Generate summary report
def generate_summary_report(**context):
    """Generate ETL execution summary"""
    logger.info("Generating ETL summary report")
    
    summary = {
        'execution_date': context['execution_date'].strftime('%Y-%m-%d %H:%M:%S'),
        'dag_run_id': context['run_id'],
        'status': 'SUCCESS',
        'pipeline': 'India Finance & UPI Transaction ETL'
    }
    
    logger.info(f"ETL Summary: {summary}")
    return summary

generate_report = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
init_kafka_topics >> [produce_finance_data, produce_upi_data]
[produce_finance_data, produce_upi_data] >> run_complete_etl
run_complete_etl >> generate_report

# Alternative detailed workflow (commented out, use one or the other)
"""
init_kafka_topics >> [produce_finance_data, produce_upi_data]
[produce_finance_data, produce_upi_data] >> extract_data
extract_data >> [transform_finance, transform_upi]
[transform_finance, transform_upi] >> join_data
join_data >> quality_checks
quality_checks >> load_to_hive
load_to_hive >> generate_report
"""