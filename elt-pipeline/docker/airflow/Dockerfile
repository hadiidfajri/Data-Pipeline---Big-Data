FROM apache/airflow:2.7.1-python3.10

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.3 \
    apache-airflow-providers-apache-hive==6.1.0 \
    psycopg2-binary==2.9.5 \
    kafka-python==2.0.2 \
    pyspark==3.3.0

# Initialize Airflow database
RUN airflow db init

WORKDIR /opt/airflow