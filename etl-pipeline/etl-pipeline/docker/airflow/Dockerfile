FROM apache/airflow:2.7.3-python3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Install additional Airflow providers
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.3.0 \
    apache-airflow-providers-postgres==5.7.1