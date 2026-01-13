# Use official Apache Spark image
FROM apache/spark:3.3.2-scala2.12-java11-python3-ubuntu

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip3 install --upgrade pip

# Install Python dependencies
RUN pip3 install --no-cache-dir \
    pyspark==3.3.2 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.5 \
    pandas==1.5.3 \
    numpy==1.24.0 \
    pyarrow==11.0.0

# Create directories
RUN mkdir -p /opt/spark-jobs /opt/data /opt/logs /opt/kafka /opt/hive

# Set permissions
RUN chmod -R 777 /opt/spark-jobs /opt/data /opt/logs /opt/kafka /opt/hive

# Set working directory
WORKDIR /opt/spark-jobs

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Switch to spark user (default in apache/spark image)
USER ${spark_uid}
