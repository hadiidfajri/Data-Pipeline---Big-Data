FROM bitnami/spark:3.3.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.3.0 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.5 \
    pandas==1.5.3 \
    numpy==1.24.0 \
    pyarrow==11.0.0

# Create directories
RUN mkdir -p /opt/spark-jobs /opt/data /opt/logs

# Set permissions
RUN chmod -R 777 /opt/spark-jobs /opt/data /opt/logs

WORKDIR /opt/spark-jobs

USER 1001