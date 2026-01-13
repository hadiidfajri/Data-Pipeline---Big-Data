FROM bitnami/spark:3.3.2-debian-11-r10

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.3.2 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    pyyaml==6.0.1

# Create application directories
RUN mkdir -p /opt/spark-apps /opt/spark-data /opt/spark-config /opt/spark-logs

# Set permissions
RUN chmod -R 777 /opt/spark-apps /opt/spark-data /opt/spark-logs

WORKDIR /opt/spark-apps

USER 1001
