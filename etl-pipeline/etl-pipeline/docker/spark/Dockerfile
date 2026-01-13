FROM apache/spark:3.3.2-scala2.12-java11-ubuntu

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.3.2 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.5 \
    pandas==1.5.3 \
    numpy==1.24.0 \
    pyarrow==11.0.0 \
    pyyaml==6.0.1

# Create application directories matching your structure
RUN mkdir -p /opt/spark-apps /opt/spark-data /opt/spark-config /opt/spark-logs

# Set permissions
RUN chmod -R 777 /opt/spark-apps /opt/spark-data /opt/spark-logs

WORKDIR /opt/spark-apps

USER 1001
