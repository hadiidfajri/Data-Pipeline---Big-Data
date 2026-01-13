FROM postgres:14

# Install additional tools if needed
RUN apt-get update && apt-get install -y \
    postgresql-contrib \
    && rm -rf /var/lib/apt/lists/*

# Copy initialization scripts
COPY ../../postgres/init/*.sql /docker-entrypoint-initdb.d/