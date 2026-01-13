FROM postgres:14

# Install PostgreSQL extensions if needed
RUN apt-get update && \
    apt-get install -y postgresql-contrib && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment
ENV POSTGRES_INITDB_ARGS="--encoding=UTF8 --locale=en_US.utf8"